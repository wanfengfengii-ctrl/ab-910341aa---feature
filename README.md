# 海洋观测浮标采样 API

浮标观测数据的原子批量接入与**稳定快照分页**服务。分析人员分页导出期间，
即使持续接收到新数据（含更早时刻的观测），也不会出现漏项、重复或页序漂移。

- 运行时：Node.js 22，**零第三方依赖**（内置 `node:sqlite`、`fetch`、`node:test`）
- 存储：SQLite（WAL 模式，持久化到 `/data`）
- 容器：多阶段 Dockerfile（`runtime` / `verify`）+ Docker Compose（健康检查、一次性 verify）

## 快速开始

```bash
# 宿主机端口可用环境变量配置（默认 8080）
HOST_PORT=18080 docker compose up -d --build api
curl http://localhost:18080/health

# 一键验证：构建镜像 -> 清洁启动 -> 代码测试 -> API 冒烟（以退出码汇报）
./scripts/verify.sh
docker compose logs verify     # 查看 verify 明细
```

本地开发（无需 Docker）：

```bash
npm test                 # 单元 + HTTP 集成测试（32 项）
npm run smoke            # 本地拉起服务冒烟，含进程重启后续游标（49 项检查）
PORT=8080 npm start
```

## API

### `POST /api/streams/{streamId}/samples`

原子接收 **1–100** 条观测。请求体：

```json
{
  "samples": [
    { "sampleId": "buoy-001", "ts": "2026-10-05T08:30:00Z", "value": 42 },
    { "sampleId": "buoy-002", "ts": "2026-10-05T08:30:00+08:00", "value": -7 }
  ]
}
```

- `sampleId`：流内唯一（非空字符串）；`ts`：严格 RFC3339（带时区，内部归一化为 UTC）；`value`：整数。
- 批内重复，或与已有编号冲突 → **整批回滚**，返回 `409`，不留下任何部分写入。
- 成功返回 `200`：`{ "accepted": n, "firstSeq": x, "lastSeq": y }`。

错误码：`duplicate_in_batch` / `conflict_existing`（409），`invalid_request`（400）。

### `POST /api/streams/{streamId}/samples/{sampleId}/revisions`

质控勘误：修正误报的**时刻或读数**。原始观测视为**第 1 版**，每次成功勘误创建下一版。
请求体：

```json
{ "expectedRevision": 1, "ts": "2026-10-05T08:30:00Z", "value": 42 }
```

- `expectedRevision`：调用方已知的当前修订号（>=1 整数，必填）；首次勘误传 `1`。
- `ts`：严格 RFC3339（带时区，内部归一化为 UTC）；`value`：整数。
- 成功返回 `200`：`{ "sampleId": ..., "revision": n, "effectiveSeq": x }`，
  其中 `effectiveSeq` 是新版本分配到的接收序号（下一次快照上界）。
- **未知观测（含未知流）→ 404** `sample_not_found`；
  **`expectedRevision` 与当前修订号不一致 → 409** `revision_conflict`，
  响应 `details.currentRevision` 回送当前修订号，**且不改写任何数据**；
  请求体非法 → 400 `invalid_request`。

勘误版的生效机制：新版本不覆盖原始行，而是分配下一个全局接收序号
`effective_seq` 并以乐观锁（流内 `(sampleId, revision)` 唯一）写入。于是：

- **勘误前取得的分页游标**（快照上界 `< effectiveSeq`）继续看到**旧版**——包括旧时刻
  与旧读数，同一观测在该快照中**恰好一次**，不消失、不重复、页序不漂移；
- **勘误后开启的无游标会话**固定新的快照上界，只看到**新版**，并按**新版时刻**
  重新参与 `(ts, sampleId)` 稳定排序；
- 时间范围按所见版本的时刻过滤：旧会话按旧时刻，新会话按新时刻。

错误码：`sample_not_found`（404），`revision_conflict`（409），`invalid_request`（400）。

### `GET /api/streams/{streamId}/samples`

按 **(时刻, sampleId) 稳定升序**遍历，支持：

| 参数 | 说明 |
| --- | --- |
| `from` / `to` | RFC3339 闭区间时间范围，可缺省 |
| `pageSize` | 每页条数，1–1000，默认 100 |
| `cursor` | 上一页响应中的不透明游标 |

```json
{
  "items": [ { "sampleId": "buoy-001", "ts": "2026-10-05T08:30:00Z", "value": 42 } ],
  "pageSize": 100,
  "snapshotSeq": 128,
  "nextCursor": "eyJ2Ijox….<HMAC>",
  "done": false
}
```

### 快照一致性保证

- 每个无游标请求（会话首页）把**当前接收序号固定为快照上界 `snapshotSeq`**；
  后续页只可见生效序号 `<= snapshotSeq` 的数据（新接收与质控勘误共用该序号空间）。
- 遍历期间到达的任何数据——哪怕时刻更早，或某条已遍历/未遍历观测被勘误——都不会
  进入本次遍历：**最终恰好返回快照中每项一次**，顺序恒定，旧版时刻与读数保持不变。
- 每次响应都带回同一个 `snapshotSeq`；遍历结束时 `nextCursor` 为 `null` 且 `done: true`。
- 需要新数据（含勘误后的新版）时重新发起一次无游标请求，即固定新的快照上界；
  新版按其**新时刻**重新参与稳定排序，同一观测仍只出现一次。
- 分页采用 keyset（`(ts, sampleId)` 元组比较）而非 OFFSET，插入不会导致漂移。

### 游标安全与生命周期

- 游标为 `base64url(JSON).HMAC-SHA256`，不透明、不可伪造；密钥由环境变量
  `CURSOR_SECRET` 配置（compose 固定注入，故**服务重启后旧游标仍可继续**）。
- 游标载荷绑定流 ID、快照序号、原始时间范围与页大小。非法使用返回明确的 4xx：

| 情形 | 状态码 | error.code |
| --- | --- | --- |
| 篡改/伪造/格式损坏 | 400 | `cursor_signature_invalid` / `invalid_cursor` |
| 跨流复用 | 400 | `cursor_stream_mismatch` |
| 携带游标但改变原 `from`/`to` | 400 | `cursor_range_mismatch` |

### `GET /health`

容器健康检查端点，返回服务状态、当前接收序号与样本总数。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `HOST_PORT` | `8080` | 宿主机映射端口（compose 层） |
| `PORT` | `8080` | 容器内监听端口 |
| `HOST` | `0.0.0.0` | 监听地址 |
| `DB_PATH` | `/data/observations.db` | SQLite 数据文件 |
| `CURSOR_SECRET` | 随机（仅开发） | 游标 HMAC 密钥；生产必须固定以便重启续用 |

## verify 一次性服务

`docker compose up verify`（或 `./scripts/verify.sh`）会：

1. 删除残留容器与**命名数据卷**（`down -v`），保证 API 从空库清洁启动；
2. 等待 `api` 健康检查通过（`depends_on: service_healthy`）；
3. 在**清洁启动**的 API 上运行代码测试；
4. 插入初始数据、分页取首页，**在分页中途追加更早时刻的观测**，继续翻页并断言
   不漏项 / 不重复 / 页序不漂移、快照序号不变；
5. **在分页中途执行质控勘误**（时刻前移与后移两种情形），断言勘误前游标看到旧版且
   恰好一次、勘误后新会话只见新版并按新时刻重排；并覆盖过期修订号 409（回送当前
   修订号且不改写）、未知观测 404、非法请求体 400；
6. 覆盖批内重复、编号冲突整批拒绝、游标篡改、跨流复用、改变时间范围等错误路径。

全部通过退出码为 `0`，任一失败非零；结束时自动清理数据卷。
