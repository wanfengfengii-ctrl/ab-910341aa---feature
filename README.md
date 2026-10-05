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
npm test                 # 单元 + HTTP 集成测试（28 项）
npm run smoke            # 本地拉起服务冒烟，含进程重启后续游标与分页中途勘误
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

质控人员对已入库观测发布**勘误**（修正误报的时刻或读数）。原始观测视为**第 1 版**，
每次成功勘误追加下一版，历史版本永不覆写。请求体：

```json
{ "expectedRevision": 1, "ts": "2026-10-05T08:45:00Z", "value": 43 }
```

- `expectedRevision`：客户端所依据的当前修订号（正整数）；仅当它等于服务端当前修订号时
  才创建下一版（乐观锁）。
- `ts`：修正后的观测时刻（严格 RFC3339，归一化为 UTC）；`value`：修正后的整数读数。
- 成功返回 `201`：`{ "sampleId": "...", "revision": 2, "effectiveSeq": 129 }`，
  其中 `effectiveSeq` 是新版生效的接收序号（新版自该序号起对新快照可见）。

| 情形 | 状态码 | error.code |
| --- | --- | --- |
| 未知观测（流内不存在该 sampleId） | 404 | `sample_not_found` |
| `expectedRevision` 过期（已被他人更新） | 409 | `revision_conflict`（`details.currentRevision` 回传当前修订号） |
| 请求体非法（坏 JSON、修订号非正整数、时刻非法、读数非整数） | 400 | `invalid_request` |

> 409 时服务端**不写入、不占用序号**：用响应里的 `currentRevision`（及最新内容）
> 刷新依据后重试即可。并发勘误由单写事务串行化，不会丢更新。

### 勘误与快照的关系

勘误版本与普通接收一样分配单调接收序号，因此既有快照机制直接覆盖勘误场景：

- **修订前取得的分页游标**继续看到**旧版**观测（旧时刻、旧读数、旧页序），
  即便勘误把该观测移到别处，它在该快照中仍**恰好出现一次**，不会消失或重复。
- **修订后开启的无游标会话**只看到**新版**，并按**新版时刻**重新参与
  `(ts, sampleId)` 稳定排序；同一观测仍恰好一次。
- 不使用勘误接口时，批量接收、时间范围、游标重启续用与全部错误语义完全不变。

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
  后续页只可见 `seq <= snapshotSeq` 的数据（勘误版本同样受此上界约束）。
- 遍历期间到达的任何数据——哪怕时刻更早，或质控人员在遍历中途发布勘误——都不会进入
  本次遍历：**最终恰好返回快照中每项一次**，顺序恒定，且旧游标持续展示旧版内容。
- 每次响应都带回同一个 `snapshotSeq`；遍历结束时 `nextCursor` 为 `null` 且 `done: true`。
- 需要新数据/新版勘误时重新发起一次无游标请求，即固定新的快照上界。
- 分页采用 keyset（`(ts, sampleId)` 元组比较）而非 OFFSET，插入或勘误都不会导致漂移。

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

1. 构建 api / verify 镜像，并在启动前删除 `obs-data` 命名卷（**清洁数据卷**，退出时再次清理）；
2. 等待 `api` 健康检查通过（`depends_on: service_healthy`）；
3. 在清洁启动的 API 上运行代码测试（单元 + HTTP 集成）；
4. 插入初始数据、分页取首页，**在分页中途追加更早时刻的观测**，继续翻页并断言
   不漏项 / 不重复 / 页序不漂移、快照序号不变；
5. **在分页中途执行质控勘误**：断言修订前取得的游标继续看到旧版且每观测恰好一次、
   修订后新会话只看新版并按新版时刻重排；并覆盖未知观测 404、过期修订号 409
   （回传当前修订号、不改写数据）、非法请求 400 等路径；
6. 覆盖批内重复、编号冲突整批拒绝、游标篡改、跨流复用、改变时间范围等错误路径。

全部通过退出码为 `0`，任一失败非零。
