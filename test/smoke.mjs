/**
 * API 冒烟测试。
 *
 * 用法：
 *   BASE_URL=http://api:8080 node test/smoke.mjs   # 对已运行服务冒烟（compose verify 用）
 *   node test/smoke.mjs --local                    # 本地临时拉起服务，含“重启后续游标”
 *
 * 任一检查失败 -> 进程以非零退出码结束。
 */
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import process from 'node:process';

let failures = 0;
const logs = [];
function check(name, cond, detail = '') {
  if (cond) {
    logs.push(`  ✓ ${name}`);
  } else {
    logs.push(`  ✗ ${name} ${detail}`);
    failures += 1;
  }
}

async function waitForHealth(baseUrl, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(baseUrl + '/health');
      if (r.ok) return true;
    } catch { /* 重试 */ }
    await new Promise((r) => setTimeout(r, 200));
  }
  return false;
}

async function runSmoke(baseUrl, { restartFlow = null } = {}) {
  logs.push(`# 对 ${baseUrl} 进行 API 冒烟`);
  // 每次运行使用唯一流名：对持久卷重复执行 verify 也互不污染（清洁会话）。
  const runId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const S = (name) => `${name}-${runId}`;
  const get = (p) => fetch(baseUrl + p);
  const post = (stream, samples) =>
    fetch(`${baseUrl}/api/streams/${stream}/samples`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ samples }),
    });
  const revise = (stream, sampleId, body) =>
    fetch(`${baseUrl}/api/streams/${stream}/samples/${encodeURIComponent(sampleId)}/revisions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });

  // 1) 健康检查
  check('GET /health 200', await waitForHealth(baseUrl));

  // 2) 正常批量接收
  const stream = S('buoy-smoke');
  let r = await post(stream, [
    { sampleId: 'b03', ts: '2026-09-01T00:00:03Z', value: 30 },
    { sampleId: 'b01', ts: '2026-09-01T00:00:01Z', value: 10 },
    { sampleId: 'b02', ts: '2026-09-01T00:00:02Z', value: 20 },
  ]);
  check('POST 合法批次 200', r.status === 200, `status=${r.status}`);
  const posted = await r.json();

  // 3) 批内重复 -> 409，整批拒绝
  r = await post(stream, [
    { sampleId: 'x', ts: '2026-09-02T00:00:00Z', value: 1 },
    { sampleId: 'x', ts: '2026-09-02T00:00:01Z', value: 2 },
  ]);
  let body = await r.json();
  check('POST 批内重复 -> 409 duplicate_in_batch',
    r.status === 409 && body.error.code === 'duplicate_in_batch',
    `status=${r.status}`);

  // 4) 与已有冲突 -> 409，且同批的合法项不留部分写入
  r = await post(stream, [
    { sampleId: 'ghost', ts: '2026-09-03T00:00:00Z', value: 9 },
    { sampleId: 'b01', ts: '2026-09-03T00:00:01Z', value: 9 },
  ]);
  body = await r.json();
  check('POST 编号冲突 -> 409 conflict_existing',
    r.status === 409 && body.error.code === 'conflict_existing', `status=${r.status}`);

  // 5) 快照分页：pageSize=2 先取首页
  r = await get(`/api/streams/${stream}/samples?pageSize=2`);
  let page = await r.json();
  const snapSeq = page.snapshotSeq;
  check('首页按 (ts, sampleId) 稳定排序',
    JSON.stringify(page.items.map((i) => i.sampleId)) === JSON.stringify(['b01', 'b02']),
    JSON.stringify(page.items));
  check('首页给出下一游标', typeof page.nextCursor === 'string' && page.items.length === 2);
  check('首页 snapshotSeq 等于写入后序号', snapSeq === posted.lastSeq,
    `${snapSeq} != ${posted.lastSeq}`);
  const cursor1 = page.nextCursor;

  // 6) 分页中途插入更早时刻（及更晚时刻）数据
  r = await post(stream, [
    { sampleId: 'old1', ts: '2026-08-01T00:00:00Z', value: 100 },
    { sampleId: 'old2', ts: '2026-08-02T00:00:00Z', value: 101 },
    { sampleId: 'b04', ts: '2026-09-01T00:00:04Z', value: 40 },
  ]);
  check('中途插入早时刻批次被接收', r.status === 200, `status=${r.status}`);

  // 7) 继续遍历旧快照：不漏、不重、不漂移，snapshotSeq 不变
  const seen = ['b01', 'b02'];
  let cursor = cursor1;
  let guard = 0;
  while (cursor) {
    r = await get(`/api/streams/${stream}/samples?pageSize=2&cursor=${encodeURIComponent(cursor)}`);
    check('翻页请求 200', r.status === 200, `status=${r.status}`);
    page = await r.json();
    check('后续页 snapshotSeq 保持不变', page.snapshotSeq === snapSeq,
      `${page.snapshotSeq} != ${snapSeq}`);
    for (const it of page.items) seen.push(it.sampleId);
    cursor = page.nextCursor;
    if (++guard > 20) { check('分页守卫（死循环保护）', false); break; }
  }
  check('旧快照恰好每项一次（无漏项/重复/漂移）',
    JSON.stringify(seen) === JSON.stringify(['b01', 'b02', 'b03']),
    JSON.stringify(seen));
  check('结束标记 done=true', page.done === true && page.nextCursor === null);
  check('ghost/x 未发生部分写入',
    !seen.includes('ghost') && !seen.includes('x'));

  // 8) 游标篡改
  const tampered = cursor1.slice(0, -1) + (cursor1.endsWith('A') ? 'B' : 'A');
  r = await get(`/api/streams/${stream}/samples?cursor=${encodeURIComponent(tampered)}`);
  body = await r.json();
  check('篡改游标 -> 400 cursor_signature_invalid',
    r.status === 400 && body.error.code === 'cursor_signature_invalid',
    `status=${r.status}`);

  // 9) 跨流复用
  r = await get(`/api/streams/other-buoy/samples?cursor=${encodeURIComponent(cursor1)}`);
  body = await r.json();
  check('跨流复用游标 -> 400 cursor_stream_mismatch',
    r.status === 400 && body.error.code === 'cursor_stream_mismatch',
    `status=${r.status}`);

  // 10) 改变原时间范围
  const rangeStream = S('ranged');
  await post(rangeStream, [
    { sampleId: 'c1', ts: '2026-01-01T00:00:00Z', value: 1 },
    { sampleId: 'c2', ts: '2026-01-01T00:00:00Z', value: 2 },
  ]);
  r = await get(`/api/streams/${rangeStream}/samples?pageSize=1` +
    '&from=2026-01-01T00:00:00Z&to=2026-01-01T00:00:00Z');
  page = await r.json();
  const rangeCursor = page.nextCursor;
  r = await get(`/api/streams/${rangeStream}/samples?pageSize=1` +
    `&cursor=${encodeURIComponent(rangeCursor)}` +
    '&from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z');
  body = await r.json();
  check('改变时间范围 -> 400 cursor_range_mismatch',
    r.status === 400 && body.error.code === 'cursor_range_mismatch',
    `status=${r.status}`);

  // 11) 非法请求体 / 批量越界
  r = await fetch(`${baseUrl}/api/streams/${stream}/samples`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: '{oops',
  });
  check('坏 JSON -> 400', r.status === 400, `status=${r.status}`);
  r = await post(stream, []);
  check('空批次 -> 400', r.status === 400, `status=${r.status}`);
  r = await post(stream, Array.from({ length: 101 }, (_, i) =>
    ({ sampleId: `big${i}`, ts: '2026-09-04T00:00:00Z', value: i })));
  check('101 条批次 -> 400', r.status === 400, `status=${r.status}`);

  // 12) 新快照能看到早到数据
  r = await get(`/api/streams/${stream}/samples?pageSize=100`);
  page = await r.json();
  check('新会话固定新快照上界', page.snapshotSeq > snapSeq);
  check('新快照包含中途插入的早时刻数据',
    page.items.slice(0, 2).map((i) => i.sampleId).join(',') === 'old1,old2',
    JSON.stringify(page.items.map((i) => i.sampleId)));

  // 14) 勘误：分页中途修正时刻/读数 —— 旧游标见旧版且恰好一次，新会话按新版时刻排序
  // 14a) 勘误把“已翻页过”的观测时刻后移：旧会话不得在新位置再次看到它
  const revStream = S('revise-later');
  r = await post(revStream, [1, 2, 3, 4, 5].map((n) => ({
    sampleId: `m${n}`, ts: `2026-10-01T00:00:0${n}Z`, value: n * 10,
  })));
  const revPosted = await r.json();
  r = await get(`/api/streams/${revStream}/samples?pageSize=2`);
  page = await r.json();
  const revSnap = page.snapshotSeq;
  check('勘误流首页取到 m1,m2 旧版',
    JSON.stringify(page.items.map((i) => [i.sampleId, i.value])) ===
    JSON.stringify([['m1', 10], ['m2', 20]]), JSON.stringify(page.items));
  let revCursor = page.nextCursor;

  r = await revise(revStream, 'm1', {
    expectedRevision: 1, ts: '2026-10-01T00:00:10Z', value: 11,
  });
  let revBody = await r.json();
  check('勘误 m1 -> 200 revision=2 且分配下一个接收序号',
    r.status === 200 && revBody.revision === 2 &&
    revBody.effectiveSeq === revPosted.lastSeq + 1,
    `status=${r.status} body=${JSON.stringify(revBody)}`);

  const oldSeen = [...page.items.map((i) => i.sampleId)];
  while (revCursor) {
    r = await get(`/api/streams/${revStream}/samples?pageSize=2&cursor=${encodeURIComponent(revCursor)}`);
    check('勘误后旧游标翻页 200', r.status === 200, `status=${r.status}`);
    page = await r.json();
    check('勘误后旧会话 snapshotSeq 不变', page.snapshotSeq === revSnap,
      `${page.snapshotSeq} != ${revSnap}`);
    oldSeen.push(...page.items.map((i) => i.sampleId));
    revCursor = page.nextCursor;
  }
  check('旧快照恰好 m1..m5 各一次（后移的新版不在尾部重复）',
    JSON.stringify(oldSeen) === JSON.stringify(['m1', 'm2', 'm3', 'm4', 'm5']),
    JSON.stringify(oldSeen));

  // 新无游标会话：只见新版，m1 按新时刻排到最后，值为 11
  r = await get(`/api/streams/${revStream}/samples?pageSize=100`);
  page = await r.json();
  check('勘误后新会话 snapshotSeq 推进', page.snapshotSeq >= revBody.effectiveSeq,
    `${page.snapshotSeq} < ${revBody.effectiveSeq}`);
  check('新会话按新版时刻重排：m2..m5,m1，且 m1 为新读数',
    JSON.stringify(page.items.map((i) => i.sampleId)) ===
    JSON.stringify(['m2', 'm3', 'm4', 'm5', 'm1']) &&
    page.items[4].value === 11 &&
    new Set(page.items.map((i) => i.sampleId)).size === 5,
    JSON.stringify(page.items));

  // 14b) 勘误把“尚未翻到”的观测时刻前移：旧会话不得跳过它，仍按旧位置旧值出现
  const earlyStream = S('revise-earlier');
  await post(earlyStream, [1, 2, 3, 4, 5].map((n) => ({
    sampleId: `e${n}`, ts: `2026-10-02T00:00:0${n}Z`, value: n,
  })));
  r = await get(`/api/streams/${earlyStream}/samples?pageSize=2`);
  page = await r.json();
  const earlySnap = page.snapshotSeq;
  let earlyCursor = page.nextCursor;
  r = await revise(earlyStream, 'e4', {
    expectedRevision: 1, ts: '2026-10-02T00:00:00Z', value: 404,
  });
  check('勘误 e4（前移）-> 200 revision=2',
    r.status === 200 && (await r.json()).revision === 2, `status=${r.status}`);
  const earlySeen = ['e1', 'e2'];
  const earlyValues = new Map(page.items.map((i) => [i.sampleId, i.value]));
  while (earlyCursor) {
    r = await get(`/api/streams/${earlyStream}/samples?pageSize=2&cursor=${encodeURIComponent(earlyCursor)}`);
    page = await r.json();
    check('前移勘误后旧会话 snapshotSeq 不变', page.snapshotSeq === earlySnap);
    for (const it of page.items) {
      earlySeen.push(it.sampleId);
      earlyValues.set(it.sampleId, it.value);
    }
    earlyCursor = page.nextCursor;
  }
  check('旧快照仍按旧位置看到 e4 旧值，且无漏项/重复',
    JSON.stringify(earlySeen) === JSON.stringify(['e1', 'e2', 'e3', 'e4', 'e5']) &&
    earlyValues.get('e4') === 4,
    JSON.stringify(earlySeen));
  r = await get(`/api/streams/${earlyStream}/samples?pageSize=100`);
  page = await r.json();
  check('新会话中 e4 按新时刻排到首位且为新值，其余各一次',
    JSON.stringify(page.items.map((i) => i.sampleId)) ===
    JSON.stringify(['e4', 'e1', 'e2', 'e3', 'e5']) &&
    page.items[0].value === 404,
    JSON.stringify(page.items));

  // 14c) 并发冲突：过期修订号 -> 409 回送当前修订号，且不改写数据
  r = await revise(revStream, 'm1', {
    expectedRevision: 1, ts: '2026-10-01T00:00:11Z', value: 999,
  });
  revBody = await r.json();
  check('过期 expectedRevision -> 409 revision_conflict 且回送 currentRevision=2',
    r.status === 409 && revBody.error.code === 'revision_conflict' &&
    revBody.error.details.currentRevision === 2 &&
    revBody.error.details.expectedRevision === 1,
    `status=${r.status} body=${JSON.stringify(revBody)}`);
  r = await get(`/api/streams/${revStream}/samples?pageSize=100`);
  page = await r.json();
  check('409 不改写数据：m1 仍是第 2 版（值 11，时刻 10）',
    page.items.find((i) => i.sampleId === 'm1').value === 11,
    JSON.stringify(page.items.find((i) => i.sampleId === 'm1')));
  r = await revise(revStream, 'm1', {
    expectedRevision: 2, ts: '2026-10-01T00:00:09Z', value: 12,
  });
  revBody = await r.json();
  check('用当前修订号再次勘误 -> 200 revision=3',
    r.status === 200 && revBody.revision === 3, `status=${r.status} body=${JSON.stringify(revBody)}`);

  // 14d) 未知观测 / 未知流 -> 404；非法请求体 -> 400
  r = await revise(revStream, 'ghost', {
    expectedRevision: 1, ts: '2026-10-01T00:00:00Z', value: 1,
  });
  body = await r.json();
  check('未知观测勘误 -> 404 sample_not_found',
    r.status === 404 && body.error.code === 'sample_not_found', `status=${r.status}`);
  r = await revise(S('no-such-stream'), 'm1', {
    expectedRevision: 1, ts: '2026-10-01T00:00:00Z', value: 1,
  });
  check('未知流勘误 -> 404', r.status === 404, `status=${r.status}`);
  r = await revise(revStream, 'm1', { ts: '2026-10-01T00:00:00Z', value: 1 });
  check('缺 expectedRevision -> 400', r.status === 400, `status=${r.status}`);
  r = await revise(revStream, 'm1', {
    expectedRevision: 3, ts: 'not-a-time', value: 1,
  });
  check('非法时刻 -> 400', r.status === 400, `status=${r.status}`);
  r = await fetch(`${baseUrl}/api/streams/${revStream}/samples/m1/revisions`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: '{oops',
  });
  check('勘误坏 JSON -> 400', r.status === 400, `status=${r.status}`);

  // 13) 重启后游标继续（仅本地模式可以真的重启进程）
  if (restartFlow) {
    const persistStream = S('persist');
    await post(persistStream, Array.from({ length: 5 }, (_, i) => ({
      sampleId: `p${i}`, ts: `2026-06-01T00:00:0${i}Z`, value: i,
    })));
    r = await get(`/api/streams/${persistStream}/samples?pageSize=2`);
    page = await r.json();
    const heldCursor = page.nextCursor;
    const heldSeq = page.snapshotSeq;
    check('重启前取到游标', typeof heldCursor === 'string');

    const newBase = await restartFlow();
    baseUrl = newBase; // eslint-disable-line no-param-reassign

    r = await fetch(`${newBase}/health`);
    check('重启后健康检查 200', r.status === 200, `status=${r.status}`);
    r = await fetch(`${newBase}/api/streams/${persistStream}/samples?pageSize=2` +
      `&cursor=${encodeURIComponent(heldCursor)}`);
    check('重启后旧游标可用 -> 200', r.status === 200, `status=${r.status}`);
    page = await r.json();
    check('重启后接着遍历 p2,p3',
      JSON.stringify(page.items.map((i) => i.sampleId)) === JSON.stringify(['p2', 'p3']),
      JSON.stringify(page.items));
    check('重启后快照序号不变', page.snapshotSeq === heldSeq,
      `${page.snapshotSeq} != ${heldSeq}`);
    r = await fetch(`${newBase}/api/streams/${persistStream}/samples?pageSize=2` +
      `&cursor=${encodeURIComponent(page.nextCursor)}`);
    page = await r.json();
    check('重启后翻到末页并结束',
      JSON.stringify(page.items.map((i) => i.sampleId)) === JSON.stringify(['p4']) &&
      page.done === true);
  }
}

function startLocalServer(dbPath) {
  const child = spawn(process.execPath, ['src/server.js'], {
    env: {
      ...process.env,
      PORT: '0',
      HOST: '127.0.0.1',
      DB_PATH: dbPath,
      CURSOR_SECRET: 'smoke-fixed-secret',
    },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  return new Promise((resolve, reject) => {
    child.stdout.on('data', (d) => {
      const m = String(d).match(/listening on http:\/\/[^:]+:(\d+)/);
      if (m) resolve({ child, port: Number(m[1]) });
    });
    child.on('exit', (code) => reject(new Error(`服务提前退出 code=${code}`)));
  });
}

async function main() {
  const local = process.argv.includes('--local');
  let baseUrl = process.env.BASE_URL;
  let cleanup = async () => {};

  if (local || !baseUrl) {
    const dir = mkdtempSync(join(tmpdir(), 'obs-smoke-'));
    const dbPath = join(dir, 'smoke.db');
    const secret = 'smoke-fixed-secret';
    let running = await startLocalServer(dbPath);
    baseUrl = `http://127.0.0.1:${running.port}`;

    const restartFlow = async () => {
      await new Promise((res) => {
        running.child.on('exit', res);
        running.child.kill('SIGTERM');
      });
      running = await startLocalServer(dbPath);
      return `http://127.0.0.1:${running.port}`;
    };
    cleanup = async () => {
      running.child.kill('SIGTERM');
      await new Promise((r) => setTimeout(r, 200));
      rmSync(dir, { recursive: true, force: true });
    };

    try {
      await runSmoke(baseUrl, { restartFlow });
    } finally {
      await cleanup();
    }
  } else {
    await runSmoke(baseUrl, { restartFlow: null });
  }

  console.log(logs.join('\n'));
  if (failures > 0) {
    console.error(`\n冒烟结果: 失败 ${failures} 项`);
    process.exit(1);
  }
  console.log('\n冒烟结果: 全部通过');
}

main().catch((err) => {
  console.error('冒烟执行异常:', err);
  process.exit(1);
});
