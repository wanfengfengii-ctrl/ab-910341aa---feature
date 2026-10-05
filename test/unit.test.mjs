import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Store, BatchConflictError, SampleNotFoundError, RevisionConflictError } from '../src/db.js';
import { issueCursor, parseCursor, bindCursor, CursorError } from '../src/cursor.js';
import { validateBatch, validateQuery, validateRevision, ValidationError } from '../src/validation.js';

const R = (ts, sid, v = 1) => ({ sampleId: sid, ts, value: v });

test('Store: 批次写入分配连续接收序号', () => {
  const s = new Store(':memory:');
  assert.equal(s.currentSeq(), 0);
  const r1 = s.insertBatch('st', [R('2026-01-01T00:00:00Z', 'a'), R('2026-01-01T00:00:01Z', 'b')]);
  assert.deepEqual([r1.firstSeq, r1.lastSeq, r1.count], [1, 2, 2]);
  const r2 = s.insertBatch('st', [R('2026-01-01T00:00:02Z', 'c')]);
  assert.deepEqual([r2.firstSeq, r2.lastSeq], [3, 3]);
  assert.equal(s.currentSeq(), 3);
  s.close();
});

test('Store: 批内重复 -> 整批拒绝且无部分写入', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [R('2026-01-01T00:00:00Z', 'a')]);
  assert.throws(
    () => s.insertBatch('st', [
      R('2026-01-01T00:00:05Z', 'x'),
      R('2026-01-01T00:00:06Z', 'x'),
    ]),
    (e) => e instanceof BatchConflictError && e.details.reason === 'duplicate_in_batch'
  );
  assert.equal(s.totalSamples(), 1);
  assert.equal(s.currentSeq(), 1, '冲突不得占用序号');
  s.close();
});

test('Store: 与已有编号冲突 -> 整批回滚', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:00Z', 'a'),
    R('2026-01-01T00:00:01Z', 'b'),
  ]);
  assert.throws(
    () => s.insertBatch('st', [
      R('2026-01-02T00:00:00Z', 'c'),
      R('2026-01-02T00:00:01Z', 'a'), // 与已有冲突
      R('2026-01-02T00:00:02Z', 'd'),
    ]),
    (e) => e instanceof BatchConflictError && e.details.reason === 'conflict_existing'
  );
  assert.equal(s.totalSamples(), 2);
  assert.equal(s.currentSeq(), 2);
  // c/d 都不得留下
  assert.equal(s.readPage('st', s.currentSeq(), { fromTs: '0', toTs: 'z' }, null, 100).length, 2);
  s.close();
});

test('Store: 同一样本能在不同流中各自存在', () => {
  const s = new Store(':memory:');
  s.insertBatch('s1', [R('2026-01-01T00:00:00Z', 'a')]);
  s.insertBatch('s2', [R('2026-01-01T00:00:00Z', 'a')]); // 不应冲突
  assert.equal(s.totalSamples(), 2);
  s.close();
});

test('Store: 快照按 (ts, sampleId) 稳定排序且快照上界隔离新写入', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:02Z', 'c'),
    R('2026-01-01T00:00:01Z', 'b'),
    R('2026-01-01T00:00:01Z', 'a'), // 与 b 同时刻 -> sampleId 次序
  ]);
  const snap = s.currentSeq(); // 3

  // 快照固定后插入“更早时刻”的观测
  s.insertBatch('st', [
    R('2025-12-31T23:59:59Z', 'early'),
    R('2026-01-01T00:00:03Z', 'late'),
  ]);

  const all = [];
  let after = null;
  for (;;) {
    const page = s.readPage('st', snap, { fromTs: '0', toTs: 'z' }, after, 2);
    all.push(...page);
    if (page.length < 2) break;
    const last = page[page.length - 1];
    after = { ts: last.ts, sampleId: last.sampleId };
  }
  assert.deepEqual(all.map((x) => x.sampleId), ['a', 'b', 'c']);
  assert.equal(all.length, 3, '快照外的两条（更早/更晚）均不得出现');

  // 新快照包含全部 5 条，且 early 排在最前
  const fresh = s.readPage('st', s.currentSeq(), { fromTs: '0', toTs: 'z' }, null, 100);
  assert.deepEqual(fresh.map((x) => x.sampleId), ['early', 'a', 'b', 'c', 'late']);
  s.close();
});

test('Store: 时间范围过滤（闭区间）', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:00Z', 'a'),
    R('2026-01-01T00:00:05Z', 'b'),
    R('2026-01-01T00:00:10Z', 'c'),
  ]);
  const seq = s.currentSeq();
  const rows = s.readPage(
    'st', seq,
    { fromTs: '2026-01-01T00:00:05Z', toTs: '2026-01-01T00:00:10Z' },
    null, 100
  );
  assert.deepEqual(rows.map((r) => r.sampleId), ['b', 'c']);
  s.close();
});

test('游标: 签发/解析往返', () => {
  const secret = 'test-secret';
  const payload = {
    v: 1, s: 'st1', q: 42,
    f: '2026-01-01T00:00:00Z', t: '9999-12-31T23:59:59.999999999Z',
    p: 10, a: { ts: '2026-01-01T00:00:01Z', i: 'x' },
  };
  const c = issueCursor(secret, payload);
  const got = parseCursor(secret, c);
  assert.deepEqual(got, payload);
  assert.equal(typeof c, 'string');
});

test('游标: 篡改载荷或签名 -> 明确错误', () => {
  const c = issueCursor('sec', {
    v: 1, s: 'st', q: 1, f: 'a', t: 'b', p: 10, a: null,
  });
  const [body, sig] = c.split('.');
  assert.throws(() => parseCursor('sec', body + '.' + sig.replace(/./, 'X')),
    (e) => e instanceof CursorError && e.code === 'cursor_signature_invalid');
  // 用另一个密钥也无法通过
  assert.throws(() => parseCursor('other', c),
    (e) => e instanceof CursorError && e.code === 'cursor_signature_invalid');
  assert.throws(() => parseCursor('sec', 'garbage'),
    (e) => e instanceof CursorError && e.code === 'invalid_cursor');
  assert.throws(() => parseCursor('sec', ''),
    (e) => e instanceof CursorError && e.code === 'invalid_cursor');
});

test('游标: 跨流复用 / 改变时间范围 -> 明确的客户端错误', () => {
  const p = {
    v: 1, s: 'stream-A', q: 9,
    f: '2026-01-01T00:00:00Z', t: '9999-12-31T23:59:59.999999999Z',
    p: 25, a: null,
  };
  assert.throws(
    () => bindCursor(p, { streamId: 'stream-B', fromTs: null, toTs: null, pageSize: null }),
    (e) => e instanceof CursorError && e.code === 'cursor_stream_mismatch'
  );
  assert.throws(
    () => bindCursor(p, {
      streamId: 'stream-A',
      fromTs: '2025-01-01T00:00:00Z', toTs: null, pageSize: null,
    }),
    (e) => e instanceof CursorError && e.code === 'cursor_range_mismatch'
  );
  assert.throws(
    () => bindCursor(p, {
      streamId: 'stream-A',
      fromTs: null, to: null, toTs: '2027-01-01T00:00:00Z', pageSize: null,
    }),
    (e) => e instanceof CursorError && e.code === 'cursor_range_mismatch'
  );
  // 合法绑定
  const bound = bindCursor(p, { streamId: 'stream-A', fromTs: null, toTs: null, pageSize: null });
  assert.equal(bound.snapshotSeq, 9);
});

test('校验: 批次大小 1..100', () => {
  assert.throws(() => validateBatch({ samples: [] }), ValidationError);
  assert.throws(() => validateBatch({ samples: 'no' }), ValidationError);
  const ok = validateBatch({ samples: Array.from({ length: 100 }, (_, i) =>
    R('2026-01-01T00:00:00Z', `s${i}`)) });
  assert.equal(ok.items.length, 100);
  assert.throws(() => validateBatch({ samples: Array.from({ length: 101 }, (_, i) =>
    R('2026-01-01T00:00:00Z', `s${i}`)) }), ValidationError);
});

test('校验: sampleId / ts / value 规则', () => {
  assert.throws(() => validateBatch({ samples: [{ ts: '2026-01-01T00:00:00Z', value: 1 }] }),
    ValidationError);
  assert.throws(() => validateBatch({ samples: [R('not-a-time', 'a')] }), ValidationError);
  assert.throws(() => validateBatch({ samples: [R('2026-01-01 00:00:00', 'a')] }),
    ValidationError, '缺时区偏移应拒绝');
  assert.throws(() => validateBatch({ samples: [
    { sampleId: 'a', ts: '2026-01-01T00:00:00Z', value: 1.5 }] }), ValidationError);
  assert.throws(() => validateBatch({ samples: [
    { sampleId: 'a', ts: '2026-01-01T00:00:00Z', value: '1' }] }), ValidationError);
});

test('校验: 带偏移时间归一化为 UTC 字符串（字典序可比较）', () => {
  const { items } = validateBatch({ samples: [
    { sampleId: 'a', ts: '2026-01-01T08:00:00+08:00', value: -5 },
  ] });
  assert.equal(items[0].ts, '2026-01-01T00:00:00.000000000Z');
  assert.equal(items[0].value, -5);
});

test('校验: 小数秒补齐 9 位，字典序与时间序一致', () => {
  const { items } = validateBatch({ samples: [
    { sampleId: 'a', ts: '2026-01-01T00:00:00.1Z', value: 1 },
    { sampleId: 'b', ts: '2026-01-01T00:00:00.09Z', value: 2 },
    { sampleId: 'c', ts: '2026-01-01T00:00:00.19Z', value: 3 },
  ] });
  const ts = Object.fromEntries(items.map((i) => [i.sampleId, i.ts]));
  assert.equal(ts.a, '2026-01-01T00:00:00.100000000Z');
  assert.equal(ts.b, '2026-01-01T00:00:00.090000000Z');
  assert.equal(ts.c, '2026-01-01T00:00:00.190000000Z');
  assert.ok(ts.b < ts.a && ts.a < ts.c, '0.09 < 0.1 < 0.19');
});

test('校验: 查询参数 pageSize / 时间范围', () => {
  const q = validateQuery(new URLSearchParams('pageSize=50&from=2026-01-01T00:00:00Z'));
  assert.equal(q.pageSize, 50);
  assert.equal(q.fromTs, '2026-01-01T00:00:00.000000000Z');
  assert.throws(() => validateQuery(new URLSearchParams('pageSize=0')), ValidationError);
  assert.throws(() => validateQuery(new URLSearchParams('pageSize=abc')), ValidationError);
  assert.throws(
    () => validateQuery(new URLSearchParams(
      'from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z')),
    ValidationError
  );
});

test('校验: 勘误请求体 expectedRevision/ts/value', () => {
  const ok = validateRevision({
    expectedRevision: 1, ts: '2026-01-01T08:00:00+08:00', value: -9,
  });
  assert.deepEqual(ok, {
    expectedRevision: 1,
    ts: '2026-01-01T00:00:00.000000000Z',
    value: -9,
  });
  assert.throws(() => validateRevision(null), ValidationError);
  assert.throws(() => validateRevision({ ts: '2026-01-01T00:00:00Z', value: 1 }),
    ValidationError, '缺 expectedRevision');
  assert.throws(() => validateRevision({ expectedRevision: 0, ts: '2026-01-01T00:00:00Z', value: 1 }),
    ValidationError, '修订号从 1 起');
  assert.throws(() => validateRevision({ expectedRevision: '1', ts: '2026-01-01T00:00:00Z', value: 1 }),
    ValidationError, '修订号必须是整数');
  assert.throws(() => validateRevision({ expectedRevision: 1, ts: 'bad', value: 1 }),
    ValidationError, '时刻必须 RFC3339');
  assert.throws(() => validateRevision({ expectedRevision: 1, ts: '2026-01-01T00:00:00Z', value: 2.5 }),
    ValidationError, '读数必须整数');
});

test('Store: 勘误创建下一版并分配 effective_seq，原始行为第 1 版', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [R('2026-01-01T00:00:00Z', 'a')]);
  assert.equal(s.currentRevision('st', 'a'), 1);
  const r = s.reviseSample('st', 'a', {
    ts: '2026-01-01T00:00:30Z', value: 42, expectedRevision: 1,
  });
  assert.deepEqual(r, { revision: 2, effectiveSeq: 2 });
  assert.equal(s.currentSeq(), 2, '勘误占用下一个接收序号');
  assert.equal(s.currentRevision('st', 'a'), 2);

  const r2 = s.reviseSample('st', 'a', {
    ts: '2026-01-02T00:00:00Z', value: 7, expectedRevision: 2,
  });
  assert.deepEqual(r2, { revision: 3, effectiveSeq: 3 });
  assert.equal(s.totalSamples(), 1, '勘误不新增观测行');
  s.close();
});

test('Store: 未知观测勘误 -> 404 且不占用序号', () => {
  const s = new Store(':memory:');
  assert.throws(
    () => s.reviseSample('st', 'ghost', {
      ts: '2026-01-01T00:00:00Z', value: 1, expectedRevision: 1,
    }),
    (e) => e instanceof SampleNotFoundError && e.details.reason === 'sample_not_found'
  );
  assert.equal(s.currentSeq(), 0);
  assert.equal(s.currentRevision('st', 'ghost'), null);
  // 流隔离：别的流有同样编号也不算存在
  s.insertBatch('other', [R('2026-01-01T00:00:00Z', 'a')]);
  assert.throws(
    () => s.reviseSample('st', 'a', {
      ts: '2026-01-01T00:00:00Z', value: 1, expectedRevision: 1,
    }),
    SampleNotFoundError
  );
  s.close();
});

test('Store: 过期修订号 -> 409 回送当前修订号且不改写数据', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [R('2026-01-01T00:00:00Z', 'a')]);
  s.reviseSample('st', 'a', {
    ts: '2026-01-01T00:00:09Z', value: 9, expectedRevision: 1,
  });
  assert.throws(
    () => s.reviseSample('st', 'a', {
      ts: '2026-01-01T00:00:08Z', value: 8, expectedRevision: 1,
    }),
    (e) =>
      e instanceof RevisionConflictError &&
      e.details.reason === 'revision_conflict' &&
      e.details.currentRevision === 2 &&
      e.details.expectedRevision === 1
  );
  assert.equal(s.currentRevision('st', 'a'), 2);
  assert.equal(s.currentSeq(), 2, '冲突不占用序号');
  const fresh = s.readPage('st', s.currentSeq(), { fromTs: '0', toTs: 'z' }, null, 10);
  assert.equal(fresh[0].value, 9, '失败的勘误不得改写数据');
  s.close();
});

test('Store: 勘误的快照隔离 —— 旧游标只见旧版且恰好一次，新会话只见新版', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:01Z', 'a', 1),
    R('2026-01-01T00:00:02Z', 'b', 2),
    R('2026-01-01T00:00:03Z', 'c', 3),
  ]);
  const snapBefore = s.currentSeq(); // 3

  // 勘误 a：修正读数 + 时刻前移到最早
  const rev = s.reviseSample('st', 'a', {
    ts: '2026-01-01T00:00:00Z', value: 100, expectedRevision: 1,
  });
  assert.equal(rev.effectiveSeq, 4);

  // 旧快照（勘误前取得的游标）：仍见旧版（旧时刻、旧读数），每项恰好一次
  const oldPage = s.readPage('st', snapBefore, { fromTs: '0', toTs: 'z' }, null, 100);
  assert.deepEqual(oldPage.map((r) => r.sampleId), ['a', 'b', 'c']);
  assert.deepEqual(oldPage.map((r) => [r.ts, r.value, r.revision]), [
    ['2026-01-01T00:00:01Z', 1, 1],
    ['2026-01-01T00:00:02Z', 2, 1],
    ['2026-01-01T00:00:03Z', 3, 1],
  ]);

  // 新快照：只见新版；a 因时刻前移而排到最前，且不重复出现
  const newPage = s.readPage('st', s.currentSeq(), { fromTs: '0', toTs: 'z' }, null, 100);
  assert.deepEqual(newPage.map((r) => r.sampleId), ['a', 'b', 'c']);
  assert.equal(newPage[0].value, 100);
  assert.equal(newPage[0].revision, 2);
  assert.equal(newPage[0].ts, '2026-01-01T00:00:00Z');
  s.close();
});

test('Store: 勘误时刻后移，旧/新快照各自稳定且不重不漏（含 keyset 翻页）', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:01Z', 'a', 1),
    R('2026-01-01T00:00:02Z', 'b', 2),
    R('2026-01-01T00:00:03Z', 'c', 3),
  ]);
  const snapBefore = s.currentSeq();
  // 把 a 勘误到最晚时刻
  s.reviseSample('st', 'a', {
    ts: '2026-01-01T00:00:09Z', value: 11, expectedRevision: 1,
  });

  function drain(snap) {
    const all = [];
    let after = null;
    for (;;) {
      const page = s.readPage('st', snap, { fromTs: '0', toTs: 'z' }, after, 2);
      all.push(...page);
      if (page.length < 2) break;
      const last = page[page.length - 1];
      after = { ts: last.ts, sampleId: last.sampleId };
    }
    return all;
  }

  const oldAll = drain(snapBefore);
  assert.deepEqual(oldAll.map((r) => r.sampleId), ['a', 'b', 'c']);
  assert.deepEqual(oldAll.map((r) => r.value), [1, 2, 3]);

  const newAll = drain(s.currentSeq());
  assert.deepEqual(newAll.map((r) => r.sampleId), ['b', 'c', 'a'],
    '新版按新时刻重新参与稳定排序');
  assert.deepEqual(newAll.map((r) => r.value), [2, 3, 11]);
  assert.deepEqual(newAll.map((r) => r.revision), [1, 1, 2]);
  s.close();
});

test('Store: 勘误与时间范围——旧快照按旧时刻过滤，新快照按新时刻过滤', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:00Z', 'a'),
    R('2026-01-03T00:00:00Z', 'b'),
  ]);
  const snapBefore = s.currentSeq();
  // a 从 1/1 勘误到 1/5（移出 1/1..1/3 范围）
  s.reviseSample('st', 'a', {
    ts: '2026-01-05T00:00:00Z', value: 5, expectedRevision: 1,
  });
  const range = { fromTs: '2026-01-01T00:00:00.000000000Z', toTs: '2026-01-03T23:59:59.999999999Z' };
  assert.deepEqual(
    s.readPage('st', snapBefore, range, null, 100).map((r) => r.sampleId),
    ['a', 'b'], '旧会话仍按旧时刻看到 a'
  );
  assert.deepEqual(
    s.readPage('st', s.currentSeq(), range, null, 100).map((r) => r.sampleId),
    ['b'], '新会话按新时刻，a 已移出范围'
  );
  s.close();
});
