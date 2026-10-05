import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { Store, BatchConflictError, RevisionConflictError } from '../src/db.js';
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

test('Store: 勘误追加版本；旧快照看旧版、新快照看新版并按新时刻重排', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:01Z', 'a', 1),
    R('2026-01-01T00:00:02Z', 'b', 2),
    R('2026-01-01T00:00:03Z', 'c', 3),
  ]);
  const oldSnap = s.currentSeq(); // 3

  // 把 b 从 00:00:02 勘误到 00:00:04，并改读数
  const rv = s.reviseSample(
    'st', 'b', 1, '2026-01-01T00:00:04.000000000Z', 22
  );
  assert.deepEqual(rv, { revision: 2, effectiveSeq: 4 });

  // 旧快照：b 仍是旧版（旧时刻/旧读数），页序不变
  const oldPage = s.readPage('st', oldSnap, { fromTs: '0', toTs: 'z' }, null, 100);
  assert.deepEqual(oldPage.map((r) => [r.sampleId, r.ts, r.value]), [
    ['a', '2026-01-01T00:00:01Z', 1],
    ['b', '2026-01-01T00:00:02Z', 2],
    ['c', '2026-01-01T00:00:03Z', 3],
  ]);

  // 新快照：b 显示新版，并移动到末尾（新时刻 00:00:04），仍只出现一次
  const newPage = s.readPage('st', s.currentSeq(), { fromTs: '0', toTs: 'z' }, null, 100);
  assert.deepEqual(newPage.map((r) => [r.sampleId, r.ts, r.value]), [
    ['a', '2026-01-01T00:00:01Z', 1],
    ['c', '2026-01-01T00:00:03Z', 3],
    ['b', '2026-01-01T00:00:04.000000000Z', 22],
  ]);
  assert.equal(s.totalSamples(), 3, '逻辑观测数不随勘误增长');
  s.close();
});

test('Store: 勘误把时刻前移也只影响新快照，且每个观测恰好一次', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:01Z', 'a'),
    R('2026-01-01T00:00:02Z', 'b'),
  ]);
  const oldSnap = s.currentSeq();
  s.reviseSample('st', 'a', 1, '2025-12-31T23:59:59.000000000Z', 99);

  const oldPage = s.readPage('st', oldSnap, { fromTs: '0', toTs: 'z' }, null, 100);
  assert.deepEqual(oldPage.map((r) => r.sampleId), ['a', 'b']);
  assert.equal(oldPage[0].value, 1);

  const newPage = s.readPage('st', s.currentSeq(), { fromTs: '0', toTs: 'z' }, null, 100);
  assert.deepEqual(newPage.map((r) => r.sampleId), ['a', 'b']);
  assert.equal(newPage[0].value, 99);
  assert.equal(newPage.length, 2, '同一观测不得出现两次');
  s.close();
});

test('Store: 勘误乐观锁——未知观测 null；过期修订号 409 且不改写', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [R('2026-01-01T00:00:00Z', 'a')]);

  assert.equal(s.reviseSample('st', 'nope', 1, '2026-01-01T00:01:00Z', 1), null);
  assert.equal(s.reviseSample('st2', 'a', 1, '2026-01-01T00:01:00Z', 1),
    null, '其他流的同号观测不可见');

  // 期望修订号 1 首次成功
  const rv = s.reviseSample('st', 'a', 1, '2026-01-01T00:01:00.000000000Z', 11);
  assert.equal(rv.revision, 2);

  // 再次用过期的 1 -> 冲突并带当前修订号
  assert.throws(
    () => s.reviseSample('st', 'a', 1, '2026-01-01T00:02:00.000000000Z', 12),
    (e) =>
      e instanceof RevisionConflictError &&
      e.details.currentRevision === 2 &&
      e.details.expectedRevision === 1
  );
  // 用当前修订号 2 -> 第 3 版
  const rv3 = s.reviseSample('st', 'a', 2, '2026-01-01T00:03:00.000000000Z', 13);
  assert.equal(rv3.revision, 3);
  assert.equal(rv3.effectiveSeq, 3);

  // 最新快照只显示第 3 版，恰好一次
  const page = s.readPage('st', s.currentSeq(), { fromTs: '0', toTs: 'z' }, null, 100);
  assert.equal(page.length, 1);
  assert.equal(page[0].value, 13);
  assert.equal(s.currentSeq(), 3, '失败的勘误不占用序号');
  s.close();
});

test('Store: 勘误中途修改旧快照 keyset 位置，旧游标仍恰好遍历一次', () => {
  const s = new Store(':memory:');
  s.insertBatch('st', [
    R('2026-01-01T00:00:01Z', 'a'),
    R('2026-01-01T00:00:02Z', 'b'),
    R('2026-01-01T00:00:03Z', 'c'),
    R('2026-01-01T00:00:04Z', 'd'),
  ]);
  const snap = s.currentSeq(); // 4
  // 首页取 a,b（pageSize=2），游标停在 b 之后
  const first = s.readPage('st', snap, { fromTs: '0', toTs: 'z' }, null, 2);
  assert.deepEqual(first.map((r) => r.sampleId), ['a', 'b']);

  // 分页中途勘误：a 移到最末时刻、c 移到最早时刻
  s.reviseSample('st', 'a', 1, '2026-01-01T00:00:09.000000000Z', 10);
  s.reviseSample('st', 'c', 1, '2025-12-31T23:59:59.000000000Z', 30);

  // 用旧快照游标继续：after=(旧 b 位置)，必须仍按旧版页序返回 c,d 且不漏不重
  const rest = s.readPage(
    'st', snap, { fromTs: '0', toTs: 'z' },
    { ts: first[1].ts, sampleId: first[1].sampleId }, 100
  );
  assert.deepEqual(rest.map((r) => r.sampleId), ['c', 'd']);
  assert.equal(rest[0].value, 1, 'c 仍为旧读数');
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

test('校验: 勘误请求体 expectedRevision / ts / value', () => {
  const ok = validateRevision({
    expectedRevision: 2, ts: '2026-01-01T08:00:00+08:00', value: -3,
  });
  assert.deepEqual(ok, {
    expectedRevision: 2,
    ts: '2026-01-01T00:00:00.000000000Z',
    value: -3,
  });
  assert.throws(() => validateRevision(null), ValidationError);
  assert.throws(() => validateRevision({ ts: '2026-01-01T00:00:00Z', value: 1 }),
    ValidationError, '缺 expectedRevision');
  assert.throws(() => validateRevision({ expectedRevision: 0, ts: 'x', value: 1 }),
    ValidationError);
  assert.throws(() => validateRevision({ expectedRevision: -1, ts: 'x', value: 1 }),
    ValidationError);
  assert.throws(() => validateRevision({ expectedRevision: 1.5, ts: 'x', value: 1 }),
    ValidationError);
  assert.throws(
    () => validateRevision({ expectedRevision: '1', ts: '2026-01-01T00:00:00Z', value: 1 }),
    ValidationError, 'expectedRevision 不得为字符串');
  assert.throws(
    () => validateRevision({ expectedRevision: 1, ts: '2026-01-01T00:00:00Z', value: 1.5 }),
    ValidationError);
  assert.throws(
    () => validateRevision({ expectedRevision: 1, ts: 'not-a-time', value: 1 }),
    ValidationError);
});

test('校验: 查询参数 pageSize / 时间范围', () => {  const q = validateQuery(new URLSearchParams('pageSize=50&from=2026-01-01T00:00:00Z'));
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
