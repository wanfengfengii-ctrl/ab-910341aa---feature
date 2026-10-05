import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

/**
 * 持久化层（node:sqlite，Node 22 内置，同步 API；WAL 模式，崩溃后不损坏）。
 *
 * 数据模型：
 *  - samples(stream_id, sample_id, ts, value, seq)
 *      seq 为全局单调递增的接收序号（写入即分配），(stream_id, sample_id) 唯一。
 *      该行即观测的“第 1 版”（原始观测），seq 同时是第 1 版的生效序号。
 *  - sample_revisions(stream_id, sample_id, revision, ts, value, effective_seq)
 *      质控勘误产生的第 2..N 版；revision 从 2 起单调递增，effective_seq 为该版
 *      生效时分配的接收序号。快照上界 Q 只可见 effective_seq <= Q 的最新版本，
 *      故勘误前取得的分页游标始终看到旧版、勘误后开启的会话才看到新版。
 *  - seq_meta：单行，保存已分配的最后一个接收序号（= 快照上界来源）。
 */
export class Store {
  constructor(dbPath) {
    if (dbPath !== ':memory:') mkdirSync(dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode=WAL');
    this.db.exec('PRAGMA foreign_keys=ON');
    this.db.exec('PRAGMA busy_timeout=5000');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS samples (
        stream_id TEXT    NOT NULL,
        sample_id TEXT    NOT NULL,
        ts        TEXT    NOT NULL,   -- RFC3339，字典序即时间序（要求 UTC / 'Z'）
        value     INTEGER NOT NULL,
        seq       INTEGER NOT NULL,
        PRIMARY KEY (stream_id, sample_id)
      ) WITHOUT ROWID
    `);
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_samples_stream_ts
        ON samples (stream_id, ts, sample_id)
    `);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS seq_meta (
        id     INTEGER PRIMARY KEY CHECK (id = 1),
        last_seq INTEGER NOT NULL
      )
    `);
    this.db.exec(`
      INSERT OR IGNORE INTO seq_meta (id, last_seq) VALUES (1, 0)
    `);
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS sample_revisions (
        stream_id     TEXT    NOT NULL,
        sample_id     TEXT    NOT NULL,
        revision      INTEGER NOT NULL,   -- 原始观测为第 1 版，勘误从 2 起
        ts            TEXT    NOT NULL,   -- 该版时刻（RFC3339 UTC 规范串）
        value         INTEGER NOT NULL,
        effective_seq INTEGER NOT NULL,   -- 该版生效时的接收序号
        PRIMARY KEY (stream_id, sample_id, revision)
      ) WITHOUT ROWID
    `);
    this.#prepareStatements();
  }

  #prepareStatements() {
    const db = this.db;
    // 快照可见版本：第 1 版（samples 行，生效序号 = seq）与各勘误版中，
    // effective_seq <= 快照上界的最高 revision。勘误版不改变第 1 版的行，
    // 因此旧快照（游标）永远看到旧版，新快照才看到新版。
    // 流过滤下推到 UNION ALL 两支，窗口函数只在单流内分区取最高修订版。
    const visibleCte = `
      WITH visible AS (
        SELECT
          s.stream_id,
          s.sample_id,
          s.ts,
          s.value,
          s.seq,
          1 AS revision,
          s.seq AS effective_seq,
          s.seq AS visible_seq
        FROM samples s
        WHERE s.stream_id = ?
        UNION ALL
        SELECT
          r.stream_id,
          r.sample_id,
          r.ts,
          r.value,
          s.seq,
          r.revision,
          r.effective_seq,
          r.effective_seq AS visible_seq
        FROM sample_revisions r
        JOIN samples s
          ON s.stream_id = r.stream_id AND s.sample_id = r.sample_id
        WHERE r.stream_id = ?
          AND r.effective_seq <= ?
      ),
      chosen AS (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY stream_id, sample_id ORDER BY revision DESC
          ) AS rn
        FROM visible
      )`;
    this.stmts = {
      getLastSeq: db.prepare('SELECT last_seq FROM seq_meta WHERE id = 1'),
      bumpSeq: db.prepare('UPDATE seq_meta SET last_seq = ? WHERE id = 1'),
      existsDup: db.prepare(
        'SELECT 1 FROM samples WHERE stream_id = ? AND sample_id = ?'
      ),
      insert: db.prepare(
        'INSERT INTO samples (stream_id, sample_id, ts, value, seq) VALUES (?, ?, ?, ?, ?)'
      ),
      pageStart: db.prepare(
        `${visibleCte}
         SELECT sample_id AS sampleId, ts, value, seq, revision, effective_seq AS effectiveSeq
           FROM chosen
          WHERE rn = 1
            AND visible_seq <= ?
            AND ts >= ? AND ts <= ?
          ORDER BY ts ASC, sample_id ASC
          LIMIT ?`
      ),
      pageAfter: db.prepare(
        `${visibleCte}
         SELECT sample_id AS sampleId, ts, value, seq, revision, effective_seq AS effectiveSeq
           FROM chosen
          WHERE rn = 1
            AND visible_seq <= ?
            AND ts >= ? AND ts <= ?
            AND (ts > ? OR (ts = ? AND sample_id > ?))
          ORDER BY ts ASC, sample_id ASC
          LIMIT ?`
      ),
      countSnapshot: db.prepare(
        `${visibleCte}
         SELECT COUNT(*) AS n FROM chosen
          WHERE rn = 1
            AND visible_seq <= ?
            AND ts >= ? AND ts <= ?`
      ),
      totalSamples: db.prepare('SELECT COUNT(*) AS n FROM samples'),
      // ---- 勘误（乐观并发）----
      getSampleForRevision: db.prepare(
        `SELECT s.seq,
                COALESCE(MAX(r.revision), 1) AS currentRevision
           FROM samples s
           LEFT JOIN sample_revisions r
             ON r.stream_id = s.stream_id AND r.sample_id = s.sample_id
          WHERE s.stream_id = ? AND s.sample_id = ?
          GROUP BY s.stream_id, s.sample_id`
      ),
      insertRevision: db.prepare(
        `INSERT INTO sample_revisions
           (stream_id, sample_id, revision, ts, value, effective_seq)
         VALUES (?, ?, ?, ?, ?, ?)`
      ),
    };
  }

  /** 当前已分配的最后接收序号（同时也是“最新快照”的上界）。 */
  currentSeq() {
    return Number(this.stmts.getLastSeq.get().last_seq);
  }

  /**
   * 原子批量写入（1..100 条）。
   * 批内重复、或与已存在的 sampleId 冲突 -> 抛 BatchConflictError，整批回滚，不留部分写入。
   * 返回 { firstSeq, lastSeq }。
   */
  insertBatch(streamId, items) {
    const db = this.db;
    const insert = this.stmts.insert;
    const existsDup = this.stmts.existsDup;
    const bumpSeq = this.stmts.bumpSeq;

    db.exec('BEGIN IMMEDIATE');
    try {
      let seq = Number(this.stmts.getLastSeq.get().last_seq);
      const firstSeq = seq + 1;

      // 批内唯一性预检（尽早失败，且避免唯一约束报错时占用 seq 号段）
      const seen = new Set();
      for (const it of items) {
        if (seen.has(it.sampleId)) {
          throw new BatchConflictError(
            `批内 sampleId 重复: ${it.sampleId}`,
            { reason: 'duplicate_in_batch', sampleId: it.sampleId }
          );
        }
        seen.add(it.sampleId);
      }

      for (const it of items) {
        if (existsDup.get(streamId, it.sampleId)) {
          throw new BatchConflictError(
            `sampleId 已存在: ${it.sampleId}`,
            { reason: 'conflict_existing', sampleId: it.sampleId }
          );
        }
        seq += 1;
        insert.run(streamId, it.sampleId, it.ts, it.value, seq);
      }
      bumpSeq.run(seq);
      db.exec('COMMIT');
      return { firstSeq, lastSeq: seq, count: items.length };
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }
  }

  /**
   * 读取快照页（稳定排序：ts ASC, sampleId ASC）。
   * @param {string} streamId
   * @param {number} snapshotSeq 快照上界（只含 effective_seq <= snapshotSeq 的版本）
   * @param {{fromTs:string,toTs:string}} range 闭区间，按 ts 字符串比较
   * @param {{ts:string,sampleId:string}|null} after 严格大于该位置之后开始
   * @param {number} pageSize
   */
  readPage(streamId, snapshotSeq, range, after, pageSize) {
    // keyset 分页：比较 (ts, sample_id) 元组，稳定且不依赖 OFFSET。
    // 绑定顺序：CTE 两支各绑定流（勘误支再绑快照上界），外层绑定可见性/范围/页大小。
    let rows;
    if (after) {
      rows = this.stmts.pageAfter.all(
        streamId, streamId, snapshotSeq,
        snapshotSeq, range.fromTs, range.toTs,
        after.ts, after.ts, after.sampleId, pageSize
      );
    } else {
      rows = this.stmts.pageStart.all(
        streamId, streamId, snapshotSeq,
        snapshotSeq, range.fromTs, range.toTs, pageSize
      );
    }
    return rows.map((r) => ({
      ts: r.ts,
      sampleId: r.sampleId,
      value: Number(r.value),
      seq: Number(r.seq),
      revision: Number(r.revision),
      effectiveSeq: Number(r.effectiveSeq),
    }));
  }

  countSnapshot(streamId, snapshotSeq, range) {
    return Number(
      this.stmts.countSnapshot.get(
        streamId, streamId, snapshotSeq,
        snapshotSeq, range.fromTs, range.toTs
      ).n
    );
  }

  /**
   * 质控勘误：为已存在的观测创建下一版本。
   * 原始观测视为第 1 版；仅当 expectedRevision 等于当前修订号时才创建下一版。
   * 勘误与批次写入共用同一接收序号：新版本分配下一个 seq 作为 effective_seq，
   * 因此它与新接收数据一样，只会进入此后开启的新快照。
   *
   * @returns {{revision:number, effectiveSeq:number}}
   * @throws {SampleNotFoundError} 观测不存在（未知流 / 未知编号）
   * @throws {RevisionConflictError} expectedRevision 过期；携带 currentRevision，且不改写数据
   */
  reviseSample(streamId, sampleId, { ts, value, expectedRevision }) {
    const db = this.db;
    db.exec('BEGIN IMMEDIATE');
    try {
      const cur = this.stmts.getSampleForRevision.get(streamId, sampleId);
      if (!cur) {
        throw new SampleNotFoundError(
          `观测不存在: stream=${streamId} sample=${sampleId}`,
          { reason: 'sample_not_found', streamId, sampleId }
        );
      }
      const currentRevision = Number(cur.currentRevision);
      if (expectedRevision !== currentRevision) {
        throw new RevisionConflictError(
          `修订号过期：期望 ${expectedRevision}，当前为 ${currentRevision}`,
          {
            reason: 'revision_conflict',
            sampleId,
            expectedRevision,
            currentRevision,
          }
        );
      }
      const effectiveSeq = Number(this.stmts.getLastSeq.get().last_seq) + 1;
      const revision = currentRevision + 1;
      this.stmts.insertRevision.run(
        streamId, sampleId, revision, ts, value, effectiveSeq
      );
      this.stmts.bumpSeq.run(effectiveSeq);
      db.exec('COMMIT');
      return { revision, effectiveSeq };
    } catch (err) {
      db.exec('ROLLBACK');
      throw err;
    }
  }

  /** 观测当前（最新）修订号；不存在返回 null。 */
  currentRevision(streamId, sampleId) {
    const row = this.stmts.getSampleForRevision.get(streamId, sampleId);
    return row ? Number(row.currentRevision) : null;
  }

  totalSamples() {
    return Number(this.stmts.totalSamples.get().n);
  }

  close() {
    this.db.close();
  }
}

export class BatchConflictError extends Error {
  constructor(message, details) {
    super(message);
    this.name = 'BatchConflictError';
    this.statusCode = 409;
    this.details = details;
  }
}

export class SampleNotFoundError extends Error {
  constructor(message, details) {
    super(message);
    this.name = 'SampleNotFoundError';
    this.statusCode = 404;
    this.details = details;
  }
}

export class RevisionConflictError extends Error {
  constructor(message, details) {
    super(message);
    this.name = 'RevisionConflictError';
    this.statusCode = 409;
    this.details = details;
  }
}
