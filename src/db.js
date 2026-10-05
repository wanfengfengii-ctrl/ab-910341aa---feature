import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

/**
 * 持久化层（node:sqlite，Node 22 内置，同步 API；WAL 模式，崩溃后不损坏）。
 *
 * 数据模型：
 *  - samples(stream_id, sample_id, rev, ts, value, seq)
 *      seq 为全局单调递增的接收序号（任何写入——接收或勘误——即分配）。
 *      原始观测占 rev=1，每次勘误追加一行 rev=2,3,...，历史版本永不改写。
 *      (stream_id, sample_id, rev) 唯一。
 *  - seq_meta：单行，保存已分配的最后一个序号（= 快照上界来源）。
 *
 * 快照可见性：读取时只取 seq <= snapshotSeq 的行，并对每个观测
 * (stream_id, sample_id) 选取其中 rev 最大的一版。因此：
 *  - 快照固定后发生的勘误（seq 更大）对该快照不可见，旧游标始终看到旧版；
 *  - 勘误后开启的新快照直接看到新版，并按新版 ts 重新参与稳定排序；
 *  - 每个观测在任一快照中恰好实体化为一行，不会消失或出现两次。
 */
export class Store {
  constructor(dbPath) {
    if (dbPath !== ':memory:') mkdirSync(dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode=WAL');
    this.db.exec('PRAGMA foreign_keys=ON');
    this.db.exec('PRAGMA busy_timeout=5000');
    this.#initSchema();
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
    this.#prepareStatements();
  }

  #createSamplesTable() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS samples (
        stream_id TEXT    NOT NULL,
        sample_id TEXT    NOT NULL,
        rev       INTEGER NOT NULL,          -- 1=原始观测，2..=勘误版本
        ts        TEXT    NOT NULL,   -- RFC3339，字典序即时间序（要求 UTC / 'Z'）
        value     INTEGER NOT NULL,
        seq       INTEGER NOT NULL,
        PRIMARY KEY (stream_id, sample_id, rev)
      ) WITHOUT ROWID
    `);
  }

  #initSchema() {
    const info = this.db.prepare('PRAGMA table_info(samples)').all();
    if (info.length === 0) {
      this.#createSamplesTable();
      return;
    }
    if (info.some((c) => c.name === 'rev')) return; // 已是多版本结构

    // 旧版表主键为 (stream_id, sample_id)，无法直接容纳多版本 -> 整表重建，
    // 既有观测全部作为第 1 版迁入，seq 保持不变（旧游标快照上界仍有效）。
    this.db.exec('ALTER TABLE samples RENAME TO samples_legacy');
    this.#createSamplesTable();
    this.db.exec(`
      INSERT INTO samples (stream_id, sample_id, rev, ts, value, seq)
      SELECT stream_id, sample_id, 1, ts, value, seq FROM samples_legacy
    `);
    this.db.exec('DROP TABLE samples_legacy');
  }

  #prepareStatements() {
    const db = this.db;
    // 快照实体化子查询：在 seq <= 快照上界的行中，取每个观测的最大 rev。
    const latestJoin = `
      JOIN (
        SELECT stream_id AS sid, sample_id AS iid, MAX(rev) AS rev
          FROM samples
         WHERE stream_id = ? AND seq <= ?
         GROUP BY stream_id, sample_id
      ) v ON v.sid = s.stream_id AND v.iid = s.sample_id AND v.rev = s.rev
    `;
    this.stmts = {
      getLastSeq: db.prepare('SELECT last_seq FROM seq_meta WHERE id = 1'),
      bumpSeq: db.prepare('UPDATE seq_meta SET last_seq = ? WHERE id = 1'),
      existsDup: db.prepare(
        'SELECT 1 FROM samples WHERE stream_id = ? AND sample_id = ? AND rev = 1'
      ),
      insert: db.prepare(
        'INSERT INTO samples (stream_id, sample_id, rev, ts, value, seq)' +
        ' VALUES (?, ?, 1, ?, ?, ?)'
      ),
      getCurrentRevision: db.prepare(
        `SELECT rev AS revision FROM samples
          WHERE stream_id = ? AND sample_id = ?
          ORDER BY rev DESC LIMIT 1`
      ),
      insertRevision: db.prepare(
        'INSERT INTO samples (stream_id, sample_id, rev, ts, value, seq)' +
        ' VALUES (?, ?, ?, ?, ?, ?)'
      ),
      pageStart: db.prepare(
        `SELECT s.ts, s.sample_id AS sampleId, s.value, s.seq
           FROM samples s
           ${latestJoin}
          WHERE s.stream_id = ?
            AND s.seq <= ?
            AND s.ts >= ? AND s.ts <= ?
          ORDER BY s.ts ASC, s.sample_id ASC
          LIMIT ?`
      ),
      pageAfter: db.prepare(
        `SELECT s.ts, s.sample_id AS sampleId, s.value, s.seq
           FROM samples s
           ${latestJoin}
          WHERE s.stream_id = ?
            AND s.seq <= ?
            AND s.ts >= ? AND s.ts <= ?
            AND (s.ts > ? OR (s.ts = ? AND s.sample_id > ?))
          ORDER BY s.ts ASC, s.sample_id ASC
          LIMIT ?`
      ),
      countSnapshot: db.prepare(
        `SELECT COUNT(*) AS n FROM samples s
           ${latestJoin}
          WHERE s.stream_id = ? AND s.seq <= ? AND s.ts >= ? AND s.ts <= ?`
      ),
      totalSamples: db.prepare(
        'SELECT COUNT(*) AS n FROM samples WHERE rev = 1'
      ),
    };
  }

  /** 当前已分配的最后序号（同时也是“最新快照”的上界）。 */
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
   * 乐观锁勘误：仅当 expectedRevision 等于当前修订号时，追加下一版（原始为第 1 版）。
   * 整个检查-写入在一个 IMMEDIATE 事务内，并发勘误天然串行化，不会丢更新。
   *
   * @returns {null} 观测不存在（调用方应答 404）
   * @returns {{revision:number, effectiveSeq:number}} 成功：新版本号与生效序号
   * @throws {RevisionConflictError} 修订号过期（调用方应答 409），数据不被改写
   */
  reviseSample(streamId, sampleId, expectedRevision, ts, value) {
    const db = this.db;
    db.exec('BEGIN IMMEDIATE');
    let txClosed = false; // 显式跟踪事务是否已结束（node:sqlite 无 inTransaction 属性）
    const rollback = () => {
      if (!txClosed) {
        db.exec('ROLLBACK');
        txClosed = true;
      }
    };
    try {
      const cur = this.stmts.getCurrentRevision.get(streamId, sampleId);
      if (!cur) {
        rollback();
        return null;
      }
      const currentRevision = Number(cur.revision);
      if (expectedRevision !== currentRevision) {
        rollback();
        throw new RevisionConflictError(
          `修订号过期：当前为第 ${currentRevision} 版，提交基于第 ${expectedRevision} 版`,
          { currentRevision, expectedRevision }
        );
      }
      const seq = Number(this.stmts.getLastSeq.get().last_seq) + 1;
      this.stmts.insertRevision.run(
        streamId, sampleId, currentRevision + 1, ts, value, seq
      );
      this.stmts.bumpSeq.run(seq);
      db.exec('COMMIT');
      txClosed = true;
      return { revision: currentRevision + 1, effectiveSeq: seq };
    } catch (err) {
      rollback();
      throw err;
    }
  }

  /**
   * 读取快照页（稳定排序：ts ASC, sampleId ASC）。
   * @param {string} streamId
   * @param {number} snapshotSeq 快照上界（只含 seq <= snapshotSeq 的版本）
   * @param {{fromTs:string,toTs:string}} range 闭区间，按 ts 字符串比较
   * @param {{ts:string,sampleId:string}|null} after 严格大于该位置之后开始
   * @param {number} pageSize
   */
  readPage(streamId, snapshotSeq, range, after, pageSize) {
    // keyset 分页：比较 (ts, sample_id) 元组，稳定且不依赖 OFFSET
    let rows;
    if (after) {
      rows = this.stmts.pageAfter.all(
        streamId, snapshotSeq,
        streamId, snapshotSeq, range.fromTs, range.toTs,
        after.ts, after.ts, after.sampleId, pageSize
      );
    } else {
      rows = this.stmts.pageStart.all(
        streamId, snapshotSeq,
        streamId, snapshotSeq, range.fromTs, range.toTs, pageSize
      );
    }
    return rows.map((r) => ({
      ts: r.ts,
      sampleId: r.sampleId,
      value: Number(r.value),
      seq: Number(r.seq),
    }));
  }

  countSnapshot(streamId, snapshotSeq, range) {
    return Number(
      this.stmts.countSnapshot.get(
        streamId, snapshotSeq,
        streamId, snapshotSeq, range.fromTs, range.toTs
      ).n
    );
  }

  /** 逻辑观测总数（原始行每个观测恰好一行，与勘误次数无关）。 */
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

export class RevisionConflictError extends Error {
  constructor(message, details) {
    super(message);
    this.name = 'RevisionConflictError';
    this.statusCode = 409;
    this.details = details;
  }
}
