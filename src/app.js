import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { Store, BatchConflictError, SampleNotFoundError, RevisionConflictError } from './db.js';
import {
  issueCursor,
  parseCursor,
  bindCursor,
  CursorError,
} from './cursor.js';
import {
  validateBatch,
  validateQuery,
  validateRevision,
  ValidationError,
  TIME_BOUNDS,
} from './validation.js';

const STREAM_RE = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}$/;

export function createApp({ dbPath, cursorSecret = randomBytes(32).toString('hex') } = {}) {
  const store = new Store(dbPath ?? process.env.DB_PATH ?? '/data/observations.db');

  function send(res, status, payload) {
    const body = JSON.stringify(payload);
    res.writeHead(status, {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
    });
    res.end(body);
  }

  function sendError(res, status, code, message, details) {
    send(res, status, {
      error: { code, message, ...(details ? { details } : {}) },
    });
  }

  async function readJson(req) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
      size += chunk.length;
      if (size > 5 * 1024 * 1024) {
        throw new ValidationError('请求体过大（上限 5 MiB）');
      }
      chunks.push(chunk);
    }
    if (chunks.length === 0) {
      throw new ValidationError('缺少 JSON 请求体');
    }
    try {
      return JSON.parse(Buffer.concat(chunks).toString('utf8'));
    } catch {
      throw new ValidationError('请求体不是合法 JSON');
    }
  }

  /**
   * GET /api/streams/:streamId/samples
   * 首次请求（无 cursor）：固定当前接收序号为快照上界。
   * 后续请求（带 cursor）：沿用发起时的快照序号与时间范围，游标重启后仍可用。
   */
  function handleList(req, res, streamId, url) {
    let q;
    try {
      q = validateQuery(url.searchParams);
    } catch (err) {
      if (err instanceof ValidationError) {
        return sendError(res, 400, 'invalid_query', err.message, err.details);
      }
      throw err;
    }

    let snapshotSeq, fromTs, toTs, pageSize, after;
    let sessionStarted = false;

    if (q.cursor) {
      let payload;
      try {
        payload = parseCursor(cursorSecret, q.cursor);
      } catch (err) {
        if (err instanceof CursorError) {
          return sendError(res, err.statusCode, err.code, err.message);
        }
        throw err;
      }
      try {
        ({ snapshotSeq, fromTs, toTs, pageSize, after } = bindCursor(payload, {
          streamId,
          fromTs: url.searchParams.has('from') ? q.fromTs : null,
          toTs: url.searchParams.has('to') ? q.toTs : null,
          pageSize: url.searchParams.has('pageSize') ? q.pageSize : null,
        }));
      } catch (err) {
        if (err instanceof CursorError) {
          return sendError(res, err.statusCode, err.code, err.message);
        }
        throw err;
      }
    } else {
      // 首请求：快照 = 此刻已接收的全部数据
      snapshotSeq = store.currentSeq();
      fromTs = q.fromTs;
      toTs = q.toTs;
      pageSize = q.pageSize;
      after = null;
      sessionStarted = true;
    }

    const limit = pageSize + 1; // 多取一条以判断是否还有下一页
    const rows = store.readPage(
      streamId, snapshotSeq, { fromTs, toTs }, after, limit
    );
    const hasMore = rows.length > pageSize;
    const page = hasMore ? rows.slice(0, pageSize) : rows;

    const items = page.map((r) => ({
      sampleId: r.sampleId,
      ts: r.ts,
      value: r.value,
    }));

    let nextCursor = null;
    if (hasMore && page.length > 0) {
      const last = page[page.length - 1];
      nextCursor = issueCursor(cursorSecret, {
        v: 1,
        s: streamId,
        q: snapshotSeq,
        f: fromTs,
        t: toTs,
        p: pageSize,
        a: { ts: last.ts, i: last.sampleId },
      });
    }

    return send(res, 200, {
      items,
      pageSize,
      snapshotSeq,
      nextCursor,
      done: nextCursor === null,
    });
  }

  async function handlePost(req, res, streamId) {
    const body = await readJson(req);
    const { items } = validateBatch(body);
    try {
      const result = store.insertBatch(streamId, items);
      return send(res, 200, {
        accepted: result.count,
        firstSeq: result.firstSeq,
        lastSeq: result.lastSeq,
      });
    } catch (err) {
      if (err instanceof BatchConflictError) {
        // 整批已回滚，无部分写入
        return sendError(res, 409, err.details.reason, err.message, err.details);
      }
      throw err;
    }
  }

  /**
   * POST /api/streams/:streamId/samples/:sampleId/revisions
   * 质控勘误：仅当 expectedRevision 等于当前修订号时创建下一版。
   * 未知观测 -> 404；过期修订 -> 409（回送当前修订号，不改写数据）。
   */
  async function handleRevision(req, res, streamId, sampleId) {
    if (!sampleId || sampleId.length > 200) {
      return sendError(res, 400, 'invalid_sample_id', 'sampleId 非法（非空且最多 200 字符）');
    }
    const body = await readJson(req);
    const rev = validateRevision(body);
    try {
      const result = store.reviseSample(streamId, sampleId, rev);
      return send(res, 200, {
        sampleId,
        revision: result.revision,
        effectiveSeq: result.effectiveSeq,
      });
    } catch (err) {
      if (err instanceof SampleNotFoundError) {
        return sendError(res, 404, err.details.reason, err.message, err.details);
      }
      if (err instanceof RevisionConflictError) {
        return sendError(res, 409, err.details.reason, err.message, err.details);
      }
      throw err;
    }
  }

  const handler = async (req, res) => {
    try {
      const url = new URL(req.url, 'http://localhost');
      if (req.method === 'GET' && url.pathname === '/health') {
        return send(res, 200, {
          status: 'ok',
          currentSeq: store.currentSeq(),
          totalSamples: store.totalSamples(),
        });
      }

      const mCollection = url.pathname.match(/^\/api\/streams\/([^/]+)\/samples\/?$/);
      const mRevision = !mCollection &&
        url.pathname.match(/^\/api\/streams\/([^/]+)\/samples\/([^/]+)\/revisions\/?$/);
      if (!mCollection && !mRevision) {
        return sendError(res, 404, 'not_found', `路径不存在: ${url.pathname}`);
      }
      let streamId;
      try {
        streamId = decodeURIComponent(mCollection ? mCollection[1] : mRevision[1]);
      } catch {
        return sendError(res, 400, 'invalid_stream_id', 'streamId 编码非法');
      }
      if (!STREAM_RE.test(streamId)) {
        return sendError(res, 400, 'invalid_stream_id', 'streamId 非法');
      }

      if (mRevision) {
        let sampleId;
        try {
          sampleId = decodeURIComponent(mRevision[2]);
        } catch {
          return sendError(res, 400, 'invalid_sample_id', 'sampleId 编码非法');
        }
        if (req.method !== 'POST') {
          return sendError(res, 405, 'method_not_allowed', '勘误接口仅支持 POST');
        }
        return await handleRevision(req, res, streamId, sampleId);
      }

      if (req.method === 'POST') return await handlePost(req, res, streamId);
      if (req.method === 'GET') return handleList(req, res, streamId, url);
      return sendError(res, 405, 'method_not_allowed', '仅支持 GET / POST');
    } catch (err) {
      if (err instanceof ValidationError) {
        return sendError(res, 400, 'invalid_request', err.message, err.details);
      }
      console.error('unhandled error:', err);
      if (!res.headersSent) {
        sendError(res, 500, 'internal_error', '服务内部错误');
      } else {
        res.end();
      }
    }
  };

  const server = createServer(handler);
  return {
    store,
    server,
    close: () =>
      new Promise((resolve) =>
        server.close(() => {
          store.close();
          resolve();
        })
      ),
  };
}

export function startServer() {
  const port = Number(process.env.PORT ?? 8080);
  const host = process.env.HOST ?? '0.0.0.0';
  const secret = process.env.CURSOR_SECRET; // 缺省时随机 -> 游标重启失效，故 compose 固定注入
  const app = createApp(
    secret ? { cursorSecret: secret } : {}
  );
  app.server.listen(port, host, () => {
    const actualPort = app.server.address().port;
    console.log(`ocean observation API listening on http://${host}:${actualPort}`);
  });
  return app;
}
