/**
 * Phase 5 integration tests.
 *
 * Tests the new REST endpoints for the observability dashboard:
 *   GET  /queues
 *   GET  /queues/:name/jobs
 *   POST /jobs/:id/retry
 *   GET  /workers
 *   GET  /queues/:name/dlq
 *   POST /queues/:name/dlq/requeue
 *
 * Uses ioredis-mock + mock pg Pool (same pattern as phase 1-4 tests).
 * Uses Node's built-in http module + fetch to test routes via a real Express app.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import RedisMock from 'ioredis-mock';
import express from 'express';
import http from 'http';
import { redisKeys } from '@nexusqueue/shared';
import { buildRouter } from '../server/src/routes.js';
import { Producer } from '../server/src/producer.js';

/**
 * Minimal pg Pool mock that captures queries and returns configurable results.
 */
function createMockPool() {
  const queries: Array<{ text: string; values: unknown[] }> = [];
  let nextResult: { rows: any[]; rowCount: number } = { rows: [], rowCount: 0 };
  const resultQueue: Array<{ rows: any[]; rowCount: number }> = [];

  const pool = {
    query: vi.fn(async (text: string, values: unknown[] = []) => {
      queries.push({ text, values });
      if (resultQueue.length > 0) {
        return resultQueue.shift()!;
      }
      return nextResult;
    }),
    end: vi.fn(async () => {}),
  };

  return {
    pool: pool as any,
    queries,
    setNextResult: (result: { rows: any[]; rowCount: number }) => { nextResult = result; },
    pushResult: (result: { rows: any[]; rowCount: number }) => { resultQueue.push(result); },
  };
}

function createTestApp(redis: any, pg: any) {
  const producer = new Producer({ redis, pg });
  const app = express();
  app.use(express.json());
  app.use(buildRouter({ producer, pg, redis }));
  return app;
}

function startServer(app: express.Application): Promise<{ server: http.Server; baseUrl: string }> {
  return new Promise((resolve) => {
    const server = app.listen(0, '127.0.0.1', () => {
      const addr = server.address() as { port: number };
      resolve({ server, baseUrl: `http://127.0.0.1:${addr.port}` });
    });
  });
}

function stopServer(server: http.Server): Promise<void> {
  return new Promise((resolve) => {
    server.close(() => resolve());
  });
}

describe('Phase 5 -- GET /queues', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    await stopServer(server);
  });

  it('returns empty array when no queues registered', async () => {
    // pg returns empty results for active + stats queries
    mockPg.setNextResult({ rows: [], rowCount: 0 });

    const res = await fetch(`${baseUrl}/queues`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body).toEqual({ queues: [] });
  });

  it('returns queue stats for registered queues', async () => {
    // Seed a queue in the registry
    await redis.sadd(redisKeys.queueRegistry, 'emails');

    // Seed some pending jobs in the normal queue
    await redis.lpush(redisKeys.queue('emails'), 'job-1', 'job-2');
    await redis.lpush(redisKeys.queuePriority('emails', 'high'), 'job-3');

    // Mock pg: first call = active count, second call = stats
    mockPg.pushResult({ rows: [{ count: '1' }], rowCount: 1 }); // active count
    mockPg.pushResult({ rows: [
      { status: 'completed', count: '5' },
      { status: 'failed', count: '2' },
    ], rowCount: 2 }); // stats

    const res = await fetch(`${baseUrl}/queues`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.queues).toHaveLength(1);
    expect(body.queues[0]).toEqual({
      name: 'emails',
      pending: 3, // 2 normal + 1 high + 0 low
      active: 1,
      completed: 5,
      failed: 2,
      dlq: 0,
    });
  });
});

describe('Phase 5 -- GET /queues/:name/jobs', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    await stopServer(server);
  });

  it('returns jobs from Postgres for a queue', async () => {
    const now = new Date();
    // First call: count query
    mockPg.pushResult({ rows: [{ total: '2' }], rowCount: 1 });
    // Second call: data query
    mockPg.pushResult({ rows: [
      {
        id: 'j1', queue_name: 'q1', job_name: 'send-email',
        payload: { to: 'a@b.com' }, status: 'completed',
        attempts: 1, max_attempts: 3, error_message: null,
        created_at: now, started_at: now, completed_at: now,
      },
      {
        id: 'j2', queue_name: 'q1', job_name: 'send-sms',
        payload: { to: '555' }, status: 'pending',
        attempts: 0, max_attempts: 3, error_message: null,
        created_at: now, started_at: null, completed_at: null,
      },
    ], rowCount: 2 });

    const res = await fetch(`${baseUrl}/queues/q1/jobs`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.total).toBe(2);
    expect(body.jobs).toHaveLength(2);
    expect(body.jobs[0].id).toBe('j1');
    expect(body.jobs[0].queueName).toBe('q1');
    expect(body.jobs[1].id).toBe('j2');
  });

  it('passes status filter to query', async () => {
    mockPg.pushResult({ rows: [{ total: '0' }], rowCount: 1 });
    mockPg.pushResult({ rows: [], rowCount: 0 });

    await fetch(`${baseUrl}/queues/q1/jobs?status=failed&limit=10&offset=5`);

    // Count query should include status filter
    const countQuery = mockPg.queries[0];
    expect(countQuery.text).toContain('status = $2');
    expect(countQuery.values).toContain('failed');

    // Data query should include status, limit, offset
    const dataQuery = mockPg.queries[1];
    expect(dataQuery.text).toContain('status = $2');
    expect(dataQuery.values).toEqual(['q1', 'failed', 10, 5]);
  });
});

describe('Phase 5 -- POST /jobs/:id/retry', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    await stopServer(server);
  });

  it('returns 404 for unknown job', async () => {
    // getJob queries Postgres and returns no rows
    mockPg.setNextResult({ rows: [], rowCount: 0 });

    const res = await fetch(`${baseUrl}/jobs/unknown-id/retry`, { method: 'POST' });
    expect(res.status).toBe(404);
    const body = await res.json();
    expect(body.error).toBe('not_found');
  });

  it('returns 400 for non-failed/dlq job', async () => {
    // getJob returns a completed job
    mockPg.setNextResult({
      rows: [{
        id: 'j1', queue_name: 'q1', job_name: 'test',
        payload: {}, status: 'completed', attempts: 1, max_attempts: 3,
        error_message: null, created_at: new Date(), started_at: new Date(), completed_at: new Date(),
      }],
      rowCount: 1,
    });

    const res = await fetch(`${baseUrl}/jobs/j1/retry`, { method: 'POST' });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toBe('job_not_retriable');
  });

  it('succeeds for failed job, resets state and pushes to queue', async () => {
    // Seed the Redis job hash
    await redis.hset(redisKeys.job('j1'), {
      id: 'j1', queueName: 'q1', jobName: 'send-email',
      payload: JSON.stringify({ x: 1 }), status: 'failed',
      attempts: '3', maxAttempts: '3', priority: 'normal',
      errorMessage: 'timeout', createdAt: new Date().toISOString(),
      startedAt: '', completedAt: '',
    });

    // Also place in DLQ
    await redis.lpush(redisKeys.dlq('q1'), 'j1');

    // getJob returns a failed job
    mockPg.pushResult({
      rows: [{
        id: 'j1', queue_name: 'q1', job_name: 'send-email',
        payload: { x: 1 }, status: 'failed', attempts: 3, max_attempts: 3,
        error_message: 'timeout', created_at: new Date(), started_at: new Date(), completed_at: new Date(),
      }],
      rowCount: 1,
    });
    // markJobPendingForRetry result
    mockPg.pushResult({ rows: [], rowCount: 1 });

    const res = await fetch(`${baseUrl}/jobs/j1/retry`, { method: 'POST' });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.success).toBe(true);

    // Verify Redis state was reset
    const hash = await redis.hgetall(redisKeys.job('j1'));
    expect(hash.status).toBe('pending');
    expect(hash.attempts).toBe('0');

    // Verify job was pushed to queue
    const queueLen = await redis.llen(redisKeys.queue('q1'));
    expect(queueLen).toBe(1);

    // Verify removed from DLQ
    const dlqLen = await redis.llen(redisKeys.dlq('q1'));
    expect(dlqLen).toBe(0);
  });
});

describe('Phase 5 -- GET /workers', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    await stopServer(server);
  });

  it('returns empty array when no workers registered', async () => {
    const res = await fetch(`${baseUrl}/workers`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.workers).toEqual([]);
  });

  it('returns worker list with metadata from Redis', async () => {
    // Register a worker
    await redis.sadd(redisKeys.workerRegistry, 'w1');
    await redis.hset(redisKeys.workerMeta('w1'), {
      status: 'active',
      queue: 'emails',
      startedAt: '2024-01-01T00:00:00Z',
      currentJobs: '2',
    });
    await redis.set(redisKeys.heartbeat('w1'), '1704067200000');

    const res = await fetch(`${baseUrl}/workers`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.workers).toHaveLength(1);
    expect(body.workers[0]).toEqual({
      id: 'w1',
      status: 'active',
      queue: 'emails',
      startedAt: '2024-01-01T00:00:00Z',
      currentJobs: 2,
      lastHeartbeat: 1704067200000,
    });
  });
});

describe('Phase 5 -- GET /queues/:name/dlq', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    await stopServer(server);
  });

  it('returns empty array when DLQ is empty', async () => {
    const res = await fetch(`${baseUrl}/queues/q1/dlq`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.jobs).toEqual([]);
  });

  it('returns DLQ jobs from Redis', async () => {
    // Seed a DLQ job
    await redis.lpush(redisKeys.dlq('q1'), 'dlq-job-1');
    await redis.hset(redisKeys.job('dlq-job-1'), {
      id: 'dlq-job-1', queueName: 'q1', jobName: 'failing-task',
      payload: JSON.stringify({ key: 'val' }), status: 'dlq',
      attempts: '3', maxAttempts: '3',
      errorMessage: 'max retries exceeded',
      createdAt: '2024-01-01T00:00:00Z', startedAt: '', completedAt: '',
    });

    const res = await fetch(`${baseUrl}/queues/q1/dlq`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.jobs).toHaveLength(1);
    expect(body.jobs[0].id).toBe('dlq-job-1');
    expect(body.jobs[0].jobName).toBe('failing-task');
    expect(body.jobs[0].status).toBe('dlq');
    expect(body.jobs[0].payload).toEqual({ key: 'val' });
    expect(body.jobs[0].errorMessage).toBe('max retries exceeded');
  });
});

describe('Phase 5 -- POST /queues/:name/dlq/requeue', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    await stopServer(server);
  });

  it('returns 400 when no jobIds or all provided', async () => {
    const res = await fetch(`${baseUrl}/queues/rq-inv/dlq/requeue`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    expect(res.status).toBe(400);
    const body = await res.json();
    expect(body.error).toBe('invalid_request');
  });

  it('requeues specified jobs by IDs', async () => {
    // Seed DLQ jobs
    await redis.lpush(redisKeys.dlq('rq-ids'), 'rq-j1', 'rq-j2');
    await redis.hset(redisKeys.job('rq-j1'), {
      id: 'rq-j1', queueName: 'rq-ids', jobName: 'task-a',
      payload: JSON.stringify({}), status: 'dlq',
      attempts: '3', maxAttempts: '3', priority: 'normal',
      errorMessage: 'error', createdAt: '2024-01-01T00:00:00Z',
      startedAt: '', completedAt: '',
    });
    await redis.hset(redisKeys.job('rq-j2'), {
      id: 'rq-j2', queueName: 'rq-ids', jobName: 'task-b',
      payload: JSON.stringify({}), status: 'dlq',
      attempts: '3', maxAttempts: '3', priority: 'high',
      errorMessage: 'error', createdAt: '2024-01-01T00:00:00Z',
      startedAt: '', completedAt: '',
    });

    // markJobPendingForRetry will be called twice
    mockPg.pushResult({ rows: [], rowCount: 1 });
    mockPg.pushResult({ rows: [], rowCount: 1 });

    const res = await fetch(`${baseUrl}/queues/rq-ids/dlq/requeue`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jobIds: ['rq-j1', 'rq-j2'] }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.requeued).toBe(2);

    // Verify rq-j1 went to normal queue
    const normalQueue = await redis.lrange(redisKeys.queue('rq-ids'), 0, -1);
    expect(normalQueue).toContain('rq-j1');

    // Verify rq-j2 went to high priority queue
    const highQueue = await redis.lrange(redisKeys.queuePriority('rq-ids', 'high'), 0, -1);
    expect(highQueue).toContain('rq-j2');

    // DLQ should be empty
    const dlqLen = await redis.llen(redisKeys.dlq('rq-ids'));
    expect(dlqLen).toBe(0);

    // Redis hashes updated
    const h1 = await redis.hgetall(redisKeys.job('rq-j1'));
    expect(h1.status).toBe('pending');
    expect(h1.attempts).toBe('0');
  });

  it('requeues all DLQ jobs when all:true', async () => {
    // Seed DLQ jobs
    await redis.lpush(redisKeys.dlq('rq-all'), 'rq-a1');
    await redis.hset(redisKeys.job('rq-a1'), {
      id: 'rq-a1', queueName: 'rq-all', jobName: 'task-a',
      payload: JSON.stringify({}), status: 'dlq',
      attempts: '3', maxAttempts: '3', priority: 'normal',
      errorMessage: 'error', createdAt: '2024-01-01T00:00:00Z',
      startedAt: '', completedAt: '',
    });

    mockPg.pushResult({ rows: [], rowCount: 1 });

    const res = await fetch(`${baseUrl}/queues/rq-all/dlq/requeue`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ all: true }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.requeued).toBe(1);

    // Verify queue has the job
    const queueItems = await redis.lrange(redisKeys.queue('rq-all'), 0, -1);
    expect(queueItems).toContain('rq-a1');
  });
});
