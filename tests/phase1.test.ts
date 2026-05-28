/**
 * Phase 1 integration test.
 *
 * Uses ioredis-mock to avoid needing a real Redis, and a simple
 * mock for the pg Pool that captures SQL operations. This validates
 * the end-to-end flow: enqueue -> worker picks up -> state transitions.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import RedisMock from 'ioredis-mock';
import { redisKeys } from '@nexusqueue/shared';
import { Producer } from '../server/src/producer.js';
import { Worker } from '../worker/src/worker.js';

/**
 * Minimal pg Pool mock that captures queries for assertion.
 */
function createMockPool() {
  const queries: Array<{ text: string; values: unknown[] }> = [];
  const pool = {
    query: vi.fn(async (text: string, values: unknown[] = []) => {
      queries.push({ text, values });
      return { rows: [], rowCount: 0 };
    }),
    end: vi.fn(async () => {}),
  };
  return { pool: pool as any, queries };
}

describe('Phase 1 — Producer', () => {
  let redis: InstanceType<typeof RedisMock>;
  let pool: ReturnType<typeof createMockPool>;
  let producer: Producer;

  beforeEach(() => {
    redis = new RedisMock();
    pool = createMockPool();
    producer = new Producer({ redis: redis as any, pg: pool.pool });
  });

  it('enqueue() returns a UUID job ID', async () => {
    const id = await producer.enqueue('send-email', { to: 'a@b.com' });
    expect(id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
    );
  });

  it('enqueue() inserts a Postgres row first', async () => {
    await producer.enqueue('test-job', { x: 1 }, { queue: 'myq' });
    expect(pool.queries.length).toBeGreaterThanOrEqual(1);
    expect(pool.queries[0]!.text).toContain('INSERT INTO jobs');
    expect(pool.queries[0]!.values[1]).toBe('myq'); // queue_name
  });

  it('enqueue() pushes job ID to Redis list and stores hash', async () => {
    const id = await producer.enqueue('test-job', { hello: 'world' }, { queue: 'q1' });

    // Check Redis list
    const listLen = await redis.llen(redisKeys.queue('q1'));
    expect(listLen).toBe(1);
    const items = await redis.lrange(redisKeys.queue('q1'), 0, -1);
    expect(items).toContain(id);

    // Check Redis hash
    const hash = await redis.hgetall(redisKeys.job(id));
    expect(hash.jobName).toBe('test-job');
    expect(hash.status).toBe('pending');
    expect(JSON.parse(hash.payload)).toEqual({ hello: 'world' });
  });

  it('enqueue() registers the queue name in the queueRegistry set', async () => {
    await producer.enqueue('test-job', {}, { queue: 'alpha' });
    const members = await redis.smembers(redisKeys.queueRegistry);
    expect(members).toContain('alpha');
  });

  it('enqueue() defaults queue to "default" and maxAttempts to 1', async () => {
    const id = await producer.enqueue('test-job', {});
    const hash = await redis.hgetall(redisKeys.job(id));
    expect(hash.queueName).toBe('default');
    expect(hash.maxAttempts).toBe('1');
  });

  it('enqueue() rejects empty jobName', async () => {
    await expect(producer.enqueue('', {})).rejects.toThrow('non-empty string');
  });
});

describe('Phase 1 — Worker (unit: processOne via direct call)', () => {
  /**
   * ioredis-mock doesn't support BRPOP (blocking commands). This is fine:
   * BRPOP is a transport concern; the business logic lives in processOne.
   *
   * Strategy: expose processOne as a public method (it's already private;
   * we use (worker as any) to access it in tests). Alternatively we test
   * via the RPOP fallback path below. We choose to invoke processOne
   * directly for clarity.
   */
  let redis: InstanceType<typeof RedisMock>;
  let pool: ReturnType<typeof createMockPool>;
  let worker: Worker;

  beforeEach(() => {
    redis = new RedisMock();
    pool = createMockPool();
    worker = new Worker({
      redis: redis as any,
      pg: pool.pool,
      queue: 'test',
      workerId: 'w1',
    });
  });

  async function seedJob(jobId: string, jobName: string, payload: unknown) {
    await redis.hset(redisKeys.job(jobId), {
      id: jobId,
      queueName: 'test',
      jobName,
      payload: JSON.stringify(payload),
      status: 'pending',
      attempts: '0',
      maxAttempts: '1',
      errorMessage: '',
      createdAt: new Date().toISOString(),
      startedAt: '',
      completedAt: '',
    });
  }

  it('processes a job and marks it completed', async () => {
    const handlerFn = vi.fn(async () => 'done');
    worker.register('greet', handlerFn);

    const jobId = 'job-001';
    await seedJob(jobId, 'greet', { name: 'Ada' });

    // Call processOne directly (it's private, but we access it for testing).
    await (worker as any).processOne(jobId);

    // Handler was called with parsed payload and context
    expect(handlerFn).toHaveBeenCalledOnce();
    expect(handlerFn).toHaveBeenCalledWith(
      { name: 'Ada' },
      expect.objectContaining({ jobId, jobName: 'greet', workerId: 'w1' }),
    );

    // Redis hash updated to completed
    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('completed');
    expect(hash.completedAt).toBeTruthy();

    // Postgres got the state transitions (active then completed)
    const pgTexts = pool.queries.map((q) => q.text);
    expect(pgTexts.some((t) => t.includes("status = 'active'"))).toBe(true);
    expect(pgTexts.some((t) => t.includes("status = 'completed'"))).toBe(true);
  });

  it('marks a job failed when handler throws', async () => {
    worker.register('boom', async () => {
      throw new Error('kaboom');
    });

    const jobId = 'job-002';
    await seedJob(jobId, 'boom', {});

    await (worker as any).processOne(jobId);

    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('failed');
    expect(hash.errorMessage).toBe('kaboom');

    // Postgres records the failure
    const pgTexts = pool.queries.map((q) => q.text);
    expect(pgTexts.some((t) => t.includes("status = 'failed'"))).toBe(true);
  });

  it('fails gracefully when no handler is registered', async () => {
    const jobId = 'job-003';
    await seedJob(jobId, 'unknown-job', {});

    await (worker as any).processOne(jobId);

    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('failed');
    expect(hash.errorMessage).toContain('no handler registered');
  });

  it('handles missing job hash gracefully', async () => {
    // processOne is called with a jobId whose hash doesn't exist in Redis.
    // This can happen if Redis was flushed between BRPOP and processing.
    await (worker as any).processOne('ghost-job');

    // Should not throw, should not update Postgres.
    expect(pool.queries.length).toBe(0);
  });
});
