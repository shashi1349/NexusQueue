/**
 * Phase 2 integration tests.
 *
 * Tests cover: ACK (processing list removal), retry with exponential
 * backoff, dead letter queue, and idempotency keys.
 *
 * Uses ioredis-mock + mock pg Pool (same pattern as phase1.test.ts).
 * Worker logic is exercised via processOne() directly since ioredis-mock
 * does not support BLMOVE.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import RedisMock from 'ioredis-mock';
import { redisKeys } from '@nexusqueue/shared';
import { Producer } from '../server/src/producer.js';
import { Worker } from '../worker/src/worker.js';

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

async function seedJob(
  redis: any,
  jobId: string,
  jobName: string,
  payload: unknown,
  opts?: { attempts?: string; maxAttempts?: string; queueName?: string },
) {
  await redis.hset(redisKeys.job(jobId), {
    id: jobId,
    queueName: opts?.queueName ?? 'test',
    jobName,
    payload: JSON.stringify(payload),
    status: 'pending',
    attempts: opts?.attempts ?? '0',
    maxAttempts: opts?.maxAttempts ?? '3',
    errorMessage: '',
    createdAt: new Date().toISOString(),
    startedAt: '',
    completedAt: '',
  });
}

describe('Phase 2 -- ACK (processing list removal)', () => {
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

  it('processOne removes jobId from processing list on success (ACK)', async () => {
    const jobId = 'ack-success-001';
    await seedJob(redis, jobId, 'greet', { name: 'Ada' });
    worker.register('greet', async () => 'done');

    // Simulate BLMOVE having placed the jobId on the processing list.
    await redis.lpush(redisKeys.processing('w1'), jobId);

    await (worker as any).processOne(jobId);

    // Processing list should be empty (ACK removes the entry).
    const processingList = await redis.lrange(redisKeys.processing('w1'), 0, -1);
    expect(processingList).toEqual([]);

    // Redis hash should be completed.
    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('completed');
  });

  it('processOne removes jobId from processing list on failure', async () => {
    const jobId = 'ack-fail-001';
    await seedJob(redis, jobId, 'boom', {}, { maxAttempts: '1', attempts: '0' });
    worker.register('boom', async () => {
      throw new Error('kaboom');
    });

    // Simulate BLMOVE.
    await redis.lpush(redisKeys.processing('w1'), jobId);

    await (worker as any).processOne(jobId);

    // Processing list should be empty.
    const processingList = await redis.lrange(redisKeys.processing('w1'), 0, -1);
    expect(processingList).toEqual([]);
  });
});

describe('Phase 2 -- Retry with exponential backoff', () => {
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

  it('retries job with exponential backoff when attempts < maxAttempts', async () => {
    const jobId = 'retry-001';
    await seedJob(redis, jobId, 'flaky', { x: 1 }, { maxAttempts: '3', attempts: '0' });
    worker.register('flaky', async () => {
      throw new Error('transient');
    });

    // Simulate BLMOVE.
    await redis.lpush(redisKeys.processing('w1'), jobId);

    await (worker as any).processOne(jobId);

    // Redis hash: status is 'delayed', attempts incremented to '1'.
    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('delayed');
    expect(hash.attempts).toBe('1');

    // Job is in the delayed sorted set (not the queue list).
    const delayedMembers = await redis.zrangebyscore(redisKeys.delayed('test'), '-inf', '+inf');
    expect(delayedMembers).toContain(jobId);

    // Job is NOT in the queue list.
    const queueList = await redis.lrange(redisKeys.queue('test'), 0, -1);
    expect(queueList).not.toContain(jobId);

    // Postgres received a query for marking delayed.
    const pgTexts = pool.queries.map((q) => q.text);
    expect(pgTexts.some((t) => t.includes("status = 'delayed'"))).toBe(true);
  });

  it('second retry uses longer backoff delay (higher score)', async () => {
    const jobId = 'retry-002';
    // attempts='1' means this is the second attempt.
    await seedJob(redis, jobId, 'flaky', {}, { maxAttempts: '3', attempts: '1' });
    worker.register('flaky', async () => {
      throw new Error('transient again');
    });

    // Simulate BLMOVE.
    await redis.lpush(redisKeys.processing('w1'), jobId);

    const before = Date.now();
    await (worker as any).processOne(jobId);
    const after = Date.now();

    // Status should be 'delayed', attempts incremented to '2'.
    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('delayed');
    expect(hash.attempts).toBe('2');

    // Job is in the delayed sorted set.
    const delayedMembers = await redis.zrangebyscore(redisKeys.delayed('test'), '-inf', '+inf');
    expect(delayedMembers).toContain(jobId);

    // Verify the score is approximately now + 2000ms (1000 * 2^1).
    const score = await redis.zscore(redisKeys.delayed('test'), jobId);
    const scoreNum = Number(score);
    expect(scoreNum).toBeGreaterThanOrEqual(before + 2000);
    expect(scoreNum).toBeLessThanOrEqual(after + 2000);
  });
});

describe('Phase 2 -- Dead Letter Queue', () => {
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

  it('moves job to DLQ when max attempts exhausted', async () => {
    const jobId = 'dlq-001';
    // maxAttempts=2, attempts=1 means this is the last allowed attempt.
    await seedJob(redis, jobId, 'doomed', {}, { maxAttempts: '2', attempts: '1' });
    worker.register('doomed', async () => {
      throw new Error('fatal');
    });

    // Simulate BLMOVE.
    await redis.lpush(redisKeys.processing('w1'), jobId);

    await (worker as any).processOne(jobId);

    // Redis hash status is 'dlq'.
    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('dlq');

    // jobId is in the DLQ list.
    const dlqList = await redis.lrange(redisKeys.dlq('test'), 0, -1);
    expect(dlqList).toContain(jobId);

    // jobId is NOT in the queue list (not re-enqueued).
    const queueList = await redis.lrange(redisKeys.queue('test'), 0, -1);
    expect(queueList).not.toContain(jobId);

    // Postgres received a DLQ status update.
    const pgTexts = pool.queries.map((q) => q.text);
    expect(pgTexts.some((t) => t.includes("status = 'dlq'"))).toBe(true);

    // Processing list is empty.
    const processingList = await redis.lrange(redisKeys.processing('w1'), 0, -1);
    expect(processingList).toEqual([]);
  });

  it('moves job to DLQ when maxAttempts is 1 (no retries at all)', async () => {
    const jobId = 'dlq-002';
    await seedJob(redis, jobId, 'once', {}, { maxAttempts: '1', attempts: '0' });
    worker.register('once', async () => {
      throw new Error('first and last');
    });

    // Simulate BLMOVE.
    await redis.lpush(redisKeys.processing('w1'), jobId);

    await (worker as any).processOne(jobId);

    // Redis hash status is 'dlq'.
    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('dlq');

    // jobId is in the DLQ list.
    const dlqList = await redis.lrange(redisKeys.dlq('test'), 0, -1);
    expect(dlqList).toContain(jobId);
  });
});

describe('Phase 2 -- Idempotency keys', () => {
  let redis: InstanceType<typeof RedisMock>;
  let pool: ReturnType<typeof createMockPool>;
  let producer: Producer;

  beforeEach(() => {
    redis = new RedisMock();
    pool = createMockPool();
    producer = new Producer({ redis: redis as any, pg: pool.pool });
  });

  it('returns existing jobId for duplicate idempotency key', async () => {
    const jobId1 = await producer.enqueue('test-job', { x: 1 }, { idempotencyKey: 'idem-1' });
    const jobId2 = await producer.enqueue('test-job', { x: 2 }, { idempotencyKey: 'idem-1' });

    // Same ID returned.
    expect(jobId1).toBe(jobId2);

    // Only 1 INSERT INTO jobs query was made.
    const inserts = pool.queries.filter((q) => q.text.includes('INSERT INTO jobs'));
    expect(inserts.length).toBe(1);

    // Redis queue list has only 1 entry.
    const queueList = await redis.lrange(redisKeys.queue('default'), 0, -1);
    expect(queueList.length).toBe(1);
  });

  it('does not set idempotency key when none provided', async () => {
    const jobId = await producer.enqueue('test-job', {});

    // No idempotency key should be stored.
    // The idempotency key for this job would be at redisKeys.idempotency(''),
    // but more importantly, no idem key maps to this jobId.
    // Verify by checking that no key in the idem namespace holds the jobId.
    const idemValue = await redis.get(redisKeys.idempotency(''));
    expect(idemValue).toBeNull();

    // The enqueue should still succeed.
    expect(jobId).toBeTruthy();
  });

  it('idempotency key is stored with TTL', async () => {
    const jobId = await producer.enqueue('test-job', {}, { idempotencyKey: 'ttl-test' });

    // The idempotency key should hold the jobId.
    const stored = await redis.get(redisKeys.idempotency('ttl-test'));
    expect(stored).toBe(jobId);

    // TTL should be > 0 (set with EX 86400).
    const ttl = await redis.ttl(redisKeys.idempotency('ttl-test'));
    expect(ttl).toBeGreaterThan(0);
  });
});
