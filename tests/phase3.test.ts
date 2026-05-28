/**
 * Phase 3 integration tests.
 *
 * Tests cover: delayed jobs, scheduler promotion, priority routing,
 * weighted fair pull, cron scheduling, rate limiting, and retry via
 * delayed sorted set.
 *
 * Uses ioredis-mock + mock pg Pool (same pattern as phase1/phase2).
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';
import RedisMock from 'ioredis-mock';
import { redisKeys } from '@nexusqueue/shared';
import { Producer } from '../server/src/producer.js';
import { Worker } from '../worker/src/worker.js';
import { Scheduler } from '../worker/src/scheduler.js';
import { CronManager } from '../worker/src/cron.js';
import { RateLimiter } from '../worker/src/rate-limiter.js';

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

describe('Phase 3 -- Delayed jobs', () => {
  let redis: InstanceType<typeof RedisMock>;
  let pool: ReturnType<typeof createMockPool>;
  let producer: Producer;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    pool = createMockPool();
    producer = new Producer({ redis: redis as any, pg: pool.pool });
  });

  it('enqueue with delay ZADDs to delayed sorted set instead of LPUSH', async () => {
    const id = await producer.enqueue('delayed-job', { x: 1 }, {
      queue: 'myq',
      delay: 5000,
    });

    // Job should NOT be in the queue list.
    const queueList = await redis.lrange(redisKeys.queue('myq'), 0, -1);
    expect(queueList).not.toContain(id);

    // Job should be in the delayed sorted set.
    const delayedMembers = await redis.zrangebyscore(redisKeys.delayed('myq'), '-inf', '+inf');
    expect(delayedMembers).toContain(id);

    // Redis hash status is 'delayed'.
    const hash = await redis.hgetall(redisKeys.job(id));
    expect(hash.status).toBe('delayed');
  });

  it('delayed job has correct score (approx Date.now() + delay)', async () => {
    const before = Date.now();
    const id = await producer.enqueue('delayed-job', {}, {
      queue: 'q1',
      delay: 3000,
    });
    const after = Date.now();

    const score = await redis.zscore(redisKeys.delayed('q1'), id);
    const scoreNum = Number(score);
    expect(scoreNum).toBeGreaterThanOrEqual(before + 3000);
    expect(scoreNum).toBeLessThanOrEqual(after + 3000);
  });

  it('delayed job calls insertDelayedJob in Postgres', async () => {
    await producer.enqueue('delayed-job', { foo: 'bar' }, {
      queue: 'dq',
      delay: 1000,
    });

    // Should have an INSERT with status='delayed'.
    const inserts = pool.queries.filter((q) => q.text.includes('INSERT INTO jobs'));
    expect(inserts.length).toBe(1);
    expect(inserts[0]!.text).toContain("'delayed'");
  });
});

describe('Phase 3 -- Scheduler promotes delayed jobs', () => {
  let redis: InstanceType<typeof RedisMock>;
  let pool: ReturnType<typeof createMockPool>;
  let scheduler: Scheduler;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    pool = createMockPool();
    scheduler = new Scheduler({ redis: redis as any, pg: pool.pool });
  });

  it('promotes a delayed job with past score to the queue list', async () => {
    const jobId = 'delayed-promote-001';
    const queueName = 'testq';

    // Register the queue.
    await redis.sadd(redisKeys.queueRegistry, queueName);

    // Seed the job hash.
    await redis.hset(redisKeys.job(jobId), {
      id: jobId,
      queueName,
      jobName: 'test-job',
      payload: '{}',
      status: 'delayed',
      attempts: '0',
      maxAttempts: '3',
      priority: 'normal',
      errorMessage: '',
      createdAt: new Date().toISOString(),
      startedAt: '',
      completedAt: '',
    });

    // Add to delayed set with a past score (already due).
    await redis.zadd(redisKeys.delayed(queueName), String(Date.now() - 1000), jobId);

    // Run one scheduler tick.
    await scheduler.tick();

    // Job should now be in the queue list.
    const queueList = await redis.lrange(redisKeys.queue(queueName), 0, -1);
    expect(queueList).toContain(jobId);

    // Job should be removed from the delayed set.
    const delayedMembers = await redis.zrangebyscore(redisKeys.delayed(queueName), '-inf', '+inf');
    expect(delayedMembers).not.toContain(jobId);

    // Status updated to 'pending'.
    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('pending');
  });

  it('promotes a high-priority delayed job to the priority queue', async () => {
    const jobId = 'delayed-high-001';
    const queueName = 'prioq';

    await redis.sadd(redisKeys.queueRegistry, queueName);
    await redis.hset(redisKeys.job(jobId), {
      id: jobId,
      queueName,
      jobName: 'prio-job',
      payload: '{}',
      status: 'delayed',
      attempts: '0',
      maxAttempts: '3',
      priority: 'high',
      errorMessage: '',
      createdAt: new Date().toISOString(),
      startedAt: '',
      completedAt: '',
    });
    await redis.zadd(redisKeys.delayed(queueName), String(Date.now() - 500), jobId);

    await scheduler.tick();

    // Should be in the high priority queue.
    const highList = await redis.lrange(redisKeys.queuePriority(queueName, 'high'), 0, -1);
    expect(highList).toContain(jobId);

    // Not in the normal queue.
    const normalList = await redis.lrange(redisKeys.queue(queueName), 0, -1);
    expect(normalList).not.toContain(jobId);
  });

  it('does not promote jobs with future scores', async () => {
    const jobId = 'delayed-future-001';
    const queueName = 'futq';

    await redis.sadd(redisKeys.queueRegistry, queueName);
    await redis.hset(redisKeys.job(jobId), {
      id: jobId,
      queueName,
      jobName: 'future-job',
      payload: '{}',
      status: 'delayed',
      attempts: '0',
      maxAttempts: '3',
      priority: 'normal',
      errorMessage: '',
      createdAt: new Date().toISOString(),
      startedAt: '',
      completedAt: '',
    });
    await redis.zadd(redisKeys.delayed(queueName), String(Date.now() + 60000), jobId);

    await scheduler.tick();

    // Should still be in the delayed set.
    const delayedMembers = await redis.zrangebyscore(redisKeys.delayed(queueName), '-inf', '+inf');
    expect(delayedMembers).toContain(jobId);

    // Not in the queue list.
    const queueList = await redis.lrange(redisKeys.queue(queueName), 0, -1);
    expect(queueList).not.toContain(jobId);
  });
});

describe('Phase 3 -- Priority routing', () => {
  let redis: InstanceType<typeof RedisMock>;
  let pool: ReturnType<typeof createMockPool>;
  let producer: Producer;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    pool = createMockPool();
    producer = new Producer({ redis: redis as any, pg: pool.pool });
  });

  it('enqueue with priority=high pushes to nexus:queue:{name}:high', async () => {
    const id = await producer.enqueue('high-job', {}, {
      queue: 'pq',
      priority: 'high',
    });

    const highList = await redis.lrange(redisKeys.queuePriority('pq', 'high'), 0, -1);
    expect(highList).toContain(id);

    // Not in the normal queue.
    const normalList = await redis.lrange(redisKeys.queue('pq'), 0, -1);
    expect(normalList).not.toContain(id);
  });

  it('enqueue with priority=low pushes to nexus:queue:{name}:low', async () => {
    const id = await producer.enqueue('low-job', {}, {
      queue: 'pq',
      priority: 'low',
    });

    const lowList = await redis.lrange(redisKeys.queuePriority('pq', 'low'), 0, -1);
    expect(lowList).toContain(id);
  });

  it('enqueue with priority=normal (default) pushes to nexus:queue:{name}', async () => {
    const id = await producer.enqueue('normal-job', {}, {
      queue: 'pq',
    });

    const normalList = await redis.lrange(redisKeys.queue('pq'), 0, -1);
    expect(normalList).toContain(id);
  });

  it('stores priority in the Redis job hash', async () => {
    const id = await producer.enqueue('prio-job', {}, {
      queue: 'pq',
      priority: 'high',
    });

    const hash = await redis.hgetall(redisKeys.job(id));
    expect(hash.priority).toBe('high');
  });
});

describe('Phase 3 -- Weighted priority pull', () => {
  let redis: InstanceType<typeof RedisMock>;
  let pool: ReturnType<typeof createMockPool>;
  let worker: Worker;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    pool = createMockPool();
    worker = new Worker({
      redis: redis as any,
      pg: pool.pool,
      queue: 'wpq',
      workerId: 'w1',
    });
    worker.register('test-handler', async () => 'done');
  });

  it('pulls from high priority queue first on early indices', async () => {
    // Seed jobs in all priority queues.
    const highJobId = 'high-001';
    const normalJobId = 'normal-001';

    await redis.lpush(redisKeys.queuePriority('wpq', 'high'), highJobId);
    await redis.lpush(redisKeys.queue('wpq'), normalJobId);

    // Seed job hashes.
    for (const jid of [highJobId, normalJobId]) {
      await redis.hset(redisKeys.job(jid), {
        id: jid,
        queueName: 'wpq',
        jobName: 'test-handler',
        payload: '{}',
        status: 'pending',
        attempts: '0',
        maxAttempts: '3',
        priority: jid.includes('high') ? 'high' : 'normal',
        errorMessage: '',
        createdAt: new Date().toISOString(),
        startedAt: '',
        completedAt: '',
      });
    }

    // Use pullJob directly (private method accessed for testing).
    const processingKey = redisKeys.processing('w1');
    const firstJob = await (worker as any).pullJob(processingKey);

    // First pull (index 0) should try high first.
    expect(firstJob).toBe(highJobId);
  });
});

describe('Phase 3 -- Cron scheduling', () => {
  let redis: InstanceType<typeof RedisMock>;
  let cronManager: CronManager;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    cronManager = new CronManager(redis as any);
  });

  it('addJob stores definition and schedules next occurrence', async () => {
    await cronManager.addJob({
      cronId: 'cron-001',
      jobName: 'daily-report',
      payload: { type: 'daily' },
      cronExpression: '0 9 * * *', // Every day at 9 AM
      queue: 'reports',
    });

    // Definition stored.
    const def = await redis.hgetall(redisKeys.cronDef('cron-001'));
    expect(def.jobName).toBe('daily-report');
    expect(def.cronExpression).toBe('0 9 * * *');
    expect(def.queue).toBe('reports');

    // Schedule entry exists.
    const members = await redis.zrangebyscore(redisKeys.cronSchedule, '-inf', '+inf');
    expect(members).toContain('cron-001');

    // Score is in the future (next 9 AM).
    const score = await redis.zscore(redisKeys.cronSchedule, 'cron-001');
    expect(Number(score)).toBeGreaterThan(Date.now());
  });

  it('removeJob removes both definition and schedule entry', async () => {
    await cronManager.addJob({
      cronId: 'cron-remove-001',
      jobName: 'cleanup',
      cronExpression: '*/5 * * * *',
    });

    await cronManager.removeJob('cron-remove-001');

    const def = await redis.hgetall(redisKeys.cronDef('cron-remove-001'));
    expect(Object.keys(def).length).toBe(0);

    const members = await redis.zrangebyscore(redisKeys.cronSchedule, '-inf', '+inf');
    expect(members).not.toContain('cron-remove-001');
  });

  it('listJobs returns all registered cron jobs', async () => {
    await cronManager.addJob({
      cronId: 'cron-list-1',
      jobName: 'job1',
      cronExpression: '0 * * * *',
    });
    await cronManager.addJob({
      cronId: 'cron-list-2',
      jobName: 'job2',
      cronExpression: '30 * * * *',
    });

    const jobs = await cronManager.listJobs();
    expect(jobs.length).toBe(2);
    const ids = jobs.map((j) => j.cronId);
    expect(ids).toContain('cron-list-1');
    expect(ids).toContain('cron-list-2');
  });
});

describe('Phase 3 -- Cron re-scheduling via Scheduler', () => {
  let redis: InstanceType<typeof RedisMock>;
  let pool: ReturnType<typeof createMockPool>;
  let scheduler: Scheduler;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    pool = createMockPool();
    scheduler = new Scheduler({ redis: redis as any, pg: pool.pool });
  });

  it('fires a due cron job and reschedules with new score', async () => {
    const cronId = 'cron-fire-001';

    // Store the cron definition.
    await redis.hset(redisKeys.cronDef(cronId), {
      cronId,
      jobName: 'scheduled-task',
      payload: JSON.stringify({ msg: 'hello' }),
      cronExpression: '* * * * *', // Every minute
      queue: 'cronq',
      maxAttempts: '3',
      priority: 'normal',
    });

    // Schedule with a past score (already due).
    await redis.zadd(redisKeys.cronSchedule, String(Date.now() - 1000), cronId);

    // Register the queue so scheduler can find it.
    await redis.sadd(redisKeys.queueRegistry, 'cronq');

    await scheduler.tick();

    // A job should have been enqueued in the queue list.
    const queueList = await redis.lrange(redisKeys.queue('cronq'), 0, -1);
    expect(queueList.length).toBe(1);

    // The cron entry should be rescheduled with a future score.
    const newScore = await redis.zscore(redisKeys.cronSchedule, cronId);
    expect(Number(newScore)).toBeGreaterThan(Date.now());
  });
});

describe('Phase 3 -- Rate limiter (fallback logic)', () => {
  let redis: InstanceType<typeof RedisMock>;
  let rateLimiter: RateLimiter;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    rateLimiter = new RateLimiter();
  });

  it('allows first call when tokens are available', async () => {
    const result = await rateLimiter.checkLimitFallback(redis as any, 'rl-queue', {
      maxTokens: 5,
      refillRate: 1,
    });
    expect(result.allowed).toBe(true);
    expect(result.retryAfterMs).toBe(0);
  });

  it('denies call when tokens are exhausted', async () => {
    const config = { maxTokens: 2, refillRate: 1 };

    // Consume all tokens.
    await rateLimiter.checkLimitFallback(redis as any, 'rl-queue', config);
    await rateLimiter.checkLimitFallback(redis as any, 'rl-queue', config);

    // Next call should be denied.
    const result = await rateLimiter.checkLimitFallback(redis as any, 'rl-queue', config);
    expect(result.allowed).toBe(false);
    expect(result.retryAfterMs).toBeGreaterThan(0);
  });

  it('refills tokens over time', async () => {
    const config = { maxTokens: 1, refillRate: 10 }; // 10 tokens/sec

    // Consume the one token.
    await rateLimiter.checkLimitFallback(redis as any, 'rl-queue', config);

    // Manually set lastRefill to 200ms ago so that 2 tokens have refilled.
    const key = redisKeys.rateLimit('rl-queue');
    const now = Date.now();
    await redis.hset(key, { tokens: '0', lastRefill: String(now - 200) });

    // Should now be allowed (0 + 0.2s * 10 tokens/s = 2 tokens).
    const result = await rateLimiter.checkLimitFallback(redis as any, 'rl-queue', config);
    expect(result.allowed).toBe(true);
  });

  it('returns correct retryAfterMs when denied', async () => {
    const config = { maxTokens: 1, refillRate: 2 }; // 2 tokens/sec

    // Consume the one token.
    await rateLimiter.checkLimitFallback(redis as any, 'rl-queue2', config);

    // Next call: 0 tokens, need 1, refillRate=2 -> retryAfter = ceil(1/2 * 1000) = 500ms
    const result = await rateLimiter.checkLimitFallback(redis as any, 'rl-queue2', config);
    expect(result.allowed).toBe(false);
    expect(result.retryAfterMs).toBe(500);
  });
});

describe('Phase 3 -- Retry uses delayed sorted set', () => {
  let redis: InstanceType<typeof RedisMock>;
  let pool: ReturnType<typeof createMockPool>;
  let worker: Worker;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    pool = createMockPool();
    worker = new Worker({
      redis: redis as any,
      pg: pool.pool,
      queue: 'retryq',
      workerId: 'w1',
    });
  });

  it('handleFailure ZADDs job to delayed set when retries remain', async () => {
    const jobId = 'retry-delayed-001';

    await redis.hset(redisKeys.job(jobId), {
      id: jobId,
      queueName: 'retryq',
      jobName: 'flaky-task',
      payload: '{}',
      status: 'active',
      attempts: '1',
      maxAttempts: '3',
      priority: 'normal',
      errorMessage: '',
      createdAt: new Date().toISOString(),
      startedAt: new Date().toISOString(),
      completedAt: '',
    });
    await redis.lpush(redisKeys.processing('w1'), jobId);

    worker.register('flaky-task', async () => {
      throw new Error('fail');
    });

    await (worker as any).processOne(jobId);

    // Job should be in delayed sorted set.
    const delayedMembers = await redis.zrangebyscore(redisKeys.delayed('retryq'), '-inf', '+inf');
    expect(delayedMembers).toContain(jobId);

    // Status should be 'delayed'.
    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('delayed');

    // Job should NOT be in the queue list.
    const queueList = await redis.lrange(redisKeys.queue('retryq'), 0, -1);
    expect(queueList).not.toContain(jobId);

    // Postgres received markJobDelayed call.
    const pgTexts = pool.queries.map((q) => q.text);
    expect(pgTexts.some((t) => t.includes("status = 'delayed'"))).toBe(true);
  });

  it('handleFailure moves to DLQ when maxAttempts reached', async () => {
    const jobId = 'retry-dlq-001';

    await redis.hset(redisKeys.job(jobId), {
      id: jobId,
      queueName: 'retryq',
      jobName: 'doomed-task',
      payload: '{}',
      status: 'active',
      attempts: '3',
      maxAttempts: '3',
      priority: 'normal',
      errorMessage: '',
      createdAt: new Date().toISOString(),
      startedAt: new Date().toISOString(),
      completedAt: '',
    });
    await redis.lpush(redisKeys.processing('w1'), jobId);

    worker.register('doomed-task', async () => {
      throw new Error('fatal');
    });

    await (worker as any).processOne(jobId);

    // Job should be in DLQ.
    const dlqList = await redis.lrange(redisKeys.dlq('retryq'), 0, -1);
    expect(dlqList).toContain(jobId);

    // Status should be 'dlq'.
    const hash = await redis.hgetall(redisKeys.job(jobId));
    expect(hash.status).toBe('dlq');

    // NOT in delayed set.
    const delayedMembers = await redis.zrangebyscore(redisKeys.delayed('retryq'), '-inf', '+inf');
    expect(delayedMembers).not.toContain(jobId);
  });
});
