import type { Redis } from 'ioredis';
import {
  redisKeys,
  markJobActive,
  markJobCompleted,
  markJobDlq,
  markJobDelayed,
  type Pool,
  type RateLimitConfig,
} from '@nexusqueue/shared';
import { HandlerRegistry, type JobHandler } from './handlers.js';
import { RateLimiter } from './rate-limiter.js';

/**
 * Worker -- the consumer side of NexusQueue.
 *
 * Phase 3 model: weighted fair priority pulling, rate limiting,
 * and retry via delayed sorted set (instead of setTimeout).
 */

export interface WorkerDeps {
  redis: Redis;
  pg: Pool;
  queue: string;
  workerId: string;
}

export class Worker {
  private readonly registry = new HandlerRegistry();
  private readonly blockingRedis: Redis;
  private running = false;
  private stopRequested = false;
  private currentLoop: Promise<void> | null = null;
  private pullIndex = 0;
  private rateLimits = new Map<string, RateLimitConfig>();
  private rateLimiter = new RateLimiter();

  constructor(private readonly deps: WorkerDeps) {
    // Dedicated client for BLMOVE so we don't block other commands.
    this.blockingRedis = deps.redis.duplicate();
  }

  register<TPayload, TResult>(
    jobName: string,
    handler: JobHandler<TPayload, TResult>,
  ): this {
    this.registry.register(jobName, handler);
    return this;
  }

  /** Set a rate limit configuration for a queue. */
  setRateLimit(queue: string, config: RateLimitConfig): this {
    this.rateLimits.set(queue, config);
    return this;
  }

  /** Starts the pull loop. Returns immediately; the loop runs in background. */
  start(): void {
    if (this.running) throw new Error('worker already started');
    this.running = true;
    this.currentLoop = this.loop();
  }

  /** Signal the loop to stop after the current iteration and await it. */
  async stop(): Promise<void> {
    this.stopRequested = true;
    if (this.currentLoop) await this.currentLoop;
    await this.blockingRedis.quit();
    this.running = false;
  }

  /**
   * Weighted fair priority pull pattern.
   * Every 10 pulls cycle: indices 0-5 try high, 6-8 try normal, 9 tries low.
   * If the target priority queue is empty, fall through to other priorities.
   * If all empty, do BLMOVE on normal queue with 5s timeout.
   *
   * Known limitation: When only high-priority jobs exist and the rotation points
   * to normal or low first, 1-2 wasted rpoplpush round-trips occur before finding
   * the high-priority job. This is functionally correct and only matters at very
   * high throughput where those extra Redis calls become significant.
   */
  private getPriorityOrder(): Array<'high' | 'normal' | 'low'> {
    const idx = this.pullIndex % 10;
    this.pullIndex++;
    if (idx <= 5) return ['high', 'normal', 'low'];
    if (idx <= 8) return ['normal', 'high', 'low'];
    return ['low', 'high', 'normal'];
  }

  private async loop(): Promise<void> {
    const processingKey = redisKeys.processing(this.deps.workerId);
    while (!this.stopRequested) {
      try {
        const jobId = await this.pullJob(processingKey);
        if (!jobId) continue;
        await this.processOne(jobId);
      } catch (err) {
        // Loop must never die from a transient error. Log + continue.
        // eslint-disable-next-line no-console
        console.error(`[worker:${this.deps.workerId}] loop error:`, err);
        // Tiny backoff so we don't hot-spin if Redis is down.
        await new Promise((r) => setTimeout(r, 500));
      }
    }
  }

  private async pullJob(processingKey: string): Promise<string | null> {
    const queueName = this.deps.queue;
    const priorities = this.getPriorityOrder();

    // Try non-blocking pull from priority queues.
    for (const priority of priorities) {
      const sourceList = priority === 'normal'
        ? redisKeys.queue(queueName)
        : redisKeys.queuePriority(queueName, priority);

      // Use rpoplpush (atomic move from source to processing list).
      const jobId = await this.deps.redis.rpoplpush(sourceList, processingKey);
      if (jobId) return jobId;
    }

    // All priority queues empty: blocking wait on normal queue.
    const jobId = await this.blockingRedis.call(
      'BLMOVE', redisKeys.queue(queueName), processingKey, 'RIGHT', 'LEFT', '5',
    ) as string | null;

    return jobId;
  }

  private async processOne(jobId: string): Promise<void> {
    // Load the job hash from Redis (faster than going to Postgres).
    const hash = await this.deps.redis.hgetall(redisKeys.job(jobId));
    if (!hash || !hash.jobName) {
      // eslint-disable-next-line no-console
      console.warn(`[worker] job ${jobId} hash missing -- skipping`);
      return;
    }
    const jobName = hash.jobName;
    const queueName = hash.queueName ?? this.deps.queue;
    const handler = this.registry.get(jobName);

    // Rate limit check before executing handler.
    const rateConfig = this.rateLimits.get(queueName);
    if (rateConfig) {
      const result = await this.rateLimiter.checkLimitFallback(
        this.deps.redis,
        queueName,
        rateConfig,
      );
      if (!result.allowed) {
        // Push the job back to the front of its priority queue and remove from
        // processing list atomically to avoid windows where job is in both places.
        const priority = hash.priority ?? 'normal';
        const targetList = priority === 'normal'
          ? redisKeys.queue(queueName)
          : redisKeys.queuePriority(queueName, priority);
        const multi = this.deps.redis.multi();
        multi.lrem(redisKeys.processing(this.deps.workerId), 1, jobId);
        multi.lpush(targetList, jobId);
        await multi.exec();
        await new Promise((r) => setTimeout(r, result.retryAfterMs));
        return;
      }
    }

    // Transition to 'active' in BOTH stores. Postgres first so an observer
    // never sees a Redis 'active' with no Postgres row.
    await markJobActive(this.deps.pg, jobId);
    await this.deps.redis.hset(redisKeys.job(jobId), {
      status: 'active',
      startedAt: new Date().toISOString(),
      attempts: String(Number(hash.attempts ?? '0') + 1),
    });

    if (!handler) {
      const msg = `no handler registered for jobName="${jobName}"`;
      await this.handleFailure(jobId, msg);
      return;
    }

    let payload: unknown = {};
    try {
      payload = hash.payload ? JSON.parse(hash.payload) : {};
    } catch {
      await this.handleFailure(jobId, 'payload JSON parse error');
      return;
    }

    const attempt = Number(hash.attempts ?? '0') + 1;
    try {
      await handler(payload, {
        jobId,
        jobName,
        attempt,
        workerId: this.deps.workerId,
      });
      await this.complete(jobId);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.handleFailure(jobId, message);
    }
  }

  private async complete(jobId: string): Promise<void> {
    await markJobCompleted(this.deps.pg, jobId);
    await this.deps.redis.hset(redisKeys.job(jobId), {
      status: 'completed',
      completedAt: new Date().toISOString(),
      errorMessage: '',
    });
    // ACK: remove from processing list.
    await this.deps.redis.lrem(redisKeys.processing(this.deps.workerId), 1, jobId);
  }

  private async handleFailure(jobId: string, errorMessage: string): Promise<void> {
    // Get current state from Redis hash.
    const hash = await this.deps.redis.hgetall(redisKeys.job(jobId));
    const attempts = Number(hash.attempts ?? '0');
    const maxAttempts = Number(hash.maxAttempts ?? '3');
    const queueName = hash.queueName ?? 'default';

    if (attempts >= maxAttempts) {
      // DLQ path: no more retries.
      await markJobDlq(this.deps.pg, jobId, errorMessage);
      await this.deps.redis.hset(redisKeys.job(jobId), {
        status: 'dlq',
        completedAt: new Date().toISOString(),
        errorMessage,
      });
      await this.deps.redis.lpush(redisKeys.dlq(queueName), jobId);
      // Remove from processing list after durable write.
      await this.deps.redis.lrem(redisKeys.processing(this.deps.workerId), 1, jobId);
    } else {
      // Retry path: ZADD to delayed sorted set with exponential backoff.
      // ZADD first so job is never lost if we crash between steps.
      const delay = Math.min(1000 * Math.pow(2, attempts - 1), 30000);
      const dueAt = Date.now() + delay;
      await markJobDelayed(this.deps.pg, jobId);
      await this.deps.redis.hset(redisKeys.job(jobId), {
        status: 'delayed',
        startedAt: '',
        completedAt: '',
        errorMessage: '',
      });
      await this.deps.redis.zadd(redisKeys.delayed(queueName), String(dueAt), jobId);
      // Remove from processing list only after ZADD succeeds.
      await this.deps.redis.lrem(redisKeys.processing(this.deps.workerId), 1, jobId);
    }
  }
}
