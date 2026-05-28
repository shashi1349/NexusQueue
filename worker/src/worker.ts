import type { Redis } from 'ioredis';
import {
  redisKeys,
  markJobActive,
  markJobCompleted,
  markJobDlq,
  markJobPendingForRetry,
  type Pool,
} from '@nexusqueue/shared';
import { HandlerRegistry, type JobHandler } from './handlers.js';

/**
 * Worker -- the consumer side of NexusQueue.
 *
 * Phase 2 model: BLMOVE-based at-LEAST-once delivery with per-worker
 * processing lists, explicit ACK, exponential backoff retry, and DLQ.
 *
 * Why BLMOVE instead of BRPOP?
 *   - BRPOP atomically REMOVES the item from the queue. If the worker
 *     crashes between pop and completion, the job is lost.
 *   - BLMOVE atomically moves the item from the queue list to a
 *     per-worker "processing" list. If the worker crashes, a janitor
 *     can scan the processing list and re-enqueue stale items.
 *
 * On success: LREM from processing list (ACK) + mark completed.
 * On failure with retries remaining: LREM from processing list,
 *   exponential backoff, then re-push to queue.
 * On failure with no retries left: LREM from processing list,
 *   move to DLQ.
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

  private async loop(): Promise<void> {
    const queueKey = redisKeys.queue(this.deps.queue);
    const processingKey = redisKeys.processing(this.deps.workerId);
    while (!this.stopRequested) {
      try {
        const jobId = await this.blockingRedis.call(
          'BLMOVE', queueKey, processingKey, 'RIGHT', 'LEFT', '5',
        ) as string | null;
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

  private async processOne(jobId: string): Promise<void> {
    // Load the job hash from Redis (faster than going to Postgres).
    const hash = await this.deps.redis.hgetall(redisKeys.job(jobId));
    if (!hash || !hash.jobName) {
      // eslint-disable-next-line no-console
      console.warn(`[worker] job ${jobId} hash missing -- skipping`);
      return;
    }
    const jobName = hash.jobName;
    const handler = this.registry.get(jobName);

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
    // Remove from processing list first.
    await this.deps.redis.lrem(redisKeys.processing(this.deps.workerId), 1, jobId);

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
    } else {
      // Retry path: exponential backoff then re-enqueue.
      const delay = Math.min(1000 * Math.pow(2, attempts - 1), 30000);
      await markJobPendingForRetry(this.deps.pg, jobId);
      await this.deps.redis.hset(redisKeys.job(jobId), {
        status: 'pending',
        startedAt: '',
        completedAt: '',
        errorMessage: '',
      });
      setTimeout(() => {
        this.deps.redis.lpush(redisKeys.queue(queueName), jobId).catch(() => {});
      }, delay);
    }
  }
}
