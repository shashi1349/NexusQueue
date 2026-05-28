import type { Redis } from 'ioredis';
import {
  redisKeys,
  markJobActive,
  markJobCompleted,
  markJobFailed,
  type Pool,
} from '@nexusqueue/shared';
import { HandlerRegistry, type JobHandler } from './handlers.js';

/**
 * Worker — the consumer side of NexusQueue.
 *
 * Phase 1 model: single in-flight job, BRPOP loop, at-MOST-once delivery.
 *
 * Why BRPOP and not LPOP-in-a-loop?
 *   - LPOP returns nil when the list is empty, forcing us to sleep+retry.
 *     That's polling: wastes CPU and adds enqueue->run latency.
 *   - BRPOP blocks server-side until an item arrives or the timeout fires,
 *     so we react in microseconds with zero busy-wait.
 *   - Why a 5-second timeout instead of 0 (block forever)? Because a
 *     SIGTERM arrives between iterations. With timeout=0 the worker
 *     would ignore shutdown until the next job. 5s is a tolerable
 *     drain window.
 *
 * Why a separate Redis client for BRPOP?
 *   - A blocking command holds the entire connection. If we reused the
 *     producer's client, no other command on it could run while we wait.
 *   - ioredis exposes `duplicate()` for exactly this.
 *
 * Why is this AT-MOST-ONCE in Phase 1?
 *   - BRPOP atomically removes the job from the list. If the worker
 *     crashes between the pop and the handler completing, the jobId
 *     is gone from Redis forever. The Postgres row stays in 'active'
 *     looking like a zombie.
 *   - Phase 2 fixes this with BRPOPLPUSH (move to a per-worker
 *     "processing" list), explicit ACK on success, and a janitor that
 *     reclaims jobs from dead workers' processing lists.
 *
 * Pedagogically: live with this gap until Phase 2 so you feel why those
 * features exist. Don't pre-build them.
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
    // Dedicated client for BRPOP so we don't block other commands.
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
    while (!this.stopRequested) {
      try {
        // BRPOP returns [key, value] or null on timeout.
        const result = await this.blockingRedis.brpop(queueKey, 5);
        if (!result) continue; // timeout — loop and re-check stopRequested
        const jobId = result[1];
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
      console.warn(`[worker] job ${jobId} hash missing — skipping`);
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
    });

    if (!handler) {
      const msg = `no handler registered for jobName="${jobName}"`;
      await this.fail(jobId, msg);
      return;
    }

    let payload: unknown = {};
    try {
      payload = hash.payload ? JSON.parse(hash.payload) : {};
    } catch {
      await this.fail(jobId, 'payload JSON parse error');
      return;
    }

    const attempt = Number(hash.attempts ?? '0') + 1; // matches markJobActive's increment
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
      await this.fail(jobId, message);
    }
  }

  private async complete(jobId: string): Promise<void> {
    await markJobCompleted(this.deps.pg, jobId);
    await this.deps.redis.hset(redisKeys.job(jobId), {
      status: 'completed',
      completedAt: new Date().toISOString(),
      errorMessage: '',
    });
  }

  private async fail(jobId: string, errorMessage: string): Promise<void> {
    await markJobFailed(this.deps.pg, jobId, errorMessage);
    await this.deps.redis.hset(redisKeys.job(jobId), {
      status: 'failed',
      completedAt: new Date().toISOString(),
      errorMessage,
    });
  }
}
