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
 * Phase 4 model: concurrent job processing, heartbeats, graceful shutdown.
 */

export interface WorkerDeps {
  redis: Redis;
  pg: Pool;
  queue: string;
  workerId: string;
  concurrency?: number;
}

export class Worker {
  private readonly registry = new HandlerRegistry();
  private readonly blockingRedis: Redis;
  private readonly concurrency: number;
  private running = false;
  private stopRequested = false;
  private currentLoop: Promise<void> | null = null;
  private pullIndex = 0;
  private rateLimits = new Map<string, RateLimitConfig>();
  private rateLimiter = new RateLimiter();
  private activeJobs = new Set<Promise<void>>();
  private heartbeatInterval: ReturnType<typeof setInterval> | null = null;

  constructor(private readonly deps: WorkerDeps) {
    // Dedicated client for BLMOVE so we don't block other commands.
    this.blockingRedis = deps.redis.duplicate();
    this.concurrency = deps.concurrency ?? 5;
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

  /** Starts the pull loop and registers the worker. Returns immediately; the loop runs in background. */
  async start(): Promise<void> {
    if (this.running) throw new Error('worker already started');
    this.running = true;
    this.stopRequested = false;

    // Register in worker set.
    await this.deps.redis.sadd(redisKeys.workerRegistry, this.deps.workerId);

    // Initial heartbeat.
    await this.deps.redis.set(
      redisKeys.heartbeat(this.deps.workerId),
      String(Date.now()),
      'EX',
      15,
    );

    // Worker metadata.
    await this.deps.redis.hset(redisKeys.workerMeta(this.deps.workerId), {
      status: 'active',
      queue: this.deps.queue,
      startedAt: new Date().toISOString(),
      currentJobs: '0',
    });

    // Heartbeat interval.
    this.heartbeatInterval = setInterval(() => {
      void this.writeHeartbeat();
    }, 5000);

    this.currentLoop = this.loop();
  }

  /** Signal the loop to stop, drain in-flight jobs, clean up keys. */
  async stop(): Promise<void> {
    this.stopRequested = true;

    // Update status to draining.
    await this.deps.redis.hset(redisKeys.workerMeta(this.deps.workerId), {
      status: 'draining',
    });

    // Wait for in-flight jobs with 30s timeout.
    if (this.activeJobs.size > 0) {
      const drainPromise = Promise.allSettled([...this.activeJobs]);
      const timeoutPromise = new Promise<'timeout'>((resolve) =>
        setTimeout(() => resolve('timeout'), 30000),
      );
      const result = await Promise.race([drainPromise, timeoutPromise]);
      if (result === 'timeout') {
        // eslint-disable-next-line no-console
        console.warn(
          `[worker:${this.deps.workerId}] shutdown timeout: ${this.activeJobs.size} jobs still in-flight`,
        );
      }
    }

    // Wait for loop to exit.
    if (this.currentLoop) await this.currentLoop;

    // Deregister from worker set.
    await this.deps.redis.srem(redisKeys.workerRegistry, this.deps.workerId);

    // Delete heartbeat and metadata keys.
    await this.deps.redis.del(redisKeys.heartbeat(this.deps.workerId));
    await this.deps.redis.del(redisKeys.workerMeta(this.deps.workerId));

    // Clear heartbeat interval.
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }

    await this.blockingRedis.quit();
    this.running = false;
  }

  /**
   * Process all available jobs without blocking (for testing).
   * Uses rpoplpush only (no BLMOVE), processes up to concurrency jobs at a time.
   */
  async processAvailable(): Promise<void> {
    const processingKey = redisKeys.processing(this.deps.workerId);
    let foundJob = true;
    while (foundJob) {
      foundJob = false;
      // Wait for slots to be available.
      while (this.activeJobs.size >= this.concurrency) {
        await Promise.race([...this.activeJobs]);
      }

      const jobId = await this.pullJobNonBlocking(processingKey);
      if (jobId) {
        foundJob = true;
        const jobPromise = this.processOne(jobId).then(
          () => { this.activeJobs.delete(jobPromise); },
          () => { this.activeJobs.delete(jobPromise); },
        );
        this.activeJobs.add(jobPromise);
      }
    }
    // Wait for remaining active jobs to finish.
    if (this.activeJobs.size > 0) {
      await Promise.allSettled([...this.activeJobs]);
    }
  }

  private async writeHeartbeat(): Promise<void> {
    await this.deps.redis.set(
      redisKeys.heartbeat(this.deps.workerId),
      String(Date.now()),
      'EX',
      15,
    );
    await this.deps.redis.hset(redisKeys.workerMeta(this.deps.workerId), {
      currentJobs: String(this.activeJobs.size),
    });
  }

  /**
   * Weighted fair priority pull pattern.
   * Every 10 pulls cycle: indices 0-5 try high, 6-8 try normal, 9 tries low.
   * If the target priority queue is empty, fall through to other priorities.
   * If all empty, do BLMOVE on normal queue with 5s timeout.
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
        // Only pull when we have available concurrency slots.
        if (this.activeJobs.size >= this.concurrency) {
          await new Promise((r) => setTimeout(r, 50));
          continue;
        }

        const jobId = await this.pullJob(processingKey);
        if (!jobId) continue;

        const jobPromise = this.processOne(jobId).then(
          () => { this.activeJobs.delete(jobPromise); },
          () => { this.activeJobs.delete(jobPromise); },
        );
        this.activeJobs.add(jobPromise);
      } catch (err) {
        // Loop must never die from a transient error. Log + continue.
        // eslint-disable-next-line no-console
        console.error(`[worker:${this.deps.workerId}] loop error:`, err);
        // Tiny backoff so we don't hot-spin if Redis is down.
        await new Promise((r) => setTimeout(r, 500));
      }
    }
  }

  private async pullJobNonBlocking(processingKey: string): Promise<string | null> {
    const queueName = this.deps.queue;
    const priorities = this.getPriorityOrder();

    for (const priority of priorities) {
      const sourceList = priority === 'normal'
        ? redisKeys.queue(queueName)
        : redisKeys.queuePriority(queueName, priority);

      const jobId = await this.deps.redis.rpoplpush(sourceList, processingKey);
      if (jobId) return jobId;
    }
    return null;
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
