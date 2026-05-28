import type { Redis } from 'ioredis';
import { redisKeys, markJobPendingForRetry, type Pool } from '@nexusqueue/shared';

export interface JanitorDeps {
  redis: Redis;
  pg: Pool;
  intervalMs?: number;
}

/**
 * Janitor - detects dead workers (missing heartbeat) and re-enqueues
 * their orphaned jobs back to the appropriate priority queues.
 */
export class Janitor {
  private interval: ReturnType<typeof setInterval> | null = null;
  private readonly intervalMs: number;
  private readonly redis: Redis;
  private readonly pg: Pool;

  constructor(deps: JanitorDeps) {
    this.redis = deps.redis;
    this.pg = deps.pg;
    this.intervalMs = deps.intervalMs ?? 30000;
  }

  start(): void {
    this.interval = setInterval(() => {
      void this.tick();
    }, this.intervalMs);
  }

  stop(): void {
    if (this.interval) {
      clearInterval(this.interval);
      this.interval = null;
    }
  }

  /** Run one janitor cycle. Public for testing. */
  async tick(): Promise<void> {
    // Get all registered workers.
    const workerIds = await this.redis.smembers(redisKeys.workerRegistry);

    for (const workerId of workerIds) {
      // Check if heartbeat exists.
      const exists = await this.redis.exists(redisKeys.heartbeat(workerId));
      if (exists) continue;

      // Worker is dead - recover orphaned jobs.
      const processingKey = redisKeys.processing(workerId);
      const orphanedJobs = await this.redis.lrange(processingKey, 0, -1);

      for (const jobId of orphanedJobs) {
        const hash = await this.redis.hgetall(redisKeys.job(jobId));
        const queueName = hash.queueName ?? 'default';
        const priority = hash.priority ?? 'normal';

        // Determine target queue based on priority.
        const targetList = priority === 'normal'
          ? redisKeys.queue(queueName)
          : redisKeys.queuePriority(queueName, priority);

        // Re-enqueue the job.
        await this.redis.lpush(targetList, jobId);

        // Reset job state.
        await this.redis.hset(redisKeys.job(jobId), {
          status: 'pending',
          startedAt: '',
          completedAt: '',
        });

        // Update Postgres.
        await markJobPendingForRetry(this.pg, jobId);
      }

      // Clean up dead worker state.
      await this.redis.del(processingKey);
      await this.redis.srem(redisKeys.workerRegistry, workerId);
      await this.redis.del(redisKeys.workerMeta(workerId));

      if (orphanedJobs.length > 0) {
        // eslint-disable-next-line no-console
        console.log(
          `[janitor] recovered ${orphanedJobs.length} orphaned jobs from dead worker ${workerId}`,
        );
      }
    }
  }
}
