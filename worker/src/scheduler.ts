import type { Redis } from 'ioredis';
import { redisKeys, type Pool } from '@nexusqueue/shared';

/**
 * Scheduler -- polling loop that promotes delayed jobs and fires cron jobs.
 *
 * Runs on a 1-second interval. On each tick:
 *   (a) For each known queue, check the delayed sorted set for due jobs
 *       and move them to the appropriate priority queue list.
 *   (b) Check the cron schedule sorted set for due cron entries, enqueue
 *       them, and reschedule the next occurrence.
 */

export interface SchedulerDeps {
  redis: Redis;
  pg: Pool;
}

export class Scheduler {
  private timer: ReturnType<typeof setInterval> | null = null;
  private running = false;

  constructor(private readonly deps: SchedulerDeps) {}

  /** Start the polling loop. */
  start(): void {
    if (this.running) return;
    this.running = true;
    this.timer = setInterval(() => {
      void this.tick();
    }, 1000);
  }

  /** Stop the polling loop. */
  stop(): void {
    this.running = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /** Execute one tick of the scheduler (exposed for testing). */
  async tick(): Promise<void> {
    await this.promoteDelayedJobs();
    await this.fireCronJobs();
  }

  private async promoteDelayedJobs(): Promise<void> {
    // Get all known queues.
    const queues = await this.deps.redis.smembers(redisKeys.queueRegistry);
    const now = Date.now();

    for (const queue of queues) {
      const delayedKey = redisKeys.delayed(queue);
      // Find all jobs whose score <= now (i.e., they are due).
      const dueJobs = await this.deps.redis.zrangebyscore(delayedKey, '-inf', String(now));

      for (const jobId of dueJobs) {
        // Read the job's priority to route to the correct queue.
        const priority = await this.deps.redis.hget(redisKeys.job(jobId), 'priority') ?? 'normal';
        const targetList = priority === 'normal'
          ? redisKeys.queue(queue)
          : redisKeys.queuePriority(queue, priority);

        // Atomic: remove from delayed, push to queue, update status.
        const multi = this.deps.redis.multi();
        multi.zrem(delayedKey, jobId);
        multi.lpush(targetList, jobId);
        multi.hset(redisKeys.job(jobId), { status: 'pending' });
        await multi.exec();
      }
    }
  }

  private async fireCronJobs(): Promise<void> {
    const now = Date.now();
    const dueEntries = await this.deps.redis.zrangebyscore(
      redisKeys.cronSchedule, '-inf', String(now),
    );

    for (const cronId of dueEntries) {
      const defRaw = await this.deps.redis.hgetall(redisKeys.cronDef(cronId));
      if (!defRaw || !defRaw.cronExpression) continue;

      // Dynamically import cron-parser to compute next occurrence.
      const { CronExpressionParser } = await import('cron-parser');
      const interval = CronExpressionParser.parse(defRaw.cronExpression);
      const next = interval.next();
      const nextMs = next.getTime();

      // Re-schedule with new score.
      await this.deps.redis.zadd(redisKeys.cronSchedule, String(nextMs), cronId);

      // Enqueue the cron job: push to the target queue.
      const queueName = defRaw.queue ?? 'default';
      const jobName = defRaw.jobName ?? cronId;
      const payload = defRaw.payload ? JSON.parse(defRaw.payload) : {};

      // Use inline enqueue logic (no circular dep on Producer).
      const { v4: uuidv4 } = await import('uuid');
      const id = uuidv4();
      const nowIso = new Date().toISOString();
      const maxAttempts = defRaw.maxAttempts ?? '3';
      const priority = defRaw.priority ?? 'normal';

      const targetList = priority === 'normal'
        ? redisKeys.queue(queueName)
        : redisKeys.queuePriority(queueName, priority);

      const multi = this.deps.redis.multi();
      multi.sadd(redisKeys.queueRegistry, queueName);
      multi.hset(redisKeys.job(id), {
        id,
        queueName,
        jobName,
        payload: JSON.stringify(payload),
        status: 'pending',
        attempts: '0',
        maxAttempts: String(maxAttempts),
        priority,
        errorMessage: '',
        createdAt: nowIso,
        startedAt: '',
        completedAt: '',
      });
      multi.lpush(targetList, id);
      await multi.exec();
    }
  }
}
