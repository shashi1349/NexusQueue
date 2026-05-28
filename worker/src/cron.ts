import type { Redis } from 'ioredis';
import { CronExpressionParser } from 'cron-parser';
import { redisKeys } from '@nexusqueue/shared';

/**
 * CronManager -- manages recurring job definitions.
 *
 * Stores cron definitions in Redis hashes and schedules their next
 * occurrence in the cron schedule sorted set. The Scheduler polling
 * loop picks them up when due.
 */

export interface CronJobDef {
  cronId: string;
  jobName: string;
  payload?: unknown;
  cronExpression: string;
  queue?: string;
  options?: {
    maxAttempts?: number;
    priority?: string;
  };
}

export class CronManager {
  constructor(private readonly redis: Redis) {}

  /** Register a new cron job. Stores the definition and schedules the next run. */
  async addJob(def: CronJobDef): Promise<void> {
    const { cronId, jobName, payload, cronExpression, queue, options } = def;

    // Compute next occurrence.
    const interval = CronExpressionParser.parse(cronExpression);
    const next = interval.next();
    const nextMs = next.getTime();

    // Store the definition in a Redis hash.
    await this.redis.hset(redisKeys.cronDef(cronId), {
      cronId,
      jobName,
      payload: JSON.stringify(payload ?? {}),
      cronExpression,
      queue: queue ?? 'default',
      maxAttempts: String(options?.maxAttempts ?? 3),
      priority: options?.priority ?? 'normal',
    });

    // Schedule the next occurrence.
    await this.redis.zadd(redisKeys.cronSchedule, String(nextMs), cronId);
  }

  /** Remove a cron job from both the definition hash and the schedule. */
  async removeJob(cronId: string): Promise<void> {
    await this.redis.del(redisKeys.cronDef(cronId));
    await this.redis.zrem(redisKeys.cronSchedule, cronId);
  }

  /** List all registered cron jobs. */
  async listJobs(): Promise<CronJobDef[]> {
    // Get all cronIds from the schedule sorted set.
    const cronIds = await this.redis.zrangebyscore(redisKeys.cronSchedule, '-inf', '+inf');
    const jobs: CronJobDef[] = [];

    for (const cronId of cronIds) {
      const data = await this.redis.hgetall(redisKeys.cronDef(cronId));
      if (data && data.cronId) {
        const jobDef: CronJobDef = {
          cronId: data.cronId,
          jobName: data.jobName ?? '',
          payload: data.payload ? JSON.parse(data.payload) : {},
          cronExpression: data.cronExpression ?? '',
        };
        if (data.queue) {
          jobDef.queue = data.queue;
        }
        if (data.maxAttempts || data.priority) {
          jobDef.options = {
            maxAttempts: data.maxAttempts ? Number(data.maxAttempts) : 3,
            priority: data.priority ?? 'normal',
          };
        }
        jobs.push(jobDef);
      }
    }

    return jobs;
  }
}
