import type { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import {
  redisKeys,
  insertPendingJob,
  insertDelayedJob,
  type EnqueueOptions,
  type Pool,
} from '@nexusqueue/shared';
import type { NexusEventBus } from './websocket.js';

/**
 * The Producer SDK.
 *
 * Public surface (Phase 1):
 *
 *     const producer = new Producer({ redis, pg });
 *     const jobId = await producer.enqueue('send-email', { to: '...' }, {
 *       queue: 'emails',
 *       maxAttempts: 3,
 *     });
 *
 * The SDK is a class only because it holds two clients. Methods stay thin.
 *
 * --- Durability ordering ---
 *
 * On every enqueue we have TWO writes that can each fail independently:
 *
 *   (A) Postgres INSERT  — durable audit record
 *   (B) Redis  LPUSH     — makes the job visible to workers
 *
 * We do (A) first, then (B). Reasoning:
 *
 *   - If (A) fails: nothing happened, throw to caller. Clean.
 *   - If (B) fails AFTER (A) succeeded: the job exists in Postgres in
 *     'pending' state but never reaches a worker. We surface the error
 *     to the caller, who can retry. Phase 4's janitor will also detect
 *     "pending" rows older than N seconds and re-enqueue them.
 *
 * The opposite order (Redis first) would mean a job could RUN before
 * its audit row exists — which makes status updates race against
 * inserts and breaks /jobs/:id reads.
 */

export interface ProducerDeps {
  redis: Redis;
  pg: Pool;
  eventBus?: NexusEventBus | undefined;
}

const DEFAULT_QUEUE = 'default';
const DEFAULT_MAX_ATTEMPTS = 3;

export class Producer {
  constructor(private readonly deps: ProducerDeps) {}

  async enqueue<TPayload>(
    jobName: string,
    payload: TPayload,
    options: EnqueueOptions = {},
  ): Promise<string> {
    if (!jobName || typeof jobName !== 'string') {
      throw new Error('enqueue: jobName must be a non-empty string');
    }

    // Idempotency check: if the key already exists, return the stored jobId.
    if (options.idempotencyKey) {
      const existing = await this.deps.redis.get(redisKeys.idempotency(options.idempotencyKey));
      if (existing) return existing;
    }

    const id = uuidv4();
    const queueName = options.queue ?? DEFAULT_QUEUE;
    const maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    const priority = options.priority ?? 'normal';
    const delay = options.delay;

    const nowIso = new Date().toISOString();

    if (delay && delay > 0) {
      // Delayed job path: ZADD to sorted set, don't push to queue list.
      const dueAt = Date.now() + delay;

      // (A) Durable audit row first.
      await insertDelayedJob(this.deps.pg, {
        id,
        queueName,
        jobName,
        payload,
        maxAttempts,
        delayedUntil: new Date(dueAt),
        priority,
      });

      // (B) Redis: register queue, store hash, ZADD to delayed set.
      const multi = this.deps.redis.multi();
      multi.sadd(redisKeys.queueRegistry, queueName);
      multi.hset(redisKeys.job(id), {
        id,
        queueName,
        jobName,
        payload: JSON.stringify(payload),
        status: 'delayed',
        attempts: '0',
        maxAttempts: String(maxAttempts),
        priority,
        errorMessage: '',
        createdAt: nowIso,
        startedAt: '',
        completedAt: '',
      });
      multi.zadd(redisKeys.delayed(queueName), String(dueAt), id);
      await multi.exec();
    } else {
      // Immediate job path (original behavior with priority routing).
      const insertParams: {
        id: string;
        queueName: string;
        jobName: string;
        payload: TPayload;
        maxAttempts: number;
        idempotencyKey?: string;
      } = {
        id,
        queueName,
        jobName,
        payload,
        maxAttempts,
      };
      if (options.idempotencyKey) {
        insertParams.idempotencyKey = options.idempotencyKey;
      }
      await insertPendingJob(this.deps.pg, insertParams);

      // Determine which list to push to based on priority.
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

    // Set idempotency key with 24h TTL after successful enqueue.
    if (options.idempotencyKey) {
      await this.deps.redis.set(redisKeys.idempotency(options.idempotencyKey), id, 'EX', 86400);
    }

    // Emit job.created event if event bus is available.
    if (this.deps.eventBus) {
      await this.deps.eventBus.publish({
        type: 'job.created',
        jobId: id,
        jobName,
        queueName,
        timestamp: Date.now(),
      });
    }

    return id;
  }
}
