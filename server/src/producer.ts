import type { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import {
  redisKeys,
  insertPendingJob,
  type EnqueueOptions,
  type Pool,
} from '@nexusqueue/shared';

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
}

const DEFAULT_QUEUE = 'default';
const DEFAULT_MAX_ATTEMPTS = 1; // Phase 1: no retries yet (Phase 2)

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

    const id = uuidv4();
    const queueName = options.queue ?? DEFAULT_QUEUE;
    const maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;

    // (A) Durable audit row first.
    await insertPendingJob(this.deps.pg, {
      id,
      queueName,
      jobName,
      payload,
      maxAttempts,
    });

    // (B) Make the job visible to workers.
    //   - SADD registers the queue so /queues can list it.
    //   - HSET stores the runtime job hash workers will read on dequeue.
    //   - LPUSH puts the job on the FIFO list (workers BRPOP from the right).
    //
    // We use a MULTI so all three Redis writes either succeed or fail
    // together — keeps Redis state internally consistent even on crash.
    const nowIso = new Date().toISOString();
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
      errorMessage: '',
      createdAt: nowIso,
      startedAt: '',
      completedAt: '',
    });
    multi.lpush(redisKeys.queue(queueName), id);
    await multi.exec();

    return id;
  }
}
