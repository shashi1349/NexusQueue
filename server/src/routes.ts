import { Router } from 'express';
import { z } from 'zod';
import { getJob, markJobPendingForRetry, redisKeys } from '@nexusqueue/shared';
import type { Producer } from './producer.js';
import type { Pool } from '@nexusqueue/shared';
import type { Redis } from 'ioredis';
import type { NexusEventBus } from './websocket.js';

/**
 * REST surface:
 *   POST /jobs        -> enqueue
 *   GET  /jobs/:id    -> read job from Postgres
 *   GET  /health      -> liveness for Render/Railway
 *   GET  /queues      -> list queues with stats
 *   GET  /queues/:name/jobs  -> list jobs for a queue
 *   POST /jobs/:id/retry     -> retry a failed/dlq job
 *   GET  /workers            -> list active workers
 *   GET  /queues/:name/dlq   -> list DLQ jobs
 *   POST /queues/:name/dlq/requeue -> bulk requeue DLQ jobs
 */

const enqueueBodySchema = z.object({
  jobName: z.string().min(1),
  payload: z.unknown(),
  queue: z.string().min(1).optional(),
  maxAttempts: z.number().int().positive().optional(),
  idempotencyKey: z.string().min(1).optional(),
  delay: z.number().int().nonnegative().optional(),
  priority: z.enum(['high', 'normal', 'low']).optional(),
});

export interface RouterDeps {
  producer: Producer;
  pg: Pool;
  redis?: Redis;
  eventBus?: NexusEventBus;
}

export function buildRouter(deps: RouterDeps): Router {
  const r = Router();

  r.get('/health', (_req, res) => {
    res.json({ status: 'ok' });
  });

  r.post('/jobs', async (req, res, next) => {
    try {
      const body = enqueueBodySchema.parse(req.body);
      const opts: { queue?: string; maxAttempts?: number; idempotencyKey?: string; delay?: number; priority?: 'high' | 'normal' | 'low' } = {};
      if (body.queue !== undefined) opts.queue = body.queue;
      if (body.maxAttempts !== undefined) opts.maxAttempts = body.maxAttempts;
      if (body.idempotencyKey !== undefined) opts.idempotencyKey = body.idempotencyKey;
      if (body.delay !== undefined) opts.delay = body.delay;
      if (body.priority !== undefined) opts.priority = body.priority;

      const jobId = await deps.producer.enqueue(body.jobName, body.payload, opts);
      res.status(201).json({ jobId });
    } catch (err) {
      if (err instanceof z.ZodError) {
        res.status(400).json({ error: 'invalid_request', issues: err.issues });
        return;
      }
      next(err);
    }
  });

  r.get('/jobs/:id', async (req, res, next) => {
    try {
      const id = req.params.id;
      if (!id) {
        res.status(400).json({ error: 'missing_id' });
        return;
      }
      const job = await getJob(deps.pg, id);
      if (!job) {
        res.status(404).json({ error: 'not_found' });
        return;
      }
      res.json(job);
    } catch (err) {
      next(err);
    }
  });

  // Phase 5 endpoints below require redis
  if (deps.redis) {
    const redis = deps.redis;

    r.get('/queues', async (_req, res, next) => {
      try {
        const queueNames = await redis.smembers(redisKeys.queueRegistry);
        const queues = await Promise.all(queueNames.map(async (name) => {
          const [normalLen, highLen, lowLen] = await Promise.all([
            redis.llen(redisKeys.queue(name)),
            redis.llen(redisKeys.queuePriority(name, 'high')),
            redis.llen(redisKeys.queuePriority(name, 'low')),
          ]);
          const pending = normalLen + highLen + lowLen;

          const activeResult = await deps.pg.query(
            `SELECT COUNT(*) as count FROM jobs WHERE queue_name = $1 AND status = 'active'`,
            [name],
          );
          const active = Number(activeResult.rows[0]?.count ?? 0);

          const statsResult = await deps.pg.query(
            `SELECT status, COUNT(*) as count FROM jobs WHERE queue_name = $1 AND status IN ('completed', 'failed', 'dlq') GROUP BY status`,
            [name],
          );
          let completed = 0, failed = 0, dlq = 0;
          for (const row of statsResult.rows) {
            if (row.status === 'completed') completed = Number(row.count);
            else if (row.status === 'failed') failed = Number(row.count);
            else if (row.status === 'dlq') dlq = Number(row.count);
          }

          return { name, pending, active, completed, failed, dlq };
        }));
        res.json({ queues });
      } catch (err) { next(err); }
    });

    r.get('/queues/:name/jobs', async (req, res, next) => {
      try {
        const name = req.params.name!;
        const status = req.query.status as string | undefined;
        const limit = Math.min(Number(req.query.limit) || 50, 200);
        const offset = Number(req.query.offset) || 0;

        let countQuery: string;
        let dataQuery: string;
        let params: unknown[];

        if (status) {
          countQuery = `SELECT COUNT(*) as total FROM jobs WHERE queue_name = $1 AND status = $2`;
          dataQuery = `SELECT * FROM jobs WHERE queue_name = $1 AND status = $2 ORDER BY created_at DESC LIMIT $3 OFFSET $4`;
          params = [name, status, limit, offset];
        } else {
          countQuery = `SELECT COUNT(*) as total FROM jobs WHERE queue_name = $1`;
          dataQuery = `SELECT * FROM jobs WHERE queue_name = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`;
          params = [name, limit, offset];
        }

        const countResult = await deps.pg.query(countQuery, status ? [name, status] : [name]);
        const total = Number(countResult.rows[0]?.total ?? 0);
        const dataResult = await deps.pg.query(dataQuery, params);

        const jobs = dataResult.rows.map((row: any) => ({
          id: row.id,
          queueName: row.queue_name,
          jobName: row.job_name,
          payload: row.payload,
          status: row.status,
          attempts: row.attempts,
          maxAttempts: row.max_attempts,
          errorMessage: row.error_message,
          createdAt: row.created_at?.toISOString?.() ?? row.created_at,
          startedAt: row.started_at?.toISOString?.() ?? row.started_at ?? null,
          completedAt: row.completed_at?.toISOString?.() ?? row.completed_at ?? null,
        }));

        res.json({ jobs, total });
      } catch (err) { next(err); }
    });

    r.post('/jobs/:id/retry', async (req, res, next) => {
      try {
        const id = req.params.id!;
        const job = await getJob(deps.pg, id);
        if (!job) { res.status(404).json({ error: 'not_found' }); return; }
        if (job.status !== 'failed' && job.status !== 'dlq') {
          res.status(400).json({ error: 'job_not_retriable', message: 'Only failed or dlq jobs can be retried' });
          return;
        }

        // Reset in Postgres
        await markJobPendingForRetry(deps.pg, id);

        // Reset in Redis
        await redis.hset(redisKeys.job(id), {
          status: 'pending',
          attempts: '0',
          startedAt: '',
          completedAt: '',
          errorMessage: '',
        });

        // Remove from DLQ if present
        await redis.lrem(redisKeys.dlq(job.queueName), 0, id);

        // Push to appropriate queue
        const hash = await redis.hgetall(redisKeys.job(id));
        const priority = hash.priority ?? 'normal';
        const targetList = priority === 'normal'
          ? redisKeys.queue(job.queueName)
          : redisKeys.queuePriority(job.queueName, priority);
        await redis.lpush(targetList, id);

        // Publish event
        if (deps.eventBus) {
          await deps.eventBus.publish({
            type: 'job.retried',
            jobId: id,
            jobName: job.jobName,
            queueName: job.queueName,
            timestamp: Date.now(),
          });
        }

        res.json({ success: true });
      } catch (err) { next(err); }
    });

    r.get('/workers', async (_req, res, next) => {
      try {
        const workerIds = await redis.smembers(redisKeys.workerRegistry);
        const workers = await Promise.all(workerIds.map(async (id) => {
          const meta = await redis.hgetall(redisKeys.workerMeta(id));
          const heartbeat = await redis.get(redisKeys.heartbeat(id));
          return {
            id,
            status: meta.status ?? 'unknown',
            queue: meta.queue ?? '',
            startedAt: meta.startedAt ?? null,
            currentJobs: Number(meta.currentJobs ?? 0),
            lastHeartbeat: heartbeat ? Number(heartbeat) : null,
          };
        }));
        res.json({ workers });
      } catch (err) { next(err); }
    });

    r.get('/queues/:name/dlq', async (req, res, next) => {
      try {
        const name = req.params.name!;
        const jobIds = await redis.lrange(redisKeys.dlq(name), 0, -1);
        const jobs = await Promise.all(jobIds.map(async (id) => {
          const hash = await redis.hgetall(redisKeys.job(id));
          return {
            id: hash.id ?? id,
            queueName: hash.queueName ?? name,
            jobName: hash.jobName ?? '',
            payload: hash.payload ? JSON.parse(hash.payload) : null,
            status: hash.status ?? 'dlq',
            attempts: Number(hash.attempts ?? 0),
            maxAttempts: Number(hash.maxAttempts ?? 0),
            errorMessage: hash.errorMessage || null,
            createdAt: hash.createdAt ?? null,
            startedAt: hash.startedAt || null,
            completedAt: hash.completedAt || null,
          };
        }));
        res.json({ jobs });
      } catch (err) { next(err); }
    });

    r.post('/queues/:name/dlq/requeue', async (req, res, next) => {
      try {
        const name = req.params.name!;
        const body = req.body as { jobIds?: string[]; all?: boolean };

        let jobIds: string[];
        if (body.all) {
          jobIds = await redis.lrange(redisKeys.dlq(name), 0, -1);
        } else if (body.jobIds && Array.isArray(body.jobIds)) {
          jobIds = body.jobIds;
        } else {
          res.status(400).json({ error: 'invalid_request', message: 'Provide jobIds array or { all: true }' });
          return;
        }

        let requeued = 0;
        for (const id of jobIds) {
          // Reset in Redis
          await redis.hset(redisKeys.job(id), {
            status: 'pending',
            attempts: '0',
            startedAt: '',
            completedAt: '',
            errorMessage: '',
          });

          // Remove from DLQ
          await redis.lrem(redisKeys.dlq(name), 1, id);

          // Push to queue
          const hash = await redis.hgetall(redisKeys.job(id));
          const priority = hash.priority ?? 'normal';
          const targetList = priority === 'normal'
            ? redisKeys.queue(name)
            : redisKeys.queuePriority(name, priority);
          await redis.lpush(targetList, id);

          // Update Postgres
          await markJobPendingForRetry(deps.pg, id);

          // Publish event
          if (deps.eventBus) {
            await deps.eventBus.publish({
              type: 'job.retried',
              jobId: id,
              jobName: hash.jobName ?? '',
              queueName: name,
              timestamp: Date.now(),
            });
          }

          requeued++;
        }

        res.json({ requeued });
      } catch (err) { next(err); }
    });
  }

  return r;
}
