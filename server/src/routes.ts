import { Router } from 'express';
import { z } from 'zod';
import { getJob } from '@nexusqueue/shared';
import type { Producer } from './producer.js';
import type { Pool } from '@nexusqueue/shared';

/**
 * Phase 1 REST surface: just enough to demonstrate the SDK over HTTP.
 *   POST /jobs        -> enqueue
 *   GET  /jobs/:id    -> read job from Postgres
 *   GET  /health      -> liveness for Render/Railway
 *
 * Phase 5 will add /queues, /queues/:name/jobs, retry, DLQ inspector, etc.
 */

const enqueueBodySchema = z.object({
  jobName: z.string().min(1),
  payload: z.unknown(),
  queue: z.string().min(1).optional(),
  maxAttempts: z.number().int().positive().optional(),
});

export function buildRouter(deps: { producer: Producer; pg: Pool }): Router {
  const r = Router();

  r.get('/health', (_req, res) => {
    res.json({ status: 'ok' });
  });

  r.post('/jobs', async (req, res, next) => {
    try {
      const body = enqueueBodySchema.parse(req.body);
      // zod gives us only what's defined; spread to drop undefined keys
      // so we don't pass "queue: undefined" when caller omitted it.
      const opts: { queue?: string; maxAttempts?: number } = {};
      if (body.queue !== undefined) opts.queue = body.queue;
      if (body.maxAttempts !== undefined) opts.maxAttempts = body.maxAttempts;

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

  return r;
}
