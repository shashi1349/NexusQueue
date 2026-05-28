import { Registry, collectDefaultMetrics, Counter, Gauge, Histogram } from 'prom-client';
import type { Request, Response } from 'express';

const register = new Registry();
collectDefaultMetrics({ register });

export const jobsEnqueuedTotal = new Counter({
  name: 'nexusqueue_jobs_enqueued_total',
  help: 'Total number of jobs enqueued',
  labelNames: ['queue', 'priority'] as const,
  registers: [register],
});

// TODO: Worker-side metrics (jobsCompletedTotal, jobsFailedTotal, jobsDlqTotal,
// jobDurationSeconds, queueDepth, activeWorkers) require a Prometheus push gateway
// or a per-worker /metrics endpoint, since workers run as separate processes and
// cannot share the server's Prometheus registry.
export const jobsCompletedTotal = new Counter({
  name: 'nexusqueue_jobs_completed_total',
  help: 'Total number of jobs completed',
  labelNames: ['queue'] as const,
  registers: [register],
});

export const jobsFailedTotal = new Counter({
  name: 'nexusqueue_jobs_failed_total',
  help: 'Total number of jobs failed',
  labelNames: ['queue'] as const,
  registers: [register],
});

export const jobsDlqTotal = new Counter({
  name: 'nexusqueue_jobs_dlq_total',
  help: 'Total number of jobs sent to DLQ',
  labelNames: ['queue'] as const,
  registers: [register],
});

export const jobsRetriedTotal = new Counter({
  name: 'nexusqueue_jobs_retried_total',
  help: 'Total number of jobs retried',
  labelNames: ['queue'] as const,
  registers: [register],
});

export const queueDepth = new Gauge({
  name: 'nexusqueue_queue_depth',
  help: 'Current queue depth',
  labelNames: ['queue', 'priority'] as const,
  registers: [register],
});

export const activeWorkers = new Gauge({
  name: 'nexusqueue_active_workers',
  help: 'Number of active workers',
  labelNames: ['queue'] as const,
  registers: [register],
});

export const jobDurationSeconds = new Histogram({
  name: 'nexusqueue_job_duration_seconds',
  help: 'Job processing duration in seconds',
  labelNames: ['queue', 'jobName'] as const,
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60],
  registers: [register],
});

export async function getMetricsHandler(_req: Request, res: Response): Promise<void> {
  const metrics = await register.metrics();
  res.set('Content-Type', register.contentType);
  res.end(metrics);
}

export function incrementEnqueued(queue: string, priority: string): void {
  jobsEnqueuedTotal.inc({ queue, priority });
}

export function incrementRetried(queue: string): void {
  jobsRetriedTotal.inc({ queue });
}
