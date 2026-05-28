import pg from 'pg';
import type { Job, JobStatus } from './types.js';

const { Pool } = pg;
export type { Pool } from 'pg';

/**
 * Postgres connection pool factory.
 *
 * Why a pool, not a single client?
 *   - Producer enqueue() is called from HTTP handlers — multiple concurrent
 *     requests need parallel connections.
 *   - The worker uses one connection at a time today, but Phase 4
 *     concurrency makes it parallel-by-handler.
 *
 * Pool size note: the default of 10 is fine for local dev. In Phase 7
 * we'll tune this against Render/Neon connection limits.
 */
export function createPgPool(connectionString: string): pg.Pool {
  const pool = new Pool({
    connectionString,
    max: 10,
    idleTimeoutMillis: 30_000,
    // Fail fast on bad config rather than retry forever.
    connectionTimeoutMillis: 5_000,
  });

  pool.on('error', (err: Error) => {
    // eslint-disable-next-line no-console
    console.error('[pg] idle client error:', err.message);
  });

  return pool;
}

/* ------------------------------------------------------------------ *
 *  Job DAO — thin functions, not a class. Keeps SQL co-located and
 *  makes it obvious where every state transition is.
 * ------------------------------------------------------------------ */

/** Insert a brand-new job row in 'pending' state. */
export async function insertPendingJob(
  pool: pg.Pool,
  job: {
    id: string;
    queueName: string;
    jobName: string;
    payload: unknown;
    maxAttempts: number;
    idempotencyKey?: string;
  },
): Promise<void> {
  await pool.query(
    `INSERT INTO jobs (id, queue_name, job_name, payload, status, max_attempts, idempotency_key)
     VALUES ($1, $2, $3, $4, 'pending', $5, $6)`,
    [job.id, job.queueName, job.jobName, JSON.stringify(job.payload), job.maxAttempts, job.idempotencyKey ?? null],
  );
}

/** Mark a job 'active' and stamp started_at + increment attempts. */
export async function markJobActive(pool: pg.Pool, jobId: string): Promise<void> {
  await pool.query(
    `UPDATE jobs
        SET status = 'active',
            started_at = NOW(),
            attempts = attempts + 1
      WHERE id = $1`,
    [jobId],
  );
}

/** Mark a job 'completed' and stamp completed_at. */
export async function markJobCompleted(pool: pg.Pool, jobId: string): Promise<void> {
  await pool.query(
    `UPDATE jobs
        SET status = 'completed',
            completed_at = NOW(),
            error_message = NULL
      WHERE id = $1`,
    [jobId],
  );
}

/** Mark a job 'failed' and persist the error string. */
export async function markJobFailed(
  pool: pg.Pool,
  jobId: string,
  errorMessage: string,
): Promise<void> {
  await pool.query(
    `UPDATE jobs
        SET status = 'failed',
            completed_at = NOW(),
            error_message = $2
      WHERE id = $1`,
    [jobId, errorMessage],
  );
}

/** Mark a job 'dlq' (dead-letter) and persist the error string. */
export async function markJobDlq(
  pool: pg.Pool,
  jobId: string,
  errorMessage: string,
): Promise<void> {
  await pool.query(
    `UPDATE jobs
        SET status = 'dlq',
            completed_at = NOW(),
            error_message = $2
      WHERE id = $1`,
    [jobId, errorMessage],
  );
}

/** Reset a job back to 'pending' for retry, clearing timing fields. */
export async function markJobPendingForRetry(
  pool: pg.Pool,
  jobId: string,
): Promise<void> {
  await pool.query(
    `UPDATE jobs
        SET status = 'pending',
            started_at = NULL,
            completed_at = NULL
      WHERE id = $1`,
    [jobId],
  );
}

/** Fetch a single job row. Used by smoke tests and (later) the dashboard. */
export async function getJob(pool: pg.Pool, jobId: string): Promise<Job | null> {
  const { rows } = await pool.query(
    `SELECT id, queue_name, job_name, payload, status, attempts, max_attempts,
            error_message, created_at, started_at, completed_at
       FROM jobs WHERE id = $1`,
    [jobId],
  );
  const row = rows[0];
  if (!row) return null;
  return {
    id: row.id,
    queueName: row.queue_name,
    jobName: row.job_name,
    payload: row.payload,
    status: row.status as JobStatus,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    errorMessage: row.error_message,
    createdAt: row.created_at.toISOString(),
    startedAt: row.started_at ? row.started_at.toISOString() : null,
    completedAt: row.completed_at ? row.completed_at.toISOString() : null,
  };
}

/** Insert a job in 'delayed' state with a delayed_until timestamp. */
export async function insertDelayedJob(
  pool: pg.Pool,
  job: {
    id: string;
    queueName: string;
    jobName: string;
    payload: unknown;
    maxAttempts: number;
    delayedUntil: Date;
    priority?: string;
  },
): Promise<void> {
  await pool.query(
    `INSERT INTO jobs (id, queue_name, job_name, payload, status, max_attempts, delayed_until, priority)
     VALUES ($1, $2, $3, $4, 'delayed', $5, $6, $7)`,
    [job.id, job.queueName, job.jobName, JSON.stringify(job.payload), job.maxAttempts, job.delayedUntil, job.priority ?? 'normal'],
  );
}

/** Mark a job as 'delayed' (used when retry puts job in delayed sorted set). */
export async function markJobDelayed(
  pool: pg.Pool,
  jobId: string,
): Promise<void> {
  await pool.query(
    `UPDATE jobs
        SET status = 'delayed',
            started_at = NULL,
            completed_at = NULL
      WHERE id = $1`,
    [jobId],
  );
}
