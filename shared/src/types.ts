/**
 * Job lifecycle states.
 *
 *   pending   -> in queue, waiting for a worker to pick it up
 *   active    -> popped by a worker, handler is currently running
 *   completed -> handler returned successfully
 *   failed    -> handler threw and we are NOT going to retry (terminal in Phase 1)
 *   delayed   -> scheduled for the future (Phase 3)
 *   dlq       -> exhausted retries, moved to dead-letter queue (Phase 2)
 *
 * Phase 1 only exercises pending/active/completed/failed.
 * The other states exist in the type so we don't have to migrate the schema later.
 */
export type JobStatus =
  | 'pending'
  | 'active'
  | 'completed'
  | 'failed'
  | 'delayed'
  | 'dlq';

export interface Job<TPayload = unknown> {
  id: string;
  queueName: string;
  jobName: string;
  payload: TPayload;
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  errorMessage: string | null;
  createdAt: string; // ISO-8601
  startedAt: string | null;
  completedAt: string | null;
}

export type Priority = 'high' | 'normal' | 'low';

export interface RateLimitConfig {
  maxTokens: number;
  refillRate: number;
}

export interface EnqueueOptions {
  /** Logical queue name. Defaults to "default". */
  queue?: string;
  /**
   * Maximum number of attempts before the job goes to DLQ.
   * Phase 1 ignores this (retries land in Phase 2), but the SDK
   * accepts it now so we don't change the producer signature later.
   */
  maxAttempts?: number;
  /** Optional idempotency key to prevent duplicate job creation. */
  idempotencyKey?: string | undefined;
  /** Delay in milliseconds before the job becomes visible to workers. */
  delay?: number;
  /** Priority level for the job. Defaults to 'normal'. */
  priority?: Priority;
}

/**
 * The shape we serialize into the Redis job hash.
 * All values are strings because Redis hashes only store strings.
 */
export type RedisJobHash = Record<keyof Job, string>;
