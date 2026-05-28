/**
 * Centralized Redis key naming.
 *
 * Why a module instead of inline string concatenation?
 *   - Producer and worker MUST agree byte-for-byte on key names. A typo
 *     in one process means jobs vanish silently. Forcing both to import
 *     the same helper makes that class of bug impossible.
 *   - When Phase 4 introduces the janitor and Phase 3 introduces the
 *     scheduler, they need to know about every key NexusQueue owns.
 *     One module is the index of our Redis namespace.
 *
 * Convention: "nexus:" prefix isolates us from any other tenant
 * sharing the same Redis instance (common on managed Redis tiers).
 */
const PREFIX = 'nexus';

export const redisKeys = {
  /** List of pending job IDs for a queue (FIFO via LPUSH/BRPOP). */
  queue: (name: string) => `${PREFIX}:queue:${name}`,

  /** Priority-specific queue list. */
  queuePriority: (name: string, priority: string) => `${PREFIX}:queue:${name}:${priority}`,

  /** Hash holding the full job state. */
  job: (id: string) => `${PREFIX}:job:${id}`,

  /** Set of all queue names we have ever seen — used by GET /queues. */
  queueRegistry: `${PREFIX}:queues`,

  /** List of job IDs currently being processed by a specific worker. */
  processing: (workerId: string) => `${PREFIX}:processing:${workerId}`,

  /** Dead-letter queue list for a specific queue. */
  dlq: (queueName: string) => `${PREFIX}:dlq:${queueName}`,

  /** Idempotency lookup key. */
  idempotency: (key: string) => `${PREFIX}:idem:${key}`,

  /** Sorted set for delayed jobs (score = timestamp when job becomes due). */
  delayed: (queueName: string) => `${PREFIX}:delayed:${queueName}`,

  /** Sorted set holding cron schedule entries (score = next run timestamp). */
  cronSchedule: `${PREFIX}:cron:schedule`,

  /** Hash holding cron job definition by cronId. */
  cronDef: (cronId: string) => `${PREFIX}:cron:def:${cronId}`,

  /** Hash holding rate limit state for a queue. */
  rateLimit: (queueName: string) => `${PREFIX}:ratelimit:${queueName}`,

  /** Worker heartbeat key with TTL (value = timestamp). */
  heartbeat: (workerId: string) => `${PREFIX}:heartbeat:${workerId}`,

  /** Hash holding worker metadata (status, queue, startedAt, currentJobs). */
  workerMeta: (workerId: string) => `${PREFIX}:worker:${workerId}`,

  /** Set of all active worker IDs. */
  workerRegistry: `${PREFIX}:workers`,

  /** PUB/SUB channel for real-time job events. */
  events: `${PREFIX}:events`,
} as const;
