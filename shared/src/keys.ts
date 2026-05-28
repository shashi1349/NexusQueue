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

  /** Hash holding the full job state. */
  job: (id: string) => `${PREFIX}:job:${id}`,

  /** Set of all queue names we have ever seen — used by GET /queues. */
  queueRegistry: `${PREFIX}:queues`,
} as const;
