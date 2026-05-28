import { Redis, type RedisOptions } from 'ioredis';

/**
 * Why ioredis (not node-redis)?
 *   - First-class TypeScript types and Promise API.
 *   - Built-in support for blocking commands (BLPOP/BRPOP/BRPOPLPUSH)
 *     which we rely on for the worker pull loop.
 *   - Battle-tested in BullMQ — same client we'd reach for in production.
 *
 * Why a factory function?
 *   - Worker processes need a *separate* connection for blocking commands,
 *     because BRPOP holds the connection until a job appears. Sharing the
 *     blocking connection with regular commands would deadlock.
 *   - Tests can pass in a mock or in-memory Redis (ioredis-mock) without
 *     touching production code paths.
 */
export function createRedisClient(
  url: string,
  overrides: RedisOptions = {},
): Redis {
  const client = new Redis(url, {
    // Don't queue commands while disconnected — fail fast so callers
    // (the producer SDK) surface connection problems instead of hanging.
    enableOfflineQueue: false,
    maxRetriesPerRequest: 3,
    ...overrides,
  });

  client.on('error', (err: Error) => {
    // Console for Phase 1; we swap in pino in Phase 6.
    // eslint-disable-next-line no-console
    console.error('[redis] client error:', err.message);
  });

  return client;
}
