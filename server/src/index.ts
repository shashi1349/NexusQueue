import express from 'express';
import { createPgPool, createRedisClient } from '@nexusqueue/shared';
import { loadServerConfig } from './config.js';
import { Producer } from './producer.js';
import { buildRouter } from './routes.js';

async function main(): Promise<void> {
  const cfg = loadServerConfig();
  const redis = createRedisClient(cfg.redisUrl);
  const pg = createPgPool(cfg.databaseUrl);

  const producer = new Producer({ redis, pg });

  const app = express();
  app.use(express.json({ limit: '1mb' }));
  app.use(buildRouter({ producer, pg }));

  // Centralized error handler. Phase 6 will swap to pino.
  app.use(
    (
      err: unknown,
      _req: express.Request,
      res: express.Response,
      _next: express.NextFunction,
    ) => {
      // eslint-disable-next-line no-console
      console.error('[server] unhandled error:', err);
      res.status(500).json({ error: 'internal_error' });
    },
  );

  const server = app.listen(cfg.port, cfg.host, () => {
    // eslint-disable-next-line no-console
    console.log(`[server] listening on http://${cfg.host}:${cfg.port}`);
  });

  // Graceful shutdown so docker stop / Ctrl+C don't drop in-flight requests.
  const shutdown = async (signal: string): Promise<void> => {
    // eslint-disable-next-line no-console
    console.log(`[server] received ${signal}, draining...`);
    server.close(() => {
      void Promise.all([redis.quit(), pg.end()]).then(() => process.exit(0));
    });
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[server] failed to start:', err);
  process.exit(1);
});

export { Producer } from './producer.js';
export { loadServerConfig } from './config.js';
