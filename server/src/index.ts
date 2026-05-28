import http from 'node:http';
import express from 'express';
import cors from 'cors';
import pinoHttpModule from 'pino-http';

// ESM/CJS interop workaround for pino-http under NodeNext resolution
const pinoHttp = pinoHttpModule as unknown as typeof pinoHttpModule.default;
import { createPgPool, createRedisClient, createLogger } from '@nexusqueue/shared';
import { loadServerConfig } from './config.js';
import { Producer } from './producer.js';
import { buildRouter } from './routes.js';
import { NexusEventBus } from './websocket.js';
import { setupSwagger } from './openapi.js';

const logger = createLogger('server');

async function main(): Promise<void> {
  const cfg = loadServerConfig();
  const redis = createRedisClient(cfg.redisUrl);
  const pg = createPgPool(cfg.databaseUrl);

  // Wait for Redis connection before issuing commands.
  await new Promise<void>((resolve, reject) => {
    if (redis.status === 'ready') { resolve(); return; }
    redis.once('ready', resolve);
    redis.once('error', reject);
  });

  const app = express();
  app.use(cors());
  app.use(express.json({ limit: '1mb' }));

  // Request logging (skip /health and /metrics)
  const httpLogger = pinoHttp({
    logger,
    autoLogging: {
      ignore: (req) => {
        const url = req.url ?? '';
        return url === '/health' || url === '/metrics';
      },
    },
  });
  app.use(httpLogger);

  // Mount Swagger UI
  setupSwagger(app);

  // Create HTTP server (needed by WebSocket event bus before listen)
  const server = http.createServer(app);

  // Create a dedicated subscriber Redis client for PUB/SUB.
  // enableOfflineQueue must be true for the subscriber so it queues the
  // SUBSCRIBE command while the TCP connection is being established.
  const subscriberRedis = createRedisClient(cfg.redisUrl, { enableOfflineQueue: true });
  const eventBus = new NexusEventBus(server, subscriberRedis, redis);

  // Create producer with event bus passed via constructor (type-safe)
  const producer = new Producer({ redis, pg, eventBus });

  // Mount routes
  app.use(buildRouter({ producer, pg, redis, eventBus }));

  // Centralized error handler - must come AFTER routes.
  app.use(
    (
      err: unknown,
      _req: express.Request,
      res: express.Response,
      _next: express.NextFunction,
    ) => {
      logger.error({ err }, 'unhandled error');
      res.status(500).json({ error: 'internal_error' });
    },
  );

  server.listen(cfg.port, cfg.host, () => {
    logger.info({ host: cfg.host, port: cfg.port }, 'server listening');
  });

  // Graceful shutdown so docker stop / Ctrl+C don't drop in-flight requests.
  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'received shutdown signal, draining...');
    eventBus.close();
    server.close(() => {
      void Promise.all([redis.quit(), subscriberRedis.quit(), pg.end()]).then(() => process.exit(0));
    });
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((err) => {
  logger.fatal({ err }, 'failed to start');
  process.exit(1);
});

export { Producer } from './producer.js';
export { loadServerConfig } from './config.js';
