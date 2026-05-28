/**
 * Phase 6 integration tests.
 *
 * Tests:
 *   - GET /metrics (Prometheus endpoint)
 *   - API key auth (POST /jobs with API_KEYS set)
 *   - JWT auth (GET /queues with JWT_SECRET set)
 *   - POST /auth/login
 *   - GET /docs (Swagger UI)
 *   - Auth disabled when env vars not set
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import RedisMock from 'ioredis-mock';
import express from 'express';
import http from 'http';
import jwt from 'jsonwebtoken';
import { redisKeys } from '@nexusqueue/shared';
import { buildRouter } from '../server/src/routes.js';
import { Producer } from '../server/src/producer.js';
import { setupSwagger } from '../server/src/openapi.js';

/**
 * Minimal pg Pool mock.
 */
function createMockPool() {
  const queries: Array<{ text: string; values: unknown[] }> = [];
  let nextResult: { rows: any[]; rowCount: number } = { rows: [], rowCount: 0 };
  const resultQueue: Array<{ rows: any[]; rowCount: number }> = [];

  const pool = {
    query: vi.fn(async (text: string, values: unknown[] = []) => {
      queries.push({ text, values });
      if (resultQueue.length > 0) {
        return resultQueue.shift()!;
      }
      return nextResult;
    }),
    end: vi.fn(async () => {}),
  };

  return {
    pool: pool as any,
    queries,
    setNextResult: (result: { rows: any[]; rowCount: number }) => { nextResult = result; },
    pushResult: (result: { rows: any[]; rowCount: number }) => { resultQueue.push(result); },
  };
}

function createTestApp(redis: any, pg: any) {
  const producer = new Producer({ redis, pg });
  const app = express();
  app.use(express.json());
  setupSwagger(app);
  app.use(buildRouter({ producer, pg, redis }));
  return app;
}

function startServer(app: express.Application): Promise<{ server: http.Server; baseUrl: string }> {
  return new Promise((resolve) => {
    const server = app.listen(0, '127.0.0.1', () => {
      const addr = server.address() as { port: number };
      resolve({ server, baseUrl: `http://127.0.0.1:${addr.port}` });
    });
  });
}

function stopServer(server: http.Server): Promise<void> {
  return new Promise((resolve) => {
    server.close(() => resolve());
  });
}

describe('Phase 6 -- GET /metrics', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    await stopServer(server);
  });

  it('returns 200 with Prometheus text format', async () => {
    const res = await fetch(`${baseUrl}/metrics`);
    expect(res.status).toBe(200);
    const contentType = res.headers.get('content-type') ?? '';
    expect(contentType).toContain('text/');
    const body = await res.text();
    expect(body).toContain('nexusqueue_jobs_enqueued_total');
  });

  it('contains custom NexusQueue metrics', async () => {
    const res = await fetch(`${baseUrl}/metrics`);
    const body = await res.text();
    expect(body).toContain('nexusqueue_jobs_completed_total');
    expect(body).toContain('nexusqueue_jobs_failed_total');
    expect(body).toContain('nexusqueue_jobs_dlq_total');
    expect(body).toContain('nexusqueue_jobs_retried_total');
    expect(body).toContain('nexusqueue_job_duration_seconds');
  });
});

describe('Phase 6 -- API Key auth', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    delete process.env.API_KEYS;
    await stopServer(server);
  });

  it('POST /jobs returns 401 when API_KEYS is set and no key provided', async () => {
    process.env.API_KEYS = 'test-key-1,test-key-2';

    const res = await fetch(`${baseUrl}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jobName: 'test', payload: {} }),
    });
    expect(res.status).toBe(401);
    const body = await res.json();
    expect(body.error).toBe('unauthorized');
  });

  it('POST /jobs returns 401 with invalid API key', async () => {
    process.env.API_KEYS = 'test-key-1';

    const res = await fetch(`${baseUrl}/jobs`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer wrong-key',
      },
      body: JSON.stringify({ jobName: 'test', payload: {} }),
    });
    expect(res.status).toBe(401);
  });

  it('POST /jobs succeeds with valid API key in Authorization header', async () => {
    process.env.API_KEYS = 'test-key-1,test-key-2';

    // Mock the insert for enqueue
    mockPg.pushResult({ rows: [], rowCount: 1 });

    const res = await fetch(`${baseUrl}/jobs`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer test-key-1',
      },
      body: JSON.stringify({ jobName: 'test', payload: { x: 1 } }),
    });
    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.jobId).toBeDefined();
  });

  it('POST /jobs succeeds with valid API key in X-API-Key header', async () => {
    process.env.API_KEYS = 'my-api-key';

    mockPg.pushResult({ rows: [], rowCount: 1 });

    const res = await fetch(`${baseUrl}/jobs`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': 'my-api-key',
      },
      body: JSON.stringify({ jobName: 'test', payload: {} }),
    });
    expect(res.status).toBe(201);
  });

  it('POST /jobs passes through when API_KEYS not set (auth disabled)', async () => {
    delete process.env.API_KEYS;

    mockPg.pushResult({ rows: [], rowCount: 1 });

    const res = await fetch(`${baseUrl}/jobs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jobName: 'test', payload: {} }),
    });
    expect(res.status).toBe(201);
  });
});

describe('Phase 6 -- JWT auth', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    delete process.env.JWT_SECRET;
    await stopServer(server);
  });

  it('GET /queues returns 401 when JWT_SECRET is set and no token provided', async () => {
    process.env.JWT_SECRET = 'test-secret-123';

    const res = await fetch(`${baseUrl}/queues`);
    expect(res.status).toBe(401);
    const body = await res.json();
    expect(body.error).toBe('unauthorized');
  });

  it('GET /queues returns 401 with invalid JWT', async () => {
    process.env.JWT_SECRET = 'test-secret-123';

    const res = await fetch(`${baseUrl}/queues`, {
      headers: { 'Authorization': 'Bearer invalid-token' },
    });
    expect(res.status).toBe(401);
  });

  it('GET /queues succeeds with valid JWT', async () => {
    process.env.JWT_SECRET = 'test-secret-123';
    const token = jwt.sign({ sub: 'admin' }, 'test-secret-123', { expiresIn: '1h' });

    mockPg.setNextResult({ rows: [], rowCount: 0 });

    const res = await fetch(`${baseUrl}/queues`, {
      headers: { 'Authorization': `Bearer ${token}` },
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.queues).toEqual([]);
  });

  it('GET /queues passes through when JWT_SECRET not set (auth disabled)', async () => {
    delete process.env.JWT_SECRET;

    mockPg.setNextResult({ rows: [], rowCount: 0 });

    const res = await fetch(`${baseUrl}/queues`);
    expect(res.status).toBe(200);
  });
});

describe('Phase 6 -- POST /auth/login', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    delete process.env.JWT_SECRET;
    delete process.env.DASHBOARD_USER;
    delete process.env.DASHBOARD_PASSWORD;
    await stopServer(server);
  });

  it('returns 501 when auth env vars are not configured', async () => {
    delete process.env.JWT_SECRET;
    delete process.env.DASHBOARD_USER;
    delete process.env.DASHBOARD_PASSWORD;

    const res = await fetch(`${baseUrl}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'admin', password: 'secret' }),
    });
    expect(res.status).toBe(501);
  });

  it('returns 401 with invalid credentials', async () => {
    process.env.JWT_SECRET = 'test-secret';
    process.env.DASHBOARD_USER = 'admin';
    process.env.DASHBOARD_PASSWORD = 'correct-password';

    const res = await fetch(`${baseUrl}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'admin', password: 'wrong' }),
    });
    expect(res.status).toBe(401);
  });

  it('returns a valid JWT token on successful login', async () => {
    process.env.JWT_SECRET = 'test-secret';
    process.env.DASHBOARD_USER = 'admin';
    process.env.DASHBOARD_PASSWORD = 'correct-password';

    const res = await fetch(`${baseUrl}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'admin', password: 'correct-password' }),
    });
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.token).toBeDefined();

    // Verify token is valid
    const decoded = jwt.verify(body.token, 'test-secret') as { sub: string };
    expect(decoded.sub).toBe('admin');
  });

  it('returned JWT can be used to access protected routes', async () => {
    process.env.JWT_SECRET = 'test-secret';
    process.env.DASHBOARD_USER = 'admin';
    process.env.DASHBOARD_PASSWORD = 'secret123';

    // Login
    const loginRes = await fetch(`${baseUrl}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'admin', password: 'secret123' }),
    });
    const { token } = await loginRes.json();

    // Use token to access protected route
    mockPg.setNextResult({ rows: [], rowCount: 0 });
    const queuesRes = await fetch(`${baseUrl}/queues`, {
      headers: { 'Authorization': `Bearer ${token}` },
    });
    expect(queuesRes.status).toBe(200);
  });
});

describe('Phase 6 -- GET /docs', () => {
  let redis: InstanceType<typeof RedisMock>;
  let mockPg: ReturnType<typeof createMockPool>;
  let server: http.Server;
  let baseUrl: string;

  beforeEach(async () => {
    redis = new RedisMock();
    await redis.flushall();
    mockPg = createMockPool();
    const app = createTestApp(redis, mockPg.pool);
    const result = await startServer(app);
    server = result.server;
    baseUrl = result.baseUrl;
  });

  afterEach(async () => {
    await stopServer(server);
  });

  it('GET /docs returns 200 with HTML (Swagger UI)', async () => {
    const res = await fetch(`${baseUrl}/docs/`, { redirect: 'follow' });
    expect(res.status).toBe(200);
    const contentType = res.headers.get('content-type') ?? '';
    expect(contentType).toContain('text/html');
    const body = await res.text();
    expect(body).toContain('swagger');
  });

  it('GET /docs/json returns the OpenAPI spec as JSON', async () => {
    const res = await fetch(`${baseUrl}/docs/json`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.openapi).toBe('3.0.3');
    expect(body.info.title).toBe('NexusQueue API');
    expect(body.paths['/jobs']).toBeDefined();
    expect(body.paths['/metrics']).toBeDefined();
    expect(body.paths['/auth/login']).toBeDefined();
  });
});
