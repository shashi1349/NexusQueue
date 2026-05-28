import type { Express } from 'express';
import swaggerUi from 'swagger-ui-express';

const openapiSpec = {
  openapi: '3.0.3',
  info: {
    title: 'NexusQueue API',
    version: '0.1.0',
    description: 'Distributed task queue engine REST API',
  },
  servers: [{ url: '/' }],
  components: {
    securitySchemes: {
      ApiKeyAuth: {
        type: 'apiKey',
        in: 'header',
        name: 'X-API-Key',
      },
      BearerAuth: {
        type: 'http',
        scheme: 'bearer',
        bearerFormat: 'JWT',
      },
    },
    schemas: {
      Job: {
        type: 'object',
        properties: {
          id: { type: 'string', format: 'uuid' },
          queueName: { type: 'string' },
          jobName: { type: 'string' },
          payload: {},
          status: { type: 'string', enum: ['pending', 'active', 'completed', 'failed', 'dlq', 'delayed'] },
          attempts: { type: 'integer' },
          maxAttempts: { type: 'integer' },
          errorMessage: { type: 'string', nullable: true },
          createdAt: { type: 'string', format: 'date-time' },
          startedAt: { type: 'string', format: 'date-time', nullable: true },
          completedAt: { type: 'string', format: 'date-time', nullable: true },
        },
      },
      EnqueueRequest: {
        type: 'object',
        required: ['jobName', 'payload'],
        properties: {
          jobName: { type: 'string', minLength: 1 },
          payload: {},
          queue: { type: 'string' },
          maxAttempts: { type: 'integer', minimum: 1 },
          idempotencyKey: { type: 'string' },
          delay: { type: 'integer', minimum: 0 },
          priority: { type: 'string', enum: ['high', 'normal', 'low'] },
        },
      },
      LoginRequest: {
        type: 'object',
        required: ['username', 'password'],
        properties: {
          username: { type: 'string' },
          password: { type: 'string' },
        },
      },
    },
  },
  paths: {
    '/health': {
      get: {
        summary: 'Health check',
        tags: ['System'],
        responses: { '200': { description: 'OK', content: { 'application/json': { schema: { type: 'object', properties: { status: { type: 'string' } } } } } } },
      },
    },
    '/metrics': {
      get: {
        summary: 'Prometheus metrics',
        tags: ['System'],
        responses: { '200': { description: 'Prometheus text format metrics', content: { 'text/plain': { schema: { type: 'string' } } } } },
      },
    },
    '/jobs': {
      post: {
        summary: 'Enqueue a new job',
        tags: ['Jobs'],
        security: [{ ApiKeyAuth: [] }],
        requestBody: { required: true, content: { 'application/json': { schema: { $ref: '#/components/schemas/EnqueueRequest' } } } },
        responses: {
          '201': { description: 'Job created', content: { 'application/json': { schema: { type: 'object', properties: { jobId: { type: 'string' } } } } } },
          '400': { description: 'Validation error' },
          '401': { description: 'Unauthorized' },
        },
      },
    },
    '/jobs/{id}': {
      get: {
        summary: 'Get job by ID',
        tags: ['Jobs'],
        parameters: [{ name: 'id', in: 'path', required: true, schema: { type: 'string' } }],
        responses: {
          '200': { description: 'Job details', content: { 'application/json': { schema: { $ref: '#/components/schemas/Job' } } } },
          '404': { description: 'Not found' },
        },
      },
    },
    '/jobs/{id}/retry': {
      post: {
        summary: 'Retry a failed or DLQ job',
        tags: ['Jobs'],
        security: [{ BearerAuth: [] }],
        parameters: [{ name: 'id', in: 'path', required: true, schema: { type: 'string' } }],
        responses: {
          '200': { description: 'Job retried', content: { 'application/json': { schema: { type: 'object', properties: { success: { type: 'boolean' } } } } } },
          '400': { description: 'Job not retriable' },
          '404': { description: 'Not found' },
        },
      },
    },
    '/queues': {
      get: {
        summary: 'List queues with stats',
        tags: ['Queues'],
        security: [{ BearerAuth: [] }],
        responses: { '200': { description: 'Queue list', content: { 'application/json': { schema: { type: 'object', properties: { queues: { type: 'array', items: { type: 'object' } } } } } } } },
      },
    },
    '/queues/{name}/jobs': {
      get: {
        summary: 'List jobs for a queue',
        tags: ['Queues'],
        security: [{ BearerAuth: [] }],
        parameters: [
          { name: 'name', in: 'path', required: true, schema: { type: 'string' } },
          { name: 'status', in: 'query', schema: { type: 'string', enum: ['pending', 'active', 'completed', 'failed', 'dlq', 'delayed'] } },
          { name: 'limit', in: 'query', schema: { type: 'integer', maximum: 200 } },
          { name: 'offset', in: 'query', schema: { type: 'integer' } },
        ],
        responses: { '200': { description: 'Job list', content: { 'application/json': { schema: { type: 'object', properties: { jobs: { type: 'array', items: { $ref: '#/components/schemas/Job' } }, total: { type: 'integer' } } } } } } },
      },
    },
    '/workers': {
      get: {
        summary: 'List active workers',
        tags: ['Workers'],
        security: [{ BearerAuth: [] }],
        responses: { '200': { description: 'Worker list', content: { 'application/json': { schema: { type: 'object', properties: { workers: { type: 'array', items: { type: 'object' } } } } } } } },
      },
    },
    '/queues/{name}/dlq': {
      get: {
        summary: 'List DLQ jobs for a queue',
        tags: ['Queues'],
        security: [{ BearerAuth: [] }],
        parameters: [
          { name: 'name', in: 'path', required: true, schema: { type: 'string' } },
          { name: 'limit', in: 'query', schema: { type: 'integer' } },
          { name: 'offset', in: 'query', schema: { type: 'integer' } },
        ],
        responses: { '200': { description: 'DLQ job list' } },
      },
    },
    '/queues/{name}/dlq/requeue': {
      post: {
        summary: 'Bulk requeue DLQ jobs',
        tags: ['Queues'],
        security: [{ BearerAuth: [] }],
        parameters: [{ name: 'name', in: 'path', required: true, schema: { type: 'string' } }],
        requestBody: { content: { 'application/json': { schema: { type: 'object', properties: { jobIds: { type: 'array', items: { type: 'string' } }, all: { type: 'boolean' } } } } } },
        responses: { '200': { description: 'Requeue result', content: { 'application/json': { schema: { type: 'object', properties: { requeued: { type: 'integer' }, failed: { type: 'integer' } } } } } } },
      },
    },
    '/auth/login': {
      post: {
        summary: 'Login and get JWT token',
        tags: ['Auth'],
        requestBody: { required: true, content: { 'application/json': { schema: { $ref: '#/components/schemas/LoginRequest' } } } },
        responses: {
          '200': { description: 'JWT token', content: { 'application/json': { schema: { type: 'object', properties: { token: { type: 'string' } } } } } },
          '401': { description: 'Invalid credentials' },
          '501': { description: 'Auth not configured' },
        },
      },
    },
  },
};

export { openapiSpec };

export function setupSwagger(app: Express): void {
  app.get('/docs/json', (_req, res) => {
    res.json(openapiSpec);
  });
  app.use('/docs', swaggerUi.serve, swaggerUi.setup(openapiSpec));
}
