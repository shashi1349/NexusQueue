# NexusQueue

A distributed task queue engine built on **Redis + Postgres**, written in TypeScript.
Inspired by BullMQ and Celery, NexusQueue pairs Redis's speed with Postgres's durability
to give you both real-time throughput and a permanent audit trail.

Built phase-by-phase as a learning project with clear architectural boundaries.

## Architecture

```mermaid
flowchart TD
    Client["Client / Producer"]
    Server["Server (REST API)"]
    Redis["Redis"]
    Postgres["Postgres"]
    Worker["Worker"]
    Dashboard["React Dashboard"]
    PubSub["Redis PUB/SUB"]

    Client -->|"POST /jobs"| Server
    Server -->|"LPUSH queue"| Redis
    Server -->|"INSERT job"| Postgres
    Worker -->|"BLMOVE pull"| Redis
    Worker -->|"UPDATE state"| Postgres
    Server -->|"PUBLISH events"| PubSub
    Dashboard -->|"REST API"| Server
    Dashboard <-->|"WebSocket"| Server
    Server <-->|"SUBSCRIBE"| PubSub

    subgraph Worker Process
        direction TB
        Handler["Handler Registry"]
        Scheduler["Scheduler (delayed + cron)"]
        Janitor["Janitor (dead worker recovery)"]
        RateLimiter["Rate Limiter (token bucket)"]
    end

    Worker --- Handler
    Worker --- Scheduler
    Worker --- Janitor
    Worker --- RateLimiter
```

## Features

### Phase 1 - Foundation

- Producer SDK with Express REST API
- Worker with handler registry and lifecycle state transitions
- Dual-write to Redis (hot path) and Postgres (durable audit log)
- Job state machine: `waiting` -> `active` -> `completed` / `failed`

### Phase 2 - Reliability

- At-least-once delivery via `BLMOVE` with explicit ACK
- Automatic retries with configurable exponential backoff
- Dead Letter Queue (DLQ) for permanently failed jobs
- Idempotency keys to prevent duplicate job creation

### Phase 3 - Advanced Scheduling

- Delayed jobs via Redis sorted sets (ZRANGEBYSCORE polling)
- Weighted priority queues (critical > high > normal > low)
- Cron jobs with recurring schedule support
- Token-bucket rate limiting per queue

### Phase 4 - Worker Coordination

- Heartbeat-based worker liveness detection
- Janitor process for dead worker recovery (re-queues orphaned jobs)
- Graceful shutdown with in-flight job draining
- Concurrent job processing (configurable concurrency per worker)

### Phase 5 - Observability Dashboard

- REST API for queue, worker, and DLQ inspection
- WebSocket real-time event streaming via Redis PUB/SUB
- React + Vite + Tailwind dashboard with live updates
- Throughput charts powered by Recharts

### Phase 6 - Production Polish

- Structured JSON logging with pino (pretty output in development)
- Prometheus `/metrics` endpoint with custom counters, gauges, and histograms
- API key authentication for job submission endpoints
- JWT authentication for dashboard/admin endpoints
- OpenAPI 3.0 spec with Swagger UI at `/docs`

## API Reference

| Method | Endpoint | Description | Auth |
|--------|----------|-------------|------|
| `POST` | `/jobs` | Enqueue a new job | API Key |
| `GET` | `/jobs/:id` | Get job details by ID | None |
| `GET` | `/queues` | List all queues with depth stats | JWT |
| `GET` | `/queues/:name/jobs` | List jobs for a specific queue | JWT |
| `POST` | `/jobs/:id/retry` | Retry a failed or DLQ job | JWT |
| `GET` | `/workers` | List active workers with heartbeat info | JWT |
| `GET` | `/queues/:name/dlq` | List dead-letter queue jobs | JWT |
| `POST` | `/queues/:name/dlq/requeue` | Bulk requeue DLQ jobs | JWT |
| `POST` | `/auth/login` | Authenticate and receive a JWT token | None |
| `GET` | `/health` | Liveness check | None |
| `GET` | `/metrics` | Prometheus metrics (text format) | None |
| `GET` | `/docs` | Swagger UI (interactive API docs) | None |
| `GET` | `/docs/json` | OpenAPI 3.0 spec as JSON | None |

## Configuration

All configuration is through environment variables. Copy `.env.example` to `.env` to get started.

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `DATABASE_URL` | `postgres://nexus:nexus@localhost:5432/nexusqueue` | Postgres connection string |
| `SERVER_PORT` | `3000` | HTTP server port |
| `SERVER_HOST` | `0.0.0.0` | HTTP server bind address |
| `WORKER_QUEUE` | `default` | Queue name for the worker to pull from |
| `WORKER_CONCURRENCY` | `5` | Number of concurrent jobs per worker |
| `WORKER_ID` | Auto-generated UUID | Unique identifier for this worker instance |
| `JANITOR_ENABLED` | `true` | Enable/disable dead worker detection |
| `JANITOR_INTERVAL_MS` | `30000` | How often the janitor checks for dead workers (ms) |
| `LOG_LEVEL` | `info` | Pino log level (trace, debug, info, warn, error, fatal) |
| `NODE_ENV` | `development` | Set to `production` for JSON log output |
| `API_KEYS` | _(disabled)_ | Comma-separated list of valid API keys |
| `JWT_SECRET` | _(disabled)_ | Secret for signing/verifying JWT tokens |
| `DASHBOARD_USER` | _(disabled)_ | Username for `/auth/login` |
| `DASHBOARD_PASSWORD` | _(disabled)_ | Password for `/auth/login` |

> **Note:** When `API_KEYS`, `JWT_SECRET`, `DASHBOARD_USER`, or `DASHBOARD_PASSWORD` are not set,
> authentication is disabled and all endpoints are open. This makes local development frictionless.

## Quickstart

### Prerequisites

- Node.js 18+
- Docker and Docker Compose (for Redis and Postgres)

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts Redis and Postgres with the schema automatically applied.

### 2. Install and build

```bash
cp .env.example .env
npm install
npm run build
```

### 3. Start the server

```bash
npm run dev:server
```

The REST API is now available at `http://localhost:3000`.

### 4. Start a worker

In a separate terminal:

```bash
npm run dev:worker
```

### 5. Enqueue a job

```bash
curl -X POST http://localhost:3000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "jobName": "echo",
    "payload": { "message": "Hello, NexusQueue!" },
    "queue": "default"
  }'
```

### 6. Check job status

```bash
curl http://localhost:3000/jobs/<jobId>
```

### 7. View the dashboard

```bash
npm run dev:dashboard
```

Open `http://localhost:5173` to see queues, workers, and real-time job events.

### 8. Explore the API docs

Navigate to `http://localhost:3000/docs` for the interactive Swagger UI.

## Project Layout

```
shared/     @nexusqueue/shared    Types, Redis keys, ioredis + pg factories, logger
server/     @nexusqueue/server    Producer SDK, Express REST API, WebSocket, metrics, auth
worker/     @nexusqueue/worker    BLMOVE pull loop, handler registry, scheduler, janitor
dashboard/  React + Vite app      Real-time monitoring UI with Tailwind + Recharts
tests/      Vitest suite          Integration tests covering all 6 phases (92 tests)
examples/   smoke.ts              End-to-end demo script
infra/      postgres/*.sql        Database migrations
```

## Trade-offs vs BullMQ

NexusQueue is a learning project that makes different architectural choices from BullMQ.
Here is how they compare:

| Concern | NexusQueue | BullMQ |
|---------|-----------|--------|
| **Data stores** | Redis + Postgres dual-write | Redis only |
| **Audit trail** | Durable Postgres history survives Redis flushes | Ephemeral; lost if Redis restarts without persistence |
| **API model** | Separate HTTP server process (REST + WebSocket) | In-process library; no separate server |
| **Dead worker detection** | Explicit Janitor process with heartbeat monitoring | Built-in stalled job check via Redis scripts |
| **Scheduling** | Sorted sets with polling + cron support | Built-in delayed/repeatable jobs via Lua scripts |
| **Rate limiting** | Token-bucket algorithm per queue | Built-in group-based rate limiter |
| **Maturity** | Educational project with clear phase-by-phase design | Production-hardened, battle-tested library |
| **Dependencies** | Redis + Postgres required | Redis only |

**When to use NexusQueue:** You want to understand how a distributed task queue works from the ground up, or you need a durable audit trail in a relational database alongside your queue.

**When to use BullMQ:** You need a production-ready, well-supported task queue with minimal infrastructure requirements and proven scalability.

## License

MIT
