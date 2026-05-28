# NexusQueue

A distributed task queue engine (BullMQ/Celery-style) built on Redis + Postgres,
in TypeScript. Built phase-by-phase as a learning project.

> **Phase 1 status:** foundation only. Single-worker, at-most-once delivery,
> no retries, no scheduling, no dashboard. Each subsequent phase fills in one
> production concern.

## Architecture (Phase 1)

```
                   +------------+        LPUSH         +-------------------+
   client --HTTP-->|  /server   |--------------------->|  Redis            |
                   |  Producer  |   nexus:queue:NAME   |  - List per queue |
                   +------------+                      |  - Hash per job   |
                          |                            +---------+---------+
                          | INSERT jobs                          |
                          v                                      | BRPOP
                   +-------------------+                         v
                   |  Postgres         |               +--------------------+
                   |  jobs (history)   |<--UPDATE------|  /worker           |
                   |  status/attempts  |   on each     |  - handler registry|
                   +-------------------+   transition  |  - lifecycle xitions|
                                                       +--------------------+
```

* **Redis** is the hot-path runtime store: pending queue and live job state.
* **Postgres** is the durable audit log + queryable history.
* **Producer** writes Postgres first (durability), then Redis (visibility).
* **Worker** updates both on every state transition.

## Layout

```
shared/   @nexusqueue/shared   types, Redis keys, ioredis + pg factories, jobs DAO
server/   @nexusqueue/server   Producer SDK class + Express REST API
worker/   @nexusqueue/worker   BRPOP pull loop, handler registry
examples/ smoke.ts             end-to-end demo (Phase 1)
infra/    postgres/001_init.sql jobs table + indexes
```

## Quickstart

```bash
# 1. Start Redis + Postgres
docker compose up -d

# 2. Copy env
cp .env.example .env

# 3. Install + build
npm install
npm run build

# 4. (DB schema is auto-applied by docker-compose on first run.
#    If you connect to an external Postgres, run:)
# npm run db:init

# 5. Run the end-to-end smoke test
npm run smoke
```

In separate terminals you can also run:

```bash
npm run dev:server   # producer HTTP API on :3000
npm run dev:worker   # worker on queue=$WORKER_QUEUE (default: "default")
```

Enqueue via HTTP:

```bash
curl -X POST localhost:3000/jobs \
  -H 'content-type: application/json' \
  -d '{"jobName":"echo","payload":{"hi":"there"},"queue":"default"}'

curl localhost:3000/jobs/<jobId>
```

## Phase roadmap

| Phase | Theme                       | Status |
|-------|-----------------------------|--------|
| 1     | Foundation                  | done   |
| 2     | Reliability (ACK, retries, DLQ, idempotency) | next   |
| 3     | Advanced scheduling (delayed, priority, cron, rate limit) | |
| 4     | Worker coordination (heartbeats, janitor, graceful, concurrency) | |
| 5     | Dashboard (REST + WebSocket + React) | |
| 6     | Production polish (pino, metrics, auth, OpenAPI) | |
| 7     | Load testing + deployment   | |

## Known Phase 1 gaps (intentional)

These exist so Phase 2+ can clearly justify itself.

* **At-most-once delivery.** A worker crash between BRPOP and handler completion drops the job.
* **No retries.** `maxAttempts > 1` is accepted by the SDK but not honored yet.
* **No idempotency.** Re-enqueueing the same logical work creates duplicate jobs.
* **No graceful shutdown for in-flight work.** Worker stops between iterations only.
* **Single-flight worker.** `WORKER_CONCURRENCY` is read but ignored.
