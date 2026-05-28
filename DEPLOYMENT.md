# Deployment Guide

This guide covers deploying NexusQueue to production using free-tier cloud services or Docker Compose on your own infrastructure.

---

## Prerequisites

Before deploying, ensure you have:

- A GitHub account with this repository accessible
- Node.js 22+ installed locally (for building, if not using Docker)
- Docker and Docker Compose installed (for the Docker Compose option)
- Access to one of: Render, Railway, or a VPS for the backend services
- A Vercel account for the dashboard (optional)

---

## Option 1: Docker Compose (Recommended)

The easiest way to deploy all services together is with the production Docker Compose file.

### Setup

1. Clone the repository on your server:

```bash
git clone https://github.com/your-org/NexusQueue.git
cd NexusQueue
```

2. Create a `.env` file from the example:

```bash
cp .env.example .env
```

3. Edit `.env` with production values:

```env
# Postgres credentials
POSTGRES_USER=nexus
POSTGRES_PASSWORD=your-secure-password-here
POSTGRES_DB=nexusqueue

# Auth
API_KEYS=key1,key2,key3
JWT_SECRET=your-jwt-secret-min-32-chars
DASHBOARD_USER=admin
DASHBOARD_PASSWORD=your-dashboard-password

# Worker
WORKER_QUEUE=default
WORKER_CONCURRENCY=5

# Logging
LOG_LEVEL=info
```

4. Start all services:

```bash
docker compose -f docker-compose.prod.yml up -d --scale worker=3
```

This starts:
- 1 Redis instance (internal network only)
- 1 Postgres instance (internal network only)
- 1 Server (exposed on port 3000)
- 3 Worker replicas (internal network only)
- 1 Dashboard (exposed on port 80)

5. Verify deployment:

```bash
# Check all services are healthy
docker compose -f docker-compose.prod.yml ps

# Check server health
curl http://localhost:3000/health

# View logs
docker compose -f docker-compose.prod.yml logs -f server
```

### Scaling Workers

To adjust the number of worker replicas:

```bash
docker compose -f docker-compose.prod.yml up -d --scale worker=5
```

### Updating

To deploy a new version:

```bash
git pull
docker compose -f docker-compose.prod.yml build
docker compose -f docker-compose.prod.yml up -d
```

---

## Option 2: Backend on Render/Railway

Deploy the server as a web service on Render or Railway.

### Server Service

1. Create a new **Web Service** on Render (or Railway).
2. Connect your GitHub repository.
3. Configure the service:

| Setting | Value |
|---------|-------|
| Name | `nexusqueue-server` |
| Root Directory | _(leave empty - uses repo root)_ |
| Build Command | `npm ci && npm run build` |
| Start Command | `node server/dist/index.js` |
| Environment | Node |
| Node Version | 22 |

4. Set environment variables:

| Variable | Value |
|----------|-------|
| `REDIS_URL` | Your Redis Cloud connection URL |
| `DATABASE_URL` | Your Neon Postgres connection string |
| `SERVER_PORT` | `3000` (Render auto-detects, Railway uses PORT) |
| `SERVER_HOST` | `0.0.0.0` |
| `NODE_ENV` | `production` |
| `API_KEYS` | Comma-separated list of API keys |
| `JWT_SECRET` | A random 32+ character secret |
| `DASHBOARD_USER` | Admin username |
| `DASHBOARD_PASSWORD` | Admin password |
| `LOG_LEVEL` | `info` |

5. Deploy. The server will be available at your Render/Railway URL (e.g., `https://nexusqueue-server.onrender.com`).

### Worker Service

1. Create a new **Background Worker** service (Render) or **Worker** service (Railway).
2. Connect the same GitHub repository.
3. Configure the service:

| Setting | Value |
|---------|-------|
| Name | `nexusqueue-worker` |
| Root Directory | _(leave empty - uses repo root)_ |
| Build Command | `npm ci && npm run build` |
| Start Command | `node worker/dist/index.js` |
| Environment | Node |
| Node Version | 22 |

4. Set environment variables:

| Variable | Value |
|----------|-------|
| `REDIS_URL` | Same Redis Cloud URL as server |
| `DATABASE_URL` | Same Neon Postgres URL as server |
| `WORKER_QUEUE` | `default` (or your queue name) |
| `WORKER_CONCURRENCY` | `5` |
| `NODE_ENV` | `production` |
| `LOG_LEVEL` | `info` |

5. Deploy. The worker will start pulling jobs immediately.

**Scaling:** On Render, create multiple worker services. On Railway, use the service scaling settings.

---

## Option 3: Dashboard on Vercel

Deploy the React dashboard as a static site on Vercel.

1. Sign in to [Vercel](https://vercel.com) and click **Add New Project**.
2. Import your GitHub repository.
3. Configure the project:

| Setting | Value |
|---------|-------|
| Framework Preset | Vite |
| Root Directory | `dashboard` |
| Build Command | `npm run build` |
| Output Directory | `dist` |

4. Set environment variables:

| Variable | Value |
|----------|-------|
| `VITE_API_URL` | URL of your deployed server (e.g., `https://nexusqueue-server.onrender.com`) |

5. Deploy. The dashboard will be available at your Vercel URL.

**Important:** The dashboard communicates with the server via REST and WebSocket. Ensure your server's CORS configuration allows requests from the Vercel domain.

---

## Redis Cloud Free Tier

NexusQueue works with the Redis Cloud free tier (30MB, no credit card required).

### Setup

1. Create an account at [redis.com/try-free](https://redis.com/try-free/).
2. Create a new **Fixed** subscription (free tier).
3. Create a new database:
   - Name: `nexusqueue`
   - Module: None required
   - Data persistence: Enable AOF (recommended for durability)
4. After creation, find the **Public endpoint** and **Default user password** in the database configuration page.
5. Construct your connection URL:

```
redis://default:YOUR_PASSWORD@redis-12345.c1.us-east-1-2.ec2.redns.redis-cloud.com:12345
```

6. Use this as your `REDIS_URL` environment variable.

### Recommendations

- Enable AOF persistence to survive Redis restarts without job loss.
- The 30MB free tier supports approximately 100,000 small jobs in the queue simultaneously.
- Monitor memory usage in the Redis Cloud dashboard. If approaching the limit, scale up or ensure workers are processing jobs fast enough.

---

## Neon Postgres Free Tier

NexusQueue uses Postgres for durable job history. Neon provides a generous free tier.

### Setup

1. Create an account at [neon.tech](https://neon.tech).
2. Create a new project:
   - Name: `nexusqueue`
   - Postgres version: 16
   - Region: Choose the closest to your server deployment
3. Copy the connection string from the dashboard. It looks like:

```
postgres://username:password@ep-cool-name-123456.us-east-2.aws.neon.tech/neondb?sslmode=require
```

4. Run the database migrations. Connect using `psql` or any Postgres client and execute the SQL files in order:

```bash
psql "YOUR_CONNECTION_STRING" -f infra/postgres/001_init.sql
```

Alternatively, copy the contents of `infra/postgres/001_init.sql` into the Neon SQL Editor in their web console.

5. Use the connection string as your `DATABASE_URL` environment variable.

### Recommendations

- Neon's free tier includes 0.5 GB storage and auto-suspends after 5 minutes of inactivity. The first query after suspension will have slightly higher latency (cold start).
- For production workloads, consider upgrading to a paid plan for always-on compute.
- The `?sslmode=require` parameter is required for Neon connections.

---

## Environment Variables Reference

### Server

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `REDIS_URL` | Yes | `redis://localhost:6379` | Redis connection string |
| `DATABASE_URL` | Yes | _(none)_ | Postgres connection string |
| `SERVER_PORT` | No | `3000` | HTTP port |
| `SERVER_HOST` | No | `0.0.0.0` | Bind address |
| `NODE_ENV` | No | `development` | Set to `production` for JSON logs |
| `LOG_LEVEL` | No | `info` | Pino log level |
| `API_KEYS` | No | _(disabled)_ | Comma-separated valid API keys |
| `JWT_SECRET` | No | _(disabled)_ | JWT signing secret |
| `DASHBOARD_USER` | No | _(disabled)_ | Login username |
| `DASHBOARD_PASSWORD` | No | _(disabled)_ | Login password |

### Worker

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `REDIS_URL` | Yes | `redis://localhost:6379` | Redis connection string |
| `DATABASE_URL` | Yes | _(none)_ | Postgres connection string |
| `WORKER_QUEUE` | No | `default` | Queue to pull jobs from |
| `WORKER_CONCURRENCY` | No | `5` | Max concurrent job handlers |
| `WORKER_ID` | No | Auto-generated UUID | Unique worker identifier |
| `NODE_ENV` | No | `development` | Set to `production` for JSON logs |
| `LOG_LEVEL` | No | `info` | Pino log level |
| `JANITOR_ENABLED` | No | `true` | Enable dead worker detection |
| `JANITOR_INTERVAL_MS` | No | `30000` | Janitor polling interval (ms) |

### Dashboard

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `VITE_API_URL` | Yes | `http://localhost:3000` | Server URL for API calls |

---

## DNS and Domain Configuration

If you are using a custom domain for your deployment:

### Server

Point an A record (for IP) or CNAME record (for hostname) to your server's address:

```
api.yourdomain.com  CNAME  nexusqueue-server.onrender.com
```

### Dashboard

On Vercel, add your custom domain in the project settings. Vercel provides automatic SSL:

```
queue.yourdomain.com  CNAME  your-project.vercel.app
```

### CORS Configuration

When the dashboard and server are on different domains, ensure the server allows cross-origin requests from the dashboard domain. The NexusQueue server includes CORS headers by default, but you may need to configure the allowed origins if using custom domains.

Set the `CORS_ORIGIN` environment variable on the server if you need to restrict origins:

```
CORS_ORIGIN=https://queue.yourdomain.com
```

If not set, the server allows all origins (suitable for development, not recommended for production).

---

## Monitoring with Grafana Cloud

NexusQueue exposes Prometheus metrics at `/metrics`. You can visualize these with Grafana Cloud's free tier.

### Setup

1. Create a free account at [grafana.com](https://grafana.com/auth/sign-up/create-user).
2. In your Grafana Cloud instance, go to **Connections > Data Sources > Add data source**.
3. Select **Prometheus**.
4. Set the URL to your server's metrics endpoint:

```
https://api.yourdomain.com/metrics
```

Or if using Grafana Agent/Alloy for scraping, configure it to scrape your server:

```yaml
prometheus.scrape "nexusqueue" {
  targets = [{
    __address__ = "nexusqueue-server.onrender.com:443",
    __scheme__  = "https",
  }]
  metrics_path = "/metrics"
  scrape_interval = "15s"
}
```

### Suggested Dashboard Panels

Create a new dashboard with the following panels:

| Panel | Metric | Visualization |
|-------|--------|---------------|
| Enqueue Rate | `rate(nexusqueue_jobs_enqueued_total[5m])` | Time series |
| Active Workers | `nexusqueue_workers_active` | Stat |
| Queue Depth | `nexusqueue_queue_depth` | Bar gauge (by queue) |
| Job Processing Rate | `rate(nexusqueue_jobs_completed_total[5m])` | Time series |
| Error Rate | `rate(nexusqueue_jobs_failed_total[5m])` | Time series |
| Enqueue Latency p50 | `histogram_quantile(0.5, nexusqueue_enqueue_duration_seconds_bucket)` | Time series |
| Enqueue Latency p95 | `histogram_quantile(0.95, nexusqueue_enqueue_duration_seconds_bucket)` | Time series |
| DLQ Size | `nexusqueue_dlq_depth` | Stat (by queue) |
| Worker Concurrency Usage | `nexusqueue_worker_active_jobs / nexusqueue_worker_concurrency` | Gauge |

### Alerting

Set up alerts for critical conditions:

- **Queue depth > 10,000** for more than 5 minutes (workers not keeping up)
- **Error rate > 5%** over 1 minute (handler failures)
- **DLQ size increasing** (jobs exhausting retries)
- **Active workers = 0** (all workers down)

---

## Production Checklist

Before going live, verify:

- [ ] `API_KEYS` is set with strong, unique keys
- [ ] `JWT_SECRET` is set with a random 32+ character value
- [ ] `DASHBOARD_USER` and `DASHBOARD_PASSWORD` are configured
- [ ] Redis has AOF persistence enabled
- [ ] Postgres connection uses SSL (`?sslmode=require`)
- [ ] Server is behind HTTPS (TLS termination at load balancer or reverse proxy)
- [ ] CORS is configured to allow only your dashboard domain
- [ ] Worker concurrency is tuned for your workload
- [ ] Monitoring/alerting is configured
- [ ] Database migrations have been applied
- [ ] Healthcheck endpoint (`/health`) is monitored
