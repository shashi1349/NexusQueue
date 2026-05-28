-- NexusQueue: job history & audit log
--
-- Postgres is the durable source of truth for job lifecycle events.
-- Redis is the hot-path runtime store; Postgres is the queryable record.
-- We INSERT on enqueue and UPDATE on every state transition.

CREATE TABLE IF NOT EXISTS jobs (
    id              UUID        PRIMARY KEY,
    queue_name      TEXT        NOT NULL,
    job_name        TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL
                    CHECK (status IN ('pending','active','completed','failed','delayed','dlq')),
    attempts        INTEGER     NOT NULL DEFAULT 0,
    max_attempts    INTEGER     NOT NULL DEFAULT 1,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);

-- Indexes chosen for the dashboard queries we'll add in Phase 5:
--   GET /queues                        -> count(*) GROUP BY status, queue_name
--   GET /queues/:name/jobs?status=...  -> filter by queue_name + status
--   GET /jobs (recent)                 -> ORDER BY created_at DESC LIMIT N
CREATE INDEX IF NOT EXISTS idx_jobs_status      ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_queue_name  ON jobs (queue_name);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at  ON jobs (created_at DESC);

-- Composite index: the most common dashboard query pattern is
-- "show me jobs in queue X with status Y, newest first".
-- A composite covers it without scanning either single-column index.
CREATE INDEX IF NOT EXISTS idx_jobs_queue_status_created
    ON jobs (queue_name, status, created_at DESC);
