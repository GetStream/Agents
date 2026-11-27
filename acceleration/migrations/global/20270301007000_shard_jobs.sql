-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Work every shard runs on its own rows, such as forgetting a user: no router writes another
-- shard's database. A job has one run per shard the deployment had when it was queued, which
-- that shard's routers claim (ShardJobs).
CREATE TABLE IF NOT EXISTS shard_jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    customer_id TEXT NOT NULL DEFAULT '',
    args JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS shard_jobs_created_idx ON shard_jobs (created_at);

CREATE TABLE IF NOT EXISTS shard_job_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES shard_jobs (id) ON DELETE CASCADE,
    shard TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'done')),
    counts JSONB,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    UNIQUE (job_id, shard)
);
CREATE INDEX IF NOT EXISTS shard_job_runs_due_idx ON shard_job_runs (shard, next_attempt_at) WHERE state = 'queued';

-- +goose Down
DROP TABLE IF EXISTS shard_job_runs;
DROP TABLE IF EXISTS shard_jobs;
