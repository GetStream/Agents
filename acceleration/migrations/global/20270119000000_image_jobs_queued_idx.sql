-- +goose NO TRANSACTION
-- +goose Up

-- The queued jobs of each kind a claim takes, still to be drawn or decided, in the order it
-- takes them (next_attempt_at, id). On image_jobs_due the message a claim filters on was read
-- off each row, so the planner, guessing few rows pass, bitmap-scanned and sorted every due
-- entry: every dead one too, which a bitmap scan never marks to skip, so each claim read more.
-- Built without stopping the writes of jobs; a build that fails is built again in Go
-- (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS image_jobs_undecided
    ON image_jobs (next_attempt_at, id) WHERE state = 'queued' AND message = '';
CREATE INDEX CONCURRENTLY IF NOT EXISTS image_jobs_decided
    ON image_jobs (next_attempt_at, id) WHERE state = 'queued' AND message <> '';
DROP INDEX CONCURRENTLY IF EXISTS image_jobs_due;

-- +goose Down
CREATE INDEX CONCURRENTLY IF NOT EXISTS image_jobs_due
    ON image_jobs (next_attempt_at) WHERE state = 'queued';
DROP INDEX CONCURRENTLY IF EXISTS image_jobs_decided;
DROP INDEX CONCURRENTLY IF EXISTS image_jobs_undecided;
