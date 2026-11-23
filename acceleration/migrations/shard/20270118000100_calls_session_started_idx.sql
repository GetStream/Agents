-- +goose NO TRANSACTION
-- +goose Up

-- A session's calls, led by session_id: deleting a session sets session_id to NULL on its
-- calls (calls_session_id_fkey), by session_id alone, which skip-scanned every customer of
-- calls_session_idx. Built without stopping the writes of calls; a build that fails is built
-- again in Go (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_session_started_idx
    ON calls (session_id, started_at DESC) WHERE session_id IS NOT NULL;
DROP INDEX CONCURRENTLY IF EXISTS calls_session_idx;

-- +goose Down
CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_session_idx
    ON calls (customer_id, session_id, started_at DESC) WHERE session_id IS NOT NULL;
DROP INDEX CONCURRENTLY IF EXISTS calls_session_started_idx;
