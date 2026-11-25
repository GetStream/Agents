-- +goose NO TRANSACTION
-- +goose Up

-- The rows the index shipper reads, changed since its mark. Built without stopping writes; a
-- build that fails is built again in Go (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_sessions_changed_idx ON agent_sessions (changed_at, customer_id, id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_changed_idx ON calls (changed_at, customer_id, id);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS calls_changed_idx;
DROP INDEX CONCURRENTLY IF EXISTS agent_sessions_changed_idx;
