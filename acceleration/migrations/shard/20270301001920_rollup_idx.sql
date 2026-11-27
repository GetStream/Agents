-- +goose NO TRANSACTION
-- +goose Up

-- What the stats shipper reads of every customer in an hour (Store.ShipStats). Built without
-- stopping writes; a build that fails is built again in Go (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_sessions_created_idx ON agent_sessions (created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_sessions_open_idx ON agent_sessions (created_at) WHERE closed_at IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_sessions_closed_idx ON agent_sessions (closed_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_responses_created_idx ON agent_responses (created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_response_items_tool_result_idx ON agent_response_items (at) WHERE kind = 'tool_result';
CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_started_idx ON calls (started_at);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS calls_started_idx;
DROP INDEX CONCURRENTLY IF EXISTS agent_response_items_tool_result_idx;
DROP INDEX CONCURRENTLY IF EXISTS agent_responses_created_idx;
DROP INDEX CONCURRENTLY IF EXISTS agent_sessions_closed_idx;
DROP INDEX CONCURRENTLY IF EXISTS agent_sessions_open_idx;
DROP INDEX CONCURRENTLY IF EXISTS agent_sessions_created_idx;
