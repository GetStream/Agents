-- +goose NO TRANSACTION
-- +goose Up

-- A session's log lines, newest first (AgentLogs.Select with a session): on
-- agent_logs_customer_order a session with fewer lines than the page walked every line the
-- customer ever logged before it could stop.
-- A campaign's attempts, which the cascade of a campaign's delete read by scanning every
-- campaign's.
-- Built without stopping writes; a build that fails is built again in Go (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_logs_session_order
    ON agent_logs (customer_id, session_id, id DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS campaign_attempts_campaign_idx
    ON campaign_attempts (campaign_id);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS campaign_attempts_campaign_idx;
DROP INDEX CONCURRENTLY IF EXISTS agent_logs_session_order;
