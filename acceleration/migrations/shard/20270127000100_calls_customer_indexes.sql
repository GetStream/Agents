-- +goose NO TRANSACTION
-- +goose Up

-- An agent's and a campaign's calls, each led by the customer they belong to. Built without
-- stopping the writes of calls; a build that fails is built again in Go (store.usageIndexes).
-- The campaign index replaces calls_campaign_idx, which is dropped only once it is built, so a
-- campaign's calls are read by an index throughout.
CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_customer_agent_idx
    ON calls (customer_id, agent_id, started_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_customer_campaign_idx
    ON calls (customer_id, campaign_id, started_at DESC) WHERE campaign_id IS NOT NULL;
DROP INDEX CONCURRENTLY IF EXISTS calls_campaign_idx;

-- +goose Down
CREATE INDEX CONCURRENTLY IF NOT EXISTS calls_campaign_idx
    ON calls (campaign_id, started_at DESC) WHERE campaign_id IS NOT NULL;
DROP INDEX CONCURRENTLY IF EXISTS calls_customer_campaign_idx;
DROP INDEX CONCURRENTLY IF EXISTS calls_customer_agent_idx;
