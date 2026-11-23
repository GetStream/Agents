-- +goose NO TRANSACTION
-- +goose Up

-- A session row by its customer and id, which is what saving one conflicts on: a row of the same
-- id another customer holds is refused rather than overwritten. Built without stopping the writes
-- of sessions; a build that fails is built again in Go (store.usageIndexes).
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS agent_sessions_customer_id_id_key
    ON agent_sessions (customer_id, id);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS agent_sessions_customer_id_id_key;
