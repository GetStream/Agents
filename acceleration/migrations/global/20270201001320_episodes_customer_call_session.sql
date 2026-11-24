-- +goose NO TRANSACTION
-- +goose Up

-- A call episode is its customer's session's, since two customers may each have a session of one
-- id. Beside episodes_call_session, which stays while a router of the release before runs. Built
-- without stopping the writes of episodes; a build that fails is built again in Go
-- (store.usageIndexes).
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS episodes_customer_call_session ON episodes (customer_id, session_id);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS episodes_customer_call_session;
