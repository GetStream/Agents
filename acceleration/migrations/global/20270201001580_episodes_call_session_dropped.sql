-- +goose NO TRANSACTION
-- +goose Up

-- Two customers may each have a session of one id, and each a call episode of it: an episode is
-- one per session of a customer's (episodes_customer_call_session) and no longer one per id.
-- Dropped without stopping the writes of episodes.
DROP INDEX CONCURRENTLY IF EXISTS episodes_call_session;

-- +goose Down
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS episodes_call_session ON episodes (session_id);
