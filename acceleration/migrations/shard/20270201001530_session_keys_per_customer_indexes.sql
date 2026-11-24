-- +goose NO TRANSACTION
-- +goose Up

-- The keys of a session's rows by its customer and its id, which the next migration makes the
-- primary keys of, built without stopping the writes of any table. A build that fails leaves an
-- invalid index of its name behind, which no key can be made of and which IF NOT EXISTS would
-- keep: this runs again only after one of its statements failed, so each is dropped and built
-- again. agent_sessions' is built last, so a failure before it costs no rebuild of that one.
DROP INDEX CONCURRENTLY IF EXISTS agent_session_tools_customer_id_session_id_key;
CREATE UNIQUE INDEX CONCURRENTLY agent_session_tools_customer_id_session_id_key
    ON agent_session_tools (customer_id, session_id);
DROP INDEX CONCURRENTLY IF EXISTS agent_branch_messages_customer_key;
CREATE UNIQUE INDEX CONCURRENTLY agent_branch_messages_customer_key
    ON agent_branch_messages (customer_id, session_id, original_message_id);
-- A session by its id alone, which its key will no longer lead with: what another customer's
-- session of the id in the same Stream app is found by. A build that fails is built again in Go
-- (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS agent_sessions_id_idx ON agent_sessions (id);
DROP INDEX CONCURRENTLY IF EXISTS agent_sessions_customer_id_id_key;
CREATE UNIQUE INDEX CONCURRENTLY agent_sessions_customer_id_id_key ON agent_sessions (customer_id, id);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS agent_sessions_customer_id_id_key;
DROP INDEX CONCURRENTLY IF EXISTS agent_sessions_id_idx;
DROP INDEX CONCURRENTLY IF EXISTS agent_branch_messages_customer_key;
DROP INDEX CONCURRENTLY IF EXISTS agent_session_tools_customer_id_session_id_key;
