-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The rows already there are checked against the keys of their sessions. Each check reads its
-- table whole under a lock that stops no write of it or of agent_sessions. A row whose customer
-- is not its session's fails it, and with it the migration, which the router's next start tries
-- again.
ALTER TABLE agent_session_tools VALIDATE CONSTRAINT agent_session_tools_customer_id_session_id_fkey;
ALTER TABLE agent_branch_messages VALIDATE CONSTRAINT agent_branch_messages_customer_id_session_id_fkey;
ALTER TABLE agent_branch_switches VALIDATE CONSTRAINT agent_branch_switches_customer_id_session_id_fkey;
ALTER TABLE agent_responses VALIDATE CONSTRAINT agent_responses_customer_id_session_id_fkey;
ALTER TABLE calls VALIDATE CONSTRAINT calls_customer_id_session_id_fkey;

-- +goose Down
