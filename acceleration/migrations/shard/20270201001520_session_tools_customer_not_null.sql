-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Checking the rows reads the whole table under a lock that stops no write. The column is then
-- made NOT NULL on the strength of that check, without reading the rows again, and the check,
-- which says no more than the column now does, is dropped.
ALTER TABLE agent_session_tools VALIDATE CONSTRAINT agent_session_tools_customer_id_present;
ALTER TABLE agent_session_tools ALTER COLUMN customer_id SET NOT NULL;
ALTER TABLE agent_session_tools DROP CONSTRAINT agent_session_tools_customer_id_present;

-- +goose Down
SET LOCAL lock_timeout = '5s';

ALTER TABLE agent_session_tools ALTER COLUMN customer_id DROP NOT NULL;
