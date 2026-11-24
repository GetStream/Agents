-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Whose session a row's tools are, since two customers may each have a session of one id. Empty
-- on the rows from before it was kept, and on those a router of the release before writes.
ALTER TABLE agent_session_tools ADD COLUMN IF NOT EXISTS customer_id text;

-- +goose Down
SET LOCAL lock_timeout = '5s';

ALTER TABLE agent_session_tools DROP COLUMN IF EXISTS customer_id;
