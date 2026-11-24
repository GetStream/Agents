-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Every row of a session's tools names its customer from now on, as every router of the release
-- before writes it. Not checked against the rows already there, so adding it reads none of them:
-- the next migration gives each the customer it lacks, and the one after checks them.
ALTER TABLE agent_session_tools
    ADD CONSTRAINT agent_session_tools_customer_id_present CHECK (customer_id IS NOT NULL) NOT VALID;

-- +goose Down
SET LOCAL lock_timeout = '5s';

ALTER TABLE agent_session_tools DROP CONSTRAINT IF EXISTS agent_session_tools_customer_id_present;
