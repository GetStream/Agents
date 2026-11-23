-- +goose Up
SET LOCAL lock_timeout = '5s';

-- When a router last opened the session, which a close from a router that held it before leaves
-- alone, and when another shard carried it on, which keeps this row out of lists from then on.
ALTER TABLE agent_sessions
    ADD COLUMN IF NOT EXISTS opened_at timestamptz,
    ADD COLUMN IF NOT EXISTS superseded_at timestamptz;

-- +goose Down
ALTER TABLE agent_sessions DROP COLUMN IF EXISTS superseded_at, DROP COLUMN IF EXISTS opened_at;
