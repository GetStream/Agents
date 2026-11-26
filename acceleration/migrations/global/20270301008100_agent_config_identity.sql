-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Who a config's agent is in Stream Chat for a session that names none: the agent id a new
-- conversation is kept under, and the user the agent writes as with its display name. Empty
-- names nothing, which every config made before them is.
ALTER TABLE agent_configs ADD COLUMN IF NOT EXISTS identity_agent_id TEXT NOT NULL DEFAULT '';
ALTER TABLE agent_configs ADD COLUMN IF NOT EXISTS identity_user_id TEXT NOT NULL DEFAULT '';
ALTER TABLE agent_configs ADD COLUMN IF NOT EXISTS identity_user_name TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE agent_configs DROP COLUMN IF EXISTS identity_user_name;
ALTER TABLE agent_configs DROP COLUMN IF EXISTS identity_user_id;
ALTER TABLE agent_configs DROP COLUMN IF EXISTS identity_agent_id;
