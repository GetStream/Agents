-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Where the agent's work may be placed (shardmap.Policy), such as eu_only. Empty follows the
-- app's routing_policy setting.
ALTER TABLE agent_configs ADD COLUMN IF NOT EXISTS routing_policy TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE agent_configs DROP COLUMN IF EXISTS routing_policy;
