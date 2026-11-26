-- +goose Up
SET LOCAL lock_timeout = '5s';

-- How long the model's cached prefix stays warm: 5m or 1h. Empty leaves the provider's default.
ALTER TABLE agent_configs ADD COLUMN IF NOT EXISTS prompt_cache_ttl TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE agent_configs DROP COLUMN IF EXISTS prompt_cache_ttl;
