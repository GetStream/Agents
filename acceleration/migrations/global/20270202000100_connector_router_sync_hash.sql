-- +goose Up
SET LOCAL lock_timeout = '5s';

-- A fingerprint of the file the CLI last synced onto the row, as agent_configs.sync_hash is for
-- an agent's directory: empty when the row was never synced, or was written since by anything
-- that is not a sync, so the CLI tells an unchanged file from an edit made elsewhere.
ALTER TABLE connector_definitions ADD COLUMN IF NOT EXISTS sync_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE router_configs ADD COLUMN IF NOT EXISTS sync_hash TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE router_configs DROP COLUMN IF EXISTS sync_hash;
ALTER TABLE connector_definitions DROP COLUMN IF EXISTS sync_hash;
