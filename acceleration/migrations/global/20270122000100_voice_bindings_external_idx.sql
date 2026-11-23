-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Whose a provider's voice id is, which a voice passed straight through to a provider is
-- looked up by on every stream that opens, so one customer cannot speak in another's clone.
CREATE INDEX IF NOT EXISTS voice_bindings_external_idx ON voice_bindings (provider, external_id);

-- +goose Down
DROP INDEX IF EXISTS voice_bindings_external_idx;
