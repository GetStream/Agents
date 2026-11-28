-- +goose Up
SET LOCAL lock_timeout = '5s';

-- NULL means the secret is sealed under the router's key at kek_version, as before. A number
-- means it is sealed under that version of the app's data key (app_data_keys).
ALTER TABLE provider_keys ADD COLUMN IF NOT EXISTS api_key_data_key_version INTEGER;
ALTER TABLE custom_models ADD COLUMN IF NOT EXISTS api_key_data_key_version INTEGER;
ALTER TABLE channel_accounts ADD COLUMN IF NOT EXISTS secrets_data_key_version INTEGER;
ALTER TABLE stream_app_tokens ADD COLUMN IF NOT EXISTS token_data_key_version INTEGER;

-- +goose Down
ALTER TABLE stream_app_tokens DROP COLUMN IF EXISTS token_data_key_version;
ALTER TABLE channel_accounts DROP COLUMN IF EXISTS secrets_data_key_version;
ALTER TABLE custom_models DROP COLUMN IF EXISTS api_key_data_key_version;
ALTER TABLE provider_keys DROP COLUMN IF EXISTS api_key_data_key_version;
