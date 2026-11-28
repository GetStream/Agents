-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Which version of the app's data key seals each secret. NULL means the secret is sealed
-- under the router's key at the row's kek_version, as before. A number means it is sealed
-- under that version of the app's data key. The row keeps a kek_version of 1 or more even
-- then, since the tables' CHECKs ask for one beside every sealed secret.
ALTER TABLE connector_oauth_clients ADD COLUMN IF NOT EXISTS secret_data_key_version INTEGER;
ALTER TABLE connector_oauth_clients ADD COLUMN IF NOT EXISTS signing_secret_data_key_version INTEGER;
ALTER TABLE connector_config_tokens ADD COLUMN IF NOT EXISTS tokens_data_key_version INTEGER;
ALTER TABLE connector_authorization_attempts ADD COLUMN IF NOT EXISTS attempt_data_key_version INTEGER;
ALTER TABLE connector_event_destinations ADD COLUMN IF NOT EXISTS secret_data_key_version INTEGER;
ALTER TABLE connector_event_destinations ADD COLUMN IF NOT EXISTS previous_secret_data_key_version INTEGER;
ALTER TABLE connection_event_subscriptions ADD COLUMN IF NOT EXISTS secret_data_key_version INTEGER;

-- +goose Down
ALTER TABLE connection_event_subscriptions DROP COLUMN IF EXISTS secret_data_key_version;
ALTER TABLE connector_event_destinations DROP COLUMN IF EXISTS previous_secret_data_key_version;
ALTER TABLE connector_event_destinations DROP COLUMN IF EXISTS secret_data_key_version;
ALTER TABLE connector_authorization_attempts DROP COLUMN IF EXISTS attempt_data_key_version;
ALTER TABLE connector_config_tokens DROP COLUMN IF EXISTS tokens_data_key_version;
ALTER TABLE connector_oauth_clients DROP COLUMN IF EXISTS signing_secret_data_key_version;
ALTER TABLE connector_oauth_clients DROP COLUMN IF EXISTS secret_data_key_version;
