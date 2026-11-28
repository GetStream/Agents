-- +goose Up
SET LOCAL lock_timeout = '5s';

-- NULL means the trunk's password is sealed under the router's key at its kek version, as
-- before. A number means it is sealed under that version of the app's data key.
ALTER TABLE stream_trunks ADD COLUMN IF NOT EXISTS bridge_password_data_key_version INTEGER;
ALTER TABLE sip_trunks ADD COLUMN IF NOT EXISTS password_data_key_version INTEGER;

-- +goose Down
ALTER TABLE sip_trunks DROP COLUMN IF EXISTS password_data_key_version;
ALTER TABLE stream_trunks DROP COLUMN IF EXISTS bridge_password_data_key_version;
