-- +goose Up
SET LOCAL lock_timeout = '5s';

-- NULL means the parked bridge's trunk password is sealed under the router's key at its kek
-- version, as before. A number means it is sealed under that version of the app's data key.
ALTER TABLE call_bridges ADD COLUMN IF NOT EXISTS trunk_password_data_key_version INTEGER;

-- +goose Down
ALTER TABLE call_bridges DROP COLUMN IF EXISTS trunk_password_data_key_version;
