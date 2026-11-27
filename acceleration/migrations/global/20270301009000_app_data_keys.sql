-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Each app's data key, wrapped by the Cloud KMS key auth.app_keys_kms_key names. The router
-- keeps only the wrapped form, so opening a secret sealed under it takes a KMS call.
CREATE TABLE IF NOT EXISTS app_data_keys (
    customer_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    wrapped_key BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, version)
);

-- NULL means the credentials are sealed under the router's key at credentials_kek_version, as
-- before. A number means they are sealed under that version of the app's data key.
ALTER TABLE connector_connections ADD COLUMN IF NOT EXISTS credentials_data_key_version INTEGER;

-- +goose Down
ALTER TABLE connector_connections DROP COLUMN IF EXISTS credentials_data_key_version;
DROP TABLE IF EXISTS app_data_keys;
