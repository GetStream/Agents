-- +goose Up
SET LOCAL lock_timeout = '5s';

-- When a guest's refresh secret stops working: 30 days after it was issued or last rotated.
-- The default dates every secret already handed out from now, without rewriting the table, and
-- covers the guests a router of the release before writes while this one rolls out.
ALTER TABLE users ADD COLUMN IF NOT EXISTS refresh_secret_expires_at timestamptz DEFAULT now() + interval '30 days';

-- The secret the last rotation spent, kept so that one presented again is seen as leaked. Named
-- *_secret_hash so no export, import or recorded change carries it (isSecret).
ALTER TABLE users ADD COLUMN IF NOT EXISTS previous_refresh_secret_hash bytea;

-- +goose Down
ALTER TABLE users DROP COLUMN IF EXISTS previous_refresh_secret_hash;
ALTER TABLE users DROP COLUMN IF EXISTS refresh_secret_expires_at;
