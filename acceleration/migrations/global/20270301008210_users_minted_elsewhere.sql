-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Whether an import said another deployment's router minted this guest. Nullable: a router of the
-- release before imports the column from a row that does not hold it.
ALTER TABLE users ADD COLUMN IF NOT EXISTS minted_elsewhere boolean;

-- +goose Down
ALTER TABLE users DROP COLUMN IF EXISTS minted_elsewhere;
