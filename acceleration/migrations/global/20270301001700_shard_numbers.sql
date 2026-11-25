-- +goose Up
SET LOCAL lock_timeout = '5s';

-- A number for each shard, by its name, that sets apart the ids its own tables draw from a
-- sequence of theirs (store.logRangeBits), so one id names one row in every database.
CREATE TABLE IF NOT EXISTS shard_numbers (
    name text PRIMARY KEY,
    number integer GENERATED ALWAYS AS IDENTITY (MINVALUE 1 MAXVALUE 2047) UNIQUE
);

-- +goose Down
DROP TABLE IF EXISTS shard_numbers;
