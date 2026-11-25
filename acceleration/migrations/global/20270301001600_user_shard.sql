-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The shard that last served a request the user's own client sent, which the load balancer
-- picked by where they are. NULL until one has: their work stays where it lands.
ALTER TABLE users ADD COLUMN IF NOT EXISTS shard text;

-- +goose Down
ALTER TABLE users DROP COLUMN IF EXISTS shard;
