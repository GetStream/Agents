-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The shared copy of agent_sessions.fork_point (shard migration 20270301001930), since the
-- session lists read every column of a session from the copy (Store.ShipIndex).
ALTER TABLE session_index ADD COLUMN IF NOT EXISTS fork_point jsonb;

-- +goose Down
ALTER TABLE session_index DROP COLUMN IF EXISTS fork_point;
