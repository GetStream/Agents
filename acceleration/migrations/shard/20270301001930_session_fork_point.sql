-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Where a fork's history out of its parent stops, so a fork carried on from its row reads the
-- same history it opened with: {"messages": true, "response_id": ...} for the parent's turns up
-- to that response (the one named, or for a whole fork the parent's last when it was made),
-- {"messages": true} for a parent that had recorded none, {"messages": false} for none. NULL is
-- a session that is not a fork, or a fork made before this was kept, which is carried on
-- without its parent's turns.
ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS fork_point jsonb;

-- +goose Down
ALTER TABLE agent_sessions DROP COLUMN IF EXISTS fork_point;
