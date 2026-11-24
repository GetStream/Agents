-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Whether a server-side caller named the session's project, whose usage is then tagged with it.
-- NULL on a row from before it was kept. No default: the column is added without a rewrite.
ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS project_server_side boolean;

-- +goose Down
SET LOCAL lock_timeout = '5s';

ALTER TABLE agent_sessions DROP COLUMN IF EXISTS project_server_side;
