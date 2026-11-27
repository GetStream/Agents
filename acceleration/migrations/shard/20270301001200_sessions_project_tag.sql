-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The project the session's usage is tagged with: the last one a server-side caller named, empty
-- for none. NULL on a row from before it was kept. No default: the column is added without a rewrite.
ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS project_tag text;

-- +goose Down
SET LOCAL lock_timeout = '5s';

ALTER TABLE agent_sessions DROP COLUMN IF EXISTS project_tag;
