-- +goose Up
SET LOCAL lock_timeout = '5s';

-- When a session's or a call's row last changed in any way, a pin or a review included, which
-- updated_at leaves alone: what the shard's index shipper reads from (Store.ShipIndex).
ALTER TABLE agent_sessions ADD COLUMN IF NOT EXISTS changed_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE calls ADD COLUMN IF NOT EXISTS changed_at timestamptz NOT NULL DEFAULT now();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION mark_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.changed_at := clock_timestamp();
    RETURN NEW;
END $$;
-- +goose StatementEnd

CREATE OR REPLACE TRIGGER agent_sessions_changed BEFORE UPDATE ON agent_sessions
FOR EACH ROW EXECUTE FUNCTION mark_changed();
CREATE OR REPLACE TRIGGER calls_changed BEFORE UPDATE ON calls
FOR EACH ROW EXECUTE FUNCTION mark_changed();

-- +goose Down
DROP TRIGGER IF EXISTS calls_changed ON calls;
DROP TRIGGER IF EXISTS agent_sessions_changed ON agent_sessions;
DROP FUNCTION IF EXISTS mark_changed();
ALTER TABLE calls DROP COLUMN IF EXISTS changed_at;
ALTER TABLE agent_sessions DROP COLUMN IF EXISTS changed_at;
