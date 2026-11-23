-- +goose Up
SET LOCAL lock_timeout = '5s';

-- A run's router holds it until lease_until, as a queue row is held, so the column is named
-- as the queue's is. A router of the release before reads held_until and fails until it is
-- replaced; a run it was having is written off by the next one's Abandon. An import from an
-- older release carries held_until over (store.renameImported). Run again on a database that
-- has it already, as an adopted one does, it changes nothing.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = current_schema() AND table_name = 'simulation_runs' AND column_name = 'held_until') THEN
        ALTER TABLE simulation_runs RENAME COLUMN held_until TO lease_until;
    END IF;
END $$;
-- +goose StatementEnd
ALTER INDEX IF EXISTS simulation_runs_held_idx RENAME TO simulation_runs_lease_idx;

-- +goose Down
SET LOCAL lock_timeout = '5s';

ALTER TABLE simulation_runs RENAME COLUMN lease_until TO held_until;
ALTER INDEX IF EXISTS simulation_runs_lease_idx RENAME TO simulation_runs_held_idx;
