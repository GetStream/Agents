-- +goose Up

-- The column holds which worker's lease the task is under, not a claim of its own.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = current_schema() AND table_name = 'scheduled_tasks' AND column_name = 'claim_id') THEN
        ALTER TABLE scheduled_tasks RENAME COLUMN claim_id TO leased_by;
    END IF;
END $$;
-- +goose StatementEnd

-- +goose Down

ALTER TABLE scheduled_tasks RENAME COLUMN leased_by TO claim_id;
