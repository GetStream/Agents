-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The column holds which worker's lease the task is under, not a claim of its own.
ALTER TABLE scheduled_tasks RENAME COLUMN claim_id TO leased_by;

-- +goose Down

ALTER TABLE scheduled_tasks RENAME COLUMN leased_by TO claim_id;
