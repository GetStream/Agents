-- +goose Up
SET LOCAL lock_timeout = '5s';

-- When every step of a guest's claim ran: its sessions moved and the account joined their
-- channels. NULL while a claim that set claimed_by has not got that far, so the same claim sent
-- again runs its steps again; once set, the same claim sent again has nothing left to do.
ALTER TABLE users ADD COLUMN IF NOT EXISTS claim_finished_at timestamptz;

-- +goose Down
ALTER TABLE users DROP COLUMN IF EXISTS claim_finished_at;
