-- +goose Up
SET LOCAL lock_timeout = '5s';
-- The name every queue gives the end of a lease (store.queue).
ALTER TABLE campaign_contacts RENAME COLUMN leased_until TO lease_until;

-- +goose Down
ALTER TABLE campaign_contacts RENAME COLUMN lease_until TO leased_until;
