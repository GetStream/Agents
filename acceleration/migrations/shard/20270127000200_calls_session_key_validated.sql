-- +goose Up

-- The rows the backfill wrote are checked against their sessions. Validating takes a lock that
-- stops no write of either table. On a database that checked the key when it was added, this
-- changes nothing. The lock waits at most 5 s behind a running query, past which the migration
-- fails and the router's next start tries again.
SET LOCAL lock_timeout = '5s';

ALTER TABLE calls VALIDATE CONSTRAINT calls_session_id_fkey;

-- +goose Down
