-- +goose Up

-- A call leg's token and a call bridge's are kept as the hex SHA-256 of the token, and a
-- bridge's trunk password sealed (phone.Service.park), so the tables hold neither the bearer
-- value a vendor presents nor the password. On a live table: the legs whose token is still set
-- (a status url not yet retired, phone.SweepStatusTokens) are hashed in place, so the status
-- posts of calls in flight still find them. The bridges parked now are deleted, as a password
-- cannot be sealed here: a vendor fetching one is refused, and its call is not bridged. A
-- router of the release before fails to place a call until it is replaced. The lock waits at
-- most 5 s behind a running query, past which the migration fails and the router's next start
-- tries again.
SET LOCAL lock_timeout = '5s';

UPDATE call_legs SET token = encode(sha256(convert_to(token, 'UTF8')), 'hex') WHERE token IS NOT NULL;
ALTER TABLE call_legs RENAME COLUMN token TO token_hash;

TRUNCATE call_bridges;
ALTER TABLE call_bridges RENAME COLUMN token TO token_hash;
ALTER TABLE call_bridges
    DROP COLUMN trunk_password,
    ADD COLUMN trunk_password_sealed BYTEA,
    ADD COLUMN trunk_password_kek_version INTEGER;

-- +goose Down

-- A hash gives no token back: the status urls of placed calls are refused from here on, and
-- the bridges parked are deleted.
SET LOCAL lock_timeout = '5s';

UPDATE call_legs SET token_hash = NULL WHERE token_hash IS NOT NULL;
ALTER TABLE call_legs RENAME COLUMN token_hash TO token;

TRUNCATE call_bridges;
ALTER TABLE call_bridges RENAME COLUMN token_hash TO token;
ALTER TABLE call_bridges
    DROP COLUMN trunk_password_sealed,
    DROP COLUMN trunk_password_kek_version,
    ADD COLUMN trunk_password TEXT;
