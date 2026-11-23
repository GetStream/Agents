-- +goose Up

-- A call leg's token and a call bridge's are kept as the hex SHA-256 of the token, in
-- token_hash, and a bridge's trunk password sealed (phone.Service.park), so the tables hold
-- neither the bearer value a vendor presents nor the password. Only columns are added: token
-- and trunk_password stay, nullable, for the release before, which still reads and writes them
-- while it runs beside this one, and this release writes neither. A later migration drops them
-- once no router of the release before can run against the database.
--
-- The legs whose token is still set (a status url not yet retired, phone.SweepStatusTokens)
-- are hashed in place and their token cleared, so this release finds the status posts of calls
-- in flight and the release before misses them. A bridge parked before is left to the release
-- that parked it, and is deleted by the phone sweep once it expires. The lock waits at most 5 s
-- behind a running query, past which the migration fails and the router's next start tries
-- again.
SET LOCAL lock_timeout = '5s';

ALTER TABLE call_legs ADD COLUMN token_hash text;
UPDATE call_legs SET token_hash = encode(sha256(convert_to(token, 'UTF8')), 'hex'), token = NULL
WHERE token IS NOT NULL;

ALTER TABLE call_bridges
    DROP CONSTRAINT call_bridges_pkey,
    ALTER COLUMN token DROP NOT NULL,
    ADD CONSTRAINT call_bridges_token_key UNIQUE (token),
    ADD COLUMN token_hash text,
    ADD CONSTRAINT call_bridges_token_hash_key UNIQUE (token_hash),
    ADD COLUMN trunk_password_sealed BYTEA,
    ADD COLUMN trunk_password_kek_version INTEGER;

-- +goose Down

-- A hash gives no token back: the status urls of the calls this release placed are refused from
-- here on, and the bridges it parked are deleted.
SET LOCAL lock_timeout = '5s';

ALTER TABLE call_legs DROP COLUMN token_hash;

DELETE FROM call_bridges WHERE token IS NULL;
ALTER TABLE call_bridges
    DROP CONSTRAINT call_bridges_token_hash_key,
    DROP COLUMN token_hash,
    DROP COLUMN trunk_password_sealed,
    DROP COLUMN trunk_password_kek_version,
    DROP CONSTRAINT call_bridges_token_key,
    ALTER COLUMN token SET NOT NULL,
    ADD CONSTRAINT call_bridges_pkey PRIMARY KEY (token);
