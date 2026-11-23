-- +goose Up
SET LOCAL lock_timeout = '5s';
-- A trunk made for one number, before trunks were shared or without a key to seal a shared
-- one's password, is a row too: its password stays sealed to the number it was made for, and a
-- line adopts it into its group.
ALTER TABLE stream_trunks ADD COLUMN own boolean NOT NULL DEFAULT false, DROP COLUMN numbers;

INSERT INTO stream_trunks (id, customer_id, stream_app_pk, vendor, bridge_uri, bridge_username,
    bridge_password_sealed, bridge_password_kek_version, created_at, own)
SELECT DISTINCT ON (pn.stream_trunk_id) pn.stream_trunk_id, pn.customer_id, pn.stream_app_pk, pn.vendor, pn.bridge_uri,
    COALESCE(pn.bridge_username, ''), COALESCE(pn.bridge_password_sealed, ''::bytea),
    COALESCE(pn.bridge_password_kek_version, 0), COALESCE(pn.attached_at, pn.purchased_at), true
FROM phone_numbers pn
WHERE pn.released_at IS NULL AND pn.stream_trunk_id IS NOT NULL AND pn.bridge_uri IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM stream_trunks st WHERE st.id = pn.stream_trunk_id)
ORDER BY pn.stream_trunk_id, pn.id DESC;

-- A trunk's numbers are the numbers held on it.
CREATE INDEX phone_numbers_trunk_idx ON phone_numbers (customer_id, stream_trunk_id) WHERE released_at IS NULL;

ALTER TABLE phone_numbers
    DROP COLUMN bridge_uri,
    DROP COLUMN bridge_username,
    DROP COLUMN bridge_password_sealed,
    DROP COLUMN bridge_password_kek_version;

-- +goose Down
ALTER TABLE phone_numbers
    ADD COLUMN bridge_uri text,
    ADD COLUMN bridge_username text,
    ADD COLUMN bridge_password_sealed bytea,
    ADD COLUMN bridge_password_kek_version integer;
ALTER TABLE stream_trunks ADD COLUMN numbers text[] NOT NULL DEFAULT '{}';
UPDATE stream_trunks st SET numbers = ARRAY(
    SELECT pn.e164 FROM phone_numbers pn
    WHERE pn.customer_id = st.customer_id AND pn.stream_trunk_id = st.id AND pn.released_at IS NULL ORDER BY pn.id);
ALTER TABLE stream_trunks ALTER COLUMN numbers DROP DEFAULT;
-- An own trunk's password was sealed to its number, so it goes back there; a shared one's was
-- sealed to the trunk and cannot, so its numbers keep only the address.
UPDATE phone_numbers pn SET bridge_uri = st.bridge_uri, bridge_username = st.bridge_username,
    bridge_password_sealed = CASE WHEN st.own AND st.bridge_password_sealed <> ''::bytea THEN st.bridge_password_sealed END,
    bridge_password_kek_version = CASE WHEN st.own AND st.bridge_password_sealed <> ''::bytea THEN st.bridge_password_kek_version END
FROM stream_trunks st
WHERE st.id = pn.stream_trunk_id AND st.customer_id = pn.customer_id AND pn.released_at IS NULL;
DELETE FROM stream_trunks WHERE own;
DROP INDEX phone_numbers_trunk_idx;
ALTER TABLE stream_trunks DROP COLUMN own;
