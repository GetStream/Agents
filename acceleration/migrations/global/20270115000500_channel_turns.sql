-- +goose Up
SET LOCAL lock_timeout = '5s';

-- A channel's running turn is one lease, whether the channel is an app chat's or a thread
-- channel the channel bridge writes into: chat_channel_turns becomes channel_turns, and the
-- thread channels' leases on channel_threads move into it under their cid. A cid is unique
-- only within its Stream app, so the app stays in the key; 0 is the deployment's own.
ALTER TABLE chat_channel_turns RENAME TO channel_turns;
ALTER TABLE channel_turns RENAME CONSTRAINT chat_channel_turns_pkey TO channel_turns_pkey;
ALTER TABLE channel_turns RENAME COLUMN turn_holder TO leased_by;
ALTER TABLE channel_turns RENAME COLUMN turn_until TO lease_until;

INSERT INTO channel_turns (stream_app_pk, cid, leased_by, lease_until)
SELECT COALESCE(stream_app_pk, 0), 'agent:' || channel_id, turn_holder, turn_until
FROM channel_threads
WHERE turn_holder IS NOT NULL AND turn_until IS NOT NULL
ON CONFLICT DO NOTHING;

ALTER TABLE channel_threads DROP COLUMN turn_holder, DROP COLUMN turn_until;

-- +goose Down
ALTER TABLE channel_threads ADD COLUMN turn_holder TEXT, ADD COLUMN turn_until TIMESTAMPTZ;
UPDATE channel_threads AS ct
SET turn_holder = t.leased_by, turn_until = t.lease_until
FROM channel_turns AS t
WHERE t.stream_app_pk = COALESCE(ct.stream_app_pk, 0) AND t.cid = 'agent:' || ct.channel_id;
DELETE FROM channel_turns AS t USING channel_threads AS ct
WHERE t.stream_app_pk = COALESCE(ct.stream_app_pk, 0) AND t.cid = 'agent:' || ct.channel_id;
ALTER TABLE channel_turns RENAME COLUMN lease_until TO turn_until;
ALTER TABLE channel_turns RENAME COLUMN leased_by TO turn_holder;
ALTER TABLE channel_turns RENAME CONSTRAINT channel_turns_pkey TO chat_channel_turns_pkey;
ALTER TABLE channel_turns RENAME TO chat_channel_turns;
