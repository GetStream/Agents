-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The shard that placed or answered each Stream call, whose legs, lines and participants are
-- there alone: a call event that lands in another shard is handed to a router of this one.
-- Swept once no call could still be up (CallLegs.Sweep).
CREATE TABLE IF NOT EXISTS call_homes (
    call_id TEXT NOT NULL,
    call_type TEXT NOT NULL,
    -- Zero is the deployment's own app.
    stream_app_pk BIGINT NOT NULL,
    customer_id TEXT NOT NULL,
    shard TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (call_id, call_type, stream_app_pk, customer_id)
);

CREATE INDEX IF NOT EXISTS call_homes_shard_opened_idx ON call_homes (shard, opened_at);

-- Usage whose id is the same whichever shard bills it (a number's month, a 10DLC fee), claimed
-- by the first shard to bill it, so no other bills it again (Store.UsageToBill).
CREATE TABLE IF NOT EXISTS usage_claims (
    usage_id UUID PRIMARY KEY,
    shard TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS usage_claims;
DROP TABLE IF EXISTS call_homes;
