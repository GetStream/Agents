-- +goose Up
SET LOCAL lock_timeout = '5s';

-- What the spend, activity and concurrency pages read of every shard, shipped here by one router
-- of each shard (Store.ShipStats) so that no router reads another shard. Each table holds whole
-- hours (or minutes), and a ship replaces the shard's rows of the hours it ships, so a row that
-- changed since, or went away, is shipped as it is now.

-- The requests, summed per hour over everything a spend read filters or groups by: dims is a
-- digest of those columns, the key of one group.
CREATE TABLE IF NOT EXISTS shard_spend_hourly (
    shard TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    dims TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    api_key TEXT NOT NULL,
    user_id TEXT NOT NULL,
    usage_categories TEXT[] NOT NULL,
    tags JSONB NOT NULL,
    modality TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    billable BOOLEAN NOT NULL,
    success BOOLEAN NOT NULL,
    measured BOOLEAN NOT NULL,
    rows BIGINT NOT NULL,
    cost_micros BIGINT NOT NULL,
    input_tokens BIGINT NOT NULL,
    cached_input_tokens BIGINT NOT NULL,
    output_tokens BIGINT NOT NULL,
    audio_ms BIGINT NOT NULL,
    characters BIGINT NOT NULL,
    images BIGINT NOT NULL,
    duration_ms DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (shard, started_at, dims)
);
CREATE INDEX IF NOT EXISTS shard_spend_hourly_customer_idx ON shard_spend_hourly (customer_id, started_at);
CREATE INDEX IF NOT EXISTS shard_spend_hourly_started_idx ON shard_spend_hourly (started_at);
CREATE INDEX IF NOT EXISTS shard_spend_hourly_modality_idx ON shard_spend_hourly (modality, started_at);

-- Sessions opened, messages answered and calls started in each hour.
CREATE TABLE IF NOT EXISTS shard_activity_hourly (
    shard TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    sessions BIGINT NOT NULL,
    messages BIGINT NOT NULL,
    calls BIGINT NOT NULL,
    voice_minutes DOUBLE PRECISION NOT NULL,
    phone_minutes DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (customer_id, bucket, shard)
);
CREATE INDEX IF NOT EXISTS shard_activity_hourly_shard_idx ON shard_activity_hourly (shard, bucket);

-- Who was seen in each hour: a person is counted once however many shards and hours they were
-- seen in, so they are kept by name rather than counted.
CREATE TABLE IF NOT EXISTS shard_people_hourly (
    shard TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    user_id TEXT NOT NULL,
    PRIMARY KEY (customer_id, bucket, user_id, shard)
);
CREATE INDEX IF NOT EXISTS shard_people_hourly_shard_idx ON shard_people_hourly (shard, bucket);

-- The turns each session took in each hour.
CREATE TABLE IF NOT EXISTS shard_session_turns_hourly (
    shard TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    config_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    turns BIGINT NOT NULL,
    PRIMARY KEY (customer_id, bucket, config_id, session_id, shard)
);
CREATE INDEX IF NOT EXISTS shard_session_turns_hourly_shard_idx ON shard_session_turns_hourly (shard, bucket);

-- How each tool was used in each hour. A percentile is not a sum, so p99_ms is the hour's, and
-- a window weighs each hour's by its calls.
CREATE TABLE IF NOT EXISTS shard_tool_hourly (
    shard TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    config_id TEXT NOT NULL,
    tool TEXT NOT NULL,
    calls BIGINT NOT NULL,
    failed BIGINT NOT NULL,
    tokens_total DOUBLE PRECISION NOT NULL,
    ms_total DOUBLE PRECISION NOT NULL,
    ms_count BIGINT NOT NULL,
    p99_ms DOUBLE PRECISION,
    offered_at TIMESTAMPTZ,
    loading TEXT NOT NULL,
    definition_tokens BIGINT,
    summary_tokens BIGINT,
    PRIMARY KEY (customer_id, bucket, config_id, tool, shard)
);
CREATE INDEX IF NOT EXISTS shard_tool_hourly_shard_idx ON shard_tool_hourly (shard, bucket);

-- The sessions that used each tool in each hour, with the first use's turn and the turns the
-- session took before it, none when that turn was rewound.
CREATE TABLE IF NOT EXISTS shard_tool_sessions_hourly (
    shard TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    config_id TEXT NOT NULL,
    tool TEXT NOT NULL,
    session_id TEXT NOT NULL,
    first_at TIMESTAMPTZ,
    first_turn TEXT,
    first_before BIGINT,
    PRIMARY KEY (customer_id, bucket, config_id, tool, session_id, shard)
);
CREATE INDEX IF NOT EXISTS shard_tool_sessions_hourly_shard_idx ON shard_tool_sessions_hourly (shard, bucket);

-- The sessions open at any moment of each minute.
CREATE TABLE IF NOT EXISTS shard_concurrency_minute (
    shard TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    minute TIMESTAMPTZ NOT NULL,
    sessions BIGINT NOT NULL,
    PRIMARY KEY (customer_id, minute, shard)
);
CREATE INDEX IF NOT EXISTS shard_concurrency_minute_shard_idx ON shard_concurrency_minute (shard, minute);

-- +goose Down
SET LOCAL lock_timeout = '5s';
DROP TABLE IF EXISTS shard_concurrency_minute;
DROP TABLE IF EXISTS shard_tool_sessions_hourly;
DROP TABLE IF EXISTS shard_tool_hourly;
DROP TABLE IF EXISTS shard_session_turns_hourly;
DROP TABLE IF EXISTS shard_people_hourly;
DROP TABLE IF EXISTS shard_activity_hourly;
DROP TABLE IF EXISTS shard_spend_hourly;
