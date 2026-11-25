-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Each shard's rollups, shipped here by one router of the shard (Store.ShipStats), so a stats
-- page reads the shared database alone rather than every shard. A row is one shard's bucket
-- and holds what that shard counted, replaced on every ship rather than added to, so shipping
-- the same bucket again changes nothing. A page sums the shards' rows.
CREATE TABLE IF NOT EXISTS shard_stats_hourly (
    shard TEXT NOT NULL,
    modality TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    audio_ms_total BIGINT NOT NULL,
    characters_total BIGINT NOT NULL,
    input_tokens_total BIGINT NOT NULL,
    cached_input_tokens_total BIGINT NOT NULL,
    output_tokens_total BIGINT NOT NULL,
    images_total BIGINT NOT NULL,
    cost_micros_total BIGINT NOT NULL,
    request_count BIGINT NOT NULL,
    error_count BIGINT NOT NULL,
    latency_p50_ms DOUBLE PRECISION,
    latency_p95_ms DOUBLE PRECISION,
    uptime DOUBLE PRECISION GENERATED ALWAYS AS ((request_count - error_count)::double precision / NULLIF(request_count, 0)::double precision) STORED,
    PRIMARY KEY (modality, customer_id, bucket, provider, model, shard)
);

CREATE TABLE IF NOT EXISTS shard_stats_daily (LIKE shard_stats_hourly INCLUDING ALL);

CREATE TABLE IF NOT EXISTS shard_stats_tags_hourly (
    shard TEXT NOT NULL,
    modality TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    audio_ms_total BIGINT NOT NULL,
    characters_total BIGINT NOT NULL,
    input_tokens_total BIGINT NOT NULL,
    cached_input_tokens_total BIGINT NOT NULL,
    output_tokens_total BIGINT NOT NULL,
    images_total BIGINT NOT NULL,
    cost_micros_total BIGINT NOT NULL,
    request_count BIGINT NOT NULL,
    error_count BIGINT NOT NULL,
    latency_p50_ms DOUBLE PRECISION,
    latency_p95_ms DOUBLE PRECISION,
    uptime DOUBLE PRECISION GENERATED ALWAYS AS ((request_count - error_count)::double precision / NULLIF(request_count, 0)::double precision) STORED,
    PRIMARY KEY (modality, customer_id, tag_key, bucket, tag_value, shard)
);

CREATE TABLE IF NOT EXISTS shard_stats_tags_daily (LIKE shard_stats_tags_hourly INCLUDING ALL);

CREATE TABLE IF NOT EXISTS shard_turn_stats_hourly (
    shard TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    turn_count BIGINT NOT NULL,
    interrupted_count BIGINT NOT NULL,
    audio_out_ms_total DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (customer_id, bucket, agent_id, shard)
);

CREATE TABLE IF NOT EXISTS shard_turn_latency_hourly (
    shard TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    bucket TIMESTAMPTZ NOT NULL,
    leg TEXT NOT NULL,
    bin SMALLINT NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (customer_id, bucket, agent_id, leg, bin, shard)
);

-- How far each shard has shipped: every hour before shipped_through was shipped after it
-- closed. A shard with no row has shipped nothing, and its first ship backfills all it has.
CREATE TABLE IF NOT EXISTS shard_stats_shipped (
    shard TEXT PRIMARY KEY,
    shipped_through TIMESTAMPTZ NOT NULL,
    shipped_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE shard_stats_shipped;
DROP TABLE shard_turn_latency_hourly;
DROP TABLE shard_turn_stats_hourly;
DROP TABLE shard_stats_tags_daily;
DROP TABLE shard_stats_tags_hourly;
DROP TABLE shard_stats_daily;
DROP TABLE shard_stats_hourly;
