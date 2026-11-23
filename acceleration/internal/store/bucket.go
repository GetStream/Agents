package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Bucket is one aggregated row from stats_hourly or stats_daily.
type Bucket struct {
	Modality               string    `bun:"modality"`
	CustomerID             string    `bun:"customer_id"`
	Provider               string    `bun:"provider"`
	Model                  string    `bun:"model"`
	Bucket                 time.Time `bun:"bucket"`
	AudioMsTotal           int64     `bun:"audio_ms_total"`
	CharactersTotal        int64     `bun:"characters_total"`
	InputTokensTotal       int64     `bun:"input_tokens_total"`
	CachedInputTokensTotal int64     `bun:"cached_input_tokens_total"`
	OutputTokensTotal      int64     `bun:"output_tokens_total"`
	ImagesTotal            int64     `bun:"images_total"`
	CostMicrosTotal        int64     `bun:"cost_micros_total"`
	RequestCount           int64     `bun:"request_count"`
	ErrorCount             int64     `bun:"error_count"`
	LatencyP50Ms           *float64  `bun:"latency_p50_ms"`
	LatencyP95Ms           *float64  `bun:"latency_p95_ms"`
	Uptime                 *float64  `bun:"uptime"`
}

// Granularity selects which rollup table to read or write.
type Granularity string

const (
	Hourly Granularity = "hourly"
	Daily  Granularity = "daily"
)

// table returns the rollup table for the granularity.
func (g Granularity) table() string {
	if g == Daily {
		return "stats_daily"
	}
	return "stats_hourly"
}

// tagTable returns the tag rollup table for the granularity.
func (g Granularity) tagTable() string {
	if g == Daily {
		return "stats_tags_daily"
	}
	return "stats_tags_hourly"
}

// truncateUnit returns the Postgres date_trunc unit for the granularity.
func (g Granularity) truncateUnit() string {
	if g == Daily {
		return "day"
	}
	return "hour"
}

// Valid reports whether the granularity is one this store knows.
func (g Granularity) Valid() bool {
	return g == Hourly || g == Daily
}

// BucketFilter picks a customer's buckets of one modality in [From, To), of the requests that
// carry every one of Tags.
type BucketFilter struct {
	Modality    string
	Granularity Granularity
	From, To    time.Time
	Tags        map[string]string
}

// Buckets are each shard's rollups of the requests it recorded, by provider and model.
type Buckets struct {
	store *Store
}

// Rollup aggregates the requests in [from, to) into the rollup tables for the given
// granularity, both the provider breakdown and the cost-tag breakdown, and returns how
// many buckets were written across the two. The window is widened to whole buckets, from
// the start of the one holding from to the end of the one holding the last instant before
// to, because a bucket is written whole: summed over part of it, it would replace the
// totals of the rest. It is idempotent: re-running it recomputes the buckets it touches,
// so a missed run is fixed by running it again over the same window.
func (r *Buckets) Rollup(ctx context.Context, granularity Granularity, from, to time.Time) (int64, error) {
	if !granularity.Valid() {
		return 0, stack.Wrap(fmt.Errorf("store: unknown granularity %q", granularity))
	}
	if !to.After(from) {
		return 0, stack.Wrap(fmt.Errorf("store: rollup window must be non-empty, got %s to %s", from, to))
	}

	providers, err := r.rollup(ctx, granularity, from, to)
	if err != nil {
		return 0, err
	}
	tags, err := r.store.TagBuckets.rollup(ctx, granularity, from, to)
	if err != nil {
		return 0, err
	}
	return providers + tags, nil
}

// wholeBuckets is a rollup's WHERE on column: the whole buckets [from, to) touches, taking
// from and to as its two parameters. Postgres keeps timestamps to the microsecond, so to
// less one is the window's last instant.
func wholeBuckets(granularity Granularity, column string) string {
	return fmt.Sprintf("%[1]s >= date_trunc('%[2]s', ?::timestamptz, 'UTC') AND %[1]s < "+
		"date_trunc('%[2]s', ?::timestamptz - interval '1 microsecond', 'UTC') + interval '1 %[2]s'",
		column, granularity.truncateUnit())
}

func (r *Buckets) rollup(ctx context.Context, granularity Granularity, from, to time.Time) (int64, error) {
	query := fmt.Sprintf(`
INSERT INTO %[1]s (
    modality, customer_id, provider, model, bucket,
    audio_ms_total, characters_total,
    input_tokens_total, cached_input_tokens_total, output_tokens_total,
    images_total,
    cost_micros_total,
    request_count, error_count,
    latency_p50_ms, latency_p95_ms
)
SELECT
    modality,
    customer_id,
    provider,
    model,
    date_trunc('%[2]s', started_at, 'UTC') AS bucket,
    COALESCE(SUM(audio_ms), 0),
    COALESCE(SUM(characters), 0),
    COALESCE(SUM(input_tokens), 0),
    COALESCE(SUM(cached_input_tokens), 0),
    COALESCE(SUM(output_tokens), 0),
    COALESCE(SUM(images), 0),
    COALESCE(SUM(cost_micros), 0),
    COUNT(*),
    COUNT(*) FILTER (WHERE NOT success AND %[3]s),
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE %[3]s),
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE %[3]s)
FROM requests
WHERE %[4]s
GROUP BY modality, customer_id, provider, model, bucket
ON CONFLICT (modality, customer_id, provider, model, bucket) DO UPDATE SET
    audio_ms_total = EXCLUDED.audio_ms_total,
    characters_total = EXCLUDED.characters_total,
    input_tokens_total = EXCLUDED.input_tokens_total,
    cached_input_tokens_total = EXCLUDED.cached_input_tokens_total,
    output_tokens_total = EXCLUDED.output_tokens_total,
    images_total = EXCLUDED.images_total,
    cost_micros_total = EXCLUDED.cost_micros_total,
    request_count = EXCLUDED.request_count,
    error_count = EXCLUDED.error_count,
    latency_p50_ms = EXCLUDED.latency_p50_ms,
    latency_p95_ms = EXCLUDED.latency_p95_ms`,
		granularity.table(), granularity.truncateUnit(), measured(""), wholeBuckets(granularity, "started_at"))

	// Each shard rolls up the requests it recorded, into its own rollups.
	result, err := r.store.shard.ExecContext(ctx, query, from, to)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: rollup %s: %w", granularity, err))
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: rollup %s: %w", granularity, err))
	}
	return affected, nil
}

// Select returns the buckets for one customer and modality in [from, to), oldest first.
//
// Reads recorded requests directly so recent activity is visible before rollups run.
func (r *Buckets) Select(ctx context.Context, customerID string, f BucketFilter) ([]Bucket, error) {
	if !f.Granularity.Valid() {
		return nil, stack.Wrap(fmt.Errorf("store: unknown granularity %q", f.Granularity))
	}
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}
	if f.Modality == "" {
		return nil, stack.Wrap(errors.New("store: modality is required"))
	}
	tags := f.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	filter, err := json.Marshal(tags)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer stats: encode tag filter: %w", err))
	}

	query := fmt.Sprintf(`
SELECT
    modality,
    customer_id,
    provider,
    model,
    date_trunc('%[1]s', started_at, 'UTC') AS bucket,
    COALESCE(SUM(audio_ms), 0) AS audio_ms_total,
    COALESCE(SUM(characters), 0) AS characters_total,
    COALESCE(SUM(input_tokens), 0) AS input_tokens_total,
    COALESCE(SUM(cached_input_tokens), 0) AS cached_input_tokens_total,
    COALESCE(SUM(output_tokens), 0) AS output_tokens_total,
    COALESCE(SUM(images), 0) AS images_total,
    COALESCE(SUM(cost_micros), 0) AS cost_micros_total,
    COUNT(*) AS request_count,
    COUNT(*) FILTER (WHERE NOT success AND %[2]s) AS error_count,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE %[2]s) AS latency_p50_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE %[2]s) AS latency_p95_ms,
    (COUNT(*) - COUNT(*) FILTER (WHERE NOT success AND %[2]s))::double precision
        / NULLIF(COUNT(*), 0) AS uptime
FROM requests
WHERE modality = ? AND customer_id = ?
  AND started_at >= ? AND started_at < ?
  AND tags @> ?::jsonb
GROUP BY modality, customer_id, provider, model, bucket
ORDER BY bucket ASC, provider ASC, model ASC`, f.Granularity.truncateUnit(), measured(""))

	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]Bucket, error) {
		var buckets []Bucket
		err := db.NewRaw(query, f.Modality, customerID, f.From, f.To, string(filter)).Scan(ctx, &buckets)
		return buckets, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer stats by tag: %w", err))
	}
	return mergeBuckets(lists), nil
}
