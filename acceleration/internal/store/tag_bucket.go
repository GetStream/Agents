package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// TagBucket is what the requests carrying one label cost in one time bucket, as Select reads
// it from the request rows and the rollup writes it to stats_tags_hourly or stats_tags_daily.
type TagBucket struct {
	Modality               string    `bun:"modality"`
	CustomerID             string    `bun:"customer_id"`
	TagKey                 string    `bun:"tag_key"`
	TagValue               string    `bun:"tag_value"`
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

// TagBucketFilter picks a customer's buckets of one modality and one tag key: every bucket
// [From, To) touches, whole, as a rollup writes it.
type TagBucketFilter struct {
	Modality    string
	Key         string
	Granularity Granularity
	From, To    time.Time
}

// TagBuckets are each shard's requests broken down by cost label: read from the request rows,
// and rolled up by Rollup into stats_tags_hourly and stats_tags_daily.
type TagBuckets struct {
	store *Store
}

// rollup aggregates the same window as Buckets.Rollup by cost tag. Each request is unrolled
// into one row per label it carries, so a request tagged with a project and an environment
// counts towards both breakdowns.
func (r *TagBuckets) rollup(ctx context.Context, granularity Granularity, from, to time.Time) (int64, error) {
	query := fmt.Sprintf(`
INSERT INTO %[1]s (
    modality, customer_id, tag_key, tag_value, bucket,
    audio_ms_total, characters_total,
    input_tokens_total, cached_input_tokens_total, output_tokens_total,
    images_total,
    cost_micros_total,
    request_count, error_count,
    latency_p50_ms, latency_p95_ms
)
SELECT
    r.modality,
    r.customer_id,
    tag.key,
    tag.value,
    date_trunc('%[2]s', r.started_at, 'UTC') AS bucket,
    COALESCE(SUM(r.audio_ms), 0),
    COALESCE(SUM(r.characters), 0),
    COALESCE(SUM(r.input_tokens), 0),
    COALESCE(SUM(r.cached_input_tokens), 0),
    COALESCE(SUM(r.output_tokens), 0),
    COALESCE(SUM(r.images), 0),
    COALESCE(SUM(r.cost_micros), 0),
    COUNT(*),
    COUNT(*) FILTER (WHERE NOT r.success AND %[3]s),
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.latency_ms) FILTER (WHERE %[3]s),
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY r.latency_ms) FILTER (WHERE %[3]s)
FROM requests AS r
CROSS JOIN LATERAL jsonb_each_text(r.tags) AS tag(key, value)
WHERE %[4]s
GROUP BY r.modality, r.customer_id, tag.key, tag.value, bucket
ON CONFLICT (modality, customer_id, tag_key, tag_value, bucket) DO UPDATE SET
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
		granularity.tagTable(), granularity.truncateUnit(), measured("r."), wholeBuckets(granularity, "r.started_at"))

	result, err := r.store.shard.ExecContext(ctx, query, from, to)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: rollup %s tags: %w", granularity, err))
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: rollup %s tags: %w", granularity, err))
	}
	return affected, nil
}

// Select returns what each value of one tag key cost, oldest bucket first. It is the "what
// drives our spend" query: group by project, or by environment, or by whichever label the
// customer bills on.
//
// Reads recorded requests directly, as Buckets.Select does, so a request is counted as soon
// as it is recorded rather than once somebody runs a rollup.
func (r *TagBuckets) Select(ctx context.Context, customerID string, f TagBucketFilter) ([]TagBucket, error) {
	if !f.Granularity.Valid() {
		return nil, stack.Wrap(fmt.Errorf("store: unknown granularity %q", f.Granularity))
	}
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}
	if f.Modality == "" {
		return nil, stack.Wrap(errors.New("store: modality is required"))
	}
	if f.Key == "" {
		return nil, stack.Wrap(errors.New("store: tag key is required"))
	}

	query := fmt.Sprintf(`
SELECT
    modality,
    customer_id,
    ?0 AS tag_key,
    tags->>?0 AS tag_value,
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
WHERE modality = ?1 AND customer_id = ?2
  AND started_at >= ?3 AND started_at < ?4
  AND tags->>?0 IS NOT NULL
GROUP BY modality, customer_id, tag_value, bucket
ORDER BY bucket ASC, tag_value ASC`, f.Granularity.truncateUnit(), measured(""))

	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]TagBucket, error) {
		var buckets []TagBucket
		err := db.NewRaw(query, f.Key, f.Modality, customerID, f.From, f.To).Scan(ctx, &buckets)
		return buckets, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer tag stats: %w", err))
	}
	return mergeTagBuckets(lists), nil
}
