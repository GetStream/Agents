package store

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// TurnSample is what a finished turn adds to its hour's stats. The legs are pointers
// because a pipeline need not have all of them, and a leg that never happened is left out
// of the percentiles rather than counted as instant.
type TurnSample struct {
	CustomerID  string
	AgentID     string
	StartedAt   time.Time
	Interrupted bool
	AudioOutMs  float64
	// STTLatencyMs is the provider's decode time for the transcript that settled the turn.
	STTLatencyMs *float64
	// LLMTTFTMs is the wait between asking the model and its first token.
	LLMTTFTMs *float64
	// TTSTTFBMs is the wait between sending the first sentence and the first audio.
	TTSTTFBMs *float64
	// RoundtripMs is the whole delay: settled transcript to first audio published.
	RoundtripMs *float64
}

// Validate refuses a turn no agent's stats could hold: every turn is one customer's agent's.
func (t *TurnSample) Validate() error {
	return validate("turn").
		required("customer_id", t.CustomerID).
		required("agent_id", t.AgentID).
		done()
}

// TurnBucket is one agent's turns in an hour or a day. The percentiles are read off a
// histogram, so they are within 5% of the true ones.
type TurnBucket struct {
	CustomerID       string    `bun:"customer_id"`
	AgentID          string    `bun:"agent_id"`
	Bucket           time.Time `bun:"bucket"`
	TurnCount        int64     `bun:"turn_count"`
	InterruptedCount int64     `bun:"interrupted_count"`
	AudioOutMsTotal  float64   `bun:"audio_out_ms_total"`
	STTLatencyP50Ms  *float64  `bun:"-"`
	STTLatencyP95Ms  *float64  `bun:"-"`
	LLMTTFTP50Ms     *float64  `bun:"-"`
	LLMTTFTP95Ms     *float64  `bun:"-"`
	TTSTTFBP50Ms     *float64  `bun:"-"`
	TTSTTFBP95Ms     *float64  `bun:"-"`
	RoundtripP50Ms   *float64  `bun:"-"`
	RoundtripP95Ms   *float64  `bun:"-"`
	RoundtripP99Ms   *float64  `bun:"-"`
}

// TurnBucketFilter picks a customer's turn buckets: every hour [From, To) touches, whole. An
// agent id narrows them to one agent.
type TurnBucketFilter struct {
	AgentID     string
	Granularity Granularity
	From, To    time.Time
}

// TurnBuckets are each shard's hourly turn counts and latency histograms, per agent.
type TurnBuckets struct {
	store *Store
}

// latencyGrowth is how much wider each histogram bin is than the one before it, and so how
// far a percentile read off the bins can be from the true one.
const latencyGrowth = 1.05

// The legs a turn's latency is counted for, as turn_latency_hourly names them.
const (
	legSTTLatency = "stt_latency"
	legLLMTTFT    = "llm_ttft"
	legTTSTTFB    = "tts_ttfb"
	legRoundtrip  = "roundtrip"
)

// Add counts one finished turn into its hour, in one statement: the turn itself, and each
// leg it had into that leg's histogram. A turn counted twice is counted twice, so the caller
// writes each one once.
func (r *TurnBuckets) Add(ctx context.Context, turn TurnSample) error {
	if err := turn.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if turn.StartedAt.IsZero() {
		turn.StartedAt = time.Now()
	}
	hour := turn.StartedAt.UTC().Truncate(time.Hour)
	interrupted := 0
	if turn.Interrupted {
		interrupted = 1
	}

	counted := `
INSERT INTO turn_stats_hourly (customer_id, agent_id, bucket, turn_count, interrupted_count, audio_out_ms_total)
VALUES (?, ?, ?, 1, ?, ?)
ON CONFLICT (customer_id, agent_id, bucket) DO UPDATE SET
    turn_count = turn_stats_hourly.turn_count + 1,
    interrupted_count = turn_stats_hourly.interrupted_count + EXCLUDED.interrupted_count,
    audio_out_ms_total = turn_stats_hourly.audio_out_ms_total + EXCLUDED.audio_out_ms_total`
	args := []any{turn.CustomerID, turn.AgentID, hour, interrupted, turn.AudioOutMs}

	var rows []string
	for _, leg := range []struct {
		name string
		ms   *float64
	}{
		{legSTTLatency, turn.STTLatencyMs},
		{legLLMTTFT, turn.LLMTTFTMs},
		{legTTSTTFB, turn.TTSTTFBMs},
		{legRoundtrip, turn.RoundtripMs},
	} {
		if leg.ms == nil {
			continue
		}
		rows = append(rows, "(?, ?, ?, ?, ?, 1)")
		args = append(args, turn.CustomerID, turn.AgentID, hour, leg.name, latencyBin(*leg.ms))
	}

	query := counted
	if len(rows) > 0 {
		query = "WITH counted AS (" + counted + `)
INSERT INTO turn_latency_hourly (customer_id, agent_id, bucket, leg, bin, count)
VALUES ` + strings.Join(rows, ", ") + `
ON CONFLICT (customer_id, agent_id, bucket, leg, bin) DO UPDATE SET
    count = turn_latency_hourly.count + 1`
	}
	if _, err := r.store.shard.ExecContext(ctx, query, args...); err != nil {
		return stack.Wrap(fmt.Errorf("store: record turn stats: %w", err))
	}
	return nil
}

// Select returns the conversational latency buckets for one customer, oldest first. The rows
// are hours, and every hour the window touches is read whole, rather than dropping the hour it
// starts in. A day is those hours of one UTC day, as the request statistics' day is the
// window's part of it, whatever the server's time zone.
func (r *TurnBuckets) Select(ctx context.Context, customerID string, f TurnBucketFilter) ([]TurnBucket, error) {
	granularity, agentID, from, to := f.Granularity, f.AgentID, f.From, f.To
	if !granularity.Valid() {
		return nil, stack.Wrap(fmt.Errorf("store: unknown granularity %q", granularity))
	}
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}
	bucket := fmt.Sprintf("date_trunc('%s', bucket, 'UTC')", granularity.truncateUnit())

	type binRow struct {
		AgentID string    `bun:"agent_id"`
		Leg     string    `bun:"leg"`
		Bin     int       `bun:"bin"`
		Bucket  time.Time `bun:"bucket"`
		Count   int64     `bun:"count"`
	}
	type shardRows struct {
		buckets []TurnBucket
		bins    []binRow
	}
	answers, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (shardRows, error) {
		counts := db.NewSelect().
			Table("turn_stats_hourly").
			ColumnExpr("customer_id, agent_id").
			ColumnExpr(bucket+" AS bucket").
			ColumnExpr("SUM(turn_count) AS turn_count").
			ColumnExpr("SUM(interrupted_count) AS interrupted_count").
			ColumnExpr("SUM(audio_out_ms_total) AS audio_out_ms_total").
			Where("customer_id = ?", customerID).
			Where(wholeBuckets(Hourly, "bucket"), from, to).
			GroupExpr("customer_id, agent_id, " + bucket).
			OrderExpr(bucket + " ASC, agent_id ASC")
		bins := db.NewSelect().
			Table("turn_latency_hourly").
			ColumnExpr("agent_id, leg, bin").
			ColumnExpr(bucket+" AS bucket").
			ColumnExpr("SUM(count) AS count").
			Where("customer_id = ?", customerID).
			Where(wholeBuckets(Hourly, "bucket"), from, to).
			GroupExpr("agent_id, leg, bin, " + bucket).
			OrderExpr("bin ASC")
		if agentID != "" {
			counts = counts.Where("agent_id = ?", agentID)
			bins = bins.Where("agent_id = ?", agentID)
		}
		var read shardRows
		if err := counts.Scan(ctx, &read.buckets); err != nil {
			return read, fmt.Errorf("store: customer turn stats: %w", err)
		}
		if err := bins.Scan(ctx, &read.bins); err != nil {
			return read, fmt.Errorf("store: customer turn latency: %w", err)
		}
		return read, nil
	})
	if err != nil {
		return nil, stack.Wrap(err)
	}

	// Each shard's buckets are summed into one per agent and bucket, and its bins kept in
	// ascending order, where a bin two shards both counted is two entries side by side.
	buckets := answers[0].buckets
	counted := answers[0].bins
	if len(answers) > 1 {
		type bucketKey struct {
			agentID string
			bucket  int64
		}
		sums := map[bucketKey]*TurnBucket{}
		buckets, counted = nil, nil
		for _, answer := range answers {
			for _, b := range answer.buckets {
				key := bucketKey{b.AgentID, b.Bucket.Unix()}
				if sum := sums[key]; sum != nil {
					sum.TurnCount += b.TurnCount
					sum.InterruptedCount += b.InterruptedCount
					sum.AudioOutMsTotal += b.AudioOutMsTotal
					continue
				}
				sums[key] = &b
			}
			counted = append(counted, answer.bins...)
		}
		for _, sum := range sums {
			buckets = append(buckets, *sum)
		}
		slices.SortFunc(buckets, func(a, b TurnBucket) int {
			return cmp.Or(a.Bucket.Compare(b.Bucket), strings.Compare(a.AgentID, b.AgentID))
		})
		slices.SortStableFunc(counted, func(a, b binRow) int { return cmp.Compare(a.Bin, b.Bin) })
	}

	type histogramKey struct {
		agentID, leg string
		bucket       int64
	}
	histograms := map[histogramKey]*histogram{}
	for _, row := range counted {
		key := histogramKey{row.AgentID, row.Leg, row.Bucket.Unix()}
		if histograms[key] == nil {
			histograms[key] = &histogram{}
		}
		histograms[key].add(row.Bin, row.Count)
	}
	for i := range buckets {
		of := func(leg string) *histogram {
			return histograms[histogramKey{buckets[i].AgentID, leg, buckets[i].Bucket.Unix()}]
		}
		stt, llm, tts, roundtrip := of(legSTTLatency), of(legLLMTTFT), of(legTTSTTFB), of(legRoundtrip)
		buckets[i].STTLatencyP50Ms, buckets[i].STTLatencyP95Ms = stt.percentile(0.5), stt.percentile(0.95)
		buckets[i].LLMTTFTP50Ms, buckets[i].LLMTTFTP95Ms = llm.percentile(0.5), llm.percentile(0.95)
		buckets[i].TTSTTFBP50Ms, buckets[i].TTSTTFBP95Ms = tts.percentile(0.5), tts.percentile(0.95)
		buckets[i].RoundtripP50Ms = roundtrip.percentile(0.5)
		buckets[i].RoundtripP95Ms = roundtrip.percentile(0.95)
		buckets[i].RoundtripP99Ms = roundtrip.percentile(0.99)
	}
	return buckets, nil
}

// turnLegs are the legs a turn's latency is counted for, in the order a turn runs them.
var turnLegs = []string{legSTTLatency, legLLMTTFT, legTTSTTFB, legRoundtrip}

// LegLatency is how long one leg of some apps' turns took: how many turns it was counted for,
// and its percentiles in milliseconds, within 5% of the true ones and nil when no turn had it.
type LegLatency struct {
	Leg   string
	Turns int64
	P50Ms *float64
	P95Ms *float64
	P99Ms *float64
}

// Latency is how long the turns apps started from the hour of since on took, every agent of
// theirs and every shard summed, one per leg in the order of turnLegs.
func (r *TurnBuckets) Latency(ctx context.Context, apps []string, since time.Time) ([]LegLatency, error) {
	type binRow struct {
		Leg   string `bun:"leg"`
		Bin   int    `bun:"bin"`
		Count int64  `bun:"count"`
	}
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]binRow, error) {
		var bins []binRow
		err := db.NewSelect().
			Table("turn_latency_hourly").
			ColumnExpr("leg, bin, SUM(count) AS count").
			Where("customer_id IN (?)", bun.In(apps)).
			Where("bucket >= ?", since.UTC().Truncate(time.Hour)).
			GroupExpr("leg, bin").
			Scan(ctx, &bins)
		return bins, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: turn latency: %w", err))
	}
	// A histogram takes its bins in ascending order, where a bin two shards both counted is
	// two entries side by side.
	var counted []binRow
	for _, list := range lists {
		counted = append(counted, list...)
	}
	slices.SortStableFunc(counted, func(a, b binRow) int { return cmp.Compare(a.Bin, b.Bin) })
	histograms := map[string]*histogram{}
	for _, row := range counted {
		if histograms[row.Leg] == nil {
			histograms[row.Leg] = &histogram{}
		}
		histograms[row.Leg].add(row.Bin, row.Count)
	}
	legs := make([]LegLatency, 0, len(turnLegs))
	for _, leg := range turnLegs {
		h := histograms[leg]
		latency := LegLatency{Leg: leg, P50Ms: h.percentile(0.5), P95Ms: h.percentile(0.95), P99Ms: h.percentile(0.99)}
		if h != nil {
			latency.Turns = h.total
		}
		legs = append(legs, latency)
	}
	return legs, nil
}

// latencyBin is the histogram bin a latency falls in: bin b holds (growth^(b-1), growth^b]
// ms, and anything up to a millisecond is bin 0.
func latencyBin(ms float64) int {
	if ms <= 1 {
		return 0
	}
	return int(math.Ceil(math.Log(ms) / math.Log(latencyGrowth)))
}

// histogram is one leg's counted bins, in ascending order.
type histogram struct {
	bins   []int
	counts []int64
	total  int64
}

func (h *histogram) add(bin int, count int64) {
	h.bins = append(h.bins, bin)
	h.counts = append(h.counts, count)
	h.total += count
}

// percentile is the nearest-rank p-th percentile, as the middle of the bin it falls in, or
// nil for a leg no turn had.
func (h *histogram) percentile(p float64) *float64 {
	if h == nil || h.total == 0 {
		return nil
	}
	rank := int64(math.Ceil(p * float64(h.total)))
	var seen int64
	for i, count := range h.counts {
		seen += count
		if seen >= rank {
			ms := math.Pow(latencyGrowth, float64(h.bins[i])-0.5)
			return &ms
		}
	}
	return nil
}
