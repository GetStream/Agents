//go:build integration

package store

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func (s *StoreSuite) TestRollupAggregatesRequestsIntoOneBucket() {
	s.record("acme", s.base.Add(1*time.Minute), 1000, 100, true)
	s.record("acme", s.base.Add(2*time.Minute), 2000, 200, true)
	s.record("acme", s.base.Add(3*time.Minute), 500, 300, false)

	affected, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.EqualValues(1, affected, "three requests in one hour make one bucket")

	buckets, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)

	bucket := buckets[0]
	s.Equal(s.base, bucket.Bucket.UTC(), "the bucket is the truncated hour")
	s.EqualValues(3500, bucket.AudioMsTotal)
	s.EqualValues(3, bucket.RequestCount)
	s.EqualValues(1, bucket.ErrorCount)
	s.Require().NotNil(bucket.LatencyP50Ms)
	s.InDelta(200.0, *bucket.LatencyP50Ms, 0.001)
	s.Require().NotNil(bucket.Uptime)
	s.InDelta(2.0/3.0, *bucket.Uptime, 0.001, "uptime is successes over total")
}

func (s *StoreSuite) TestRollupSplitsHoursAndKeepsProvidersApart() {
	s.record("acme", s.base.Add(10*time.Minute), 1000, 100, true)
	s.record("acme", s.base.Add(70*time.Minute), 1000, 100, true)

	other := &Request{
		Modality:   "stt",
		CustomerID: "acme",
		Provider:   "parakeet",
		Model:      "parakeet-tdt-0.6b-v3",
		StartedAt:  s.base.Add(20 * time.Minute),
		AudioMs:    4000,
		Success:    true,
	}
	s.Require().NoError(insertRequest(s.ctx, s.store, other))

	_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(3*time.Hour))
	s.Require().NoError(err)

	buckets, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(3 * time.Hour)})
	s.Require().NoError(err)
	s.Len(buckets, 3, "two hours for deepgram plus one for parakeet")

	byProvider := map[string]int64{}
	for _, bucket := range buckets {
		byProvider[bucket.Provider] += bucket.AudioMsTotal
	}
	s.EqualValues(2000, byProvider["deepgram"])
	s.EqualValues(4000, byProvider["parakeet"])
}

func (s *StoreSuite) TestModalitiesAreAggregatedSeparately() {
	s.record("acme", s.base.Add(1*time.Minute), 3000, 100, true)

	spoken := &Request{
		Modality:   "tts",
		CustomerID: "acme",
		Provider:   "elevenlabs",
		Model:      "eleven_flash_v2_5",
		StartedAt:  s.base.Add(2 * time.Minute),
		AudioMs:    1500,
		Characters: 84,
		CostMicros: 4200,
		Success:    true,
	}
	s.Require().NoError(insertRequest(s.ctx, s.store, spoken))

	_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)

	transcription, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(transcription, 1, "the synthesis row belongs to the other modality")
	s.EqualValues(3000, transcription[0].AudioMsTotal)
	s.Zero(transcription[0].CharactersTotal)

	synthesis, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "tts", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(synthesis, 1)
	s.EqualValues(84, synthesis[0].CharactersTotal)
	s.EqualValues(4200, synthesis[0].CostMicrosTotal)
	s.EqualValues(1500, synthesis[0].AudioMsTotal)
}

func (s *StoreSuite) TestRollupIsIdempotent() {
	s.record("acme", s.base.Add(1*time.Minute), 1000, 100, true)

	for range 3 {
		_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
		s.Require().NoError(err)
	}

	buckets, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1, buckets[0].RequestCount, "re-running a rollup recomputes rather than accumulates")
	s.EqualValues(1000, buckets[0].AudioMsTotal)
}

func (s *StoreSuite) TestRollupPicksUpLateArrivingRequests() {
	s.record("acme", s.base.Add(1*time.Minute), 1000, 100, true)
	_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)

	s.record("acme", s.base.Add(2*time.Minute), 1000, 100, true)
	_, err = s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)

	buckets, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(2, buckets[0].RequestCount)
	s.EqualValues(2000, buckets[0].AudioMsTotal)
}

func (s *StoreSuite) TestDailyRollupCollapsesEveryHourOfTheDay() {
	s.record("acme", s.base, 1000, 100, true)
	s.record("acme", s.base.Add(5*time.Hour), 1000, 100, true)
	s.record("acme", s.base.Add(30*time.Hour), 1000, 100, true)

	_, err := s.store.Buckets.Rollup(s.ctx, Daily, s.base, s.base.Add(48*time.Hour))
	s.Require().NoError(err)

	buckets, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Daily, From: s.base.Add(-24 * time.Hour), To: s.base.Add(72 * time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 2, "two calendar days")
	s.EqualValues(2000, buckets[0].AudioMsTotal, "the first day holds two requests")
	s.EqualValues(1000, buckets[1].AudioMsTotal)
}

func (s *StoreSuite) TestRollupIgnoresRequestsOutsideTheWindow() {
	s.record("acme", s.base.Add(-2*time.Hour), 9999, 100, true)
	s.record("acme", s.base.Add(10*time.Minute), 1000, 100, true)

	_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)

	var buckets []Bucket
	err = s.store.shard.NewSelect().Table("stats_hourly").Where("customer_id = ?", "acme").Scan(s.ctx, &buckets)
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1000, buckets[0].AudioMsTotal)
}

func (s *StoreSuite) TestRollupStopsAtTheBucketHoldingTheEndOfTheWindow() {
	s.record("acme", s.base, 1000, 100, true)
	s.record("acme", s.base.Add(time.Hour), 9999, 100, true)

	affected, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.EqualValues(1, affected, "a request at the window's end opens the next bucket, which is not touched")

	buckets := s.rolledUp("stats_hourly", "acme")
	s.Require().Len(buckets, 1)
	s.Equal(s.base, buckets[0].Bucket.UTC())
	s.EqualValues(1000, buckets[0].AudioMsTotal)
}

func (s *StoreSuite) TestRollupOverPartOfABucketRecomputesAllOfIt() {
	for _, window := range []struct {
		name     string
		from, to time.Duration
	}{
		{"starting inside the hour", 30 * time.Minute, 90 * time.Minute},
		{"ending inside the hour", -time.Hour, 30 * time.Minute},
		{"inside the hour", 35 * time.Minute, 41 * time.Minute},
	} {
		s.Run(window.name, func() {
			// A customer of its own, so each window starts from no rollup.
			customer := window.name
			project := map[string]string{"project": "support"}
			s.tagged(customer, s.base.Add(10*time.Minute), 1000, project)
			s.tagged(customer, s.base.Add(40*time.Minute), 2000, project)
			_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
			s.Require().NoError(err)

			_, err = s.store.Buckets.Rollup(s.ctx, Hourly, s.base.Add(window.from), s.base.Add(window.to))
			s.Require().NoError(err)

			buckets := s.rolledUp("stats_hourly", customer)
			s.Require().Len(buckets, 1)
			s.EqualValues(3000, buckets[0].AudioMsTotal, "the bucket keeps both requests")
			s.EqualValues(2, buckets[0].RequestCount)

			var tagged []TagBucket
			s.Require().NoError(s.store.shard.NewSelect().Table("stats_tags_hourly").
				Where("customer_id = ?", customer).Scan(s.ctx, &tagged))
			s.Require().Len(tagged, 1)
			s.EqualValues(3000, tagged[0].AudioMsTotal, "so does the label's")
		})
	}
}

func (s *StoreSuite) TestDailyRollupOverAnHourRecomputesTheWholeDay() {
	s.record("acme", s.base.Add(time.Hour), 1000, 100, true)
	s.record("acme", s.base.Add(5*time.Hour), 2000, 100, true)
	_, err := s.store.Buckets.Rollup(s.ctx, Daily, s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)

	_, err = s.store.Buckets.Rollup(s.ctx, Daily, s.base.Add(time.Hour), s.base.Add(2*time.Hour))
	s.Require().NoError(err)

	buckets := s.rolledUp("stats_daily", "acme")
	s.Require().Len(buckets, 1)
	s.EqualValues(3000, buckets[0].AudioMsTotal, "the day keeps the request outside the hour asked for")
}

func (s *StoreSuite) TestADayIsAUTCDayWhateverTheDatabasesTimeZone() {
	// 03:00 UTC is the evening before in Denver, and 10:00 UTC the same morning.
	early, late := time.Date(2026, 3, 1, 3, 0, 0, 0, time.UTC), s.base
	project := map[string]string{"project": "support"}
	s.tagged("acme", early, 1000, project)
	s.tagged("acme", late, 2000, project)
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(early, 100)))
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(late, 100)))
	for i, at := range []time.Time{early, late} {
		session := s.opened(fmt.Sprintf("session-%d", i), "acme", at, nil)
		s.Require().NoError(s.store.Responses.Insert(s.ctx, &AgentResponse{
			ID: session.ID + "-1", SessionID: session.ID, CustomerID: "acme", CreatedAt: at,
		}))
		call := &Call{ID: fmt.Sprintf("call-%d", i), CustomerID: "acme", CallID: fmt.Sprintf("c-%d", i), AgentID: "a-1", StartedAt: at}
		s.Require().NoError(s.store.Calls.Start(s.ctx, call))
		s.Require().NoError(s.store.Calls.Finish(s.ctx, "acme", call.ID, at.Add(time.Minute)))
	}
	denver := s.inTimeZone("America/Denver")
	day := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	from, to := day.Add(-24*time.Hour), day.Add(48*time.Hour)
	s.shipStats(to)

	requests, err := denver.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Daily, From: from, To: to})
	s.Require().NoError(err)
	s.Require().Len(requests, 1)
	s.Equal(day, requests[0].Bucket.UTC())
	s.EqualValues(3000, requests[0].AudioMsTotal)

	spend, err := denver.Requests.Spend(s.ctx, "acme", "modality", Daily, from, to, 6, nil, UsageFilter{})
	s.Require().NoError(err)
	s.Require().Len(spend, 1)
	s.Equal(day, spend[0].Bucket.UTC())
	s.EqualValues(2, spend[0].RequestCount)

	turns, err := denver.TurnBuckets.Select(s.ctx, "acme", TurnBucketFilter{Granularity: Daily, From: from, To: to})
	s.Require().NoError(err)
	s.Require().Len(turns, 1)
	s.Equal(day, turns[0].Bucket.UTC())
	s.EqualValues(2, turns[0].TurnCount)

	activity, err := denver.CustomerActivity(s.ctx, "acme", ActivityDaily, from, to)
	s.Require().NoError(err)
	s.Require().Len(activity, 1, "sessions, messages, calls and people all fall in the one UTC day")
	s.Equal(day, activity[0].Bucket.UTC())
	s.Equal(ActivityBucket{Bucket: activity[0].Bucket, ActiveUsers: 1, Sessions: 2, Messages: 2, Calls: 2, VoiceMinutes: 2}, activity[0])

	_, err = denver.Buckets.Rollup(s.ctx, Daily, day.Add(time.Hour), day.Add(2*time.Hour))
	s.Require().NoError(err)
	rolled := s.rolledUp("stats_daily", "acme")
	s.Require().Len(rolled, 1)
	s.Equal(day, rolled[0].Bucket.UTC())
	s.EqualValues(3000, rolled[0].AudioMsTotal, "the rollup's day is the UTC day too")

	tagged, err := denver.TagBuckets.Select(s.ctx, "acme", TagBucketFilter{Modality: "stt", Key: "project", Granularity: Daily, From: from, To: to})
	s.Require().NoError(err)
	s.Require().Len(tagged, 1)
	s.Equal(day, tagged[0].Bucket.UTC())
	s.EqualValues(3000, tagged[0].AudioMsTotal, "so is the label rollup's")
}

func (s *StoreSuite) TestADailyRollupOnTheDayTheClocksChangeKeepsTheWholeUTCDay() {
	// Denver moves to summer time on 2026-03-08, so that day is 23 hours long there.
	day := time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC)
	s.record("acme", day.Add(time.Hour), 1000, 100, true)
	s.record("acme", day.Add(23*time.Hour+30*time.Minute), 2000, 100, true)
	// Midday the day before, which Denver's day holding midnight UTC also holds.
	s.record("acme", day.Add(-12*time.Hour), 4000, 100, true)
	denver := s.inTimeZone("America/Denver")

	_, err := denver.Buckets.Rollup(s.ctx, Daily, day, day.Add(24*time.Hour))
	s.Require().NoError(err)

	rolled := s.rolledUp("stats_daily", "acme")
	s.Require().Len(rolled, 1)
	s.Equal(day, rolled[0].Bucket.UTC())
	s.EqualValues(3000, rolled[0].AudioMsTotal, "the request in the day's last half hour is in it, and the day before is not touched")
}

// inTimeZone is another router on the suite's databases whose connections are in zone, as a
// server whose default is not UTC gives them. The zone is a startup parameter of its DSNs, which
// pgdriver passes on, so nothing outlives the test.
func (s *StoreSuite) inTimeZone(zone string) *Store {
	router, err := OpenShards(Options{DSN: inZone(s.T(), s.dsn, zone), ShardDSN: inZone(s.T(), s.shardDSN, zone)})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(router.Close()) })
	for _, db := range []*bun.DB{router.global, router.shard} {
		var current string
		s.Require().NoError(db.QueryRowContext(s.ctx, "SHOW timezone").Scan(&current))
		s.Require().Equal(zone, current)
	}
	return router
}

// inZone is dsn with its connections' timezone set to zone.
func inZone(t *testing.T, dsn, zone string) string {
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("timezone", zone)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// rolledUp is what a rollup table holds for a customer, oldest first.
func (s *StoreSuite) rolledUp(table, customer string) []Bucket {
	var buckets []Bucket
	err := s.store.shard.NewSelect().Table(table).Where("customer_id = ?", customer).Order("bucket").Scan(s.ctx, &buckets)
	s.Require().NoError(err)
	return buckets
}

func (s *StoreSuite) TestCustomerStatsAreIsolatedPerCustomer() {
	s.record("acme", s.base.Add(1*time.Minute), 1000, 100, true)
	s.record("globex", s.base.Add(1*time.Minute), 5000, 100, true)

	_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)

	acme, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(acme, 1)
	s.EqualValues(1000, acme[0].AudioMsTotal)

	globex, err := s.store.Buckets.Select(s.ctx, "globex", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(globex, 1)
	s.EqualValues(5000, globex[0].AudioMsTotal)
}

func (s *StoreSuite) TestCustomerStatsIsEmptyForAnUnknownCustomer() {
	buckets, err := s.store.Buckets.Select(s.ctx, "nobody", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Empty(buckets)
}

func (s *StoreSuite) TestRollupRejectsBadArguments() {
	_, err := s.store.Buckets.Rollup(s.ctx, Granularity("weekly"), s.base, s.base.Add(time.Hour))
	s.ErrorContains(err, `unknown granularity "weekly"`)

	_, err = s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base)
	s.ErrorContains(err, "window must be non-empty")
}

func (s *StoreSuite) TestCustomerStatsRejectsBadArguments() {
	_, err := s.store.Buckets.Select(s.ctx, "", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.ErrorContains(err, "customer id is required")

	_, err = s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.ErrorContains(err, "modality is required")

	_, err = s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Granularity("weekly"), From: s.base, To: s.base.Add(time.Hour)})
	s.ErrorContains(err, "unknown granularity")
}

func (s *StoreSuite) TestFilteringByLabelReadsTheRequestsTheRollupHasForgotten() {
	s.tagged("acme", s.base.Add(time.Minute), 1000, map[string]string{"project": "support"})
	s.tagged("acme", s.base.Add(2*time.Minute), 9000, map[string]string{"project": "sales"})

	buckets, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour), Tags: map[string]string{"project": "support"}})
	s.Require().NoError(err)

	s.Require().Len(buckets, 1)
	s.EqualValues(1000, buckets[0].AudioMsTotal, "only the support request counts")
}

func (s *StoreSuite) TestStatsIncludeNewRequestsWithoutRollups() {
	s.record("acme", s.base.Add(time.Minute), 1000, 100, true)
	s.record("other", s.base.Add(time.Minute), 9000, 100, true)
	buckets, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1, buckets[0].RequestCount)
	s.EqualValues(1000, buckets[0].AudioMsTotal)
	s.record("acme", s.base.Add(2*time.Minute), 2000, 100, true)
	buckets, err = s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(2, buckets[0].RequestCount)
	s.EqualValues(3000, buckets[0].AudioMsTotal)
}

// recordCancelled stores a request its caller gave up on after it had generated something.
func (s *StoreSuite) recordCancelled(at time.Time, latencyMs *float64) {
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "stt", CustomerID: "acme", Provider: "deepgram", Model: "flux-general-en",
		Tags: map[string]string{"project": "support"}, StartedAt: at,
		AudioMs: 700, CostMicros: 50, LatencyMs: latencyMs,
		Success: false, ErrorCode: ErrorCancelled,
	}))
}

func (s *StoreSuite) TestACancelledRequestIsSpendButNeitherAnErrorNorALatency() {
	for index, latency := range []float64{100, 200} {
		s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
			Modality: "stt", CustomerID: "acme", Provider: "deepgram", Model: "flux-general-en",
			Tags: map[string]string{"project": "support"}, StartedAt: s.base.Add(time.Duration(index+1) * time.Minute),
			AudioMs: 1000, CostMicros: 100, LatencyMs: &latency, Success: true,
		}))
	}
	failedLatency := 900.0
	s.Require().NoError(insertRequest(s.ctx, s.store, &Request{
		Modality: "stt", CustomerID: "acme", Provider: "deepgram", Model: "flux-general-en",
		Tags: map[string]string{"project": "support"}, StartedAt: s.base.Add(3 * time.Minute),
		AudioMs: 500, CostMicros: 60, LatencyMs: &failedLatency, Success: false, ErrorCode: "upstream_error",
	}))
	// Closed after a first token, and so with a latency, and closed before one, with none.
	cancelledLatency := 5.0
	s.recordCancelled(s.base.Add(4*time.Minute), &cancelledLatency)
	s.recordCancelled(s.base.Add(5*time.Minute), nil)

	_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)

	check := func(name string, requests, errors, audio, cost int64, p50 *float64, uptime *float64) {
		s.EqualValues(5, requests, name+": a cancelled request is still a request")
		s.EqualValues(1, errors, name+": and is no error")
		s.EqualValues(3900, audio, name+": what it generated is still counted")
		s.EqualValues(360, cost, name+": and still billed")
		s.Require().NotNil(p50, name)
		s.InDelta(200.0, *p50, 0.001, name+": its latency is not one the provider had")
		s.Require().NotNil(uptime, name)
		s.InDelta(4.0/5.0, *uptime, 0.001, name+": nor is it downtime")
	}

	byProvider, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(byProvider, 1)
	bucket := byProvider[0]
	check("rollup", bucket.RequestCount, bucket.ErrorCount, bucket.AudioMsTotal, bucket.CostMicrosTotal,
		bucket.LatencyP50Ms, bucket.Uptime)

	byTag, err := s.store.TagBuckets.Select(s.ctx, "acme", TagBucketFilter{Modality: "stt", Key: "project", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(byTag, 1)
	tag := byTag[0]
	s.EqualValues(5, tag.RequestCount, "tag rollup: a cancelled request is still a request")
	s.EqualValues(1, tag.ErrorCount, "tag rollup: and is no error")
	s.EqualValues(360, tag.CostMicrosTotal, "tag rollup: and still billed")
	s.Require().NotNil(tag.LatencyP50Ms)
	s.InDelta(200.0, *tag.LatencyP50Ms, 0.001, "tag rollup: its latency is not one the provider had")

	filtered, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour), Tags: map[string]string{"project": "support"}})
	s.Require().NoError(err)
	s.Require().Len(filtered, 1)
	bucket = filtered[0]
	check("raw rows", bucket.RequestCount, bucket.ErrorCount, bucket.AudioMsTotal, bucket.CostMicrosTotal,
		bucket.LatencyP50Ms, bucket.Uptime)
}

func (s *StoreSuite) TestARefusedRequestIsSpendButNeitherAnErrorNorALatency() {
	served, refused := 100.0, 900.0
	for index, request := range []Request{
		{LatencyMs: &served, Success: true},
		{LatencyMs: &refused, Success: false, ErrorCode: ErrorRefused},
	} {
		request.Modality, request.CustomerID, request.Provider, request.Model = "llm", "acme", "openai", "gpt"
		request.Tags = map[string]string{"project": "support"}
		request.StartedAt = s.base.Add(time.Duration(index+1) * time.Minute)
		request.OutputTokens, request.CostMicros = 10, 25
		s.Require().NoError(insertRequest(s.ctx, s.store, &request))
	}
	_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)

	check := func(name string, requests, errors, cost int64, p50 *float64) {
		s.EqualValues(2, requests, name+": a refused request is still a request")
		s.EqualValues(0, errors, name+": and is no error of the provider's")
		s.EqualValues(50, cost, name+": and still billed")
		s.Require().NotNil(p50, name)
		s.InDelta(100.0, *p50, 0.001, name+": its latency is not one the provider had")
	}
	byProvider, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "llm", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(byProvider, 1)
	check("rollup", byProvider[0].RequestCount, byProvider[0].ErrorCount, byProvider[0].CostMicrosTotal, byProvider[0].LatencyP50Ms)
	s.Require().NotNil(byProvider[0].Uptime)
	s.InDelta(1.0, *byProvider[0].Uptime, 0.001, "nor is it downtime")

	byTag, err := s.store.TagBuckets.Select(s.ctx, "acme", TagBucketFilter{Modality: "llm", Key: "project", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(byTag, 1)
	check("tag rollup", byTag[0].RequestCount, byTag[0].ErrorCount, byTag[0].CostMicrosTotal, byTag[0].LatencyP50Ms)

	filtered, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "llm", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour), Tags: map[string]string{"project": "support"}})
	s.Require().NoError(err)
	s.Require().Len(filtered, 1)
	check("raw rows", filtered[0].RequestCount, filtered[0].ErrorCount, filtered[0].CostMicrosTotal, filtered[0].LatencyP50Ms)
	s.Require().NotNil(filtered[0].Uptime)
	s.InDelta(1.0, *filtered[0].Uptime, 0.001, "raw rows: nor is it downtime")
}

func (s *StoreSuite) TestBucketQueryBudgets() {
	for i := range 4 {
		s.record("acme", s.base.Add(time.Duration(i)*time.Minute), 1000, 100, true)
	}

	// A router rolls up its own shard: one statement for the table and one for its labels'.
	s.Equal(2, s.countQueries(func() {
		_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
		s.Require().NoError(err)
	}))
	// A wider window is the same two a bucket at a time, so no statement covers all of it.
	s.Equal(6, s.countQueries(func() {
		_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base.Add(30*time.Minute), s.base.Add(3*time.Hour))
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		buckets, err := s.store.Buckets.Select(s.ctx, "acme", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
		s.Require().NoError(err)
		s.Len(buckets, 1)
	}))
}
