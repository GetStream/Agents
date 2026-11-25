//go:build integration

package store

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// unreachableShard is a DSN no Postgres answers on.
const unreachableShard = "postgres://postgres:postgres@127.0.0.1:1/down?sslmode=disable&dial_timeout=1s"

// requestIn stores a request of app's in the shard of into.
func (s *ShardedStoreSuite) requestIn(into *Store, app string, at time.Time, audioMs int64, tags map[string]string) {
	latency := 100.0
	s.Require().NoError(insertRequest(s.ctx, into, &Request{Modality: "stt", CustomerID: app, Provider: "deepgram",
		Model: "flux-general-en", StartedAt: at, AudioMs: audioMs, LatencyMs: &latency, Success: true, Tags: tags}))
}

// withTheOtherShardDown is a router of this shard whose other shard does not answer.
func (s *ShardedStoreSuite) withTheOtherShardDown() *Store {
	down, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN, Shards: map[string]string{"here": s.shardDSN, "there": unreachableShard}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(down.Close()) })
	return down
}

// shipped is what the shared database holds of every shard's stats, a row per line in key order.
func (s *ShardedStoreSuite) shipped() string {
	var rows []string
	for _, table := range []shippedTable{shippedStats(Hourly), shippedStats(Daily), shippedTagStats(Hourly),
		shippedTagStats(Daily), shippedTurns, shippedLatency} {
		var found []string
		s.Require().NoError(s.store.global.NewSelect().TableExpr(table.name+" AS t").ColumnExpr("t::text").
			OrderExpr("t::text").Scan(s.ctx, &found))
		rows = append(rows, table.name)
		rows = append(rows, found...)
	}
	return strings.Join(rows, "\n")
}

// Defect: a ship that adds a bucket's values to what the shard shipped before rather than
// replacing them, which counts every request again each minute.
func (s *ShardedStoreSuite) TestShippingTwiceLeavesTheSameStats() {
	project := map[string]string{"project": "support"}
	s.requestIn(s.store, "app", s.base.Add(5*time.Minute), 1000, project)
	s.requestIn(s.other, "app", s.base.Add(6*time.Minute), 2000, project)
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, timed("app", s.base, nil, nil, nil, lasted(100))))
	now := s.base.Add(10 * time.Minute)

	s.Require().NoError(s.store.ShipStats(s.ctx, now))
	s.Require().NoError(s.other.ShipStats(s.ctx, now))
	once := s.shipped()
	s.Require().NoError(s.store.ShipStats(s.ctx, now))
	s.Require().NoError(s.other.ShipStats(s.ctx, now))

	s.Equal(once, s.shipped())
	buckets, err := s.store.Buckets.Shipped(s.ctx, "app", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: now})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(3000, buckets[0].AudioMsTotal)
	s.EqualValues(2, buckets[0].RequestCount)
	turns, err := s.store.TurnBuckets.Shipped(s.ctx, "app", TurnBucketFilter{Granularity: Hourly, From: s.base, To: now})
	s.Require().NoError(err)
	s.Require().Len(turns, 1)
	s.EqualValues(1, turns[0].TurnCount)
}

// Defect: an hour shipped only while it is the current one, so a request or a turn written
// after its hour closed never reaches the stats pages.
func (s *ShardedStoreSuite) TestARequestWrittenAfterItsHourClosedIsShippedTheHourAfter() {
	s.requestIn(s.store, "app", s.base.Add(5*time.Minute), 1000, nil)
	s.Require().NoError(s.store.ShipStats(s.ctx, s.base.Add(59*time.Minute)))
	s.Require().NoError(s.store.ShipStats(s.ctx, s.base.Add(time.Hour+time.Minute)))
	s.requestIn(s.store, "app", s.base.Add(58*time.Minute), 500, nil)
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, timed("app", s.base.Add(58*time.Minute), nil, nil, nil, lasted(100))))

	s.Require().NoError(s.store.ShipStats(s.ctx, s.base.Add(time.Hour+2*time.Minute)))

	hour := BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)}
	buckets, err := s.store.Buckets.Shipped(s.ctx, "app", hour)
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1500, buckets[0].AudioMsTotal, "the late request is in its hour")
	day := hour
	day.Granularity = Daily
	days, err := s.store.Buckets.Shipped(s.ctx, "app", day)
	s.Require().NoError(err)
	s.Require().Len(days, 1)
	s.EqualValues(1500, days[0].AudioMsTotal, "and in its day")
	turns, err := s.store.TurnBuckets.Shipped(s.ctx, "app", TurnBucketFilter{Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(turns, 1)
	s.EqualValues(1, turns[0].TurnCount, "the late turn is in its hour")
}

// Defect: a first ship that sends only the hour it runs in, which leaves every stats page empty
// for the time before the shard started shipping.
func (s *ShardedStoreSuite) TestTheFirstShipSendsEveryHourTheShardHas() {
	long := s.base.Add(-72 * time.Hour)
	s.requestIn(s.store, "app", long, 1000, nil)
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, timed("app", long.Add(-time.Hour), nil, nil, nil, lasted(100))))

	s.Require().NoError(s.store.ShipStats(s.ctx, s.base))

	buckets, err := s.store.Buckets.Shipped(s.ctx, "app", BucketFilter{Modality: "stt", Granularity: Daily, From: long, To: s.base})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1000, buckets[0].AudioMsTotal)
	turns, err := s.store.TurnBuckets.Shipped(s.ctx, "app", TurnBucketFilter{Granularity: Daily, From: long.Add(-time.Hour), To: s.base})
	s.Require().NoError(err)
	s.Require().Len(turns, 1, "the turns before the first request are sent too")
}

// Defect: a shard that stopped shipping for a while carries on from the hour before now, and
// the hours it was away stay as they were when it stopped.
func (s *ShardedStoreSuite) TestAShardThatStoppedShippingSendsTheHoursSince() {
	s.Require().NoError(s.store.ShipStats(s.ctx, s.base))
	s.requestIn(s.store, "app", s.base.Add(2*time.Hour), 1000, nil)

	s.Require().NoError(s.store.ShipStats(s.ctx, s.base.Add(5*time.Hour)))

	buckets, err := s.store.Buckets.Shipped(s.ctx, "app", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: s.base.Add(5 * time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1000, buckets[0].AudioMsTotal)
}

// Defect: the stats read every shard rather than what they shipped, so one shard being down
// fails everybody's stats; or they read only this shard's rows of the shared database.
func (s *ShardedStoreSuite) TestShippedStatsAreEveryShardsSumWithAShardDown() {
	project := map[string]string{"project": "support"}
	s.requestIn(s.store, "app", s.base.Add(5*time.Minute), 1000, project)
	s.requestIn(s.other, "app", s.base.Add(6*time.Minute), 2000, project)
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, timed("app", s.base, nil, nil, nil, lasted(1000))))
	s.Require().NoError(s.other.TurnBuckets.Add(s.ctx, timed("app", s.base, nil, nil, nil, lasted(100))))
	s.Require().NoError(s.other.TurnBuckets.Add(s.ctx, timed("app", s.base, nil, nil, nil, lasted(100))))
	now := s.base.Add(10 * time.Minute)
	s.Require().NoError(s.store.ShipStats(s.ctx, now))
	s.Require().NoError(s.other.ShipStats(s.ctx, now))
	down := s.withTheOtherShardDown()
	_, err := down.Buckets.Select(s.ctx, "app", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: now})
	s.Require().Error(err, "the other shard does not answer")

	buckets, err := down.Buckets.Shipped(s.ctx, "app", BucketFilter{Modality: "stt", Granularity: Daily, From: s.base, To: now})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(3000, buckets[0].AudioMsTotal)
	s.EqualValues(2, buckets[0].RequestCount)
	s.Require().NotNil(buckets[0].Uptime)
	s.InDelta(1, *buckets[0].Uptime, 0.001)
	tags, err := down.TagBuckets.Shipped(s.ctx, "app", TagBucketFilter{Modality: "stt", Key: "project", Granularity: Hourly, From: s.base, To: now})
	s.Require().NoError(err)
	s.Require().Len(tags, 1)
	s.Equal("support", tags[0].TagValue)
	s.EqualValues(3000, tags[0].AudioMsTotal)
	turns, err := down.TurnBuckets.Shipped(s.ctx, "app", TurnBucketFilter{Granularity: Hourly, From: s.base, To: now})
	s.Require().NoError(err)
	s.Require().Len(turns, 1)
	s.EqualValues(3, turns[0].TurnCount)
	s.Require().NotNil(turns[0].RoundtripP95Ms)
	s.InEpsilon(1000, *turns[0].RoundtripP95Ms, 0.05)
	legs, err := down.TurnBuckets.ShippedLatency(s.ctx, []string{"app"}, s.base)
	s.Require().NoError(err)
	s.EqualValues(3, legs[3].Turns)
}

// Defect: the list fails while a shard is down, or names a shard other than the one that did
// not answer.
func (s *ShardedStoreSuite) TestACallListLeavesOutAShardThatIsDownAndNamesIt() {
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{ID: "here", CustomerID: "app", StartedAt: s.base}))
	down := s.withTheOtherShardDown()

	calls, missed, err := down.Calls.SelectReachable(s.ctx, "app", CallFilter{})
	s.Require().NoError(err)
	s.Require().Len(calls, 1)
	s.Equal("here", calls[0].ID)
	s.Require().NotNil(missed)
	s.Equal([]string{"there"}, missed.Shards())
	_, err = down.Calls.Select(s.ctx, "app", CallFilter{})
	s.ErrorContains(err, "there", "a read that needs every shard still fails")
}

// Defect: every router of a shard ships at once, or one router holds the lock of every shard,
// leaving the other shards' stats unshipped.
func (s *ShardedStoreSuite) TestOneRouterOfEachShardShips() {
	second, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(second.Close()) })
	ctx, cancel := context.WithCancel(s.ctx)
	var running sync.WaitGroup
	shippers := []*StatsShipper{}
	for _, router := range []*Store{s.store, second, s.other} {
		shipper := router.StatsShipper(slog.New(slog.DiscardHandler), 10*time.Millisecond)
		shippers = append(shippers, shipper)
		running.Go(func() { shipper.Run(ctx) })
	}
	defer func() {
		cancel()
		running.Wait()
	}()

	s.Eventually(func() bool {
		return (shippers[0].Holding() || shippers[1].Holding()) && shippers[2].Holding()
	}, 10*time.Second, 10*time.Millisecond, "a router of each shard ships")
	s.Never(func() bool { return shippers[0].Holding() && shippers[1].Holding() },
		200*time.Millisecond, 10*time.Millisecond, "two routers of one shard ship")
}
