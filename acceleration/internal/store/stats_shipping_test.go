//go:build integration

package store

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// requestIn stores a request of app's in the shard of into.
func (s *ShardedStoreSuite) requestIn(into *Store, app string, at time.Time, audioMs int64, tags map[string]string) {
	latency := 100.0
	s.Require().NoError(insertRequest(s.ctx, into, &Request{Modality: "stt", CustomerID: app, Provider: "deepgram",
		Model: "flux-general-en", StartedAt: at, AudioMs: audioMs, LatencyMs: &latency, Success: true, Tags: tags}))
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

// Defect: a first ship that starts at the first request or turn, which leaves out the sessions
// and calls of a shard whose agents only chatted before.
func (s *ShardedStoreSuite) TestTheFirstShipSendsTheSessionsBeforeAnyRequest() {
	long := s.base.Add(-72 * time.Hour)
	s.opened("chat", "app", long, nil)
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{ID: "called", CustomerID: "app", CallID: "c", StartedAt: long}))
	s.Require().NoError(s.store.Calls.Finish(s.ctx, "app", "called", long.Add(time.Minute)))

	s.Require().NoError(s.store.ShipStats(s.ctx, s.base))

	activity, err := s.store.CustomerActivity(s.ctx, "app", ActivityDaily, long, s.base)
	s.Require().NoError(err)
	s.Require().Len(activity, 1)
	s.EqualValues(1, activity[0].Sessions)
	s.EqualValues(1, activity[0].Calls)
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

// Defect: the stats read another shard rather than what it shipped; or they read only this
// shard's rows of the shared database.
func (s *ShardedStoreSuite) TestShippedStatsAreEveryShardsSumWhereTheLocalOnesAreThisShardsAlone() {
	project := map[string]string{"project": "support"}
	s.requestIn(s.store, "app", s.base.Add(5*time.Minute), 1000, project)
	s.requestIn(s.other, "app", s.base.Add(6*time.Minute), 2000, project)
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, timed("app", s.base, nil, nil, nil, lasted(1000))))
	s.Require().NoError(s.other.TurnBuckets.Add(s.ctx, timed("app", s.base, nil, nil, nil, lasted(100))))
	s.Require().NoError(s.other.TurnBuckets.Add(s.ctx, timed("app", s.base, nil, nil, nil, lasted(100))))
	now := s.base.Add(10 * time.Minute)
	s.Require().NoError(s.store.ShipStats(s.ctx, now))
	s.Require().NoError(s.other.ShipStats(s.ctx, now))
	local, err := s.store.Buckets.Select(s.ctx, "app", BucketFilter{Modality: "stt", Granularity: Hourly, From: s.base, To: now})
	s.Require().NoError(err)
	s.Require().Len(local, 1)
	s.EqualValues(1000, local[0].AudioMsTotal, "the other shard's requests are not read")

	buckets, err := s.store.Buckets.Shipped(s.ctx, "app", BucketFilter{Modality: "stt", Granularity: Daily, From: s.base, To: now})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(3000, buckets[0].AudioMsTotal)
	s.EqualValues(2, buckets[0].RequestCount)
	s.Require().NotNil(buckets[0].Uptime)
	s.InDelta(1, *buckets[0].Uptime, 0.001)
	tags, err := s.store.TagBuckets.Shipped(s.ctx, "app", TagBucketFilter{Modality: "stt", Key: "project", Granularity: Hourly, From: s.base, To: now})
	s.Require().NoError(err)
	s.Require().Len(tags, 1)
	s.Equal("support", tags[0].TagValue)
	s.EqualValues(3000, tags[0].AudioMsTotal)
	turns, err := s.store.TurnBuckets.Shipped(s.ctx, "app", TurnBucketFilter{Granularity: Hourly, From: s.base, To: now})
	s.Require().NoError(err)
	s.Require().Len(turns, 1)
	s.EqualValues(3, turns[0].TurnCount)
	s.Require().NotNil(turns[0].RoundtripP95Ms)
	s.InEpsilon(1000, *turns[0].RoundtripP95Ms, 0.05)
	legs, err := s.store.TurnBuckets.ShippedLatency(s.ctx, []string{"app"}, s.base)
	s.Require().NoError(err)
	s.EqualValues(3, legs[3].Turns)
}

// Defect: a call list reads another shard, or lists a call twice, or without the shard it lives
// in.
func (s *ShardedStoreSuite) TestACallListReadsAnotherShardsCallsFromTheIndex() {
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{ID: "here", CustomerID: "app", StartedAt: s.base}))
	s.Require().NoError(s.other.Calls.Start(s.ctx, &Call{ID: "there", CustomerID: "app", StartedAt: s.base.Add(time.Minute)}))
	s.Require().NoError(s.store.ShipIndex(s.ctx, s.base))
	s.Require().NoError(s.other.ShipIndex(s.ctx, s.base))

	calls, err := s.store.Calls.Select(s.ctx, "app", CallFilter{})
	s.Require().NoError(err)
	regions := map[string]string{}
	for _, call := range calls {
		regions[call.ID] = call.Region
	}
	s.Equal(map[string]string{"here": "here", "there": "there"}, regions)
	s.Len(calls, 2)
}

// Defect: a session list reads this shard alone, or misses a pin another shard shipped, or a
// pin here it has not shipped yet, or lists a session twice.
func (s *ShardedStoreSuite) TestASessionListReadsEveryShardsSessionsWithTheirPins() {
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &AgentSession{ID: "mine", CustomerID: "app"}))
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, &AgentSession{ID: "theirs", CustomerID: "app"}))
	s.Require().NoError(s.store.ShipIndex(s.ctx, s.base))
	s.Require().NoError(s.other.ShipIndex(s.ctx, s.base))
	_, err := s.other.Sessions.Pin(s.ctx, "app", "theirs", true, s.base)
	s.Require().NoError(err)
	s.Require().NoError(s.other.ShipIndex(s.ctx, s.base))
	_, err = s.store.Sessions.Pin(s.ctx, "app", "mine", true, s.base)
	s.Require().NoError(err)

	listed, err := s.store.Sessions.Select(s.ctx, "app", SessionFilter{})
	s.Require().NoError(err)
	regions := map[string]string{}
	for _, session := range listed {
		regions[session.ID] = session.Region
		s.NotNil(session.PinnedAt, session.ID)
	}
	s.Equal(map[string]string{"mine": "here", "theirs": "there"}, regions)
	s.Len(listed, 2)
}

// workedIn is one session of app's in into, opened at at and closed ten minutes later, with
// two turns, a call of tool in the second, a phone call of five minutes and a request costing
// cost.
func (s *ShardedStoreSuite) workedIn(into *Store, id, user, tool string, failed bool, at time.Time, cost int64) {
	s.Require().NoError(into.Sessions.Upsert(s.ctx, &AgentSession{ID: id, CustomerID: "app", AgentName: "docs",
		ConfigID: "cfg-docs", UserID: user, CallerKind: "user", CreatedAt: at}))
	s.Require().NoError(into.Sessions.Close(s.ctx, "app", id, at, at.Add(10*time.Minute), "hangup"))
	s.Require().NoError(into.Responses.Insert(s.ctx, &AgentResponse{ID: id + "-1", SessionID: id, CustomerID: "app",
		CreatedAt: at.Add(time.Minute)}))
	s.Require().NoError(into.Responses.Insert(s.ctx, &AgentResponse{ID: id + "-2", SessionID: id, CustomerID: "app",
		ParentResponseID: id + "-1", CreatedAt: at.Add(2 * time.Minute)}))
	result := map[string]any{"call_id": id}
	if failed {
		result["error"] = "not found"
	}
	s.Require().NoError(into.ResponseItems.InsertMany(s.ctx, []AgentResponseItem{
		{ResponseID: id + "-2", Ordinal: 0, SessionID: id, Kind: ItemToolCall, ToolName: tool,
			Payload: map[string]any{"call_id": id}, At: at.Add(2 * time.Minute)},
		{ResponseID: id + "-2", Ordinal: 1, SessionID: id, Kind: ItemToolResult, ToolName: tool,
			Text: "done", Payload: result, At: at.Add(2*time.Minute + time.Second)},
	}))
	s.Require().NoError(into.Calls.Start(s.ctx, &Call{ID: id + "-call", SessionID: id, CustomerID: "app",
		CallType: "default", CallID: id, StartedAt: at}))
	s.Require().NoError(into.Calls.Finish(s.ctx, "app", id+"-call", at.Add(5*time.Minute)))
	s.Require().NoError(insertRequest(s.ctx, into, &Request{Modality: "llm", CustomerID: "app", Provider: "openai",
		Model: "gpt", StartedAt: at.Add(time.Minute), InputTokens: cost, CostMicros: cost, Success: true}))
}

// statsOf is every stats and spend page of app's over the two hours from s.base, as read.
func (s *ShardedStoreSuite) statsOf(read *Store) map[string]any {
	from, to := s.base, s.base.Add(2*time.Hour)
	meters, err := read.Spend.Meters(s.ctx, []string{"app"}, from)
	s.Require().NoError(err)
	activity, err := read.CustomerActivity(s.ctx, "app", ActivityDaily, from, to)
	s.Require().NoError(err)
	for i := range activity {
		activity[i].Bucket = activity[i].Bucket.UTC()
	}
	turns, err := read.CustomerTurns(s.ctx, "app", "", from, to)
	s.Require().NoError(err)
	overall, tools, err := read.CustomerToolUsage(s.ctx, "app", "", from, to)
	s.Require().NoError(err)
	peak, err := read.CustomerConcurrencyPeak(s.ctx, "app", from, to)
	s.Require().NoError(err)
	s.Require().NotNil(peak.At)
	return map[string]any{"meters": meters, "activity": activity, "turns": turns,
		"overall tool use": overall, "tools": tools, "peak": peak.Sessions, "peak at": peak.At.UTC()}
}

// Defect: a rollup that ships one shard's rows over another's, a reader that reads this shard's
// rollup alone, or a sum of shards that counts a session, a person or a minute's sessions other
// than one database of them all would.
func (s *ShardedStoreSuite) TestTwoShardsRollupsAgreeWithOneDatabaseOfThemAll() {
	local, err := OpenShards(Options{DSN: s.emptyDatabase("store_local")})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(local.Close()) })
	s.Require().NoError(local.Migrate(s.ctx))
	// a, b and c are open at once from the fifth minute; d in the next hour. jlahey is seen
	// in both shards.
	work := []struct {
		id, user, tool string
		failed         bool
		at             time.Duration
		cost           int64
		there          bool
	}{
		{"a", "jlahey", "lookup_order", false, 0, 100, false},
		{"b", "randy", "lookup_order", true, 3 * time.Minute, 200, false},
		{"c", "jlahey", "refund", false, 5 * time.Minute, 400, true},
		{"d", "bubbles", "lookup_order", false, 70 * time.Minute, 800, true},
	}
	for _, w := range work {
		s.workedIn(local, w.id, w.user, w.tool, w.failed, s.base.Add(w.at), w.cost)
		shard := s.store
		if w.there {
			shard = s.other
		}
		s.workedIn(shard, w.id, w.user, w.tool, w.failed, s.base.Add(w.at), w.cost)
	}
	now := s.base.Add(2 * time.Hour)
	s.Require().NoError(s.store.ShipStats(s.ctx, now))
	s.Require().NoError(s.other.ShipStats(s.ctx, now))

	want := s.statsOf(local)
	meters := want["meters"].([]MeterUse)
	s.Require().Len(meters, 1)
	s.Require().EqualValues(1500, meters[0].CostMicros, "the local answer is the whole workload's")
	s.Require().EqualValues(int64(3), want["peak"])
	s.Equal(want, s.statsOf(s.store))
	s.Equal(want, s.statsOf(s.other), "every shard reads the same copy")
}

// Defect: every router of a shard ships at once, or one router holds the lock of every shard,
// leaving the other shards' stats unshipped.
func (s *ShardedStoreSuite) TestOneRouterOfEachShardShips() {
	second, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(second.Close()) })
	ctx, cancel := context.WithCancel(s.ctx)
	var running sync.WaitGroup
	shippers := []*Shipper{}
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
