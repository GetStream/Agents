//go:build integration

package store

import "time"

func (s *StoreSuite) TestTurnStatsReportThePercentilesOfWhatCallersWaited() {
	for i, roundtrip := range []float64{100, 200, 300, 400} {
		s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(s.base.Add(time.Duration(i)*time.Minute), roundtrip)))
	}

	buckets, err := s.store.TurnBuckets.Select(s.ctx, "acme", TurnBucketFilter{AgentID: "agent-1", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(4, buckets[0].TurnCount)
	s.InDelta(8000, buckets[0].AudioOutMsTotal, 0.001)
	s.Require().NotNil(buckets[0].RoundtripP50Ms)
	s.InEpsilon(200, *buckets[0].RoundtripP50Ms, 0.05, "the median of 100,200,300,400, to the nearest rank")
	s.InEpsilon(400, *buckets[0].RoundtripP95Ms, 0.05)
	s.InEpsilon(120, *buckets[0].LLMTTFTP50Ms, 0.05)
}

func (s *StoreSuite) TestADayIsItsHoursAddedUp() {
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(s.base.Add(time.Minute), 100)))
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(s.base.Add(5*time.Hour), 100)))
	interrupted := s.turn(s.base.Add(9*time.Hour), 900)
	interrupted.Interrupted = true
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, interrupted))

	hours, err := s.store.TurnBuckets.Select(s.ctx, "acme", TurnBucketFilter{Granularity: Hourly, From: s.base, To: s.base.Add(24 * time.Hour)})
	s.Require().NoError(err)
	days, err := s.store.TurnBuckets.Select(s.ctx, "acme", TurnBucketFilter{Granularity: Daily, From: s.base, To: s.base.Add(24 * time.Hour)})
	s.Require().NoError(err)

	s.Len(hours, 3)
	s.Require().Len(days, 1)
	s.EqualValues(3, days[0].TurnCount)
	s.EqualValues(1, days[0].InterruptedCount)
	s.InEpsilon(100, *days[0].RoundtripP50Ms, 0.05)
	s.InEpsilon(900, *days[0].RoundtripP99Ms, 0.05, "the slow turn is the day's tail")
}

func (s *StoreSuite) TestALegThatNeverHappenedIsNotCountedAsInstant() {
	// A realtime model that hears and speaks for itself has no transcription leg, and
	// counting that as zero would flatter the percentiles.
	turn := s.turn(s.base.Add(time.Minute), 300)
	turn.STTLatencyMs = nil
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, turn))

	buckets, err := s.store.TurnBuckets.Select(s.ctx, "acme", TurnBucketFilter{Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.Nil(buckets[0].STTLatencyP50Ms, "no transcription happened, so there is nothing to report")
	s.NotNil(buckets[0].RoundtripP50Ms)
}

func (s *StoreSuite) TestOneAgentsTurnsAreNotAnothers() {
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(s.base.Add(time.Minute), 100)))
	theirs := s.turn(s.base.Add(time.Minute), 900)
	theirs.AgentID = "agent-2"
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, theirs))

	buckets, err := s.store.TurnBuckets.Select(s.ctx, "acme", TurnBucketFilter{AgentID: "agent-1", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1, buckets[0].TurnCount)
	s.InEpsilon(100, *buckets[0].RoundtripP99Ms, 0.05)
}

func (s *StoreSuite) TestATurnWindowInsideItsHoursReturnsEveryHourItTouches() {
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(s.base.Add(45*time.Minute), 100)))
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(s.base.Add(2*time.Hour+45*time.Minute), 100)))

	buckets, err := s.store.TurnBuckets.Select(s.ctx, "acme", TurnBucketFilter{Granularity: Hourly,
		From: s.base.Add(30 * time.Minute), To: s.base.Add(2*time.Hour + 30*time.Minute)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 2, "the hour the window starts in and the hour it ends in")
	s.Equal(s.base, buckets[0].Bucket.UTC())
	s.Equal(s.base.Add(2*time.Hour), buckets[1].Bucket.UTC())
	s.Require().NotNil(buckets[0].RoundtripP50Ms, "the start hour's latency is read with it")
}

func (s *StoreSuite) TestADailyTurnWindowStartingMidDayCountsOnlyTheHoursInIt() {
	day := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(day.Add(5*time.Hour), 100)))
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(day.Add(15*time.Hour), 100)))
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(day.Add(32*time.Hour), 100)))
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(day.Add(40*time.Hour), 100)))

	days, err := s.store.TurnBuckets.Select(s.ctx, "acme", TurnBucketFilter{Granularity: Daily,
		From: day.Add(10 * time.Hour), To: day.Add(34 * time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(days, 2)
	s.Equal(day, days[0].Bucket.UTC())
	s.EqualValues(1, days[0].TurnCount, "the morning before the window is not in it")
	s.Equal(day.Add(24*time.Hour), days[1].Bucket.UTC())
	s.EqualValues(1, days[1].TurnCount, "nor the evening after it")
}

// turn builds one conversational turn with every leg measured.
func (s *StoreSuite) turn(at time.Time, roundtripMs float64) TurnSample {
	sttMs, ttftMs, ttfbMs := 30.0, 120.0, 90.0
	return TurnSample{
		CustomerID:   "acme",
		AgentID:      "agent-1",
		StartedAt:    at,
		AudioOutMs:   2000,
		STTLatencyMs: &sttMs,
		LLMTTFTMs:    &ttftMs,
		TTSTTFBMs:    &ttfbMs,
		RoundtripMs:  &roundtripMs,
	}
}

// timed is a turn of app's at at whose legs took the milliseconds given, nil for a leg it
// did not have.
func timed(appID string, at time.Time, stt, llm, tts, roundtrip *float64) TurnSample {
	return TurnSample{CustomerID: appID, AgentID: "agent-" + appID, StartedAt: at,
		STTLatencyMs: stt, LLMTTFTMs: llm, TTSTTFBMs: tts, RoundtripMs: roundtrip}
}

// lasted is v milliseconds, as a turn holds a leg it had.
func lasted(v float64) *float64 { return &v }

// Defect: the latency of one app of the organization only, another organization's turns or
// turns before the hour of since counted, a turn in the hour since falls in left out, a leg
// read off another leg's bins, or a leg no turn had left out of the list.
func (s *StoreSuite) TestTurnLatencyReadsEveryLegOfTheAppsAskedFor() {
	since := s.base.Add(30 * time.Minute)
	for n := 1; n <= 10; n++ {
		app := []string{"one", "two"}[n%2]
		s.Require().NoError(s.store.TurnBuckets.Add(s.ctx,
			timed(app, s.base.Add(time.Duration(n)*time.Minute), nil, nil, nil, lasted(float64(n*100)))))
	}
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, timed("one", s.base.Add(time.Hour), lasted(40), nil, lasted(300), nil)))
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, timed("two", s.base.Add(time.Hour), lasted(40), nil, nil, nil)))
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, timed("stranger", s.base, lasted(60_000), lasted(60_000), lasted(60_000), lasted(60_000))))
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx,
		timed("one", s.base.Add(-time.Minute), lasted(60_000), lasted(60_000), lasted(60_000), lasted(60_000))))

	legs, err := s.store.TurnBuckets.Latency(s.ctx, []string{"one", "two"}, since)
	s.Require().NoError(err)

	s.Require().Equal([]string{"stt_latency", "llm_ttft", "tts_ttfb", "roundtrip"},
		[]string{legs[0].Leg, legs[1].Leg, legs[2].Leg, legs[3].Leg})
	s.Equal(int64(2), legs[0].Turns)
	s.Equal(LegLatency{Leg: "llm_ttft"}, legs[1])
	s.Equal(int64(1), legs[2].Turns)
	s.Equal(int64(10), legs[3].Turns)
	// Bins are 5% wide, so each percentile is within 5% of the turn it falls on.
	within := func(want float64, got *float64, name string) {
		s.Require().NotNil(got, name)
		s.InEpsilon(want, *got, 0.05, name)
	}
	within(40, legs[0].P50Ms, "stt p50")
	within(40, legs[0].P99Ms, "stt p99")
	within(300, legs[2].P95Ms, "tts p95")
	within(500, legs[3].P50Ms, "roundtrip p50")
	within(1000, legs[3].P95Ms, "roundtrip p95")
	within(1000, legs[3].P99Ms, "roundtrip p99")
}

// Defect: a percentile picked from the 95th or 50th where the 99th was asked, which turns
// enough apart tell from each other.
func (s *StoreSuite) TestTurnLatencyTellsEachPercentileApart() {
	// Ninety turns of 100 ms, seven of 1000 and three of 5000: the 50th, 95th and 99th turns
	// each fall in another bin.
	for n := 1; n <= 100; n++ {
		took := 100.0
		if n > 97 {
			took = 5000
		} else if n > 90 {
			took = 1000
		}
		s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, timed("one", s.base, nil, nil, nil, lasted(took))))
	}

	legs, err := s.store.TurnBuckets.Latency(s.ctx, []string{"one"}, s.base)
	s.Require().NoError(err)

	roundtrip := legs[3]
	s.Require().NotNil(roundtrip.P50Ms)
	s.Require().NotNil(roundtrip.P95Ms)
	s.Require().NotNil(roundtrip.P99Ms)
	s.InEpsilon(100, *roundtrip.P50Ms, 0.05)
	s.InEpsilon(1000, *roundtrip.P95Ms, 0.05)
	s.InEpsilon(5000, *roundtrip.P99Ms, 0.05)
}

// Defect: the turns of another shard left out, or the bins of two shards read in the order
// the shards answered rather than by latency, which moves the percentiles.
func (s *ShardedStoreSuite) TestTurnLatencySumsEveryShard() {
	// This shard's slow turn is answered before the other's fast ones, so only a read that sorts
	// the bins of both puts it last.
	s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, timed("one", s.base, nil, nil, nil, lasted(1000))))
	s.Require().NoError(s.other.TurnBuckets.Add(s.ctx, timed("two", s.base, nil, nil, nil, lasted(100))))
	s.Require().NoError(s.other.TurnBuckets.Add(s.ctx, timed("two", s.base, nil, nil, nil, lasted(100))))

	legs, err := s.store.TurnBuckets.Latency(s.ctx, []string{"one", "two"}, s.base)
	s.Require().NoError(err)

	roundtrip := legs[3]
	s.Equal(int64(3), roundtrip.Turns)
	s.Require().NotNil(roundtrip.P50Ms)
	s.Require().NotNil(roundtrip.P95Ms)
	s.InEpsilon(100, *roundtrip.P50Ms, 0.05)
	s.InEpsilon(1000, *roundtrip.P95Ms, 0.05)
}

func (s *StoreSuite) TestTurnBucketQueryBudgets() {
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.TurnBuckets.Add(s.ctx, s.turn(s.base, 500))) }))
	// The counts and the latency bins, from every shard.
	s.Equal(2*len(s.store.shards()), s.countQueries(func() {
		buckets, err := s.store.TurnBuckets.Select(s.ctx, "acme", TurnBucketFilter{AgentID: "agent-1", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
		s.Require().NoError(err)
		s.Len(buckets, 1)
	}))
	s.Equal(len(s.store.shards()), s.countQueries(func() {
		_, err := s.store.TurnBuckets.Latency(s.ctx, []string{"acme"}, s.base)
		s.Require().NoError(err)
	}))
}
