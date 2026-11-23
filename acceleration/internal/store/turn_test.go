//go:build integration

package store

import "time"

// turn is one exchange that started at, defaulting what a test does not care about.
func turn(turnID string, at time.Time, roundtripMs float64) Turn {
	return Turn{CustomerID: "acme", AgentID: "agent-1", TurnID: turnID, StartedAt: at, RoundtripMs: &roundtripMs}
}

func (s *StoreSuite) TestACallsTurnsAreReadBackInTheOrderTheyStarted() {
	now := time.Now()
	queuedMs, audibleMs := 880.0, 940.0
	first := turn("turn-1", now.Add(-2*time.Minute), 1400)
	first.FirstFrameQueuedMs, first.FirstAudibleFrameMs = &queuedMs, &audibleMs
	s.Require().NoError(s.store.Turns.Insert(s.ctx, turn("turn-2", now.Add(-time.Minute), 900)))
	s.Require().NoError(s.store.Turns.Insert(s.ctx, first))

	turns, err := s.store.Turns.Select(s.ctx, "acme", "agent-1", now.Add(-5*time.Minute), nil)

	s.Require().NoError(err)
	s.Require().Len(turns, 2)
	s.Equal("turn-1", turns[0].TurnID)
	s.InDelta(1400, *turns[0].RoundtripMs, 0.001)
	s.InDelta(880, *turns[0].FirstFrameQueuedMs, 0.001)
	s.InDelta(940, *turns[0].FirstAudibleFrameMs, 0.001)
	s.Nil(turns[1].FirstFrameQueuedMs, "an edge that does not report them leaves them out")
}

func (s *StoreSuite) TestACallIsItsAgentsTurnsInTheWindowItRan() {
	now := time.Now()
	s.Require().NoError(s.store.Turns.Insert(s.ctx, turn("before", now.Add(-10*time.Minute), 100)))
	s.Require().NoError(s.store.Turns.Insert(s.ctx, turn("during", now.Add(-5*time.Minute), 100)))
	s.Require().NoError(s.store.Turns.Insert(s.ctx, turn("after", now.Add(-time.Minute), 100)))
	other := turn("someone else's", now.Add(-5*time.Minute), 100)
	other.AgentID = "agent-2"
	s.Require().NoError(s.store.Turns.Insert(s.ctx, other))
	theirs := turn("another customer's", now.Add(-5*time.Minute), 100)
	theirs.CustomerID = "globex"
	s.Require().NoError(s.store.Turns.Insert(s.ctx, theirs))

	ended := now.Add(-2 * time.Minute)
	turns, err := s.store.Turns.Select(s.ctx, "acme", "agent-1", now.Add(-6*time.Minute), &ended)

	s.Require().NoError(err)
	s.Require().Len(turns, 1)
	s.Equal("during", turns[0].TurnID)
}

func (s *StoreSuite) TestTurnsAreKeptForAnHour() {
	s.Require().NoError(s.store.Turns.Insert(s.ctx, turn("old", time.Now().Add(-2*RecentFor), 100)))
	s.Require().NoError(s.store.Turns.Insert(s.ctx, turn("new", time.Now(), 100)))

	turns, err := s.store.Turns.Select(s.ctx, "acme", "agent-1", time.Now().Add(-3*RecentFor), nil)
	s.Require().NoError(err)
	s.Require().Len(turns, 1, "the turn older than an hour is let go when the next is written")
	s.Equal("new", turns[0].TurnID)

	redis := s.store.sharedRedis()
	ttl, err := redis.Do(s.ctx, redis.B().Pttl().Key(turnsKey("acme", "agent-1")).Build()).AsInt64()
	s.Require().NoError(err)
	s.InDelta(RecentFor.Milliseconds(), ttl, float64(time.Minute.Milliseconds()))
}

func (s *StoreSuite) TestATurnIsWrittenAndReadInOneRoundTripEach() {
	now := time.Now()
	s.Equal(1, s.countRedis(func() {
		s.Require().NoError(s.store.Turns.Insert(s.ctx, turn("turn-1", now, 100)))
	}), "adding, trimming and expiring are one pipeline")
	for i := range 30 {
		s.Require().NoError(s.store.Turns.Insert(s.ctx, turn("more", now.Add(-time.Duration(i)*time.Second), 100)))
	}

	var turns []Turn
	s.Equal(1, s.countRedis(func() {
		var err error
		turns, err = s.store.Turns.Select(s.ctx, "acme", "agent-1", now.Add(-time.Minute), nil)
		s.Require().NoError(err)
	}))
	s.Len(turns, 31)
}

func (s *StoreSuite) TestATurnNamingNoAgentIsRefused() {
	err := s.store.Turns.Insert(s.ctx, Turn{CustomerID: "acme", TurnID: "turn-1", StartedAt: time.Now()})

	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
}
