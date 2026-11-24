//go:build integration

package store

import "time"

// decision is one judgement made at, defaulting what a test does not care about.
func decision(kind, reason string, at time.Time) CallEvent {
	latency := 30.0
	return CallEvent{
		CustomerID: "acme", CallID: "call-1", AgentID: "agent-1", At: at,
		Kind: kind, Reason: reason, TurnID: "turn-1", Participant: "caller",
		Said: "book a table for four", LatencyMs: &latency,
	}
}

// floorCheck is a check on the floor about turn, asked at.
func floorCheck(callID, turn string, at time.Time) CallEvent {
	paused := 300.0
	return CallEvent{CustomerID: "acme", CallID: callID, AgentID: "agent-1", At: at, Kind: "floor_check", TurnID: turn,
		FloorCheck: &FloorCheck{At: at, Kind: FloorCheckPause, PausedMs: &paused,
			Options: []FloorOption{{Option: "respond", Probability: 0.91}, {Option: "wait", Probability: 0.09}},
			Ruling:  FloorRuling{Disposition: "respond", Floor: "continue", Tool: "get_weather"}, Acted: true, LatencyMs: 38}}
}

func (s *StoreSuite) TestWhatTheConversationDecidedIsReadBackInTheOrderItHappened() {
	// The trail only explains a call if it is in order: a wait followed by an answer is
	// an agent giving somebody time to finish, and the same two the other way round is
	// an agent talking over them.
	now := time.Now()
	s.Require().NoError(s.store.CallEvents.InsertMany(s.ctx, []CallEvent{
		decision("answer", "a complete thought addressed to the agent", now.Add(-time.Second)),
		decision("wait", "the caller has not finished the thought", now.Add(-2*time.Second)),
	}))

	read, err := s.store.CallEvents.Select(s.ctx, "acme", "call-1", now.Add(-time.Minute), nil, 0)

	s.Require().NoError(err)
	s.Require().Len(read, 2)
	s.Equal("wait", read[0].Kind)
	s.Equal("the caller has not finished the thought", read[0].Reason)
	s.Equal("answer", read[1].Kind)
	s.Equal("caller", read[1].Participant)
	s.Require().NotNil(read[1].LatencyMs)
	s.InDelta(30, *read[1].LatencyMs, 0.001)
}

func (s *StoreSuite) TestOneCallsReasoningIsNotAnothers() {
	mine := decision("answer", "mine", time.Now())
	theirs := decision("answer", "theirs", time.Now())
	theirs.CallID = "call-2"
	otherCustomer := decision("answer", "another customer's", time.Now())
	otherCustomer.CustomerID = "globex"
	s.Require().NoError(s.store.CallEvents.InsertMany(s.ctx, []CallEvent{mine, theirs, otherCustomer}))

	read, err := s.store.CallEvents.Select(s.ctx, "acme", "call-1", time.Now().Add(-time.Minute), nil, 0)

	s.Require().NoError(err)
	s.Require().Len(read, 1)
	s.Equal("mine", read[0].Reason)
}

func (s *StoreSuite) TestAnEarlierCallOnTheSameIdIsNotThisOnes() {
	// A call id is the room, not the conversation: dialling the same room again is a
	// second call, and the reasoning behind the first one does not explain it.
	started := time.Now().Add(-time.Minute)
	s.Require().NoError(s.store.CallEvents.InsertMany(s.ctx, []CallEvent{
		decision("answer", "the call before", started.Add(-10*time.Minute)),
		decision("answer", "this call", started.Add(time.Second)),
	}))

	read, err := s.store.CallEvents.Select(s.ctx, "acme", "call-1", started, nil, 0)

	s.Require().NoError(err)
	s.Require().Len(read, 1)
	s.Equal("this call", read[0].Reason)
}

func (s *StoreSuite) TestADecisionOlderThanAnHourIsGone() {
	s.Require().NoError(s.store.CallEvents.InsertMany(s.ctx, []CallEvent{
		decision("answer", "long ago", time.Now().Add(-2*RecentFor)),
		decision("answer", "just now", time.Now()),
	}))

	read, err := s.store.CallEvents.Select(s.ctx, "acme", "call-1", time.Now().Add(-3*RecentFor), nil, 0)

	s.Require().NoError(err)
	s.Require().Len(read, 1)
	s.Equal("just now", read[0].Reason)
}

func (s *StoreSuite) TestALimitKeepsTheEarliestDecisions() {
	now := time.Now()
	s.Require().NoError(s.store.CallEvents.InsertMany(s.ctx, []CallEvent{
		decision("wait", "first", now.Add(-3*time.Second)),
		decision("wait", "second", now.Add(-2*time.Second)),
		decision("answer", "third", now.Add(-time.Second)),
	}))

	read, err := s.store.CallEvents.Select(s.ctx, "acme", "call-1", now.Add(-time.Minute), nil, 2)

	s.Require().NoError(err)
	s.Require().Len(read, 2)
	s.Equal("second", read[1].Reason)
}

func (s *StoreSuite) TestRecordingNoDecisionsIsNotAnError() {
	s.Equal(0, s.countRedis(func() { s.Require().NoError(s.store.CallEvents.InsertMany(s.ctx, nil)) }))
}

// A call's floor checks are kept apart from its judgements, each with the turn it is about, so
// that neither crowds the other out, and read back oldest first.
func (s *StoreSuite) TestACallsFloorChecksAreKeptApartFromItsDecisions() {
	now := time.Now().UTC()
	s.Require().NoError(s.store.CallEvents.InsertMany(s.ctx, []CallEvent{
		floorCheck("call-1", "turn-1", now.Add(-2*time.Second)),
		decision("wait", "still talking", now.Add(-time.Second)),
		floorCheck("call-1", "", now.Add(-3*time.Second)),
		floorCheck("call-1", "before", now.Add(-2*time.Minute)),
		floorCheck("call-1", "after", now.Add(time.Minute)),
	}))

	decisions, err := s.store.CallEvents.Select(s.ctx, "acme", "call-1", now.Add(-time.Minute), nil, 0)
	s.Require().NoError(err)
	s.Require().Len(decisions, 1, "the judgements alone")
	s.Nil(decisions[0].FloorCheck)

	checks, err := s.store.CallEvents.SelectFloorChecks(s.ctx, "acme", "call-1", now.Add(-time.Minute), nil)
	s.Require().NoError(err)
	s.Require().Len(checks, 2, "none from before the start, nor from after now")
	s.Empty(checks[0].TurnID, "oldest first")
	s.Equal("turn-1", checks[1].TurnID)
	s.Equal([]FloorOption{{Option: "respond", Probability: 0.91}, {Option: "wait", Probability: 0.09}}, checks[1].FloorCheck.Options)
	s.Equal("get_weather", checks[1].FloorCheck.Ruling.Tool)
	s.InDelta(300, *checks[1].FloorCheck.PausedMs, 0.001)

	before := now.Add(-2500 * time.Millisecond)
	checks, err = s.store.CallEvents.SelectFloorChecks(s.ctx, "acme", "call-1", now.Add(-time.Minute), &before)
	s.Require().NoError(err)
	s.Require().Len(checks, 1, "those from the start up to the call's end")
	s.Empty(checks[0].TurnID)
}

func (s *StoreSuite) TestABatchOfManyCallsIsWrittenInOneRoundTrip() {
	// The recorder batches judgements of every call an agent process holds, and a round trip
	// per call or per key would leave it behind on a busy one.
	now := time.Now()
	var batch []CallEvent
	for _, call := range []string{"call-1", "call-2", "call-3"} {
		for i := range 40 {
			decided := decision("wait", "still talking", now.Add(-time.Duration(i)*time.Second))
			decided.CallID = call
			batch = append(batch, decided, floorCheck(call, "turn-1", now.Add(-time.Duration(i)*time.Second)))
		}
	}

	s.Equal(1, s.countRedis(func() { s.Require().NoError(s.store.CallEvents.InsertMany(s.ctx, batch)) }))

	var read, checks []CallEvent
	s.Equal(1, s.countRedis(func() {
		var err error
		read, err = s.store.CallEvents.Select(s.ctx, "acme", "call-2", now.Add(-time.Minute), nil, 0)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countRedis(func() {
		var err error
		checks, err = s.store.CallEvents.SelectFloorChecks(s.ctx, "acme", "call-2", now.Add(-time.Minute), nil)
		s.Require().NoError(err)
	}))
	s.Len(read, 40)
	s.Len(checks, 40)
}

func (s *StoreSuite) TestAJudgementNamingNoCallIsRefused() {
	err := s.store.CallEvents.InsertMany(s.ctx, []CallEvent{{CustomerID: "acme", Kind: "wait", At: time.Now()}})

	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
}
