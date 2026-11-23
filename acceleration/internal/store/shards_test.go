//go:build integration

package store

import (
	"encoding/json"
	"time"
)

func (s *ShardedStoreSuite) TestACallAnotherShardRanIsListedAndFound() {
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{ID: "here", CustomerID: "app", StartedAt: s.base}))
	s.Require().NoError(s.other.Calls.Start(s.ctx, &Call{ID: "there", CustomerID: "app", StartedAt: s.base.Add(time.Minute)}))

	calls, err := s.store.Calls.Select(s.ctx, "app", CallFilter{})
	s.Require().NoError(err)
	s.Require().Len(calls, 2)
	s.Equal("there", calls[0].ID, "newest first, whichever shard ran it")
	s.Equal("here", calls[1].ID)

	found, err := s.store.Calls.Get(s.ctx, "app", "there")
	s.Require().NoError(err)
	s.Equal("there", found.ID)
}

func (s *ShardedStoreSuite) TestSpendIsCountedInEveryShard() {
	s.Require().NoError(s.store.Requests.Insert(s.ctx, &Request{Modality: "llm", CustomerID: "app", Provider: "openai", Model: "gpt", StartedAt: s.base, CostMicros: 300}))
	s.Require().NoError(s.other.Requests.Insert(s.ctx, &Request{Modality: "llm", CustomerID: "app", Provider: "openai", Model: "gpt", StartedAt: s.base, CostMicros: 200}))

	spent, err := s.store.Spend.Since(s.ctx, "app", s.base.Add(-time.Hour))
	s.Require().NoError(err)
	s.EqualValues(500, spent)
}

func (s *ShardedStoreSuite) TestARequestsLogIsInTheSharedDatabase() {
	s.sessionCall(s.other, Call{ID: "call", SessionID: "session", CustomerID: "app", AgentID: "agent", StartedAt: s.base}, "cfg")
	s.Require().NoError(s.other.Requests.Insert(s.ctx, &Request{Modality: "tts", CustomerID: "app", AgentID: "agent", Provider: "cartesia", Model: "sonic", StartedAt: s.base.Add(time.Second), Success: true}))

	logs, err := s.store.AgentLogs.Select(s.ctx, "app", AgentLogFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(logs, 1, "one row, in the shared database, rather than one in the shard as well")
	s.Equal("tts request completed", logs[0].Message)
	s.Equal("cfg", logs[0].ConfigID)
	s.Equal("session", logs[0].SessionID, "the session of the call, not the call")

	var inShard int
	s.Require().NoError(s.other.shard.NewSelect().Table("agent_logs").ColumnExpr("count(*)").Scan(s.ctx, &inShard))
	s.Zero(inShard)
}

// requestLogged is the log row a request the agent made writes, in a shard. Each test names its
// own customer, since nothing clears the log between tests.
func (s *ShardedStoreSuite) requestLogged(customer string, success bool, errorCode string) AgentLog {
	s.sessionCall(s.other, Call{ID: "call", SessionID: "session-" + customer, CustomerID: customer, AgentID: "agent", StartedAt: s.base}, "cfg")
	s.Require().NoError(s.other.Requests.Insert(s.ctx, &Request{Modality: "llm", CustomerID: customer, AgentID: "agent", Provider: "openai", Model: "gpt",
		StartedAt: s.base.Add(time.Second), InputTokens: 10, OutputTokens: 4, Success: success, ErrorCode: errorCode}))
	logs, err := s.store.AgentLogs.Select(s.ctx, customer, AgentLogFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(logs, 1)
	logged, err := s.store.AgentLogs.Get(s.ctx, customer, logs[0].ID)
	s.Require().NoError(err)
	return logged
}

func (s *ShardedStoreSuite) TestAFailedRequestIsLoggedAsAnError() {
	logged := s.requestLogged("failed-log", false, "provider_error")

	s.Equal("llm request failed", logged.Message)
	s.Equal("error", logged.Severity)
	s.Equal("provider_error", logged.Details["error_code"])
}

func (s *ShardedStoreSuite) TestARequestItsCallerCancelledIsLoggedAsNoError() {
	logged := s.requestLogged("cancelled-log", false, ErrorCancelled)

	s.Equal("llm request cancelled", logged.Message)
	s.Equal("info", logged.Severity, "a superseded flow decision or a barge-in is routine")
	s.Equal(ErrorCancelled, logged.Details["error_code"])
	s.EqualValues(4, logged.Details["output_tokens"], "what it generated is still there")
}

func (s *ShardedStoreSuite) TestASessionCarriedOnInAnotherShardIsListedOnce() {
	s.opened("chat", "app", s.base, func(session *AgentSession) { session.Title = "before" })
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base.Add(time.Minute), ""))
	carried := &AgentSession{ID: "chat", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", Title: "after", CreatedAt: s.base}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, carried))

	sessions, err := s.store.Sessions.Select(s.ctx, "app", SessionFilter{})
	s.Require().NoError(err)
	s.Require().Len(sessions, 1)
	s.Equal("after", sessions[0].Title, "the row it was carried on in is newer")
	s.Equal(SessionRunning, sessions[0].State)
}

func (s *ShardedStoreSuite) TestSessionsOpenInTwoShardsAtOnceAreOnePeak() {
	s.opened("here", "app", s.base, nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "here", s.base.Add(10*time.Minute), ""))
	there := &AgentSession{ID: "there", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", CreatedAt: s.base.Add(5 * time.Minute)}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, there))
	s.Require().NoError(s.other.Sessions.Close(s.ctx, "app", "there", s.base.Add(20*time.Minute), ""))

	peak, err := s.store.CustomerConcurrencyPeak(s.ctx, "app", s.base.Add(-time.Hour), s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.EqualValues(2, peak.Sessions)
	s.Require().NotNil(peak.At)
	s.True(peak.At.Equal(s.base.Add(5 * time.Minute)))
}

func (s *ShardedStoreSuite) TestASessionOpenInTwoShardsAtOnceIsOneInThePeak() {
	s.opened("chat", "app", s.base, nil)
	carried := &AgentSession{ID: "chat", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", CreatedAt: s.base}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, carried))
	s.Require().NoError(s.other.Sessions.Close(s.ctx, "app", "chat", s.base.Add(20*time.Minute), ""))
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base.Add(30*time.Minute), ""))

	peak, err := s.store.CustomerConcurrencyPeak(s.ctx, "app", s.base.Add(-time.Hour), s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.EqualValues(1, peak.Sessions)
}

func (s *ShardedStoreSuite) TestASessionCarriedOnInAnotherShardIsOneSessionOfItsTurnsAndTools() {
	session := s.opened("chat", "app", s.base, nil)
	s.turned(session, "chat-1", s.base, nil)
	s.turned(session, "chat-2", s.base.Add(time.Minute), answeredAfter("chat-1"))
	s.calledTool(session, "chat-2", "lookup_order", s.base.Add(time.Minute), map[string]any{"call_id": "c1"}, map[string]any{"call_id": "c1"})
	carried := &AgentSession{ID: "chat", CustomerID: "app", AgentName: "docs", ConfigID: "cfg-docs", UserID: "jlahey", CallerKind: "user", CreatedAt: s.base}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, carried))
	s.Require().NoError(s.other.Responses.Insert(s.ctx, &AgentResponse{
		ID: "chat-3", SessionID: "chat", CustomerID: "app", ParentResponseID: "chat-2", CreatedAt: s.base.Add(2 * time.Minute),
	}))
	at := s.base.Add(2 * time.Minute)
	s.Require().NoError(s.other.ResponseItems.InsertMany(s.ctx, []AgentResponseItem{
		{ResponseID: "chat-3", Ordinal: 0, SessionID: "chat", Kind: ItemToolCall, ToolName: "lookup_order",
			Payload: map[string]any{"call_id": "c2"}, At: at},
		{ResponseID: "chat-3", Ordinal: 1, SessionID: "chat", Kind: ItemToolResult, ToolName: "lookup_order",
			Text: "shipped", Payload: map[string]any{"call_id": "c2"}, At: at.Add(time.Second)},
		{ResponseID: "chat-3", Ordinal: 2, SessionID: "chat", Kind: ItemToolCall, ToolName: "refund",
			Payload: map[string]any{"call_id": "c3"}, At: at.Add(2 * time.Second)},
		{ResponseID: "chat-3", Ordinal: 3, SessionID: "chat", Kind: ItemToolResult, ToolName: "refund",
			Text: "refunded", Payload: map[string]any{"call_id": "c3"}, At: at.Add(3 * time.Second)},
	}))

	turns, err := s.store.CustomerTurns(s.ctx, "app", "", s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.Equal(SessionTurns{Sessions: 1, Turns: 3}, turns)

	overall, tools, err := s.store.CustomerToolUsage(s.ctx, "app", "", s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 2)
	s.EqualValues(1, overall.Sessions)
	s.Nil(overall.AvgTurnsBeforeUse)

	s.Equal("lookup_order", tools[0].Tool)
	s.EqualValues(2, tools[0].Calls)
	s.EqualValues(1, tools[0].Sessions)
	s.Require().NotNil(tools[0].AvgTurnsBeforeUse)
	s.InDelta(1.0, *tools[0].AvgTurnsBeforeUse, 0.001, "first used on the second turn, in this shard")

	s.Equal("refund", tools[1].Tool)
	s.Require().NotNil(tools[1].AvgTurnsBeforeUse)
	s.InDelta(2.0, *tools[1].AvgTurnsBeforeUse, 0.001, "first used on the third turn, two of them in the other shard")
}

func (s *ShardedStoreSuite) TestASessionCarriedOnInAnotherShardIsOneSessionOfItsActivity() {
	session := s.opened("chat", "app", s.base, nil)
	s.turned(session, "chat-1", s.base, nil)
	s.carriedOn("chat", s.base.Add(time.Minute))
	s.turnedThere("chat", "chat-2", "chat-1", s.base.Add(time.Minute), nil)
	s.opened("another", "app", s.base.Add(time.Minute), nil)

	buckets, err := s.store.CustomerActivity(s.ctx, "app", ActivityDaily, s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(2, buckets[0].Sessions)
	s.EqualValues(2, buckets[0].Messages)
	s.EqualValues(1, buckets[0].ActiveUsers)
}

func (s *ShardedStoreSuite) TestASessionCarriedOnIsCountedOnTheDayItBeganWhateverTheWindow() {
	dayOne, dayTwo := s.base, s.base.Add(24*time.Hour)
	session := s.opened("chat", "app", dayOne, nil)
	s.turned(session, "chat-1", dayOne, nil)
	s.carriedOn("chat", dayTwo)
	s.turnedThere("chat", "chat-2", "chat-1", dayTwo, nil)

	second, err := s.store.CustomerActivity(s.ctx, "app", ActivityDaily, dayTwo, dayTwo.Add(24*time.Hour))
	s.Require().NoError(err)
	var sessions int64
	for _, bucket := range second {
		sessions += bucket.Sessions
	}
	s.Zero(sessions, "it began the day before")

	first, err := s.store.CustomerActivity(s.ctx, "app", ActivityDaily, dayOne, dayTwo)
	s.Require().NoError(err)
	s.Require().Len(first, 1)
	s.EqualValues(1, first[0].Sessions)
}

func (s *ShardedStoreSuite) TestADayHoldingOnlyACarriedOnRowIsNotListed() {
	dayOne, dayTwo := s.base, s.base.Add(24*time.Hour)
	session := s.opened("chat", "app", dayOne, nil)
	s.turned(session, "chat-1", dayOne, nil)
	s.carriedOn("chat", dayTwo)
	s.turnedThere("chat", "chat-2", "chat-1", dayTwo.Add(24*time.Hour), nil)

	buckets, err := s.store.CustomerActivity(s.ctx, "app", ActivityDaily, dayTwo, dayTwo.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Empty(buckets, "nothing that counts happened that day")
}

func (s *ShardedStoreSuite) TestATurnRewoundInTheOtherShardIsNotCountedBeforeAToolsFirstUse() {
	session := s.opened("chat", "app", s.base, nil)
	s.turned(session, "chat-1", s.base, nil)
	s.turned(session, "chat-2", s.base.Add(time.Minute), answeredAfter("chat-1"))
	s.carriedOn("chat", s.base.Add(2*time.Minute))
	rewound := s.base.Add(3 * time.Minute)
	s.turnedThere("chat", "chat-gone", "chat-2", s.base.Add(2*time.Minute), &rewound)
	s.turnedThere("chat", "chat-3", "chat-2", s.base.Add(3*time.Minute), nil)
	s.calledToolThere("chat", "chat-3", "refund", s.base.Add(3*time.Minute))

	_, tools, err := s.store.CustomerToolUsage(s.ctx, "app", "", s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 1)
	s.Require().NotNil(tools[0].AvgTurnsBeforeUse)
	s.InDelta(2.0, *tools[0].AvgTurnsBeforeUse, 0.001)
}

func (s *ShardedStoreSuite) TestTurnsAtTheSameInstantInTwoShardsAreOrderedById() {
	// w, x, y and z share an instant. The first use is y, in the other shard, after x here and
	// w there: three turns before it. Ordered by anything but the id, it would be z, here.
	at := s.base.Add(time.Minute)
	session := s.opened("chat", "app", s.base, nil)
	s.turned(session, "chat-1", s.base, nil)
	s.turned(session, "x", at, answeredAfter("chat-1"))
	s.turned(session, "z", at, answeredAfter("x"))
	s.calledTool(session, "z", "refund", at, map[string]any{"call_id": "c1"}, map[string]any{"call_id": "c1"})
	s.carriedOn("chat", at)
	s.turnedThere("chat", "w", "chat-1", at, nil)
	s.turnedThere("chat", "y", "w", at, nil)
	s.calledToolThere("chat", "y", "refund", at)

	_, tools, err := s.store.CustomerToolUsage(s.ctx, "app", "", s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 1)
	s.Require().NotNil(tools[0].AvgTurnsBeforeUse)
	s.InDelta(3.0, *tools[0].AvgTurnsBeforeUse, 0.001)
}

// carriedOn writes the row a session carried on in the other shard has there.
func (s *ShardedStoreSuite) carriedOn(id string, at time.Time) {
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, &AgentSession{ID: id, CustomerID: "app", AgentName: "docs",
		ConfigID: "cfg-docs", UserID: "jlahey", CallerKind: "user", CreatedAt: at}))
}

// turnedThere records a turn of a session in the other shard, answered after parent.
func (s *ShardedStoreSuite) turnedThere(sessionID, id, parent string, at time.Time, rewound *time.Time) {
	s.Require().NoError(s.other.Responses.Insert(s.ctx, &AgentResponse{
		ID: id, SessionID: sessionID, CustomerID: "app", ParentResponseID: parent, CreatedAt: at, RewoundAt: rewound,
	}))
}

// calledToolThere records one call of tool and its result in a turn in the other shard.
func (s *ShardedStoreSuite) calledToolThere(sessionID, responseID, tool string, at time.Time) {
	s.Require().NoError(s.other.ResponseItems.InsertMany(s.ctx, []AgentResponseItem{
		{ResponseID: responseID, Ordinal: 0, SessionID: sessionID, Kind: ItemToolCall, ToolName: tool,
			Payload: map[string]any{"call_id": responseID}, At: at},
		{ResponseID: responseID, Ordinal: 1, SessionID: sessionID, Kind: ItemToolResult, ToolName: tool,
			Text: "done", Payload: map[string]any{"call_id": responseID}, At: at.Add(time.Second)},
	}))
}

func (s *ShardedStoreSuite) TestASessionAwayFromOneShardWhileAnotherRanIsNotOpenThroughTheGap() {
	s.opened("chat", "app", s.base, nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base.Add(10*time.Minute), ""))
	carried := &AgentSession{ID: "chat", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", CreatedAt: s.base.Add(20 * time.Minute)}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, carried))
	s.Require().NoError(s.other.Sessions.Close(s.ctx, "app", "chat", s.base.Add(30*time.Minute), ""))
	s.opened("between", "app", s.base.Add(12*time.Minute), nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "between", s.base.Add(18*time.Minute), ""))

	peak, err := s.store.CustomerConcurrencyPeak(s.ctx, "app", s.base.Add(-time.Hour), s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.EqualValues(1, peak.Sessions, "the first session was not open while the other ran")
}

func (s *ShardedStoreSuite) TestAGuestsSessionsInEveryShardAreClaimed() {
	s.Require().NoError(s.store.RecordGuest(s.ctx, &User{ID: "guest-1", CustomerID: "app", CreatedAt: s.base}))
	s.opened("here", "app", s.base, func(session *AgentSession) { session.UserID = "guest-1" })
	there := &AgentSession{ID: "there", CustomerID: "app", AgentName: "docs", UserID: "guest-1", CallerKind: "guest", CreatedAt: s.base}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, there))

	moved, err := s.store.ClaimGuest(s.ctx, "app", "guest-1", "jlahey")
	s.Require().NoError(err)
	s.EqualValues(2, moved)

	again, err := s.store.ClaimGuest(s.ctx, "app", "guest-1", "jlahey")
	s.Require().NoError(err, "the same user claiming again finishes a claim rather than failing")
	s.Zero(again)
	_, err = s.store.ClaimGuest(s.ctx, "app", "guest-1", "randy")
	s.Error(err)
}

func (s *ShardedStoreSuite) TestAnExportOfTwoShardsFoldsTheirRollups() {
	latency := 100.0
	s.Require().NoError(s.store.Requests.Insert(s.ctx, &Request{Modality: "stt", CustomerID: "app", Provider: "deepgram", Model: "flux", StartedAt: s.base, AudioMs: 1000, LatencyMs: &latency, Success: true}))
	slower := 300.0
	s.Require().NoError(s.other.Requests.Insert(s.ctx, &Request{Modality: "stt", CustomerID: "app", Provider: "deepgram", Model: "flux", StartedAt: s.base, AudioMs: 500, LatencyMs: &slower, Success: true}))
	_, err := s.store.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)
	_, err = s.other.Buckets.Rollup(s.ctx, Hourly, s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)

	var hours []map[string]any
	requests := 0
	_, err = s.store.ExportCustomer(s.ctx, "app", func(table string, row json.RawMessage) error {
		switch table {
		case "stats_hourly":
			var hour map[string]any
			s.Require().NoError(json.Unmarshal(row, &hour))
			hours = append(hours, hour)
		case "requests":
			requests++
		}
		return nil
	})
	s.Require().NoError(err)
	s.Equal(2, requests, "every shard's requests")
	s.Require().Len(hours, 1, "one row for the hour both shards counted")
	s.EqualValues(1500, hours[0]["audio_ms_total"])
	s.EqualValues(2, hours[0]["request_count"])
	s.EqualValues(200, hours[0]["latency_p50_ms"])
}
