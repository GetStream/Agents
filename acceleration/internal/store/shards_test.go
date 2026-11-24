//go:build integration

package store

import (
	"encoding/json"
	"fmt"
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
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base, s.base.Add(time.Minute), ""))
	carried := &AgentSession{ID: "chat", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", Title: "after", CreatedAt: s.base}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, carried))

	sessions, err := s.store.Sessions.Select(s.ctx, "app", SessionFilter{})
	s.Require().NoError(err)
	s.Require().Len(sessions, 1)
	s.Equal("after", sessions[0].Title, "the row it was carried on in is newer")
	s.Equal(SessionRunning, sessions[0].State)
}

// carriedOn writes what a router of the other shard writes of session id of "app" when it
// carries the session on there, having opened it at opened.
func (s *ShardedStoreSuite) carriedOn(id string, opened time.Time) {
	carried := &AgentSession{ID: id, CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user",
		CreatedAt: s.base, OpenedAt: opened, Remembers: new(false)}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, carried))
	s.Require().NoError(s.other.Sessions.Supersede(s.ctx, "app", id, opened))
}

// answered records a turn of session id of "app" ending at, in into's shard.
func (s *ShardedStoreSuite) answered(into *Store, id string, at time.Time) {
	response := id + "-" + at.Format(time.RFC3339)
	s.Require().NoError(into.Responses.Insert(s.ctx, &AgentResponse{ID: response, SessionID: id, CustomerID: "app", CreatedAt: at}))
	s.Require().NoError(into.Responses.Finish(s.ctx, "app", response, ResponseCompleted, "", at))
}

func (s *ShardedStoreSuite) TestASessionCarriedOnInAnotherShardIsListedOnceOnEveryPageWithItsPin() {
	s.opened("chat", "app", s.base, nil)
	_, err := s.store.Sessions.Pin(s.ctx, "app", "chat", true, s.base)
	s.Require().NoError(err)
	s.answered(s.store, "chat", s.base.Add(10*time.Minute))
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base, s.base.Add(11*time.Minute), ""))
	// Answered between chat's last answer here and its first there, so on pages of one session
	// each of chat's two rows falls on a page of its own.
	s.opened("other", "app", s.base.Add(15*time.Minute), nil)
	s.answered(s.store, "other", s.base.Add(20*time.Minute))
	s.carriedOn("chat", s.base.Add(25*time.Minute))
	s.answered(s.other, "chat", s.base.Add(30*time.Minute))

	listed := s.pagesOfOne()
	s.Equal([]string{"chat", "other"}, sessionIDs(listed), "once, by the row it runs in")
	s.NotNil(listed[0].PinnedAt, "pinned before it was carried on")
	s.Equal(SessionRunning, listed[0].State)
}

func (s *ShardedStoreSuite) TestALateReopenWrittenInTheShardASessionLeftKeepsItListedOnce() {
	s.opened("chat", "app", s.base, nil)
	s.answered(s.store, "chat", s.base.Add(10*time.Minute))
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base, s.base.Add(11*time.Minute), ""))
	s.opened("other", "app", s.base.Add(15*time.Minute), nil)
	s.answered(s.store, "other", s.base.Add(20*time.Minute))
	s.carriedOn("chat", s.base.Add(25*time.Minute))
	s.answered(s.other, "chat", s.base.Add(30*time.Minute))

	// A router of this shard reopened it before it was carried on there, and the row of that
	// reopen lands only now. Its supersede comes next, and may fail.
	late := &AgentSession{ID: "chat", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user",
		CreatedAt: s.base, OpenedAt: s.base.Add(12 * time.Minute)}
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, late))

	s.Equal([]string{"chat", "other"}, sessionIDs(s.pagesOfOne()), "listed by the row it runs in alone")
}

// pagesOfOne lists the sessions of "app" one to a page, following each page's cursor.
func (s *ShardedStoreSuite) pagesOfOne() []AgentSession {
	var listed []AgentSession
	filter := SessionFilter{Limit: 1}
	for range 5 {
		page, err := s.store.Sessions.Select(s.ctx, "app", filter)
		s.Require().NoError(err)
		s.Require().NotEmpty(page)
		listed = append(listed, page[0])
		if len(page) == 1 {
			break
		}
		filter.Cursor = after(page[0])
	}
	return listed
}

func (s *ShardedStoreSuite) TestASessionCarriedOnInAnotherShardIsFoundAsItIsThereFromTheShardItLeft() {
	s.opened("chat", "app", s.base, nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base, s.base.Add(time.Minute), "left"))
	s.carriedOn("chat", s.base.Add(time.Hour))

	found, err := s.store.Sessions.Get(s.ctx, "app", "chat")
	s.Require().NoError(err)
	s.Equal(SessionRunning, found.State, "the row it runs in, not the one it left closed here")
	s.Nil(found.ClosedAt)
	s.Empty(found.EndReason)
}

func (s *ShardedStoreSuite) TestASessionCarriedOnInAnotherShardKeepsWhatItsRowHereSaid() {
	s.opened("chat", "app", s.base, func(session *AgentSession) {
		session.Modality = ModalityVoice
		session.Remembers = new(true)
	})
	s.answered(s.store, "chat", s.base.Add(10*time.Minute))
	s.changed("chat", "review_score = ?", 4)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base, s.base.Add(11*time.Minute), ""))
	s.carriedOn("chat", s.base.Add(time.Hour))

	found, err := s.other.Sessions.Get(s.ctx, "app", "chat")
	s.Require().NoError(err)
	s.Equal(SessionRunning, found.State)
	s.Require().NotNil(found.LastResponseAt, "it sorts by its last answer, not by when it began")
	s.True(found.LastResponseAt.Equal(s.base.Add(10 * time.Minute)))
	s.Require().NotNil(found.ReviewScore)
	s.Equal(4, *found.ReviewScore)
	s.Equal(ModalityVoice, found.Modality, "a session spoken in stays a voice one")
	s.Require().NotNil(found.Remembers)
	s.True(*found.Remembers, "deleting it still forgets what it taught the memory store")
}

func (s *ShardedStoreSuite) TestASupersedeTriedAgainAfterAShardFailedTakesWhatTheEarlierRowSaid() {
	s.opened("chat", "app", s.base, func(session *AgentSession) { session.Remembers = new(true) })
	_, err := s.store.Sessions.Pin(s.ctx, "app", "chat", true, s.base)
	s.Require().NoError(err)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base, s.base.Add(time.Minute), ""))
	opened := s.base.Add(time.Hour)
	carried := &AgentSession{ID: "chat", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user",
		CreatedAt: s.base, OpenedAt: opened, Remembers: new(false)}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, carried))

	// The first try also reads a shard that is down, and fails once the shard holding the
	// earlier row has answered. The recorder tries again.
	s.Require().Error(s.withAShardDown(s.other.shardDSN, s.shardDSN).Sessions.Supersede(s.ctx, "app", "chat", opened))
	s.Require().NoError(s.other.Sessions.Supersede(s.ctx, "app", "chat", opened))

	found, err := s.other.Sessions.Get(s.ctx, "app", "chat")
	s.Require().NoError(err)
	s.NotNil(found.PinnedAt, "pinned before it was carried on")
	s.Require().NotNil(found.Remembers)
	s.True(*found.Remembers, "deleting it still forgets what its first run taught the memory store")
}

func (s *ShardedStoreSuite) TestASessionHereIsFoundWhileAnotherShardIsDown() {
	s.opened("chat", "app", s.base, nil)

	found, err := s.withAShardDown(s.shardDSN, s.other.shardDSN).Sessions.Get(s.ctx, "app", "chat")
	s.Require().NoError(err, "a shard that does not hold the session failed its read")
	s.Equal("chat", found.ID)
}

func (s *ShardedStoreSuite) TestASessionCarriedOnInAShardThatIsDownIsFoundAsItLeftHere() {
	s.opened("chat", "app", s.base, nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base, s.base.Add(time.Minute), "left"))
	s.carriedOn("chat", s.base.Add(time.Hour))

	found, err := s.withAShardDown(s.shardDSN).Sessions.Get(s.ctx, "app", "chat")
	s.Require().NoError(err, "a session of the caller's own read as nobody's")
	s.Equal(SessionClosed, found.State, "as it was when it left here")
}

// withAShardDown is a router of the shard own that reads the shards up and one more, which is
// down: nothing listens on port 1.
func (s *ShardedStoreSuite) withAShardDown(own string, up ...string) *Store {
	shards := map[string]string{"own": own, "down": "postgres://postgres:postgres@127.0.0.1:1/down?sslmode=disable"}
	for i, dsn := range up {
		shards[fmt.Sprintf("up-%d", i)] = dsn
	}
	router, err := OpenShards(Options{DSN: s.dsn, ShardDSN: own, Shards: shards})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(router.Close()) })
	return router
}

func (s *ShardedStoreSuite) TestALateRowFromARouterThatHeldASessionEarlierLeavesItListedOnceByTheRunOpenedSince() {
	s.opened("chat", "app", s.base, func(session *AgentSession) { session.OpenedAt = s.base.Add(2 * time.Hour) })
	s.answered(s.store, "chat", s.base.Add(3*time.Hour))
	s.opened("other", "app", s.base.Add(time.Minute), nil)
	// The other shard's write of a run before the one here, landing after it.
	s.carriedOn("chat", s.base.Add(time.Hour))

	listed := s.pagesOfOne()
	s.Equal([]string{"chat", "other"}, sessionIDs(listed), "listed by the row of the run opened since alone")
	s.Equal(SessionRunning, listed[0].State)
}

func (s *ShardedStoreSuite) TestSessionsOpenInTwoShardsAtOnceAreOnePeak() {
	s.opened("here", "app", s.base, nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "here", s.base, s.base.Add(10*time.Minute), ""))
	there := &AgentSession{ID: "there", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", CreatedAt: s.base.Add(5 * time.Minute)}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, there))
	s.Require().NoError(s.other.Sessions.Close(s.ctx, "app", "there", s.base.Add(5*time.Minute), s.base.Add(20*time.Minute), ""))

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
	s.Require().NoError(s.other.Sessions.Close(s.ctx, "app", "chat", s.base, s.base.Add(20*time.Minute), ""))
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base, s.base.Add(30*time.Minute), ""))

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
	s.carriedOnAt("chat", s.base.Add(time.Minute))
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
	s.carriedOnAt("chat", dayTwo)
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
	s.carriedOnAt("chat", dayTwo)
	s.turnedThere("chat", "chat-2", "chat-1", dayTwo.Add(24*time.Hour), nil)

	buckets, err := s.store.CustomerActivity(s.ctx, "app", ActivityDaily, dayTwo, dayTwo.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Empty(buckets, "nothing that counts happened that day")
}

func (s *ShardedStoreSuite) TestATurnRewoundInTheOtherShardIsNotCountedBeforeAToolsFirstUse() {
	session := s.opened("chat", "app", s.base, nil)
	s.turned(session, "chat-1", s.base, nil)
	s.turned(session, "chat-2", s.base.Add(time.Minute), answeredAfter("chat-1"))
	s.carriedOnAt("chat", s.base.Add(2*time.Minute))
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
	s.carriedOnAt("chat", at)
	s.turnedThere("chat", "w", "chat-1", at, nil)
	s.turnedThere("chat", "y", "w", at, nil)
	s.calledToolThere("chat", "y", "refund", at)

	_, tools, err := s.store.CustomerToolUsage(s.ctx, "app", "", s.base, s.base.Add(time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 1)
	s.Require().NotNil(tools[0].AvgTurnsBeforeUse)
	s.InDelta(3.0, *tools[0].AvgTurnsBeforeUse, 0.001)
}

// Two customers may each have a session of one id in a shard: another customer's first turn
// there does not make the row a session was carried on in count as the one it began in.
func (s *ShardedStoreSuite) TestASessionCarriedOnBesideAnotherCustomersOfItsIdIsOneSessionOfItsActivity() {
	session := s.opened("chat", "app", s.base, nil)
	s.turned(session, "chat-1", s.base, nil)
	s.carriedOnAt("chat", s.base.Add(time.Minute))
	s.turnedThere("chat", "chat-2", "chat-1", s.base.Add(time.Minute), nil)
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, &AgentSession{ID: "chat", CustomerID: "somebody-else", AgentName: "docs",
		UserID: "sam", CallerKind: "user", CreatedAt: s.base}))
	s.Require().NoError(s.other.Responses.Insert(s.ctx, &AgentResponse{ID: "theirs-1", SessionID: "chat", CustomerID: "somebody-else",
		CreatedAt: s.base}))

	buckets, err := s.store.CustomerActivity(s.ctx, "app", ActivityDaily, s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1, buckets[0].Sessions)
}

// Another customer's session of the id carried on into a shard does not make a session that
// began there one carried on: its turns before a tool's first use are counted where they are,
// not asked of every shard again.
func (s *ShardedStoreSuite) TestAnotherCustomersSessionOfTheIdCarriedOnHereAsksNoShardAgainForAToolsFirstUse() {
	session := s.opened("chat", "app", s.base, nil)
	s.turned(session, "chat-1", s.base, nil)
	s.turned(session, "chat-2", s.base.Add(time.Minute), answeredAfter("chat-1"))
	s.calledTool(session, "chat-2", "refund", s.base.Add(time.Minute), map[string]any{"call_id": "c1"}, map[string]any{"call_id": "c1"})
	theirs := s.opened("chat", "somebody-else", s.base, nil)
	s.turned(theirs, "theirs-2", s.base.Add(time.Minute), answeredAfter("theirs-1"))

	var tools []ToolUsage
	queries := s.countQueries(func() {
		var err error
		_, tools, err = s.store.CustomerToolUsage(s.ctx, "app", "", s.base, s.base.Add(time.Hour))
		s.Require().NoError(err)
	})
	s.Require().Len(tools, 1)
	s.Require().NotNil(tools[0].AvgTurnsBeforeUse)
	s.InDelta(1.0, *tools[0].AvgTurnsBeforeUse, 0.001)
	s.Equal(2, queries, "one read of each shard")
}

// carriedOnAt writes the row a session carried on in the other shard has there.
func (s *ShardedStoreSuite) carriedOnAt(id string, at time.Time) {
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
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "chat", s.base, s.base.Add(10*time.Minute), ""))
	carried := &AgentSession{ID: "chat", CustomerID: "app", AgentName: "docs", UserID: "jlahey", CallerKind: "user", CreatedAt: s.base.Add(20 * time.Minute)}
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, carried))
	s.Require().NoError(s.other.Sessions.Close(s.ctx, "app", "chat", s.base.Add(20*time.Minute), s.base.Add(30*time.Minute), ""))
	s.opened("between", "app", s.base.Add(12*time.Minute), nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "app", "between", s.base.Add(12*time.Minute), s.base.Add(18*time.Minute), ""))

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
