//go:build integration

package store

import (
	"fmt"
	"time"
)

func (s *StoreSuite) TestActivityCountsWhatWasAskedOfTheAgents() {
	session := s.opened("one", "acme", s.base, nil)
	for i := range 3 {
		s.Require().NoError(s.store.Responses.Insert(s.ctx, &AgentResponse{
			ID:         fmt.Sprintf("r-%d", i),
			SessionID:  session.ID,
			CustomerID: "acme",
			Said:       "how much is it",
			CreatedAt:  s.base.Add(time.Duration(i) * time.Minute),
		}))
	}

	buckets, err := s.store.CustomerActivity(
		s.ctx, "acme", ActivityDaily, s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1, buckets[0].Sessions)
	s.EqualValues(3, buckets[0].Messages)
	s.EqualValues(1, buckets[0].ActiveUsers)
}

func (s *StoreSuite) TestAClaimedGuestIsTheSamePersonAsTheUserWhoClaimedThem() {
	s.Require().NoError(s.store.RecordGuest(s.ctx, &User{
		ID: "guest-1", CustomerID: "acme", Name: "Randy", CreatedAt: s.base,
	}))
	_, err := s.store.ClaimGuest(s.ctx, "acme", "guest-1", "randy")
	s.Require().NoError(err)

	s.opened("as-a-guest", "acme", s.base.Add(time.Minute), func(session *AgentSession) {
		session.UserID = "guest-1"
		session.CallerKind = "guest"
	})
	s.opened("signed-in", "acme", s.base.Add(2*time.Minute), func(session *AgentSession) {
		session.UserID = "randy"
	})

	buckets, err := s.store.CustomerActivity(
		s.ctx, "acme", ActivityDaily, s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(2, buckets[0].Sessions)
	s.EqualValues(1, buckets[0].ActiveUsers, "two sessions, one person")
}

func (s *StoreSuite) TestAnAnonymousNameIsNotCountedAsAPerson() {
	// An anonymous name is a claim nothing verified, so counting it would make guessing a
	// name enough to inflate this.
	s.opened("claimed", "acme", s.base.Add(time.Minute), func(session *AgentSession) {
		session.UserID = "whoever"
		session.CallerKind = "anonymous"
	})
	s.opened("nameless", "acme", s.base.Add(2*time.Minute), func(session *AgentSession) {
		session.UserID = ""
		session.CallerKind = "anonymous"
	})
	s.opened("verified", "acme", s.base.Add(3*time.Minute), func(session *AgentSession) {
		session.UserID = "randy"
		session.CallerKind = "authenticated"
	})

	buckets, err := s.store.CustomerActivity(
		s.ctx, "acme", ActivityDaily, s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(3, buckets[0].Sessions)
	s.EqualValues(1, buckets[0].ActiveUsers)
}

func (s *StoreSuite) TestAMonthOfUsersIsWhoCameBackRatherThanTheSumOfItsDays() {
	s.opened("monday", "acme", s.base, func(session *AgentSession) { session.UserID = "randy" })
	s.opened("tuesday", "acme", s.base.Add(24*time.Hour), func(session *AgentSession) {
		session.UserID = "randy"
	})
	s.opened("wednesday", "acme", s.base.Add(48*time.Hour), func(session *AgentSession) {
		session.UserID = "lahey"
	})

	from, to := s.base, s.base.Add(72*time.Hour)

	daily, err := s.store.CustomerActivity(s.ctx, "acme", ActivityDaily, from, to)
	s.Require().NoError(err)
	s.Require().Len(daily, 3)
	var summed int64
	for _, bucket := range daily {
		summed += bucket.ActiveUsers
	}
	s.EqualValues(3, summed)

	monthly, err := s.store.CustomerActivity(s.ctx, "acme", ActivityMonthly, from, to)
	s.Require().NoError(err)
	s.Require().Len(monthly, 1)
	s.EqualValues(2, monthly[0].ActiveUsers, "randy on two days is one person for the month")
}

func (s *StoreSuite) TestPhoneMinutesAreThePartOfTheTalkingThatArrivedOnANumber() {
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{
		ID: "over-the-web", CustomerID: "acme", CallID: "c-1", AgentID: "a-1", StartedAt: s.base,
	}))
	s.Require().NoError(s.store.Calls.Finish(s.ctx, "acme", "over-the-web", s.base.Add(5*time.Minute)))

	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{
		ID: "over-a-number", CustomerID: "acme", CallID: "c-2", AgentID: "a-2",
		FromNumber: "+15125551234", StartedAt: s.base.Add(time.Minute),
	}))
	s.Require().NoError(s.store.Calls.Finish(s.ctx, "acme", "over-a-number", s.base.Add(4*time.Minute)))

	buckets, err := s.store.CustomerActivity(
		s.ctx, "acme", ActivityDaily, s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(2, buckets[0].Calls)
	s.InDelta(8, buckets[0].VoiceMinutes, 0.001)
	s.InDelta(3, buckets[0].PhoneMinutes, 0.001, "only the call that arrived on a number")
}

func (s *StoreSuite) TestActivityIsOnlyTheCallingCustomers() {
	s.opened("mine", "acme", s.base, nil)
	s.opened("theirs", "globex", s.base, nil)

	buckets, err := s.store.CustomerActivity(
		s.ctx, "acme", ActivityDaily, s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1, buckets[0].Sessions)
}

// A turn names its session by id, and a report joined on the id alone took the person of
// whichever customer's session had it.
func (s *StoreSuite) TestActivityCountsOnlyThePeopleOfTheCustomersOwnSessions() {
	theirs := s.opened("one", "globex", s.base, nil)
	s.turned(&AgentSession{ID: theirs.ID, CustomerID: "acme"}, "acme-turn", s.base.Add(time.Minute), nil)

	buckets, err := s.store.CustomerActivity(s.ctx, "acme", ActivityDaily, s.base, s.base.Add(24*time.Hour))

	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1, buckets[0].Messages)
	s.Zero(buckets[0].ActiveUsers, "globex's person is not acme's")
}

// ranTool records one tool call and its result, the way a session writes them, on a
// response of its own.
func (s *StoreSuite) ranTool(session *AgentSession, tool string, at time.Time, took time.Duration, result string, failed bool) {
	responseID := fmt.Sprintf("%s-%s-%d", session.ID, tool, at.UnixNano())
	s.Require().NoError(s.store.Responses.Insert(s.ctx, &AgentResponse{
		ID: responseID, SessionID: session.ID, CustomerID: session.CustomerID, CreatedAt: at,
	}))
	payload := map[string]any{"call_id": "call-1", "arguments": `{"q":"x"}`}
	if failed {
		payload["error"] = "it broke"
	}
	s.Require().NoError(s.store.ResponseItems.InsertMany(s.ctx, []AgentResponseItem{
		{ResponseID: responseID, Ordinal: 0, SessionID: session.ID, Kind: ItemToolCall,
			ToolName: tool, Payload: map[string]any{"call_id": "call-1"}, At: at},
		{ResponseID: responseID, Ordinal: 1, SessionID: session.ID, Kind: ItemToolResult,
			ToolName: tool, Text: result, Payload: payload, At: at.Add(took)},
	}))
}

func (s *StoreSuite) TestThePeakIsTheMostSessionsOpenAtOnce() {
	s.opened("first", "acme", s.base, nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "acme", "first", s.base.Add(10*time.Minute), ""))
	s.opened("second", "acme", s.base.Add(2*time.Minute), nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "acme", "second", s.base.Add(5*time.Minute), ""))
	s.opened("third", "acme", s.base.Add(3*time.Minute), nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "acme", "third", s.base.Add(4*time.Minute), ""))
	s.opened("later", "acme", s.base.Add(20*time.Minute), nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "acme", "later", s.base.Add(21*time.Minute), ""))

	peak, err := s.store.CustomerConcurrencyPeak(s.ctx, "acme", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.EqualValues(3, peak.Sessions)
	s.Require().NotNil(peak.At)
	s.True(peak.At.Equal(s.base.Add(3 * time.Minute)))
}

func (s *StoreSuite) TestASessionThatClosedAsAnotherOpenedDidNotOverlapIt() {
	s.opened("before", "acme", s.base, nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "acme", "before", s.base.Add(time.Minute), ""))
	s.opened("after", "acme", s.base.Add(time.Minute), nil)
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "acme", "after", s.base.Add(2*time.Minute), ""))

	peak, err := s.store.CustomerConcurrencyPeak(s.ctx, "acme", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.EqualValues(1, peak.Sessions)
}

func (s *StoreSuite) TestASessionOpenedBeforeTheWindowStillCountsInIt() {
	s.opened("overnight", "acme", s.base.Add(-time.Hour), nil)
	s.opened("morning", "acme", s.base.Add(time.Minute), nil)

	peak, err := s.store.CustomerConcurrencyPeak(s.ctx, "acme", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.EqualValues(2, peak.Sessions, "both still running")
}

func (s *StoreSuite) TestThereIsNoPeakWhenNothingRan() {
	s.opened("theirs", "globex", s.base, nil)

	peak, err := s.store.CustomerConcurrencyPeak(s.ctx, "acme", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Zero(peak.Sessions)
	s.Nil(peak.At)
}

func (s *StoreSuite) TestToolUsageIsMostCalledFirstWithHowLongEachTook() {
	session := s.opened("one", "acme", s.base, nil)
	s.ranTool(session, "lookup_order", s.base.Add(time.Minute), 100*time.Millisecond, "shipped", false)
	s.ranTool(session, "lookup_order", s.base.Add(2*time.Minute), 300*time.Millisecond, "shipped", false)
	s.ranTool(session, "refund", s.base.Add(3*time.Minute), 2*time.Second, "", true)

	overall, tools, err := s.store.CustomerToolUsage(s.ctx, "acme", "", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 2)

	s.Equal("lookup_order", tools[0].Tool)
	s.EqualValues(2, tools[0].Calls)
	s.Zero(tools[0].Failed)
	s.Require().NotNil(tools[0].AvgMs)
	s.InDelta(200, *tools[0].AvgMs, 0.01)
	// lookup_order, {"q":"x"} and shipped are 12, 9 and 7 bytes: 28, at four a token.
	s.InDelta(7, tools[0].AvgTokens, 0.01)

	s.Equal("refund", tools[1].Tool)
	s.EqualValues(1, tools[1].Failed)

	s.Empty(overall.Tool)
	s.EqualValues(3, overall.Calls)
	s.EqualValues(1, overall.Failed)
	s.Require().NotNil(overall.P99Ms)
	s.Greater(*overall.P99Ms, 1900.0, "the slow call is the tail")
}

func (s *StoreSuite) TestToolUsageIsOnlyTheCallingCustomers() {
	theirs := s.opened("theirs", "globex", s.base, nil)
	s.ranTool(theirs, "lookup_order", s.base.Add(time.Minute), time.Second, "shipped", false)

	overall, tools, err := s.store.CustomerToolUsage(s.ctx, "acme", "", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Empty(tools)
	s.Zero(overall.Calls)
	s.Nil(overall.AvgMs)
}

// turned records a turn the session took at, before change.
func (s *StoreSuite) turned(session *AgentSession, id string, at time.Time, change func(*AgentResponse)) {
	response := &AgentResponse{ID: id, SessionID: session.ID, CustomerID: session.CustomerID, CreatedAt: at}
	if change != nil {
		change(response)
	}
	s.Require().NoError(s.store.Responses.Insert(s.ctx, response))
}

// calledTool records one call of tool in the turn responseID, its result a second later, with
// what the session put on each item.
func (s *StoreSuite) calledTool(session *AgentSession, responseID, tool string, at time.Time, call, result map[string]any) {
	s.Require().NoError(s.store.ResponseItems.InsertMany(s.ctx, []AgentResponseItem{
		{ResponseID: responseID, Ordinal: 0, SessionID: session.ID, Kind: ItemToolCall, ToolName: tool,
			Payload: call, At: at},
		{ResponseID: responseID, Ordinal: 1, SessionID: session.ID, Kind: ItemToolResult, ToolName: tool,
			Text: "shipped", Payload: result, At: at.Add(time.Second)},
	}))
}

func (s *StoreSuite) TestToolUsageOfOneAgentIsOutOfThatAgentsSessions() {
	for i := range 4 {
		session := s.opened(fmt.Sprintf("a%d", i), "acme", s.base, func(session *AgentSession) { session.ConfigID = "cfg-a" })
		s.turned(session, session.ID+"-1", s.base.Add(time.Minute), nil)
		if i == 0 {
			s.calledTool(session, session.ID+"-1", "lookup_order", s.base.Add(time.Minute),
				map[string]any{"call_id": "c1"}, map[string]any{"call_id": "c1"})
		}
	}
	other := s.opened("b0", "acme", s.base, func(session *AgentSession) { session.ConfigID = "cfg-b" })
	s.turned(other, "b0-1", s.base.Add(time.Minute), nil)
	s.calledTool(other, "b0-1", "refund", s.base.Add(time.Minute), map[string]any{"call_id": "c1"}, map[string]any{"call_id": "c1"})

	turns, err := s.store.CustomerTurns(s.ctx, "acme", "cfg-a", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Equal(SessionTurns{Sessions: 4, Turns: 4}, turns)
	overall, tools, err := s.store.CustomerToolUsage(s.ctx, "acme", "cfg-a", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 1, "the other agent's refund is not this one's")
	s.Equal("lookup_order", tools[0].Tool)
	s.EqualValues(1, tools[0].Sessions)
	s.EqualValues(1, overall.Sessions)

	_, tools, err = s.store.CustomerToolUsage(s.ctx, "acme", "cfg-b", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 1)
	s.Equal("refund", tools[0].Tool)

	everyone, err := s.store.CustomerTurns(s.ctx, "acme", "", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.EqualValues(5, everyone.Sessions)
}

// One agent's turns and tools are those of the sessions it ran, which were read by the turn's
// session id alone: another customer's session of that id put them under its agent.
func (s *StoreSuite) TestOneAgentsTurnsAndToolsAreReadOffTheCustomersOwnSessions() {
	theirs := s.opened("one", "globex", s.base, func(session *AgentSession) { session.ConfigID = "cfg-a" })
	mine := &AgentSession{ID: theirs.ID, CustomerID: "acme"}
	s.turned(mine, "acme-turn", s.base.Add(time.Minute), nil)
	s.calledTool(mine, "acme-turn", "refund", s.base.Add(time.Minute), map[string]any{"call_id": "c1"}, map[string]any{"call_id": "c1"})

	turns, err := s.store.CustomerTurns(s.ctx, "acme", "cfg-a", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	_, tools, err := s.store.CustomerToolUsage(s.ctx, "acme", "cfg-a", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)

	s.Zero(turns, "acme ran no session of cfg-a")
	s.Empty(tools)
}

func (s *StoreSuite) TestTurnsBeforeAToolIsUsedAreTheConversationsOwn() {
	first := s.opened("first", "acme", s.base, nil)
	s.turned(first, "first-1", s.base, nil)
	rewound := s.base.Add(2 * time.Minute)
	s.turned(first, "first-gone", s.base.Add(time.Minute), func(response *AgentResponse) {
		response.ParentResponseID, response.RewoundAt = "first-1", &rewound
	})
	s.turned(first, "first-2", s.base.Add(2*time.Minute), answeredAfter("first-1"))
	s.turned(first, "first-3", s.base.Add(3*time.Minute), answeredAfter("first-2"))
	s.calledTool(first, "first-3", "lookup_order", s.base.Add(3*time.Minute), map[string]any{"call_id": "c1"}, map[string]any{"call_id": "c1"})
	s.turned(first, "first-4", s.base.Add(4*time.Minute), answeredAfter("first-3"))
	s.calledTool(first, "first-4", "lookup_order", s.base.Add(4*time.Minute), map[string]any{"call_id": "c2"}, map[string]any{"call_id": "c2"})

	second := s.opened("second", "acme", s.base, nil)
	s.turned(second, "second-1", s.base, nil)
	s.calledTool(second, "second-1", "lookup_order", s.base, map[string]any{"call_id": "c1"}, map[string]any{"call_id": "c1"})

	overall, tools, err := s.store.CustomerToolUsage(s.ctx, "acme", "", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 1)
	s.EqualValues(3, tools[0].Calls)
	s.EqualValues(2, tools[0].Sessions)
	s.Require().NotNil(tools[0].AvgTurnsBeforeUse)
	// Two turns before the first session's first call, the rewound one not among them, and
	// none before the second's.
	s.InDelta(1.0, *tools[0].AvgTurnsBeforeUse, 0.001)
	s.Nil(overall.AvgTurnsBeforeUse)

	turns, err := s.store.CustomerTurns(s.ctx, "acme", "", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Equal(SessionTurns{Sessions: 2, Turns: 5}, turns)
}

func (s *StoreSuite) TestASessionOfFiveTurnsIsFiveTurnsPerSession() {
	session := s.opened("five", "acme", s.base, nil)
	parent := ""
	for i := range 5 {
		id := fmt.Sprintf("five-%d", i)
		s.turned(session, id, s.base.Add(time.Duration(i)*time.Minute), answeredAfter(parent))
		parent = id
	}

	turns, err := s.store.CustomerTurns(s.ctx, "acme", "", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Equal(SessionTurns{Sessions: 1, Turns: 5}, turns)
}

func (s *StoreSuite) TestAToolUsedOnTheFourthTurnHadThreeTurnsBeforeIt() {
	session := s.opened("late", "acme", s.base, nil)
	parent := ""
	for i := range 4 {
		id := fmt.Sprintf("late-%d", i)
		s.turned(session, id, s.base.Add(time.Duration(i)*time.Minute), answeredAfter(parent))
		parent = id
	}
	s.calledTool(session, "late-3", "lookup_order", s.base.Add(3*time.Minute), map[string]any{"call_id": "c1"}, map[string]any{"call_id": "c1"})

	_, tools, err := s.store.CustomerToolUsage(s.ctx, "acme", "", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 1)
	s.Require().NotNil(tools[0].AvgTurnsBeforeUse)
	s.InDelta(3.0, *tools[0].AvgTurnsBeforeUse, 0.001)
}

func (s *StoreSuite) TestATextSessionAddsNothingToCallsOrVoiceMinutes() {
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{
		ID: "typed", CustomerID: "acme", AgentID: "a-1", StartedAt: s.base,
	}))
	s.Require().NoError(s.store.Calls.Finish(s.ctx, "acme", "typed", s.base.Add(30*time.Minute)))
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{
		ID: "spoken", CustomerID: "acme", CallID: "c-1", AgentID: "a-1", StartedAt: s.base.Add(time.Minute),
	}))
	s.Require().NoError(s.store.Calls.Finish(s.ctx, "acme", "spoken", s.base.Add(3*time.Minute)))

	buckets, err := s.store.CustomerActivity(s.ctx, "acme", ActivityDaily, s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1, buckets[0].Calls, "only the stretch that had a call")
	s.InDelta(2, buckets[0].VoiceMinutes, 0.001)
}

// answeredAfter is a turn answered after parent, as every turn but a session's first is.
func answeredAfter(parent string) func(*AgentResponse) {
	return func(response *AgentResponse) { response.ParentResponseID = parent }
}

func (s *StoreSuite) TestToolUsageReportsTheLatestDefinitionAToolWasOfferedWith() {
	session := s.opened("one", "acme", s.base, nil)
	s.turned(session, "one-1", s.base.Add(time.Minute), nil)
	s.calledTool(session, "one-1", "lookup_order", s.base.Add(time.Minute),
		map[string]any{"call_id": "c1", "loading": ToolLoadingFull, "definition_tokens": 461}, map[string]any{"call_id": "c1"})
	s.turned(session, "one-2", s.base.Add(2*time.Minute), nil)
	s.calledTool(session, "one-2", "lookup_order", s.base.Add(2*time.Minute),
		map[string]any{"call_id": "c2", "loading": ToolLoadingSummary, "definition_tokens": 470, "summary_tokens": 84},
		map[string]any{"call_id": "c2"})
	s.turned(session, "one-3", s.base.Add(3*time.Minute), nil)
	s.calledTool(session, "one-3", "lookup_order", s.base.Add(3*time.Minute), map[string]any{"call_id": "c3"}, map[string]any{"call_id": "c3"})

	_, tools, err := s.store.CustomerToolUsage(s.ctx, "acme", "", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 1)
	s.Equal(ToolLoadingSummary, tools[0].Loading, "the config moved it to a summary")
	s.Require().NotNil(tools[0].DefinitionTokens)
	s.EqualValues(470, *tools[0].DefinitionTokens)
	s.Require().NotNil(tools[0].SummaryTokens)
	s.EqualValues(84, *tools[0].SummaryTokens)
}

func (s *StoreSuite) TestARedactedCallStillReportsWhatItAndItsToolCost() {
	session := s.opened("one", "acme", s.base, nil)
	s.turned(session, "one-1", s.base, nil)
	s.calledTool(session, "one-1", "lookup_order", s.base,
		map[string]any{"call_id": "c1", "loading": ToolLoadingSummary, "definition_tokens": 461, "summary_tokens": 84},
		map[string]any{"call_id": "c1", "arguments": `{"order":"A-1"}`, "tokens": 250})
	_, err := s.store.ShardDB().NewUpdate().Model((*AgentResponseItem)(nil)).
		Set("redact_at = ?", s.base).Where("session_id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)
	redacted, err := s.store.ResponseItems.Redact(s.ctx, s.base.Add(time.Hour), 100)
	s.Require().NoError(err)
	s.EqualValues(2, redacted)

	_, tools, err := s.store.CustomerToolUsage(s.ctx, "acme", "", s.base, s.base.Add(24*time.Hour))
	s.Require().NoError(err)
	s.Require().Len(tools, 1)
	s.InDelta(250, tools[0].AvgTokens, 0.001, "the result it gave the model is gone, not what it cost")
	s.Equal(ToolLoadingSummary, tools[0].Loading)
	s.Require().NotNil(tools[0].DefinitionTokens)
	s.EqualValues(461, *tools[0].DefinitionTokens)
	s.Require().NotNil(tools[0].SummaryTokens)
	s.EqualValues(84, *tools[0].SummaryTokens)
}

func (s *StoreSuite) TestUsageQueriesRejectBadArguments() {
	_, err := s.store.Requests.Spend(s.ctx, "", "modality", Daily, s.base, s.base.Add(time.Hour), 6, nil, UsageFilter{})
	s.ErrorContains(err, "customer id is required")

	_, err = s.store.Requests.Spend(
		s.ctx, "acme", "modality", Granularity("weekly"), s.base, s.base.Add(time.Hour), 6, nil, UsageFilter{})
	s.ErrorContains(err, "unknown granularity")

	_, err = s.store.Requests.Spend(s.ctx, "acme", "modality", Daily, s.base, s.base.Add(time.Hour), 0, nil, UsageFilter{})
	s.ErrorContains(err, "limit must be at least 1")

	_, err = s.store.Requests.TagKeys(s.ctx, "", s.base, s.base.Add(time.Hour), nil)
	s.ErrorContains(err, "customer id is required")

	_, err = s.store.CustomerActivity(s.ctx, "acme", ActivityGranularity("weekly"), s.base, s.base.Add(time.Hour))
	s.ErrorContains(err, "unknown activity granularity")

	_, err = s.store.CustomerActivity(s.ctx, "", ActivityDaily, s.base, s.base.Add(time.Hour))
	s.ErrorContains(err, "customer id is required")
}
