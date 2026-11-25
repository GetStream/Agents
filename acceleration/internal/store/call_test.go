//go:build integration

package store

import (
	"fmt"
	"time"
)

func (s *StoreSuite) TestAnAgentIdSaysWhoseChannelItIsAndWhatRanOnIt() {
	// A message arriving on a channel names the agent and nothing else. Without this row
	// there is no way to tell whose message it is, and a message that cannot be billed to
	// anybody cannot be answered.
	//
	// The agent id belongs to this test rather than being shared: a channel is looked up
	// by it alone, so two tests naming the same one are asking about each other's calls.
	agentID := "agent-whose-channel"
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &AgentSession{
		ID: "session-whose-channel", CustomerID: "acme", AgentID: agentID, ConfigID: "chat_support",
	}))
	call := Call{
		ID: "call-whose-channel", SessionID: "session-whose-channel", CustomerID: "acme",
		CallID: "call-1", AgentID: agentID, StartedAt: s.base,
	}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))

	found, err := s.store.CallByAgentInApp(s.ctx, AppScope{Unpinned: true}, agentID)

	s.Require().NoError(err)
	s.Equal("acme", found.CustomerID)
	s.Equal("chat_support", found.ConfigID, "which agent is being written to")
}

func (s *StoreSuite) TestWritingToAChannelReachesTheLastConversationOnIt() {
	// An agent id outlives the call that made it, so the same channel can hold several.
	// The one somebody writing there is continuing is the last one.
	agentID := "agent-last-conversation"
	for id, config := range map[string]string{"older-session": "old_config", "newer-session": "chat_support"} {
		s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &AgentSession{ID: id, CustomerID: "acme", AgentID: agentID, ConfigID: config}))
	}
	older := Call{
		ID: "older-last-conversation", SessionID: "older-session", CustomerID: "acme",
		CallID: "call-1", AgentID: agentID, StartedAt: s.base.Add(-time.Hour),
	}
	newer := Call{
		ID: "newer-last-conversation", SessionID: "newer-session", CustomerID: "acme",
		CallID: "call-2", AgentID: agentID, StartedAt: s.base,
	}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &older))
	s.Require().NoError(s.store.Calls.Start(s.ctx, &newer))

	found, err := s.store.CallByAgentInApp(s.ctx, AppScope{Unpinned: true}, agentID)

	s.Require().NoError(err)
	s.Equal(newer.ID, found.ID)
	s.Equal("chat_support", found.ConfigID)
}

// A call names its session by id, and the agent config was read off whichever customer's
// session had it.
func (s *StoreSuite) TestAChannelsAgentConfigIsReadOffTheCustomersOwnSession() {
	agentID := "agent-of-a-borrowed-id"
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &AgentSession{ID: "session-of-a-borrowed-id", CustomerID: "globex", ConfigID: "globex_support"}))
	call := Call{
		ID: "call-of-a-borrowed-id", SessionID: "session-of-a-borrowed-id", CustomerID: "acme",
		CallID: "call-1", AgentID: agentID, StartedAt: s.base,
	}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))

	found, err := s.store.CallByAgentInApp(s.ctx, AppScope{Unpinned: true}, agentID)

	s.Require().NoError(err)
	s.Equal("acme", found.CustomerID)
	s.Empty(found.ConfigID, "globex's agent config is not acme's")
}

// sessionCall starts call as one of its session's, kept under configID: the session's row
// first, as the session's recorder writes them, since the call names it.
func (s *StoreSuite) sessionCall(into *Store, call Call, configID string) {
	s.Require().NoError(into.Sessions.Upsert(s.ctx, &AgentSession{
		ID: call.SessionID, CustomerID: call.CustomerID, AgentID: call.AgentID, ConfigID: configID,
	}))
	s.Require().NoError(into.Calls.Start(s.ctx, &call))
}

func (s *StoreSuite) TestNoConversationIsFoundForAChannelNoAgentHasBeenOn() {
	// This is a message in a channel that merely looks like an agent's. There is nobody to
	// bill it to and nothing to start, and saying so is the whole answer.
	_, err := s.store.CallByAgentInApp(s.ctx, AppScope{Unpinned: true}, "a-channel-nobody-ran-on")

	s.Require().Error(err)
}

func (s *StoreSuite) TestACallKeepsTheTimeItFirstEnded() {
	// An agent leaves once. A second close is the same leaving reported again, and must
	// not stretch the call to cover it.
	call := Call{
		ID: "session-ended-once", CustomerID: "acme",
		CallID: "call-1", AgentID: "agent-ended-once", StartedAt: s.base,
	}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))
	s.Require().NoError(s.store.Calls.Finish(s.ctx, call.CustomerID, call.ID, s.base.Add(time.Minute)))
	s.Require().NoError(s.store.Calls.Finish(s.ctx, call.CustomerID, call.ID, s.base.Add(time.Hour)))

	read, err := s.store.Calls.Get(s.ctx, "acme", call.ID)
	s.Require().NoError(err)
	s.Require().NotNil(read.EndedAt)
	s.WithinDuration(s.base.Add(time.Minute), *read.EndedAt, time.Second)
}

func (s *StoreSuite) TestACallTheRouterPlacedIsStoredAsOutboundBetweenItsNumbers() {
	// A client opening the session of a call it placed may name neither number.
	s.placedLeg("placed", func(leg *CallLeg) {
		leg.CustomerID, leg.CallType, leg.CallID = "acme", "agent", "session-placed"
	})
	call := Call{ID: "session-placed", CustomerID: "acme", CallID: "session-placed", AgentID: "agent-placed", StartedAt: s.base}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))

	read, err := s.store.Calls.Get(s.ctx, "acme", call.ID)
	s.Require().NoError(err)
	s.Equal(Outbound, read.Direction)
	s.Equal("+15125550100", read.FromNumber)
	s.Equal("+15125550199", read.ToNumber)
}

func (s *StoreSuite) TestAnArrivingCallIsStoredAsInboundEvenWhereAnotherCustomerPlacedOne() {
	s.placedLeg("theirs", func(leg *CallLeg) {
		leg.CustomerID, leg.CallType, leg.CallID = "someone-else", "agent", "session-arrived"
	})
	call := Call{ID: "session-arrived", CustomerID: "acme", CallID: "session-arrived", AgentID: "agent-arrived",
		FromNumber: "+15125550142", StartedAt: s.base}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))

	read, err := s.store.Calls.Get(s.ctx, "acme", call.ID)
	s.Require().NoError(err)
	s.Equal(Inbound, read.Direction)
	s.Equal("+15125550142", read.FromNumber)
	s.Empty(read.ToNumber)
}

func (s *ShardedStoreSuite) TestACallAnotherShardPlacedIsStoredAsOutbound() {
	at := s.base
	placed := CallLeg{Token: "there", CustomerID: "acme", CallType: "agent", CallID: "session-there", Vendor: "telnyx",
		Direction: Outbound, LocalNumber: "+15125550100", RemoteNumber: "+15125550199", PlacedAt: &at}
	s.Require().NoError(s.other.CallLegs.Place(s.ctx, &placed))
	call := Call{ID: "session-there", CustomerID: "acme", CallID: "session-there", AgentID: "agent-there", StartedAt: s.base}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))

	read, err := s.store.Calls.Get(s.ctx, "acme", call.ID)
	s.Require().NoError(err)
	s.Equal(Outbound, read.Direction)
	s.Equal("+15125550199", read.ToNumber)
}

func (s *StoreSuite) TestACallStartedAgainLearnsWhoTheAgentWroteAsAndKeepsIt() {
	// A call row from before the column names no writer. The session carrying on under the
	// same id names it, and the row keeps the first one it was told.
	call := Call{
		ID: "session-learns-writer", CustomerID: "acme",
		CallID: "call-1", AgentID: "agent-learns-writer", StartedAt: s.base,
	}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))
	before, err := s.store.Calls.Get(s.ctx, "acme", call.ID)
	s.Require().NoError(err)
	s.Require().Empty(before.AgentUserID)

	learned := call
	learned.AgentUserID = "vision-agent"
	s.Require().NoError(s.store.Calls.Start(s.ctx, &learned))
	other := call
	other.AgentUserID = "another-agent"
	s.Require().NoError(s.store.Calls.Start(s.ctx, &other))
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))

	read, err := s.store.Calls.Get(s.ctx, "acme", call.ID)
	s.Require().NoError(err)
	s.Equal("vision-agent", read.AgentUserID)
}

func (s *StoreSuite) TestACallStartedAgainLearnsTheConversationItWasBoundToAndKeepsIt() {
	// A call row from before the column names no conversation. The session carrying on under
	// the same id names it, and the row keeps the first one it was told.
	call := Call{
		ID: "session-learns-conversation", CustomerID: "acme",
		CallID: "call-1", AgentID: "agent-learns-conversation", StartedAt: s.base,
	}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))
	before, err := s.store.Calls.Get(s.ctx, "acme", call.ID)
	s.Require().NoError(err)
	s.Require().Empty(before.ConversationID)

	learned := call
	learned.ConversationID = "agent:first"
	s.Require().NoError(s.store.Calls.Start(s.ctx, &learned))
	other := call
	other.ConversationID = "agent:second"
	s.Require().NoError(s.store.Calls.Start(s.ctx, &other))
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))

	read, err := s.store.Calls.Get(s.ctx, "acme", call.ID)
	s.Require().NoError(err)
	s.Equal("agent:first", read.ConversationID)
}

func (s *StoreSuite) TestCallQueryBudgets() {
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	s.opened("called", "app", at, nil)
	for i := range 60 {
		s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{ID: fmt.Sprintf("call-%02d", i), SessionID: "called",
			CustomerID: "app", AgentID: "agent", CallID: fmt.Sprintf("stream-%02d", i%2*i),
			StartedAt: at.Add(time.Duration(i) * time.Second)}))
	}
	request := Request{CustomerID: "app", AgentID: "agent", SessionID: "called", Modality: "llm", Provider: "stub",
		Model: "stub-llm", InputTokens: 10, StartedAt: at.Add(time.Second), Success: true}
	s.Require().NoError(insertRequest(s.ctx, s.store, &request))
	call := &Call{ID: "budgeted", SessionID: "called", CustomerID: "app", AgentID: "agent", StartedAt: at.Add(time.Hour)}
	score := 4
	end := at.Add(2 * time.Hour)
	s.withinQueryBudgets([]queryBudget{
		{"Start", 1, 0, func() error { return s.store.Calls.Start(s.ctx, call) }},
		{"ChangeModels", 1, 0, func() error { call.LLM = "stub-llm"; return s.store.Calls.ChangeModels(s.ctx, call) }},
		{"Finish", 1, 0, func() error { return s.store.Calls.Finish(s.ctx, call.CustomerID, call.ID, end) }},
		{"Review", 0, 2, func() error { return s.store.Calls.Review(s.ctx, "app", call.ID, "Fine.", &score, "") }},
		{"Get", 1, 0, func() error { _, err := s.store.Calls.Get(s.ctx, "app", call.ID); return err }},
		{"Latest", 0, 1, func() error {
			latest, err := s.store.Calls.Latest(s.ctx, "app", []string{"called", "nobody"})
			s.Equal(call.ID, latest["called"].ID)
			return err
		}},
		{"LastVoiced", 1, 0, func() error {
			voiced, err := s.store.Calls.LastVoiced(s.ctx, "app", "called")
			s.Equal("call-59", voiced.ID)
			return err
		}},
		{"Select", 0, 1, func() error {
			listed, err := s.store.Calls.Select(s.ctx, "app", CallFilter{SessionID: "called"})
			s.Len(listed, defaultCallLimit)
			return err
		}},
		{"ModelCalls", 0, 1, func() error {
			_, err := s.store.Calls.ModelCalls(s.ctx, "app", "agent", "stream-01", at, &end)
			return err
		}},
		{"UsedModels", 0, 1, func() error { _, err := s.store.Calls.UsedModels(s.ctx, "app", "agent", at, &end); return err }},
		{"Usage", 0, 1, func() error {
			used, err := s.store.Calls.Usage(s.ctx, "app", "agent", "called", at, &end)
			s.EqualValues(1, used.Requests)
			return err
		}},
		{"Tokens", 0, 1, func() error {
			_, err := s.store.Calls.Tokens(s.ctx, "app", "agent", "called", at, &end)
			return err
		}},
	})
}

func (s *StoreSuite) TestACallGoingNoKnownWayIsNotWritten() {
	err := s.store.Calls.Start(s.ctx, &Call{ID: "invalid", CustomerID: "app", Direction: "sideways"})
	s.refusedUnwritten(err, "calls", "id", "invalid")
}

// Defect: deleting a session takes its calls with it, or leaves them naming a session that is gone.
func (s *StoreSuite) TestDeletingASessionKeepsItsCallsWithoutIt() {
	s.opened("deleted", "app", s.base, nil)
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{ID: "kept", SessionID: "deleted", CustomerID: "app",
		ConversationID: "agent:deleted", StartedAt: s.base}))

	s.Require().NoError(s.store.Sessions.Delete(s.ctx, "app", "deleted"))

	call, err := s.store.Calls.Get(s.ctx, "app", "kept")
	s.Require().NoError(err)
	s.Empty(call.SessionID)
	s.Equal("agent:deleted", call.ConversationID, "the only place left that names the conversation")
}

// Defect: a session's calls listed with another session's, out of order, or to another customer.
func (s *StoreSuite) TestACallListNarrowsToOneSessionNewestFirst() {
	for _, id := range []string{"one", "two"} {
		s.opened(id, "app", s.base, nil)
	}
	for i, call := range []Call{
		{ID: "one-first", SessionID: "one"}, {ID: "two-only", SessionID: "two"}, {ID: "one-second", SessionID: "one"},
	} {
		call.CustomerID, call.StartedAt = "app", s.base.Add(time.Duration(i)*time.Minute)
		s.Require().NoError(s.store.Calls.Start(s.ctx, &call))
	}

	listed, err := s.store.Calls.Select(s.ctx, "app", CallFilter{SessionID: "one"})
	s.Require().NoError(err)
	ids := make([]string, 0, len(listed))
	for _, call := range listed {
		ids = append(ids, call.ID)
	}
	s.Equal([]string{"one-second", "one-first"}, ids)

	theirs, err := s.store.Calls.Select(s.ctx, "somebody-else", CallFilter{SessionID: "one"})
	s.Require().NoError(err)
	s.Empty(theirs)
}

// Defect: a session reopened after it went back to text holds the text call, which has no
// Stream call to hold, rather than the voice call before it.
func (s *StoreSuite) TestTheLastVoicedCallSkipsTheTextAfterIt() {
	s.opened("switched", "app", s.base, nil)
	s.opened("typed", "app", s.base, nil)
	for i, call := range []Call{
		{ID: "typed-first", SessionID: "switched"},
		{ID: "spoken", SessionID: "switched", CallID: "stream-call"},
		{ID: "typed-after", SessionID: "switched"},
		{ID: "only-typed", SessionID: "typed"},
	} {
		call.CustomerID, call.StartedAt = "app", s.base.Add(time.Duration(i)*time.Minute)
		s.Require().NoError(s.store.Calls.Start(s.ctx, &call))
	}

	voiced, err := s.store.Calls.LastVoiced(s.ctx, "app", "switched")
	s.Require().NoError(err)
	s.Equal("spoken", voiced.ID)
	latest, err := s.store.Calls.Latest(s.ctx, "app", []string{"switched", "typed"})
	s.Require().NoError(err)
	s.Equal("typed-after", latest["switched"].ID)
	s.Equal("only-typed", latest["typed"].ID)

	_, err = s.store.Calls.LastVoiced(s.ctx, "app", "typed")
	s.ErrorIs(err, ErrNoCall)
	_, err = s.store.Calls.LastVoiced(s.ctx, "somebody-else", "switched")
	s.ErrorIs(err, ErrNoCall)
}

func (s *StoreSuite) TestAnotherCustomersWriteToACallIdChangesNothing() {
	writes := map[string]func() error{
		"Finish": func() error { return s.store.Calls.Finish(s.ctx, "somebody-else", "one", s.base.Add(time.Hour)) },
		"ChangeModels": func() error {
			return s.store.Calls.ChangeModels(s.ctx, &Call{ID: "one", CustomerID: "somebody-else", LLM: "their-llm", Voice: "their-voice"})
		},
	}
	for name, write := range writes {
		s.Run(name, func() {
			s.SetupTest()
			s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{ID: "one", CustomerID: "app", CallID: "call-1", AgentID: "agent",
				LLM: "app-llm", Voice: "app-voice", StartedAt: s.base}))
			before, err := s.store.Calls.Get(s.ctx, "app", "one")
			s.Require().NoError(err)

			s.Require().NoError(write())

			after, err := s.store.Calls.Get(s.ctx, "app", "one")
			s.Require().NoError(err)
			s.Equal(before, after, "app's call is as it was")
		})
	}
}

func (s *StoreSuite) TestAnotherCustomersCallByTheSameIdIsNotOpenedAgain() {
	call := Call{ID: "one", CustomerID: "app", CallID: "call-1", AgentID: "agent-theirs", StartedAt: s.base}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call))
	s.Require().NoError(s.store.Calls.Finish(s.ctx, "app", call.ID, s.base.Add(time.Minute)))
	before, err := s.store.Calls.Get(s.ctx, "app", "one")
	s.Require().NoError(err)

	err = s.store.Calls.Start(s.ctx, &Call{
		ID: "one", CustomerID: "somebody-else", CallID: "call-2", AgentID: "agent-other", StartedAt: s.base.Add(time.Hour),
	})
	s.Require().ErrorIs(err, ErrIDTaken)

	after, err := s.store.Calls.Get(s.ctx, "app", "one")
	s.Require().NoError(err)
	s.Equal(before, after, "app's call is as it was, still ended")
	s.Require().NoError(s.store.Calls.Start(s.ctx, &call), "app starting its own call again is not refused")
}
