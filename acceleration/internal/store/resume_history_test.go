//go:build integration

package store

import (
	"maps"
	"slices"
	"time"
)

// replied records one response with the items given, numbered in order.
func (s *StoreSuite) replied(sessionID, responseID, said string, at time.Time, items ...AgentResponseItem) {
	s.Require().NoError(s.store.Responses.Insert(s.ctx, &AgentResponse{
		ID: responseID, SessionID: sessionID, CustomerID: "app", Said: said, CreatedAt: at,
	}))
	for i := range items {
		items[i].ResponseID, items[i].Ordinal, items[i].SessionID = responseID, i, sessionID
		items[i].At = at.Add(time.Duration(i) * time.Second)
	}
	s.Require().NoError(s.store.ResponseItems.InsertMany(s.ctx, items))
	s.Require().NoError(s.store.Responses.Finish(s.ctx, "app", responseID, ResponseCompleted, "", at.Add(time.Minute)))
}

func answer(turn, text string) AgentResponseItem {
	return AgentResponseItem{Kind: ItemAnswer, Text: text, Payload: map[string]any{"turn_id": turn}}
}

func result(callID, text string) AgentResponseItem {
	return AgentResponseItem{Kind: ItemToolResult, ToolName: "lookup", Text: text, Payload: map[string]any{"call_id": callID}}
}

func (s *StoreSuite) TestAReplyIsFoundByTheTurnItsTranscriptWasWrittenUnder() {
	s.opened("one", "app", s.base, func(session *AgentSession) { session.ConversationID = "agent:chat" })
	s.replied("one", "first", "look it up", s.base,
		AgentResponseItem{Kind: ItemSaid, Text: "look it up"},
		answer("turn-1", ""),
		AgentResponseItem{Kind: ItemToolCall, ToolName: "lookup", Payload: map[string]any{"call_id": "call-1"}},
		result("call-1", "42"),
		answer("turn-2", "It is 42."),
	)

	replies, err := s.store.Responses.Replies(s.ctx, "app", "agent:chat", []string{"turn-2", "turn-unknown"}, nil)

	s.Require().NoError(err)
	s.Require().Len(replies, 1)
	kinds := []string{}
	for _, item := range replies["turn-2"].Items {
		kinds = append(kinds, item.Kind)
	}
	s.Equal([]string{ItemAnswer, ItemToolResult, ItemAnswer}, kinds)
}

func (s *StoreSuite) TestAnotherConversationsTurnFindsNothing() {
	s.opened("one", "app", s.base, func(session *AgentSession) { session.ConversationID = "agent:theirs" })
	s.replied("one", "first", "secret", s.base, result("call-1", "private"), answer("turn-1", "done"))
	s.opened("two", "app", s.base, func(session *AgentSession) { session.ConversationID = "agent:mine" })

	replies, err := s.store.Responses.Replies(s.ctx, "app", "agent:mine", []string{"turn-1"}, nil)

	s.Require().NoError(err)
	s.Empty(replies)
}

// In a conversation open to its members, a reader asking for their own replies finds
// none of another member's, though both wrote under the same conversation.
func (s *StoreSuite) TestAnOwnersReadFindsNoOtherMembersReply() {
	member := func(user string) func(*AgentSession) {
		return func(session *AgentSession) { session.ConversationID, session.UserID = "agent:shared", user }
	}
	s.opened("anyas", "app", s.base, member("anya"))
	s.replied("anyas", "first", "check my inbox", s.base, result("call-1", "PRIVATE-INBOX-OF-ANYA"), answer("turn-anya", "done"))
	s.opened("bens", "app", s.base.Add(time.Hour), member("ben"))
	s.replied("bens", "second", "and mine", s.base.Add(time.Hour), result("call-2", "inbox of ben"), answer("turn-ben", "done"))
	turns := []string{"turn-anya", "turn-ben"}

	bens, err := s.store.Responses.Replies(s.ctx, "app", "agent:shared", turns, &SessionOwner{UserID: "ben", CallerKind: "user"})
	s.Require().NoError(err)
	everyones, err := s.store.Responses.Replies(s.ctx, "app", "agent:shared", turns, nil)
	s.Require().NoError(err)

	s.Equal([]string{"turn-ben"}, slices.Collect(maps.Keys(bens)))
	s.Len(everyones, 2, "a read for no owner still finds every session's")
}

// A reader of the same user id under another kind is not the session's owner.
func (s *StoreSuite) TestAnOwnersReadFindsNoReplyOfTheSameIDUnderAnotherKind() {
	s.opened("guests", "app", s.base, func(session *AgentSession) {
		session.ConversationID, session.UserID, session.CallerKind = "agent:shared", "ben", "guest"
	})
	s.replied("guests", "first", "a", s.base, result("call-1", "x"), answer("turn-1", "done"))

	replies, err := s.store.Responses.Replies(s.ctx, "app", "agent:shared", []string{"turn-1"}, &SessionOwner{UserID: "ben", CallerKind: "user"})

	s.Require().NoError(err)
	s.Empty(replies)
}

func (s *StoreSuite) TestARewoundReplyFindsNothing() {
	s.opened("one", "app", s.base, func(session *AgentSession) { session.ConversationID = "agent:chat" })
	s.replied("one", "first", "a", s.base, answer("turn-1", "one"))
	s.replied("one", "second", "b", s.base.Add(time.Hour), result("call-1", "x"), answer("turn-2", "two"))
	s.Require().NoError(s.store.Responses.Rewind(s.ctx, "app", "one", "first", s.base.Add(2*time.Hour)))

	replies, err := s.store.Responses.Replies(s.ctx, "app", "agent:chat", []string{"turn-1", "turn-2"}, nil)

	s.Require().NoError(err)
	s.Len(replies, 1)
	s.Contains(replies, "turn-1")
}

func (s *StoreSuite) TestTheOpeningIsTheFirstThingSaidAcrossTheConversationsSessions() {
	s.opened("later", "app", s.base.Add(time.Hour), func(session *AgentSession) { session.ConversationID = "agent:chat" })
	s.opened("first", "app", s.base, func(session *AgentSession) { session.ConversationID = "agent:chat" })
	s.replied("later", "r2", "and then", s.base.Add(time.Hour), answer("turn-2", "ok"))
	s.replied("first", "r1", "the task", s.base, answer("turn-1", "ok"))

	opening, err := s.store.Responses.Opening(s.ctx, "app", "agent:chat")

	s.Require().NoError(err)
	s.Equal("the task", opening.Said)
	s.True(s.base.Equal(opening.At), "when it was said, which a link's boundary is held against: %v", opening.At)
}

func (s *StoreSuite) TestAConfigKeepsItsResumeHistory() {
	yes := true
	config := &AgentConfig{CustomerID: "app", Name: "resume", Mode: AgentModeText,
		ResumeHistory: ResumeHistory{IncludeTools: &yes, MaxCharacters: 120000}}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))
	config.ResumeHistory.MaxMessages = 50
	s.Require().NoError(s.store.AgentConfigs.Update(s.ctx, config))

	stored, err := s.store.AgentConfigs.Get(s.ctx, "app", config.ID)

	s.Require().NoError(err)
	s.Equal(ResumeHistory{IncludeTools: &yes, MaxMessages: 50, MaxCharacters: 120000}, stored.ResumeHistory)
}
