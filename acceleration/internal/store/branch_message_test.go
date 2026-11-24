//go:build integration

package store

import (
	"fmt"
	"slices"
	"time"
)

// hidden is a copy of a message of session "one" that switch hid.
func hidden(id, response, switchID string, at time.Time, text string) AgentBranchMessage {
	return AgentBranchMessage{
		SessionID: "one", OriginalMessageID: id, CustomerID: "app", ResponseID: response,
		SwitchID: switchID, ChatCreatedAt: at, AuthorID: "jlahey", Message: map[string]any{"text": text},
	}
}

func (s *StoreSuite) TestACopiedMessageIsListedByTheResponseItBelongsTo() {
	s.opened("one", "app", s.base, nil)
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{
		hidden("m2", "second", "switch", s.base.Add(time.Minute), "answer"),
		hidden("m1", "second", "switch", s.base, "question"),
		hidden("m3", "third", "switch", s.base.Add(2*time.Minute), "later"),
	}))

	found, err := s.store.BranchMessages.Select(s.ctx, "app", "one", []string{"second"})
	s.Require().NoError(err)
	s.Require().Len(found, 2)
	s.Equal("m1", found[0].OriginalMessageID)
	s.Equal("m2", found[1].OriginalMessageID)
	s.Equal("answer", found[1].Message["text"])

	all, err := s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Len(all, 3)
}

func (s *StoreSuite) TestCopyingAMessageAgainKeepsWhatItSaysNow() {
	s.opened("one", "app", s.base, nil)
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "second", "switch", s.base, "partial")}))
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "second", "switch", s.base, "partial and finished")}))

	found, err := s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Require().Len(found, 1)
	s.Equal("partial and finished", found[0].Message["text"])
}

// A save names no conflict target, so copies are kept and saved again under the key the release
// after this one gives them, where one naming the session and message alone fails every save.
func (s *StoreSuite) TestCopiesAreSavedAgainUnderAKeyOfCustomerSessionAndMessage() {
	s.keyedBy("agent_branch_messages", []string{"session_id", "original_message_id"},
		[]string{"customer_id", "session_id", "original_message_id"})
	s.opened("one", "app", s.base, nil)

	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "second", "switch", s.base, "partial")}))
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{
		hidden("m1", "second", "switch", s.base, "partial and finished"), hidden("m2", "second", "switch", s.base, "new"),
	}))

	found, err := s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Require().Len(found, 2)
	s.Equal("partial and finished", found[0].Message["text"])
	s.Equal("new", found[1].Message["text"])
}

// Copies of another customer's are left as they are: its row of the message is not the one a
// save of the customer's replaces.
func (s *StoreSuite) TestSavingACopyAgainLeavesAnotherCustomersAlone() {
	s.opened("one", "app", s.base, nil)
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "second", "switch", s.base, "app's")}))
	theirs := hidden("m1", "second", "switch", s.base, "theirs")
	theirs.CustomerID = "somebody-else"

	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{theirs}))

	found, err := s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Require().Len(found, 1)
	s.Equal("app's", found[0].Message["text"])
}

func (s *StoreSuite) TestForgettingAUserDeletesTheCopiesOfTheirSessionsAlone() {
	s.opened("one", "app", s.base, nil)
	s.opened("two", "app", s.base, func(session *AgentSession) { session.UserID = "rlafleur" })
	theirs := hidden("m2", "second", "switch", s.base, "kept")
	theirs.SessionID = "two"
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "second", "switch", s.base, "gone"), theirs}))

	deleted, err := s.store.BranchMessages.DeleteUser(s.ctx, "app", "jlahey")
	s.Require().NoError(err)
	s.EqualValues(1, deleted)

	mine, err := s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Empty(mine)
	left, err := s.store.BranchMessages.Select(s.ctx, "app", "two", nil)
	s.Require().NoError(err)
	s.Len(left, 1)
}

func (s *StoreSuite) TestCountingAUsersCopiesChangesNothingAndMatchesTheDeletion() {
	s.opened("one", "app", s.base, nil)
	s.opened("two", "app", s.base, func(session *AgentSession) { session.UserID = "rlafleur" })
	theirs := hidden("m2", "second", "switch", s.base, "kept")
	theirs.SessionID = "two"
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "second", "switch", s.base, "gone"), theirs}))

	counted, err := s.store.BranchMessages.CountUser(s.ctx, "app", "jlahey")
	s.Require().NoError(err)
	s.EqualValues(1, counted)
	all, err := s.store.BranchMessages.Select(s.ctx, "app", "one", nil)
	s.Require().NoError(err)
	s.Len(all, 1, "counting deletes nothing")
	other, err := s.store.BranchMessages.CountUser(s.ctx, "somebody-else", "jlahey")
	s.Require().NoError(err)
	s.Zero(other)
	_, err = s.store.BranchMessages.CountUser(s.ctx, "app", "")
	s.Error(err)
}

func (s *StoreSuite) TestBranchMessageQueryBudgets() {
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	s.opened("branched", "app", at, nil)
	messages := make([]AgentBranchMessage, 0, 30)
	for i := range 30 {
		messages = append(messages, AgentBranchMessage{SessionID: "branched", CustomerID: "app", Message: map[string]any{},
			OriginalMessageID: fmt.Sprintf("message-%02d", i), ResponseID: fmt.Sprintf("response-%02d", i)})
	}
	s.withinQueryBudgets([]queryBudget{
		{"BranchMessages.UpsertMany", 1, 0, func() error { return s.store.BranchMessages.UpsertMany(s.ctx, messages) }},
		{"BranchMessages.Select", 0, 1, func() error {
			listed, err := s.store.BranchMessages.Select(s.ctx, "app", "branched", []string{"response-01", "response-02"})
			s.Len(listed, 2)
			return err
		}},
		{"BranchMessages.CountUser", 0, 1, func() error {
			counted, err := s.store.BranchMessages.CountUser(s.ctx, "app", "jlahey")
			s.EqualValues(30, counted)
			return err
		}},
		{"BranchMessages.DeleteUser", 0, 1, func() error {
			_, err := s.store.BranchMessages.DeleteUser(s.ctx, "app", "jlahey")
			return err
		}},
	})
}

func (s *StoreSuite) TestACopyNamingNoCustomerIsNotWritten() {
	err := s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{{SessionID: "one", OriginalMessageID: "invalid"}})
	s.refusedUnwritten(err, "agent_branch_messages", "original_message_id", "invalid")
}

func (s *StoreSuite) TestForgettingAUserEmptiesTheChangesRecordedOfTheirCopies() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	s.opened("one", "app", s.base, nil)
	s.opened("two", "app", s.base, func(session *AgentSession) { session.UserID = "rlafleur" })
	theirs := hidden("m2", "second", "switch", s.base, "kept words")
	theirs.SessionID = "two"
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "second", "switch", s.base, "forgotten words"), theirs}))
	s.Require().NoError(s.store.Calls.Start(s.ctx, &Call{ID: "call-1", SessionID: "one", CustomerID: "app", ConversationID: "agent:one", StartedAt: s.base}))
	before := seqs(s.recordedFor("app"))
	s.Require().NotEmpty(s.payloadsNaming("app", "forgotten words"), "the copy's insert was recorded")

	_, err := s.store.BranchMessages.DeleteUser(s.ctx, "app", "jlahey")
	s.Require().NoError(err)

	s.Empty(s.payloadsNaming("app", "forgotten words"))
	s.NotEmpty(s.payloadsNaming("app", "kept words"), "another user's copies keep their changes")
	s.Subset(seqs(s.recordedFor("app")), before, "every change keeps its number")
	deletes := slices.DeleteFunc(s.recordedOf("app", "agent_branch_messages"), func(change DataChange) bool { return change.Op != ChangeDelete })
	s.Len(deletes, 2, "the copy's insert, emptied, and its delete, still recorded for the deployment the app is moving to")
	calls := s.recordedOf("app", "calls")
	s.Require().Len(calls, 1)
	s.Contains(string(calls[0].Payload), `"agent:one"`, "a call of the session is no copy")
}

func (s *StoreSuite) TestForgettingAUserAfterTheirSessionWasDeletedLeavesNoChangeOfItsCopies() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	s.opened("one", "app", s.base, nil)
	s.opened("two", "app", s.base, func(session *AgentSession) { session.UserID = "rlafleur" })
	other := hidden("m2", "second", "switch", s.base, "kept words")
	other.SessionID = "two"
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m1", "second", "switch", s.base, "forgotten words"), other}))
	s.Require().NotEmpty(s.payloadsNaming("app", "forgotten words"), "the copy's insert was recorded")

	s.Require().NoError(s.store.Sessions.Delete(s.ctx, "app", "one"))
	_, err := s.store.BranchMessages.DeleteUser(s.ctx, "app", "jlahey")
	s.Require().NoError(err)

	s.Empty(s.payloadsNaming("app", "forgotten words"))
	s.NotEmpty(s.payloadsNaming("app", "kept words"), "a copy in another session keeps its change")
}
