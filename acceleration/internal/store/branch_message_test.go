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
	// Sharded, a delete queues a job for every shard, then claims, reads, runs and finishes this one's run.
	deletes := 1
	if s.store.HasShards() {
		deletes = 5
	}
	s.withinQueryBudgets([]queryBudget{
		{"BranchMessages.UpsertMany", 1, 0, func() error { return s.store.BranchMessages.UpsertMany(s.ctx, messages) }},
		{"BranchMessages.Select", 1, 0, func() error {
			listed, err := s.store.BranchMessages.Select(s.ctx, "app", "branched", []string{"response-01", "response-02"})
			s.Len(listed, 2)
			return err
		}},
		{"BranchMessages.CountUser", 1, 0, func() error {
			counted, err := s.store.BranchMessages.CountUser(s.ctx, "app", "jlahey")
			s.EqualValues(30, counted)
			return err
		}},
		{"BranchMessages.DeleteUser", deletes, 0, func() error {
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

// inConversation opens session id of customer in conversation, pinned to app (zero for none),
// holding a copy of message.
func (s *StoreSuite) inConversation(id, customer, conversation string, app int64, message, text string) {
	s.opened(id, customer, s.base, func(session *AgentSession) {
		session.ConversationID = conversation
		session.StreamAppPK = app
	})
	copied := hidden(message, "second", "switch", s.base, text)
	copied.SessionID, copied.CustomerID = id, customer
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{copied}))
}

// copiesIn are the ids of the messages a session holds copies of.
func (s *StoreSuite) copiesIn(customer, session string) []string {
	found, err := s.store.BranchMessages.Select(s.ctx, customer, session, nil)
	s.Require().NoError(err)
	var ids []string
	for _, m := range found {
		ids = append(ids, m.OriginalMessageID)
	}
	return ids
}

func (s *StoreSuite) TestDeletingAMessageDeletesItsCopyInItsConversationAndApp() {
	s.inConversation("one", "app", "agent:chat-1", 7, "m1", "deleted words")
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m2", "second", "switch", s.base, "kept")}))
	s.inConversation("elsewhere", "app", "agent:chat-2", 7, "m1", "same id, other channel")
	s.inConversation("other-app", "app", "agent:chat-1", 8, "m1", "same channel, other app")

	deleted, err := s.store.BranchMessages.DeleteMessage(s.ctx, AppScope{App: 7}, "app", "agent:chat-1", "m1")

	s.Require().NoError(err)
	s.EqualValues(1, deleted)
	s.Equal([]string{"m2"}, s.copiesIn("app", "one"), "the session's other copies stay")
	s.Equal([]string{"m1"}, s.copiesIn("app", "elsewhere"), "a copy in another conversation stays")
	s.Equal([]string{"m1"}, s.copiesIn("app", "other-app"), "a copy in another app stays")
}

func (s *StoreSuite) TestDeletingAMessageNamingACustomerLeavesAnotherCustomersCopy() {
	s.inConversation("ours", "app", "agent:chat-1", 0, "m1", "ours")
	s.inConversation("theirs", "other", "agent:chat-1", 0, "m1", "theirs")

	deleted, err := s.store.BranchMessages.DeleteMessage(s.ctx, AppScope{App: 7, Unpinned: true}, "app", "agent:chat-1", "m1")

	s.Require().NoError(err)
	s.EqualValues(1, deleted)
	s.Empty(s.copiesIn("app", "ours"))
	s.Equal([]string{"m1"}, s.copiesIn("other", "theirs"))
}

// The deployment's own app is every customer's: a delete there names no customer, and reaches
// only the sessions written in that app.
func (s *StoreSuite) TestDeletingAMessageInTheDeploymentsAppDeletesTheCopiesOfItsSessions() {
	s.inConversation("unpinned", "app", "agent:chat-1", 0, "m1", "unpinned")
	s.inConversation("pinned", "other", "agent:chat-1", 7, "m1", "pinned here")
	s.inConversation("away", "other", "agent:chat-1", 8, "m1", "another app")

	deleted, err := s.store.BranchMessages.DeleteMessage(s.ctx, AppScope{App: 7, Unpinned: true}, "", "agent:chat-1", "m1")

	s.Require().NoError(err)
	s.EqualValues(2, deleted)
	s.Empty(s.copiesIn("app", "unpinned"))
	s.Empty(s.copiesIn("other", "pinned"))
	s.Equal([]string{"m1"}, s.copiesIn("other", "away"))
}

func (s *StoreSuite) TestDeletingAMessageNothingCopiedOrDeletingItAgainDeletesNothing() {
	s.inConversation("one", "app", "agent:chat-1", 7, "m1", "words")

	first, err := s.store.BranchMessages.DeleteMessage(s.ctx, AppScope{App: 7}, "app", "agent:chat-1", "m1")
	s.Require().NoError(err)
	again, err := s.store.BranchMessages.DeleteMessage(s.ctx, AppScope{App: 7}, "app", "agent:chat-1", "m1")
	s.Require().NoError(err)
	never, err := s.store.BranchMessages.DeleteMessage(s.ctx, AppScope{App: 7}, "app", "agent:chat-1", "never-copied")
	s.Require().NoError(err)

	s.EqualValues(1, first)
	s.Zero(again)
	s.Zero(never)
}

func (s *StoreSuite) TestDeletingAMessageEmptiesTheChangesRecordedOfItsCopy() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "app", time.Hour, time.Second))
	s.inConversation("one", "app", "agent:chat-1", 7, "m1", "deleted words")
	s.Require().NoError(s.store.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{hidden("m2", "second", "switch", s.base, "kept words")}))
	s.Require().NotEmpty(s.payloadsNaming("app", "deleted words"), "the copy's insert was recorded")

	_, err := s.store.BranchMessages.DeleteMessage(s.ctx, AppScope{App: 7}, "app", "agent:chat-1", "m1")

	s.Require().NoError(err)
	s.Empty(s.payloadsNaming("app", "deleted words"))
	s.NotEmpty(s.payloadsNaming("app", "kept words"), "another message's copy keeps its change")
}

func (s *StoreSuite) TestDeletingAMessageNeedsAConversationAndAMessage() {
	_, err := s.store.BranchMessages.DeleteMessage(s.ctx, AppScope{App: 7}, "app", "", "m1")
	s.Error(err)
	_, err = s.store.BranchMessages.DeleteMessage(s.ctx, AppScope{App: 7}, "app", "agent:chat-1", "")
	s.Error(err)
}

func (s *ShardedStoreSuite) TestDeletingAMessageDeletesItsCopiesOnEveryShardOnceEachRuns() {
	s.inConversation("here", "app", "agent:chat-1", 7, "m1", "here")
	s.Require().NoError(s.other.Sessions.Upsert(s.ctx, &AgentSession{ID: "there", CustomerID: "app", AgentName: "docs",
		State: SessionRunning, ConversationID: "agent:chat-1", StreamAppPK: 7, CreatedAt: s.base}))
	theirs := hidden("m1", "second", "switch", s.base, "there")
	theirs.SessionID = "there"
	s.Require().NoError(s.other.BranchMessages.UpsertMany(s.ctx, []AgentBranchMessage{theirs}))

	deleted, err := s.store.BranchMessages.DeleteMessage(s.ctx, AppScope{App: 7}, "app", "agent:chat-1", "m1")

	s.Require().NoError(err)
	s.EqualValues(1, deleted, "this shard's copy is deleted at once")
	left, err := s.other.BranchMessages.Select(s.ctx, "app", "there", nil)
	s.Require().NoError(err)
	s.Len(left, 1, "the other shard's copy is its own to delete")
	ran, err := s.other.ShardJobs.RunDue(s.ctx)
	s.Require().NoError(err)
	s.Equal(1, ran)
	left, err = s.other.BranchMessages.Select(s.ctx, "app", "there", nil)
	s.Require().NoError(err)
	s.Empty(left)
}
