//go:build integration

package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/scheduling"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// restartedNode is a router that never held the suite's conversations, as one started after a
// restart, or a peer that carries a session on after its router drained, or after the session's
// unwatched grace ended there: a session it carries on reads its conversation back from Chat
// (commandsIn), with nothing of the router that ran the turn kept in memory. It returns the
// router and its sessions.
func (s *TelnyxUnpaidSuite) restartedNode() (*Server, *session.Manager) {
	held := s.conversations
	own := conversation.NewForChats(conversation.StreamApps(s.stream))
	s.T().Cleanup(own.Close)
	s.conversations = own
	_, server, sessions := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler))
	s.conversations = held
	return server, sessions
}

// endedAfterItsTurn runs the turn of the first attempt at an occurrence of a task of user's on
// channel, due in the session opened, until the turn's reply has ended in state as Chat stores
// it, then stops the session. The attempt recorded no reason: it was cut short once it began the
// turn, as a router that drains or restarts cuts it. It returns the occurrence as the next
// attempt claims it.
func (s *TelnyxUnpaidSuite) endedAfterItsTurn(user *testClient, opened Session, channel, state string) store.ClaimedScheduledTask {
	claimed := store.ClaimedScheduledTask{Task: s.userTask(user, opened.Id, channel),
		Run: store.ScheduledTaskRun{ID: "run", ScheduledFor: time.Now().UTC()}}
	delivery := s.router.occurrenceDelivery(context.Background(), claimed)
	found, running := s.router.sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	s.Require().NoError(s.router.followUpWithin(s.ownerContext(context.Background(), delivery), found, delivery))
	s.Require().Eventually(func() bool {
		for _, stored := range s.chat.Stored(value(opened.ConversationId)) {
			custom, _ := stored["custom"].(map[string]any)
			runtime, _ := custom["support_runtime"].(map[string]any)
			if runtime["command_id"] == delivery.CommandID && runtime["state"] == state {
				return true
			}
		}
		return false
	}, deliveryWait, 10*time.Millisecond, "the turn's reply never reached Chat as "+state)
	status, body := user.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/stop", nil)
	s.Require().Equal(http.StatusNoContent, status, string(body))
	return claimed
}

// creditedAgain credits the organization, and waits until what costs money is admitted again: a
// session that ended may then be carried on.
func (s *TelnyxUnpaidSuite) creditedAgain() {
	s.usage.Credit(s.T(), s.RouterSuite.app.organization.ID, 10_000_000)
	s.Require().Eventually(func() bool {
		status, _ := s.serverClient.call(http.MethodPost, "/v1/search", SearchRequest{Query: "paid"})
		return status == http.StatusOK
	}, 10*time.Second, 20*time.Millisecond, "the credit is read")
}

// deliveredOn is the attempt at claimed on node, as a worker there makes it.
func (s *TelnyxUnpaidSuite) deliveredOn(node *Server, claimed store.ClaimedScheduledTask) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), deliveryWait)
	defer cancel()
	deliveredTo, _, err := node.DeliverScheduledTask(ctx, claimed)
	return deliveredTo, err
}

// A conversation task whose turn the model refused for money, and whose attempt was cut short
// before it read how the turn ended, fails under the refusal's code on the attempt after the
// session is carried on again, and is not tried again: the conversation opened again reads the
// refusal back. Before, the reopened conversation held no failure, and the run was delivered
// though the user got nothing (AI-3902).
func (s *TelnyxUnpaidSuite) TestAConversationTurnRefusedForMoneyBeforeAReopenFailsUnderTheMoney() {
	user, opened := s.spentAfterOpening()
	claimed := s.endedAfterItsTurn(user, opened, "conversation", "failed")
	s.creditedAgain()
	restarted, sessions := s.restartedNode()

	_, err := s.deliveredOn(restarted, claimed)

	var failure *scheduling.Failure
	s.Require().ErrorAs(err, &failure, "the turn the user got nothing from is not delivered")
	s.Equal(codeInsufficientCredits, failure.Reason)
	s.True(failure.Final, "a refusal for money is never retried")
	found, running := sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running, "the session was carried on on the restarted router")
	_, _, rerun := found.FollowUpReply(conversation.ReadBackPrefix + "run" + rerunSuffix)
	s.False(rerun, "the turn is not run again")
}

// A text channel's turn the model refused for money before the session was carried on again fails
// under the refusal's code, not as a reply that failed (AI-3902).
func (s *TelnyxUnpaidSuite) TestATextTurnRefusedForMoneyBeforeAReopenFailsUnderTheMoney() {
	user, opened := s.spentAfterOpening()
	claimed := s.endedAfterItsTurn(user, opened, "sms", "failed")
	s.creditedAgain()
	restarted, _ := s.restartedNode()

	_, err := s.deliveredOn(restarted, claimed)

	var failure *scheduling.Failure
	s.Require().ErrorAs(err, &failure)
	s.Equal(codeInsufficientCredits, failure.Reason)
	s.True(failure.Final, "a refusal for money is never retried")
}

// A conversation turn refused because the money could not be checked, before the session was
// carried on again, is tried again, and its retry runs the turn again once the check passes: the
// user gets the reply the first turn never gave (AI-3183, AI-3902).
func (s *TelnyxUnpaidSuite) TestAConversationTurnRefusedUncheckedBeforeAReopenRunsAgainOnItsRetry() {
	user, opened := s.uncheckableAfterOpening()
	claimed := s.endedAfterItsTurn(user, opened, "conversation", "failed")
	s.sharedAgain()
	restarted, sessions := s.restartedNode()

	_, err := s.deliveredOn(restarted, claimed)

	var failure *scheduling.Failure
	s.Require().ErrorAs(err, &failure, "the turn the user got nothing from is not delivered")
	s.Equal(codeSpendCheckUnavailable, failure.Reason)
	s.False(failure.Final, "the check passes again on its own")

	reason := scheduling.ReasonOf(err)
	claimed.Run.Error = &reason
	deliveredTo, err := s.deliveredOn(restarted, claimed)

	s.Require().NoError(err)
	s.Equal(store.ScheduledTaskDeliveredConversation, deliveredTo)
	found, running := sessions.Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: user.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	s.Require().Eventually(func() bool {
		state, said, _ := found.FollowUpReply(conversation.ReadBackPrefix + "run" + rerunSuffix)
		return state == "completed" && said != ""
	}, deliveryWait, 10*time.Millisecond, "the retry ran the turn again, and the user got a reply")
}

// A text channel's turn that failed for a reason that is not money before the session was carried
// on again still fails as a reply that failed: only a refusal for money is read back as one.
func (s *TelnyxUnpaidSuite) TestATextTurnFailedNotForMoneyBeforeAReopenFailsAsAReplyThatFailed() {
	user, opened := s.creditedOpening("failing/failing-model")
	claimed := s.endedAfterItsTurn(user, opened, "sms", "failed")
	restarted, _ := s.restartedNode()

	_, err := s.deliveredOn(restarted, claimed)

	var failure *scheduling.Failure
	s.Require().ErrorAs(err, &failure)
	s.Equal(scheduling.ReasonReplyFailed, failure.Reason)
	s.False(failure.Final)
}

// A text channel's turn that completed before the session was carried on again is texted as it
// was, without running again.
func (s *TelnyxUnpaidSuite) TestATextTurnCompletedBeforeAReopenIsTexted() {
	user, opened := s.creditedOpening("recites/recites-model")
	claimed := s.endedAfterItsTurn(user, opened, "sms", "completed")
	restarted, _ := s.restartedNode()

	deliveredTo, err := s.deliveredOn(restarted, claimed)

	s.Require().NoError(err)
	s.Equal("sms", deliveredTo)
}
