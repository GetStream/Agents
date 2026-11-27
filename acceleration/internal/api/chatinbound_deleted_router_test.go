//go:build integration

package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// deleted is a message.deleted of message in the channel in app, made at.
func (d *chatInbounds) deleted(app int64, message string, forMe bool, at time.Time) string {
	return fmt.Sprintf(`{
  "type": "message.deleted",
  "app_id": %d,
  "cid": "agent:%s",
  "channel_id": %q,
  "channel_type": "agent",
  "created_at": %q,
  "hard_delete": false,
  "deleted_for_me": %t,
  "message": {"id": %q, "type": "deleted", "text": "hidden words", "user": {"id": "sam", "name": "Sam"}}
}`, app, d.channelID, d.channelID, at.UTC().Format(time.RFC3339Nano), forMe, message)
}

// copied keeps a copy of message, as a rewind hid it, in a session of customer on the channel,
// pinned to app (zero for none), and answers the session's id.
func (d *chatInbounds) copied(router *RouterSuite, customer string, app int64, message string) string {
	ctx := context.Background()
	session := router.utils.uuid()
	router.Require().NoError(router.store.Sessions.Upsert(ctx, &store.AgentSession{ID: session, CustomerID: customer,
		AgentName: "docs", State: store.SessionRunning, ConversationID: "agent:" + d.channelID, StreamAppPK: app,
		CreatedAt: time.Now()}))
	router.Require().NoError(router.store.BranchMessages.UpsertMany(ctx, []store.AgentBranchMessage{{
		SessionID: session, OriginalMessageID: message, CustomerID: customer, SwitchID: "switch",
		ChatCreatedAt: time.Now(), AuthorID: "sam", Message: map[string]any{"text": "hidden words"},
	}}))
	return session
}

// copies are the ids of the messages a session holds copies of.
func (d *chatInbounds) copies(router *RouterSuite, customer, session string) []string {
	found, err := router.store.BranchMessages.Select(context.Background(), customer, session, nil)
	router.Require().NoError(err)
	var ids []string
	for _, m := range found {
		ids = append(ids, m.OriginalMessageID)
	}
	return ids
}

func (s *ChatInboundSuite) TestADeletedMessageDeliveredDeletesItsBranchCopy() {
	message := "message-" + s.utils.uuid()
	session := s.copied(&s.RouterSuite, s.customerID(), 0, message)
	kept := s.copied(&s.RouterSuite, s.customerID(), 0, "message-"+s.utils.uuid())

	s.Equal(http.StatusOK, s.delivers(&s.RouterSuite, s.deleted(suiteStreamApp, message, false, time.Now()), s.current))

	s.Empty(s.copies(&s.RouterSuite, s.customerID(), session))
	s.Len(s.copies(&s.RouterSuite, s.customerID(), kept), 1, "another message's copy stays")
	s.Zero(s.queued(&s.RouterSuite), "a delete is not queued for the worker")
}

// Deleted for one user only, the message is still in the channel, and so is its branch.
func (s *ChatInboundSuite) TestAMessageDeletedForOneUserKeepsItsBranchCopy() {
	message := "message-" + s.utils.uuid()
	session := s.copied(&s.RouterSuite, s.customerID(), 0, message)

	s.Equal(http.StatusOK, s.delivers(&s.RouterSuite, s.deleted(suiteStreamApp, message, true, time.Now()), s.current))

	s.Equal([]string{message}, s.copies(&s.RouterSuite, s.customerID(), session))
}

func (s *ChatInboundSuite) TestADeletedMessageNothingCopiedIsTaken() {
	s.Equal(http.StatusOK, s.delivers(&s.RouterSuite, s.deleted(suiteStreamApp, "message-"+s.utils.uuid(), false, time.Now()), s.current))
}

func (s *ChatInboundSuite) TestADeletedMessageHookedDeletesItsBranchCopy() {
	message := "message-" + s.utils.uuid()
	session := s.copied(&s.RouterSuite, s.customerID(), 0, message)

	body := s.deleted(suiteStreamApp, message, false, time.Now())
	s.Equal(http.StatusOK, s.hooked(&s.RouterSuite, "/v1/chat/hooks/stream", body, suiteStreamSecret))

	s.Empty(s.copies(&s.RouterSuite, s.customerID(), session))
}

// A delete that could not be done is a 503, which Stream delivers again.
func (s *ChatInboundSuite) TestADeletedMessageWhoseCopiesCouldNotBeDeletedIsMadeAgain() {
	message := "message-" + s.utils.uuid()
	session := s.copied(&s.RouterSuite, s.customerID(), 0, message)
	body := s.deleted(suiteStreamApp, message, false, time.Now())
	signed := signDelivery(s.current, "msg_"+s.utils.uuid(), time.Now(), body)

	s.Equal(http.StatusServiceUnavailable, s.deliversTo(&s.RouterSuite, s.routerOnAClosedStore(), body, signed))
	s.Equal(http.StatusOK, s.deliversTo(&s.RouterSuite, s.server.URL, body, signed), "the delivery made again")

	s.Empty(s.copies(&s.RouterSuite, s.customerID(), session))
}

func (s *ChatInboundAppSuite) TestADeletedMessageDeletesTheBranchCopyOfItsAppOnly() {
	message := "message-" + s.utils.uuid()
	own := s.copied(&s.RouterSuite, s.customerID(), s.appID(), message)
	other := "someone-" + s.utils.uuid()
	theirs := s.copied(&s.RouterSuite, other, s.appID()+1, message)

	s.Equal(http.StatusOK, s.delivers(&s.RouterSuite, s.deleted(s.appID(), message, false, time.Now()), s.current))

	s.Empty(s.copies(&s.RouterSuite, s.customerID(), own))
	s.Equal([]string{message}, s.copies(&s.RouterSuite, other, theirs), "another app's session on a channel of the same name keeps its copy")
}

// Stream delivers again for longer than a hook stays fresh, and a late delete still deletes.
func (s *ChatInboundAppSuite) TestADeletedMessageDeliveredLateStillDeletesItsBranchCopy() {
	message := "message-" + s.utils.uuid()
	session := s.copied(&s.RouterSuite, s.customerID(), s.appID(), message)

	body := s.deleted(s.appID(), message, false, time.Now().Add(-hookStaleAfter-time.Minute))
	s.Equal(http.StatusOK, s.delivers(&s.RouterSuite, body, s.current))

	s.Empty(s.copies(&s.RouterSuite, s.customerID(), session))
}

// Before the deployment knows its own app, a hooked delete could reach only unpinned sessions,
// so it is answered for Stream to make again, and deletes a pinned session's copy then.
func (s *ChatInboundSuite) TestADeletedMessageHookedBeforeTheRouterKnowsItsAppIsMadeAgain() {
	message := "message-" + s.utils.uuid()
	session := s.copied(&s.RouterSuite, s.customerID(), suiteStreamApp, message)
	router, deployment := s.routerLearningItsApp(s.chatInboundKeys)
	body := s.deleted(suiteStreamApp, message, false, time.Now())

	s.Equal(http.StatusServiceUnavailable, s.hookedAt(router, body))
	s.Equal([]string{message}, s.copies(&s.RouterSuite, s.customerID(), session))
	deployment.SetApp(suiteStreamApp)
	s.Equal(http.StatusOK, s.hookedAt(router, body))

	s.Empty(s.copies(&s.RouterSuite, s.customerID(), session))
}
