//go:build integration

package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/chatlog"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// Who may name a thread channel. Only the customer's backend, or the Router's own message
// hook (threadhooks.go), holds a session on one. An end user's device naming one, by its
// agent id or by any other way into a session, is answered exactly as for an id no thread
// channel has, so nothing it is told says the thread exists. A device names no agent id at
// all (errDeviceAgentID), so by its agent id it is refused alike. The tests below compare the
// two answers; SlackChannelSuite's own tests are the backend and the hook still working.

// TestADeviceNamingAThreadChannelsAgentIDIsRefusedAsForAnUnknownOne: a device's text session
// under a thread channel's agent id is refused exactly as one under an agent id no thread
// channel has.
func (s *SlackChannelSuite) TestADeviceNamingAThreadChannelsAgentIDIsRefusedAsForAnUnknownOne() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	unknown := conversation.ThreadChannelPrefix + s.utils.uuid()

	s.refusedAlike(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel},
		CreateSessionRequest{Agent: &s.config.Name, AgentId: &unknown})
}

// TestTheThreadIsAnsweredByTheRouterNotByADevicesSessionNamingIt: a device asking for a
// session under the thread channel's agent id is refused, and the person's message in the
// thread is still answered by the Router's own session, into the Slack thread.
func (s *SlackChannelSuite) TestTheThreadIsAnsweredByTheRouterNotByADevicesSessionNamingIt() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	s.refused(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel})

	s.Require().Equal(http.StatusOK, s.streamDelivers(channel, 0))

	s.Equal("Noted.", s.posted(1)[0].Text, "the reply leaves into the Slack thread")
}

// TestTheBackendsSessionOnAThreadChannelAnswersTheThread: the turn the inbound worker hands
// over finds the session the backend opened on the thread channel by its conversation, and it
// answers there; the Router opens none of its own, which would take the backend's over.
func (s *SlackChannelSuite) TestTheBackendsSessionOnAThreadChannelAnswersTheThread() {
	handOver := s.holdHandOffs()
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	held := s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel})

	handOver()

	s.Equal("Noted.", s.posted(1)[0].Text)
	s.Nil(s.serverClient.getSession(held.Id).ClosedAt, "the backend's session answered and is still running")
}

// TestADevicesCallNamingAThreadChannelWritesNoTranscriptThere: a device's call under a thread
// channel's agent id is refused as one under an unknown agent id, and writes nothing there.
func (s *SlackChannelSuite) TestADevicesCallNamingAThreadChannelWritesNoTranscriptThere() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	unknown := conversation.ThreadChannelPrefix + s.utils.uuid()

	s.refusedAlike(CreateSessionRequest{AgentId: &channel, StartVoice: pointerTo(true)},
		CreateSessionRequest{AgentId: &unknown, StartVoice: pointerTo(true)})

	s.False(s.transcribed.opened(chatlog.ChannelType+":"+channel), "the thread channel holds no transcript of the device's call")
}

// TestADevicesCallNamingAThreadChannelsAgentIDInUpperCaseWritesNoTranscriptThere.
func (s *SlackChannelSuite) TestADevicesCallNamingAThreadChannelsAgentIDInUpperCaseWritesNoTranscriptThere() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	upper, unknown := strings.ToUpper(channel), strings.ToUpper(conversation.ThreadChannelPrefix+s.utils.uuid())

	s.refusedAlike(CreateSessionRequest{AgentId: &upper, StartVoice: pointerTo(true)},
		CreateSessionRequest{AgentId: &unknown, StartVoice: pointerTo(true)})

	s.False(s.transcribed.opened(chatlog.ChannelType+":"+upper), "the thread channel's id in upper case holds no transcript")
}

// TestADevicesCallNamingAThreadChannelsAgentIDWithALeadingSpaceWritesNoTranscriptThere:
// Normalize keeps an agent id as given, and whether Stream Chat takes " thread-<uuid>" for
// the thread channel is unverified, so it is kept out as if it did.
func (s *SlackChannelSuite) TestADevicesCallNamingAThreadChannelsAgentIDWithALeadingSpaceWritesNoTranscriptThere() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	padded, unknown := " "+channel, " "+conversation.ThreadChannelPrefix+s.utils.uuid()

	s.refusedAlike(CreateSessionRequest{AgentId: &padded, StartVoice: pointerTo(true)},
		CreateSessionRequest{AgentId: &unknown, StartVoice: pointerTo(true)})

	s.False(s.transcribed.opened(chatlog.ChannelType+":"+padded), "the padded thread channel id holds no transcript")
}

// TestABackendNamingAThreadChannelInUpperCaseIsAnsweredAsBefore: the backend opens a thread
// channel by its exact id, as at base; any other spelling is any other agent id.
func (s *SlackChannelSuite) TestABackendNamingAThreadChannelInUpperCaseIsAnsweredAsBefore() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	upper := strings.ToUpper(channel)

	opened := s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &upper})

	s.Equal(upper, opened.AgentId)
	s.Equal("agent:"+opened.Id, value(opened.ConversationId))
}

// TestADevicesCallIsBarredFromAChannelWhoseThreadLinkCannotBeRead: when channel_threads
// cannot be read, whether the agent id is a thread channel is not known, and a device is kept
// out of it as if it were one.
func (s *SlackChannelSuite) TestADevicesCallIsBarredFromAChannelWhoseThreadLinkCannotBeRead() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	s.unreadableLink(channel)

	s.refused(CreateSessionRequest{AgentId: &channel, StartVoice: pointerTo(true)})

	s.False(s.transcribed.opened(chatlog.ChannelType + ":" + channel))
}

// TestAMessageInAChannelWhoseThreadLinkCannotBeReadIsAnsweredNowhere: the hook does not hand
// the thread's message to a session under the channel's agent id when it cannot tell whether
// the channel holds a thread.
func (s *SlackChannelSuite) TestAMessageInAChannelWhoseThreadLinkCannotBeReadIsAnsweredNowhere() {
	said := "and the deploy? " + s.utils.uuid()
	channel := s.answeredThread()
	s.unreadableLink(channel)
	s.refused(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel, StartVoice: pointerTo(true)})

	s.sentInThread(channel, said)

	s.Never(func() bool { return s.told(said) }, dropped, 20*time.Millisecond, "no session was told the thread's message")
	s.Len(s.slack.Posts(), 1, "the reply to the first message alone")
}

// TestABackendsCallNamingAThreadChannelWritesItsTranscriptIntoItsOwnConversation: the
// backend's call under a thread channel's agent id is a conversation of its own, like any
// other session.
func (s *SlackChannelSuite) TestABackendsCallNamingAThreadChannelWritesItsTranscriptIntoItsOwnConversation() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")

	created := s.serverClient.createSession(CreateSessionRequest{AgentId: &channel, StartVoice: pointerTo(true)})

	s.True(s.transcribed.opened(value(created.ConversationId)))
}

// TestADevicesSocketNamingAThreadChannelWritesNoTranscriptThere: the voice socket is a way
// into a session of its own (socketws.go), answered as POST /v1/agents/sessions is.
func (s *SlackChannelSuite) TestADevicesSocketNamingAThreadChannelWritesNoTranscriptThere() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	unknown := conversation.ThreadChannelPrefix + s.utils.uuid()

	named := s.socketRefusal(channel)
	guessed := s.socketRefusal(unknown)

	s.Equal(errDeviceAgentID.Message, named["error"])
	s.Equal(guessed, named)
	s.False(s.transcribed.opened(chatlog.ChannelType+":"+channel), "the thread channel holds no transcript of the device's socket")
}

// TestADeviceReadingAThreadChannelsSessionIsAnsweredAsForAnUnknownOne: the session the
// backend holds on a thread channel is, to a device, an id that does not exist.
func (s *SlackChannelSuite) TestADeviceReadingAThreadChannelsSessionIsAnsweredAsForAnUnknownOne() {
	held := s.heldOnThread()

	s.answeredAsUnknown(http.MethodGet, "/v1/agents/sessions/%s", held, nil)
}

// TestADeviceForkingAThreadChannelsSessionIsAnsweredAsForAnUnknownOne.
func (s *SlackChannelSuite) TestADeviceForkingAThreadChannelsSessionIsAnsweredAsForAnUnknownOne() {
	held := s.heldOnThread()

	s.answeredAsUnknown(http.MethodPost, "/v1/agents/sessions/%s/fork", held, ForkSessionRequest{})
}

// TestADeviceAnsweringInAThreadChannelsSessionIsAnsweredAsForAnUnknownOne: a response is
// how a chat that ended is reopened (sessionToAnswer), and how a running one is written in.
func (s *SlackChannelSuite) TestADeviceAnsweringInAThreadChannelsSessionIsAnsweredAsForAnUnknownOne() {
	held := s.heldOnThread()

	s.answeredAsUnknown(http.MethodPost, "/v1/agents/sessions/%s/responses", held, CreateResponseRequest{Text: "what did they say?"})
}

// TestADeviceListingAThreadChannelsResponsesIsAnsweredAsForAnUnknownSession.
func (s *SlackChannelSuite) TestADeviceListingAThreadChannelsResponsesIsAnsweredAsForAnUnknownSession() {
	held := s.heldOnThread()

	s.answeredAsUnknown(http.MethodGet, "/v1/agents/sessions/%s/responses", held, nil)
}

// TestADeviceWatchingAThreadChannelsSessionIsAnsweredAsForAnUnknownOne: the events socket.
func (s *SlackChannelSuite) TestADeviceWatchingAThreadChannelsSessionIsAnsweredAsForAnUnknownOne() {
	held := s.heldOnThread()

	_, watched := s.client.watch("/v1/agents/sessions/" + held + "/events")
	_, guessed := s.client.watch("/v1/agents/sessions/" + s.utils.uuid() + "/events")

	s.Equal(guessed, watched)
	s.Equal(http.StatusNotFound, watched)
}

// TestNoRequestReadsAThreadChannelsMessages: a device is refused this server-side operation
// first, as for a channel nobody holds, and the backend is refused the thread channel.
func (s *SlackChannelSuite) TestNoRequestReadsAThreadChannelsMessages() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	unknown := conversation.ThreadChannelPrefix + s.utils.uuid()

	s.Equal(s.readConversation(s.client, unknown, "/messages"), s.readConversation(s.client, channel, "/messages"))
	s.Equal(http.StatusForbidden, s.readConversation(s.client, channel, "/messages").status)
	read := s.readConversation(s.serverClient, channel, "/messages")
	s.Equal(http.StatusBadRequest, read.status)
	s.Equal("invalid conversation channel", read.error.Message)
}

// TestReadingACommandInAThreadChannelIsAnsweredAsForAnUnknownChannel.
func (s *SlackChannelSuite) TestReadingACommandInAThreadChannelIsAnsweredAsForAnUnknownChannel() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	unknown := conversation.ThreadChannelPrefix + s.utils.uuid()

	s.Equal(s.readConversation(s.client, unknown, "/commands/command-1"), s.readConversation(s.client, channel, "/commands/command-1"))
	s.Equal(http.StatusForbidden, s.readConversation(s.client, channel, "/commands/command-1").status)
	s.Equal(s.readConversation(s.serverClient, unknown, "/commands/command-1"), s.readConversation(s.serverClient, channel, "/commands/command-1"))
}

// TestTheBackendReadingAnUnknownChannelsMessagesIsAnsweredAsForAThreadChannel: Chat's refusal
// of a channel it does not have is no text of Chat's for the backend, and tells it nothing a
// thread channel's refusal does not.
func (s *SlackChannelSuite) TestTheBackendReadingAnUnknownChannelsMessagesIsAnsweredAsForAThreadChannel() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	unknown := conversation.ThreadChannelPrefix + s.utils.uuid()

	s.Equal(s.readConversation(s.serverClient, channel, "/messages"), s.readConversation(s.serverClient, unknown, "/messages"))
}

// TestChatFailingAConversationsReadIsAnswered500WithNothingOfChat: an outage is neither the
// caller's request to correct (a 400 with Chat's words) nor a command that does not exist.
func (s *SlackChannelSuite) TestChatFailingAConversationsReadIsAnswered500WithNothingOfChat() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	s.chat.Fail(http.MethodPost, "/query")
	defer s.chat.Heal(http.MethodPost, "/query")

	for _, what := range []string{"/messages", "/commands/command-1"} {
		read := s.readConversation(s.serverClient, channel, what)

		s.Equal(http.StatusInternalServerError, read.status, what)
		s.Equal("internal_error", read.error.Code, what)
		s.NotContains(read.error.Message, "boom", what)
	}
}

// TestABackendDoesNotForkAThreadChannelsRunningSession: a thread is everyone's in it
// (Spec.Shared), and a fork would carry their words into a conversation one caller owns,
// outside the thread and its connector rule. None is made, with its history or without.
func (s *SlackChannelSuite) TestABackendDoesNotForkAThreadChannelsRunningSession() {
	held := s.heldOnThread()

	carried, carriedFailure := s.serverClient.failure(http.MethodPost, "/v1/agents/sessions/"+held+"/fork", ForkSessionRequest{})
	fresh, freshFailure := s.serverClient.failure(http.MethodPost, "/v1/agents/sessions/"+held+"/fork",
		ForkSessionRequest{Messages: pointerTo(false)})

	s.Equal(http.StatusBadRequest, carried)
	s.Equal("a thread channel's conversation is not forked", carriedFailure)
	s.Equal(http.StatusBadRequest, fresh)
	s.Equal("a thread channel's conversation is not forked", freshFailure)
}

// TestABackendDoesNotForkAThreadChannelsSessionThatEnded: from its stored row either. The
// backend carries on in the thread by naming its agent id again.
func (s *SlackChannelSuite) TestABackendDoesNotForkAThreadChannelsSessionThatEnded() {
	held := s.heldOnThread()
	s.serverClient.stopSession(held)

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/sessions/"+held+"/fork",
		ForkSessionRequest{Messages: pointerTo(false)})

	s.Equal(http.StatusBadRequest, status)
	s.Equal("a thread channel's conversation is not forked", failure)
}

// TestABackendDoesNotReopenAThreadChannelsSessionThatEnded: a chat that ended is carried on
// under its id (sessionToAnswer), from the spec it ran on. A thread channel's is not, as at
// base: only its agent id opens one, after reading the thread's link again.
func (s *SlackChannelSuite) TestABackendDoesNotReopenAThreadChannelsSessionThatEnded() {
	held := s.heldOnThread()
	s.serverClient.stopSession(held)

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/sessions/"+held+"/responses",
		CreateResponseRequest{Text: "and the deploy?"})

	s.Equal(http.StatusBadRequest, status)
	s.Equal("invalid conversation channel", failure)
}

// TestASessionIsNotNamedAfterAThreadChannel: a session's channel is named after it, so an id a
// thread channel has is taken, by a device or the backend, in any case.
func (s *SlackChannelSuite) TestASessionIsNotNamedAfterAThreadChannel() {
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	members := s.chat.Members(channel)
	shouted := strings.ToUpper(channel)

	s.Equal(http.StatusConflict, s.client.do(http.MethodPost, "/v1/agents/sessions",
		CreateSessionRequest{Agent: &s.config.Name, Id: &channel}, nil))
	s.Equal(http.StatusConflict, s.serverClient.do(http.MethodPost, "/v1/agents/sessions",
		CreateSessionRequest{Agent: &s.config.Name, Id: &shouted}, nil))
	s.Equal(members, s.chat.Members(channel), "nobody was made a member of the thread")
}

// TestABackendOpensAThreadChannelAgainByItsAgentID: the one way in, after a session on it
// ended.
func (s *SlackChannelSuite) TestABackendOpensAThreadChannelAgainByItsAgentID() {
	s.holdHandOffs()
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	held := s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel})
	s.serverClient.stopSession(held.Id)

	again := s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel})

	s.Equal("agent:"+channel, value(again.ConversationId))
}

// TestWithNoThreadLinkedADevicesAgentIDIsRefusedAsAnyOther: with no channel_threads row, as
// staging runs with connectors off, an agent id in the thread- namespace is any agent id,
// which a device does not name, and no thread channel is opened for it.
func (s *SlackChannelSuite) TestWithNoThreadLinkedADevicesAgentIDIsRefusedAsAnyOther() {
	texted, called := conversation.ThreadChannelPrefix+s.utils.uuid(), conversation.ThreadChannelPrefix+s.utils.uuid()

	s.refused(CreateSessionRequest{Agent: &s.config.Name, AgentId: &texted})
	s.refused(CreateSessionRequest{AgentId: &called, StartVoice: pointerTo(true)})

	s.Zero(s.threadChannels())
}

// TestWithNoThreadLinkedABackendsAgentIDIsAnsweredAsBefore.
func (s *SlackChannelSuite) TestWithNoThreadLinkedABackendsAgentIDIsAnsweredAsBefore() {
	texted, called := conversation.ThreadChannelPrefix+s.utils.uuid(), conversation.ThreadChannelPrefix+s.utils.uuid()

	text := s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &texted})
	call := s.serverClient.createSession(CreateSessionRequest{AgentId: &called, StartVoice: pointerTo(true)})

	s.Equal(texted, text.AgentId)
	s.Equal("agent:"+text.Id, value(text.ConversationId))
	s.True(s.transcribed.opened(value(call.ConversationId)))
	s.Zero(s.threadChannels())
}

// heldOnThread is the id of a session the backend opened on a thread channel, once its row
// is stored, as a worker opens one (Dispatch.Conversation), before the agent was handed the
// thread's first message, once it answered it.
func (s *SlackChannelSuite) heldOnThread() string {
	handOver := s.holdHandOffs()
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	held := s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel})
	s.Require().Equal("agent:"+channel, value(held.ConversationId))
	s.Require().Eventually(func() bool {
		_, err := s.store.Sessions.Get(context.Background(), s.customerID(), held.Id)
		return err == nil
	}, settleFor, 20*time.Millisecond, "the session's row was never stored")
	handOver()
	s.posted(1)
	return held.Id
}

// unreadableLink leaves a thread channel's channel_threads row one the store cannot read:
// thread_parts holds a number where a string goes, so ChannelThread fails to scan it.
func (s *SlackChannelSuite) unreadableLink(channel string) {
	_, err := s.store.GlobalDB().ExecContext(context.Background(),
		`UPDATE channel_threads SET thread_parts = '{"ts": 1}'::jsonb WHERE channel_id = ?`, channel)
	s.Require().NoError(err)
	_, err = s.store.ChannelThreads.Get(context.Background(), channel)
	s.Require().Error(err)
	s.Require().NotErrorIs(err, store.ErrNoChannelThread)
}

// answeredAsUnknown asserts a device is answered for the session held as for an id no
// session has: not found, with the same error.
func (s *SlackChannelSuite) answeredAsUnknown(method, path, held string, body any) {
	thread := s.answerTo(s.client, method, strings.Replace(path, "%s", held, 1), body)
	guessed := s.answerTo(s.client, method, strings.Replace(path, "%s", s.utils.uuid(), 1), body)

	s.Equal(guessed, thread)
	s.Equal(http.StatusNotFound, thread.status)
}

// readConversation is what client is answered reading a conversation of the agent id.
func (s *SlackChannelSuite) readConversation(client *testClient, agentID, what string) reply {
	return s.answerTo(client, http.MethodGet,
		"/v1/agents/conversations/"+url.PathEscape("agent:"+agentID)+what+"?agent_id="+agentID, nil)
}

// reply is a request's status and the error it was answered with, if any.
type reply struct {
	status int
	error  ErrorDetail
}

// answerTo is what client is answered for one request.
func (s *SlackChannelSuite) answerTo(client *testClient, method, path string, body any) reply {
	status, payload := client.call(method, path, body)
	answered := reply{status: status}
	if status >= http.StatusBadRequest {
		var failure ErrorResponse
		s.Require().NoError(json.Unmarshal(payload, &failure), string(payload))
		answered.error = failure.Error
	}
	return answered
}

// TestAThreadTheMessageHookOpensIsSharedAndOffersNoCallerConnection: the message hook
// opens the thread channel's session through threadSession (threadhooks.go:177), which is
// what makes it shared. The caller's own binding is left out; a session named thread-…
// with no channel_threads row is not this path (spec_test.go TestASessionNamedLikeAThreadChannelIsNoThread).
func (s *SlackChannelSuite) TestAThreadTheMessageHookOpensIsSharedAndOffersNoCallerConnection() {
	s.config.Connectors = append(s.config.Connectors, store.ConnectorBinding{
		Name: "mine", ConnectorID: "slack_bot",
		Connection: store.ConnectionBinding{Type: "session"},
	})
	s.Require().NoError(s.store.AgentConfigs.Update(context.Background(), &s.config))
	s.holdHandOffs()
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	data, foundChannel := s.chat.Channel(channel)
	s.Require().True(foundChannel)
	custom, _ := data["custom"].(map[string]any)
	thread, err := s.store.ChannelThreads.Get(context.Background(), channel)
	s.Require().NoError(err)

	// The same event the message hook hands answerThread, which calls threadSession.
	event := messageEvent{ChannelType: chatlog.ChannelType, ChannelID: channel, ChannelCustom: custom}
	event.Message.Text = "is the build green?"
	created, err := s.router.threadSession(context.Background(), hookOrigin{
		customer: s.customerID(), app: thread.StreamAppPK,
	}, thread, event)
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		_, _ = s.manager.Close(created.ID(), session.OwnerOf(created.Spec()))
	})

	s.True(created.Spec().Shared())
	s.Equal(chatlog.ChannelType+":"+channel, created.Spec().ConversationID)
	for _, name := range created.Tools() {
		s.False(strings.HasPrefix(name, "mine__"), "a caller's own tool %s", name)
	}
	events, detach := created.Watch()
	defer detach()
	reason := ""
draining:
	for {
		select {
		case event := <-events:
			if unavailable, ok := event.(session.ConnectorUnavailable); ok && unavailable.Name == "mine" {
				reason = unavailable.Reason
			}
		default:
			break draining
		}
	}
	s.Equal("shared_session", reason)
}

func (s *SlackChannelSuite) TestAHookStartedReplyIsMarkedHookChat() {
	// No key opened the thread channel's session: the message hook did, for its sender.
	organization := s.RouterSuite.app.organization.ID
	s.Require().NoError(s.store.AppOrganizations.Upsert(context.Background(), &store.AppOrganization{AppID: s.customerID(), OrganizationID: organization}))
	s.Require().NoError(s.store.OrganizationTiers.Upsert(context.Background(), &store.OrganizationTier{OrganizationID: organization, Tier: "tier-a", SeenAt: time.Now()}))
	s.holdHandOffs()
	channel := s.messaged("U0000ALICE", "is the build green?", "1759740000.000100", "")
	data, foundChannel := s.chat.Channel(channel)
	s.Require().True(foundChannel)
	custom, _ := data["custom"].(map[string]any)
	thread, err := s.store.ChannelThreads.Get(context.Background(), channel)
	s.Require().NoError(err)
	event := messageEvent{ChannelType: chatlog.ChannelType, ChannelID: channel, ChannelCustom: custom}
	event.Message.Text = "is the build green?"
	event.Message.User.ID = "slack-alice"

	created, err := s.router.threadSession(context.Background(), hookOrigin{
		customer: s.customerID(), app: thread.StreamAppPK,
	}, thread, event)
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		_, _ = s.manager.Close(created.ID(), session.OwnerOf(created.Spec()))
	})
	_, err = created.Ask(context.Background(), "is the build green?")
	s.Require().NoError(err)

	var rows []store.Request
	s.Require().Eventually(func() bool {
		rows = nil
		err := s.store.ShardDB().NewSelect().Model(&rows).Where("session_id = ?", created.ID()).Scan(context.Background())
		return err == nil && len(rows) > 0
	}, settleFor, 25*time.Millisecond, "the reply recorded nothing")
	for _, row := range rows {
		s.Equal("hook:chat", row.APIKey)
		s.Equal("slack-alice", row.UserID, "the message's sender")
		s.Equal("tier-a", row.OrganizationTier, "the organization's last tier, which no request named")
		s.Equal("user", row.Attributes["caller"])
	}
}

// refused checks a device asking for a session as asked is refused for naming an agent id,
// and returns the message it was refused with.
func (s *SlackChannelSuite) refused(asked CreateSessionRequest) string {
	status, body := s.client.call(http.MethodPost, "/v1/agents/sessions", asked)
	s.Require().Equal(http.StatusForbidden, status, string(body))
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(body, &answered))
	s.Require().Equal(errDeviceAgentID.Message, answered.Error.Message)
	return answered.Error.Message
}

// refusedAlike checks a device naming a thread channel is refused exactly as one naming an id
// no thread channel has.
func (s *SlackChannelSuite) refusedAlike(named, guessed CreateSessionRequest) {
	s.Equal(s.refused(guessed), s.refused(named))
}

// socketRefusal is the frame a device's voice socket is refused with when it names agentID.
func (s *SlackChannelSuite) socketRefusal(agentID string) map[string]any {
	connection := s.client.opens("/v1/agents/socket")
	s.Require().NoError(connection.WriteJSON(map[string]any{
		"type": "start", "sample_rate": 24_000, "session": map[string]any{"agent_id": agentID},
	}))
	var answered map[string]any
	s.Require().NoError(connection.ReadJSON(&answered))
	s.Require().Equal("error", answered["type"], "the socket answered %v", answered)
	return answered
}

// openedTranscripts keeps the channel each voice session's transcript was opened for, named
// as cmd/router's transcriptFor names it (Spec.TranscriptChannel): the conversation's agent
// channel, else the agent id's, whatever other type of conversation_id was named.
type openedTranscripts struct {
	mu       sync.Mutex
	channels []string
}

func (o *openedTranscripts) open(_ context.Context, spec session.Spec, _ streamapp.Bound, _ *slog.Logger) (session.Transcript, error) {
	channel := spec.TranscriptChannel()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.channels = append(o.channels, channel)
	return keptNowhere{}, nil
}

func (o *openedTranscripts) opened(cid string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Contains(o.channels, cid)
}

// keptNowhere is a transcript that keeps nothing: the tests read which channel it was opened
// for, not what it was told.
type keptNowhere struct{}

func (keptNowhere) Start(context.Context) error { return nil }
func (keptNowhere) Record(agent.Event)          {}
func (keptNowhere) Reply(string)                {}
func (keptNowhere) Close()                      {}
