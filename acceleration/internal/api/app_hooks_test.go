//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/fakeprovider"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/bearer"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/sources/mcp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// AppHooksSuite delivers Stream's hooks to a router in app mode, where the gateway vouches
// for each app's own and what a hook names is acted on only within the app it came from.
type AppHooksSuite struct {
	RouterSuite

	channelID string
	apiKey    string
	// provider is the MCP server of a connector a config answering in a channel type binds,
	// and token an access token it issued (chat_channels_approval_test.go).
	provider *fakeprovider.Server
	token    string
	// logged is what the router logged, at debug and up.
	logged *lockedLog
}

func TestAppHooksSuite(t *testing.T) {
	runSuite(t, new(AppHooksSuite))
}

func (s *AppHooksSuite) SetupSuite() {
	s.appMode = true
	s.logged = &lockedLog{}
	s.logs = s.logged
	s.provider = fakeprovider.New(s.T(), fakeprovider.ClientCredentials)
	s.connectors = core.Registry{
		Schemes:     map[string]core.Scheme{bearer.Name: bearer.New()},
		ToolSources: map[string]core.ToolSource{mcp.Kind: mcp.New()},
	}
	s.connectorHTTP = s.provider.Client()
	// A call held for approval that a test leaves unanswered ends its turn this soon, not in
	// ten minutes, so the turn is over by the time the test's cleanup waits for it. A suite
	// built on this one may keep a wait of its own (ChannelTypeTurnSuite).
	if s.approvalWait == 0 {
		s.approvalWait = 10 * time.Second
	}
	s.RouterSuite.SetupSuite()
	// Sessions offer what the app's workers host, as cmd/router has them (chat_channels_hosted_test.go).
	s.manager.HostTools(s.dispatch)
	s.token = issuedToken(&s.RouterSuite, s.provider)
}

func (s *AppHooksSuite) SetupTest() {
	s.useApp(s.numberedApp(streamAppID()))
	s.apiKey = "own-key-" + s.utils.uuid()
	s.answerAs(s.apiKey, s.appID())
	s.forwarded(s.apiKey)
	s.channelID = "chat-" + s.utils.uuid()
}

func (s *AppHooksSuite) appPath(hook string) string {
	return hook + "/" + strconv.FormatInt(s.appID(), 10)
}

// hook delivers a body signed with a secret, naming the key given when there is one.
func (s *AppHooksSuite) hook(path, body, secret, apiKey string) int {
	request := s.request(path, body, sign(body, secret))
	if apiKey != "" {
		request.Header.Set(auth.APIKeyHeader, apiKey)
	}
	response, err := s.server.Client().Do(request)
	s.Require().NoError(err)
	s.Require().NoError(response.Body.Close())
	return response.StatusCode
}

// fromApp delivers a body from the customer's own app, as the gateway does once it checked
// the signature with the app's secret.
func (s *AppHooksSuite) fromApp(path, body string) int {
	request := s.request(path, body, sign(body, ownAppSecret))
	vouched(request, s.appID(), s.apiKey)
	response, err := s.server.Client().Do(request)
	s.Require().NoError(err)
	s.Require().NoError(response.Body.Close())
	return response.StatusCode
}

func (s *AppHooksSuite) config(customer string) string {
	config := store.AgentConfig{CustomerID: customer, Name: "config-" + s.utils.uuid(), Mode: "text"}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &config))
	return config.ID
}

// message is a message.new on the suite's channel, which names the config given.
func (s *AppHooksSuite) message(configID string) string {
	return s.messageIn(fmt.Sprintf(`{%q: %q}`, ConfigField, configID))
}

// messageIn is a message.new on the suite's channel, created with the custom data given.
func (s *AppHooksSuite) messageIn(custom string) string {
	return fmt.Sprintf(`{
  "type": "message.new",
  "channel_id": %q,
  "channel_type": "agent",
  "created_at": %q,
  "channel_custom": %s,
  "message": {"id": %q, "text": "is my order on its way?", "user": {"id": "sam", "name": "Sam"}}
}`, s.channelID, time.Now().UTC().Format(time.RFC3339Nano), custom, "message-"+s.utils.uuid())
}

// dispatchingSession opens a session for the app's backend on the suite's channel, whose agent
// leaves text to the customer's dispatch worker, and returns its id and what the worker is
// handed.
func (s *AppHooksSuite) dispatchingSession() (string, <-chan dispatch.Message) {
	config := store.AgentConfig{CustomerID: s.customerID(), Name: "config-" + s.utils.uuid(),
		Mode: store.AgentModeText, Models: store.AgentModels{LLM: "en-low-latency"}, Dispatch: store.AgentDispatch{Text: true}}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &config))
	opened := s.serverClient.createSession(CreateSessionRequest{AgentId: &s.channelID, Agent: &config.Name, Incognito: pointerTo(true)})
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	s.T().Cleanup(release)
	return opened.Id, worker.Messages()
}

func (s *AppHooksSuite) reaches(messages <-chan dispatch.Message) bool {
	select {
	case <-messages:
		return true
	case <-time.After(dropped):
		return false
	}
}

// Every visitor of a browser chat that talks to one named agent has a session under the same
// agent id, and a message hook finds a session by the agent id its channel is named after. A
// visitor's conversation is a channel of session commands, which no hook answers, and a session
// keeping its conversation takes no message from a hook, so what a visitor writes reaches only
// their own session, through its id, and nothing reaches the newest visitor's.
func (s *AppHooksSuite) TestAGuestsMessageReachesOnlyItsOwnSessionOfASharedAgentId() {
	config := store.AgentConfig{CustomerID: s.customerID(), Name: "config-" + s.utils.uuid(),
		Mode: store.AgentModeText, Models: store.AgentModels{LLM: "en-low-latency"}, Dispatch: store.AgentDispatch{Text: true}}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &config))
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 4})
	defer release()
	agentID := "agent-" + s.utils.uuid()
	alice, bob := s.data.createGuest(), s.data.createGuest()
	open := func(guest *testClient) Session {
		backend := s.signedIn(jwt.MapClaims{"server": true}, auth.AuthTypeServer, server, "")
		backend.header.Set(auth.UserHeader, guest.userID)
		return backend.createSession(CreateSessionRequest{AgentId: &agentID, Agent: &config.Name})
	}
	alices, bobs := open(alice), open(bob)
	s.Require().Equal(alices.AgentId, bobs.AgentId)

	for _, channel := range []string{value(alices.ConversationId), "agent:" + agentID} {
		s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/chat/hooks/stream"), s.writtenBy(alice.userID, channel, "is my order on its way?")))
		s.False(s.reaches(worker.Messages()), "a message in %s is handed to no session", channel)
	}
	s.Require().Equal(http.StatusAccepted, alice.do(http.MethodPost, "/v1/agents/sessions/"+alices.Id+"/responses",
		CreateResponseRequest{Text: "Is my order on its way?", RequestId: pointerTo(s.utils.uuid())}, nil))

	s.Require().Eventually(func() bool { return len(s.chat.Stored(value(alices.ConversationId))) >= 2 },
		settleFor, 20*time.Millisecond, "the turn and its reply are kept in the guest's own conversation")
	s.Empty(s.chat.Stored(value(bobs.ConversationId)), "nothing reaches the other guest's conversation")
}

// Defect (S1.1): a guest's incognito session of a config naming an agent id runs under that id,
// and a message hook hands a message in agent:<id> to the newest session under it that keeps no
// conversation, so any Chat user writing there drives the guest's session. An incognito session
// is given an agent id of its own instead, and the message reaches nobody's.
func (s *AppHooksSuite) TestAnotherUsersMessageReachesNoGuestsIncognitoSessionOfAConfigsAgent() {
	agentID := "agent-" + s.utils.uuid()
	config := store.AgentConfig{CustomerID: s.customerID(), Name: "config-" + s.utils.uuid(),
		Mode: store.AgentModeText, Models: store.AgentModels{LLM: "en-low-latency"}, Dispatch: store.AgentDispatch{Text: true},
		Identity: store.AgentIdentity{AgentID: agentID}}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &config))
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 4})
	defer release()
	alice, bob := s.data.createGuest(), s.data.createGuest()

	alices := alice.createSession(CreateSessionRequest{Agent: &config.Name, Incognito: pointerTo(true)})

	s.Regexp(routerMadeAgentID, alices.AgentId, "an incognito session runs under the config's agent id")
	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/chat/hooks/stream"), s.writtenBy(bob.userID, "agent:"+agentID, "is my order on its way?")))
	s.False(s.reaches(worker.Messages()), "another user's message is handed to the guest's incognito session")
}

// Defect (S1.1): another Chat user's message in agent:<the config's agent id> is put to the model
// of a guest's incognito session, beside what the guest said in it. It reaches no model.
func (s *AppHooksSuite) TestAnotherUsersMessageIsNeverAnsweredInAGuestsIncognitoSession() {
	agentID := "agent-" + s.utils.uuid()
	config := store.AgentConfig{CustomerID: s.customerID(), Name: "config-" + s.utils.uuid(),
		Mode: store.AgentModeText, Models: store.AgentModels{LLM: "vision/vision-model"},
		Identity: store.AgentIdentity{AgentID: agentID}}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &config))
	alice, bob := s.data.createGuest(), s.data.createGuest()
	alices := alice.createSession(CreateSessionRequest{Agent: &config.Name, Incognito: pointerTo(true)})
	ask := func(text string) {
		s.Require().Equal(http.StatusAccepted, alice.do(http.MethodPost, "/v1/agents/sessions/"+alices.Id+"/responses",
			CreateResponseRequest{Text: text, RequestId: pointerTo(s.utils.uuid())}, nil))
		s.Require().Eventually(func() bool { return s.wasAsked(text) }, settleFor, 10*time.Millisecond, "the guest's turn")
	}
	ask("a private note " + s.utils.uuid())
	bobs := "what did they write? " + s.utils.uuid()

	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/chat/hooks/stream"), s.writtenBy(bob.userID, "agent:"+agentID, bobs)))

	ask("and my order? " + s.utils.uuid())
	s.Never(func() bool { return s.wasAsked(bobs) }, dropped, 10*time.Millisecond, "another user's message is answered in the guest's session")
}

// writtenBy is the message.new for text the user wrote in the channel (type:id), with the custom
// data the fake Chat holds for it.
func (s *AppHooksSuite) writtenBy(user, cid, text string) string {
	custom := []byte("{}")
	if held, found := s.chat.Channel(cid); found && held["custom"] != nil {
		var err error
		custom, err = json.Marshal(held["custom"])
		s.Require().NoError(err)
	}
	return fmt.Sprintf(`{
  "type": "message.new", "channel_id": %q, "channel_type": "agent", "created_at": %q, "channel_custom": %s,
  "message": {"id": %q, "text": %q, "user": {"id": %q, "name": "Sam"}}
}`, strings.TrimPrefix(cid, "agent:"), time.Now().UTC().Format(time.RFC3339Nano), custom, "message-"+s.utils.uuid(), text, user)
}

func (s *AppHooksSuite) TestAMessageHookTheGatewayVouchedForIsDispatched() {
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	defer release()

	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/chat/hooks/stream"), s.message(s.config(s.customerID()))))

	s.True(s.reaches(worker.Messages()))
}

// The node the hook landed on recorded it as delivered before carrying it over, so the node
// with the worker must not take it for the same delivery arriving twice.
func (s *AppHooksSuite) TestAMessageHookCarriedToAnotherNodesWorkerIsDispatchedThere() {
	other := s.otherNodeDispatching(dispatch.NewPool())
	worker := s.serverClient.on(other).opens("/v1/dispatch")
	s.await(worker, "ready")

	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/chat/hooks/stream"), s.message(s.config(s.customerID()))))

	s.Equal(s.channelID, s.await(worker, "message")["channel_id"])
}

func (s *AppHooksSuite) TestAnAppsHookIsNeverCheckedWithTheDeploymentSecret() {
	// The deployment's secret signs only the deployment app's hooks.
	s.Equal(http.StatusUnauthorized, s.hook(s.appPath("/v1/chat/hooks/stream"),
		s.message(s.config(s.customerID())), suiteStreamSecret, ""))
}

func (s *AppHooksSuite) TestAnUnknownKeyWithAnAppSegmentNeverUsesTheDeploymentSecret() {
	s.Equal(http.StatusUnauthorized, s.hook(s.appPath("/v1/chat/hooks/stream"),
		s.message(s.config(s.customerID())), suiteStreamSecret, "nobody's-key"))
}

func (s *AppHooksSuite) TestAHookSignedByAnUnregisteredKeyUsesTheDeploymentSecret() {
	// A hook to the old path is the deployment app's, whatever key it names.
	s.Equal(http.StatusOK, s.hook("/v1/chat/hooks/stream", s.message(s.config(s.customerID())), suiteStreamSecret, "nobody's-key"))
}

func (s *AppHooksSuite) TestAChannelConfigIsHonouredOnlyForTheHooksApp() {
	// A channel in this app naming another customer's config starts nobody's agent.
	other := "someone-" + s.utils.uuid()
	somebodyElse, release := s.dispatch.Register(other, dispatch.Registration{Capacity: 1})
	defer release()

	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/chat/hooks/stream"), s.message(s.config(other))))

	s.False(s.reaches(somebodyElse.Messages()))
}

// A hook from the customer's own app is the customer's, so its session answers a channel there
// whatever the channel names as its customer.
func (s *AppHooksSuite) TestAMessageInTheCustomersOwnAppReachesItsSessionWhateverTheChannelNames() {
	opened, worker := s.dispatchingSession()
	body := s.messageIn(fmt.Sprintf(`{%q: "somebody-else"}`, conversation.CustomerField))

	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/chat/hooks/stream"), body))

	select {
	case message := <-worker:
		s.Equal(opened, message.SessionID)
	case <-time.After(settleFor):
		s.Fail("the customer's own session was handed nothing")
	}
}

// A channel in the deployment's app naming a customer who now acts in its own app does not
// reach that customer's session there: the channel names whose session answers, never which
// app's.
func (s *AppHooksSuite) TestAMessageInTheDeploymentAppNamingACustomerNowElsewhereReachesNotTheirSession() {
	_, worker := s.dispatchingSession()
	body := s.messageIn(fmt.Sprintf(`{%q: %q}`, conversation.CustomerField, s.customerID()))

	s.Equal(http.StatusOK, s.hook("/v1/chat/hooks/stream", body, suiteStreamSecret, ""))

	s.False(s.reaches(worker))
}

// A session in a customer's own app is not in the deployment's, so it makes no channel there
// one that two customers' sessions run on: the session in the deployment's app answers.
func (s *AppHooksSuite) TestAMessageInTheDeploymentAppIsAnsweredThereWhateverRunsInAnotherApp() {
	s.dispatchingSession()
	s.useApp(s.data.createApp())
	opened, worker := s.dispatchingSession()

	s.Equal(http.StatusOK, s.hook("/v1/chat/hooks/stream", s.messageIn("{}"), suiteStreamSecret, ""))

	select {
	case message := <-worker:
		s.Equal(opened, message.SessionID)
	case <-time.After(settleFor):
		s.Fail("the session in the deployment's app was handed nothing")
	}
}

// The same across nodes: the session in a customer's own app runs on one, the session in the
// deployment's app on another, and the message lands on a third. Only the second is in the app
// the message was written in, so it is carried there.
func (s *AppHooksSuite) TestAMessageInTheDeploymentAppReachesItsSessionOnAnotherNodeWhateverRunsInAnotherApp() {
	written := "is my order on its way? " + s.utils.uuid()
	s.serverClient.on(s.otherNode()).createSession(CreateSessionRequest{
		AgentId: &s.channelID, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true),
	})
	s.useApp(s.data.createApp())
	s.serverClient.on(s.otherNode()).createSession(CreateSessionRequest{
		AgentId: &s.channelID, Llm: pointerTo("vision/vision-model"), Incognito: pointerTo(true),
	})

	s.Equal(http.StatusOK, s.hook("/v1/chat/hooks/stream", writtenTo(s.channelID, `{}`, written), suiteStreamSecret, ""))

	s.Eventually(func() bool { return s.wasAsked(written) }, settleFor, 10*time.Millisecond,
		"the session in the deployment's app was asked nothing")
}

func (s *AppHooksSuite) TestAMessageInTheDeploymentAppIsNotDispatchedToACustomerNowElsewhere() {
	// A channel in the router's own app names this customer's config, but the customer
	// now acts in its own app, so a deployment-signed hook starts nothing of theirs.
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	defer release()

	s.Equal(http.StatusOK, s.hook("/v1/chat/hooks/stream", s.message(s.config(s.customerID())), suiteStreamSecret, ""))

	s.False(s.reaches(worker.Messages()))
}

func (s *AppHooksSuite) TestAReplayedMessageIsAnsweredOnce() {
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 2})
	defer release()
	body := s.message(s.config(s.customerID()))

	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/chat/hooks/stream"), body))
	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/chat/hooks/stream"), body))

	s.True(s.reaches(worker.Messages()))
	s.False(s.reaches(worker.Messages()), "the second delivery is the same message")
}

func (s *AppHooksSuite) TestAStaleCallEventIsIgnored() {
	e164 := s.utils.number()
	call := "phone-" + e164
	s.attach(e164, call, s.appID())
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	defer release()

	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/phone/hooks/stream"), s.ringing(call, time.Now().Add(-time.Hour))))

	select {
	case <-worker.Calls():
		s.Fail("an hour-old call was answered")
	case <-time.After(dropped):
	}
}

func (s *AppHooksSuite) TestAnArrivingCallIsDispatchedToTheNumbersOwnerInThatApp() {
	e164 := s.utils.number()
	call := "phone-" + e164
	s.attach(e164, call, s.appID())
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 2})
	defer release()

	s.Equal(http.StatusOK, s.hook("/v1/phone/hooks/stream", s.ringing(call, time.Now()), suiteStreamSecret, ""))
	select {
	case <-worker.Calls():
		s.Fail("the deployment app's hook rang a number attached in the customer's app")
	case <-time.After(dropped):
	}

	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/phone/hooks/stream"), s.ringing(call, time.Now())))
	select {
	case arrived := <-worker.Calls():
		s.Equal(call, arrived.CallID)
	case <-time.After(settleFor):
		s.Fail("the call never reached the worker")
	}
}

// attach is a number the customer holds, attached in the app given.
func (s *AppHooksSuite) attach(e164, call string, app int64) {
	ctx := context.Background()
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: e164, Vendor: "telnyx", Country: "US", CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(ctx, s.customerID(), e164, store.NumberAttachment{
		TrunkID: "trunk-" + s.utils.uuid(), StreamAppPK: app, CallType: "default", CallID: call,
	}))
}

// ringing is a call.session_started for a call, made at the time given.
func (s *AppHooksSuite) ringing(call string, at time.Time) string {
	return fmt.Sprintf(`{"type": "call.session_started", "call_cid": "default:%s", "session_id": %q,
  "created_at": %q, "call": {"id": %q, "type": "default", "custom": {}}}`,
		call, "session-"+s.utils.uuid(), at.UTC().Format(time.RFC3339Nano), call)
}

func (s *AppHooksSuite) TestACallOfTheSameNameInAnotherAppDoesNotHideThisOne() {
	// Call ids are only unique within an app: another customer attaching a line to a call of
	// the same name, in its own app, must not stop this customer's ringing.
	line := "line-" + s.utils.uuid()
	other := s.numberedApp(streamAppID())
	ctx := context.Background()
	otherNumber := s.utils.number()
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: otherNumber, Vendor: "telnyx", Country: "US", CustomerID: other.app.ID, PurchasedAt: time.Now().UTC(),
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(ctx, other.app.ID, otherNumber, store.NumberAttachment{
		TrunkID: "trunk-" + s.utils.uuid(), StreamAppPK: 7, CallType: "default", CallID: line,
	}))
	s.attach(s.utils.number(), line, s.appID())
	worker, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	defer release()

	s.Equal(http.StatusOK, s.fromApp(s.appPath("/v1/phone/hooks/stream"), s.ringing(line, time.Now())))

	select {
	case arrived := <-worker.Calls():
		s.Equal(line, arrived.CallID)
	case <-time.After(settleFor):
		s.Fail("the call never reached the worker")
	}
}
