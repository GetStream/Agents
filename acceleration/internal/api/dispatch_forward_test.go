//go:build integration

package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/shardmap"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ForwardedDispatchSuite covers hooks that land on a node with no worker connected for the
// customer, while one waits on another node.
//
// That is what a deployment of more than one router looks like: a worker's socket and
// Stream's hook each land wherever the load balancer sends them.
type ForwardedDispatchSuite struct {
	RouterSuite

	// other is the node the workers in a test connect to, and otherWorkers its pool.
	other        *httptest.Server
	otherWorkers *dispatch.Pool
	// elsewhere is how a session on a third node reaches the deployment's workers, and
	// elsewhereWorkers that node's pool, empty unless a test connects a worker to it.
	elsewhere        *node.Workers
	elsewhereWorkers *dispatch.Pool
}

func TestForwardedDispatchSuite(t *testing.T) {
	runSuite(t, new(ForwardedDispatchSuite))
}

func (s *ForwardedDispatchSuite) SetupSuite() {
	s.RouterSuite.SetupSuite()
	s.otherWorkers = dispatch.NewPool()
	s.other = s.otherNodeDispatching(s.otherWorkers)
	logger := slog.New(slog.DiscardHandler)
	third := httptest.NewUnstartedServer(nil)
	s.T().Cleanup(third.Close)
	s.elsewhereWorkers = dispatch.NewPool()
	s.elsewhere = s.deploymentWorkers(s.elsewhereWorkers, s.nodeDirectory(third, s.shard, logger), s.workBus(logger))
}

func (s *ForwardedDispatchSuite) TestARunningSessionsTextReachesAWorkerOnAnotherNode() {
	worker := s.waitsOnTheOtherNode()

	workerID, err := s.elsewhere.AssignMessage(context.Background(), s.customerID(),
		dispatch.Message{SessionID: "session-1", Text: "where is my order?"})
	s.Require().NoError(err)

	message := s.await(worker, "message")
	s.Equal("session-1", message["session_id"])
	s.Equal("where is my order?", message["text"])
	s.NotEmpty(workerID)
}

func (s *ForwardedDispatchSuite) TestThePhotosSentWithAMessageReachAWorkerOnAnotherNode() {
	worker := s.waitsOnTheOtherNode()

	_, err := s.elsewhere.AssignMessage(context.Background(), s.customerID(), dispatch.Message{
		ChannelID: "chat-1", Text: "what does this light mean?",
		Images: []dispatch.Image{{URL: "https://cdn.example/panel.jpg", MIME: "image/jpeg"}},
	})
	s.Require().NoError(err)

	message := s.await(worker, "message")
	s.Equal([]any{map[string]any{"url": "https://cdn.example/panel.jpg", "mime_type": "image/jpeg"}},
		message["images"])
}

func (s *ForwardedDispatchSuite) TestARunningSessionsTextWithNoWorkerAnywhereSaysSo() {
	_, err := s.elsewhere.AssignMessage(context.Background(), s.customerID(),
		dispatch.Message{SessionID: "session-1", Text: "where is my order?"})

	s.ErrorIs(err, dispatch.ErrNoWorkers)
}

// A worker that only hosts tools takes no message: a node holding one and no other used to
// keep the message and fail it, while a worker on another node waited for it.
func (s *ForwardedDispatchSuite) TestARunningSessionsTextPassesAToolHostForAWorkerOnAnotherNode() {
	_, release := s.elsewhereWorkers.Register(s.customerID(), dispatch.Registration{Capacity: 1, Handles: []dispatch.Kind{}})
	defer release()
	worker := s.waitsOnTheOtherNode()

	workerID, err := s.elsewhere.AssignMessage(context.Background(), s.customerID(),
		dispatch.Message{SessionID: "session-1", Text: "where is my order?"})
	s.Require().NoError(err)

	message := s.await(worker, "message")
	s.Equal("where is my order?", message["text"])
	s.NotEmpty(workerID)
}

func (s *ForwardedDispatchSuite) TestARunningSessionsTextGoesToAWorkerHereThatTakesMessages() {
	here, release := s.elsewhereWorkers.Register(s.customerID(), dispatch.Registration{Capacity: 1, Handles: []dispatch.Kind{dispatch.Messages}})
	defer release()
	elsewhere := s.waitsOnTheOtherNode()

	workerID, err := s.elsewhere.AssignMessage(context.Background(), s.customerID(),
		dispatch.Message{SessionID: "session-1", Text: "where is my order?"})
	s.Require().NoError(err)

	s.Equal(here.ID, workerID)
	s.sentNothing(elsewhere, "the message was carried to another node past a worker here")
}

// The error a node with only a tool host gave before it carried messages on, so a caller
// logging it reads the same when no node anywhere has a worker taking messages.
func (s *ForwardedDispatchSuite) TestARunningSessionsTextWithOnlyAToolHostAnywhereSaysNoWorkerHandlesMessages() {
	_, release := s.elsewhereWorkers.Register(s.customerID(), dispatch.Registration{Capacity: 1, Handles: []dispatch.Kind{}})
	defer release()

	_, err := s.elsewhere.AssignMessage(context.Background(), s.customerID(),
		dispatch.Message{SessionID: "session-1", Text: "where is my order?"})

	s.EqualError(err, "dispatch: no worker for "+s.customerID()+" handles message work")
}

func (s *ForwardedDispatchSuite) TestAToolHostedByAWorkerOnAnotherNodeIsOfferedAndRunHere() {
	agent := "agent-" + s.utils.uuid()
	worker := s.hostsOnTheOtherNode(agent)
	s.Require().Eventually(func() bool {
		offered, _ := s.elsewhere.HostedTools(s.customerID(), agent)
		return len(offered) == 1 && offered[0].Name == "investigate_sdk"
	}, settleFor, 10*time.Millisecond, "the tool was never offered here")

	type answer struct {
		output []llm.ContentPart
		err    error
	}
	answered := make(chan answer, 1)
	go func() {
		output, err := s.elsewhere.RunHosted(context.Background(), s.customerID(), agent,
			dispatch.ToolCall{ID: "call-1", SessionID: "session-1", Name: "investigate_sdk", Arguments: `{"sdk":"android"}`})
		answered <- answer{output, err}
	}()

	call := s.await(worker, "tool_call")
	s.Equal(`{"sdk":"android"}`, call["arguments"])
	s.Require().NoError(worker.WriteJSON(frame{"type": "tool_result", "id": call["id"], "output": "targetSdkVersion 35"}))

	select {
	case got := <-answered:
		s.Require().NoError(got.err)
		s.Equal("targetSdkVersion 35", llm.TextOf(got.output))
	case <-time.After(settleFor):
		s.Fail("the answer never came back from the other node")
	}
}

func (s *ForwardedDispatchSuite) TestAPictureAHostedToolAnswersWithComesBackFromAnotherNode() {
	agent := "agent-" + s.utils.uuid()
	worker := s.hostsOnTheOtherNode(agent)
	s.Require().Eventually(func() bool {
		offered, _ := s.elsewhere.HostedTools(s.customerID(), agent)
		return len(offered) == 1
	}, settleFor, 10*time.Millisecond, "the tool was never offered here")
	answered := make(chan []llm.ContentPart, 1)
	go func() {
		parts, err := s.elsewhere.RunHosted(context.Background(), s.customerID(), agent,
			dispatch.ToolCall{ID: "call-1", SessionID: "session-1", Name: "investigate_sdk"})
		s.NoError(err)
		answered <- parts
	}()
	call := s.await(worker, "tool_call")

	s.Require().NoError(worker.WriteJSON(frame{"type": "tool_result", "tool_call_id": call["id"], "output": []frame{
		{"type": "image_url", "image_url": frame{"url": roses}},
	}}))

	select {
	case parts := <-answered:
		s.Require().Len(parts, 1)
		s.Require().NotNil(parts[0].Image, "the picture was lost between nodes")
		s.NotEmpty(parts[0].Image.Data)
	case <-time.After(settleFor):
		s.Fail("the answer never came back from the other node")
	}
}

func (s *ForwardedDispatchSuite) TestACallOnAnotherNodeTheSessionGivesUpOnIsCancelledOnTheWorker() {
	agent := "agent-" + s.utils.uuid()
	worker := s.hostsOnTheOtherNode(agent)
	s.Require().Eventually(func() bool {
		offered, _ := s.elsewhere.HostedTools(s.customerID(), agent)
		return len(offered) == 1
	}, settleFor, 10*time.Millisecond, "the tool was never offered here")
	ctx, giveUp := context.WithCancel(context.Background())
	defer giveUp()
	go func() {
		_, _ = s.elsewhere.RunHosted(ctx, s.customerID(), agent,
			dispatch.ToolCall{ID: "call-1", SessionID: "session-1", Name: "investigate_sdk"})
	}()
	s.await(worker, "tool_call")

	giveUp()

	cancel := s.await(worker, "tool_cancel")
	s.Equal("call-1", cancel["id"])
}

func (s *ForwardedDispatchSuite) TestAToolIsNoLongerOfferedOnceTheWorkerHostingItLeaves() {
	agent := "agent-" + s.utils.uuid()
	worker := s.hostsOnTheOtherNode(agent)
	s.Require().Eventually(func() bool {
		offered, _ := s.elsewhere.HostedTools(s.customerID(), agent)
		return len(offered) == 1
	}, settleFor, 10*time.Millisecond)

	s.Require().NoError(worker.Close())

	s.Eventually(func() bool {
		offered, _ := s.elsewhere.HostedTools(s.customerID(), agent)
		return len(offered) == 0
	}, settleFor, 10*time.Millisecond, "a tool nobody runs any more is still offered")
}

func (s *ForwardedDispatchSuite) TestAToolHostedForAnotherCustomerIsNotOffered() {
	agent := "agent-" + s.utils.uuid()
	another := s.data.backendOfAnotherApp()
	worker := s.waitsOnTheOtherNodeAs(another)
	s.host(worker, another, agent)

	offered, _ := s.elsewhere.HostedTools(s.customerID(), agent)
	s.Empty(offered)
}

func (s *ForwardedDispatchSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *ForwardedDispatchSuite) TestACallArrivingWhereNoWorkerWaitsReachesTheWorkerOnAnotherNode() {
	callID := s.holdsANumber()
	worker := s.waitsOnTheOtherNode()

	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", arrivingCall("default:"+callID)))

	call := s.await(worker, "call")
	s.Equal(callID, call["call_id"])
}

func (s *ForwardedDispatchSuite) TestACallIsAnsweredByAWorkerOnTheNodeItArrivedAtWhenThereIsOne() {
	callID := s.holdsANumber()
	elsewhere := s.waitsOnTheOtherNode()
	here, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	defer release()

	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", arrivingCall("default:"+callID)))

	select {
	case call := <-here.Calls():
		s.Equal(callID, call.CallID)
	case <-time.After(settleFor):
		s.Fail("the call never reached the worker on the node it arrived at")
	}
	s.sentNothing(elsewhere, "the call was carried to another node as well")
}

func (s *ForwardedDispatchSuite) TestACallArrivingWhereOnlyAWorkerForMessagesWaitsReachesTheWorkerOnAnotherNode() {
	callID := s.holdsANumber()
	_, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1, Handles: []dispatch.Kind{dispatch.Messages}})
	defer release()
	worker := s.waitsOnTheOtherNode()

	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", arrivingCall("default:"+callID)))

	call := s.await(worker, "call")
	s.Equal(callID, call["call_id"])
}

func (s *ForwardedDispatchSuite) TestACallIsNotCarriedToAnotherCustomersWorker() {
	callID := s.holdsANumber()
	somebodyElse := s.waitsOnTheOtherNodeAs(s.data.backendOfAnotherApp())

	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", arrivingCall("default:"+callID)))

	s.sentNothing(somebodyElse, "a call was carried to somebody else's worker")
}

func (s *ForwardedDispatchSuite) TestAMessageArrivingWhereNoWorkerWaitsReachesTheWorkerOnAnotherNode() {
	channel, config := s.textChannel()
	worker := s.waitsOnTheOtherNode()

	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", arrivingMessage(channel, config)))

	message := s.await(worker, "message")
	s.Equal(channel, message["channel_id"])
	s.Equal(config.ID, message["config_id"])
}

// The node a message lands on may hold only a worker hosting tools, which takes no message,
// while the customer's worker taking messages waits on another (AI-2719).
func (s *ForwardedDispatchSuite) TestAMessageArrivingWhereOnlyAToolHostWaitsReachesTheWorkerOnAnotherNode() {
	channel, config := s.textChannel()
	_, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1, Handles: []dispatch.Kind{}})
	defer release()
	worker := s.waitsOnTheOtherNode()

	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", arrivingMessage(channel, config)))

	message := s.await(worker, "message")
	s.Equal(channel, message["channel_id"])
}

func (s *ForwardedDispatchSuite) TestAMessageIsAnsweredByAWorkerOnTheNodeItArrivedAtThatTakesMessages() {
	channel, config := s.textChannel()
	here, release := s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1, Handles: []dispatch.Kind{dispatch.Messages}})
	defer release()
	elsewhere := s.waitsOnTheOtherNode()

	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", arrivingMessage(channel, config)))

	select {
	case message := <-here.Messages():
		s.Equal(channel, message.ChannelID)
	case <-time.After(settleFor):
		s.Fail("the message never reached the worker on the node it arrived at")
	}
	s.sentNothing(elsewhere, "the message was carried to another node as well")
}

// textChannel is a channel for the suite's app with a text agent's config declared on it.
func (s *ForwardedDispatchSuite) textChannel() (string, store.AgentConfig) {
	config := store.AgentConfig{CustomerID: s.customerID(), Name: "config-" + s.utils.uuid(), Mode: "text"}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &config))
	return "chat-" + s.utils.uuid(), config
}

// arrivingMessage is Stream's hook for a message written in channel to config's agent.
func arrivingMessage(channel string, config store.AgentConfig) string {
	return fmt.Sprintf(`{
  "type": "message.new",
  "channel_id": %q,
  "channel_type": "agent",
  "channel_custom": {%q: %q},
  "message": {"id": "message-1", "text": "is anybody there?", "user": {"id": "sam", "name": "Sam"}}
}`, channel, ConfigField, config.ID)
}

// holdsANumber gives the suite's app a number attached to a call, and returns the call.
func (s *ForwardedDispatchSuite) holdsANumber() string {
	ctx := context.Background()
	e164 := s.utils.number()
	callID := "phone-" + e164
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: e164, Vendor: "telnyx", Country: "US", CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(ctx, s.customerID(), e164,
		store.NumberAttachment{TrunkID: "trunk-" + s.utils.uuid(), CallType: "default", CallID: callID}))
	return callID
}

// waitsOnTheOtherNode connects a worker for the suite's app to the other node.
func (s *ForwardedDispatchSuite) waitsOnTheOtherNode() *websocket.Conn {
	return s.waitsOnTheOtherNodeAs(s.serverClient)
}

func (s *ForwardedDispatchSuite) waitsOnTheOtherNodeAs(client *testClient) *websocket.Conn {
	connection := client.on(s.other).opens("/v1/dispatch")
	s.await(connection, "ready")
	return connection
}

// hostsOnTheOtherNode connects a worker for the suite's app to the other node, hosting one
// tool for an agent.
func (s *ForwardedDispatchSuite) hostsOnTheOtherNode(agent string) *websocket.Conn {
	worker := s.waitsOnTheOtherNode()
	s.host(worker, s.serverClient, agent)
	return worker
}

// host has a worker for the app client acts for host one tool for an agent of that app.
func (s *RouterSuite) host(worker *websocket.Conn, client *testClient, agent string) {
	client.agentNamed(agent)
	s.Require().NoError(worker.WriteJSON(frame{
		"type": "host_tools", "agent_id": agent,
		"tools": []frame{{"name": "investigate_sdk", "description": "Read SDK source"}},
	}))
	s.await(worker, "hosting")
}

// sentNothing fails when a worker is sent anything but pongs before the hook could have
// reached it.
func (s *RouterSuite) sentNothing(connection *websocket.Conn, complaint string) {
	s.Require().NoError(connection.SetReadDeadline(time.Now().Add(dropped)))
	for {
		var received frame
		err := connection.ReadJSON(&received)
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			return
		}
		s.Require().NoError(err)
		if received["type"] != "pong" {
			s.Failf(complaint, "the worker was sent %v", received["type"])
			return
		}
	}
}

// ShardedDispatchSuite covers which shard a dispatch worker registers in and which sessions
// reach it: the suite's node is in shard eu, the home node in shard us, the home shard, and
// shard ca of the Canadian geography has no node up.
type ShardedDispatchSuite struct {
	RouterSuite

	// home is the node of the home shard, homeWorkers its pool, and homeReach how a session
	// on it reaches the deployment's workers.
	home        *httptest.Server
	homeWorkers *dispatch.Pool
	homeReach   *node.Workers
	// euReach is how a session on the suite's node reaches them, its pool the suite's own.
	euReach *node.Workers
}

func TestShardedDispatchSuite(t *testing.T) {
	runSuite(t, new(ShardedDispatchSuite))
}

func (s *ShardedDispatchSuite) SetupSuite() {
	s.shard, s.otherShard = "eu", "us"
	s.shardRouting = shardmap.Map{Home: "us", Residency: map[string][]string{"us": {"us"}, "eu": {"eu"}, "ca": {"ca"}}}
	s.RouterSuite.SetupSuite()
	logger := slog.New(slog.DiscardHandler)
	s.homeWorkers = dispatch.NewPool()
	s.home, _, _ = s.otherNodeHolding(s.homeWorkers, s.sealer, logger, func(o *Options) { s.homeReach = o.Workers })
	s.euReach = s.deploymentWorkers(s.dispatch, s.router.directory, s.workBus(logger))
}

func (s *ShardedDispatchSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *ShardedDispatchSuite) TestAWorkerOfAClosestAppLandingOutsideTheHomeShardRegistersThere() {
	s.await(s.serverClient.opens("/v1/dispatch"), "ready")

	s.Len(s.homeWorkers.Workers(s.customerID()), 1, "the worker is not in the home shard")
	s.Empty(s.dispatch.Workers(s.customerID()), "the worker registered where it landed")
}

func (s *ShardedDispatchSuite) TestAWorkerOfAnEUOnlyAppLandingInTheEUStaysThere() {
	s.keptIn(shardmap.PolicyEUOnly)

	s.await(s.serverClient.opens("/v1/dispatch"), "ready")

	s.Len(s.dispatch.Workers(s.customerID()), 1, "the worker left the EU")
	s.Empty(s.homeWorkers.Workers(s.customerID()))
}

func (s *ShardedDispatchSuite) TestAWorkerOfAnEUOnlyAppLandingInTheHomeShardRegistersInTheEU() {
	s.keptIn(shardmap.PolicyEUOnly)

	s.await(s.serverClient.on(s.home).opens("/v1/dispatch"), "ready")

	s.Len(s.dispatch.Workers(s.customerID()), 1, "the worker is not in the EU")
	s.Empty(s.homeWorkers.Workers(s.customerID()), "the worker registered outside the EU")
}

// Registered anywhere else, the calls it took would leave the geography.
func (s *ShardedDispatchSuite) TestAWorkerOfAnAppKeptWhereNoNodeIsUpIsRefused() {
	s.keptIn(shardmap.PolicyCanadaOnly)

	status, message := s.serverClient.refusal("/v1/dispatch")

	s.Equal(http.StatusServiceUnavailable, status)
	s.Equal(errWorkerShardDown.Message, message)
	s.Empty(s.dispatch.Workers(s.customerID()))
	s.Empty(s.homeWorkers.Workers(s.customerID()))
}

// The worker landed in the EU and was carried home, and sessions in either shard run its tools.
func (s *ShardedDispatchSuite) TestASessionOfAClosestAppInAnyShardReachesTheHomeWorker() {
	agent := "agent-" + s.utils.uuid()
	worker := s.serverClient.opens("/v1/dispatch")
	s.await(worker, "ready")
	s.host(worker, s.serverClient, agent)

	for shard, reach := range map[string]*node.Workers{"eu": s.euReach, "us": s.homeReach} {
		s.Require().Eventually(func() bool {
			offered, _ := reach.HostedTools(s.customerID(), agent)
			return len(offered) == 1
		}, settleFor, 10*time.Millisecond, "the tool was never offered in %s", shard)
		answered := make(chan error, 1)
		go func() {
			_, err := reach.RunHosted(context.Background(), s.customerID(), agent,
				dispatch.ToolCall{ID: "call-" + shard, SessionID: "session-1", Name: "investigate_sdk"})
			answered <- err
		}()
		call := s.await(worker, "tool_call")
		s.Require().NoError(worker.WriteJSON(frame{"type": "tool_result", "id": call["id"], "output": "done"}))
		select {
		case err := <-answered:
			s.NoError(err, shard)
		case <-time.After(settleFor):
			s.Failf("the answer never came back", "to %s", shard)
		}
	}
}

func (s *ShardedDispatchSuite) TestASessionOfAnEUOnlyAppReachesAWorkerInTheEU() {
	s.keptIn(shardmap.PolicyEUOnly)
	agent := "agent-" + s.utils.uuid()
	worker := s.serverClient.opens("/v1/dispatch")
	s.await(worker, "ready")
	s.host(worker, s.serverClient, agent)

	offered, _ := s.euReach.HostedTools(s.customerID(), agent)
	s.Require().Len(offered, 1)
	go func() {
		_, _ = s.euReach.RunHosted(context.Background(), s.customerID(), agent,
			dispatch.ToolCall{ID: "call-1", SessionID: "session-1", Name: "investigate_sdk"})
	}()
	s.Equal("call-1", s.await(worker, "tool_call")["id"])
}

// A worker connected in the US before the app was kept in the EU is reached by no session of
// it in the EU: not for its tools, nor for a message.
func (s *ShardedDispatchSuite) TestASessionOfAnEUOnlyAppInTheEUDoesNotReachAWorkerInTheUS() {
	agent, worker := s.hostedInTheUSUntilKeptInTheEU()

	offered, _ := s.euReach.HostedTools(s.customerID(), agent)
	s.Empty(offered, "a tool run in the US is offered to an EU-only session")
	_, err := s.euReach.RunHosted(context.Background(), s.customerID(), agent,
		dispatch.ToolCall{ID: "call-1", SessionID: "session-1", Name: "investigate_sdk"})
	s.ErrorAs(err, &dispatch.NoWorkerError{})
	_, err = s.euReach.AssignMessage(context.Background(), s.customerID(),
		dispatch.Message{SessionID: "session-1", Text: "where is my order?"})
	s.ErrorIs(err, dispatch.ErrNoWorkers)
	s.sentNothing(worker, "an EU-only session's work reached a worker in the US")
}

// Nor by one opened in the US, which no session of an EU-only app should be, on the node the
// worker is connected to.
func (s *ShardedDispatchSuite) TestASessionOfAnEUOnlyAppInTheUSDoesNotUseAWorkerThere() {
	agent, worker := s.hostedInTheUSUntilKeptInTheEU()

	offered, _ := s.homeReach.HostedTools(s.customerID(), agent)
	s.Empty(offered, "a tool run in the US is offered to an EU-only session")
	_, err := s.homeReach.RunHosted(context.Background(), s.customerID(), agent,
		dispatch.ToolCall{ID: "call-1", SessionID: "session-1", Name: "investigate_sdk"})
	s.ErrorAs(err, &dispatch.NoWorkerError{})
	_, err = s.homeReach.AssignMessage(context.Background(), s.customerID(),
		dispatch.Message{SessionID: "session-1", Text: "where is my order?"})
	s.ErrorIs(err, dispatch.ErrNoWorkers)
	s.sentNothing(worker, "an EU-only session's work reached a worker in the US")
}

// hostedInTheUSUntilKeptInTheEU has a worker of the test's app, closest, connect in the home
// shard and host a tool for an agent, the EU see it offered, and then the app kept in the EU.
func (s *ShardedDispatchSuite) hostedInTheUSUntilKeptInTheEU() (string, *websocket.Conn) {
	agent := "agent-" + s.utils.uuid()
	worker := s.serverClient.on(s.home).opens("/v1/dispatch")
	s.await(worker, "ready")
	s.host(worker, s.serverClient, agent)
	s.Require().Eventually(func() bool {
		offered, _ := s.euReach.HostedTools(s.customerID(), agent)
		return len(offered) == 1
	}, settleFor, 10*time.Millisecond, "the tool was never offered in the EU")
	s.keptIn(shardmap.PolicyEUOnly)
	return agent, worker
}

// keptIn sets the test's app's routing policy, and waits until every node reads it.
func (s *ShardedDispatchSuite) keptIn(policy shardmap.Policy) {
	ctx := context.Background()
	s.Require().NoError(s.store.Apps.SetRoutingPolicy(ctx, s.customerID(), policy))
	s.Require().Eventually(func() bool {
		here, errHere := s.store.Apps.RoutingPolicy(ctx, s.customerID(), "")
		there, errThere := s.otherStore.Apps.RoutingPolicy(ctx, s.customerID(), "")
		return errHere == nil && errThere == nil && here == policy && there == policy
	}, settleFor, 10*time.Millisecond, "the routing policy was never read back")
}

// HomeShardDownDispatchSuite is a deployment whose home shard, us, has no node up.
type HomeShardDownDispatchSuite struct {
	RouterSuite
}

func TestHomeShardDownDispatchSuite(t *testing.T) {
	runSuite(t, new(HomeShardDownDispatchSuite))
}

func (s *HomeShardDownDispatchSuite) SetupSuite() {
	s.shard = "eu"
	s.shardRouting = shardmap.Map{Home: "us"}
	s.RouterSuite.SetupSuite()
}

// A worker refused would leave the app's calls unanswered everywhere; one where it landed is
// still reached from every shard.
func (s *HomeShardDownDispatchSuite) TestAWorkerOfAClosestAppRegistersWhereItLanded() {
	s.useApp(s.data.createApp())

	s.await(s.serverClient.opens("/v1/dispatch"), "ready")

	s.Len(s.dispatch.Workers(s.customerID()), 1)
}
