//go:build integration

package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/shardmap"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// routingPolicies is the deployment the routing policy suites run: the suite's node in shard
// here, outside every geography; the home and the EU in shard there; Canada's first shard one
// no node is in, then there; and no shard in the US.
var routingPolicies = shardmap.Map{Home: "there", Residency: map[string][]string{
	shardmap.GeographyEU:     {"there"},
	shardmap.GeographyCanada: {nowhere, "there"},
}}

// routingPolicyHarness is what the routing policy suites share: an app of its own and a worker
// of it waiting on the suite's node, so a hook it reaches says it was answered here.
type routingPolicyHarness struct {
	RouterSuite

	here *dispatch.Worker
}

func (s *routingPolicyHarness) setUp() {
	s.shard, s.otherShard = "here", "there"
	s.shardRouting = routingPolicies
	s.RouterSuite.SetupSuite()
}

func (s *routingPolicyHarness) SetupTest() {
	s.useApp(s.data.createApp())
	var release func()
	s.here, release = s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	s.T().Cleanup(release)
}

// appPolicy sets the test's app's routing policy.
func (s *routingPolicyHarness) appPolicy(policy shardmap.Policy) {
	s.Require().NoError(s.store.Apps.SetRoutingPolicy(context.Background(), s.customerID(), policy))
}

// agent is a text agent of the test's app under policy, empty for its app's.
func (s *routingPolicyHarness) agent(policy shardmap.Policy) store.AgentConfig {
	config := store.AgentConfig{CustomerID: s.customerID(), Name: "config-" + s.utils.uuid(), Mode: store.AgentModeText,
		Models: store.AgentModels{LLM: "en-low-latency"}, RoutingPolicy: string(policy)}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &config))
	return config
}

// policySession is an incognito session of the agent config, none for nil.
func policySession(config *store.AgentConfig) CreateSessionRequest {
	request := CreateSessionRequest{Incognito: pointerTo(true)}
	if config != nil {
		request.ConfigId = &config.ID
	}
	return request
}

// refusedWith is the code a request of the app's backend was refused with, which must be a 503.
func (s *routingPolicyHarness) refusedWith(method, path string, body any) string {
	return s.refusedTo(s.serverClient, method, path, body)
}

// refusedTo is refusedWith for a request of client.
func (s *routingPolicyHarness) refusedTo(client *testClient, method, path string, body any) string {
	status, payload := client.call(method, path, body)
	s.Require().Equal(http.StatusServiceUnavailable, status, string(payload))
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answered), string(payload))
	return answered.Error.Code
}

// numberOf gives the test's app a number of no country attached to a call, answered by config,
// none for empty, and returns the call's Stream hook.
func (s *routingPolicyHarness) numberOf(configID string) string {
	ctx := context.Background()
	e164 := s.utils.number()
	callID := "phone-" + e164
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: e164, Vendor: "telnyx", CustomerID: s.customerID(), ConfigID: configID, PurchasedAt: time.Now().UTC(),
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(ctx, s.customerID(), e164,
		store.NumberAttachment{TrunkID: "trunk-" + s.utils.uuid(), CallType: "default", CallID: callID}))
	return arrivingCall("default:" + callID)
}

// runningOn is the address of the node the deployment's register says runs a session.
func (s *routingPolicyHarness) runningOn(id string) string {
	directory, err := node.NewDirectory(node.DirectoryOptions{Redis: s.live.Redis(), Address: "observer",
		Prefix: s.relayPrefix, Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)
	defer directory.Close()
	address, err := directory.Node(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	return address
}

// idle checks that nothing reaches worker.
func (s *routingPolicyHarness) idle(worker *dispatch.Worker, complaint string) {
	select {
	case call := <-worker.Calls():
		s.Failf(complaint, "it was handed call %s", call.CallID)
	case message := <-worker.Messages():
		s.Failf(complaint, "it was handed a message in %s", message.ChannelID)
	case <-time.After(dropped):
	}
}

// RoutingPolicySuite covers new work placed by its app's or its agent's routing policy, with a
// node up in shard there, which holds a worker of the test's app too.
type RoutingPolicySuite struct {
	routingPolicyHarness

	other        *httptest.Server
	otherWorkers *dispatch.Pool
	there        *dispatch.Worker
}

func TestRoutingPolicySuite(t *testing.T) {
	runSuite(t, new(RoutingPolicySuite))
}

func (s *RoutingPolicySuite) SetupSuite() {
	s.setUp()
	s.otherWorkers = dispatch.NewPool()
	s.other = s.otherNodeDispatching(s.otherWorkers)
}

func (s *RoutingPolicySuite) SetupTest() {
	s.routingPolicyHarness.SetupTest()
	var release func()
	s.there, release = s.otherWorkers.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	s.T().Cleanup(release)
}

// reaches waits for a call or a message to arrive at worker.
func (s *RoutingPolicySuite) reaches(worker *dispatch.Worker, where string) {
	select {
	case <-worker.Calls():
	case <-worker.Messages():
	case <-time.After(settleFor):
		s.FailNow("the work never reached the worker " + where)
	}
}

func (s *RoutingPolicySuite) TestASessionOfAnAppKeptInUSEastIsOpenedInTheHomeShard() {
	s.appPolicy(shardmap.PolicyUSEast)

	opened := s.serverClient.createSession(policySession(nil))

	s.Equal(s.other.Listener.Addr().String(), s.runningOn(opened.Id))
}

func (s *RoutingPolicySuite) TestASessionOfAnAgentKeptInTheEUIsOpenedInTheEUShardOverItsApp() {
	config := s.agent(shardmap.PolicyEUOnly)

	opened := s.serverClient.createSession(policySession(&config))

	s.Equal(s.other.Listener.Addr().String(), s.runningOn(opened.Id))
}

// A caller knows an agent by its name more often than by its id.
func (s *RoutingPolicySuite) TestASessionOfAnAgentNamedByNameIsPlacedByItsPolicy() {
	config := s.agent(shardmap.PolicyEUOnly)

	opened := s.serverClient.createSession(CreateSessionRequest{Incognito: pointerTo(true), Agent: &config.Name})

	s.Equal(s.other.Listener.Addr().String(), s.runningOn(opened.Id))
}

func (s *RoutingPolicySuite) TestASessionOfAClosestAppIsOpenedWhereItLands() {
	opened := s.serverClient.createSession(policySession(nil))

	s.Equal(s.server.Listener.Addr().String(), s.runningOn(opened.Id))
}

// An agent that follows its app is placed by the app's policy, not as closest.
func (s *RoutingPolicySuite) TestASessionOfAnAgentFollowingItsAppIsPlacedByTheApps() {
	s.appPolicy(shardmap.PolicyEUOnly)
	config := s.agent("")

	opened := s.serverClient.createSession(policySession(&config))

	s.Equal(s.other.Listener.Addr().String(), s.runningOn(opened.Id))
}

// Canada's first shard has no node: its second does the work.
func (s *RoutingPolicySuite) TestWorkWhoseFirstShardIsDownIsDoneInTheNextShardOfItsGeography() {
	config := s.agent(shardmap.PolicyCanadaOnly)

	opened := s.serverClient.createSession(policySession(&config))

	s.Equal(s.other.Listener.Addr().String(), s.runningOn(opened.Id))
}

func (s *RoutingPolicySuite) TestWorkKeptInAGeographyWithNoShardIsRefused() {
	config := s.agent(shardmap.PolicyUSOnly)

	s.Equal(codeShardUnavailable, s.refusedWith(http.MethodPost, "/v1/agents/sessions", policySession(&config)))
}

// Anyone can send the header a peer marks what it carries with: it does not take work out of
// its geography.
func (s *RoutingPolicySuite) TestARequestClaimingAPeerCarriedItIsStillKeptInItsGeography() {
	config := s.agent(shardmap.PolicyEUOnly)

	s.Equal(codeShardUnavailable, s.refusedTo(s.serverClient.with(node.ForwardedHeader, "1"),
		http.MethodPost, "/v1/agents/sessions", policySession(&config)))
}

func (s *RoutingPolicySuite) TestASocketOfAnAppKeptInUSEastIsServedInTheHomeShard() {
	s.appPolicy(shardmap.PolicyUSEast)
	connection := s.serverClient.opens("/v1/agents/socket")

	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "start", "session": map[string]any{"incognito": true}}))
	answered := s.await(connection, "session")

	id, _ := answered["session"].(map[string]any)["id"].(string)
	s.Equal(s.other.Listener.Addr().String(), s.runningOn(id))
}

// The socket is open before its agent is named, so it cannot be carried to the agent's shard.
func (s *RoutingPolicySuite) TestASocketForAnAgentKeptInAnotherGeographyIsRefused() {
	config := s.agent(shardmap.PolicyEUOnly)
	connection := s.serverClient.opens("/v1/agents/socket")

	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "start",
		"session": map[string]any{"incognito": true, "config_id": config.ID}}))

	s.Equal(codeShardUnavailable, s.await(connection, "error")["code"])
}

func (s *RoutingPolicySuite) TestAMessageToAnAppKeptInUSEastIsAnsweredInTheHomeShard() {
	s.appPolicy(shardmap.PolicyUSEast)

	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", arrivingMessage("chat-"+s.utils.uuid(), s.agent(""))))

	s.reaches(s.there, "in the home shard")
	s.idle(s.here, "a message of an app kept in us-east was answered where it landed")
}

func (s *RoutingPolicySuite) TestAMessageToAnAgentKeptInTheEUIsAnsweredInTheEUShard() {
	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream",
		arrivingMessage("chat-"+s.utils.uuid(), s.agent(shardmap.PolicyEUOnly))))

	s.reaches(s.there, "in the EU shard")
	s.idle(s.here, "a message to an agent kept in the EU was answered outside it")
}

func (s *RoutingPolicySuite) TestAMessageToAClosestAgentIsAnsweredWhereItLands() {
	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", arrivingMessage("chat-"+s.utils.uuid(), s.agent(""))))

	s.reaches(s.here, "where it landed")
	s.idle(s.there, "a message to a closest agent was handed to another shard")
}

func (s *RoutingPolicySuite) TestACallToAnAppKeptInUSEastIsAnsweredInTheHomeShard() {
	s.appPolicy(shardmap.PolicyUSEast)

	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", s.numberOf("")))

	s.reaches(s.there, "in the home shard")
	s.idle(s.here, "a call of an app kept in us-east was answered where it landed")
}

func (s *RoutingPolicySuite) TestACallToAnAppKeptInTheEUIsAnsweredInTheEUShard() {
	s.appPolicy(shardmap.PolicyEUOnly)

	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", s.numberOf("")))

	s.reaches(s.there, "in the EU shard")
	s.idle(s.here, "a call of an app kept in the EU was answered outside it")
}

func (s *RoutingPolicySuite) TestACallToAClosestAppIsAnsweredWhereItLands() {
	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", s.numberOf("")))

	s.reaches(s.here, "where it landed")
	s.idle(s.there, "a call of a closest app was handed to another shard")
}

// RoutingPolicyShardDownSuite is RoutingPolicySuite with no node up in shard there: work kept
// in a geography is done nowhere, and other work is done where it landed.
type RoutingPolicyShardDownSuite struct {
	routingPolicyHarness
}

func TestRoutingPolicyShardDownSuite(t *testing.T) {
	runSuite(t, new(RoutingPolicyShardDownSuite))
}

func (s *RoutingPolicyShardDownSuite) SetupSuite() {
	s.setUp()
}

func (s *RoutingPolicyShardDownSuite) TestASessionKeptInTheEUIsRefusedRatherThanOpenedHere() {
	s.appPolicy(shardmap.PolicyEUOnly)

	s.Equal(codeShardUnavailable, s.refusedWith(http.MethodPost, "/v1/agents/sessions", policySession(nil)))
}

// Availability wins for work kept in no geography.
func (s *RoutingPolicyShardDownSuite) TestASessionOfAnAppKeptInUSEastIsOpenedHereWithTheHomeDown() {
	s.appPolicy(shardmap.PolicyUSEast)

	opened := s.serverClient.createSession(policySession(nil))

	s.Equal(s.server.Listener.Addr().String(), s.runningOn(opened.Id))
}

func (s *RoutingPolicyShardDownSuite) TestASocketKeptInTheEUIsRefusedRatherThanOpenedHere() {
	s.appPolicy(shardmap.PolicyEUOnly)

	connection, status := s.serverClient.watch("/v1/agents/socket")

	s.Nil(connection)
	s.Equal(http.StatusServiceUnavailable, status)
}

func (s *RoutingPolicyShardDownSuite) TestARunKeptInTheEUIsRefusedRatherThanRunHere() {
	config := s.agent(shardmap.PolicyEUOnly)

	s.Equal(codeShardUnavailable, s.refusedWith(http.MethodPost, "/v1/agents/runs",
		CreateRunRequest{ConfigId: config.ID, Input: "hello"}))
}

func (s *RoutingPolicyShardDownSuite) TestACallPlacedForAnAppKeptInTheEUIsRefusedRatherThanPlacedHere() {
	s.appPolicy(shardmap.PolicyEUOnly)

	s.Equal(codeShardUnavailable, s.refusedWith(http.MethodPost, "/v1/phone/calls",
		PlaceCallRequest{From: s.utils.number(), To: s.utils.number()}))
}

func (s *RoutingPolicyShardDownSuite) TestAMessageKeptInTheEUIsNotAnsweredHere() {
	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream",
		arrivingMessage("chat-"+s.utils.uuid(), s.agent(shardmap.PolicyEUOnly))))

	s.idle(s.here, "a message kept in the EU was answered outside it")
}

// Stream delivers a 503 again, by when the EU may be back.
func (s *RoutingPolicyShardDownSuite) TestACallKeptInTheEUIsAnswered503AndNotAnsweredHere() {
	s.appPolicy(shardmap.PolicyEUOnly)

	s.Equal(http.StatusServiceUnavailable, s.signedly("/v1/phone/hooks/stream", s.numberOf("")))

	s.idle(s.here, "a call kept in the EU was answered outside it")
}

func (s *RoutingPolicyShardDownSuite) TestACallToAnAppKeptInUSEastIsAnsweredHereWithTheHomeDown() {
	s.appPolicy(shardmap.PolicyUSEast)

	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", s.numberOf("")))

	select {
	case <-s.here.Calls():
	case <-time.After(settleFor):
		s.FailNow("a call of an app kept in us-east was dropped with the home down")
	}
}
