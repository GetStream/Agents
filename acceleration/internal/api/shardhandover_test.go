//go:build integration

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/shardmap"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// nowhere is a shard no node of the suite is in.
const nowhere = "nowhere"

// ShardHandoverSuite covers hooks handed to the shard nearest the person they are for: the
// suite's node is in shard here, the other node in shard there, and each has a worker of the
// test's app waiting, so the worker a hook reaches says which node did its work.
type ShardHandoverSuite struct {
	RouterSuite

	other        *httptest.Server
	otherWorkers *dispatch.Pool
	// here and there are the workers waiting on the suite's node and on the other node.
	here, there *dispatch.Worker
	// releaseHere and releaseThere let go of here and there.
	releaseHere, releaseThere func()
}

func TestShardHandoverSuite(t *testing.T) {
	runSuite(t, new(ShardHandoverSuite))
}

func (s *ShardHandoverSuite) SetupSuite() {
	s.shard, s.otherShard = "here", "there"
	s.shardRouting = shardmap.Map{
		Countries: map[string]string{"DE": "there", "US": "here", "JP": nowhere},
		Regions:   map[string]string{"eu": "there"},
	}
	s.RouterSuite.SetupSuite()
	s.otherWorkers = dispatch.NewPool()
	s.other = s.otherNodeDispatching(s.otherWorkers)
}

func (s *ShardHandoverSuite) SetupTest() {
	s.useApp(s.data.createApp())
	s.here, s.releaseHere = s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	s.T().Cleanup(s.releaseHere)
	s.there, s.releaseThere = s.otherWorkers.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	s.T().Cleanup(s.releaseThere)
}

// numberIn gives the test's app a number of country attached to a call, and returns the
// call's Stream hook and the call's id.
func (s *ShardHandoverSuite) numberIn(country string) (string, string) {
	ctx := context.Background()
	e164 := s.utils.number()
	callID := "phone-" + e164
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: e164, Vendor: "telnyx", Country: country, CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(ctx, s.customerID(), e164,
		store.NumberAttachment{TrunkID: "trunk-" + s.utils.uuid(), CallType: "default", CallID: callID}))
	return arrivingCall("default:" + callID), callID
}

// channelTo is a channel written to an agent of region, and the hook for a message sam writes
// there.
func (s *ShardHandoverSuite) channelTo(region string) (string, string) {
	config := store.AgentConfig{CustomerID: s.customerID(), Name: "config-" + s.utils.uuid(), Mode: "text", Region: region}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &config))
	channel := "chat-" + s.utils.uuid()
	return channel, arrivingMessage(channel, config)
}

// samServedBy has the shard given be the one that last served sam, who writes every message.
func (s *ShardHandoverSuite) samServedBy(shard string) {
	ctx := context.Background()
	s.Require().NoError(s.store.Users.Seen(ctx, s.customerID(), "sam", store.UserKindAuthenticated))
	s.Require().NoError(s.store.Users.Served(ctx, s.customerID(), "sam", shard))
}

// reaches waits for a call to arrive at worker.
func (s *ShardHandoverSuite) reaches(worker *dispatch.Worker, where string) dispatch.Call {
	select {
	case call := <-worker.Calls():
		return call
	case <-time.After(settleFor):
		s.FailNow("the call never reached the worker " + where)
		return dispatch.Call{}
	}
}

// messaged waits for a message to arrive at worker.
func (s *ShardHandoverSuite) messaged(worker *dispatch.Worker, where string) dispatch.Message {
	select {
	case message := <-worker.Messages():
		return message
	case <-time.After(settleFor):
		s.FailNow("the message never reached the worker " + where)
		return dispatch.Message{}
	}
}

// idle checks that nothing reaches worker.
func (s *ShardHandoverSuite) idle(worker *dispatch.Worker, complaint string) {
	select {
	case call := <-worker.Calls():
		s.Failf(complaint, "it was handed call %s", call.CallID)
	case message := <-worker.Messages():
		s.Failf(complaint, "it was handed a message in %s", message.ChannelID)
	case <-time.After(dropped):
	}
}

func (s *ShardHandoverSuite) TestACallToANumberInACountryOfAnotherShardIsAnsweredThere() {
	hook, callID := s.numberIn("DE")

	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", hook))

	s.reaches(s.there, "in the number's shard")
	s.idle(s.here, "the call was answered where it landed too")
	var legs []store.CallLeg
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&legs).Where("call_id = ?", callID).Scan(context.Background()))
	s.Require().Len(legs, 1, "the caller's minutes are billed once, by the node answering")
	s.Equal("DE", legs[0].Country)
}

func (s *ShardHandoverSuite) TestACallToANumberOfThisShardsCountryIsAnsweredHere() {
	hook, _ := s.numberIn("US")
	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", hook))

	s.reaches(s.here, "where it landed")
	s.idle(s.there, "a call of this shard was handed to another")
}

// A shard with no node up is never waited for: the caller is on the line.
func (s *ShardHandoverSuite) TestACallForAShardWithNoNodeUpIsAnsweredHereAndCounted() {
	before := testutil.ToFloat64(shardFallbacks.WithLabelValues(nowhere, shardWorkCall))

	hook, _ := s.numberIn("JP")
	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", hook))

	s.reaches(s.here, "where it landed")
	s.Equal(before+1, testutil.ToFloat64(shardFallbacks.WithLabelValues(nowhere, shardWorkCall)))
}

// A request a peer handed over stays where it was handed, even to a node that would send it
// to another shard: two routers disagreeing on where it belongs would pass it back and forth.
func (s *ShardHandoverSuite) TestACallHandedToThisShardIsNeverHandedToAnother() {
	body, _ := s.numberIn("DE")
	request, err := http.NewRequest(http.MethodPost, "/v1/phone/hooks/stream", strings.NewReader(body))
	s.Require().NoError(err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(signatureHeader, sign(body, suiteStreamSecret))
	credential, _ := s.sealer.Derive(node.CredentialPurpose)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	answer, err := node.NewForwarder(nil, credential).Ask(node.ToShard(ctx, "here"),
		s.server.Listener.Addr().String(), request, []byte(body))

	s.Require().NoError(err)
	s.Equal(int32(http.StatusOK), answer.GetStatus(), "body: %s", answer.GetBody())
	s.reaches(s.here, "it was handed to")
	s.idle(s.there, "a request handed to one shard was handed on to another")
}

func (s *ShardHandoverSuite) TestAMessageFromAUserLastServedByAnotherShardIsAnsweredThere() {
	channel, hook := s.channelTo("us")
	s.samServedBy("there")

	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", hook))

	s.Equal(channel, s.messaged(s.there, "in the user's shard").ChannelID)
	s.idle(s.here, "the message was answered where it landed too")
}

func (s *ShardHandoverSuite) TestAMessageFromAUserNoShardServedYetIsAnsweredHere() {
	channel, hook := s.channelTo("us")

	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", hook))

	s.Equal(channel, s.messaged(s.here, "where it landed").ChannelID)
	s.idle(s.there, "a message from a user no shard served was handed over")
}

// The node a message is handed to may not be the one holding the app's worker: it carries the
// message on once more, to the worker's node.
func (s *ShardHandoverSuite) TestAMessageHandedToAShardReachesTheWorkerOnAnotherNodeThere() {
	s.releaseHere()
	s.releaseThere()
	worker := s.serverClient.on(s.otherNodeDispatching(dispatch.NewPool())).opens("/v1/dispatch")
	s.await(worker, "ready")
	s.samServedBy("there")

	// Each is handed to either node of the shard, so a few make both turn up.
	for range 6 {
		channel, hook := s.channelTo("us")
		s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", hook))
		s.Equal(channel, s.await(worker, "message")["channel_id"])
	}
}

// Residency wins: an agent kept in another region does its work in that region's shard, wherever
// its user was last served.
func (s *ShardHandoverSuite) TestAMessageToAnAgentKeptInAnotherRegionIsAnsweredInItsShard() {
	channel, hook := s.channelTo("eu")
	s.samServedBy("here")

	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", hook))

	s.Equal(channel, s.messaged(s.there, "in the agent's shard").ChannelID)
	s.idle(s.here, "a message to an agent kept in another region was answered where it landed")
}

// A conversation already running stays on its node: moving it would leave two agents answering.
func (s *ShardHandoverSuite) TestAMessageInAConversationRunningHereStaysHere() {
	config := store.AgentConfig{CustomerID: s.customerID(), Name: "config-" + s.utils.uuid(),
		Mode: store.AgentModeText, Models: store.AgentModels{LLM: "en-low-latency"}, Dispatch: store.AgentDispatch{Text: true}}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &config))
	channel := "chat-" + s.utils.uuid()
	opened := s.serverClient.createSession(CreateSessionRequest{AgentId: &channel, Agent: &config.Name, Incognito: pointerTo(true)})
	s.samServedBy("there")

	s.Require().Equal(http.StatusOK, s.signedly("/v1/chat/hooks/stream", arrivingMessage(channel, config)))

	s.Equal(opened.Id, s.messaged(s.here, "running the conversation").SessionID)
	s.idle(s.there, "a message in a running conversation was handed to the user's shard")
}

func (s *ShardHandoverSuite) TestTheShardServingAUsersOwnClientIsKept() {
	user := s.data.createUser()
	s.Require().Less(user.on(s.other).do(http.MethodGet, "/v1/agents/configs", nil, nil), http.StatusInternalServerError)

	s.Equal("there", s.shardOf(user))

	s.Require().Less(user.do(http.MethodGet, "/v1/agents/configs", nil, nil), http.StatusInternalServerError)

	s.Equal("here", s.shardOf(user))
}

// A request carried to the node running its session was sent to another: the node it was
// carried to is not where the user is.
func (s *ShardHandoverSuite) TestARequestCarriedToAnotherShardKeepsNoShard() {
	user := s.data.createUser()
	// Incognito, so only the node running it can answer for it.
	opened := user.on(s.other).createSession(CreateSessionRequest{Incognito: pointerTo(true)})
	s.Require().Less(user.do(http.MethodGet, "/v1/agents/configs", nil, nil), http.StatusInternalServerError)

	user.getSession(opened.Id)

	s.Equal("here", s.shardOf(user))
}

// A backend's request says nothing of where its user is.
func (s *ShardHandoverSuite) TestABackendActingForAUserKeepsNoShardForThem() {
	user := s.data.createUser()
	s.Require().Less(user.do(http.MethodGet, "/v1/agents/configs", nil, nil), http.StatusInternalServerError)

	s.Require().Less(s.serverClient.actingFor(user).on(s.other).do(http.MethodGet, "/v1/agents/configs", nil, nil),
		http.StatusInternalServerError)

	s.Equal("here", s.shardOf(user))
}

// shardOf is the shard kept for user.
func (s *ShardHandoverSuite) shardOf(user *testClient) string {
	shard, err := s.store.Users.Shard(context.Background(), s.customerID(), user.userID)
	s.Require().NoError(err)
	return shard
}

// ShardHandoverAppSuite is ShardHandoverSuite in app mode, the one where Stream's deliveries
// are recorded so a delivery made again is not answered twice.
type ShardHandoverAppSuite struct {
	RouterSuite

	here, there *dispatch.Worker
	apiKey      string
}

func TestShardHandoverAppSuite(t *testing.T) {
	runSuite(t, new(ShardHandoverAppSuite))
}

func (s *ShardHandoverAppSuite) SetupSuite() {
	s.appMode = true
	s.shard, s.otherShard = "here", "there"
	s.shardRouting = shardmap.Map{Countries: map[string]string{"DE": "there"}}
	s.RouterSuite.SetupSuite()
	workers := dispatch.NewPool()
	s.otherNodeDispatching(workers)
	var release func()
	s.useApp(s.numberedApp(streamAppID()))
	s.here, release = s.dispatch.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	s.T().Cleanup(release)
	s.there, release = workers.Register(s.customerID(), dispatch.Registration{Capacity: 1})
	s.T().Cleanup(release)
	s.apiKey = "own-key-" + s.utils.uuid()
	s.answerAs(s.apiKey, s.appID())
	s.forwarded(s.apiKey)
}

// rings delivers a call.session_started from the app for a call on its DE number.
func (s *ShardHandoverAppSuite) rings(body string) {
	request := s.request("/v1/phone/hooks/stream/"+strconv.FormatInt(s.appID(), 10), body, sign(body, ownAppSecret))
	vouched(request, s.appID(), s.apiKey)
	response, err := s.server.Client().Do(request)
	s.Require().NoError(err)
	s.Require().NoError(response.Body.Close())
	s.Require().Equal(http.StatusOK, response.StatusCode)
}

// A call handed over leaves its delivery recorded where it landed: the node it reaches does
// not record it, so without that Stream delivering it again would be answered a second time.
func (s *ShardHandoverAppSuite) TestACallDeliveredAgainAfterItWasHandedOverIsAnsweredOnce() {
	ctx := context.Background()
	e164, call := s.utils.number(), "line-"+s.utils.uuid()
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: e164, Vendor: "telnyx", Country: "DE", CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
	s.Require().NoError(s.store.PhoneNumbers.Attach(ctx, s.customerID(), e164, store.NumberAttachment{
		TrunkID: "trunk-" + s.utils.uuid(), StreamAppPK: s.appID(), CallType: "default", CallID: call,
	}))
	body := fmt.Sprintf(`{"type": "call.session_started", "call_cid": "default:%s", "session_id": %q,
  "created_at": %q, "call": {"id": %q, "type": "default", "custom": {}}}`,
		call, "session-"+s.utils.uuid(), time.Now().UTC().Format(time.RFC3339Nano), call)

	s.rings(body)
	select {
	case <-s.there.Calls():
	case <-time.After(settleFor):
		s.FailNow("the call never reached the worker in the number's shard")
	}
	s.rings(body)

	select {
	case again := <-s.there.Calls():
		s.Failf("the same delivery was answered twice", "call %s", again.CallID)
	case again := <-s.here.Calls():
		s.Failf("the same delivery was answered where it landed", "call %s", again.CallID)
	case <-time.After(dropped):
	}
}
