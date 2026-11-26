//go:build integration

package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/bearer"
	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/outcome"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// CrossShardSessionSuite covers a session watched from a node in another shard, whose relay
// the session's events never cross.
type CrossShardSessionSuite struct {
	RouterSuite

	// other is the node in the other shard, running every session a test here opens.
	other *httptest.Server
}

func TestCrossShardSessionSuite(t *testing.T) {
	runSuite(t, new(CrossShardSessionSuite))
}

func (s *CrossShardSessionSuite) SetupSuite() {
	s.shard, s.otherShard = "here", "there"
	s.RouterSuite.SetupSuite()
	s.other = s.otherNode()
}

func (s *CrossShardSessionSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *CrossShardSessionSuite) TestASessionInAnotherShardIsWatchedThroughTheNodeRunningIt() {
	answer := s.utils.uuid()
	opened := s.serverClient.on(s.other).createSession(CreateSessionRequest{
		Incognito: pointerTo(true),
		Llm:       pointerTo("echo/echo-model"), ConfigId: s.data.instructedAgent(answer),
	})
	watching := s.serverClient.opens("/v1/agents/sessions/" + opened.Id + "/events")

	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodPost,
		"/v1/agents/sessions/"+opened.Id+"/respond", SayRequest{Text: "hello"}, nil))

	answered := s.await(watching, "responded")
	s.Contains(answered["text"], answer, "the watcher in this shard never saw the answer")
}

// endedThere is a chat the node in the other shard opened and ended, whose rows are there.
func (s *CrossShardSessionSuite) endedThere() string {
	opened := s.serverClient.on(s.other).createSession(textSession(nil))
	s.serverClient.on(s.other).stopSession(opened.Id)
	ctx := context.Background()
	s.Require().Eventually(func() bool {
		row, err := s.store.Sessions.Get(ctx, s.customerID(), opened.Id)
		home, homeErr := s.store.SessionHomes.Of(ctx, s.customerID(), opened.Id)
		return err == nil && row.State == store.SessionClosed && homeErr == nil && home == "there"
	}, settleFor, 10*time.Millisecond, "the session ended in the shard it lives in")
	return opened.Id
}

// runningOn is the address of the node the deployment's register says runs a session.
func (s *CrossShardSessionSuite) runningOn(id string) string {
	directory, err := node.NewDirectory(node.DirectoryOptions{Redis: s.live.Redis(), Address: "observer",
		Prefix: s.relayPrefix, Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)
	defer directory.Close()
	address, err := directory.Node(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	return address
}

func (s *CrossShardSessionSuite) TestAChatLivingInAnotherShardIsCarriedOnThere() {
	id := s.endedThere()

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost,
		"/v1/agents/sessions/"+id+"/resume", ResumeSessionRequest{}, nil))

	s.Equal(s.other.Listener.Addr().String(), s.runningOn(id),
		"carried on in a shard that does not have its rows")
}

func (s *CrossShardSessionSuite) TestASessionLivingInAShardNoNodeAnswersIsUnavailable() {
	id := s.endedThere()
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.SessionHome)(nil)).Set("shard = ?", "gone").
		Where("customer_id = ?", s.customerID()).Where("session_id = ?", id).Exec(context.Background())
	s.Require().NoError(err)

	status, payload := s.serverClient.call(http.MethodGet, "/v1/agents/sessions/"+id, nil)

	s.Equal(http.StatusServiceUnavailable, status, "read from a shard that does not have its rows")
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answered))
	s.Equal(codeShardUnavailable, answered.Error.Code)
}

// CallShardSuite covers what a vendor or Stream says about a call, delivered to a node in a
// shard other than the one the call lives in, and what two shards apart share: the lists and
// stats once shipped, and an organization's calls at once.
type CallShardSuite struct {
	RouterSuite

	// otherPhone is the phone service of the node in the other shard, over its own store.
	otherPhone *phone.Service
}

func TestCallShardSuite(t *testing.T) {
	runSuite(t, new(CallShardSuite))
}

func (s *CallShardSuite) SetupSuite() {
	// ShipStats ships every session in its database, other suites' too: the suite has its own.
	s.database = "call_shard"
	s.shard, s.otherShard = "here", "there"
	s.otherShardApart = true
	s.phoneVendors = s.unreachableVendors()
	s.phoneOnChat = true
	s.RouterSuite.SetupSuite()
	service, err := phone.NewService(phone.ServiceOptions{
		Registry: s.phoneVendors,
		Stream:   phone.NewStreamFromClient(s.chat.Client),
		Store:    s.otherStore,
		Now:      s.phoneNow,
		Logger:   slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	s.otherPhone = service
	s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler), func(options *Options) {
		options.Phone = s.otherPhone
	})
}

func (s *CallShardSuite) SetupTest() {
	// The fixtures are written once for the run, in the shared database, not this suite's.
	s.useApp(s.data.createApp())
}

func (s *CallShardSuite) TestAStatusForACallAnotherShardPlacedIsTakenThere() {
	ctx := context.Background()
	callID := s.utils.uuid()
	token, err := s.otherPhone.Outcomes().Placed(ctx, outcome.Call{
		Key: outcome.Key{
			CustomerID: s.customerID(), CallType: "default", CallID: callID,
			Local: s.utils.number(), Remote: s.utils.number(),
		},
		Vendor:  "telnyx",
		Reports: true,
	})
	s.Require().NoError(err)

	request, err := http.NewRequest(http.MethodPost, s.server.URL+"/v1/phone/status/"+token,
		strings.NewReader(url.Values{"CallStatus": {"busy"}}.Encode()))
	s.Require().NoError(err)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := s.server.Client().Do(request)
	s.Require().NoError(err)
	s.Require().NoError(response.Body.Close())

	s.Equal(http.StatusNoContent, response.StatusCode)
	call, err := s.otherPhone.Outcomes().Read(ctx, s.customerID(), callID)
	s.Require().NoError(err)
	s.Equal(outcome.Busy, call.Outcome)
}

func (s *CallShardSuite) TestAPlanFetchForAShardNoNodeAnswersIsAnsweredForTheVendorToRetry() {
	status, failure := s.unauthenticatedClient.failure(http.MethodGet, "/v1/phone/answer/gone."+s.utils.uuid(), nil)

	s.Equal(http.StatusServiceUnavailable, status)
	s.Contains(failure, errCallShardDown.Message)
}

func (s *CallShardSuite) TestAnArrivingCallPlacedInAShardNoNodeAnswersIsDeliveredAgain() {
	ctx := context.Background()
	options := s.storeOptions
	options.Shard = "gone"
	gone, err := store.OpenShards(options)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(gone.Close()) })
	callID := s.utils.uuid()
	s.Require().NoError(gone.CallResources.Insert(ctx, &store.CallResource{
		TrunkID: "trunk-" + callID, RouteID: "route-" + callID, CallType: "default", CallID: callID,
		CustomerID: s.customerID(), Shared: true,
	}))

	body := arrivingCallAt("default:"+callID, time.Now().UTC())
	status, failure := s.deliver("/v1/phone/hooks/stream", body, sign(body, suiteStreamSecret))

	s.Equal(http.StatusServiceUnavailable, status, "a call another shard placed was taken where it landed")
	s.Contains(failure, errCallShardDown.Message)
}

func (s *CallShardSuite) TestTheEndOfACallAnotherShardMadeIsTakenThere() {
	ctx := context.Background()
	callID := s.utils.uuid()
	s.Require().NoError(s.otherStore.CallResources.Insert(ctx, &store.CallResource{
		TrunkID: "trunk-" + callID, RouteID: "route-" + callID, CallType: "default", CallID: callID,
		CustomerID: s.customerID(), Shared: true,
	}))

	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", fmt.Sprintf(
		`{"type":"call.session_ended","call_cid":"default:%s","session_id":"session-1","created_at":%q,`+
			`"call":{"cid":"default:%s","id":"%s","type":"default","custom":{}}}`,
		callID, time.Now().UTC().Format(time.RFC3339), callID, callID)))

	left, err := s.otherStore.CallResources.Select(ctx, store.AppScope{Unpinned: true}, "default", callID)
	s.Require().NoError(err)
	s.Empty(left, "the call's end never reached the shard it lives in")
}

// Defect: a call another shard keeps is read here, where it is missing.
func (s *CallShardSuite) TestACallAnotherShardKeepsIsReadThere() {
	ctx := context.Background()
	call := store.Call{ID: s.utils.uuid(), CustomerID: s.customerID(), AgentID: "agent", CallType: "default",
		CallID: s.utils.uuid(), Direction: "inbound", StartedAt: time.Now().UTC().Truncate(time.Millisecond)}
	s.Require().NoError(s.otherStore.Calls.Start(ctx, &call))
	s.Require().NoError(s.otherStore.ShipIndex(ctx, time.Now()))

	var read Call
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/calls/"+call.ID, nil, &read))

	s.Equal(call.CallID, read.CallId)
}

// Defect: a call list reads this shard alone, or lists another shard's call twice or without
// the shard it is kept in.
func (s *CallShardSuite) TestACallListHoldsEveryShardsCalls() {
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Millisecond)
	agent := s.utils.uuid()
	here := store.Call{ID: s.utils.uuid(), CustomerID: s.customerID(), AgentID: agent, CallType: "default",
		CallID: s.utils.uuid(), Direction: "inbound", StartedAt: at}
	there := store.Call{ID: s.utils.uuid(), CustomerID: s.customerID(), AgentID: agent, CallType: "default",
		CallID: s.utils.uuid(), Direction: "inbound", StartedAt: at.Add(time.Second)}
	s.Require().NoError(s.store.Calls.Start(ctx, &here))
	s.Require().NoError(s.otherStore.Calls.Start(ctx, &there))
	s.Require().NoError(s.otherStore.ShipIndex(ctx, time.Now()))

	var listed []Call
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/calls?agent_id="+agent, nil, &listed))

	regions := map[string]string{}
	for _, each := range listed {
		regions[each.Id] = value(each.Region)
	}
	s.Equal(map[string]string{here.ID: "here", there.ID: "there"}, regions)
	s.Len(listed, 2)
}

// Defect: a plan fetch for a bridge another shard parked is answered here, where the bridge is
// not, and the person who picked up is bridged to nowhere.
func (s *CallShardSuite) TestAPlanFetchForABridgeAnotherShardParkedIsAnsweredThere() {
	ctx := context.Background()
	token := s.otherStore.MintToken()
	s.Require().NoError(s.otherStore.CallBridges.Insert(ctx, &store.CallBridge{
		Token: token, CustomerID: s.customerID(), Vendor: "plivo", TrunkURI: "sip:agent@bridge.example.com",
		CallID: s.utils.uuid(), ExpiresAt: time.Now().UTC().Add(time.Minute),
	}))

	// The suite's plivo has no credentials, so the plan itself is refused once the bridge is
	// claimed: what tells where it was answered is the bridge being spent in its shard.
	s.unauthenticatedClient.call(http.MethodGet, "/v1/phone/answer/"+token, nil)

	_, err := s.otherStore.CallBridges.Claim(ctx, token)
	s.Error(err, "the bridge is still waiting in the shard that parked it")
}

// Defect: each shard counts only the calls it placed, so two shards together ring past the
// organization's calls at once.
func (s *CallShardSuite) TestTwoShardsPlacingAtOnceCannotRingPastTheOrganizationsCap() {
	organization := s.utils.uuid()
	atOnce := store.CallsAtOnce{Limit: 3, Ringing: time.Minute}
	var placed atomic.Int64
	var wg sync.WaitGroup
	for _, keeper := range []*outcome.Keeper{s.router.phone.Outcomes(), s.otherPhone.Outcomes()} {
		for range 6 {
			wg.Go(func() {
				_, err := keeper.PlacedWithin(context.Background(), outcome.Call{
					Key: outcome.Key{CustomerID: s.customerID(), CallType: "default", CallID: s.utils.uuid(),
						Local: s.utils.number(), Remote: s.utils.number()},
					Vendor: "telnyx",
				}, organization, &atOnce, nil)
				if err == nil {
					placed.Add(1)
					return
				}
				s.ErrorIs(err, store.ErrCallsAtOnce)
			})
		}
	}
	wg.Wait()

	s.EqualValues(3, placed.Load())
}

// Defect: once both shards shipped, the session list and the agent stats disagree on how many
// sessions there were: one reads this shard alone, or counts the other's twice.
func (s *CallShardSuite) TestTheSessionListAndStatsAgreeOnBothShardsSessionsOnceShipped() {
	s.useApp(s.data.createApp())
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Millisecond)
	ids := map[string]bool{}
	for _, into := range []*store.Store{s.store, s.otherStore} {
		session := &store.AgentSession{ID: s.utils.uuid(), CustomerID: s.customerID(), AgentName: "docs",
			State: store.SessionRunning, CreatedAt: at}
		s.Require().NoError(into.Sessions.Upsert(ctx, session))
		s.Require().NoError(into.Responses.Insert(ctx, &store.AgentResponse{ID: s.utils.uuid(), SessionID: session.ID,
			CustomerID: s.customerID(), CreatedAt: at}))
		s.Require().NoError(into.ShipIndex(ctx, at))
		s.Require().NoError(into.ShipStats(ctx, at))
		ids[session.ID] = true
	}

	var page SessionPage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/sessions/query", SessionQuery{}, &page))
	var stats AgentStats
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/stats/agents?from="+
		url.QueryEscape(at.Add(-time.Hour).Format(time.RFC3339))+"&to="+url.QueryEscape(at.Add(time.Hour).Format(time.RFC3339)), nil, &stats))

	listed := map[string]bool{}
	for _, item := range page.Items {
		listed[item.Id] = true
	}
	s.Equal(ids, listed)
	s.EqualValues(len(ids), stats.Sessions)
}

func (s *CallShardSuite) TestStatsAreShippedFromTheSuitesOwnDatabaseAlone() {
	ctx := context.Background()
	shared, err := store.Open(testDatabase(s.T(), os.Getenv("ROUTER_POSTGRES_DSN")))
	s.Require().NoError(err)
	defer shared.Close() //nolint:errcheck // the error on the way out says nothing
	elsewhere := s.utils.uuid()
	s.Require().NoError(shared.Sessions.Upsert(ctx, &store.AgentSession{ID: s.utils.uuid(), CustomerID: elsewhere,
		AgentName: "docs", State: store.SessionRunning, CreatedAt: time.Now().Add(-30 * time.Minute)}))

	s.Require().NoError(s.store.ShipStats(ctx, time.Now()))

	var shipped int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(ctx,
		"SELECT count(*) FROM shard_concurrency_minute WHERE customer_id = ?", elsewhere).Scan(&shipped))
	s.Zero(shipped, "a session of the shared database was shipped")
}

// ShardRowsSuite covers the rows each shard keeps of its own and ships nowhere, the agent logs
// and a connection's invocations, read through a node of a shard that does not hold them.
type ShardRowsSuite struct {
	RouterSuite

	// other is the node in the other shard, running every session a test here opens.
	other *httptest.Server
}

func TestShardRowsSuite(t *testing.T) {
	runSuite(t, new(ShardRowsSuite))
}

func (s *ShardRowsSuite) SetupSuite() {
	s.shard, s.otherShard = "here", "there"
	s.shards = []string{"here", "there", "gone"}
	s.otherShardApart = true
	s.RouterSuite.SetupSuite()
	s.other = s.otherNode()
}

func (s *ShardRowsSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

// logged writes a log row of the app's in into's shard, and returns it.
func (s *ShardRowsSuite) logged(into *store.Store, session string) store.AgentLog {
	entry := store.AgentLog{CustomerID: s.customerID(), SessionID: session, Source: "agent", Severity: "info",
		EventType: "turn", Message: s.utils.uuid()}
	s.Require().NoError(into.AgentLogs.Insert(context.Background(), &entry))
	return entry
}

// logs is the page this node answers path with, and the shard it says answered.
func (s *ShardRowsSuite) logs(path string) (AgentLogPage, string) {
	response := s.serverClient.raw(http.MethodGet, path, nil)
	s.Require().Equal(http.StatusOK, response.StatusCode)
	var page AgentLogPage
	s.Require().NoError(json.NewDecoder(response.Body).Decode(&page))
	return page, response.Header.Get(shardHeader)
}

func logMessages(page AgentLogPage) []string {
	messages := make([]string, len(page.Items))
	for i, item := range page.Items {
		messages[i] = item.Message
	}
	return messages
}

// openedThere is a session the node in the other shard opened, once its home is recorded.
func (s *ShardRowsSuite) openedThere() string {
	opened := s.serverClient.on(s.other).createSession(textSession(nil))
	s.Require().Eventually(func() bool {
		home, err := s.store.SessionHomes.Of(context.Background(), s.customerID(), opened.Id)
		return err == nil && home == "there"
	}, settleFor, 10*time.Millisecond, "the session's home was recorded")
	return opened.Id
}

// Defect: the logs of a session another shard holds are read here, where they are not.
func (s *ShardRowsSuite) TestASessionsLogsAreReadInTheShardItLivesIn() {
	id := s.openedThere()
	there := s.logged(s.otherStore, id)

	page, shard := s.logs("/v1/agents/logs?session_id=" + id)

	s.Contains(logMessages(page), there.Message)
	s.Equal("there", shard)
}

// Defect: the logs read here include another shard's, or region does not reach that shard, or
// X-Shard names a shard that did not answer.
func (s *ShardRowsSuite) TestAnotherShardsLogsAreReadOnlyWhenItsRegionIsAskedFor() {
	there := s.logged(s.otherStore, "")

	local, shard := s.logs("/v1/agents/logs")
	s.NotContains(logMessages(local), there.Message, "another shard's log was read here")
	s.Equal("here", shard)

	remote, shard := s.logs("/v1/agents/logs?region=there")
	s.Contains(logMessages(remote), there.Message)
	s.Equal("there", shard)
}

// Defect: one log row of another shard is looked for here alone.
func (s *ShardRowsSuite) TestAnotherShardsLogIsFoundInItsRegion() {
	there := s.logged(s.otherStore, "")
	path := "/v1/agents/logs/" + strconv.FormatInt(there.ID, 10)

	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, path, nil, nil))
	var found AgentLog
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, path+"?region=there", nil, &found))
	s.Equal(there.Message, found.Message)
}

// Defect: a session's log stream is read here, where its rows are not.
func (s *ShardRowsSuite) TestASessionsLogStreamIsReadInTheShardItLivesIn() {
	id := s.openedThere()
	page, _ := s.logs("/v1/agents/logs?session_id=" + id)
	there := s.logged(s.otherStore, id)

	ctx, cancel := context.WithTimeout(context.Background(), settleFor)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.server.URL+"/v1/agents/logs/stream?session_id="+id+
		"&cursor="+url.QueryEscape(page.ResumeCursor), nil)
	s.Require().NoError(err)
	request.Header = s.serverClient.header.Clone()
	response, err := s.server.Client().Do(request)
	s.Require().NoError(err)
	defer response.Body.Close()
	s.Require().Equal(http.StatusOK, response.StatusCode)
	s.Equal("there", response.Header.Get(shardHeader))

	// Two logs, the second written once the first came: a stream that ends after its first answer
	// misses it.
	awaited := []string{there.Message}
	lines := bufio.NewScanner(response.Body)
	for lines.Scan() {
		if strings.Contains(lines.Text(), awaited[len(awaited)-1]) {
			if len(awaited) == 2 {
				return
			}
			awaited = append(awaited, s.logged(s.otherStore, id).Message)
		}
	}
	s.Fail("a log written in the session's shard never came", "%v", lines.Err())
}

func (s *ShardRowsSuite) TestARegionNoShardIsNamedIsRefused() {
	for _, path := range []string{"/v1/agents/logs?region=nowhere", "/v1/agents/connections/" + s.utils.uuid() + "/invocations?region=nowhere"} {
		status, message := s.serverClient.failure(http.MethodGet, path, nil)

		s.Equal(http.StatusBadRequest, status, path)
		s.Equal(errUnknownRegion.Message, message, path)
	}
}

func (s *ShardRowsSuite) TestARegionNoNodeAnswersIsUnavailable() {
	status, payload := s.serverClient.call(http.MethodGet, "/v1/agents/logs?region=gone", nil)

	s.Equal(http.StatusServiceUnavailable, status)
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answered))
	s.Equal(codeShardUnavailable, answered.Error.Code)
}

// invocations is the ids of the calls this node lists of a connection, and the shard it says
// answered.
func (s *ShardRowsSuite) invocations(path string) ([]string, string) {
	response := s.serverClient.raw(http.MethodGet, path, nil)
	s.Require().Equal(http.StatusOK, response.StatusCode)
	var page ConnectionInvocationPage
	s.Require().NoError(json.NewDecoder(response.Body).Decode(&page))
	ids := make([]string, len(page.Items))
	for i, item := range page.Items {
		ids[i] = item.ID
	}
	return ids, response.Header.Get(shardHeader)
}

// Defect: a connection's calls read here include another shard's, or region does not reach
// that shard, or X-Shard names a shard that did not answer.
func (s *ShardRowsSuite) TestAnotherShardsInvocationsAreListedOnlyWhenItsRegionIsAskedFor() {
	ctx := context.Background()
	connectorID := "custom_sharded" + strings.ReplaceAll(s.utils.uuid(), "-", "")
	manifest, err := core.ParseManifest([]byte("id: " + connectorID + "\nrevision: 1\nname: Sharded\n" +
		"endpoints:\n  api_base: https://api.example.com\nschemes: [bearer]\n"))
	s.Require().NoError(err)
	_, err = s.store.ConnectorDefinitions.Revise(ctx, s.customerID(), manifest)
	s.Require().NoError(err)
	connection := &store.ConnectorConnection{CustomerID: s.customerID(), ConnectorID: connectorID,
		OwnerType: store.OwnerApp, AuthScheme: bearer.Name, DefinitionRevision: 1}
	s.Require().NoError(s.store.ConnectorConnections.Insert(ctx,
		core.Registry{Schemes: map[string]core.Scheme{bearer.Name: bearer.New()}}, connection))
	there := store.ConnectorInvocation{CustomerID: s.customerID(), ConnectionID: connection.ID, ConnectorID: connectorID,
		Tool: "search", StartedAt: time.Now()}
	s.Require().NoError(s.otherStore.ConnectorInvocations.Insert(ctx, &there))
	path := "/v1/agents/connections/" + connection.ID + "/invocations"

	local, shard := s.invocations(path)
	s.NotContains(local, there.ID, "another shard's call was read here")
	s.Equal("here", shard)

	remote, shard := s.invocations(path + "?region=there")
	s.Equal([]string{there.ID}, remote)
	s.Equal("there", shard)
}

// A call another shard answered is its customer's here too: another customer's call is not routed
// into it, nor a session opened under its id, and its own customer's are.
func (s *CallShardSuite) TestACallAnotherShardAnsweredIsItsCustomersHereToo() {
	arrived := func(customer string) string {
		call := s.utils.uuid()
		s.otherPhone.OpenLeg(context.Background(), routing.Owner{CustomerID: customer}, store.CallLeg{
			CallType: "agent", CallID: call, StreamSessionID: "stream-" + call, Vendor: "twilio", Direction: store.Inbound,
			LocalNumber: s.utils.number(), RemoteNumber: s.utils.number(), DialedAt: time.Now().Add(-time.Minute),
		}, true)
		return call
	}
	e164 := s.utils.number()
	s.Require().NoError(s.store.PhoneNumbers.Insert(context.Background(), &store.PhoneNumber{
		E164: e164, Vendor: "twilio", Country: "US", CustomerID: s.customerID(), PurchasedAt: time.Now().UTC(),
	}))
	callInto := func(session string) (int, string) {
		status, payload := s.serverClient.call(http.MethodPost, "/v1/phone/calls", PlaceCallRequest{
			From: e164, To: "+15550001111", SessionId: &session,
		})
		return status, string(payload)
	}
	theirs, mine := arrived("customer-"+s.utils.uuid()), arrived(s.customerID())

	status, payload := callInto(theirs)
	s.Equal(http.StatusConflict, status, payload)
	s.Contains(payload, "session_id is taken")
	s.Equal(http.StatusConflict, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", textSession(&theirs), nil))
	status, payload = callInto(mine)
	s.Equal(http.StatusServiceUnavailable, status, payload)
	s.Contains(payload, "phone_vendor_failed")
	s.Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", textSession(&mine), nil))
}
