//go:build integration

package api

import (
	"cmp"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"
	"github.com/golang-jwt/jwt/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/chatlog"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

type CallsSuite struct {
	RouterSuite

	// base is when the calls in a test happened. It is a fixed hour, and the app is new in
	// every test, so what is listed is only what the test itself started.
	base time.Time
	// logged is what the router logged, for the errors a read leaves.
	logged *lockedLog
}

func TestCallsSuite(t *testing.T) {
	runSuite(t, new(CallsSuite))
}

func (s *CallsSuite) SetupSuite() {
	s.logged = &lockedLog{}
	s.logs = s.logged
	s.RouterSuite.SetupSuite()
}

// SetupTest gives every test an app of its own, because the running calls are everything
// one customer has not finished.
func (s *CallsSuite) SetupTest() {
	s.useApp(s.data.createApp())
	s.base = time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
}

func (s *CallsSuite) TestACallIsFoundAfterTheSessionRunningItIsGone() {
	// A session lives in a map in memory. This is the whole point of the row: the call
	// is still there once the process that held it is not.
	number := s.utils.number()
	call := s.started(store.Call{Direction: store.Outbound, ToNumber: number})

	var read Call
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/calls/"+call.ID, nil, &read))
	s.Equal(call.CallID, read.CallId)
	s.Equal(Outbound, read.Direction)
	s.Equal(number, value(read.ToNumber))
	s.Nil(read.EndedAt, "a call nobody has ended is still running")
}

func (s *CallsSuite) TestAJoinTokenSaysWhichCallToJoinAndWhoAsIt() {
	// The browser is handed a token and a call, never the secret: whoever holds this can
	// join one call as one user until it expires, and can sign nothing of their own.
	call := s.started(store.Call{})

	var minted CallToken
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost,
		"/v1/agents/calls/"+call.ID+"/token", map[string]any{}, &minted))

	s.Equal(suiteStreamKey, minted.ApiKey)
	s.Equal(call.CallID, minted.CallId, "the Stream call, not the id we hold it by")
	s.Equal(defaultCallType, minted.CallType, "a call nothing said otherwise about")
	s.NotEqual(call.AgentID, minted.UserId, "a listener is not the agent")
	s.True(minted.ExpiresAt.After(time.Now()), "a token that has expired is no use")

	claimed := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(minted.Token, claimed, func(*jwt.Token) (any, error) {
		return []byte(suiteStreamSecret), nil
	})
	s.Require().NoError(err, "the token is signed with the app secret")
	s.Equal(minted.UserId, claimed["user_id"], "and it is signed for the user it names")
}

func (s *CallsSuite) TestAJoinTokenIsMintedForTheUserTheCallerAsksFor() {
	// In its own Stream app every user id is the customer's to name.
	id := streamAppID()
	s.useApp(s.numberedApp(id))
	key := "key-" + s.customerID()
	s.giveApp(s.customerID(), id, key)
	call := s.started(store.Call{StreamAppPK: id})

	var minted CallToken
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost,
		"/v1/agents/calls/"+call.ID+"/token",
		map[string]any{"user_id": "thierry", "user_name": "Thierry"}, &minted))

	s.Equal("thierry", minted.UserId)
	s.Equal("Thierry", minted.UserName)

	claimed := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(minted.Token, claimed, func(*jwt.Token) (any, error) {
		return []byte(key + "-secret"), nil
	})
	s.Require().NoError(err)
	s.Equal("thierry", claimed["user_id"])
}

// A user id Stream keeps for itself is never signed for, even in the customer's own app.
func (s *CallsSuite) TestAJoinTokenIsNotMintedForAReservedUserID() {
	id := streamAppID()
	s.useApp(s.numberedApp(id))
	s.giveApp(s.customerID(), id, "key-"+s.customerID())
	call := s.started(store.Call{StreamAppPK: id})

	status, failure := s.serverClient.failure(http.MethodPost,
		"/v1/agents/calls/"+call.ID+"/token", map[string]any{"user_id": "!anon"})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "user_id may not start with !")
}

// Neither the reader nor the agent a chat token is for may be a user id Stream keeps for itself.
func (s *CallsSuite) TestAChatTokenIsNotMintedForAReservedUserID() {
	for field, sent := range map[string]map[string]any{
		"user_id":  {"agent_id": "agent-" + s.utils.uuid(), "user_id": "!anon"},
		"agent_id": {"agent_id": "!anon"},
	} {
		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/chat-token", sent)

		s.Equal(http.StatusBadRequest, status, field)
		s.Contains(failure, field+" may not start with !", field)
		_, made := s.chat.User("!anon")
		s.False(made, "no reserved user is made")
	}
}

func (s *CallsSuite) TestAJoinTokenInASharedAppIsMintedOnlyForAUserTheCustomerHolds() {
	// A Stream token is good for everything its user may do in the app, not for one call,
	// and in the deployment's app every customer's users are side by side.
	call := s.started(store.Call{})
	mine, theirs := s.guestOf(s.app), s.guestOf(s.data.createApp())

	for _, user := range []string{"listener-" + call.ID, mine} {
		var minted CallToken
		s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost,
			"/v1/agents/calls/"+call.ID+"/token", map[string]any{"user_id": user}, &minted), user)
		s.Equal(user, minted.UserId)
	}
	for _, user := range []string{"thierry", theirs, "listener-" + s.utils.uuid()} {
		status, failure := s.serverClient.failure(http.MethodPost,
			"/v1/agents/calls/"+call.ID+"/token", map[string]any{"user_id": user})
		s.Equal(http.StatusBadRequest, status, user)
		s.Equal(errUserInSharedApp.Message, failure, user)
	}
}

func (s *CallsSuite) TestAChatTokenInASharedAppIsMintedOnlyForAUserTheCustomerHolds() {
	mine, theirs := s.guestOf(s.app), s.guestOf(s.data.createApp())

	for _, user := range []string{"reader-" + s.customerID(), mine} {
		agent := "agent-" + s.utils.uuid()
		var minted ChatToken
		s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/chat-token",
			map[string]any{"agent_id": agent, "user_id": user}, &minted), user)
		s.Equal(user, minted.UserId)
		s.Contains(s.chat.Members(agent), user)
	}
	for _, user := range []string{"thierry-" + s.utils.uuid(), theirs, "reader-" + s.utils.uuid()} {
		agent := "agent-" + s.utils.uuid()
		_, existed := s.chat.User(user)
		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/chat-token",
			map[string]any{"agent_id": agent, "user_id": user})
		s.Equal(http.StatusBadRequest, status, user)
		s.Equal(errUserInSharedApp.Message, failure, user)
		_, opened := s.chat.Channel(agent)
		s.False(opened, "a refused reader opens no channel")
		_, exists := s.chat.User(user)
		s.Equal(existed, exists, "nor is made a user")
	}
}

func (s *CallsSuite) TestAChatTokenIsRefusedForAnotherCustomersAgentChannel() {
	// Agent ids are the customers' own words, so in a shared app two customers can name
	// the same channel; the stamp says whose it is.
	agent := "agent-" + s.utils.uuid()
	s.inChannel(agent, s.data.createApp().app.ID, s.base, "said to another customer")
	before := s.chat.Members(agent)

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/chat-token",
		map[string]any{"agent_id": agent})

	s.Equal(http.StatusBadRequest, status)
	s.Equal(errChannelOfAnother.Message, failure)
	s.Equal(before, s.chat.Members(agent), "nobody is added to it")
	for _, user := range []string{"reader-" + s.customerID(), agent} {
		_, exists := s.chat.User(user)
		s.False(exists, "nor is %s made a user", user)
	}
}

func (s *CallsSuite) TestATokenInASharedAppIsNotMintedForAGuestOnlySeenInTheCustomersOwnToken() {
	// The customer signs its guest tokens itself, for any id it likes, so a guest seen in
	// one is no sign the id is not another customer's user.
	call := s.started(store.Call{})
	seen := s.guestSeenInAToken()

	for path, body := range map[string]map[string]any{
		"/v1/agents/calls/" + call.ID + "/token": {"user_id": seen},
		"/v1/agents/chat-token":                  {"agent_id": "agent-" + s.utils.uuid(), "user_id": seen},
	} {
		status, failure := s.serverClient.failure(http.MethodPost, path, body)
		s.Equal(http.StatusBadRequest, status, path)
		s.Equal(errUserInSharedApp.Message, failure, path)
	}
}

func (s *CallsSuite) TestAGuestImportedAsMintedIsNoGuestTheRouterMinted() {
	// An imported row is the customer's word, and the customer may name any id in it; that
	// the router handed a guest out is this deployment's to say.
	call := s.started(store.Call{})
	imported := "visitor-" + s.utils.uuid()
	row := `{"customer_id":"x","id":"` + imported + `","kind":"guest","minted":true,"name":"","email":"",` +
		`"timezone":"","custom":{},"created_at":"2026-01-01T00:00:00Z"}`
	var written struct {
		Tables map[string]int64 `json:"tables"`
	}
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import",
		`{"table":"users","row":`+row+"}\n", &written))
	s.Require().Equal(int64(1), written.Tables["users"])
	held, err := s.store.Users.GetGuest(context.Background(), s.customerID(), imported)
	s.Require().NoError(err, "the guest is imported")
	s.False(held.Minted)

	for path, body := range map[string]map[string]any{
		"/v1/agents/calls/" + call.ID + "/token": {"user_id": imported},
		"/v1/agents/chat-token":                  {"agent_id": "agent-" + s.utils.uuid(), "user_id": imported},
	} {
		status, failure := s.serverClient.failure(http.MethodPost, path, body)
		s.Equal(http.StatusBadRequest, status, path)
		s.Equal(errUserInSharedApp.Message, failure, path)
	}
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/guests",
		map[string]any{"id": imported})
	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "not a guest of this app")
}

func (s *CallsSuite) TestAChatTokenReadsTheCustomersOwnAndUnstampedChannels() {
	stamped, unstamped := "agent-"+s.utils.uuid(), "agent-"+s.utils.uuid()
	s.inChannel(stamped, s.customerID(), s.base, "said to this customer")
	creator := "vision-agent"
	_, err := s.chat.Client.Chat().GetOrCreateChannel(context.Background(), chatlog.ChannelType, unstamped,
		&getstream.GetOrCreateChannelRequest{Data: &getstream.ChannelInput{CreatedByID: &creator}})
	s.Require().NoError(err)

	for _, agent := range []string{stamped, unstamped} {
		s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/chat-token",
			map[string]any{"agent_id": agent}, nil), agent)
		s.Contains(s.chat.Members(agent), "reader-"+s.customerID(), agent)
	}
}

func (s *CallsSuite) TestAChannelAChatTokenOpensIsStampedForTheCustomer() {
	// Stamped, a channel opened before the agent first speaks is not another customer's
	// to join afterwards.
	agent := "agent-" + s.utils.uuid()
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/chat-token",
		map[string]any{"agent_id": agent}, nil))

	opened, _ := s.chat.Channel(agent)
	custom, _ := opened["custom"].(map[string]any)
	s.Equal(s.customerID(), custom[conversation.CustomerField])
}

func (s *CallsSuite) TestACallAnotherAppHoldsCannotBeJoined() {
	// Handing out a token for somebody else's call would be handing out their call.
	call := s.started(store.Call{})

	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodPost, "/v1/agents/calls/"+call.ID+"/token", map[string]any{}, nil)
	})
}

func (s *CallsSuite) TestTheRunningCallsAreTheOnesThatHaveNotEnded() {
	running := s.started(store.Call{StartedAt: s.base})
	finished := s.started(store.Call{StartedAt: s.base.Add(-time.Hour)})
	s.Require().NoError(s.store.Calls.Finish(context.Background(), finished.CustomerID, finished.ID, s.base))

	var listed []Call
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/calls?running=true", nil, &listed))
	s.Require().Len(listed, 1)
	s.Equal(running.ID, listed[0].Id)

	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/calls", nil, &listed))
	s.Require().Len(listed, 2, "both calls happened")
	s.Equal(running.ID, listed[0].Id, "newest first")
	s.NotNil(listed[1].EndedAt)
}

func (s *CallsSuite) TestWhatACallDecidedIsFoundByTheRowRecordingIt() {
	// A dashboard holds the row's id, and the agent wrote its reasoning against the call
	// it joined. Reading one by the other is what puts the decision log on the page.
	// Decisions are kept for an hour, so this call is a recent one.
	call := s.started(store.Call{StartedAt: time.Now().Add(-time.Minute)})
	s.Require().NoError(s.store.CallEvents.InsertMany(context.Background(), []store.CallEvent{{
		CustomerID: s.customerID(), CallID: call.CallID, AgentID: call.AgentID,
		At: time.Now(), Kind: "answer", Reason: "a complete thought",
		Said: "how is the weather",
	}}))

	var decided []CallEvent
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/calls/"+call.ID+"/events", nil, &decided))
	s.Require().Len(decided, 1)
	s.Equal(DecisionKind("answer"), decided[0].Kind)
	s.Equal("how is the weather", value(decided[0].Said))
}

func (s *CallsSuite) TestDecisionsAreAskedForUpToTheTenThousandACallKeeps() {
	call := s.started(store.Call{StartedAt: time.Now().Add(-time.Minute)})
	first := store.CallEvent{
		CustomerID: s.customerID(), CallID: call.CallID, AgentID: call.AgentID,
		At: time.Now().Add(-time.Second), Kind: "wait",
	}
	second := first
	second.At, second.Kind = time.Now(), "answer"
	s.Require().NoError(s.store.CallEvents.InsertMany(context.Background(), []store.CallEvent{first, second}))

	for limit, held := range map[string]int{"1": 1, "500": 2, "10000": 2} {
		var decided []CallEvent
		s.Require().Equal(http.StatusOK,
			s.serverClient.do(http.MethodGet, "/v1/agents/calls/"+call.ID+"/events?limit="+limit, nil, &decided))
		s.Require().Len(decided, held, limit)
		s.Equal(DecisionKind("wait"), decided[0].Kind, "oldest first")
	}
	for _, limit := range []string{"0", "-1", "10001", "1000000000"} {
		status, answered := s.serverClient.failure(http.MethodGet, "/v1/agents/calls/"+call.ID+"/events?limit="+limit, nil)
		s.Equal(http.StatusBadRequest, status, limit)
		s.Contains(answered, "limit", limit)
	}
}

func (s *CallsSuite) TestATextSessionsDecisionsAreAnEmptyListNotAnError() {
	// A session held in writing has no Stream call, so no decisions are kept for it; the
	// store refuses a read without a call id, which used to answer 500.
	call := s.heldCall(store.Call{ID: s.utils.uuid(), CustomerID: s.customerID(),
		AgentID: "agent-" + s.utils.uuid(), StartedAt: time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)}, "", "")
	s.Require().Empty(call.CallID, "a text session's call names no Stream call")

	response := s.serverClient.raw(http.MethodGet, "/v1/agents/calls/"+call.ID+"/events", nil)
	s.Require().Equal(http.StatusOK, response.StatusCode)
	// A 500 for this request would log its access line at ERROR; the line names the request id.
	requestID := response.Header.Get(RequestIDHeader)
	s.Require().NotEmpty(requestID)
	served := func() string { return lineWith(s.logged, `msg="served a request"`, " request_id="+requestID) }
	s.Require().Eventually(func() bool { return served() != "" }, 5*time.Second, 20*time.Millisecond, s.logged.String())
	s.Contains(served(), "level=INFO", "the request was answered 200, so it is not an ERROR")

	var decided []CallEvent
	s.Require().NoError(json.NewDecoder(response.Body).Decode(&decided))
	s.Empty(decided)
}

func (s *CallsSuite) TestAFinishedNativeCallNamesTheConversationAndSubagentModels() {
	call := s.started(store.Call{STS: "stub/stub-sts", Subagent: "stub/stub-llm"})
	for modality, model := range map[string]string{"sts": "stub-sts", "llm": "stub-llm"} {
		s.Require().NoError(insertRequest(context.Background(), s.store, &store.Request{
			CustomerID: s.customerID(), AgentID: call.AgentID,
			Modality: modality, Provider: "stub", Model: model,
			StartedAt: s.base.Add(time.Second), Success: true,
		}))
	}
	s.Require().NoError(s.store.Calls.Finish(context.Background(), call.CustomerID, call.ID, s.base.Add(time.Minute)))

	var rendered Call
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/calls/"+call.ID, nil, &rendered))
	s.Equal("stub/stub-sts", value(rendered.StsUsed))
	s.Equal("stub/stub-llm", value(rendered.SubagentUsed))
	s.Nil(rendered.SttUsed)
	s.Nil(rendered.LlmUsed)
	s.Nil(rendered.TtsUsed)
}

func (s *CallsSuite) TestAFinishedCallReportsWhatItSpentAndTheSessionItWasPartOf() {
	call := s.heldCall(store.Call{ID: s.utils.uuid(), CustomerID: s.customerID(), CallID: s.utils.callID(),
		AgentID: "agent-" + s.utils.uuid(), StartedAt: s.base}, "", "")
	s.Require().NoError(insertRequest(context.Background(), s.store, &store.Request{
		CustomerID: s.customerID(), AgentID: call.AgentID,
		Modality: "llm", Provider: "stub", Model: "stub-llm",
		StartedAt:   s.base.Add(time.Second),
		InputTokens: 900, CachedInputTokens: 400, OutputTokens: 150,
		CostMicros: 2500, Success: true,
	}))
	s.Require().NoError(s.store.Calls.Finish(context.Background(), call.CustomerID, call.ID, s.base.Add(time.Minute)))

	var rendered Call
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/calls/"+call.ID, nil, &rendered))
	s.Equal(call.SessionID, value(rendered.SessionId))
	s.Require().NotNil(rendered.Usage)
	s.Equal(int64(900), rendered.Usage.InputTokens)
	s.Equal(int64(400), rendered.Usage.CachedInputTokens)
	s.Equal(int64(150), rendered.Usage.OutputTokens)
	s.Equal(int64(2500), rendered.Usage.CostMicros)
	s.Equal(int64(1), rendered.Usage.Requests)
}

func (s *CallsSuite) TestACallStillRunningIsNotToldWhatItHasSpentSoFar() {
	call := s.started(store.Call{})
	s.Require().NoError(insertRequest(context.Background(), s.store, &store.Request{
		CustomerID: s.customerID(), AgentID: call.AgentID,
		Modality: "llm", Provider: "stub", Model: "stub-llm",
		StartedAt: s.base.Add(time.Second), InputTokens: 900, Success: true,
	}))

	var rendered Call
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/calls/"+call.ID, nil, &rendered))
	s.Nil(rendered.Usage, "what a conversation cost is a question asked after it")
}

func (s *CallsSuite) TestACallsTokensAreReadWhileItRunsByModelAndByWhatThePromptsCarried() {
	call := s.inSession("", store.Call{})
	for _, request := range []store.Request{
		{Modality: "llm", Provider: "stub", Model: "stub-llm", InputTokens: 900, CachedInputTokens: 400,
			OutputTokens: 150, InputParts: store.InputParts{InstructionTokens: 300, MessageTokens: 100, ToolDefinitionTokens: 500}},
		{Modality: "llm", Provider: "stub", Model: "stub-llm", InputTokens: 100, OutputTokens: 10,
			InputParts: store.InputParts{MessageTokens: 40, ImageTokens: 60}},
		{Modality: "llm", Provider: "vision", Model: "vision-model", InputTokens: 50, OutputTokens: 5,
			InputParts: store.InputParts{VideoTokens: 50}},
		{Modality: "tts", Provider: "stub", Model: "stub-tts", Characters: 80},
	} {
		request.CustomerID, request.AgentID = s.customerID(), call.AgentID
		request.StartedAt, request.Success = s.base.Add(time.Second), true
		s.Require().NoError(insertRequest(context.Background(), s.store, &request))
	}

	var spent CallTokens
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/sessions/"+call.SessionID+"/tokens", nil, &spent))
	s.Equal(int64(1050), spent.InputTokens)
	s.Equal(int64(400), spent.CachedInputTokens)
	s.Equal(int64(165), spent.OutputTokens)
	s.Equal(InputParts{Instructions: 300, Messages: 140, ToolDefinitions: 500, Images: 60, Video: 50}, spent.InputParts)
	s.Require().Len(spent.Models, 3, "a voice is a model too, though it reads no tokens")
	s.Equal("stub-llm", spent.Models[0].Model, "the busiest first")
	s.Equal(int64(2), spent.Models[0].Requests)
	s.Equal(InputParts{Video: 50}, spent.Models[1].InputParts)
}

func (s *CallsSuite) TestACallsCostIsSplitByWhereItCameFrom() {
	call := s.inSession("", store.Call{})
	for _, request := range []store.Request{
		{Modality: "llm", Provider: "stub", Model: "stub-llm", InputTokens: 1000, OutputTokens: 100,
			CostMicros: 1500, OutputCostMicros: 500,
			InputParts: store.InputParts{InstructionTokens: 200, ToolDefinitionTokens: 799, MessageTokens: 1}},
		{Modality: "llm", Provider: "stub", Model: "stub-llm", InputTokens: 1000, CostMicros: 1000},
		{Modality: "tts", Provider: "stub", Model: "stub-tts", Characters: 80, CostMicros: 300},
	} {
		request.CustomerID, request.AgentID = s.customerID(), call.AgentID
		request.StartedAt, request.Success = s.base.Add(time.Second), true
		s.Require().NoError(insertRequest(context.Background(), s.store, &request))
	}

	var spent CallTokens
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/sessions/"+call.SessionID+"/tokens", nil, &spent))
	s.Equal(int64(2800), spent.CostMicros)
	s.Equal([]CostSource{
		{Source: "input", CostMicros: 1000},
		{Source: "tool_definitions", CostMicros: 799},
		{Source: "output", CostMicros: 500},
		{Source: "tts", CostMicros: 300},
		{Source: "instructions", CostMicros: 200},
		{Source: "messages", CostMicros: 1},
	}, spent.CostSources, "the prompt recorded without a split is input")
}

func (s *CallsSuite) TestTokensAreStillReadUnderTheCallsPathTheyMovedFrom() {
	call := s.inSession("", store.Call{})
	request := store.Request{Modality: "llm", Provider: "stub", Model: "stub-llm", InputTokens: 700, OutputTokens: 30,
		CustomerID: s.customerID(), AgentID: call.AgentID, StartedAt: s.base.Add(time.Second), Success: true}
	s.Require().NoError(insertRequest(context.Background(), s.store, &request))

	var moved, kept CallTokens
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/sessions/"+call.SessionID+"/tokens", nil, &moved))
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/calls/"+call.ID+"/tokens", nil, &kept))
	s.Equal(int64(700), moved.InputTokens)
	s.Equal(moved, kept)

	status, body := s.serverClient.call(http.MethodGet, "/v1/agents/sessions/nobody/tokens", nil)
	s.Equal(http.StatusNotFound, status)
	s.Contains(string(body), `"code":"`+codeSessionNotFound+`"`)
	status, body = s.serverClient.call(http.MethodGet, "/v1/agents/calls/nobody/tokens", nil)
	s.Equal(http.StatusNotFound, status)
	s.Contains(string(body), `"code":"`+codeCallNotFound+`"`, "the old path answers as it did")
}

func (s *CallsSuite) TestAConversationRecordsWhatItsPromptsWereMadeOf() {
	request := textSession(nil)
	request.Llm, request.ConfigId = pointerTo("counted/counted-model"), s.data.instructedAgent("Answer about "+s.utils.uuid())
	opened := s.serverClient.createSession(request)
	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodPost,
		"/v1/agents/sessions/"+opened.Id+"/respond", SayRequest{Text: "How many tokens is this?"}, nil))

	var spent CallTokens
	s.Require().Eventually(func() bool {
		return s.serverClient.do(http.MethodGet, "/v1/agents/sessions/"+opened.Id+"/tokens", nil, &spent) == http.StatusOK &&
			spent.InputTokens >= 1000
	}, settleFor, 20*time.Millisecond, "the turn's request is recorded")
	parts := spent.InputParts
	s.Equal(spent.InputTokens, parts.Instructions+parts.Messages+parts.ToolDefinitions+parts.ToolUse+parts.Images+parts.Video,
		"the parts add up to what the provider counted")
	s.Positive(parts.Instructions)
	s.Positive(parts.Messages)
}

func (s *CallsSuite) TestAnotherAppsCallIsNotFound() {
	call := s.started(store.Call{})

	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodGet, "/v1/agents/calls/"+call.ID, nil, nil)
	})
}

func (s *CallsSuite) TestNobodyIsListedACallWithoutCredentials() {
	s.started(store.Call{})

	s.Equal(http.StatusUnauthorized,
		s.unauthenticatedClient.do(http.MethodGet, "/v1/agents/calls", nil, nil))
}

func (s *CallsSuite) TestTheTranscriptOfAConversationBoundCallIsReadFromTheConversation() {
	// A voice call bound to a conversation writes into the conversation's channel, not
	// the agent's own, so reading the agent's channel would find nothing it said.
	conversation := "support-" + s.utils.uuid()
	call := s.inSession("agent:"+conversation, store.Call{})
	s.inChannel(conversation, s.customerID(), call.StartedAt.Add(time.Minute), "said in the conversation")
	s.inChannel(call.AgentID, s.customerID(), call.StartedAt.Add(time.Minute), "not where this call wrote")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Require().Len(read, 1)
	s.Equal("said in the conversation", read[0].Text)
}

func (s *CallsSuite) TestACallWithoutASessionRowReadsItsAgentChannel() {
	// Calls from before session rows were kept have only their agent to go by.
	call := s.started(store.Call{})
	s.inChannel(call.AgentID, s.customerID(), call.StartedAt.Add(time.Minute), "said on the call")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Require().Len(read, 1)
	s.Equal("said on the call", read[0].Text)
}

func (s *CallsSuite) TestACallWhoseSessionRowIsGoneReadsItsSessionsChannel() {
	// A deleted session leaves its conversation in Chat and its call row behind. A new
	// conversation is named after its session, whose id a call from before calls had ids of
	// their own still has, so that is where the call wrote.
	call := s.started(store.Call{})
	s.inChannel(call.ID, s.customerID(), call.StartedAt.Add(time.Minute), "said on the deleted session")
	s.inChannel(call.AgentID, s.customerID(), call.StartedAt.Add(time.Minute), "said on another call of the agent")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Require().Len(read, 1)
	s.Equal("said on the deleted session", read[0].Text)
}

func (s *CallsSuite) TestADeletedSessionsCallBoundToAConversationReadsThatConversation() {
	// The call row names the conversation the call was bound to, which is not named after
	// the call, so with the session's row gone it is the only place that tells.
	conversation := "support-" + s.utils.uuid()
	call := s.started(store.Call{ConversationID: "agent:" + conversation})
	s.inChannel(conversation, s.customerID(), call.StartedAt.Add(time.Minute), "said in the conversation")
	s.inChannel(call.ID, s.customerID(), call.StartedAt.Add(time.Minute), "channel named after the call")
	s.inChannel(call.AgentID, s.customerID(), call.StartedAt.Add(time.Minute), "the agent's channel")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Require().Len(read, 1)
	s.Equal("said in the conversation", read[0].Text)
}

func (s *CallsSuite) TestADeletedSessionsConversationIsReadOnlyInTheCallsWindow() {
	// The conversation holds what was typed before and after the call too.
	ended := s.base.Add(10 * time.Minute)
	conversation := "support-" + s.utils.uuid()
	call := s.started(store.Call{ConversationID: "agent:" + conversation, EndedAt: &ended})
	s.inChannel(conversation, s.customerID(), call.StartedAt.Add(-time.Hour), "typed before the call")
	s.inChannel(conversation, s.customerID(), call.StartedAt.Add(time.Minute), "said on the call")
	s.inChannel(conversation, s.customerID(), ended.Add(time.Hour), "typed after the call")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Require().Len(read, 1)
	s.Equal("said on the call", read[0].Text)
}

func (s *CallsSuite) TestADeletedSessionsConversationHoldingNothingReadsTheCallsOwnChannel() {
	// A bound conversation with nothing from the call in it is no answer: a call from before
	// calls had ids of their own may have written under its id, its session's.
	conversation := "support-" + s.utils.uuid()
	call := s.started(store.Call{ConversationID: "agent:" + conversation})
	s.inChannel(conversation, s.customerID(), call.StartedAt.Add(-time.Hour), "typed long before the call")
	s.inChannel(call.ID, s.customerID(), call.StartedAt.Add(time.Minute), "said under the call's id")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Require().Len(read, 1)
	s.Equal("said under the call's id", read[0].Text)
}

func (s *CallsSuite) TestACallBoundToItsOwnChannelReadsEachChannelOnce() {
	// The call row names agent:<call id>, which is the channel the call is named after:
	// listing it twice would put the same question to Stream twice.
	id := s.utils.uuid()
	call := store.Call{ID: id, CustomerID: s.customerID(), AgentID: "agent-" + s.utils.uuid(),
		ConversationID: "agent:" + id, StartedAt: s.base}

	var channels []string
	for _, read := range s.router.transcriptReads(context.Background(), s.customerID(), call) {
		channels = append(channels, read.Channel)
	}

	s.Equal([]string{call.ID, call.AgentID}, channels)
}

func (s *CallsSuite) TestADeletedSessionsConversationOfAnotherCustomerIsNotRead() {
	conversation := "support-" + s.utils.uuid()
	call := s.started(store.Call{ConversationID: "agent:" + conversation})
	s.inChannel(conversation, s.data.createApp().app.ID, call.StartedAt.Add(time.Minute), "said to another customer")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Empty(read)
}

func (s *CallsSuite) TestASessionRowNamesTheConversationBeforeTheCallRowDoes() {
	// While the session's row is there it is the newer word on where the conversation is.
	named := "support-" + s.utils.uuid()
	callRows := "support-" + s.utils.uuid()
	call := s.inSession("agent:"+named, store.Call{ConversationID: "agent:" + callRows})
	s.inChannel(named, s.customerID(), call.StartedAt.Add(time.Minute), "said where the session's row says")
	s.inChannel(callRows, s.customerID(), call.StartedAt.Add(time.Minute), "said where the call row says")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Require().Len(read, 1)
	s.Equal("said where the session's row says", read[0].Text)
}

func (s *CallsSuite) TestASessionsConversationIsReadOnlyInTheCallsWindow() {
	// The conversation holds what was typed before and after the call too, and the
	// session's other calls.
	ended := s.base.Add(10 * time.Minute)
	conversation := "support-" + s.utils.uuid()
	call := s.inSession("agent:"+conversation, store.Call{EndedAt: &ended})
	s.inChannel(conversation, s.customerID(), call.StartedAt.Add(-time.Hour), "typed before the call")
	s.inChannel(conversation, s.customerID(), call.StartedAt.Add(time.Minute), "said on the call")
	s.inChannel(conversation, s.customerID(), ended.Add(time.Hour), "typed after the call")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Require().Len(read, 1)
	s.Equal("said on the call", read[0].Text)
}

func (s *CallsSuite) TestACallUnderItsSessionsIdReadsEachChannelOnce() {
	// A call from before calls had ids of their own has its session's: listing the channel
	// named after both twice would put the same question to Stream twice.
	id := s.utils.uuid()
	call := store.Call{ID: id, SessionID: id, CustomerID: s.customerID(), AgentID: "agent-" + s.utils.uuid(), StartedAt: s.base}

	var channels []string
	for _, read := range s.router.transcriptReads(context.Background(), s.customerID(), call) {
		channels = append(channels, read.Channel)
	}

	s.Equal([]string{call.ID, call.AgentID}, channels)
}

func (s *CallsSuite) TestACallWhoseSessionChannelHoldsNothingReadsItsAgentChannel() {
	// An empty channel under the call's id is no answer: the agent's channel may hold the call.
	call := s.started(store.Call{})
	s.inChannel(call.ID, s.customerID(), call.StartedAt.Add(-time.Hour), "said long before the call")
	s.inChannel(call.AgentID, s.customerID(), call.StartedAt.Add(time.Minute), "said on the call")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Require().Len(read, 1)
	s.Equal("said on the call", read[0].Text)
}

func (s *CallsSuite) TestACallWhoseSessionRowIsGoneDoesNotReadAnotherCustomersChannel() {
	call := s.started(store.Call{})
	s.inChannel(call.ID, s.data.createApp().app.ID, call.StartedAt.Add(time.Minute), "said to another customer")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Empty(read)
}

func (s *CallsSuite) TestAnEndedCallWithoutASessionRowReadsOnlyItsWindowOnEveryChannel() {
	// Neither channel is the call's alone: what was said before or after the call is not
	// its transcript, whichever channel is read.
	ended := s.base.Add(10 * time.Minute)
	call := s.started(store.Call{EndedAt: &ended})
	s.inChannel(call.ID, s.customerID(), ended.Add(time.Hour), "session channel, after the call")
	s.inChannel(call.AgentID, s.customerID(), call.StartedAt.Add(-time.Hour), "agent channel, before the call")
	s.inChannel(call.AgentID, s.customerID(), call.StartedAt.Add(time.Minute), "agent channel, on the call")
	s.inChannel(call.AgentID, s.customerID(), ended.Add(time.Hour), "agent channel, after the call")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Require().Len(read, 1)
	s.Equal("agent channel, on the call", read[0].Text)
}

func (s *CallsSuite) TestACallWithASessionRowReadsOnlyItsRowsChannel() {
	// The row names the conversation; a channel named after the call or its agent is
	// not read when the conversation holds nothing in the window.
	conversation := "support-" + s.utils.uuid()
	call := s.inSession("agent:"+conversation, store.Call{})
	s.inChannel(conversation, s.customerID(), call.StartedAt.Add(-time.Hour), "the conversation, before the call")
	s.inChannel(call.ID, s.customerID(), call.StartedAt.Add(time.Minute), "channel named after the call")
	s.inChannel(call.AgentID, s.customerID(), call.StartedAt.Add(time.Minute), "the agent's channel")

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Empty(read)
}

func (s *CallsSuite) TestAFailedFirstReadOfATranscriptIsReturned() {
	// A channel Chat could not answer for is no empty channel: the next one is not tried
	// as if it were, and the caller is not told nobody spoke.
	call := s.started(store.Call{})
	s.inChannel(call.AgentID, s.customerID(), call.StartedAt.Add(time.Minute), "said on the call")
	s.chat.Fail(http.MethodPost, "/chat/channels")
	s.T().Cleanup(func() { s.chat.Heal(http.MethodPost, "/chat/channels") })

	s.Equal(http.StatusInternalServerError, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, nil))
}

func (s *CallsSuite) TestTheCampaignDialerReadsOnlyItsOwnCustomersTranscript() {
	// The dialer knows the session its call opened, and reads that session's latest call.
	conversation := "support-" + s.utils.uuid()
	earlier := s.inSession("agent:"+conversation, store.Call{StartedAt: s.base.Add(-time.Hour)})
	ended := s.base.Add(-50 * time.Minute)
	s.Require().NoError(s.store.Calls.Finish(context.Background(), earlier.CustomerID, earlier.ID, ended))
	call := s.started(store.Call{SessionID: earlier.SessionID, AgentID: earlier.AgentID})
	s.inChannel(conversation, s.customerID(), earlier.StartedAt.Add(time.Minute), "said on the earlier call")
	s.inChannel(conversation, s.customerID(), call.StartedAt.Add(time.Minute), "said on the call")

	said, err := s.router.CallTranscript(context.Background(), s.customerID(), call.SessionID)
	s.Require().NoError(err)
	s.Require().Len(said, 1)
	s.Equal("said on the call", said[0].Text)

	_, err = s.router.CallTranscript(context.Background(), "customer-"+s.utils.uuid(), call.SessionID)
	s.Error(err, "another customer's call is not read")
}

func (s *CallsSuite) TestReadingACallsTranscriptCreatesNoChannel() {
	call := s.started(store.Call{})

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))

	s.Empty(read)
	_, exists := s.chat.Channel(call.AgentID)
	s.False(exists, "a call nobody wrote down leaves nothing behind in the app")
}

func (s *CallsSuite) TestATimelinePairsWhatEachCallerSaidWithTheAgentsAnswer() {
	// A caller's line is stored when they start talking, so it is older than the turn it
	// started. The times are a staging call's: Alice's line at 18:26:10.608, her turn at
	// 18:26:11.940, Bob's line at 18:26:21.409, his turn at 18:26:22.468.
	ctx := context.Background()
	call := s.started(store.Call{StartedAt: time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)})
	at := func(ms int) time.Time { return call.StartedAt.Add(time.Duration(ms) * time.Millisecond) }
	s.inChannelAs(call.AgentID, s.customerID(), "alice", at(10_608), "My favorite color is blue.")
	s.inChannelAs(call.AgentID, s.customerID(), call.AgentID, at(13_100), "Got it, your favorite color is blue.")
	s.inChannelAs(call.AgentID, s.customerID(), "bob", at(21_409), "My favorite color is green.")
	s.inChannelAs(call.AgentID, s.customerID(), call.AgentID, at(23_500), "Noted, Bob, your favorite is green.")
	for _, turn := range []store.Turn{{TurnID: "turn-1", StartedAt: at(11_940)}, {TurnID: "turn-2", StartedAt: at(22_468)}} {
		turn.CustomerID, turn.AgentID, turn.CallID = s.customerID(), call.AgentID, call.CallID
		s.Require().NoError(s.store.Turns.Insert(ctx, turn))
	}

	var timeline []TimelineEntry
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/timeline", nil, &timeline))

	s.Require().Len(timeline, 2)
	s.Equal("My favorite color is blue.", value(timeline[0].Heard))
	s.Equal("Got it, your favorite color is blue.", value(timeline[0].Said))
	s.Equal("My favorite color is green.", value(timeline[1].Heard))
	s.Equal("Noted, Bob, your favorite is green.", value(timeline[1].Said))
}

func (s *CallsSuite) TestATimelineOfACallNobodyWroteDownHasTurnsWithoutWords() {
	ctx := context.Background()
	call := s.started(store.Call{StartedAt: time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)})
	s.Require().NoError(s.store.Turns.Insert(ctx, store.Turn{CustomerID: s.customerID(), AgentID: call.AgentID,
		CallID: call.CallID, TurnID: "turn-1", StartedAt: call.StartedAt.Add(time.Second)}))

	var timeline []TimelineEntry
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/timeline", nil, &timeline))

	s.Require().Len(timeline, 1)
	s.Equal("turn-1", timeline[0].TurnId)
	s.Nil(timeline[0].Heard)
	s.Nil(timeline[0].Said)
}

func (s *CallsSuite) TestATextSessionsTimelineReadsNoFloorChecksAndLogsNoError() {
	// A session held in writing has no Stream call, so it has no call id to keep floor checks
	// under; the read for them is refused by the store, and used to be logged as an error
	// on every read of the timeline.
	ctx := context.Background()
	call := s.heldCall(store.Call{ID: s.utils.uuid(), CustomerID: s.customerID(),
		AgentID: "agent-" + s.utils.uuid(), StartedAt: time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)}, "", "")
	s.Require().Empty(call.CallID, "a text session's call names no Stream call")
	s.Require().NoError(s.store.Turns.Insert(ctx, store.Turn{CustomerID: s.customerID(), AgentID: call.AgentID,
		TurnID: "turn-1", StartedAt: call.StartedAt.Add(time.Second)}))

	var timeline []TimelineEntry
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/timeline", nil, &timeline))

	s.Require().Len(timeline, 1)
	s.Equal("turn-1", timeline[0].TurnId)
	s.Nil(timeline[0].FloorChecks)
	s.Empty(s.errorsAbout(call.ID), "reading a text session's timeline is not an error")
}

func (s *CallsSuite) TestAVoiceCallsTimelineStillCarriesItsFloorChecks() {
	ctx := context.Background()
	call := s.started(store.Call{StartedAt: time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)})
	at := call.StartedAt.Add(time.Second)
	s.Require().NoError(s.store.Turns.Insert(ctx, store.Turn{CustomerID: s.customerID(), AgentID: call.AgentID,
		CallID: call.CallID, TurnID: "turn-1", StartedAt: at}))
	check := floorChecked("turn-1", pauseFloorCheck(at.Add(-500*time.Millisecond)))
	check.CustomerID, check.CallID, check.AgentID = s.customerID(), call.CallID, call.AgentID
	s.Require().NoError(s.store.CallEvents.InsertMany(ctx, []store.CallEvent{check}))

	var timeline []TimelineEntry
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/timeline", nil, &timeline))

	s.Require().Len(timeline, 1)
	s.Require().NotNil(timeline[0].FloorChecks)
	s.Require().Len(*timeline[0].FloorChecks, 1)
	s.Equal(FloorCheckKindPause, (*timeline[0].FloorChecks)[0].Kind)
	s.Empty(s.errorsAbout(call.ID))
}

func (s *CallsSuite) TestAVoiceCallsTimelineIsAnsweredWithoutFloorChecksWhenTheirReadFails() {
	ctx := context.Background()
	call := s.started(store.Call{StartedAt: time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)})
	at := call.StartedAt.Add(time.Second)
	s.Require().NoError(s.store.Turns.Insert(ctx, store.Turn{CustomerID: s.customerID(), AgentID: call.AgentID,
		CallID: call.CallID, TurnID: "turn-1", StartedAt: at}))
	check := floorChecked("turn-1", pauseFloorCheck(at.Add(-500*time.Millisecond)))
	check.CustomerID, check.CallID, check.AgentID = s.customerID(), call.CallID, call.AgentID
	s.Require().NoError(s.store.CallEvents.InsertMany(ctx, []store.CallEvent{check}))
	s.callEventsOnClosedRedis()

	var timeline []TimelineEntry
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/timeline", nil, &timeline), "the checks are not worth failing the timeline over")

	s.Require().Len(timeline, 1, "the turns are still read")
	s.Equal("turn-1", timeline[0].TurnId)
	s.Nil(timeline[0].FloorChecks, "the checks the read could not return are not shown")
	s.Require().Len(s.errorsAbout(call.ID), 1)
	s.Contains(s.errorsAbout(call.ID)[0], `msg="could not read the floor checks for a timeline"`)
	s.Contains(s.errorsAbout(call.ID)[0], "store: floor checks")
}

func (s *CallsSuite) TestAVoiceCallsDecisionsAreNotAnEmptyListWhenTheirReadFails() {
	// The decisions are the whole of this answer, so a read that failed is no call that decided
	// nothing: the caller is told something went wrong, and the access line is an ERROR.
	call := s.started(store.Call{StartedAt: time.Now().Add(-time.Minute)})
	s.Require().NoError(s.store.CallEvents.InsertMany(context.Background(), []store.CallEvent{{
		CustomerID: s.customerID(), CallID: call.CallID, AgentID: call.AgentID,
		At: time.Now(), Kind: "answer", Reason: "a complete thought",
	}}))
	s.callEventsOnClosedRedis()

	response := s.serverClient.raw(http.MethodGet, "/v1/agents/calls/"+call.ID+"/events", nil)
	s.Equal(http.StatusInternalServerError, response.StatusCode)
	requestID := response.Header.Get(RequestIDHeader)
	s.Require().NotEmpty(requestID)
	served := func() string { return lineWith(s.logged, `msg="served a request"`, " request_id="+requestID) }
	s.Require().Eventually(func() bool { return served() != "" }, 5*time.Second, 20*time.Millisecond, s.logged.String())
	s.Contains(served(), "level=ERROR")
	s.Contains(served(), "store: call events")
}

// Defect: a call list reads every shard, so one shard being down fails it or leaves that
// shard's calls out; or it lists a call without the shard it is kept in.
func (s *CallsSuite) TestACallListWithAShardDownListsItsCallsFromTheIndex() {
	call := s.started(store.Call{})
	client, served := s.onAShardDown()
	elsewhere := s.utils.uuid()
	_, err := served.GlobalDB().ExecContext(context.Background(), `
INSERT INTO call_index (shard, id, customer_id, call_type, call_id, agent_id, direction, started_at, changed_at)
VALUES ('there', ?, ?, 'default', ?, 'agent', 'inbound', ?, now())`, elsewhere, s.customerID(), s.utils.uuid(), time.Now().UTC())
	s.Require().NoError(err)

	var listed []Call
	s.Require().Equal(http.StatusOK, client.do(http.MethodGet, "/v1/agents/calls", nil, &listed))

	regions := map[string]string{}
	for _, each := range listed {
		regions[each.Id] = value(each.Region)
	}
	s.Equal(map[string]string{call.ID: "here", elsewhere: "there"}, regions)
}

// Defect: a call kept in a shard no node answers for is read here, where it is missing, and
// answered as not found rather than as a region that could not be reached.
func (s *CallsSuite) TestACallReadOfAShardNoNodeAnswersIsUnavailable() {
	client, served := s.onAShardDown()
	elsewhere := s.utils.uuid()
	_, err := served.GlobalDB().ExecContext(context.Background(), `
INSERT INTO call_index (shard, id, customer_id, call_type, call_id, agent_id, direction, started_at, changed_at)
VALUES ('there', ?, ?, 'default', ?, 'agent', 'inbound', ?, now())`, elsewhere, s.customerID(), s.utils.uuid(), time.Now().UTC())
	s.Require().NoError(err)

	status, failure := client.failure(http.MethodGet, "/v1/agents/calls/"+elsewhere, nil)

	s.Equal(http.StatusServiceUnavailable, status)
	s.Contains(failure, errCallShardDown.Message)
}

// callEventsOnClosedRedis has the decisions and floor checks every read of the suite's store
// makes come from a Redis that is closed, as an unreachable one is. The test's end puts them back.
func (s *CallsSuite) callEventsOnClosedRedis() {
	closed, err := store.OpenShards(store.Options{DSN: testDatabase(s.T(), os.Getenv("ROUTER_POSTGRES_DSN")),
		Redis: store.Redis{Address: s.redisAddr}})
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	original := s.store.CallEvents
	s.store.CallEvents = closed.CallEvents
	s.T().Cleanup(func() { s.store.CallEvents = original })
}

// errorsAbout is the ERROR lines the router logged about the call with this row id.
func (s *CallsSuite) errorsAbout(id string) []string {
	var errors []string
	for _, line := range strings.Split(s.logged.String(), "\n") {
		if strings.Contains(line, "level=ERROR") && strings.Contains(line, "call="+id) {
			errors = append(errors, line)
		}
	}
	return errors
}

func (s *CallsSuite) TestAMembersLineMarkedAsTheAgentsIsTheMembersOnEveryReader() {
	// Ben is a member of the channel the call's transcript is in, and marks a message of his
	// as the agent's. The call's row names the user the agent wrote as, so only its lines are.
	ctx := context.Background()
	call := s.inSession("", store.Call{AgentUserID: "vision-agent", StartedAt: time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)})
	at := func(ms int) time.Time { return call.StartedAt.Add(time.Duration(ms) * time.Millisecond) }
	s.inChannelAs(call.AgentID, s.customerID(), "alice", at(10_608), "Can I swap my shift?")
	s.markedAsTheAgents(call.AgentID, "ben", at(12_500), "The agent approved Ben's shift swap.")
	s.markedAsTheAgents(call.AgentID, "vision-agent", at(13_100), "Who would you like to swap with?")
	s.Require().NoError(s.store.Turns.Insert(ctx, store.Turn{CustomerID: s.customerID(), AgentID: call.AgentID,
		CallID: call.CallID, TurnID: "turn-1", StartedAt: at(11_940)}))

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))
	s.Equal(map[string]bool{
		"alice: Can I swap my shift?":                    false,
		"ben: The agent approved Ben's shift swap.":      false,
		"vision-agent: Who would you like to swap with?": true,
	}, agentLines(read))

	var timeline []TimelineEntry
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/timeline", nil, &timeline))
	s.Require().Len(timeline, 1)
	s.Equal("Can I swap my shift? The agent approved Ben's shift swap.", value(timeline[0].Heard))
	s.Equal("Who would you like to swap with?", value(timeline[0].Said))

	// What the campaign dialer classifies an answered call from (campaign.Transcripts).
	said, err := s.router.CallTranscript(ctx, s.customerID(), call.SessionID)
	s.Require().NoError(err)
	dialed := map[string]bool{}
	for _, line := range said {
		dialed[line.Speaker] = line.Agent
	}
	s.Equal(map[string]bool{"alice": false, "ben": false, "vision-agent": true}, dialed)
}

func (s *CallsSuite) TestACallFromBeforeItsWriterWasKeptReadsAMarkedLineAsTheAgents() {
	// A call row from before the column names no writer, and its transcript reads as it did:
	// the expected values are what the base, 4b4bcbaf5, answers for the same lines.
	ctx := context.Background()
	call := s.inSession("", store.Call{StartedAt: time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)})
	at := func(ms int) time.Time { return call.StartedAt.Add(time.Duration(ms) * time.Millisecond) }
	s.inChannelAs(call.AgentID, s.customerID(), "alice", at(10_608), "Can I swap my shift?")
	s.markedAsTheAgents(call.AgentID, "ben", at(12_500), "The agent approved Ben's shift swap.")
	s.markedAsTheAgents(call.AgentID, "vision-agent", at(13_100), "Who would you like to swap with?")
	s.Require().NoError(s.store.Turns.Insert(ctx, store.Turn{CustomerID: s.customerID(), AgentID: call.AgentID,
		CallID: call.CallID, TurnID: "turn-1", StartedAt: at(11_940)}))

	var read []TranscriptMessage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/transcript", nil, &read))
	s.Equal(map[string]bool{
		"alice: Can I swap my shift?":                    false,
		"ben: The agent approved Ben's shift swap.":      true,
		"vision-agent: Who would you like to swap with?": true,
	}, agentLines(read))

	var timeline []TimelineEntry
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls/"+call.ID+"/timeline", nil, &timeline))
	s.Require().Len(timeline, 1)
	s.Equal("Can I swap my shift?", value(timeline[0].Heard))
	s.Equal("The agent approved Ben's shift swap. Who would you like to swap with?", value(timeline[0].Said))

	said, err := s.router.CallTranscript(ctx, s.customerID(), call.SessionID)
	s.Require().NoError(err)
	dialed := map[string]bool{}
	for _, line := range said {
		dialed[line.Speaker] = line.Agent
	}
	s.Equal(map[string]bool{"alice": false, "ben": true, "vision-agent": true}, dialed)
}

// agentLines is whether each line of a transcript read as the agent's, keyed by who said what.
func agentLines(read []TranscriptMessage) map[string]bool {
	lines := map[string]bool{}
	for _, line := range read {
		lines[line.Speaker+": "+line.Text] = value(line.Agent)
	}
	return lines
}

// markedAsTheAgents writes a line as speaker into a channel inChannelAs made, carrying the source
// the agent's own lines carry (chatlog.SourceField).
func (s *CallsSuite) markedAsTheAgents(channel, speaker string, at time.Time, text string) {
	s.chat.At(at)
	_, err := s.chat.Client.Chat().SendMessage(context.Background(), chatlog.ChannelType, channel,
		&getstream.SendMessageRequest{Message: getstream.MessageRequest{Text: &text, UserID: &speaker,
			Custom: map[string]any{chatlog.SourceField: chatlog.SourceAgent}}})
	s.Require().NoError(err)
}

// inChannel writes one line into an agent channel, created and stamped for a customer the
// way the router creates the channels it writes to.
func (s *CallsSuite) inChannel(channel, customer string, at time.Time, text string) {
	s.inChannelAs(channel, customer, "alice", at, text)
}

// inChannelAs is inChannel written as somebody: a line written as the agent's own id is
// what the agent said.
func (s *CallsSuite) inChannelAs(channel, customer, speaker string, at time.Time, text string) {
	ctx := context.Background()
	creator := "vision-agent"
	_, err := s.chat.Client.Chat().GetOrCreateChannel(ctx, chatlog.ChannelType, channel,
		&getstream.GetOrCreateChannelRequest{Data: &getstream.ChannelInput{
			CreatedByID: &creator, Custom: map[string]any{conversation.CustomerField: customer},
		}})
	s.Require().NoError(err)
	s.chat.At(at)
	_, err = s.chat.Client.Chat().SendMessage(ctx, chatlog.ChannelType, channel,
		&getstream.SendMessageRequest{Message: getstream.MessageRequest{Text: &text, UserID: &speaker}})
	s.Require().NoError(err)
}

// guestOf is a guest an app's page made, the suite's app being the one given afterwards.
func (s *CallsSuite) guestOf(app testApp) string {
	mine := s.app
	s.useApp(app)
	defer s.useApp(mine)
	var guest GuestUser
	s.Require().Equal(http.StatusCreated, s.anonymousClient.do(http.MethodPost, "/v1/agents/guests", nil, &guest))
	return guest.Id
}

// started records a call the suite's app is on, filling in whatever the test did not name.
func (s *CallsSuite) TestCallsCanBeListedByHowBadlyTheyWentFor() {
	ctx := context.Background()
	bad, good, unreviewed := s.started(store.Call{}), s.started(store.Call{}), s.started(store.Call{})
	two, five := 2, 5
	s.Require().NoError(s.store.Calls.Review(ctx, s.customerID(), bad.ID, "Stalled.", &two, ""))
	s.Require().NoError(s.store.Calls.Review(ctx, s.customerID(), good.ID, "Helped.", &five, ""))

	var listed []Call
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls?max_review_score=3", nil, &listed))
	s.Require().Len(listed, 1, "neither the good call nor %s, which nobody reviewed", unreviewed.ID)
	s.Equal(bad.ID, listed[0].Id)

	s.Equal(http.StatusBadRequest, s.serverClient.do(http.MethodGet,
		"/v1/agents/calls?max_review_score=9", nil, nil))
}

// inSession is started for a call of a session in conversation.
func (s *CallsSuite) inSession(conversation string, call store.Call) store.Call {
	call.CustomerID = s.customerID()
	call.ID, call.CallID = cmp.Or(call.ID, s.utils.uuid()), cmp.Or(call.CallID, s.utils.callID())
	call.AgentID = cmp.Or(call.AgentID, "agent-"+s.utils.uuid())
	if call.StartedAt.IsZero() {
		call.StartedAt = s.base
	}
	return s.heldCall(call, "", conversation)
}

func (s *CallsSuite) started(call store.Call) store.Call {
	call.CustomerID = s.customerID()
	if call.ID == "" {
		call.ID = s.utils.uuid()
	}
	if call.CallID == "" {
		call.CallID = s.utils.callID()
	}
	if call.AgentID == "" {
		call.AgentID = "agent-" + s.utils.uuid()
	}
	if call.StartedAt.IsZero() {
		call.StartedAt = s.base
	}
	s.Require().NoError(s.store.Calls.Start(context.Background(), &call))
	return call
}
