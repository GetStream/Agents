package mem0

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/memory"
)

// platform stands in for mem0's API so the wire contract can be tested without a key.
type platform struct {
	server *httptest.Server

	path    string
	auth    string
	body    map[string]any
	status  int
	respond string
	// replies are answered in order before respond is, for a conversation of several calls.
	replies []string
	// statuses, when set, are the status of each of replies in turn.
	statuses []int
	// requests is every call that arrived, as method and path, in order.
	requests []string
	// delay is how long each call takes to be answered.
	delay time.Duration
}

func newPlatform() *platform {
	stub := &platform{status: http.StatusOK, respond: `{"results":[]}`}
	stub.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stub.path = r.URL.Path
		stub.auth = r.Header.Get("Authorization")
		stub.requests = append(stub.requests, r.Method+" "+r.URL.RequestURI())

		raw, _ := io.ReadAll(r.Body)
		stub.body = map[string]any{}
		_ = json.Unmarshal(raw, &stub.body)

		time.Sleep(stub.delay)
		reply, status := stub.respond, stub.status
		if len(stub.replies) > 0 {
			reply, stub.replies = stub.replies[0], stub.replies[1:]
			if len(stub.statuses) > 0 {
				status, stub.statuses = stub.statuses[0], stub.statuses[1:]
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}))
	return stub
}

type Mem0Suite struct {
	suite.Suite
	ctx      context.Context
	platform *platform
	store    *Store
	scope    memory.Scope
}

func TestMem0Suite(t *testing.T) {
	suite.Run(t, new(Mem0Suite))
}

func (s *Mem0Suite) SetupTest() {
	s.ctx = context.Background()
	s.platform = newPlatform()
	s.T().Cleanup(s.platform.server.Close)

	store, err := New(Options{
		APIKey:  "test-key",
		BaseURL: s.platform.server.URL,
		Logger:  slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	s.store = store
	s.scope = memory.Scope{AppID: "router", UserID: "acme", AgentID: "support", RunID: "session-1"}
}

func (s *Mem0Suite) TestAKeyIsRequired() {
	s.T().Setenv("MEM0_API_KEY", "")

	_, err := New(Options{})

	s.ErrorContains(err, "MEM0_API_KEY")
}

func (s *Mem0Suite) TestTheKeyIsSentAsATokenHeader() {
	_, err := s.store.Recall(s.ctx, memory.Query{Scope: s.scope, Text: "where do they live"})
	s.Require().NoError(err)

	s.Equal("Token test-key", s.platform.auth)
}

func (s *Mem0Suite) TestEveryCallSendsTheOneKeyFromTheEnvironment() {
	s.T().Setenv("MEM0_API_KEY", "env-key")
	s.T().Setenv("MEM0_API_KEY_1", "another-projects-key")
	store, err := New(Options{BaseURL: s.platform.server.URL, Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)

	for range 2 {
		_, err := store.Recall(s.ctx, memory.Query{Scope: s.scope, Text: "where do they live"})
		s.Require().NoError(err)
		s.Equal("Token env-key", s.platform.auth, "a second key would be another mem0 project")
	}
}

func (s *Mem0Suite) TestRecallScopesTheSearchToWhoTheMemoriesBelongTo() {
	_, err := s.store.Recall(s.ctx, memory.Query{Scope: s.scope, Text: "where do they live", Limit: 3})
	s.Require().NoError(err)

	s.Equal("/v3/memories/search/", s.platform.path)
	s.Equal("where do they live", s.platform.body["query"])
	s.Equal(float64(3), s.platform.body["top_k"])
	s.Equal(map[string]any{"user_id": "acme", "app_id": "router"}, s.platform.body["filters"],
		"v3 rejects entity ids at the top level, they belong in filters; the agent and run are not recalled by")
}

func (s *Mem0Suite) TestTheCallersLabelsNarrowRecallThroughMetadata() {
	s.scope.Extra = map[string]string{"company_id": "12312", "user_id": "somebody-else"}

	_, err := s.store.Recall(s.ctx, memory.Query{Scope: s.scope, Text: "anything"})
	s.Require().NoError(err)

	s.Equal(map[string]any{
		"user_id":  "acme",
		"app_id":   "router",
		"metadata": map[string]any{"company_id": "12312", "user_id": "somebody-else"},
	}, s.platform.body["filters"], "v3 refuses unknown top-level keys, and a label must not rewrite the user")
}

func (s *Mem0Suite) TestRecallReturnsWhatIsKnownMostRelevantFirst() {
	s.platform.respond = `{"results":[
		{"id":"m1","memory":"Lives in Austin","score":0.9},
		{"id":"m2","memory":"Allergic to nuts","score":0.4}
	]}`

	recalled, err := s.store.Recall(s.ctx, memory.Query{Scope: s.scope, Text: "anything"})
	s.Require().NoError(err)

	s.Require().Len(recalled.Memories, 2)
	s.Equal(memory.Memory{ID: "m1", Text: "Lives in Austin", Score: 0.9}, recalled.Memories[0])
	s.Equal("Allergic to nuts", recalled.Memories[1].Text)
	s.Equal(memory.Billed{Requests: 1}, recalled.Billed, "a search is one retrieval request")
}

func (s *Mem0Suite) TestJoiningWithoutAQuestionUsesANonemptyRecallQuery() {
	s.platform.respond = `{"results":[{"id":"app","memory":"Building a social network for cats"}]}`
	for _, question := range []string{"", " \n "} {
		recalled, err := s.store.Recall(s.ctx, memory.Query{Scope: s.scope, Text: question})
		s.Require().NoError(err)
		s.NotEmpty(s.platform.body["query"])
		s.Equal(map[string]any{"user_id": "acme", "app_id": "router"}, s.platform.body["filters"])
		s.Require().Len(recalled.Memories, 1)
		s.Equal("Building a social network for cats", recalled.Memories[0].Text)
	}
}

func (s *Mem0Suite) TestRecallWithoutAUserIsRejectedBeforeTheNetwork() {
	_, err := s.store.Recall(s.ctx, memory.Query{Text: "anything"})

	s.ErrorContains(err, "user id")
	s.Empty(s.platform.path, "an unscoped recall would read somebody else's memories")
}

func (s *Mem0Suite) TestRememberHandsTheConversationOver() {
	billed, err := s.store.Remember(s.ctx, s.scope, []llm.Message{
		{Role: llm.User, Content: "I moved to Austin"},
		{Role: llm.Assistant, Content: "Noted."},
	})
	s.Require().NoError(err)
	s.Equal(memory.Billed{Requests: 1}, billed, "an exchange is one add request")

	s.Equal("/v3/memories/add/", s.platform.path)
	s.Equal("acme", s.platform.body["user_id"])
	s.Equal("router", s.platform.body["app_id"])
	s.Equal("support", s.platform.body["agent_id"])
	s.Equal("session-1", s.platform.body["run_id"])
	s.NotContains(s.platform.body, "metadata", "a session with no labels writes none")
	s.Equal([]any{
		map[string]any{"role": "user", "content": "I moved to Austin"},
		map[string]any{"role": "assistant", "content": "Noted."},
	}, s.platform.body["messages"])
}

func (s *Mem0Suite) TestRememberLabelsWhatItWritesSoRecallCanFilterOnIt() {
	s.scope.Extra = map[string]string{"company_id": "12312"}

	_, err := s.store.Remember(s.ctx, s.scope, []llm.Message{{Role: llm.User, Content: "I moved to Austin"}})
	s.Require().NoError(err)

	s.Equal(map[string]any{"company_id": "12312"}, s.platform.body["metadata"])
}

func (s *Mem0Suite) TestRememberWritesTheTurnBesideTheLabelsWithoutChangingThem() {
	s.scope.Extra = map[string]string{"company_id": "12312"}
	s.scope.TurnID = "turn-7"

	_, err := s.store.Remember(s.ctx, s.scope, []llm.Message{{Role: llm.User, Content: "I am vegetarian"}})
	s.Require().NoError(err)

	s.Equal(map[string]any{"company_id": "12312", "agent_turn_id": "turn-7"}, s.platform.body["metadata"])
	s.Equal(map[string]string{"company_id": "12312"}, s.scope.Extra, "the session's labels are shared by every exchange")
}

func (s *Mem0Suite) TestTheTurnARecallIsMadeInNarrowsNothing() {
	s.scope.Extra = map[string]string{"company_id": "12312"}
	s.scope.TurnID = "turn-7"

	_, err := s.store.Recall(s.ctx, memory.Query{Scope: s.scope, Text: "anything"})
	s.Require().NoError(err)

	s.Equal(map[string]any{
		"user_id":  "acme",
		"app_id":   "router",
		"metadata": map[string]any{"company_id": "12312"},
	}, s.platform.body["filters"], "filtering on the turn would recall only what that turn taught")
}

// The session is listed once, a page at a time, and only the memories naming one of the turns
// are deleted: one with another turn, or none, is kept whatever mem0 made of the listing.
func (s *Mem0Suite) TestForgettingTurnsDeletesOnlyTheSessionsMemoriesNamingThem() {
	var calls []captured
	s.platform.server.Config.Handler = capturing(s.platform.server.Config.Handler, &calls)
	s.platform.replies = []string{
		`{"results":[{"id":"m1","metadata":{"agent_turn_id":"turn-7"}},{"id":"m2","metadata":{"agent_turn_id":"turn-9"}}],"next":"https://api.mem0.ai/v3/memories/?page=2"}`,
		`{"results":[{"id":"m3","metadata":{"agent_turn_id":"turn-8","company_id":"12312"}},{"id":"m4"}],"next":null}`,
		`{"message":"ok"}`,
	}

	billed, err := s.store.ForgetTurns(s.ctx, "acme", "session-1", []string{"turn-7", "turn-8"})
	s.Require().NoError(err)

	s.Equal(memory.Billed{Requests: 3}, billed, "two pages and a delete")
	s.Require().Len(calls, 3)
	s.Equal("POST /v3/memories/", calls[0].request)
	s.Equal(map[string]any{"app_id": "acme", "run_id": "session-1"}, calls[0].body["filters"],
		"one listing of the session, whatever the number of turns")
	s.Equal(map[string]any{"memories": []any{map[string]any{"memory_id": "m1"}, map[string]any{"memory_id": "m3"}}}, calls[2].body)
}

func (s *Mem0Suite) TestABatchOfTurnsThatFailsDoesNotStopTheOthers() {
	held := make([]string, 0, deletePageSize+1)
	for i := range deletePageSize + 1 {
		held = append(held, fmt.Sprintf(`{"id":"m%d","metadata":{"agent_turn_id":"turn-7"}}`, i))
	}
	s.platform.replies = []string{
		`{"results":[` + strings.Join(held, ",") + `],"next":null}`,
		`{"error":"overloaded"}`,
		`{"message":"ok"}`,
	}
	s.platform.statuses = []int{http.StatusOK, http.StatusInternalServerError, http.StatusOK}

	billed, err := s.store.ForgetTurns(s.ctx, "acme", "session-1", []string{"turn-7"})

	s.ErrorContains(err, "overloaded")
	s.Equal(memory.Billed{Requests: 2}, billed, "the listing and the batch that was deleted")
	s.Equal([]string{"POST /v3/memories/?page=1&page_size=200", "DELETE /v1/batch/", "DELETE /v1/batch/"}, s.platform.requests)
}

func (s *Mem0Suite) TestForgettingTurnsWithoutARunIsRejectedBeforeTheNetwork() {
	_, err := s.store.ForgetTurns(s.ctx, "acme", "", []string{"turn-7"})
	s.ErrorContains(err, "run id")
	_, err = s.store.ForgetTurns(s.ctx, "", "session-1", []string{"turn-7"})
	s.ErrorContains(err, "app id")
	s.Empty(s.platform.requests, "a turn id alone would reach another session's memories")
}

func (s *Mem0Suite) TestRememberWithoutARunIsRejectedBeforeTheNetwork() {
	s.scope.RunID = ""

	_, err := s.store.Remember(s.ctx, s.scope, []llm.Message{{Role: llm.User, Content: "I moved to Austin"}})

	s.ErrorContains(err, "run id")
	s.Empty(s.platform.path)
}

func (s *Mem0Suite) TestRememberingNothingIsNotACall() {
	for _, messages := range [][]llm.Message{nil, {{Role: llm.User}}} {
		billed, err := s.store.Remember(s.ctx, s.scope, messages)
		s.Require().NoError(err)
		s.Zero(billed.Requests)
	}

	s.Empty(s.platform.path, "an empty conversation has nothing to learn from")
}

func (s *Mem0Suite) TestForgettingASessionDeletesExactlyWhatItsFiltersList() {
	s.platform.replies = []string{
		`{"results":[{"id":"m1"},{"id":"m2"}]}`,
		`{"message":"Successfully deleted 2 memories"}`,
		`{"results":[]}`,
	}

	billed, err := s.store.ForgetRun(s.ctx, "acme", "session-1")
	s.Require().NoError(err)

	s.Equal(memory.Billed{Requests: 3}, billed, "two listings and a delete")
	s.Equal([]string{
		"POST /v3/memories/?page=1&page_size=100",
		"DELETE /v1/batch/",
		"POST /v3/memories/?page=1&page_size=100",
	}, s.platform.requests, "the delete-all endpoint ignores the run and would delete the whole app")
	s.Equal(map[string]any{"filters": map[string]any{"app_id": "acme", "run_id": "session-1"}}, s.platform.body)
}

func (s *Mem0Suite) TestTruncatingDeletesEveryPageOfTheUsersMemories() {
	s.platform.replies = []string{
		`{"results":[{"id":"m1"}]}`, `{"message":"ok"}`,
		`{"results":[{"id":"m2"}]}`, `{"message":"ok"}`,
		`{"results":[]}`,
	}

	billed, err := s.store.Truncate(s.ctx, "acme", "222")
	s.Require().NoError(err)

	s.Len(s.platform.requests, 5, "each page is deleted before the next is read")
	s.EqualValues(5, billed.Requests)
	s.Equal(map[string]any{"filters": map[string]any{"app_id": "acme", "user_id": "222"}}, s.platform.body)
}

func (s *Mem0Suite) TestAMemoryStillListedAfterDeletingStopsTheLoop() {
	s.platform.replies = []string{`{"results":[{"id":"m1"}]}`, `{"message":"ok"}`}
	s.platform.respond = `{"results":[{"id":"m1"}]}`

	_, err := s.store.ForgetRun(s.ctx, "acme", "session-1")

	s.ErrorContains(err, "still listed")
}

func (s *Mem0Suite) TestForgettingWithoutAnAppIsRejectedBeforeTheNetwork() {
	_, err := s.store.Truncate(s.ctx, "", "222")
	s.ErrorContains(err, "app id")
	_, err = s.store.ForgetRun(s.ctx, "acme", "")
	s.ErrorContains(err, "run id")
	s.Empty(s.platform.requests, "an unscoped delete would take somebody else's memories")
}

func (s *Mem0Suite) TestAFailureCarriesWhatThePlatformSaid() {
	s.platform.status = http.StatusBadRequest
	s.platform.respond = `{"error":"400 Bad Request"}`

	_, err := s.store.Recall(s.ctx, memory.Query{Scope: s.scope, Text: "anything"})

	s.ErrorContains(err, "400")
	s.ErrorContains(err, "Bad Request")
}

// captured is a request that reached the platform, by method and path, with its body.
type captured struct {
	request string
	body    map[string]any
}

// capturing records every request's body before handing it on.
func capturing(next http.Handler, into *[]captured) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		body := map[string]any{}
		_ = json.Unmarshal(raw, &body)
		*into = append(*into, captured{request: r.Method + " " + r.URL.Path, body: body})
		r.Body = io.NopCloser(bytes.NewReader(raw))
		next.ServeHTTP(w, r)
	})
}
