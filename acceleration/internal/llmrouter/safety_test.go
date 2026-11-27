package llmrouter

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/openai"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

const (
	gptLuna           = "gpt-5.6-luna"
	openAIKeyEnv      = "SAFETY_TEST_OPENAI_KEY"
	openAICustomerKey = "sk-customer-openai-0000000000"
)

// openAIVendor is the Responses API, which remembers the safety_identifier each request carried
// and the key it was sent with.
type openAIVendor struct {
	*httptest.Server
	mu   sync.Mutex
	sent []sentIdentity
}

// sentIdentity is what one request told OpenAI about its end user. Present is false when the
// request had no safety_identifier at all.
type sentIdentity struct {
	Present bool
	Value   string
	Key     string
	Body    string
}

func (s *LLMRouterSuite) startOpenAI() *openAIVendor {
	v := &openAIVendor{}
	v.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(body, &fields)
		sent := sentIdentity{Key: r.Header.Get("Authorization"), Body: string(body)}
		if raw, ok := fields["safety_identifier"]; ok {
			sent.Present = true
			_ = json.Unmarshal(raw, &sent.Value)
		}
		v.mu.Lock()
		v.sent = append(v.sent, sent)
		v.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\"}}\n\n")
	}))
	s.T().Cleanup(v.Close)
	return v
}

// last is what the latest request sent.
func (v *openAIVendor) last() sentIdentity {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.sent[len(v.sent)-1]
}

// safetyRouter routes GPT at the vendor on the deployment's key, with safetyKey as the key the
// identifiers are hashed under and keys as the keys customers keep.
func (s *LLMRouterSuite) safetyRouter(at *openAIVendor, safetyKey []byte, keys routing.CustomerKeys) *Router {
	s.T().Setenv(openAIKeyEnv, "sk-deployment-openai")
	router, err := New(Options{
		Registry: DefaultRegistry(),
		Config: routing.ModalityConfig{Providers: []routing.ProviderConfig{{Provider: openai.ProviderName, Model: gptLuna,
			Languages: []string{"en"}, Endpoint: at.URL, APIKeyEnv: openAIKeyEnv}}},
		Keys:      keys,
		SafetyKey: safetyKey,
	})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)
	return router
}

// askAs asks the router for one response for the app's user, whose id an end user's token
// vouched for (Attribution.UserID); an empty user is the app's own work. It returns what OpenAI
// was told.
func (s *LLMRouterSuite) askAs(router *Router, at *openAIVendor, app, user string) sentIdentity {
	ctx := routing.WithAttribution(s.ctx, routing.Attribution{UserID: user})
	_ = answeredFor(ctx, s.T(), router, app)
	return at.last()
}

func (s *LLMRouterSuite) TestOpenAIIsToldTheSameIdentifierForTheSameUserOfTheSameApp() {
	at := s.startOpenAI()
	router := s.safetyRouter(at, []byte("deployment safety key"), nil)

	first := s.askAs(router, at, "acme", "u1")
	again := s.askAs(router, at, "acme", "u1")

	s.Require().True(first.Present)
	s.Len(first.Value, 64, "an HMAC-SHA256 in hex, under OpenAI's 128-character limit")
	s.Equal(first.Value, again.Value, "a second session of the same user is the same user to OpenAI")
}

func (s *LLMRouterSuite) TestOpenAIIsToldAnotherIdentifierForAnotherUserOrAnotherApp() {
	at := s.startOpenAI()
	router := s.safetyRouter(at, []byte("deployment safety key"), nil)

	u1 := s.askAs(router, at, "acme", "u1").Value
	u2 := s.askAs(router, at, "acme", "u2").Value
	otherApp := s.askAs(router, at, "globex", "u1").Value

	s.NotEqual(u1, u2, "two users of one app")
	s.NotEqual(u1, otherApp, "the same user id in two apps is two people")
}

func (s *LLMRouterSuite) TestOpenAIIsNeverSentTheAppOrTheUserID() {
	at := s.startOpenAI()
	router := s.safetyRouter(at, []byte("deployment safety key"), nil)

	sent := s.askAs(router, at, "app-7f3c91", "alice@example.com")

	s.Require().True(sent.Present)
	s.NotContains(sent.Body, "alice@example.com")
	s.NotContains(sent.Body, "app-7f3c91")
}

func (s *LLMRouterSuite) TestAResponseWithNoEndUserCarriesTheAppsOwnIdentifier() {
	at := s.startOpenAI()
	router := s.safetyRouter(at, []byte("deployment safety key"), nil)

	app := s.askAs(router, at, "acme", "")
	appAgain := s.askAs(router, at, "acme", "")
	otherApp := s.askAs(router, at, "globex", "")
	user := s.askAs(router, at, "acme", "u1")

	s.Require().True(app.Present, "the app's own work still names who OpenAI should hold to account")
	s.NotEmpty(app.Value)
	s.Equal(app.Value, appAgain.Value)
	s.NotEqual(app.Value, otherApp.Value)
	s.NotEqual(app.Value, user.Value, "the app alone is not one of its users")
}

func (s *LLMRouterSuite) TestTheIdentifierDependsOnTheDeploymentsKey() {
	at := s.startOpenAI()

	one := s.askAs(s.safetyRouter(at, []byte("one deployment"), nil), at, "acme", "u1")
	other := s.askAs(s.safetyRouter(at, []byte("another deployment"), nil), at, "acme", "u1")

	s.NotEqual(one.Value, other.Value, "without the key nobody can check a guessed app and user id against it")
}

func (s *LLMRouterSuite) TestARouterWithNoKeySendsNoIdentifier() {
	at := s.startOpenAI()
	router := s.safetyRouter(at, nil, nil)

	sent := s.askAs(router, at, "acme", "u1")

	s.False(sent.Present, "an unkeyed hash of an app id and a user id could be checked by anyone who guesses them")
}

func (s *LLMRouterSuite) TestAResponseOnTheCustomersOwnKeyCarriesNoIdentifier() {
	at := s.startOpenAI()
	router := s.safetyRouter(at, []byte("deployment safety key"), keptKeys{"acme/openai/us": openAICustomerKey})

	own := s.askAs(router, at, "acme", "u1")
	pooled := s.askAs(router, at, "globex", "u1")

	s.Equal("Bearer "+openAICustomerKey, own.Key, "acme's response went out on its own key")
	s.False(own.Present, "the customer's own account is theirs to answer for")
	s.Equal("Bearer sk-deployment-openai", pooled.Key)
	s.True(pooled.Present, "another app on the deployment's key is still identified")
}

func (s *LLMRouterSuite) TestAFallbackOnTheDeploymentsKeyCarriesTheIdentifier() {
	at := s.startOpenAI()
	refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":{"message":"Incorrect API key provided","type":"invalid_request_error","code":"invalid_api_key"}}`)
	}))
	s.T().Cleanup(refusing.Close)
	s.T().Setenv(openAIKeyEnv, "sk-deployment-openai")
	router, err := New(Options{
		Registry: DefaultRegistry(),
		Config: routing.ModalityConfig{Providers: []routing.ProviderConfig{
			{Provider: openai.ProviderName, Model: gptLuna, Languages: []string{"en"}, Endpoint: refusing.URL, APIKeyEnv: openAIKeyEnv},
			{Provider: openai.ProviderName, Model: "gpt-5.5", Languages: []string{"en"}, Endpoint: at.URL, APIKeyEnv: openAIKeyEnv},
		}},
		SafetyKey: []byte("deployment safety key"),
	})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)
	ctx := routing.WithAttribution(s.ctx, routing.Attribution{UserID: "u1"})

	session, err := router.Start(ctx, Request{CustomerID: "acme", Providers: []string{"openai/" + gptLuna, "openai/gpt-5.5"}})
	s.Require().NoError(err)
	defer session.Close()
	stream, err := session.Create(ctx, llm.ResponseParams{Input: []llm.Message{{Role: llm.User, Content: "hello"}}})
	s.Require().NoError(err)
	_, err = llm.Collect(stream)
	s.Require().NoError(err)

	direct := s.askAs(s.safetyRouter(at, []byte("deployment safety key"), nil), at, "acme", "u1")
	s.Require().Len(at.sent, 2)
	s.True(at.sent[0].Present, "the model that answered after the first refused was built for the same user")
	s.Equal(direct.Value, at.sent[0].Value)
}
