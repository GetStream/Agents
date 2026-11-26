package llmrouter

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/alerts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool"
	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool/keypooltest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/anthropic"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/deepseek"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/gemini"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/qwen"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/xai"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

// keyRefusedAlert is the key_refused condition cmd/router runs, after a first check a limit
// before start that takes in what earlier tests counted, so its next check, a second after
// start, holds only what the test did. A refusal of the deployment's key is the item of the
// variable its instance's api_key_env names, deploymentKeyEnv/<status>.
func (s *LLMRouterSuite) keyRefusedAlert(start time.Time) alerts.ItemCheck {
	conditions := keypool.Alerts(time.Hour)
	s.Require().Len(conditions, 1)
	s.Require().Equal("key_refused", conditions[0].Name)
	conditions[0].Items(start.Add(-time.Hour))
	return conditions[0].Items
}

// A customer's own key the vendor refuses is counted as refused, under the vendor's variable as
// OpenAI's is, but is not the operator's to fix: the request falls back to the deployment's
// instance and key_refused stays quiet.
func (s *LLMRouterSuite) TestARefusedCustomerKeyRaisesNoKeyRefusedAlert() {
	at := startVendor(s.T(), customerKey)
	router := s.keyedRouter(keyedConfig(s.T(), at), keptKeys{"acme/anthropic/us": customerKey}, nil)
	start := time.Now()
	refused := s.keyRefusedAlert(start)
	before := keypooltest.Refused(s.T(), "ANTHROPIC_API_KEY", "401")

	response := s.answered(router)

	s.Equal("hello", response.OutputText)
	s.Equal([]string{customerKey, deploymentKey}, at.asked(), "the customer's key was refused")
	s.Equal(before+1, keypooltest.Refused(s.T(), "ANTHROPIC_API_KEY", "401"), "the refusal is still counted")
	s.Empty(refused(start.Add(time.Second)))
}

// A customer's own key on each LLM vendor an instance can be built with one for is counted,
// when refused, under the vendor's variable, as OpenAI's is: it was unknown on all but OpenAI.
func (s *LLMRouterSuite) TestARefusedCustomerKeyIsCountedUnderTheVendorsVariable() {
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":{"message":"invalid key"}}`)
	}))
	s.T().Cleanup(vendor.Close)
	for _, tc := range []struct {
		name, provider, keyVar string
		compat                 bool
	}{
		{name: "anthropic", provider: anthropic.ProviderName, keyVar: "ANTHROPIC_API_KEY"},
		{name: "anthropic compat", provider: anthropic.ProviderName, keyVar: "ANTHROPIC_API_KEY", compat: true},
		{name: "gemini", provider: gemini.ProviderName, keyVar: "GOOGLE_API_KEY"},
		{name: "xai", provider: xai.ProviderName, keyVar: "XAI_API_KEY"},
		{name: "deepseek", provider: deepseek.ProviderName, keyVar: "BASETEN_API_KEY"},
		{name: "qwen", provider: qwen.ProviderName, keyVar: "DASHSCOPE_API_KEY"},
	} {
		s.Run(tc.name, func() {
			if tc.compat {
				s.T().Setenv("ANTHROPIC_OPENAI_COMPAT", "true")
			}
			provider, err := DefaultRegistry().Build(tc.provider, routing.Spec{APIKey: "sk-customer-" + rand.Text(), BaseURL: vendor.URL, BYOK: true})
			s.Require().NoError(err)
			defer provider.Close()
			before := keypooltest.Refused(s.T(), tc.keyVar, "401")

			stream, err := provider.Create(s.ctx, llm.ResponseParams{Input: []llm.Message{{Role: llm.User, Content: "hello"}}})
			if err == nil {
				_, err = llm.Collect(stream)
			}

			s.Error(err)
			s.Equal(before+1, keypooltest.Refused(s.T(), tc.keyVar, "401"))
		})
	}
}

// The control: the deployment's own key, read from the variable its instance's api_key_env
// names, still raises key_refused when the vendor refuses it, naming the product and the model
// of the response it was refused for.
func (s *LLMRouterSuite) TestARefusedDeploymentKeyRaisesKeyRefused() {
	at := startVendor(s.T(), deploymentKey)
	router := s.keyedRouter(keyedConfig(s.T(), at), nil, nil)
	start := time.Now()
	refused := s.keyRefusedAlert(start)

	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Target: router.Config().Providers[0].Name()})
	s.Require().NoError(err)
	defer session.Close()
	_, err = collected(s.ctx, session)

	s.Error(err)
	s.Equal([]string{deploymentKey}, at.asked())
	held := refused(start.Add(time.Second))
	s.Len(held, 1)
	item := deploymentKeyEnv + "/401"
	s.Require().Contains(held, item, "named for the variable, not unknown")
	s.Equal(1.0, held[item]["increase"], "the one refusal")
	s.Equal([]string{"llm"}, held[item]["product"])
	s.Equal([]string{router.Config().Providers[0].Model}, held[item]["model"])
}

// An instance whose keys sit in its variable and its numbered ones is the router's own: a
// refusal of its key raises key_refused. Random keys: what keypool read is process-global.
func (s *LLMRouterSuite) TestKeysOfAnInstanceVariableAndItsNumberedOnesAreOurs() {
	first, second := "sk-ant-first-"+rand.Text(), "sk-ant-second-"+rand.Text()
	at := startVendor(s.T(), first, second)
	config := keyedConfig(s.T(), at)
	s.T().Setenv(deploymentKeyEnv, first)
	s.T().Setenv(deploymentKeyEnv+"_1", second)
	router := s.keyedRouter(config, nil, nil)
	start := time.Now()
	refused := s.keyRefusedAlert(start)

	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Target: router.Config().Providers[0].Name()})
	s.Require().NoError(err)
	defer session.Close()
	_, err = collected(s.ctx, session)

	s.Error(err)
	s.Contains(refused(start.Add(time.Second)), deploymentKeyEnv+"/401")
}

// A made-up key on a customer's own model, which the model's endpoint refuses, raises no
// key_refused: it is the customer's key, the case found on staging as unknown/401.
func (s *LLMRouterSuite) TestARefusedKeyOnACustomersOwnModelRaisesNoKeyRefusedAlert() {
	madeUp := "sk-made-up-" + rand.Text()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":{"message":"Incorrect API key provided","type":"invalid_request_error","code":"invalid_api_key"}}`)
	}))
	s.T().Cleanup(endpoint.Close)
	router, err := New(Options{Registry: DefaultRegistry(), Config: keyedConfig(s.T(), startVendor(s.T())), Models: keyedModel{
		endpoint: routing.Endpoint{BaseURL: endpoint.URL + "/v1", Model: "acme/tuned-27b", APIKey: madeUp, Client: endpoint.Client()},
	}})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)
	start := time.Now()
	refused := s.keyRefusedAlert(start)
	before := keypooltest.Refused(s.T(), "unknown", "401")

	session, err := router.Start(s.ctx, Request{CustomerID: "acme", Target: "custom/own"})
	s.Require().NoError(err)
	defer session.Close()
	_, err = collected(s.ctx, session)

	s.Error(err)
	s.Equal(before+1, keypooltest.Refused(s.T(), "unknown", "401"), "the refusal is still counted")
	s.Empty(refused(start.Add(time.Second)))
}

// keyedModel is a customer who serves one model of their own, own, at endpoint.
type keyedModel struct{ endpoint routing.Endpoint }

func (m keyedModel) CustomModel(_ context.Context, _, name string) (routing.ProviderConfig, routing.Endpoint, error) {
	if name != "own" {
		return routing.ProviderConfig{}, routing.Endpoint{}, fmt.Errorf("routing: %q is not one of this customer's models", name)
	}
	return routing.ProviderConfig{Provider: routing.CustomProvider, Model: name}, m.endpoint, nil
}

// collected asks session for one response and reads it to its end.
func collected(ctx context.Context, session *Session) (llm.Response, error) {
	stream, err := session.Create(ctx, llm.ResponseParams{Input: []llm.Message{{Role: llm.User, Content: "hello"}}})
	if err != nil {
		return llm.Response{}, err
	}
	return llm.Collect(stream)
}
