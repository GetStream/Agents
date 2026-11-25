package qwen

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
)

type QwenSuite struct {
	suite.Suite
	server *httptest.Server

	mu   sync.Mutex
	sent map[string]any
}

func TestQwenSuite(t *testing.T) {
	suite.Run(t, new(QwenSuite))
}

func (s *QwenSuite) SetupSuite() {
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		s.mu.Lock()
		s.sent = body
		s.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"qwen4-max","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}`+"\n\n"+
			`data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"qwen4-max","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}`+"\n\ndata: [DONE]\n\n")
	}))
}

func (s *QwenSuite) SetupTest() {
	s.T().Setenv(apiKeyEnvVar, "")
	s.T().Setenv(baseURLEnvVar, "")
	s.mu.Lock()
	s.sent = nil
	s.mu.Unlock()
}

// send makes one request through a provider built with options against the stub, and
// returns the body the stub received.
func (s *QwenSuite) send(options Options, params llm.ResponseParams) map[string]any {
	options.APIKey, options.BaseURL = "k", s.server.URL
	provider, err := New(options)
	s.Require().NoError(err)
	params.Input = []llm.Message{{Role: llm.User, Content: "hi"}}
	stream, err := provider.Create(context.Background(), params)
	s.Require().NoError(err)
	for stream.Next() {
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent
}

func (s *QwenSuite) TestCredentialsComeFromTheEnvironmentWhenNotGiven() {
	_, err := New(Options{})
	s.ErrorContains(err, apiKeyEnvVar+" is required")

	s.T().Setenv(apiKeyEnvVar, "from-env")
	provider, err := New(Options{})
	s.Require().NoError(err)
	s.Equal(ProviderName, provider.Provider())
	s.Equal(defaultModel, provider.Model())
}

func (s *QwenSuite) TestThinkingIsTurnedOffOnTheWireByDefault() {
	// Qwen4 thinks unless told not to, so saying nothing would put its whole reasoning
	// budget in front of the first word of a live answer.
	body := s.send(Options{}, llm.ResponseParams{})

	s.Equal(false, body["enable_thinking"])
	s.NotContains(body, "reasoning_effort")
}

func (s *QwenSuite) TestAnEffortIsRefusedWhileThinkingIsOff() {
	provider, err := New(Options{APIKey: "k"})
	s.Require().NoError(err)

	s.Error(provider.Capabilities().Validate(llm.ResponseParams{Reasoning: llm.ReasoningParams{Effort: "low"}}))
}

func (s *QwenSuite) TestThinkingTurnedOnSendsTheEffortAsked() {
	body := s.send(Options{Thinking: true}, llm.ResponseParams{Reasoning: llm.ReasoningParams{Effort: "xhigh"}})

	s.Equal(true, body["enable_thinking"])
	s.Equal("xhigh", body["reasoning_effort"])
}

func (s *QwenSuite) TestAnEffortQwenWouldQuietlyRemapIsRefused() {
	// Qwen4 answers medium as high rather than refusing it.
	provider, err := New(Options{APIKey: "k", Thinking: true})
	s.Require().NoError(err)

	s.Error(provider.Capabilities().Validate(llm.ResponseParams{Reasoning: llm.ReasoningParams{Effort: "medium"}}))
}

func (s *QwenSuite) TestTheBaseURLComesFromTheEnvironmentForAKeyOfAnotherRegion() {
	s.T().Setenv(baseURLEnvVar, s.server.URL)

	provider, err := New(Options{APIKey: "k"})
	s.Require().NoError(err)
	stream, err := provider.Create(context.Background(), llm.ResponseParams{Input: []llm.Message{{Role: llm.User, Content: "hi"}}})
	s.Require().NoError(err)
	for stream.Next() {
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.Equal("qwen4-max", s.sent["model"], "the request reached the endpoint the environment named")
}
