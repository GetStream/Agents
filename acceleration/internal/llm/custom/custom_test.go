package custom

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

// CustomSuite asks a customer's endpoint, served here over real HTTP, and checks what it is
// sent: whatever the weights, the request has to render through their own chat template.
type CustomSuite struct {
	suite.Suite

	server *httptest.Server
	mu     sync.Mutex
	// bodies and headers are what each request sent.
	bodies  []map[string]any
	headers []http.Header
}

func TestCustomSuite(t *testing.T) {
	suite.Run(t, new(CustomSuite))
}

func (s *CustomSuite) SetupTest() {
	s.bodies, s.headers = nil, nil
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		s.Require().NoError(err)
		var body map[string]any
		s.Require().NoError(json.Unmarshal(raw, &body))
		s.mu.Lock()
		s.bodies = append(s.bodies, body)
		s.headers = append(s.headers, r.Header.Clone())
		s.mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	s.T().Cleanup(s.server.Close)
}

// ask sends params to model on the test server, with no key, and returns the body sent.
func (s *CustomSuite) ask(model string, params llm.ResponseParams) map[string]any {
	provider, err := New(Options{Name: "mine", Model: model, BaseURL: s.server.URL})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = provider.Close() })

	stream, err := provider.Create(context.Background(), params)
	s.Require().NoError(err)
	response, err := llm.Collect(stream)
	s.Require().NoError(err)
	s.Equal("ok", response.OutputText)
	return s.bodies[len(s.bodies)-1]
}

func hello() llm.ResponseParams {
	return llm.ResponseParams{Input: []llm.Message{{Role: llm.User, Content: "hi"}}}
}

func (s *CustomSuite) TestAReplyIsCappedWhenTheCallerSetNoCap() {
	body := s.ask("mistralai/Mistral-7B-Instruct-v0.3", hello())

	s.EqualValues(defaultMaxTokens, body["max_tokens"], "llama.cpp otherwise generates until the context is full")
	s.NotContains(body, "max_completion_tokens")
}

func (s *CustomSuite) TestTheCallersCapIsSentAsMaxTokens() {
	params := hello()
	params.MaxOutputTokens = 64

	body := s.ask("mistralai/Mistral-7B-Instruct-v0.3", params)

	s.EqualValues(64, body["max_tokens"], "Ollama reads only max_tokens")
	s.EqualValues(64, body["max_completion_tokens"])
}

func (s *CustomSuite) TestAnEndpointWithNoKeyIsSentNoAuthorization() {
	s.ask("mistralai/Mistral-7B-Instruct-v0.3", hello())

	s.Empty(s.headers[0].Values("Authorization"), "not Bearer none")
}

func (s *CustomSuite) TestTheThinkingSwitchGoesOnlyToAFamilyThatHasOne() {
	mistral := s.ask("mistralai/Mistral-7B-Instruct-v0.3", hello())
	qwen := s.ask("Qwen/Qwen3-8B", hello())

	s.NotContains(mistral, "chat_template_kwargs")
	s.Equal(map[string]any{"enable_thinking": false}, qwen["chat_template_kwargs"])
}

func (s *CustomSuite) TestTheConversationIsShapedForTheWeightsOwnTemplate() {
	body := s.ask("mistralai/Mistral-7B-Instruct-v0.3", llm.ResponseParams{
		Instructions: "Be terse.",
		Input: []llm.Message{
			{Role: llm.User, Content: "what's the weather in Paris?"},
			{Role: llm.System, Content: "Today is Monday."},
			{Role: llm.Assistant, ToolCalls: []llm.ToolCall{{ID: "call-1", Name: "weather", Arguments: `{"city":"Paris"}`}}},
			{Role: llm.ToolResult, ToolCallID: "call-1", Content: "18C"},
		},
	})

	messages, ok := body["messages"].([]any)
	s.Require().True(ok)
	var roles []any
	for _, message := range messages {
		roles = append(roles, message.(map[string]any)["role"])
	}
	s.Equal([]any{"system", "user", "assistant", "tool"}, roles)
	s.Equal("what's the weather in Paris?\n\nToday is Monday.", messages[1].(map[string]any)["content"])
	s.Equal("", messages[2].(map[string]any)["content"])
}
