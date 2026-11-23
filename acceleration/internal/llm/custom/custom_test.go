package custom

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
	// refuseCap answers a request that sends max_tokens the way vLLM answers one whose prompt
	// and cap together exceed the model's length.
	refuseCap bool
}

func TestCustomSuite(t *testing.T) {
	suite.Run(t, new(CustomSuite))
}

func (s *CustomSuite) SetupTest() {
	s.bodies, s.headers, s.refuseCap = nil, nil, false
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		s.Require().NoError(err)
		var body map[string]any
		s.Require().NoError(json.Unmarshal(raw, &body))
		s.mu.Lock()
		s.bodies = append(s.bodies, body)
		s.headers = append(s.headers, r.Header.Clone())
		s.mu.Unlock()

		if _, capped := body["max_tokens"]; capped && s.refuseCap {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"object":"error","message":"This model's maximum context length is 8192 tokens. However, you requested 9000 tokens (2952 in the messages, 6048 in the completion). Please reduce the length of the messages or completion.","type":"BadRequestError","param":null,"code":400}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	s.T().Cleanup(s.server.Close)
}

// ask sends params to model on the test server, with no key, and returns the body sent.
func (s *CustomSuite) ask(model string, params llm.ResponseParams) map[string]any {
	return s.askIn(model, 0, params)
}

// askIn is ask for a model served with a context of contextWindow tokens.
func (s *CustomSuite) askIn(model string, contextWindow int64, params llm.ResponseParams) map[string]any {
	provider, err := New(Options{Name: "mine", Model: model, BaseURL: s.server.URL, ContextWindow: contextWindow})
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
	body := s.ask("Qwen/Qwen3-8B", hello())

	s.EqualValues(answerTokens, body["max_tokens"], "llama.cpp otherwise generates until the context is full")
	s.NotContains(body, "max_completion_tokens")
}

func (s *CustomSuite) TestAModelThatMayThinkIsGivenRoomToThinkBeforeItAnswers() {
	// Mistral has no thinking switch the router knows, so it cannot be told not to think.
	body := s.ask("mistralai/Mistral-7B-Instruct-v0.3", hello())

	s.EqualValues(answerTokens+llm.ThinkingRoom, body["max_tokens"])
}

func (s *CustomSuite) TestTheDefaultCapLeavesRoomForThePromptInTheContext() {
	params := hello()
	params.Instructions = strings.Repeat("Be terse. ", 1600)

	body := s.askIn("Qwen/Qwen3-8B", 8192, params)

	limit, ok := body["max_tokens"].(float64)
	s.Require().True(ok)
	s.Less(int64(limit)+llm.Compose(params).Total(), int64(8192))
	s.GreaterOrEqual(int(limit), leastCap)
}

func (s *CustomSuite) TestAContextWithNoRoomLeftSendsNoDefaultCap() {
	params := hello()
	params.Instructions = strings.Repeat("Be terse. ", 3000)

	body := s.askIn("Qwen/Qwen3-8B", 8192, params)

	s.NotContains(body, "max_tokens", "a cap too small to answer in would fail what used to be answered")
}

func (s *CustomSuite) TestADefaultCapTheEndpointRefusesIsDroppedAndTheTurnAskedAgain() {
	s.refuseCap = true

	body := s.ask("mistralai/Mistral-7B-Instruct-v0.3", hello())

	s.Len(s.bodies, 2, "asked once with the cap, once without")
	s.Contains(s.bodies[0], "max_tokens")
	s.NotContains(body, "max_tokens")
}

func (s *CustomSuite) TestACapTheCallerSetIsNotDroppedWhenRefused() {
	s.refuseCap = true
	params := hello()
	params.MaxOutputTokens = 64
	provider, err := New(Options{Name: "mine", Model: "mistralai/Mistral-7B-Instruct-v0.3", BaseURL: s.server.URL})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = provider.Close() })

	_, err = provider.Create(context.Background(), params)

	s.ErrorContains(err, "maximum context length")
	s.Len(s.bodies, 1, "the caller's own cap is theirs to change")
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

func (s *CustomSuite) TestWhatTheCallerSaysWhileAToolRunsComesAfterItsResult() {
	body := s.ask("mistralai/Mistral-7B-Instruct-v0.3", llm.ResponseParams{
		Instructions: "Be terse.",
		Input: []llm.Message{
			{Role: llm.User, Content: "what's the weather in Paris?"},
			{Role: llm.Assistant, ToolCalls: []llm.ToolCall{{ID: "call_b0jupvrg", Name: "weather", Arguments: `{"city":"Paris"}`}}},
			{Role: llm.User, Content: "hello?"},
			{Role: llm.ToolResult, ToolCallID: "call_b0jupvrg", Content: "18C"},
		},
	})

	messages, ok := body["messages"].([]any)
	s.Require().True(ok)
	var roles []any
	for _, message := range messages {
		roles = append(roles, message.(map[string]any)["role"])
	}
	s.Equal([]any{"system", "user", "assistant", "tool", "assistant", "user"}, roles)
	calls := messages[2].(map[string]any)["tool_calls"].([]any)
	id := calls[0].(map[string]any)["id"]
	s.Regexp(`^[0-9A-Za-z]{9}$`, id, "Mistral's templates take nothing else")
	s.Equal(id, messages[3].(map[string]any)["tool_call_id"])
}
