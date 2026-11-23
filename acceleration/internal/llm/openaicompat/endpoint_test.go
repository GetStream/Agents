package openaicompat

import (
	"context"
	"errors"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
)

// The tests here are about an endpoint a customer runs themselves: a chat template that takes
// only one shape of conversation, a server that answers with something other than a stream,
// one that hangs, and one that takes no key.

// roleOnlyFrame is the chunk vLLM sends as soon as it accepts a request, before the prompt has
// been read: it names the speaker and says nothing.
func roleOnlyFrame() string {
	return frame([]any{map[string]any{"index": 0, "delta": map[string]any{"role": "assistant", "content": ""}}}, nil)
}

// roles is the role of each message sent.
func roles(messages []map[string]any) []string {
	out := make([]string, 0, len(messages))
	for _, message := range messages {
		role, _ := message["role"].(string)
		out = append(out, role)
	}
	return out
}

// calledATool is a conversation in which the model called a tool, said nothing beside the
// call, and was answered.
func calledATool() llm.ResponseParams {
	return llm.ResponseParams{
		Instructions: "Be terse.",
		Input: []llm.Message{
			{Role: llm.User, Content: "what's the weather in Paris?"},
			{Role: llm.Assistant, ToolCalls: []llm.ToolCall{{ID: "call-1", Name: "weather", Arguments: `{"city":"Paris"}`}}},
			{Role: llm.ToolResult, ToolCallID: "call-1", Content: "18C and sunny"},
		},
	}
}

func (s *OpenAICompatSuite) TestStrictRolesSendsALaterSystemTurnAsPartOfTheUsersTurn() {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}

	s.ask(s.provider(Options{StrictRoles: true}), llm.ResponseParams{
		Instructions: "Be terse.",
		Input: []llm.Message{
			{Role: llm.User, Content: "What is the capital of France?"},
			{Role: llm.System, Content: "Today is Monday."},
		},
	})

	messages := s.sentMessages(0)
	s.Equal([]string{"system", "user"}, roles(messages))
	s.Equal("Be terse.", messages[0]["content"], "the system prompt does not change with the note")
	s.Equal("What is the capital of France?\n\nToday is Monday.", messages[1]["content"])
}

func (s *OpenAICompatSuite) TestStrictRolesJoinsTheSystemMessagesBeforeTheConversationIntoOne() {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}
	params := noted()
	params.Instructions = "Be terse."

	s.ask(s.provider(Options{StrictRoles: true}), params)

	messages := s.sentMessages(0)
	s.Equal([]string{"system", "user"}, roles(messages))
	s.Equal("Be terse.\n\nSummary so far.", messages[0]["content"])
	s.Equal("hi\n\nYour colleague has come back.", messages[1]["content"])
}

func (s *OpenAICompatSuite) TestStrictRolesKeepsTurnsThatAlreadyAlternate() {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}

	s.ask(s.provider(Options{StrictRoles: true}), llm.ResponseParams{Input: []llm.Message{
		{Role: llm.User, Content: "hi"},
		{Role: llm.Assistant, Content: "hello"},
		{Role: llm.User, Content: "bye"},
	}})

	messages := s.sentMessages(0)
	s.Equal([]string{"user", "assistant", "user"}, roles(messages))
	s.Equal("bye", messages[2]["content"])
}

func (s *OpenAICompatSuite) TestStrictRolesJoinsAUserTurnWithImagesIntoParts() {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}
	image := &llm.ImagePart{MIME: "image/png", Data: []byte("png")}
	provider := s.provider(Options{StrictRoles: true, Capabilities: llm.Capabilities{InputModalities: []string{llm.ModalityImage}}})

	s.ask(provider, llm.ResponseParams{Input: []llm.Message{
		{Role: llm.User, Parts: []llm.ContentPart{{Text: "what is this?"}, {Image: image}}},
		{Role: llm.System, Content: "Today is Monday."},
	}})

	messages := s.sentMessages(0)
	s.Equal([]string{"user"}, roles(messages))
	parts, ok := messages[0]["content"].([]any)
	s.Require().True(ok, "a turn with an image stays a list of parts")
	var kinds []any
	for _, part := range parts {
		kinds = append(kinds, part.(map[string]any)["type"])
	}
	s.Equal([]any{"text", "image_url", "text"}, kinds)
	s.Equal("Today is Monday.", parts[2].(map[string]any)["text"])
}

func (s *OpenAICompatSuite) TestStrictRolesSendsEmptyContentBesideToolCalls() {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}

	s.ask(s.provider(Options{StrictRoles: true}), calledATool())

	messages := s.sentMessages(0)
	s.Equal([]string{"system", "user", "assistant", "tool"}, roles(messages))
	s.Contains(messages[2], "content", "Mistral Small 3.2's template reads it")
	s.Equal("", messages[2]["content"])
	s.NotEmpty(messages[2]["tool_calls"])
}

func (s *OpenAICompatSuite) TestStrictRolesKeepsWhatTheModelSaidBesideItsCalls() {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}
	params := calledATool()
	params.Input[1].Content = "Let me check."

	s.ask(s.provider(Options{StrictRoles: true}), params)

	s.Equal("Let me check.", s.sentMessages(0)[2]["content"])
}

func (s *OpenAICompatSuite) TestWithoutStrictRolesTheConversationIsSentAsBefore() {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}

	s.ask(s.provider(Options{SystemTurnsAsUser: true}), calledATool())
	s.ask(s.provider(Options{SystemTurnsAsUser: true}), noted())

	s.NotContains(s.sentMessages(0)[2], "content", "a call with nothing said beside it sends no content")
	s.Equal([]string{"system", "user", "user"}, roles(s.sentMessages(1)))
}

func (s *OpenAICompatSuite) TestAnHTMLPageAnsweringAStreamIsAFailureNotAnEmptyAnswer() {
	s.contentType, s.body = "text/html", "<html>not json</html>"

	stream, err := s.provider(Options{}).Create(context.Background(), hello())

	s.Nil(stream)
	s.Require().Error(err, "a failed Create is what hands the turn to the next candidate")
	s.ErrorContains(err, "text/html")
	s.ErrorContains(err, "<html>not json</html>")
}

func (s *OpenAICompatSuite) TestAJSONAnswerToAStreamingRequestIsAFailure() {
	s.contentType, s.body = "application/json", `{"choices":[{"message":{"content":"hi"}}]}`

	_, err := s.provider(Options{}).Create(context.Background(), hello())

	s.ErrorContains(err, "application/json")
}

func (s *OpenAICompatSuite) TestAStreamWithACharsetIsStillAStream() {
	s.contentType, s.body = "text/event-stream; charset=utf-8", "data: "+textFrame("hi")+"\n\ndata: [DONE]\n\n"

	response, _ := s.ask(s.provider(Options{}), hello())

	s.Equal("hi", response.OutputText)
}

func (s *OpenAICompatSuite) TestAStreamThatEndsWithNothingIsATransientFailure() {
	s.silent = true

	stream, err := s.provider(Options{}).Create(context.Background(), hello())
	s.Require().NoError(err)
	var failed []llm.ResponseFailed
	for stream.Next() {
		if event, ok := stream.Current().(llm.ResponseFailed); ok {
			failed = append(failed, event)
		}
	}

	s.Equal(llm.StatusFailed, stream.Response().Status)
	s.Len(failed, 1, "the caller hears of it as an error event")
	s.True(llm.Transient(stream.Err()))
}

func (s *OpenAICompatSuite) TestAnAnswerOfNothingIsStillAnAnswer() {
	s.frames = []string{roleOnlyFrame(), usageFrame(5, 0, 0, 0, "stop")}

	response, _ := s.ask(s.provider(Options{}), hello())

	s.Equal(llm.StatusCompleted, response.Status)
	s.Empty(response.OutputText)
}

func (s *OpenAICompatSuite) TestAServerThatSendsNoHeadersFailsAtTheFirstTokenDeadline() {
	s.holdHeaders = make(chan struct{})
	s.T().Cleanup(func() { close(s.holdHeaders) })
	provider := s.provider(Options{FirstTokenTimeout: 100 * time.Millisecond})

	started := time.Now()
	_, err := provider.Create(context.Background(), hello())

	s.Require().Error(err)
	s.Less(time.Since(started), 5*time.Second)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.False(errors.Is(err, context.Canceled), "it is not read as barge-in")
	s.True(llm.Transient(err))
}

func (s *OpenAICompatSuite) TestAServerThatSaysNothingAfterItsHeadersTimesOut() {
	// The role-only chunk is not a token: it comes before the prompt is even read.
	s.frames = []string{roleOnlyFrame()}
	s.hold = make(chan struct{})
	s.T().Cleanup(func() { close(s.hold) })
	provider := s.provider(Options{FirstTokenTimeout: 100 * time.Millisecond})

	started := time.Now()
	stream, err := provider.Create(context.Background(), hello())
	s.Require().NoError(err)
	response := drain(stream)

	s.Less(time.Since(started), 5*time.Second, "not the whole response timeout")
	s.Equal(llm.StatusFailed, response.Status)
	s.True(response.TimedOut)
	s.ErrorIs(stream.Err(), context.DeadlineExceeded)
	s.True(llm.Transient(stream.Err()))
}

func (s *OpenAICompatSuite) TestTheFirstTokenDeadlineDoesNotCutALongAnswerShort() {
	s.frames = []string{textFrame("Hel")}
	s.hold = make(chan struct{})
	provider := s.provider(Options{FirstTokenTimeout: 50 * time.Millisecond})

	stream, err := provider.Create(context.Background(), hello())
	s.Require().NoError(err)
	for stream.Next() {
		if _, spoke := stream.Current().(llm.OutputTextDelta); spoke {
			break
		}
	}
	time.Sleep(200 * time.Millisecond)
	close(s.hold)
	response := drain(stream)

	s.Equal(llm.StatusCompleted, response.Status)
	s.NoError(stream.Err())
}

func (s *OpenAICompatSuite) TestTimeToFirstTokenCountsTheWaitForHeaders() {
	// llama.cpp and Ollama send their headers with the first token, so a clock started when
	// the headers came back reads a fraction of a millisecond.
	s.headersAfter = 150 * time.Millisecond
	s.frames = []string{textFrame("hi"), usageFrame(5, 0, 1, 0, "stop")}

	response, _ := s.ask(s.provider(Options{}), hello())

	s.GreaterOrEqual(response.TimeToFirstTokenMs, 150.0)
	s.GreaterOrEqual(response.DurationMs, response.TimeToFirstTokenMs)
}

func (s *OpenAICompatSuite) TestAKeylessEndpointIsSentNoAuthorization() {
	// The SDK reads OPENAI_API_KEY from the environment, which a router serving OpenAI has.
	s.T().Setenv("OPENAI_API_KEY", "sk-the-routers-own")
	s.refusedKey = "only-this-one"
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}
	provider, err := New(Options{Provider: "test", Model: "m", BaseURL: s.server.URL, Keyless: true})
	s.Require().NoError(err)
	s.T().Cleanup(func() { provider.Close() })

	s.ask(provider, hello())

	s.Empty(s.headers[0].Values("Authorization"))
}

func (s *OpenAICompatSuite) TestAKeylessEndpointGivenAKeyIsSentIt() {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}
	provider, err := New(Options{Provider: "test", Model: "m", BaseURL: s.server.URL, Keyless: true, APIKey: "secret"})
	s.Require().NoError(err)
	s.T().Cleanup(func() { provider.Close() })

	s.ask(provider, hello())

	s.Equal("Bearer secret", s.headers[0].Get("Authorization"))
}

func (s *OpenAICompatSuite) TestAnEndpointThatNeedsAKeyStillRefusesNone() {
	_, err := New(Options{Provider: "test", Model: "m", BaseURL: s.server.URL})

	s.ErrorContains(err, "api key is required")
}
