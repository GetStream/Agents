package openaicompat

import (
	"context"
	"errors"
	"strings"
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

// sse is a whole event stream that answers hi.
func sse() string { return "data: " + textFrame("hi") + "\n\ndata: [DONE]\n\n" }

func (s *OpenAICompatSuite) TestAStreamSentUnderAnotherTypeIsStillAStream() {
	for _, contentType := range []string{
		"text/plain; charset=utf-8", "application/octet-stream", "application/x-ndjson",
		"Text/Event-Stream", "text/event-stream; charset",
	} {
		s.contentType, s.body = contentType, sse()

		response, _ := s.ask(s.provider(Options{}), hello())

		s.Equal("hi", response.OutputText, contentType)
	}
}

func (s *OpenAICompatSuite) TestAStreamOpeningOnACommentIsStillAStream() {
	s.contentType, s.body = "text/plain", "\n: keep-alive\n\n"+sse()

	response, _ := s.ask(s.provider(Options{}), hello())

	s.Equal("hi", response.OutputText)
}

func (s *OpenAICompatSuite) TestABodyThatIsNoStreamIsRefusedWhateverItsType() {
	for _, contentType := range []string{"text/plain", "application/octet-stream"} {
		s.contentType, s.body = contentType, `{"error":"model is loading"}`

		_, err := s.provider(Options{}).Create(context.Background(), hello())

		s.ErrorContains(err, "model is loading", contentType)
	}
}

func (s *OpenAICompatSuite) TestAStreamThatNamesNoTypeIsLetThrough() {
	s.contentType, s.body = "", sse()

	response, _ := s.ask(s.provider(Options{}), hello())

	s.Equal("hi", response.OutputText)
}

func (s *OpenAICompatSuite) TestAPageThatNamesNoTypeIsRefused() {
	s.contentType, s.body = "", "<html>not json</html>"

	_, err := s.provider(Options{}).Create(context.Background(), hello())

	s.ErrorContains(err, "<html>not json</html>")
}

func (s *OpenAICompatSuite) TestAStreamOfNothingButDoneIsATransientFailure() {
	for _, body := range []string{"data: [DONE]\n\n", ": keep-alive\n\n: keep-alive\n\ndata: [DONE]\n\n"} {
		s.contentType, s.body = "text/event-stream", body

		stream, err := s.provider(Options{}).Create(context.Background(), hello())
		s.Require().NoError(err)

		s.Equal(llm.StatusFailed, drain(stream).Status, body)
		s.True(llm.Transient(stream.Err()), body)
	}
}

// slowAfter is a stream that sends frame, then nothing for longer than a 100ms first-token
// deadline, then its answer.
func (s *OpenAICompatSuite) slowAfter(frame string) llm.Response {
	s.frames = []string{roleOnlyFrame(), frame}
	s.hold = make(chan struct{})
	go func() {
		time.Sleep(300 * time.Millisecond)
		close(s.hold)
	}()
	stream, err := s.provider(Options{FirstTokenTimeout: 100 * time.Millisecond}).Create(context.Background(), hello())
	s.Require().NoError(err)
	response := drain(stream)
	s.NoError(stream.Err())
	return response
}

func (s *OpenAICompatSuite) TestThinkingIsAFirstTokenForTheDeadline() {
	s.Equal(llm.StatusCompleted, s.slowAfter(reasoningFrame("Let me think.")).Status)
}

func (s *OpenAICompatSuite) TestAToolCallIsAFirstTokenForTheDeadline() {
	response := s.slowAfter(toolFrame(0, "call-1", "weather", `{"city":"Paris"}`))

	s.Equal(llm.StatusCompleted, response.Status)
	s.Len(response.ToolCalls, 1)
}

// strictly sends input under StrictRoles, as custom/ does, and returns the messages sent.
func (s *OpenAICompatSuite) strictly(input ...llm.Message) []map[string]any {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}
	s.ask(s.provider(Options{StrictRoles: true, ResultsFollowCalls: true}), llm.ResponseParams{Instructions: "Be terse.", Input: input})
	return s.sentMessages(len(s.requests) - 1)
}

var (
	weatherCall = llm.Message{Role: llm.Assistant, ToolCalls: []llm.ToolCall{{ID: "call-1", Name: "weather", Arguments: `{}`}}}
	weatherSaid = llm.Message{Role: llm.ToolResult, ToolCallID: "call-1", Content: "18C"}
)

func (s *OpenAICompatSuite) TestStrictRolesPutsAnEmptyReplyBetweenAResultAndTheCallersNextWords() {
	messages := s.strictly(
		llm.Message{Role: llm.User, Content: "weather?"}, weatherCall, weatherSaid,
		llm.Message{Role: llm.User, Content: "hello?"},
	)

	s.Equal([]string{"system", "user", "assistant", "tool", "assistant", "user"}, roles(messages))
	s.Equal("", messages[4]["content"])
	s.Empty(messages[4]["tool_calls"])
}

func (s *OpenAICompatSuite) TestStrictRolesPutsALateNoteAfterAResultOnTheUsersSide() {
	messages := s.strictly(
		llm.Message{Role: llm.User, Content: "weather?"}, weatherCall, weatherSaid,
		llm.Message{Role: llm.System, Content: "Today is Monday."},
	)

	s.Equal([]string{"system", "user", "assistant", "tool", "assistant", "user"}, roles(messages))
	s.Equal("Today is Monday.", messages[5]["content"])
}

func (s *OpenAICompatSuite) TestStrictRolesMovesWhatTheCallerSaidDuringACallAfterItsResult() {
	messages := s.strictly(
		llm.Message{Role: llm.User, Content: "weather?"}, weatherCall,
		llm.Message{Role: llm.User, Content: "hello?"}, weatherSaid,
	)

	s.Equal([]string{"system", "user", "assistant", "tool", "assistant", "user"}, roles(messages))
}

func (s *OpenAICompatSuite) TestStrictRolesAddsNoEmptyReplyWhenTheModelAnsweredTheResult() {
	messages := s.strictly(
		llm.Message{Role: llm.User, Content: "weather?"}, weatherCall, weatherSaid,
		llm.Message{Role: llm.Assistant, Content: "18C."},
		llm.Message{Role: llm.User, Content: "thanks"},
	)

	s.Equal([]string{"system", "user", "assistant", "tool", "assistant", "user"}, roles(messages))
	s.Equal("18C.", messages[4]["content"])
}

func (s *OpenAICompatSuite) TestStrictRolesTellsTheModelWhatItOpenedWithInTheSystemPrompt() {
	messages := s.strictly(
		llm.Message{Role: llm.Assistant, Content: "Hi, how can I help?"},
		llm.Message{Role: llm.User, Content: "weather?"},
	)

	s.Equal([]string{"system", "user"}, roles(messages))
	s.Equal("Be terse.\n\nYou opened the conversation by saying: \"Hi, how can I help?\"", messages[0]["content"])
}

func (s *OpenAICompatSuite) TestStrictRolesJoinsTwoRepliesInARow() {
	messages := s.strictly(
		llm.Message{Role: llm.User, Content: "hi"},
		llm.Message{Role: llm.Assistant, Content: "hello"},
		llm.Message{Role: llm.Assistant, Content: "still there?"},
		llm.Message{Role: llm.User, Content: "yes"},
	)

	s.Equal([]string{"system", "user", "assistant", "user"}, roles(messages))
	s.Equal("hello\n\nstill there?", messages[2]["content"])
}

func (s *OpenAICompatSuite) TestStrictRolesSendsWhatTheModelSaidBeforeItsCallsWithThem() {
	messages := s.strictly(
		llm.Message{Role: llm.User, Content: "weather?"},
		llm.Message{Role: llm.Assistant, Content: "Let me check."}, weatherCall, weatherSaid,
		llm.Message{Role: llm.Assistant, Content: "18C."},
	)

	s.Equal([]string{"system", "user", "assistant", "tool", "assistant"}, roles(messages))
	s.Equal("Let me check.", messages[2]["content"])
	s.NotEmpty(messages[2]["tool_calls"])
}

// callIDs is the id of every call sent and every result's call, in order.
func callIDs(messages []map[string]any) []string {
	var ids []string
	for _, message := range messages {
		calls, _ := message["tool_calls"].([]any)
		for _, call := range calls {
			ids = append(ids, call.(map[string]any)["id"].(string))
		}
		if id, ok := message["tool_call_id"].(string); ok {
			ids = append(ids, id)
		}
	}
	return ids
}

func (s *OpenAICompatSuite) TestStrictRolesSendsEveryCallIDAsNineLettersAndDigits() {
	input := []llm.Message{
		{Role: llm.User, Content: "weather?"},
		{Role: llm.Assistant, ToolCalls: []llm.ToolCall{{ID: "call_b0jupvrg", Name: "weather", Arguments: `{}`}, {ID: "resp-1-tool-1", Name: "weather", Arguments: `{}`}}},
		{Role: llm.ToolResult, ToolCallID: "call_b0jupvrg", Content: "18C"},
		{Role: llm.ToolResult, ToolCallID: "resp-1-tool-1", Content: "20C"},
	}

	first := callIDs(s.strictly(input...))
	again := callIDs(s.strictly(input...))

	s.Require().Len(first, 4)
	for _, id := range first {
		s.Regexp(`^[0-9A-Za-z]{9}$`, id)
	}
	s.Equal(first[0], first[2], "a result names its call by the id the call was sent with")
	s.Equal(first[1], first[3])
	s.NotEqual(first[0], first[1])
	s.Equal(first, again, "the same history is sent the same way, so a cached prefix still matches")
}

func (s *OpenAICompatSuite) TestStrictRolesKeepsAnIDThatIsAlreadyNineLettersAndDigits() {
	// The id another call's hash would land on is already taken, so that call is hashed again.
	taken := hashCallID("call_b0jupvrg", 0)
	ids := callIDs(s.strictly(
		llm.Message{Role: llm.User, Content: "weather?"},
		llm.Message{Role: llm.Assistant, ToolCalls: []llm.ToolCall{{ID: "call_b0jupvrg", Name: "weather", Arguments: `{}`}, {ID: taken, Name: "weather", Arguments: `{}`}}},
		llm.Message{Role: llm.ToolResult, ToolCallID: "call_b0jupvrg", Content: "18C"},
		llm.Message{Role: llm.ToolResult, ToolCallID: taken, Content: "20C"},
	))

	s.Equal(taken, ids[1])
	s.Equal(hashCallID("call_b0jupvrg", 1), ids[0])
}

func (s *OpenAICompatSuite) TestAStreamedCallKeepsTheIDTheModelGaveIt() {
	s.frames = []string{toolFrame(0, "call_b0jupvrg", "weather", `{}`), usageFrame(5, 0, 1, 0, "tool_calls")}

	response, _ := s.ask(s.provider(Options{StrictRoles: true}), hello())

	s.Equal("call_b0jupvrg", response.ToolCalls[0].ID, "only what is sent is renamed")
}

func (s *OpenAICompatSuite) TestWithoutStrictRolesCallIDsAreSentAsTheyAre() {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}

	s.ask(s.provider(Options{}), calledATool())

	s.Equal([]string{"call-1", "call-1"}, callIDs(s.sentMessages(0)))
}

func (s *OpenAICompatSuite) TestAnEmptyBodyUnderAnotherTypeEndsAsAnEmptyStreamDoes() {
	s.contentType, s.body = "text/plain", "\n"

	stream, err := s.provider(Options{}).Create(context.Background(), hello())
	s.Require().NoError(err)

	s.Equal(llm.StatusFailed, drain(stream).Status)
	s.True(llm.Transient(stream.Err()))
	s.ErrorContains(stream.Err(), "ended before anything was sent")
}

func (s *OpenAICompatSuite) TestStrictRolesOpensOnAnEmptyUserTurnBeforeACallNobodyAskedFor() {
	// The agent looked up who is calling, then greeted them, before they said anything.
	messages := s.strictly(weatherCall, weatherSaid,
		llm.Message{Role: llm.Assistant, Content: "Hi Dana."},
		llm.Message{Role: llm.User, Content: "hi"},
	)

	s.Equal([]string{"system", "user", "assistant", "tool", "assistant", "user"}, roles(messages))
	s.Equal("", messages[1]["content"], "nobody had said anything yet")
	s.Equal("Be terse.", messages[0]["content"], "only a reply before anything else is an opening line")
	s.Equal("Hi Dana.", messages[4]["content"], "the greeting stays after the result it read")
}

func (s *OpenAICompatSuite) TestStrictRolesOpensOnAnEmptyUserTurnWhileTheCallIsAllThereIs() {
	messages := s.strictly(weatherCall, weatherSaid)

	s.Equal([]string{"system", "user", "assistant", "tool"}, roles(messages))
}

func (s *OpenAICompatSuite) TestStrictRolesOpensOnAnEmptyUserTurnBeforeWordsSentWithACall() {
	messages := s.strictly(
		llm.Message{Role: llm.Assistant, Content: "One moment."}, weatherCall, weatherSaid,
		llm.Message{Role: llm.User, Content: "hi"},
	)

	s.Equal([]string{"system", "user", "assistant", "tool", "assistant", "user"}, roles(messages))
	s.Equal("Be terse.\n\nYou opened the conversation by saying: \"One moment.\"", messages[0]["content"],
		"words before anything else are still the opening line")
}

func (s *OpenAICompatSuite) TestAToolResultsImagesAreLabelledWithTheIDItsCallIsSentWith() {
	s.frames = []string{textFrame("ok"), usageFrame(5, 0, 1, 0, "stop")}
	image := &llm.ImagePart{MIME: "image/png", Data: []byte("png")}
	provider := s.provider(Options{StrictRoles: true, ResultsFollowCalls: true,
		Capabilities: llm.Capabilities{InputModalities: []string{llm.ModalityImage}}})

	s.ask(provider, llm.ResponseParams{Input: []llm.Message{
		{Role: llm.User, Content: "what do the cameras show?"},
		{Role: llm.Assistant, ToolCalls: []llm.ToolCall{
			{ID: "call_front_door", Name: "camera", Arguments: `{}`},
			{ID: "call_back_door", Name: "camera", Arguments: `{}`},
		}},
		{Role: llm.ToolResult, ToolCallID: "call_front_door", Parts: []llm.ContentPart{{Text: "front"}, {Image: image}}},
		{Role: llm.ToolResult, ToolCallID: "call_back_door", Parts: []llm.ContentPart{{Text: "back"}, {Image: image}}},
	}})

	messages := s.sentMessages(0)
	ids := callIDs(messages)
	s.Require().Len(ids, 4)
	var labels []any
	for _, message := range messages {
		parts, _ := message["content"].([]any)
		for _, part := range parts {
			if text, ok := part.(map[string]any)["text"].(string); ok && strings.HasPrefix(text, imagesFrom) {
				labels = append(labels, text)
			}
		}
	}
	s.Equal([]any{imagesFrom + ids[0], imagesFrom + ids[1]}, labels)
}

func (s *OpenAICompatSuite) TestStrictRolesQuotesAnOpeningLineAsItWasSaid() {
	messages := s.strictly(
		llm.Message{Role: llm.Assistant, Content: "Hi!\nHow can I help?"},
		llm.Message{Role: llm.User, Content: "weather?"},
	)

	s.Equal("Be terse.\n\nYou opened the conversation by saying: \"Hi!\nHow can I help?\"", messages[0]["content"])
}
