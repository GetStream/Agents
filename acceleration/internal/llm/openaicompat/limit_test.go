package openaicompat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/openai/openai-go/v3"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
)

// piece is one chunk's worth of an answer that never ends.
var piece = strings.Repeat("a", 16<<10)

// endless serves an event stream that writes what next returns until the client goes away,
// and closes gone once it notices.
func (s *OpenAICompatSuite) endless(next func() string) (*LLM, <-chan struct{}) {
	provider, gone, _ := s.endlessAs("text/event-stream", next)
	return provider, gone
}

// endlessAs is endless under another Content-Type, and counts the bytes the endpoint sent.
func (s *OpenAICompatSuite) endlessAs(contentType string, next func() string) (*LLM, <-chan struct{}, *atomic.Int64) {
	gone := make(chan struct{})
	var sent atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(gone)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", contentType)
		flusher := w.(http.Flusher)
		for r.Context().Err() == nil {
			n, err := io.WriteString(w, next())
			sent.Add(int64(n))
			if err != nil {
				return
			}
			flusher.Flush()
		}
	}))
	s.T().Cleanup(server.Close)

	provider, err := New(Options{Provider: "test", Model: "test-model", APIKey: "key", BaseURL: server.URL})
	s.Require().NoError(err)
	s.T().Cleanup(func() { provider.Close() })
	return provider, gone, &sent
}

// askBounded is ask under a deadline, so a stream that is never cut short fails the test in
// seconds rather than running for the whole response timeout.
func (s *OpenAICompatSuite) askBounded(provider *LLM, params llm.ResponseParams) (llm.Response, []llm.Event) {
	ctx, cancel := context.WithTimeout(s.T().Context(), 10*time.Second)
	defer cancel()
	stream, err := provider.Create(ctx, params)
	s.Require().NoError(err)
	var events []llm.Event
	for stream.Next() {
		events = append(events, stream.Current())
	}
	s.NoError(stream.Err(), "a reply cut short at the limit is not the provider failing")
	return stream.Response(), events
}

// left fails the test unless the endpoint saw the client go away.
func (s *OpenAICompatSuite) left(gone <-chan struct{}) {
	select {
	case <-gone:
	case <-time.After(10 * time.Second):
		s.Fail("the request to the endpoint was never cancelled")
	}
}

func (s *OpenAICompatSuite) TestAnEndpointThatNeverStopsIsCutShortAtTheLimit() {
	for name, chunk := range map[string]string{
		"answer":         textFrame(piece),
		"thinking":       reasoningFrame(piece),
		"tool arguments": toolFrame(0, "", "", piece),
		"tool names":     toolFrame(0, "", piece, ""),
	} {
		s.Run(name, func() {
			provider, gone := s.endless(func() string { return "data: " + chunk + "\n\n" })

			response, events := s.askBounded(provider, withTools())

			s.Equal(llm.StatusIncomplete, response.Status)
			s.Equal(llm.ReasonMaxOutputTokens, response.IncompleteReason)
			held := 0
			for _, event := range events {
				switch event := event.(type) {
				case llm.OutputTextDelta:
					held += len(event.Delta)
				case llm.ReasoningTextDelta:
					held += len(event.Delta)
				case llm.FunctionCallArgumentsDelta:
					held += len(event.Name) + len(event.Delta)
				case llm.ResponseFailed:
					s.Fail("a reply cut short at the limit is not the provider failing", event.Err)
				}
			}
			s.LessOrEqual(held, maxAnswerBytes, "what the response holds stays within the limit")
			s.LessOrEqual(len(response.OutputText)+len(response.ReasoningText), maxAnswerBytes)
			s.Greater(held, maxAnswerBytes-len(piece)-callBytes, "and is cut at the limit rather than before it")
			s.left(gone)
		})
	}
}

func (s *OpenAICompatSuite) TestAnEndpointThatOpensToolCallsWithoutEndIsCutShort() {
	long := strings.Repeat("i", 16<<10)
	for name, call := range map[string]func(i int) string{
		"a new call a chunk":      func(i int) string { return fmt.Sprintf(`{"choices":[{"delta":{"tool_calls":[{"index":%d}]}}]}`, i) },
		"a long id a call":        func(i int) string { return toolFrame(i, fmt.Sprint(i)+long, "", "") },
		"a long signature a call": func(i int) string { return signedToolFrame(i, fmt.Sprint("c", i), "", "", long) },
	} {
		s.Run(name, func() {
			i := 0
			provider, gone := s.endless(func() string { i++; return "data: " + call(i) + "\n\n" })

			response, _ := s.askBounded(provider, withTools())

			s.Equal(llm.StatusIncomplete, response.Status)
			s.Equal(llm.ReasonMaxOutputTokens, response.IncompleteReason)
			held := 0
			for _, call := range response.ToolCalls {
				held += len(call.ID) + len(call.Name) + len(call.Arguments) + len(call.Signature)
			}
			s.LessOrEqual(len(response.ToolCalls), maxAnswerBytes/callBytes, "each call the response holds counts against the limit")
			s.LessOrEqual(held, maxAnswerBytes, "and so do their ids and signatures")
			s.left(gone)
		})
	}
}

func (s *OpenAICompatSuite) TestWhatTheResponseHoldsStaysNearTheLimit() {
	// A field the response does not read, beside each piece it does, is counted only against
	// the body's limit, so the response must not keep it alive.
	unread := `"unread":"` + strings.Repeat("u", 64<<10) + `",`
	for name, chunk := range map[string]func(i int) string{
		"the answer": func(i int) string {
			return strings.Replace(textFrame(fmt.Sprint(i)), `"content"`, unread+`"content"`, 1)
		},
		"a call's id": func(i int) string {
			return strings.Replace(toolFrame(i, fmt.Sprint("c", i), "", ""), `"function"`, unread+`"function"`, 1)
		},
		"a call's name and arguments": func(i int) string {
			return strings.Replace(toolFrame(i, "", "f", "{}"), `"function":{`, `"function":{`+unread, 1)
		},
	} {
		s.Run(name, func() {
			i := 0
			provider, gone := s.endless(func() string { i++; return "data: " + chunk(i) + "\n\n" })
			before := liveHeap()

			response, events := s.askBounded(provider, withTools())

			held := liveHeap() - before
			s.Equal(llm.StatusIncomplete, response.Status)
			s.left(gone)
			s.Less(held, int64(4*maxAnswerBytes), "the response and its events hold %d bytes", held)
			runtime.KeepAlive(response)
			runtime.KeepAlive(events)
		})
	}
}

// liveHeap is what the heap holds once garbage is collected.
func liveHeap() int64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(stats.HeapAlloc)
}

func (s *OpenAICompatSuite) TestAnEndpointThatSendsAByteAtATimeIsCutShort() {
	many := func(n int, piece string) string { return strings.TrimSuffix(strings.Repeat(piece+",", n), ",") }
	for name, chunk := range map[string]string{
		"a byte a chunk":              textFrame("a"),
		"a byte a choice":             `{"choices":[` + many(1000, `{"delta":{"content":"a"}}`) + `]}`,
		"an empty fragment of a call": `{"choices":[{"delta":{"tool_calls":[` + many(1000, `{"index":0}`) + `]}}]}`,
	} {
		s.Run(name, func() {
			provider, gone := s.endless(func() string { return "data: " + chunk + "\n\n" })

			response, events := s.askBounded(provider, withTools())

			s.Equal(llm.StatusIncomplete, response.Status)
			s.Equal(llm.ReasonMaxOutputTokens, response.IncompleteReason)
			// Besides the deltas, a few events open and settle the response.
			s.LessOrEqual(len(events), maxAnswerBytes/chunkBytes+8, "each piece counts against the limit as at least chunkBytes")
			s.left(gone)
		})
	}
}

func (s *OpenAICompatSuite) TestAnEventThatNeverEndsIsCutShort() {
	// Data lines with no blank line between them are all one event, which the stream joins
	// before any chunk reaches the response.
	for _, contentType := range []string{"text/event-stream", "text/plain"} {
		for name, line := range map[string]string{
			"data lines": "data: " + piece + "\n",
			// The SDK drops one carriage return a line, so a line of two is not empty.
			"lines of carriage returns": "data: " + piece + "\n\r\r\n",
			"a line with no end":        piece,
		} {
			s.Run(contentType+" "+name, func() {
				// The first line opens a data field, so the body reads as an event stream.
				opened := false
				provider, gone, sent := s.endlessAs(contentType, func() string {
					if !opened {
						opened = true
						return "data: " + line
					}
					return line
				})

				response, _ := s.askBounded(provider, hello())

				s.Equal(llm.StatusIncomplete, response.Status)
				s.Equal(llm.ReasonMaxOutputTokens, response.IncompleteReason)
				s.left(gone)
				s.Less(sent.Load(), int64(maxStreamBytes/2), "it is cut at the event's limit, well before the body's")
			})
		}
	}
}

func (s *OpenAICompatSuite) TestAnEventOfTheLimitCompletesAndOnePastIsCutShort() {
	// A field the response does not read pads the event, so only the event's limit decides.
	head, tail := `{"padding":"`, `","choices":[{"delta":{"content":"a"}}]}`
	for name, tc := range map[string]struct {
		counted int
		want    llm.ResponseStatus
	}{
		"at the limit":     {maxAnswerBytes, llm.StatusCompleted},
		"one byte past it": {maxAnswerBytes + 1, llm.StatusIncomplete},
	} {
		s.Run(name, func() {
			// Every byte of the event's lines counts but the line feeds, "data: " included.
			pad := tc.counted - len("data: ") - len(head) - len(tail)
			s.frames = []string{head + strings.Repeat("p", pad) + tail, textFrame("b")}

			response, _ := s.ask(s.provider(Options{}), hello())

			s.Equal(tc.want, response.Status)
		})
	}
}

func (s *OpenAICompatSuite) TestABodyPastTheLimitIsCutShort() {
	// Bytes outside the answer, such as a field the response does not read, count only against
	// the body's limit.
	chunk := strings.Replace(textFrame("a"), `"choices"`, `"padding":"`+strings.Repeat("p", 256<<10)+`","choices"`, 1)
	for _, contentType := range []string{"text/event-stream", "text/plain"} {
		s.Run(contentType, func() {
			provider, gone, sent := s.endlessAs(contentType, func() string { return "data: " + chunk + "\n\n" })

			response, _ := s.askBounded(provider, hello())

			s.Equal(llm.StatusIncomplete, response.Status)
			s.Equal(llm.ReasonMaxOutputTokens, response.IncompleteReason)
			s.left(gone)
			s.GreaterOrEqual(sent.Load(), int64(maxStreamBytes), "it is the body's limit that cut it")
		})
	}
}

func (s *OpenAICompatSuite) TestAnErrorWhoseBodyNeverEndsIsReadOnlyInPart() {
	for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError} {
		s.Run(http.StatusText(status), func() {
			var sent atomic.Int64
			gone := make(chan struct{}, 8)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer func() { gone <- struct{}{} }()
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"error":{"message":"no","padding":"`)
				for r.Context().Err() == nil {
					n, err := io.WriteString(w, piece)
					sent.Add(int64(n))
					if err != nil {
						return
					}
					w.(http.Flusher).Flush()
				}
			}))
			s.T().Cleanup(server.Close)
			provider, err := New(Options{Provider: "test", Model: "test-model", APIKey: "key", BaseURL: server.URL})
			s.Require().NoError(err)
			s.T().Cleanup(func() { provider.Close() })
			ctx, cancel := context.WithTimeout(s.T().Context(), 10*time.Second)
			defer cancel()

			_, err = provider.Create(ctx, hello())

			apiError, ok := errors.AsType[*openai.Error](err)
			s.Require().True(ok, "the error is the endpoint's, with its status: %v", err)
			s.Equal(status, apiError.StatusCode)
			s.left(gone)
			s.Less(sent.Load(), int64(16<<20), "only the start of the body is read")
		})
	}
}

func (s *OpenAICompatSuite) TestAnAnswerOfExactlyTheLimitCompletes() {
	s.frames = make([]string, maxAnswerBytes/len(piece))
	for i := range s.frames {
		s.frames[i] = textFrame(piece)
	}
	s.Require().Equal(maxAnswerBytes, len(s.frames)*len(piece))

	response, _ := s.ask(s.provider(Options{}), hello())

	s.Equal(llm.StatusCompleted, response.Status)
	s.Len(response.OutputText, maxAnswerBytes)
}

func (s *OpenAICompatSuite) TestTheLongestReplyStreamedATokenAChunkCompletes() {
	// The limits leave room for a provider that wraps every token in a chunk of its own, as
	// most do, around the longest reply a model writes: with usage on every chunk and a long
	// model id, as some servers stream it, or with lines that end in a carriage return too.
	type stream struct{ chunk, eol string }
	for name, token := range map[string]stream{
		"a bare chunk":     {textFrame("word"), "\n"},
		"carriage returns": {textFrame("word"), "\r\n"},
		"a chunk with usage": {`{"id":"chatcmpl-8f3c2a1e9b7d4c6a8e2f1b3d5c7a9e0f","object":"chat.completion.chunk","created":1732600000,` +
			`"model":"accounts/fireworks/models/llama-v3p3-70b-instruct","system_fingerprint":"fp_6b9a1c2d3e",` +
			`"choices":[{"index":0,"delta":{"content":"word"},"logprobs":null,"finish_reason":null}],` +
			`"usage":{"prompt_tokens":1834,"completion_tokens":1000,"total_tokens":2834,` +
			`"prompt_tokens_details":{"cached_tokens":1024},"completion_tokens_details":{"reasoning_tokens":0}}}`, "\n"},
	} {
		s.Run(name, func() {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				var body bytes.Buffer
				for range replyTokens {
					fmt.Fprintf(&body, "data: %s%s%s", token.chunk, token.eol, token.eol)
				}
				fmt.Fprint(&body, "data: [DONE]\n\n")
				_, _ = w.Write(body.Bytes())
			}))
			s.T().Cleanup(server.Close)
			provider, err := New(Options{Provider: "test", Model: "test-model", APIKey: "key", BaseURL: server.URL})
			s.Require().NoError(err)
			s.T().Cleanup(func() { provider.Close() })

			response, _ := s.ask(provider, hello())

			s.Equal(llm.StatusCompleted, response.Status)
			s.Len(response.OutputText, replyTokens*len("word"))
		})
	}
}

func (s *OpenAICompatSuite) TestALongAnswerCompletesWhole() {
	var want strings.Builder
	for i := range 2000 {
		sentence := fmt.Sprintf("Sentence %d of a long answer, with ünïcode and 日本語. ", i)
		want.WriteString(sentence)
		s.frames = append(s.frames, textFrame(sentence))
	}
	s.frames = append(s.frames, usageFrame(10, 0, 30000, 0, "stop"))

	response, _ := s.ask(s.provider(Options{}), hello())

	s.Equal(llm.StatusCompleted, response.Status)
	s.Empty(response.IncompleteReason)
	s.Equal(want.String(), response.OutputText)
	s.EqualValues(30000, response.Usage.OutputTokens)
}

func (s *OpenAICompatSuite) TestTheDebugLogShowsTheStartOfTheStream() {
	s.frames = []string{textFrame(strings.Repeat("b", rawBytes)), textFrame("the end")}
	var logged bytes.Buffer
	provider := s.provider(Options{Logger: slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))})

	s.ask(provider, hello())

	line := ""
	for l := range strings.Lines(logged.String()) {
		if strings.Contains(l, "the model streamed") {
			line = l
		}
	}
	s.Contains(line, "raw="+strings.Repeat("b", rawBytes))
	s.NotContains(line, "the end", "the log keeps the first rawBytes of the stream only")
}

func (s *OpenAICompatSuite) TestTheDebugLogShowsToolCallsAsTheyStreamed() {
	s.frames = []string{toolFrame(0, "call_1", "lookup", "abc"), toolFrame(0, "", "", "def")}
	var logged bytes.Buffer
	provider := s.provider(Options{Logger: slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))})

	s.ask(provider, withTools())

	s.Contains(logged.String(), "raw=lookupabcdef")
}

func (s *OpenAICompatSuite) TestTheStreamIsKeptForTheLogOnlyWhenDebugIsOn() {
	for _, debug := range []bool{false, true} {
		p := &puller{debug: debug}
		p.keep(strings.Repeat("c", rawBytes-1))
		p.keep("dd")
		if debug {
			s.Equal(strings.Repeat("c", rawBytes-1)+"d", p.raw.String())
		} else {
			s.Zero(p.raw.Len())
		}
	}
}

func (s *OpenAICompatSuite) TestTheStreamKeptForTheLogEndsOnAWholeCharacter() {
	for _, text := range []string{"é", "日本", "👋"} {
		p := &puller{debug: true}
		p.keep(strings.Repeat("c", rawBytes-1))
		p.keep(text)
		s.Equal(strings.Repeat("c", rawBytes-1), p.raw.String(), "a character the limit would cut is left out whole")
	}
	p := &puller{debug: true}
	p.keep(strings.Repeat("c", rawBytes))
	p.keep("\xa9 a byte that starts no character")
	s.Len(p.raw.String(), rawBytes)
}
