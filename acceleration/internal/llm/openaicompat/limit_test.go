package openaicompat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
)

// piece is one chunk's worth of an answer that never ends.
var piece = strings.Repeat("a", 16<<10)

// endless serves an event stream that writes what next returns until the client goes away,
// and closes gone once it notices.
func (s *OpenAICompatSuite) endless(next func() string) (*LLM, <-chan struct{}) {
	gone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(gone)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for r.Context().Err() == nil {
			if _, err := io.WriteString(w, next()); err != nil {
				return
			}
			flusher.Flush()
		}
	}))
	s.T().Cleanup(server.Close)

	provider, err := New(Options{Provider: "test", Model: "test-model", APIKey: "key", BaseURL: server.URL})
	s.Require().NoError(err)
	s.T().Cleanup(func() { provider.Close() })
	return provider, gone
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
			s.Greater(held, maxAnswerBytes-len(piece), "and is cut at the limit rather than before it")
			s.left(gone)
		})
	}
}

func (s *OpenAICompatSuite) TestAnEventThatNeverEndsIsCutShort() {
	// Data lines with no blank line between them are all one event, which the stream joins
	// before any chunk reaches the response.
	provider, gone := s.endless(func() string { return "data: " + piece + "\n" })

	response, _ := s.askBounded(provider, hello())

	s.Equal(llm.StatusIncomplete, response.Status)
	s.Equal(llm.ReasonMaxOutputTokens, response.IncompleteReason)
	s.left(gone)
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
	// The body limit leaves room for a provider that wraps every token in a chunk of its own,
	// as most do, around the longest reply a model writes.
	token := textFrame("word")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		var body bytes.Buffer
		for range replyTokens {
			fmt.Fprintf(&body, "data: %s\n\n", token)
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
