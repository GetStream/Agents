package ttsrouter

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
)

// timedSession is a session over a stub provider whose log lines are kept, owned the way an
// agent's voice in a call is.
func (s *TTSRouterSuite) timedSession(region string, byok bool) (*Session, *stubTTS, *bytes.Buffer) {
	config := routing.ProviderConfig{
		Provider: "stub",
		Model:    "stub-model",
		Region:   region,
		BYOK:     byok,
		Price:    routing.Price{PerMillionChars: 50},
	}
	prices, err := routing.Prices(routing.Config{routing.TTS: {Providers: []routing.ProviderConfig{config}}}, slog.Default())
	s.Require().NoError(err)
	recorder := routing.NewRecorder(routing.TTS, nil, nil, prices, slog.Default())
	var logs bytes.Buffer
	provider := newStubTTS()
	owner := routing.Owner{CustomerID: "acme", AgentID: "concierge", CallID: "default:lobby"}
	session := newSession(provider, config, owner, recorder, slog.New(slog.NewJSONHandler(&logs, nil)))
	s.T().Cleanup(func() {
		_ = session.Close()
		recorder.Close()
	})
	return session, provider, &logs
}

// settled closes the session and waits until every event the provider sent was observed.
func (s *TTSRouterSuite) settled(session *Session) {
	s.Require().NoError(session.Close())
	s.drain(session)
}

// synthesisLines are the "speech synthesis timing" lines among those logged, decoded.
func (s *TTSRouterSuite) synthesisLines(logs *bytes.Buffer) []map[string]any {
	var found []map[string]any
	for decoder := json.NewDecoder(logs); decoder.More(); {
		var line map[string]any
		s.Require().NoError(decoder.Decode(&line))
		if line["msg"] == "speech synthesis timing" {
			found = append(found, line)
		}
	}
	return found
}

func (s *TTSRouterSuite) TestASynthesisLogsWhoseItWasWhatServedItAndWhatItCost() {
	session, provider, logs := s.timedSession("eu", true)
	s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "twenty characters ok", Final: true}))
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now().Add(-40 * time.Millisecond)})
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: "u1", Characters: 20, AudioDurationMs: 1500, TimeToFirstByteMs: 120})
	s.settled(session)

	lines := s.synthesisLines(logs)
	s.Require().Len(lines, 1, "one line for one synthesis")
	line := lines[0]
	s.Equal("default:lobby", line["call"])
	s.Equal("acme", line["app_id"], "named under the field the request lines use, so it filters by app")
	s.Equal("concierge", line["agent_id"])
	s.Equal("stub", line["provider"])
	s.Equal("stub-model", line["model"])
	s.Equal("eu", line["region"])
	s.Equal(true, line["byok"])
	s.Equal("u1", line["operation"])
	s.EqualValues(20, line["characters"])
	s.EqualValues(1500, line["audio_ms"])
	s.EqualValues(120, line["ttfb_ms"])
	s.GreaterOrEqual(line["duration_ms"], 40.0, "from when the provider took it")
	s.EqualValues(1000, line["cost_micros"], "20 characters at 50 per million characters")
	s.Equal(true, line["success"])
	s.Equal("", line["error_code"])
	s.NotContains(line, "error", "a served synthesis says nothing went wrong")
	s.NotContains(logs.String(), "twenty characters ok", "the text spoken is never logged")
}

func (s *TTSRouterSuite) TestASynthesisWithNoRegionOrOwnKeyLeavesThemOut() {
	session, provider, logs := s.timedSession("", false)
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: "u1", Characters: 5})
	s.settled(session)

	lines := s.synthesisLines(logs)
	s.Require().Len(lines, 1)
	s.NotContains(lines[0], "region")
	s.NotContains(lines[0], "byok")
}

func (s *TTSRouterSuite) TestAFailedSynthesisLogsWhatItsProviderSaidWithoutItsSecrets() {
	session, provider, logs := s.timedSession("", false)
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	provider.emitter.Send(tts.Error{SynthesisID: "u1", Fatal: true, Context: "read",
		Err: errors.New("voice refused the request, api_key=sk-live-0123456789abcdef")})
	provider.emitter.Send(tts.Error{SynthesisID: "u1", Context: "write", Err: errors.New("a later failure")})
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: "u1", Characters: 5})
	s.settled(session)

	lines := s.synthesisLines(logs)
	s.Require().Len(lines, 1, "the failure is settled by its synthesis, as its row is")
	line := lines[0]
	s.Equal("u1", line["operation"])
	s.Equal(false, line["success"])
	s.Equal("provider_fatal", line["error_code"])
	s.Contains(line["error"], "voice refused the request", "the first failure explains the synthesis")
	s.NotContains(line["error"], "0123456789abcdef", "the key the provider echoed is redacted")
}

func (s *TTSRouterSuite) TestAFailureNoSynthesisSettlesIsLoggedOnItsOwn() {
	session, provider, logs := s.timedSession("", false)
	provider.emitter.Send(tts.Error{SynthesisID: "never-started", Context: "connect", Err: errors.New("voice unreachable")})
	s.settled(session)

	lines := s.synthesisLines(logs)
	s.Require().Len(lines, 1)
	s.Equal("acme", lines[0]["app_id"])
	s.Equal(false, lines[0]["success"])
	s.Equal("connect", lines[0]["error_code"])
	s.Equal("voice unreachable", lines[0]["error"])
}

func (s *TTSRouterSuite) TestAFailureWithNoErrorUnderItIsLoggedWithoutOne() {
	session, provider, logs := s.timedSession("", false)
	provider.emitter.Send(tts.Error{Fatal: true})
	s.settled(session)

	lines := s.synthesisLines(logs)
	s.Require().Len(lines, 1)
	s.Equal("provider_fatal", lines[0]["error_code"])
	s.NotContains(lines[0], "error")
}

func (s *TTSRouterSuite) TestARepeatedCompletionLogsItsSynthesisOnce() {
	session, provider, logs := s.timedSession("", false)
	for _, id := range []string{"u1", "u2"} {
		provider.emitter.Send(tts.SynthesisStarted{SynthesisID: id, At: time.Now()})
		provider.emitter.Send(tts.SynthesisComplete{SynthesisID: id, Characters: 5})
	}
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: "u1", Characters: 5})
	s.settled(session)

	var operations []string
	for _, line := range s.synthesisLines(logs) {
		operations = append(operations, line["operation"].(string))
	}
	s.Equal("u1,u2", strings.Join(operations, ","), "a line per synthesis, and none for the repeat")
}
