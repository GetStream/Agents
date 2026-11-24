package ttsrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
)

// timedSession is a session over a stub provider whose log lines are kept, owned the way an
// agent's voice in a call is.
func (s *TTSRouterSuite) timedSession(region string, byok bool) (*Session, *stubTTS, *bytes.Buffer) {
	provider := newStubTTS()
	session, logs := s.timedVoice(provider, region, byok)
	return session, provider, logs
}

// timedVoice is timedSession over a provider of the test's own.
func (s *TTSRouterSuite) timedVoice(provider *stubTTS, region string, byok bool) (*Session, *bytes.Buffer) {
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
	owner := routing.Owner{CustomerID: "acme", AgentID: "concierge", CallID: "default:lobby"}
	session := newSession(context.Background(), provider, config, owner, recorder, slog.New(slog.NewJSONHandler(&logs, nil)))
	s.T().Cleanup(func() {
		_ = session.Close()
		recorder.Close()
	})
	return session, &logs
}

// onFakeClock runs body in a synctest bubble, where time moves only as the test sleeps, so
// the times a line logs are exact.
func (s *TTSRouterSuite) onFakeClock(body func()) {
	outer := s.T()
	synctest.Test(outer, func(t *testing.T) {
		s.SetT(t)
		defer s.SetT(outer)
		body()
	})
}

// byOperation is the lines keyed by the synthesis each is about, failing on a second line
// for one.
func (s *TTSRouterSuite) byOperation(lines []map[string]any) map[string]map[string]any {
	keyed := map[string]map[string]any{}
	for _, line := range lines {
		operation, _ := line["operation"].(string)
		s.Require().NotContains(keyed, operation, "one line per synthesis")
		keyed[operation] = line
	}
	return keyed
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
	for _, unknown := range []string{"operation", "ttfb_ms", "duration_ms"} {
		s.NotContains(lines[0], unknown, "no synthesis was timed, so a percentile over the lines counts no zero")
	}
	s.EqualValues(0, lines[0]["characters"])
	s.EqualValues(0, lines[0]["cost_micros"])
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

func (s *TTSRouterSuite) TestASynthesisTheSessionClosedOnLogsALineWithNoCost() {
	s.onFakeClock(func() {
		session, provider, logs := s.timedSession("", false)
		s.Require().NoError(session.Synthesize(tts.Request{ID: "heard", Text: "hello there", Final: true}))
		s.Require().NoError(session.Synthesize(tts.Request{ID: "silent", Text: "four", Final: true}))
		s.Require().NoError(session.Synthesize(tts.Request{ID: "queued", Text: "never begun", Final: true}))
		provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "heard", At: time.Now()})
		provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "silent", At: time.Now()})
		provider.emitter.Send(tts.Error{SynthesisID: "silent", Context: "read",
			Err: errors.New("voice stalled, api_key=sk-live-0123456789abcdef")})
		time.Sleep(120 * time.Millisecond)
		provider.emitter.Send(tts.AudioChunk{SynthesisID: "heard", Index: 0})
		time.Sleep(200 * time.Millisecond)
		provider.emitter.Send(tts.AudioChunk{SynthesisID: "heard", Index: 1})
		time.Sleep(30 * time.Millisecond)
		s.settled(session)

		lines := s.byOperation(s.synthesisLines(logs))
		s.Require().Len(lines, 3, "a line for each synthesis the voice never completed")
		for operation, line := range lines {
			s.Equal("acme", line["app_id"], operation)
			s.Equal(false, line["success"], operation)
			if operation == "silent" {
				s.Equal("read", line["error_code"], "the code the voice reported for it")
			} else {
				s.Equal("closed", line["error_code"], operation)
			}
			s.NotContains(line, "cost_micros", "%s wrote no row, so it has no cost", operation)
			s.NotContains(line, "audio_ms", "%s: the audio of a synthesis that never completed is not counted", operation)
		}
		heard := lines["heard"]
		s.EqualValues(11, heard["characters"])
		s.EqualValues(120, heard["ttfb_ms"], "until its first audio, not its last")
		s.EqualValues(350, heard["duration_ms"], "from its start until the session closed")
		s.NotContains(heard, "error")

		silent := lines["silent"]
		s.EqualValues(4, silent["characters"])
		s.NotContains(silent, "ttfb_ms", "no audio of it arrived")
		s.EqualValues(350, silent["duration_ms"])
		s.Contains(silent["error"], "voice stalled", "what the voice said of it")
		s.NotContains(silent["error"], "0123456789abcdef", "the key the provider echoed is redacted")

		queued := lines["queued"]
		s.EqualValues(11, queued["characters"])
		s.NotContains(queued, "ttfb_ms")
		s.NotContains(queued, "duration_ms", "it never started")
	})
}

func (s *TTSRouterSuite) TestASynthesisWhoseVoiceEndedOnItsOwnLogsALine() {
	session, provider, logs := s.timedSession("", false)
	s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "cut off", Final: true}))
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	provider.emitter.Close()
	s.drain(session)

	lines := s.synthesisLines(logs)
	s.Require().Len(lines, 1, "logged once the voice's events ended, before the session closed")
	s.Equal("u1", lines[0]["operation"])
	s.Equal("closed", lines[0]["error_code"])
	s.EqualValues(7, lines[0]["characters"])
}

func (s *TTSRouterSuite) TestASentenceABargeInDropsLogsALine() {
	session, logs := s.timedVoice(&stubTTS{emitter: tts.NewEmitter(64), perSentence: true}, "", false)
	s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "half a sentence"}))
	s.Require().NoError(session.Synthesize(tts.Request{ID: "u2", Text: "said", Final: true}))
	s.Require().NoError(session.Interrupt())
	s.settled(session)

	lines := s.byOperation(s.synthesisLines(logs))
	s.Require().Len(lines, 2)
	dropped := lines["u1"]
	s.Equal(false, dropped["success"])
	s.Equal("interrupted", dropped["error_code"], "the barge-in dropped it, not the close after")
	s.EqualValues(15, dropped["characters"])
	s.NotContains(dropped, "cost_micros")
	s.Equal("closed", lines["u2"]["error_code"], "its final text was sent, so the voice still owed its completion")
}

func (s *TTSRouterSuite) TestASentenceLoggedWhenTheVoiceEndedIsNotLoggedAgainByABargeIn() {
	provider := &stubTTS{emitter: tts.NewEmitter(64), perSentence: true}
	session, logs := s.timedVoice(provider, "", false)
	s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "half a sentence"}))
	provider.emitter.Close()
	s.drain(session)
	s.Require().NoError(session.Interrupt())
	s.settled(session)

	lines := s.synthesisLines(logs)
	s.Require().Len(lines, 1, "one line for one synthesis")
	s.Equal("closed", lines[0]["error_code"])
}

func (s *TTSRouterSuite) TestEachSentenceOfAnIdTheVoiceNeverCompletedHasItsOwnLine() {
	s.onFakeClock(func() {
		session, provider, logs := s.timedSession("", false)
		s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "first"}))
		provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
		time.Sleep(20 * time.Millisecond)
		s.Require().NoError(session.Interrupt())
		s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "second part"}))
		time.Sleep(30 * time.Millisecond)
		s.settled(session)

		lines := s.synthesisLines(logs)
		s.Require().Len(lines, 2, "a line for each sentence, as each would have had a row")
		characters := map[float64]map[string]any{}
		for _, line := range lines {
			characters[line["characters"].(float64)] = line
		}
		s.Require().Contains(characters, 5.0)
		s.Require().Contains(characters, 11.0)
		s.EqualValues(50, characters[5]["duration_ms"], "the start belongs to the oldest sentence, as its row would")
		s.NotContains(characters[11], "duration_ms", "the voice never said it started the second")
	})
}

func (s *TTSRouterSuite) TestACompletedSynthesisIsNotLoggedAgainAtClose() {
	session, provider, logs := s.timedSession("", false)
	s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "done", Final: true}))
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	provider.emitter.Send(tts.AudioChunk{SynthesisID: "u1"})
	provider.emitter.Send(tts.SynthesisComplete{SynthesisID: "u1", Characters: 4, TimeToFirstByteMs: 80})
	s.settled(session)

	lines := s.synthesisLines(logs)
	s.Require().Len(lines, 1, "its row's line, and none at close")
	s.Equal(true, lines[0]["success"])
	s.EqualValues(80, lines[0]["ttfb_ms"], "a completed synthesis logs the time to first byte its voice reported")
}

func (s *TTSRouterSuite) TestASynthesisAFatalErrorEndedLogsTheCodeTheVoiceReported() {
	session, provider, logs := s.timedSession("", false)
	s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "hello", Final: true}))
	s.Require().NoError(session.Synthesize(tts.Request{ID: "u2", Text: "hung up", Final: true}))
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
	provider.emitter.Send(tts.Error{SynthesisID: "u1", Fatal: true, Err: errors.New("socket reset")})
	provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u2", At: time.Now()})
	provider.emitter.Close()
	s.drain(session)

	lines := s.byOperation(s.synthesisLines(logs))
	s.Require().Len(lines, 2)
	s.Equal("provider_fatal", lines["u1"]["error_code"], "what the row would say")
	s.Equal("socket reset", lines["u1"]["error"])
	s.Equal("closed", lines["u2"]["error_code"], "a hang-up mid-sentence reported nothing")
}

func (s *TTSRouterSuite) TestRowInterruptedLineCarriesItsStart() {
	s.onFakeClock(func() {
		provider := &stubTTS{emitter: tts.NewEmitter(64), perSentence: true}
		session, logs := s.timedVoice(provider, "", false)
		s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "half"}))
		provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
		time.Sleep(5 * time.Millisecond)
		provider.emitter.Send(tts.AudioChunk{SynthesisID: "u1"})
		time.Sleep(15 * time.Millisecond)
		s.Require().NoError(session.Interrupt())
		s.settled(session)
		lines := s.synthesisLines(logs)
		s.Require().Len(lines, 1)
		s.Equal("interrupted", lines[0]["error_code"])
		s.EqualValues(5, lines[0]["ttfb_ms"])
		s.EqualValues(20, lines[0]["duration_ms"])
	})
}

func (s *TTSRouterSuite) TestRowInterruptedSecondSentenceDoesNotCarryTheFirstsStart() {
	s.onFakeClock(func() {
		provider := &stubTTS{emitter: tts.NewEmitter(64), perSentence: true}
		session, logs := s.timedVoice(provider, "", false)
		s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "first", Final: true}))
		provider.emitter.Send(tts.SynthesisStarted{SynthesisID: "u1", At: time.Now()})
		s.Require().NoError(session.Interrupt()) // first is kept: its completion is owed
		s.Require().NoError(session.Synthesize(tts.Request{ID: "u1", Text: "more"}))
		time.Sleep(20 * time.Millisecond)
		s.Require().NoError(session.Interrupt()) // second dropped
		s.settled(session)
		lines := s.synthesisLines(logs)
		s.Require().Len(lines, 2)
		var interrupted []map[string]any
		for _, l := range lines {
			if l["error_code"] == "interrupted" {
				interrupted = append(interrupted, l)
			}
		}
		s.Require().Len(interrupted, 1, "the second sentence's drop is the one interrupted line")
		s.EqualValues(4, interrupted[0]["characters"])
		s.NotContains(interrupted[0], "duration_ms", "the second sentence never started")
	})
}
