package sttrouter

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// timingConfig is a transcriber priced at 3.60 US dollars an audio hour: a millisecond of
// audio costs one micro-dollar.
func timingConfig(provider, model string) routing.ProviderConfig {
	return routing.ProviderConfig{Provider: provider, Model: model, Price: routing.Price{PerAudioHour: 3.6}}
}

// timedSession is a session over provider whose log lines are kept, owned the way an agent's
// transcriber in a call is. reopen, when set, is the next candidate a refusal hands over to.
func (s *STTRouterSuite) timedSession(provider stt.STT, config routing.ProviderConfig,
	reopen func([]string) (stt.STT, routing.ProviderConfig, error)) (*Session, *timingLog) {
	var configs []routing.ProviderConfig
	configs = append(configs, config, timingConfig("assemblyai", "universal-streaming"))
	prices, err := routing.Prices(routing.Config{routing.STT: {Providers: configs}}, slog.Default())
	s.Require().NoError(err)
	recorder := routing.NewRecorder(routing.STT, nil, nil, prices, slog.Default())
	logs := &timingLog{call: "default:" + rand.Text()}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	owner := routing.Owner{CustomerID: "acme", AgentID: "concierge", CallID: logs.call}
	session := newSession(provider, config, owner, recorder, reopen, nil)
	s.T().Cleanup(func() {
		_ = session.Close()
		slog.SetDefault(previous)
		recorder.Close()
	})
	return session, logs
}

// timingLog keeps what is logged. The session logs from its own goroutine.
type timingLog struct {
	call string
	mu   sync.Mutex
	logs bytes.Buffer
}

func (l *timingLog) Write(line []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.logs.Write(line)
}

// lines are this session's "speech transcription timing" lines, decoded, oldest first. A
// session an earlier test closed may still be logging, so lines of other calls are left out.
func (s *STTRouterSuite) transcriptionLines(logs *timingLog) []map[string]any {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	var found []map[string]any
	for decoder := json.NewDecoder(bytes.NewReader(logs.logs.Bytes())); decoder.More(); {
		var line map[string]any
		s.Require().NoError(decoder.Decode(&line))
		if line["msg"] == "speech transcription timing" && line["call"] == logs.call {
			found = append(found, line)
		}
	}
	return found
}

// ended closes the session and waits until its events close, which is after its last line.
func (s *STTRouterSuite) ended(session *Session) {
	s.Require().NoError(session.Close())
	s.awaitEvent(session, func(stt.Event) bool { return false })
}

// awaitEvent reads the session's events until one matches, and reports whether one did before
// they closed.
func (s *STTRouterSuite) awaitEvent(session *Session, match func(stt.Event) bool) bool {
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event, open := <-session.Events():
			if !open {
				return false
			}
			if match(event) {
				return true
			}
		case <-deadline:
			s.FailNow("timed out reading the session's events")
			return false
		}
	}
}

// speak sends frames of 20 ms of audio.
func (s *STTRouterSuite) speak(session *Session, frames int) {
	frame := stt.PcmData{Samples: make([]int16, 320), SampleRate: 16000, Channels: 1}
	for range frames {
		s.Require().NoError(session.ProcessAudio(frame, stt.Participant{}))
	}
}

// onFakeClock runs body in a synctest bubble, where time moves only as the test sleeps, so
// the times a line logs are exact.
func (s *STTRouterSuite) onFakeClock(body func()) {
	outer := s.T()
	synctest.Test(outer, func(t *testing.T) {
		s.SetT(t)
		defer s.SetT(outer)
		body()
	})
}

func (s *STTRouterSuite) TestASessionLogsOneLineWhoseItWasWhatServedItAndWhatItCost() {
	s.onFakeClock(func() {
		provider := &quietSTT{emitter: stt.NewEmitter(64)}
		config := timingConfig("deepgram", "flux-general-en")
		config.Region, config.BYOK = "eu", true
		session, logs := s.timedSession(provider, config, nil)

		time.Sleep(100 * time.Millisecond)
		s.speak(session, 50)
		time.Sleep(300 * time.Millisecond)
		provider.emitter.Send(stt.Transcript{Text: "hello th", Mode: stt.ModeReplacement})
		synctest.Wait()
		time.Sleep(time.Second)
		provider.emitter.Send(stt.Transcript{Text: "hello there", Mode: stt.ModeFinal, AudioDurationMs: 600})
		s.speak(session, 25)
		provider.emitter.Send(stt.Transcript{Text: "how are you", Mode: stt.ModeFinal, AudioDurationMs: 400})
		s.ended(session)

		lines := s.transcriptionLines(logs)
		s.Require().Len(lines, 1, "one line for the session, not one per frame or per turn")
		line := lines[0]
		s.Equal("acme", line["app_id"], "named under the field the request lines use, so it filters by app")
		s.Equal("concierge", line["agent_id"])
		s.Equal("deepgram", line["provider"])
		s.Equal("flux-general-en", line["model"])
		s.Equal("eu", line["region"])
		s.Equal(true, line["byok"])
		s.EqualValues(1500, line["audio_ms"], "every frame the provider took")
		s.EqualValues(300, line["ttfb_ms"], "from its first audio to its first transcript, interim included")
		s.EqualValues(1400, line["duration_ms"], "from the session's start to its end")
		s.EqualValues(1000, line["cost_micros"], "the two turns the vendor billed, 1000 ms of audio")
		s.Equal(true, line["success"])
		s.Equal("", line["error_code"])
		s.NotContains(line, "error")
		logs.mu.Lock()
		defer logs.mu.Unlock()
		s.NotContains(logs.logs.String(), "hello there", "what was said is never logged")
	})
}

func (s *STTRouterSuite) TestATranscriptWithNoAudioUnderItIsNoTimeToFirstByte() {
	provider := &quietSTT{emitter: stt.NewEmitter(64)}
	session, logs := s.timedSession(provider, timingConfig("deepgram", "flux-general-en"), nil)

	provider.emitter.Send(stt.Transcript{Text: "", Mode: stt.ModeReplacement})
	s.ended(session)

	lines := s.transcriptionLines(logs)
	s.Require().Len(lines, 1)
	s.EqualValues(0, lines[0]["audio_ms"])
	s.NotContains(lines[0], "ttfb_ms")
}

func (s *STTRouterSuite) TestTheFirstFatalErrorIsWhatEndedTheSession() {
	provider := &quietSTT{emitter: stt.NewEmitter(64)}
	session, logs := s.timedSession(provider, timingConfig("deepgram", "flux-general-en"), nil)

	provider.emitter.Send(stt.Error{Context: "read", Err: errors.New("a hiccup the vendor recovered from")})
	provider.emitter.Send(stt.Error{Fatal: true, Context: "connect", Err: errors.New("account suspended")})
	provider.emitter.Send(stt.Error{Fatal: true, Context: "write", Err: errors.New("websocket: close sent")})
	s.ended(session)

	lines := s.transcriptionLines(logs)
	s.Require().Len(lines, 1)
	s.Equal(false, lines[0]["success"])
	s.Equal("provider_fatal", lines[0]["error_code"])
	s.Equal("account suspended", lines[0]["error"])
}

func (s *STTRouterSuite) TestAnErrorTheVendorRecoveredFromLeavesTheSessionServed() {
	provider := &quietSTT{emitter: stt.NewEmitter(64)}
	session, logs := s.timedSession(provider, timingConfig("deepgram", "flux-general-en"), nil)

	provider.emitter.Send(stt.Error{Context: "read", Err: errors.New("a hiccup the vendor recovered from")})
	s.ended(session)

	lines := s.transcriptionLines(logs)
	s.Require().Len(lines, 1)
	s.Equal(true, lines[0]["success"])
	s.Equal("", lines[0]["error_code"])
	s.NotContains(lines[0], "error")
}

func (s *STTRouterSuite) TestASessionNothingWasTranscribedInHasNoTimeToFirstByte() {
	provider := &quietSTT{emitter: stt.NewEmitter(64)}
	session, logs := s.timedSession(provider, timingConfig("deepgram", "flux-general-en"), nil)

	provider.emitter.Send(stt.Connected{At: time.Now()})
	s.speak(session, 10)
	s.ended(session)

	lines := s.transcriptionLines(logs)
	s.Require().Len(lines, 1)
	s.EqualValues(200, lines[0]["audio_ms"])
	s.NotContains(lines[0], "ttfb_ms", "no transcript answered the audio")
	s.NotContains(lines[0], "region")
	s.NotContains(lines[0], "byok")
}

func (s *STTRouterSuite) TestATranscriptReadBeforeAnyAudioIsNoTimeToFirstByte() {
	provider := &quietSTT{emitter: stt.NewEmitter(64)}
	session, logs := s.timedSession(provider, timingConfig("deepgram", "flux-general-en"), nil)

	provider.emitter.Send(stt.Transcript{Text: "", Mode: stt.ModeReplacement})
	s.awaitEvent(session, func(event stt.Event) bool { _, ok := event.(stt.Transcript); return ok })
	s.speak(session, 10)
	s.ended(session)

	lines := s.transcriptionLines(logs)
	s.Require().Len(lines, 1)
	s.NotContains(lines[0], "ttfb_ms")
}

func (s *STTRouterSuite) TestARouterMeteredSessionLogsWhatTheRouterBilledAtItsEnd() {
	provider := &meteredSTT{quietSTT: quietSTT{emitter: stt.NewEmitter(64)}}
	session, logs := s.timedSession(provider, timingConfig("deepgram", "flux-general-en"), nil)

	s.speak(session, 75)
	s.ended(session)

	lines := s.transcriptionLines(logs)
	s.Require().Len(lines, 1)
	s.EqualValues(1500, lines[0]["cost_micros"], "every second the vendor heard, billed when the session ended")
}

func (s *STTRouterSuite) TestAFailedOverSessionLogsALinePerProviderThatServedIt() {
	s.onFakeClock(func() {
		refusing := &quietSTT{emitter: stt.NewEmitter(64)}
		next := &quietSTT{emitter: stt.NewEmitter(64)}
		reopen := func([]string) (stt.STT, routing.ProviderConfig, error) {
			return next, timingConfig("assemblyai", "universal-streaming"), nil
		}
		session, logs := s.timedSession(refusing, timingConfig("deepgram", "flux-general-en"), reopen)

		s.speak(session, 10)
		time.Sleep(200 * time.Millisecond)
		refusing.emitter.Send(stt.Transcript{Text: "hello", Mode: stt.ModeFinal, AudioDurationMs: 200})
		refusing.emitter.Send(stt.Error{Fatal: true, Context: "connect",
			Err: errors.New("account suspended, api_key=sk-live-0123456789abcdef")})
		s.Require().True(s.awaitEvent(session, func(event stt.Event) bool { _, ok := event.(stt.Failover); return ok }))
		s.speak(session, 40)
		time.Sleep(500 * time.Millisecond)
		next.emitter.Send(stt.Transcript{Text: "there", Mode: stt.ModeFinal, AudioDurationMs: 800})
		s.ended(session)

		lines := s.transcriptionLines(logs)
		s.Require().Len(lines, 2, "the refused provider's line, then the one that took over")
		refused, served := lines[0], lines[1]
		s.Equal("deepgram", refused["provider"])
		s.Equal("flux-general-en", refused["model"])
		s.EqualValues(200, refused["audio_ms"])
		s.EqualValues(200, refused["ttfb_ms"])
		s.EqualValues(200, refused["duration_ms"], "until it refused")
		s.EqualValues(200, refused["cost_micros"], "its turn, and not the next provider's")
		s.Equal(false, refused["success"])
		s.Equal("provider_fatal", refused["error_code"])
		s.Contains(refused["error"], "account suspended")
		s.NotContains(refused["error"], "0123456789abcdef", "the key the vendor echoed is redacted")

		s.Equal("acme", served["app_id"])
		s.Equal("assemblyai", served["provider"])
		s.Equal("universal-streaming", served["model"])
		s.EqualValues(800, served["audio_ms"], "only the audio it took")
		s.EqualValues(500, served["ttfb_ms"])
		s.EqualValues(500, served["duration_ms"], "from when it took over")
		s.EqualValues(800, served["cost_micros"])
		s.Equal(true, served["success"])
		s.Equal("", served["error_code"])
		s.NotContains(served, "error")
	})
}

func (s *STTRouterSuite) TestARefusedRouterMeteredProviderLogsTheAudioBilledAsItRefused() {
	refusing := &meteredSTT{quietSTT: quietSTT{emitter: stt.NewEmitter(64)}}
	next := &meteredSTT{quietSTT: quietSTT{emitter: stt.NewEmitter(64)}}
	reopen := func([]string) (stt.STT, routing.ProviderConfig, error) {
		return next, timingConfig("assemblyai", "universal-streaming"), nil
	}
	session, logs := s.timedSession(refusing, timingConfig("deepgram", "flux-general-en"), reopen)

	s.speak(session, 35)
	refusing.emitter.Send(stt.Error{Fatal: true, Context: "connect", Err: errors.New("account suspended")})
	s.Require().True(s.awaitEvent(session, func(event stt.Event) bool { _, ok := event.(stt.Failover); return ok }))
	s.ended(session)

	lines := s.transcriptionLines(logs)
	s.Require().Len(lines, 2)
	s.Equal("deepgram", lines[0]["provider"])
	s.EqualValues(700, lines[0]["cost_micros"], "the 700 ms the refused vendor heard, billed as it refused")
}

func (s *STTRouterSuite) TestAFrameInFlightToARefusedProviderIsNotBilledToTheNext() {
	refusing := &meteredSTT{quietSTT: quietSTT{emitter: stt.NewEmitter(64)}, held: make(chan struct{}), entered: make(chan struct{}, 1)}
	next := &meteredSTT{quietSTT: quietSTT{emitter: stt.NewEmitter(64)}}
	reopen := func([]string) (stt.STT, routing.ProviderConfig, error) {
		return next, timingConfig("assemblyai", "universal-streaming"), nil
	}
	session, logs := s.timedSession(refusing, timingConfig("deepgram", "flux-general-en"), reopen)

	sent := make(chan error, 1)
	go func() {
		sent <- session.ProcessAudio(stt.PcmData{Samples: make([]int16, 16000), SampleRate: 16000, Channels: 1}, stt.Participant{})
	}()
	<-refusing.entered
	refusing.emitter.Send(stt.Error{Fatal: true, Context: "connect", Err: errors.New("account suspended")})
	s.Require().True(s.awaitEvent(session, func(event stt.Event) bool { _, ok := event.(stt.Failover); return ok }))
	close(refusing.held)
	s.Require().NoError(<-sent)
	s.speak(session, 25)
	s.ended(session)

	lines := s.transcriptionLines(logs)
	s.Require().Len(lines, 2)
	s.Equal("assemblyai", lines[1]["provider"])
	s.EqualValues(500, lines[1]["cost_micros"], "the refused provider's second, billed late, is not the next one's")
}

// meteredSTT is a transcriber whose vendor bills every second of audio it hears, which the
// router measures. held, when set, keeps a frame on its way to it until it is closed.
type meteredSTT struct {
	quietSTT
	held    chan struct{}
	entered chan struct{}
}

func (m *meteredSTT) Metering() stt.Metering { return stt.MeteredByRouter }

func (m *meteredSTT) ProcessAudio(stt.PcmData, stt.Participant) error {
	if m.held != nil {
		m.entered <- struct{}{}
		<-m.held
	}
	return nil
}
