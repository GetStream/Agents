package stsrouter

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sts"
)

// timingLog keeps what is logged. The session logs from its own goroutine.
type timingLog struct {
	mu   sync.Mutex
	logs bytes.Buffer
}

func (l *timingLog) Write(line []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.logs.Write(line)
}

// capturedTiming has every line logged until the test ends kept in the returned log.
func capturedTiming(t *testing.T) *timingLog {
	logs := &timingLog{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return logs
}

// timingLines are the "speech-to-speech timing" lines of the session with that id, decoded,
// oldest first. A session an earlier test closed may still be logging, so the lines of other
// sessions are left out.
func timingLines(t *testing.T, logs *timingLog, sessionID string) []map[string]any {
	logs.mu.Lock()
	defer logs.mu.Unlock()
	var found []map[string]any
	for decoder := json.NewDecoder(bytes.NewReader(logs.logs.Bytes())); decoder.More(); {
		var line map[string]any
		if err := decoder.Decode(&line); err != nil {
			t.Fatal(err)
		}
		if line["msg"] == "speech-to-speech timing" && line["operation"] == sessionID {
			found = append(found, line)
		}
	}
	return found
}

// timedSession is a session over a stub provider, owned the way an agent's voice in a call is,
// whose log lines are kept.
func (s *STSRouterSuite) timedSession() (*Session, *stubSTS, *timingLog) {
	logs := capturedTiming(s.T())
	provider := newStubSTS()
	recorder := routing.NewRecorder(routing.STS, nil, nil, nil, slog.Default())
	config := routing.ProviderConfig{Provider: "stub", Model: "stub-model", Region: "eu"}
	owner := routing.Owner{CustomerID: "acme", AgentID: "concierge", CallID: "default:" + rand.Text()}
	session := newSession(provider, config, owner, recorder, recorder.For(routing.LLM))
	s.T().Cleanup(func() {
		_ = session.Close()
		recorder.Close()
	})
	return session, provider, logs
}

// onFakeClock runs body in a synctest bubble, where time moves only as the test sleeps, so
// the times a line logs are exact.
func (s *STSRouterSuite) onFakeClock(body func()) {
	outer := s.T()
	synctest.Test(outer, func(t *testing.T) {
		s.SetT(t)
		defer s.SetT(outer)
		body()
	})
}

// heard is that much of the caller's speech.
func heard(ms int) sts.PcmData {
	return sts.PcmData{Samples: make([]int16, sts.InputSampleRate*ms/1000), SampleRate: sts.InputSampleRate, Channels: 1}
}

func (s *STSRouterSuite) TestASessionLogsOneLineWhenItEndsWithWhatItHeardAnsweredAndHowLongItRan() {
	s.onFakeClock(func() {
		session, provider, logs := s.timedSession()
		s.Require().NoError(session.ProcessAudio(heard(600), sts.Participant{}))
		provider.emitter.Send(sts.ResponseStarted{ResponseID: "r1", Generation: 1, At: time.Now()})
		provider.emitter.Send(sts.InputTranscript{Text: "A table for four at eight, please."})
		provider.emitter.Send(sts.OutputTranscript{ResponseID: "r1", Text: "Your table for four is booked at eight."})
		provider.emitter.Send(sts.ResponseComplete{ResponseID: "r1", Generation: 1, TimeToFirstByteMs: 420,
			Usage: sts.Usage{InputTokens: 30, OutputTokens: 12}})
		// A completion reported twice is one reply, its tokens counted once.
		provider.emitter.Send(sts.ResponseComplete{ResponseID: "r1", Generation: 1, TimeToFirstByteMs: 420,
			Usage: sts.Usage{InputTokens: 30, OutputTokens: 12}})
		time.Sleep(1500 * time.Millisecond)
		s.Require().NoError(session.ProcessAudio(heard(240), sts.Participant{}))
		provider.emitter.Send(sts.ResponseStarted{ResponseID: "r2", Generation: 2, At: time.Now()})
		provider.emitter.Send(sts.ResponseComplete{ResponseID: "r2", Generation: 2, TimeToFirstByteMs: 910,
			Usage: sts.Usage{InputTokens: 55, OutputTokens: 7}})
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		s.Empty(timingLines(s.T(), logs, session.id), "nothing is logged while the session runs")

		s.Require().NoError(session.Close())
		s.drain(session)

		lines := timingLines(s.T(), logs, session.id)
		s.Require().Len(lines, 1)
		line := lines[0]
		delete(line, "time")
		s.Equal(map[string]any{
			"level": "INFO", "msg": "speech-to-speech timing",
			"call": session.owner.CallID, "app_id": "acme", "agent_id": "concierge",
			"provider": "stub", "model": "stub-model", "region": "eu", "operation": session.id,
			"audio_ms": 840.0, "input_tokens": 85.0, "output_tokens": 19.0, "replies": 2.0,
			"ttfb_ms": 420.0, "duration_ms": 2000.0, "cost_micros": 0.0, "success": true, "error_code": "",
		}, line, "the first reply's wait, everything heard and answered and no word of it; no row was stored, so nothing spent")
	})
}

func (s *STSRouterSuite) TestASessionWithNoReplyLeavesOutTheWait() {
	session, provider, logs := s.timedSession()
	s.Require().NoError(session.ProcessAudio(heard(100), sts.Participant{}))
	provider.emitter.Send(sts.ResponseStarted{ResponseID: "r1", Generation: 1, At: time.Now()})
	s.Require().NoError(session.Close())
	s.drain(session)

	lines := timingLines(s.T(), logs, session.id)
	s.Require().Len(lines, 1)
	s.NotContains(lines[0], "ttfb_ms", "no reply came, so nobody waited for one")
	s.Equal(0.0, lines[0]["replies"])
	s.Equal(100.0, lines[0]["audio_ms"])
	s.Equal(true, lines[0]["success"])
}

// An agent opens a call with a greeting nobody waited for, which the provider reports with a wait
// of zero: the wait the line logs is the first reply someone waited for.
func (s *STSRouterSuite) TestTheWaitIsTheFirstReplyACallerWaitedFor() {
	session, provider, logs := s.timedSession()
	provider.emitter.Send(sts.ResponseStarted{ResponseID: "greet", Generation: 1, At: time.Now()})
	provider.emitter.Send(sts.ResponseComplete{ResponseID: "greet", Generation: 1, TimeToFirstByteMs: 0})
	s.Require().NoError(session.ProcessAudio(heard(600), sts.Participant{}))
	provider.emitter.Send(sts.ResponseStarted{ResponseID: "r2", Generation: 2, At: time.Now()})
	provider.emitter.Send(sts.ResponseComplete{ResponseID: "r2", Generation: 2, TimeToFirstByteMs: 420})
	provider.emitter.Send(sts.ResponseStarted{ResponseID: "r3", Generation: 3, At: time.Now()})
	provider.emitter.Send(sts.ResponseComplete{ResponseID: "r3", Generation: 3, TimeToFirstByteMs: 900})
	s.Require().NoError(session.Close())
	s.drain(session)

	lines := timingLines(s.T(), logs, session.id)
	s.Require().Len(lines, 1)
	s.Equal(420.0, lines[0]["ttfb_ms"])
	s.Equal(3.0, lines[0]["replies"], "the greeting is still a reply")
}

func (s *STSRouterSuite) TestASessionWhoseOnlyReplyWasAGreetingLeavesOutTheWait() {
	session, provider, logs := s.timedSession()
	provider.emitter.Send(sts.ResponseStarted{ResponseID: "greet", Generation: 1, At: time.Now()})
	provider.emitter.Send(sts.ResponseComplete{ResponseID: "greet", Generation: 1, TimeToFirstByteMs: 0})
	s.Require().NoError(session.Close())
	s.drain(session)

	lines := timingLines(s.T(), logs, session.id)
	s.Require().Len(lines, 1)
	s.NotContains(lines[0], "ttfb_ms", "nobody waited for the greeting")
	s.Equal(1.0, lines[0]["replies"])
}

// A provider whose socket dropped ends the session without anyone closing it, and the session
// still says once how it went.
func (s *STSRouterSuite) TestASessionTheProviderEndedLogsOneLine() {
	session, provider, logs := s.timedSession()
	provider.emitter.Send(sts.Error{Err: errors.New("socket closed by peer"), Context: "read", Fatal: true})
	provider.emitter.Close()
	s.drain(session)
	s.Require().NoError(session.Close())

	lines := timingLines(s.T(), logs, session.id)
	s.Require().Len(lines, 1)
	s.Equal(false, lines[0]["success"])
	s.Equal("provider_fatal", lines[0]["error_code"])
	s.Equal("socket closed by peer", lines[0]["error"])
}

// The first failure is what went wrong: a reply that failed before the session lost its socket
// names the reply's failure, and the other way round names the session's.
func (s *STSRouterSuite) TestAFailedSessionNamesItsFirstFailure() {
	for _, tc := range []struct {
		name      string
		send      func(*stubSTS)
		code, err string
	}{
		{
			name: "a reply, then the session",
			send: func(provider *stubSTS) {
				provider.emitter.Send(sts.ResponseStarted{ResponseID: "r1", Generation: 1, At: time.Now()})
				provider.emitter.Send(sts.Error{ResponseID: "r1", Err: errors.New("reply timed out"), Context: "response"})
				provider.emitter.Send(sts.ResponseComplete{ResponseID: "r1", Generation: 1, TimeToFirstByteMs: 300})
				provider.emitter.Send(sts.Error{Err: errors.New("socket closed by peer"), Context: "read", Fatal: true})
			},
			code: "response", err: "reply timed out",
		},
		{
			name: "the session, then a reply",
			send: func(provider *stubSTS) {
				provider.emitter.Send(sts.ResponseStarted{ResponseID: "r1", Generation: 1, At: time.Now()})
				provider.emitter.Send(sts.Error{Err: errors.New("rate limited"), Context: "session"})
				provider.emitter.Send(sts.Error{ResponseID: "r1", Err: errors.New("reply timed out"), Context: "response"})
				provider.emitter.Send(sts.ResponseComplete{ResponseID: "r1", Generation: 1, TimeToFirstByteMs: 300})
			},
			code: "session", err: "rate limited",
		},
	} {
		s.Run(tc.name, func() {
			session, provider, logs := s.timedSession()
			tc.send(provider)
			s.Require().NoError(session.Close())
			s.drain(session)

			lines := timingLines(s.T(), logs, session.id)
			s.Require().Len(lines, 1)
			s.Equal(false, lines[0]["success"])
			s.Equal(tc.code, lines[0]["error_code"])
			s.Equal(tc.err, lines[0]["error"])
			s.Equal(1.0, lines[0]["replies"], "the failed reply is still a reply")
		})
	}
}

// A provider's failure can quote what it was sent, a key included; the line never carries one.
func (s *STSRouterSuite) TestAFailureLogsNoCredentialItQuotes() {
	session, provider, logs := s.timedSession()
	secret := "sk-" + rand.Text() + rand.Text()
	provider.emitter.Send(sts.Error{Err: errors.New("401 invalid api key " + secret), Context: "session"})
	s.Require().NoError(session.Close())
	s.drain(session)

	lines := timingLines(s.T(), logs, session.id)
	s.Require().Len(lines, 1)
	logged, _ := lines[0]["error"].(string)
	s.NotContains(logged, secret)
	s.Contains(logged, "[redacted]")
	s.True(strings.HasPrefix(logged, "401 invalid api key"), "the rest of what the provider said stays: %q", logged)
}

// The caller's audio arrives on the call's goroutine while the provider's replies are counted on
// the session's own, so every frame taken is counted however the two interleave.
func (s *STSRouterSuite) TestAudioTakenWhileRepliesCompleteIsAllCounted() {
	session, provider, logs := s.timedSession()
	var caller sync.WaitGroup
	caller.Go(func() {
		for range 200 {
			s.NoError(session.ProcessAudio(heard(20), sts.Participant{}))
		}
	})
	for i := range 50 {
		id := "r" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		provider.emitter.Send(sts.ResponseStarted{ResponseID: id, Generation: i + 1, At: time.Now()})
		provider.emitter.Send(sts.ResponseComplete{ResponseID: id, Generation: i + 1, Usage: sts.Usage{OutputTokens: 1}})
	}
	caller.Wait()
	s.Require().NoError(session.Close())
	s.drain(session)

	lines := timingLines(s.T(), logs, session.id)
	s.Require().Len(lines, 1)
	s.Equal(4000.0, lines[0]["audio_ms"], "200 frames of 20 ms")
	s.Equal(50.0, lines[0]["replies"])
	s.Equal(50.0, lines[0]["output_tokens"])
}

// A caller still talking as the provider hangs up sends audio on the call's goroutine while the
// session's own one writes the line that counts it.
func (s *STSRouterSuite) TestAudioArrivingAsTheProviderHangsUpIsCountedUpToTheLine() {
	session, provider, logs := s.timedSession()
	stop := make(chan struct{})
	var sent atomic.Int64
	var caller sync.WaitGroup
	caller.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			s.NoError(session.ProcessAudio(heard(20), sts.Participant{}))
			sent.Add(1)
		}
	})
	s.Require().Eventually(func() bool { return sent.Load() >= 50 }, 5*time.Second, time.Millisecond)
	provider.emitter.Close()
	s.drain(session)
	close(stop)
	caller.Wait()

	lines := timingLines(s.T(), logs, session.id)
	s.Require().Len(lines, 1)
	logged, _ := lines[0]["audio_ms"].(float64)
	s.GreaterOrEqual(logged, 1000.0, "the 50 frames taken before the hang-up")
	s.LessOrEqual(logged, float64(sent.Load()*20), "no more than was sent")
}
