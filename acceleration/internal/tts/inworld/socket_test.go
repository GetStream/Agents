package inworld

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool"
	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool/keypooltest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
)

// fakeInworld is a WebSocket server that speaks the context protocol, so the provider can
// be driven over a real connection without an API key.
type fakeInworld struct {
	server  *httptest.Server
	conns   chan *websocket.Conn
	done    chan struct{}
	request chan *http.Request
}

func newFakeInworld() *fakeInworld {
	fake := &fakeInworld{
		conns:   make(chan *websocket.Conn, 4),
		done:    make(chan struct{}),
		request: make(chan *http.Request, 4),
	}

	upgrader := websocket.Upgrader{}
	fake.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.request <- r.Clone(context.Background())
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		fake.conns <- conn
		<-fake.done
	}))
	return fake
}

func (f *fakeInworld) baseURL() string {
	return "ws://" + strings.TrimPrefix(f.server.URL, "http://")
}

func (f *fakeInworld) close() {
	close(f.done)
	f.server.Close()
}

func (f *fakeInworld) accept() *websocket.Conn {
	select {
	case conn := <-f.conns:
		return conn
	case <-time.After(5 * time.Second):
		return nil
	}
}

func speak(conn *websocket.Conn, contextID string, samples []int16) error {
	pcm := audio.PcmData{Samples: samples, SampleRate: DefaultSampleRate, Channels: 1}
	payload, err := json.Marshal(map[string]any{
		"result": map[string]any{
			"contextId":  contextID,
			"audioChunk": map[string]any{"audioContent": base64.StdEncoding.EncodeToString(pcm.Bytes())},
		},
	})
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func flushed(conn *websocket.Conn, contextID string) error {
	payload, err := json.Marshal(map[string]any{
		"result": map[string]any{
			"contextId":      contextID,
			"flushCompleted": map[string]any{},
		},
	})
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

// closed is the server acknowledging a close_context, which is the last thing it sends for
// a context.
func closed(conn *websocket.Conn, contextID string) error {
	payload, err := json.Marshal(map[string]any{
		"result": map[string]any{
			"contextId":     contextID,
			"contextClosed": map[string]any{},
		},
	})
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

type InworldSocketSuite struct {
	suite.Suite
}

func TestInworldSocketSuite(t *testing.T) {
	suite.Run(t, new(InworldSocketSuite))
}

func (s *InworldSocketSuite) newTTS(options Options) *TTS {
	if options.APIKey == "" {
		options.APIKey = "test-key"
	}
	provider, err := New(options)
	s.Require().NoError(err)
	return provider
}

func (s *InworldSocketSuite) connect(fake *fakeInworld, options Options) (*TTS, *websocket.Conn) {
	options.BaseURL = fake.baseURL()
	provider := s.newTTS(options)

	ctx, cancel := context.WithTimeout(keypool.WithCaller(context.Background(), "tts", "inworld-tts-1"), 5*time.Second)
	s.T().Cleanup(cancel)
	s.Require().NoError(provider.Start(ctx))

	conn := fake.accept()
	s.Require().NotNil(conn, "the provider should have connected")
	return provider, conn
}

func (s *InworldSocketSuite) collect(provider *TTS, until func(tts.Event) bool) []tts.Event {
	var events []tts.Event
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event, open := <-provider.Events():
			if !open {
				return events
			}
			events = append(events, event)
			if until(event) {
				return events
			}
		case <-deadline:
			s.FailNow("timed out waiting for events")
			return events
		}
	}
}

func (s *InworldSocketSuite) frames(conn *websocket.Conn, want int) []clientFrame {
	var messages []clientFrame
	for len(messages) < want {
		s.Require().NoError(conn.SetReadDeadline(time.Now().Add(5 * time.Second)))
		_, raw, err := conn.ReadMessage()
		s.Require().NoError(err)

		var message clientFrame
		s.Require().NoError(json.Unmarshal(raw, &message))
		messages = append(messages, message)
	}
	return messages
}

func (s *InworldSocketSuite) TestTheAPIKeyTravelsAsBasicAuth() {
	fake := newFakeInworld()
	defer fake.close()
	provider, _ := s.connect(fake, Options{APIKey: "secret-key"})
	defer provider.Close()

	request := <-fake.request
	s.Equal("Basic secret-key", request.Header.Get("Authorization"))
	s.NotEmpty(request.Header.Get("X-Request-Id"))
	s.Equal(streamPath, request.URL.Path)
}

func (s *InworldSocketSuite) TestAnUtteranceCreatesThenStreamsThenFlushes() {
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{VoiceID: "Clive", SampleRate: 16_000})
	defer provider.Close()

	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "hello"}))
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: " world"}))
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Final: true}))

	messages := s.frames(conn, 4)

	s.Require().NotNil(messages[0].Create)
	s.Equal("u1", messages[0].ContextID)
	s.Equal("Clive", messages[0].Create.VoiceID)
	s.Equal(DefaultModel, messages[0].Create.ModelID)
	s.Equal("PCM", messages[0].Create.AudioConfig.AudioEncoding)
	s.Equal(16_000, messages[0].Create.AudioConfig.SampleRateHertz)

	s.Require().NotNil(messages[1].SendText)
	s.Equal("hello", messages[1].SendText.Text)
	s.Equal("u1", messages[1].ContextID)

	s.Require().NotNil(messages[2].SendText)
	s.Equal(" world", messages[2].SendText.Text)

	s.Require().NotNil(messages[3].FlushContext)
	s.Equal("u1", messages[3].ContextID)
}

func (s *InworldSocketSuite) TestAudioAndFlushSettleTheSynthesis() {
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer provider.Close()

	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "hello there", Final: true}))
	s.frames(conn, 3)

	s.Require().NoError(speak(conn, "u1", make([]int16, 2400)))
	s.Require().NoError(speak(conn, "u1", make([]int16, 2400)))
	s.Require().NoError(flushed(conn, "u1"))

	events := s.collect(provider, func(event tts.Event) bool {
		_, done := event.(tts.SynthesisComplete)
		return done
	})

	var chunks []tts.AudioChunk
	var complete tts.SynthesisComplete
	var started tts.SynthesisStarted
	for _, event := range events {
		switch typed := event.(type) {
		case tts.SynthesisStarted:
			started = typed
		case tts.AudioChunk:
			chunks = append(chunks, typed)
		case tts.SynthesisComplete:
			complete = typed
		}
	}

	s.Equal("u1", started.SynthesisID)
	s.Equal(DefaultVoiceID, started.Voice)
	s.Require().Len(chunks, 2)
	s.Equal(0, chunks[0].Index)
	s.Equal(1, chunks[1].Index)

	s.Equal("u1", complete.SynthesisID)
	s.EqualValues(len("hello there"), complete.Characters)
	s.InDelta(200.0, complete.AudioDurationMs, 1.0)
	s.Positive(complete.TimeToFirstByteMs)
	s.False(complete.Interrupted)
}

func (s *InworldSocketSuite) TestTheTailAfterAFlushIsStillSpoken() {
	// A flush says the text is in, not that the audio for it has all been sent. The rest
	// arrives while the context is being closed, and it is the end of a sentence the caller
	// is waiting to hear rather than something to drop as somebody else's.
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer provider.Close()

	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "hello there", Final: true}))
	s.frames(conn, 3)
	s.Require().NoError(speak(conn, "u1", make([]int16, 2400)))
	s.Require().NoError(flushed(conn, "u1"))
	s.Require().NoError(speak(conn, "u1", make([]int16, 2400)))
	s.Require().NoError(closed(conn, "u1"))

	// The tail arrives after the completion, so the completion cannot be what ends this.
	events := s.collect(provider, func(event tts.Event) bool {
		chunk, ok := event.(tts.AudioChunk)
		return ok && chunk.Index == 1
	})

	var chunks []tts.AudioChunk
	var completions int
	for _, event := range events {
		switch typed := event.(type) {
		case tts.AudioChunk:
			chunks = append(chunks, typed)
		case tts.SynthesisComplete:
			completions++
		}
	}
	s.Len(chunks, 2, "the tail of the utterance was dropped rather than spoken")
	s.Equal(1, completions, "the utterance was settled more than once")
}

func (s *InworldSocketSuite) TestAudioForAClosedContextIsDropped() {
	// Once the server says the context is closed, whatever keeps draining onto the shared
	// socket belongs to nothing, and speaking it would cut into whatever came next.
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer provider.Close()

	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "hello", Final: true}))
	s.frames(conn, 3)
	s.Require().NoError(flushed(conn, "u1"))
	s.Require().NoError(closed(conn, "u1"))
	s.collect(provider, func(event tts.Event) bool {
		_, done := event.(tts.SynthesisComplete)
		return done
	})

	s.Require().NoError(speak(conn, "u1", make([]int16, 2400)))
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u2", Text: "next", Final: true}))
	s.frames(conn, 3)
	s.Require().NoError(speak(conn, "u2", make([]int16, 2400)))

	events := s.collect(provider, func(event tts.Event) bool {
		chunk, ok := event.(tts.AudioChunk)
		return ok && chunk.SynthesisID == "u2"
	})
	for _, event := range events {
		if chunk, ok := event.(tts.AudioChunk); ok {
			s.NotEqual("u1", chunk.SynthesisID,
				"audio for a closed context was spoken over the next utterance")
		}
	}
}

func (s *InworldSocketSuite) TestFlushClosesTheContextOnce() {
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer provider.Close()

	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "hello", Final: true}))
	s.frames(conn, 3)
	s.Require().NoError(flushed(conn, "u1"))
	s.collect(provider, func(event tts.Event) bool {
		_, done := event.(tts.SynthesisComplete)
		return done
	})

	closed := s.frames(conn, 1)
	s.Require().NotNil(closed[0].CloseContext)
	s.Equal("u1", closed[0].ContextID)
}

func (s *InworldSocketSuite) TestInterruptClosesTheContextAndDropsAudioStillOnTheWire() {
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer provider.Close()

	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "a long answer", Final: true}))
	s.frames(conn, 3)
	s.Require().NoError(speak(conn, "u1", make([]int16, 2400)))
	s.collect(provider, func(event tts.Event) bool {
		_, ok := event.(tts.AudioChunk)
		return ok
	})

	s.Require().NoError(provider.Interrupt())

	s.Require().NoError(speak(conn, "u1", make([]int16, 2400)))

	events := s.collect(provider, func(event tts.Event) bool {
		_, done := event.(tts.SynthesisComplete)
		return done
	})
	complete := events[len(events)-1].(tts.SynthesisComplete)

	s.True(complete.Interrupted)
	s.InDelta(100.0, complete.AudioDurationMs, 1.0,
		"only the audio heard before the interruption counts")

	var closed clientFrame
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(5 * time.Second)))
	_, raw, err := conn.ReadMessage()
	s.Require().NoError(err)
	s.Require().NoError(json.Unmarshal(raw, &closed))
	s.Equal("u1", closed.ContextID)
	s.NotNil(closed.CloseContext)
}

func (s *InworldSocketSuite) TestAnUtteranceCannotChangeVoiceHalfway() {
	fake := newFakeInworld()
	defer fake.close()
	provider, _ := s.connect(fake, Options{VoiceID: "Sarah"})
	defer provider.Close()

	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "hello"}))

	err := provider.Synthesize(tts.Request{ID: "u1", Text: " world", Voice: "Clive"})
	s.ErrorContains(err, "being said in voice Sarah, not Clive")
}

func (s *InworldSocketSuite) TestAnUtteranceCanPickItsOwnVoice() {
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{VoiceID: "Sarah"})
	defer provider.Close()

	s.Require().NoError(provider.Synthesize(tts.Request{
		ID: "u1", Text: "hello", Voice: "Clive", Final: true,
	}))

	s.Equal("Clive", s.frames(conn, 1)[0].Create.VoiceID)
}

func (s *InworldSocketSuite) TestALostConnectionSettlesWhatWasInFlight() {
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer provider.Close()

	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "hello", Final: true}))
	s.Require().NoError(conn.Close())

	events := s.collect(provider, func(event tts.Event) bool {
		_, done := event.(tts.Error)
		return done
	})
	var completed bool
	for _, event := range events {
		if complete, ok := event.(tts.SynthesisComplete); ok {
			completed = true
			s.Equal("u1", complete.SynthesisID)
			s.True(complete.Interrupted, "work cut short by a dropped connection is not work completed")
		}
	}
	s.True(completed)
	s.True(events[len(events)-1].(tts.Error).Fatal, "lost speech must still fail the session")
}

func (s *InworldSocketSuite) TestAnIdleEOFReconnectsAndTheNextUtteranceIsAudible() {
	fake := newFakeInworld()
	provider, conn := s.connect(fake, Options{})
	s.collect(provider, func(event tts.Event) bool { _, ok := event.(tts.Connected); return ok })

	// The previous reply has closed, and the server drops TCP without a WebSocket close
	// frame while nothing is being synthesized.
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "previous", Text: "hello", Final: true}))
	s.frames(conn, 3)
	s.Require().NoError(closed(conn, "previous"))
	s.collect(provider, func(event tts.Event) bool { _, ok := event.(tts.SynthesisComplete); return ok })
	s.Require().NoError(conn.Close())
	events := s.collect(provider, func(event tts.Event) bool {
		_, connected := event.(tts.Connected)
		_, failed := event.(tts.Error)
		return connected || failed
	})
	s.Require().IsType(tts.Connected{}, events[len(events)-1])

	replacement := fake.accept()
	s.Require().NotNil(replacement)
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "next", Text: "still here", Final: true}))
	frames := s.frames(replacement, 3)
	s.NotNil(frames[0].Create)
	s.Equal("next", frames[0].ContextID)
	s.Equal("still here", frames[1].SendText.Text)
	s.NotNil(frames[2].FlushContext)
	s.Require().NoError(speak(replacement, "next", []int16{1000, 2000, -1000}))
	s.Require().NoError(closed(replacement, "next"))
	events = s.collect(provider, func(event tts.Event) bool { _, ok := event.(tts.SynthesisComplete); return ok })
	var heard bool
	for _, event := range events {
		switch typed := event.(type) {
		case tts.AudioChunk:
			heard = true
			s.Equal("next", typed.SynthesisID)
			s.Equal([]int16{1000, 2000, -1000}, typed.Audio.Samples)
		case tts.Error:
			s.Fail("idle recovery emitted a fatal measurement error", typed.Error())
		}
	}
	s.True(heard)
	s.False(events[len(events)-1].(tts.SynthesisComplete).Interrupted)
}

func (s *InworldSocketSuite) TestAnIdleEOFFailsWhenTheVoiceCannotReconnect() {
	fake := newFakeInworld()
	provider, conn := s.connect(fake, Options{HandshakeTimeout: time.Second})
	s.collect(provider, func(event tts.Event) bool { _, ok := event.(tts.Connected); return ok })
	s.Require().NoError(fake.server.Listener.Close())
	s.Require().NoError(conn.Close())
	events := s.collect(provider, func(event tts.Event) bool {
		_, connected := event.(tts.Connected)
		_, failed := event.(tts.Error)
		return connected || failed
	})
	s.Require().IsType(tts.Error{}, events[len(events)-1])
	failure := events[len(events)-1].(tts.Error)
	s.True(failure.Fatal)
	s.ErrorContains(failure.Err, "reopen after websocket: close 1006")
}

func (s *InworldSocketSuite) TestAnEOFWhileTheAudioTailIsPendingRemainsAFailure() {
	fake := newFakeInworld()
	provider, conn := s.connect(fake, Options{})
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "tail", Text: "not finished", Final: true}))
	s.frames(conn, 3)
	s.Require().NoError(flushed(conn, "tail"))
	s.collect(provider, func(event tts.Event) bool { _, ok := event.(tts.SynthesisComplete); return ok })
	// flushCompleted settles accounting before the server's final audio. Only
	// contextClosed makes it safe to call this connection idle.
	s.Require().NoError(conn.Close())
	events := s.collect(provider, func(event tts.Event) bool {
		_, connected := event.(tts.Connected)
		_, failed := event.(tts.Error)
		return connected || failed
	})
	s.Require().IsType(tts.Error{}, events[len(events)-1])
	s.True(events[len(events)-1].(tts.Error).Fatal)
}

// invalidKeyFrame is how Inworld refused a made-up key on 2026-10-28, right after the
// handshake, before closing with 1000.
const invalidKeyFrame = `{"error":{"code":7,"message":"API key does not exist or was deleted: \"ZGty***\"","details":[]}}`

// refusedBy is how many invalid keys INWORLD_API_KEY's pool counted while the server sent
// these frames on a voice opened on a key of its own; it waits for the error each frame
// reports.
func (s *InworldSocketSuite) refusedBy(frames ...string) float64 {
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{APIKey: rand.Text()})
	defer provider.Close()
	before := keypooltest.Series(s.T(), "router_keypool_refused_attempts_total", "INWORLD_API_KEY")["/customer/tts/invalid_key"]

	for _, frame := range frames {
		s.Require().NoError(conn.WriteMessage(websocket.TextMessage, []byte(frame)))
	}
	failures := 0
	s.collect(provider, func(event tts.Event) bool {
		if _, failed := event.(tts.Error); failed {
			failures++
		}
		return failures == len(frames)
	})
	return keypooltest.Series(s.T(), "router_keypool_refused_attempts_total", "INWORLD_API_KEY")["/customer/tts/invalid_key"] - before
}

func (s *InworldSocketSuite) TestAnInvalidKeyFrameIsCountedOncePerConnection() {
	s.Equal(1.0, s.refusedBy(invalidKeyFrame, invalidKeyFrame))
}

// Inworld's answer to a key that is not base64 of id:secret: a 101, this frame, then a close
// with 1000 (probed 2026-10-28), though the same text on a 403 is counted at the handshake.
func (s *InworldSocketSuite) TestInvalidCredentialsAreAnInvalidKey() {
	s.Equal(1.0, s.refusedBy(`{"error":{"code":7,"message":"Invalid authorization credentials","details":[]}}`))
}

func (s *InworldSocketSuite) TestAnotherPermissionDeniedIsNotAnInvalidKey() {
	s.Zero(s.refusedBy(`{"error":{"code":7,"message":"Permission denied on model inworld-tts-1","details":[]}}`))
}

func (s *InworldSocketSuite) TestTheKeyTextUnderAnotherCodeIsNotAnInvalidKey() {
	s.Zero(s.refusedBy(`{"error":{"code":5,"message":"API key does not exist or was deleted","details":[]}}`))
}

func (s *InworldSocketSuite) TestTheKeyCodeAsAStringIsNotAnInvalidKey() {
	s.Zero(s.refusedBy(`{"error":{"code":"7","message":"API key does not exist or was deleted","details":[]}}`))
}

func (s *InworldSocketSuite) TestTheKeyTextAsAPlainErrorIsNotAnInvalidKey() {
	s.Zero(s.refusedBy(`{"error":"API key does not exist or was deleted"}`))
}

func (s *InworldSocketSuite) TestTextSentAfterTheFinalTextBeforeItsFlushIsSpokenInTheSameUtterance() {
	// The router bills a streaming voice once for each completion, so text spoken in an
	// utterance still open must not complete it a second time.
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer provider.Close()

	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "a", Final: true}))
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "bcdefgh", Final: true}))
	var creates int
	for _, frame := range s.frames(conn, 5) {
		s.Equal("u1", frame.ContextID, "both texts go to the one context")
		if frame.Create != nil {
			creates++
		}
	}
	s.Equal(1, creates, "the context is opened once")
	s.Require().NoError(speak(conn, "u1", make([]int16, 240)))
	s.Require().NoError(flushed(conn, "u1"))
	s.Require().NoError(speak(conn, "u1", make([]int16, 480)))
	s.Require().NoError(flushed(conn, "u1"))
	s.Require().NoError(closed(conn, "u1"))
	// Whatever u1 still sends comes before what the next utterance does.
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u2", Text: "i", Final: true}))
	s.Require().NoError(speak(conn, "u2", make([]int16, 240)))
	s.Require().NoError(flushed(conn, "u2"))

	events := s.collect(provider, func(event tts.Event) bool {
		complete, ok := event.(tts.SynthesisComplete)
		return ok && complete.SynthesisID == "u2"
	})
	var completions []tts.SynthesisComplete
	var chunks int
	for _, event := range events {
		switch typed := event.(type) {
		case tts.SynthesisComplete:
			if typed.SynthesisID == "u1" {
				completions = append(completions, typed)
			}
		case tts.AudioChunk:
			if typed.SynthesisID == "u1" {
				chunks++
			}
		}
	}
	s.Require().Len(completions, 1, "the utterance completes once for both texts")
	s.EqualValues(8, completions[0].Characters)
	s.Equal(2, chunks, "the audio for both texts is heard")
}

func (s *InworldSocketSuite) TestFailedResetConnectionLeavesSessionRedialable() {
	var reject atomic.Bool
	rejected := make(chan struct{}, 1)
	conns := make(chan *websocket.Conn, 4)
	done := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reject.Load() {
			select {
			case rejected <- struct{}{}:
			default:
			}
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conns <- conn
		<-done
	}))
	defer func() {
		close(done)
		server.Close()
	}()

	provider := s.newTTS(Options{BaseURL: "ws://" + strings.TrimPrefix(server.URL, "http://")})
	defer provider.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Require().NoError(provider.Start(ctx))
	first := <-conns
	s.collect(provider, func(event tts.Event) bool { _, ok := event.(tts.Connected); return ok })

	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "hello", Final: true}))
	s.frames(first, 3)

	// Force the reconnect inside resetConnection to fail, then verify the session stays
	// in Disconnected rather than bricking subsequent Synthesize calls with "not started".
	reject.Store(true)
	payload, err := json.Marshal(map[string]any{
		"result": map[string]any{
			"contextId": "u1",
			"status":    map[string]any{"code": 8, "message": "max contexts limit reached"},
		},
	})
	s.Require().NoError(err)
	s.Require().NoError(first.WriteMessage(websocket.TextMessage, payload))
	s.collect(provider, func(event tts.Event) bool {
		complete, ok := event.(tts.SynthesisComplete)
		return ok && complete.SynthesisID == "u1"
	})

	// Wait until resetConnection's dial actually hits the server and finishes its failure transition.
	select {
	case <-rejected:
	case <-time.After(5 * time.Second):
		s.FailNow("resetConnection did not attempt a reconnect while server was rejecting")
	}
	s.Require().Eventually(func() bool {
		provider.mu.Lock()
		settled := provider.state != connConnecting && provider.conn == nil
		provider.mu.Unlock()
		return settled
	}, 5*time.Second, 5*time.Millisecond)

	reject.Store(false)
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u2", Text: "recovered", Final: true}))
	var second *websocket.Conn
	select {
	case second = <-conns:
	case <-time.After(5 * time.Second):
		s.FailNow("Synthesize did not redial after a failed resetConnection")
	}
	frames := s.frames(second, 3)
	s.Require().NotNil(frames[0].Create)
	s.Equal("u2", frames[0].ContextID)
	s.Equal("recovered", frames[1].SendText.Text)
	s.Require().NotNil(frames[2].FlushContext)

	s.Require().NoError(speak(second, "u2", []int16{1000, -1000}))
	s.Require().NoError(closed(second, "u2"))
	events := s.collect(provider, func(event tts.Event) bool {
		complete, ok := event.(tts.SynthesisComplete)
		return ok && complete.SynthesisID == "u2"
	})
	var heard bool
	for _, event := range events {
		if chunk, ok := event.(tts.AudioChunk); ok && chunk.SynthesisID == "u2" {
			heard = true
		}
	}
	s.True(heard)
}

func (s *InworldSocketSuite) TestAnIdleEOFReconnectsAfterInterruptedAndErroredUtterances() {
	fake := newFakeInworld()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer provider.Close()
	s.collect(provider, func(event tts.Event) bool { _, ok := event.(tts.Connected); return ok })

	// Interrupt an active utterance without the server ever sending contextClosed.
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u1", Text: "interrupted", Final: true}))
	s.frames(conn, 3)
	s.Require().NoError(provider.Interrupt())
	s.frames(conn, 1)
	s.collect(provider, func(event tts.Event) bool {
		complete, ok := event.(tts.SynthesisComplete)
		return ok && complete.SynthesisID == "u1"
	})

	// Reject a second utterance with a status error and no contextClosed.
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u2", Text: "rejected", Final: true}))
	s.frames(conn, 3)
	errFrame, err := json.Marshal(map[string]any{
		"result": map[string]any{
			"contextId": "u2",
			"status":    map[string]any{"code": 3, "message": "Invalid model"},
		},
	})
	s.Require().NoError(err)
	s.Require().NoError(conn.WriteMessage(websocket.TextMessage, errFrame))
	s.collect(provider, func(event tts.Event) bool {
		complete, ok := event.(tts.SynthesisComplete)
		return ok && complete.SynthesisID == "u2"
	})

	// Interrupt a draining utterance (flushed, before contextClosed) so its tail is dropped
	// and it is removed from active.
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u3", Text: "draining", Final: true}))
	s.frames(conn, 3)
	s.Require().NoError(flushed(conn, "u3"))
	s.collect(provider, func(event tts.Event) bool {
		complete, ok := event.(tts.SynthesisComplete)
		return ok && complete.SynthesisID == "u3"
	})
	s.frames(conn, 1)
	s.Require().NoError(provider.Interrupt())

	// With all utterances settled and forgotten, an abnormal EOF is idle and reconnects.
	s.Require().NoError(conn.Close())
	events := s.collect(provider, func(event tts.Event) bool {
		_, connected := event.(tts.Connected)
		_, failed := event.(tts.Error)
		return connected || failed
	})
	s.Require().IsType(tts.Connected{}, events[len(events)-1])

	replacement := fake.accept()
	s.Require().NotNil(replacement)
	s.Require().NoError(provider.Synthesize(tts.Request{ID: "u4", Text: "after idle", Final: true}))
	s.Equal("after idle", s.frames(replacement, 3)[1].SendText.Text)
}

func (s *InworldSocketSuite) TestFailedSendCleansUpActiveUtterance() {
	fake := newFakeInworld()
	provider, conn := s.connect(fake, Options{HandshakeTimeout: time.Second})
	s.collect(provider, func(event tts.Event) bool { _, ok := event.(tts.Connected); return ok })

	// Stop the server and drop the socket so send cannot write or redial.
	s.Require().NoError(fake.server.Listener.Close())
	s.Require().NoError(conn.Close())
	s.collect(provider, func(event tts.Event) bool {
		failure, ok := event.(tts.Error)
		return ok && failure.Fatal
	})

	err := provider.Synthesize(tts.Request{ID: "u1", Text: "hello", Final: true})
	s.Require().Error(err)

	provider.mu.Lock()
	activeCount := len(provider.active)
	provider.mu.Unlock()
	s.Zero(activeCount, "a failed send must not leave the utterance registered in active")

	s.Require().NoError(provider.Close())
	for _, event := range s.collect(provider, func(tts.Event) bool { return false }) {
		if complete, ok := event.(tts.SynthesisComplete); ok {
			s.Failf("unexpected completion", "failed utterance %s should not be settled on Close", complete.SynthesisID)
		}
	}
}

func (s *InworldSocketSuite) TestConcurrentRedialAndSendDoNotDoubleDialOrRaceWithClient() {
	var dials atomic.Int32
	dial2Started := make(chan struct{}, 1)
	releaseDial2 := make(chan struct{})
	var releaseOnce sync.Once
	conns := make(chan *websocket.Conn, 4)
	done := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := dials.Add(1)
		if n == 2 {
			select {
			case dial2Started <- struct{}{}:
			default:
			}
			<-releaseDial2
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conns <- conn
		<-done
	}))
	defer func() {
		releaseOnce.Do(func() { close(releaseDial2) })
		close(done)
		server.Close()
	}()

	provider := s.newTTS(Options{
		BaseURL:   "ws://" + strings.TrimPrefix(server.URL, "http://"),
		reconnect: time.Millisecond,
	})
	defer provider.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Require().NoError(provider.Start(ctx))
	first := <-conns
	s.collect(provider, func(event tts.Event) bool { _, ok := event.(tts.Connected); return ok })

	closure := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "idle")
	s.Require().NoError(first.WriteControl(websocket.CloseMessage, closure, time.Now().Add(time.Second)))
	s.Require().NoError(first.Close())
	s.collect(provider, func(event tts.Event) bool { _, ok := event.(tts.Disconnected); return ok })

	// Let redial enter dialLocked first, then race Synthesize and Client() against it.
	select {
	case <-dial2Started:
	case <-time.After(5 * time.Second):
		s.FailNow("redial did not start replacement dial")
	}

	stopPoll := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopPoll:
				return
			default:
				_ = provider.Client()
			}
		}
	}()

	synthErr := make(chan error, 1)
	go func() {
		synthErr <- provider.Synthesize(tts.Request{ID: "u1", Text: "hello", Final: true})
	}()
	// Give Synthesize time to reach writeMu while redial holds the dial open.
	time.Sleep(25 * time.Millisecond)
	releaseOnce.Do(func() { close(releaseDial2) })

	s.Require().NoError(<-synthErr)
	close(stopPoll)
	wg.Wait()

	second := <-conns
	s.Equal("hello", s.frames(second, 3)[1].SendText.Text)
	s.Equal(int32(2), dials.Load(), "background redial and on-demand send must share one dial")
}

func (s *InworldSocketSuite) TestSynthesizeDoesNotReconnectAfterStartFailed() {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	provider := s.newTTS(Options{BaseURL: "ws://" + strings.TrimPrefix(server.URL, "http://")})
	defer provider.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.Error(provider.Start(ctx))
	s.ErrorContains(provider.Synthesize(tts.Request{ID: "u1", Text: "hello"}), "not started")
	s.Equal(int32(1), requests.Load(), "Synthesize must not silently retry a failed initial Start")
}
