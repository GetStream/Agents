package gemini

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

	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool"
	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool/keypooltest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sts"
)

// GeminiSocketSuite exercises the provider against a Live API that is answering, which is
// where the setup frame, the audio encoding and the resume live.
type GeminiSocketSuite struct {
	suite.Suite
}

func TestGeminiSocketSuite(t *testing.T) {
	suite.Run(t, new(GeminiSocketSuite))
}

// fakeLive is a Live API that accepts sessions one after another. It completes the setup
// exchange itself, so a test starts from a connection that is ready for audio, and it keeps
// every setup it was sent so a resumed session can be checked for its handle.
type fakeLive struct {
	server *httptest.Server
	conns  chan *websocket.Conn
	setups chan setup
	done   chan struct{}
}

func newFakeLive() *fakeLive {
	fake := &fakeLive{
		conns:  make(chan *websocket.Conn, 4),
		setups: make(chan setup, 4),
		done:   make(chan struct{}),
	}

	upgrader := websocket.Upgrader{}
	fake.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var frame clientMessage
		if err := json.Unmarshal(raw, &frame); err != nil || frame.Setup == nil {
			return
		}
		fake.setups <- *frame.Setup

		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"setupComplete":{}}`)); err != nil {
			return
		}
		fake.conns <- conn
		<-fake.done
	}))
	return fake
}

func (f *fakeLive) endpoint() string {
	return "ws://" + strings.TrimPrefix(f.server.URL, "http://")
}

func (f *fakeLive) close() {
	close(f.done)
	f.server.Close()
}

// connect returns a started provider and the server side of its connection.
func (s *GeminiSocketSuite) connect(fake *fakeLive, settings Options) (*STS, *websocket.Conn) {
	settings.URL = fake.endpoint()
	if settings.APIKey == "" {
		settings.APIKey = "test-key"
	}
	if settings.HandshakeTimeout == 0 {
		settings.HandshakeTimeout = 5 * time.Second
	}
	provider, err := New(settings)
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	s.T().Cleanup(cancel)
	s.Require().NoError(provider.Start(ctx))
	return provider, s.nextConn(fake)
}

func (s *GeminiSocketSuite) nextConn(fake *fakeLive) *websocket.Conn {
	select {
	case conn := <-fake.conns:
		return conn
	case <-time.After(5 * time.Second):
		s.FailNow("the provider never connected")
		return nil
	}
}

// nextFrame reads the next message the provider sent upstream.
func (s *GeminiSocketSuite) nextFrame(conn *websocket.Conn) clientMessage {
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(5 * time.Second)))
	_, raw, err := conn.ReadMessage()
	s.Require().NoError(err)

	var frame clientMessage
	s.Require().NoError(json.Unmarshal(raw, &frame))
	return frame
}

func (s *GeminiSocketSuite) serve(conn *websocket.Conn, message serverMessage) {
	raw, err := json.Marshal(message)
	s.Require().NoError(err)
	s.Require().NoError(conn.WriteMessage(websocket.TextMessage, raw))
}

// nextEvent waits for the next event of the wanted kind.
func (s *GeminiSocketSuite) nextEvent(provider *STS, wanted func(sts.Event) bool) sts.Event {
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event, open := <-provider.Events():
			if !open {
				s.FailNow("the session ended first")
				return nil
			}
			if wanted(event) {
				return event
			}
		case <-deadline:
			s.FailNow("timed out waiting for an event")
			return nil
		}
	}
}

func (s *GeminiSocketSuite) TestTheSessionAsksForAudioTranscriptsToolsAndAVoice() {
	fake := newFakeLive()
	defer fake.close()
	on := true
	provider, _ := s.connect(fake, Options{
		Voice:            "Kore",
		Instructions:     "Be brief.",
		Tools:            []llm.Tool{{Name: "get_weather", Description: "Weather.", Parameters: map[string]any{"type": "object"}}},
		InputTranscript:  true,
		OutputTranscript: true,
		ThinkingLevel:    "low",
		ProactiveAudio:   &on,
	})
	defer func() { _ = provider.Close() }()

	opened := <-fake.setups
	s.Equal("models/"+DefaultModel, opened.Model)
	s.Equal([]string{"AUDIO"}, opened.GenerationConfig.ResponseModalities)
	s.Equal("Kore", opened.GenerationConfig.SpeechConfig.VoiceConfig.PrebuiltVoiceConfig.VoiceName)
	s.Equal("low", opened.GenerationConfig.ThinkingConfig.ThinkingLevel)
	s.Equal("Be brief.", opened.SystemInstruction.Parts[0].Text)
	s.Require().Len(opened.Tools, 1)
	s.Equal("get_weather", opened.Tools[0].FunctionDeclarations[0].Name)
	s.NotNil(opened.InputAudioTranscription)
	s.NotNil(opened.OutputAudioTranscription)
	s.Require().NotNil(opened.SessionResumption, "always asked for, or the session could never be resumed")
	s.Empty(opened.SessionResumption.Handle)
	s.True(opened.Proactivity.ProactiveAudio)
}

func (s *GeminiSocketSuite) TestASessionAskingForNothingExtraSendsNothingExtra() {
	fake := newFakeLive()
	defer fake.close()
	provider, _ := s.connect(fake, Options{})
	defer func() { _ = provider.Close() }()

	opened := <-fake.setups
	s.Nil(opened.GenerationConfig.SpeechConfig, "no voice, so the model's own")
	s.Nil(opened.InputAudioTranscription, "not asked for, so not billed for")
	s.Nil(opened.SystemInstruction)
	s.Nil(opened.RealtimeInputConfig)
}

func (s *GeminiSocketSuite) TestAudioAndFramesArriveAsWhatTheyWere() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer func() { _ = provider.Close() }()

	samples := []int16{1, -2, 3, -4}
	pcm := sts.PcmData{Samples: samples, SampleRate: sts.InputSampleRate, Channels: 1}
	s.Require().NoError(provider.ProcessAudio(pcm, sts.Participant{ID: "alice"}))

	frame := s.nextFrame(conn)
	s.Require().NotNil(frame.RealtimeInput)
	s.Equal("audio/pcm;rate=16000", frame.RealtimeInput.Audio.MimeType)
	decoded, err := base64.StdEncoding.DecodeString(frame.RealtimeInput.Audio.Data)
	s.Require().NoError(err)
	s.Equal(pcm.Bytes(), decoded)

	s.Require().NoError(provider.SendFrame(llm.ImagePart{MIME: "image/jpeg", Data: []byte{0xff, 0xd8}}))
	still := s.nextFrame(conn)
	s.Equal("image/jpeg", still.RealtimeInput.Video.MimeType)
}

func (s *GeminiSocketSuite) TestATypedTurnAndAPromptAreSentComplete() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer func() { _ = provider.Close() }()

	s.Require().NoError(provider.SendText("hello", sts.Participant{ID: "alice"}))
	turn := s.nextFrame(conn)
	s.Require().NotNil(turn.ClientContent)
	s.True(turn.ClientContent.TurnComplete, "an incomplete turn would never be answered")
	s.Equal("user", turn.ClientContent.Turns[0].Role)
	s.Equal("hello", turn.ClientContent.Turns[0].Parts[0].Text)

	s.Require().NoError(provider.Prompt("Greet the caller."))
	prompt := s.nextFrame(conn)
	s.Equal("Greet the caller.", prompt.ClientContent.Turns[0].Parts[0].Text)
}

func (s *GeminiSocketSuite) TestAToolAnswerCarriesTheNameItWasCalledUnder() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer func() { _ = provider.Close() }()

	s.serve(conn, serverMessage{ToolCall: &toolCall{FunctionCalls: []functionCall{{ID: "call_1", Name: "get_weather"}}}})
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.ToolCall); return ok })

	s.Require().NoError(provider.Answer("call_1", `{"forecast":"sunny"}`, nil))
	answer := s.nextFrame(conn)
	s.Require().NotNil(answer.ToolResponse)
	s.Equal("call_1", answer.ToolResponse.FunctionResponses[0].ID)
	s.Equal("get_weather", answer.ToolResponse.FunctionResponses[0].Name)
	s.Equal(`{"forecast":"sunny"}`, answer.ToolResponse.FunctionResponses[0].Response["output"])
}

func (s *GeminiSocketSuite) TestTheSessionResumesWithItsHandleWhenTheServerHangsUp() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer func() { _ = provider.Close() }()
	<-fake.setups

	s.serve(conn, serverMessage{SessionResumptionUpdate: &resumptionUpdate{NewHandle: "h-1", Resumable: true}})
	s.serve(conn, serverMessage{GoAway: &goAway{TimeLeft: "5s"}})
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.SessionExpiring); return ok })

	// The server cuts the connection the way it does every ten minutes.
	s.Require().NoError(conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "session timeout"), time.Now().Add(time.Second)))
	conn.Close()

	dropped := s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.Disconnected); return ok })
	s.True(dropped.(sts.Disconnected).Clean, "a connection cut so it can be resumed is not a failure")
	s.Equal("resuming", dropped.(sts.Disconnected).Reason)

	s.nextConn(fake)
	resumed := <-fake.setups
	s.Require().NotNil(resumed.SessionResumption)
	s.Equal("h-1", resumed.SessionResumption.Handle, "the new connection has to pick the conversation up where it was")
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.Connected); return ok })
}

// sendAudio streams ms of silence and returns the wall times around it, which bracket when
// the provider took the audio as sent.
func (s *GeminiSocketSuite) sendAudio(provider *STS, conn *websocket.Conn, ms int) (time.Time, time.Time) {
	before := time.Now()
	pcm := sts.PcmData{Samples: make([]int16, sts.InputSampleRate*ms/1000), SampleRate: sts.InputSampleRate, Channels: 1}
	s.Require().NoError(provider.ProcessAudio(pcm, sts.Participant{ID: "alice"}))
	after := time.Now()
	s.NotNil(s.nextFrame(conn).RealtimeInput)
	return before, after
}

// answer has the server hear words, end the caller's speech at offset in the same frame as
// the reply's audio, and finish the turn. It returns the stop and the settled reply.
func (s *GeminiSocketSuite) answer(provider *STS, conn *websocket.Conn, activity *voiceActivity) (sts.SpeechStopped, sts.ResponseComplete) {
	s.serve(conn, serverMessage{ServerContent: &serverContent{InputTranscription: &transcription{Text: "hello"}}})
	reply := spoke(2400)
	reply.VoiceActivity = activity
	s.serve(conn, reply)
	s.serve(conn, turnComplete())
	stopped := s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.SpeechStopped); return ok })
	complete := s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.ResponseComplete); return ok })
	return stopped.(sts.SpeechStopped), complete.(sts.ResponseComplete)
}

func (s *GeminiSocketSuite) TestTheCallerStoppedWhereTheDetectorSaysTheirSpeechEnded() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{InputTranscript: true})
	defer func() { _ = provider.Close() }()

	s.sendAudio(provider, conn, 1000)
	before, after := s.sendAudio(provider, conn, 1000)
	time.Sleep(50 * time.Millisecond)
	stopped, complete := s.answer(provider, conn, &voiceActivity{Type: "ACTIVITY_END", AudioOffset: "1.700s"})

	// 2 s were sent and the speech ended 0.3 s before the last of it went.
	s.False(stopped.At.Before(before.Add(-300*time.Millisecond)), "stop %v, last audio sent from %v", stopped.At, before)
	s.False(stopped.At.After(after.Add(-300*time.Millisecond)), "stop %v, last audio sent by %v", stopped.At, after)
	s.GreaterOrEqual(complete.TimeToFirstByteMs, 350.0, "the wait is timed from the detector's end of speech, not the words")
}

func (s *GeminiSocketSuite) TestAnEndOfSpeechPastWhatWasSentLeavesTheLastWordHeard() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{InputTranscript: true})
	defer func() { _ = provider.Close() }()

	_, after := s.sendAudio(provider, conn, 500)
	stopped, _ := s.answer(provider, conn, &voiceActivity{Type: "ACTIVITY_END", AudioOffset: "600.000s"})
	s.True(stopped.At.After(after), "an offset counted from another connection is not placed on this one")
	s.False(stopped.At.After(time.Now()), "nor in the future")
}

func (s *GeminiSocketSuite) TestAStartDropsAnEndOfSpeechTheTurnBeforeLeftUnused() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{InputTranscript: true})
	defer func() { _ = provider.Close() }()

	s.sendAudio(provider, conn, 1000)
	s.answer(provider, conn, nil)
	// The end of the first turn's speech arrives after its reply settled.
	s.serve(conn, serverMessage{VoiceActivity: &voiceActivity{Type: "ACTIVITY_END", AudioOffset: "0.500s"}})
	s.serve(conn, serverMessage{VoiceActivity: &voiceActivity{Type: "ACTIVITY_START", AudioOffset: "0.900s"}})
	_, after := s.sendAudio(provider, conn, 100)
	stopped, _ := s.answer(provider, conn, nil)
	s.True(stopped.At.After(after), "the next turn is timed from its own words, not the stale end")
}

func (s *GeminiSocketSuite) TestAResumedConnectionCountsItsAudioAfresh() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{InputTranscript: true})
	defer func() { _ = provider.Close() }()
	<-fake.setups

	s.sendAudio(provider, conn, 2000)
	s.serve(conn, serverMessage{SessionResumptionUpdate: &resumptionUpdate{NewHandle: "h-1", Resumable: true}})
	s.Require().NoError(conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "session timeout"), time.Now().Add(time.Second)))
	conn.Close()
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.Disconnected); return ok })
	resumed := s.nextConn(fake)
	<-fake.setups
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.Connected); return ok })

	before, after := s.sendAudio(provider, resumed, 500)
	stopped, _ := s.answer(provider, resumed, &voiceActivity{Type: "ACTIVITY_END", AudioOffset: "0.300s"})
	s.False(stopped.At.Before(before.Add(-200*time.Millisecond)), "the offset is placed on this connection's audio only")
	s.False(stopped.At.After(after.Add(-200 * time.Millisecond)))
}

func (s *GeminiSocketSuite) TestANonBlockingSessionsToolsSayTheirResultFollows() {
	fake := newFakeLive()
	defer fake.close()
	provider, _ := s.connect(fake, Options{
		NonBlocking: true,
		Tools:       []llm.Tool{{Name: "get_weather", Description: "Weather.", Parameters: map[string]any{"type": "object"}}},
	})
	defer func() { _ = provider.Close() }()

	declared := (<-fake.setups).Tools[0].FunctionDeclarations[0]
	s.Equal("NON_BLOCKING", declared.Behavior)
	s.Contains(declared.Description, "Weather.")
	s.Contains(declared.Description, "do not guess", "a model that keeps talking must not make the result up")
	s.True(provider.UpdatesCalls())
}

func (s *GeminiSocketSuite) TestAnOpenCallIsToldOfWithAPartialResponseAndEndedByItsResult() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{NonBlocking: true, Tools: []llm.Tool{{Name: "get_weather"}}})
	defer func() { _ = provider.Close() }()
	s.serve(conn, serverMessage{ToolCall: &toolCall{FunctionCalls: []functionCall{{ID: "call_1", Name: "get_weather"}}}})
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.ToolCall); return ok })

	s.Require().NoError(provider.UpdateCall(sts.CallUpdate{CallID: "call_1", Status: "searching", Guidance: "Tell them where it is."}))

	partial := s.nextFrame(conn).ToolResponse.FunctionResponses[0]
	s.Equal("call_1", partial.ID)
	s.Equal("get_weather", partial.Name)
	s.Equal(map[string]any{"status": "searching", "instructions": "Tell them where it is."}, partial.Response)
	s.Require().NotNil(partial.WillContinue)
	s.True(*partial.WillContinue, "the call is still open")
	s.Equal("WHEN_IDLE", partial.Scheduling)

	s.Require().NoError(provider.Answer("call_1", `{"forecast":"sunny"}`, nil))
	result := s.nextFrame(conn).ToolResponse.FunctionResponses[0]
	s.Equal(`{"forecast":"sunny"}`, result.Response["output"])
	s.Require().NotNil(result.WillContinue)
	s.False(*result.WillContinue, "the result closes the call")
	s.Equal("WHEN_IDLE", result.Scheduling)
}

func (s *GeminiSocketSuite) TestABlockingSessionIsNotToldOfAnOpenCall() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{Tools: []llm.Tool{{Name: "get_weather"}}})
	defer func() { _ = provider.Close() }()
	s.Empty((<-fake.setups).Tools[0].FunctionDeclarations[0].Behavior)
	s.serve(conn, serverMessage{ToolCall: &toolCall{FunctionCalls: []functionCall{{ID: "call_1", Name: "get_weather"}}}})
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.ToolCall); return ok })

	s.False(provider.UpdatesCalls())
	s.ErrorIs(provider.UpdateCall(sts.CallUpdate{CallID: "call_1", Status: "searching"}), sts.ErrNoCallUpdates)
	s.Require().NoError(provider.Answer("call_1", "{}", nil))
	result := s.nextFrame(conn).ToolResponse.FunctionResponses[0]
	s.Nil(result.WillContinue, "a blocking call's answer is as it always was")
	s.Empty(result.Scheduling)
}

// invalidKeyText is how the Live API refused a made-up key on 2026-10-28: a close 1007 with
// this reason, 50ms after the setup frame.
const invalidKeyText = "API key not valid. Please pass a valid API key."

// startRefused starts a provider on a key of its own against a Live API that reads the setup
// frame and closes with code and reason, and returns how many invalid keys the pool counted
// meanwhile and Start's error.
func (s *GeminiSocketSuite) startRefused(code int, reason string) (float64, error) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(time.Second))
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()

	before := keypooltest.Series(s.T(), "router_keypool_refused_attempts_total", apiKeyEnvVar)["/customer/sts/invalid_key"]
	provider, err := New(Options{
		URL:              "ws://" + strings.TrimPrefix(server.URL, "http://"),
		APIKey:           "AIza-" + rand.Text(),
		HandshakeTimeout: 5 * time.Second,
	})
	s.Require().NoError(err)
	ctx, cancel := context.WithTimeout(keypool.WithCaller(context.Background(), "sts", "gemini-live"), 5*time.Second)
	defer cancel()
	err = provider.Start(ctx)
	return keypooltest.Series(s.T(), "router_keypool_refused_attempts_total", apiKeyEnvVar)["/customer/sts/invalid_key"] - before, err
}

func (s *GeminiSocketSuite) TestAKeyRefusedByACloseAfterSetupIsCountedAsInvalid() {
	counted, err := s.startRefused(websocket.CloseInvalidFramePayloadData, invalidKeyText)

	s.Require().Error(err)
	s.Contains(err.Error(), invalidKeyText)
	s.Equal(1.0, counted)
}

func (s *GeminiSocketSuite) TestAnotherReasonOnTheSameCloseCodeIsNotAnInvalidKey() {
	counted, err := s.startRefused(websocket.CloseInvalidFramePayloadData, "Request contains an invalid argument.")

	s.Require().Error(err)
	s.Zero(counted)
}

func (s *GeminiSocketSuite) TestTheKeyTextOnAnotherCloseCodeIsNotAnInvalidKey() {
	counted, err := s.startRefused(websocket.ClosePolicyViolation, invalidKeyText)

	s.Require().Error(err)
	s.Zero(counted)
}

// A key refused when the session resumes is counted for the caller the session was started for.
func (s *GeminiSocketSuite) TestAKeyRefusedOnResumeIsCountedForTheSessionsCaller() {
	upgrader := websocket.Upgrader{}
	first := make(chan *websocket.Conn, 1)
	var served atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		if served.Add(1) == 1 {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"setupComplete":{}}`))
			first <- conn
			return
		}
		defer conn.Close()
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(1007, invalidKeyText), time.Now().Add(time.Second))
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	counted := func() float64 {
		return keypooltest.Series(s.T(), "router_keypool_refused_attempts_total", apiKeyEnvVar)["/customer/sts/invalid_key"]
	}
	before := counted()
	provider, err := New(Options{URL: "ws://" + strings.TrimPrefix(server.URL, "http://"), APIKey: "AIza-" + rand.Text(), HandshakeTimeout: 2 * time.Second})
	s.Require().NoError(err)
	defer func() { _ = provider.Close() }()
	ctx, cancel := context.WithTimeout(keypool.WithCaller(context.Background(), "sts", "gemini-live"), 30*time.Second)
	defer cancel()
	s.Require().NoError(provider.Start(ctx))
	conn := <-first
	s.serve(conn, serverMessage{SessionResumptionUpdate: &resumptionUpdate{NewHandle: "h-1", Resumable: true}})
	s.Require().NoError(conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "session timeout"), time.Now().Add(time.Second)))
	conn.Close()

	s.Eventually(func() bool { return counted() > before }, 10*time.Second, 20*time.Millisecond)
}

func (s *GeminiSocketSuite) TestInterruptBeforeConnectionDropDoesNotMuteNextReplyAfterResume() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{OutputTranscript: true})
	defer func() { _ = provider.Close() }()
	<-fake.setups

	s.serve(conn, serverMessage{SessionResumptionUpdate: &resumptionUpdate{NewHandle: "h-1", Resumable: true}})
	s.serve(conn, spoke(240))
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.AudioChunk); return ok })

	// Cut off the reply locally, then have the server drop the socket before TurnComplete.
	s.Require().NoError(provider.Interrupt(0))
	s.Require().NoError(conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "session timeout"), time.Now().Add(time.Second)))
	conn.Close()

	settled := s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.ResponseComplete); return ok }).(sts.ResponseComplete)
	s.True(settled.Interrupted)
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.Disconnected); return ok })
	resumed := s.nextConn(fake)
	<-fake.setups
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.Connected); return ok })

	// The next reply on the resumed connection must not be swallowed.
	s.serve(resumed, serverMessage{ServerContent: &serverContent{OutputTranscription: &transcription{Text: "I am back."}}})
	s.serve(resumed, spoke(240))
	s.serve(resumed, turnComplete())

	var transcripts []sts.OutputTranscript
	var chunks []sts.AudioChunk
	var complete sts.ResponseComplete
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event := <-provider.Events():
			switch typed := event.(type) {
			case sts.OutputTranscript:
				transcripts = append(transcripts, typed)
			case sts.AudioChunk:
				chunks = append(chunks, typed)
			case sts.ResponseComplete:
				complete = typed
				goto done
			}
		case <-deadline:
			s.FailNow("timed out waiting for resumed reply to complete")
		}
	}
done:
	s.Require().Len(transcripts, 1, "output transcript on resumed connection must not be dropped")
	s.Equal("I am back.", transcripts[0].Text)
	s.Require().Len(chunks, 1, "audio on resumed connection must not be dropped")
	s.Equal(2, chunks[0].Generation)
	s.False(complete.Interrupted, "resumed reply was not interrupted")
}

func (s *GeminiSocketSuite) TestExhaustedResumeEmitsOnlyOneTerminalEvent() {
	upgrader := websocket.Upgrader{}
	first := make(chan *websocket.Conn, 1)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) > 1 {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"setupComplete":{}}`))
		first <- conn
	}))
	defer server.Close()

	provider, err := New(Options{
		URL:              "ws://" + strings.TrimPrefix(server.URL, "http://"),
		APIKey:           "test-key",
		HandshakeTimeout: 2 * time.Second,
	})
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.Require().NoError(provider.Start(ctx))
	conn := <-first
	s.serve(conn, serverMessage{SessionResumptionUpdate: &resumptionUpdate{NewHandle: "h-1", Resumable: true}})
	s.Require().NoError(conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseGoingAway, "going away"), time.Now().Add(time.Second)))
	conn.Close()

	firstErr := s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.Error); return ok }).(sts.Error)
	s.Equal("resume", firstErr.Context)
	s.True(firstErr.Fatal)

	s.Require().NoError(provider.Close())

	var extraTerminal []sts.Event
	for event := range provider.Events() {
		switch event.(type) {
		case sts.Error, sts.Disconnected:
			extraTerminal = append(extraTerminal, event)
		}
	}
	s.Empty(extraTerminal, "exhausted resume must emit exactly one terminal event")
	s.Equal(int32(1+resumeAttempts), attempts.Load())
}

func (s *GeminiSocketSuite) TestCloseCancelsResumeImmediatelyAndWaitsForReadLoop() {
	upgrader := websocket.Upgrader{}
	first := make(chan *websocket.Conn, 1)
	dialStarted := make(chan struct{}, 1)
	dialAborted := make(chan struct{}, 1)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"setupComplete":{}}`))
			first <- conn
			return
		}
		select {
		case dialStarted <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		select {
		case dialAborted <- struct{}{}:
		default:
		}
	}))
	defer server.Close()

	provider, err := New(Options{
		URL:              "ws://" + strings.TrimPrefix(server.URL, "http://"),
		APIKey:           "test-key",
		HandshakeTimeout: 10 * time.Second,
	})
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.Require().NoError(provider.Start(ctx))
	conn := <-first
	s.serve(conn, serverMessage{SessionResumptionUpdate: &resumptionUpdate{NewHandle: "h-1", Resumable: true}})
	s.Require().NoError(conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "drop"), time.Now().Add(time.Second)))
	conn.Close()

	select {
	case <-dialStarted:
	case <-time.After(5 * time.Second):
		s.FailNow("resume never started dialing")
	}

	start := time.Now()
	s.Require().NoError(provider.Close())
	s.Less(time.Since(start), time.Second, "Close must cancel resume dial instead of waiting for HandshakeTimeout")

	select {
	case <-provider.loopDone:
	default:
		s.FailNow("Close returned before readLoop exited")
	}
	select {
	case <-dialAborted:
	case <-time.After(2 * time.Second):
		s.FailNow("in-flight resume dial was not cancelled by Close")
	}
	s.Equal(int32(2), attempts.Load(), "Close must not allow further resume attempts")
}

func (s *GeminiSocketSuite) TestCloseCancelsResumeDuringBackoff() {
	upgrader := websocket.Upgrader{}
	first := make(chan *websocket.Conn, 1)
	attempt2 := make(chan struct{}, 1)
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n == 1 {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"setupComplete":{}}`))
			first <- conn
			return
		}
		if n == 2 {
			select {
			case attempt2 <- struct{}{}:
			default:
			}
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	provider, err := New(Options{
		URL:              "ws://" + strings.TrimPrefix(server.URL, "http://"),
		APIKey:           "test-key",
		HandshakeTimeout: 5 * time.Second,
	})
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.Require().NoError(provider.Start(ctx))
	conn := <-first
	s.serve(conn, serverMessage{SessionResumptionUpdate: &resumptionUpdate{NewHandle: "h-1", Resumable: true}})
	s.Require().NoError(conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "drop"), time.Now().Add(time.Second)))
	conn.Close()

	select {
	case <-attempt2:
	case <-time.After(5 * time.Second):
		s.FailNow("first resume attempt never happened")
	}
	// Let the client receive the 503 and enter the 500ms backoff select before Close.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	s.Require().NoError(provider.Close())
	s.Less(time.Since(start), 200*time.Millisecond, "Close must cancel resume backoff sleep immediately")
	select {
	case <-provider.loopDone:
	default:
		s.FailNow("Close returned before readLoop exited")
	}
	s.Never(func() bool { return attempts.Load() > 2 }, 700*time.Millisecond, 20*time.Millisecond, "no further resume attempt after Close")
}

func (s *GeminiSocketSuite) TestFailedStartCanBeRetried() {
	upgrader := websocket.Upgrader{}
	var attempts atomic.Int32
	done := make(chan struct{})
	defer close(done)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"setupComplete":{}}`))
		<-done
	}))
	defer server.Close()

	provider, err := New(Options{
		URL:              "ws://" + strings.TrimPrefix(server.URL, "http://"),
		APIKey:           "test-key",
		HandshakeTimeout: 5 * time.Second,
	})
	s.Require().NoError(err)
	defer func() { _ = provider.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Require().Error(provider.Start(ctx))
	s.Require().NoError(provider.Start(ctx), "a failed Start must roll back to unstarted so it can be retried")
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.Connected); return ok })
}

func (s *GeminiSocketSuite) TestCloseWhileConnectingAbortsStartAndStaysClosed() {
	dialStarted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case dialStarted <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	provider, err := New(Options{
		URL:              "ws://" + strings.TrimPrefix(server.URL, "http://"),
		APIKey:           "test-key",
		HandshakeTimeout: 10 * time.Second,
	})
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	startErr := make(chan error, 1)
	go func() { startErr <- provider.Start(ctx) }()

	select {
	case <-dialStarted:
	case <-time.After(5 * time.Second):
		s.FailNow("Start never reached server")
	}

	s.Require().NoError(provider.Close())
	select {
	case err := <-startErr:
		s.Require().Error(err)
	case <-time.After(time.Second):
		s.FailNow("Start did not abort when Close was called")
	}

	err = provider.Start(ctx)
	s.Require().Error(err)
	s.ErrorContains(err, "closed")
}

func (s *GeminiSocketSuite) TestSecondStartWhileConnectedIsRefused() {
	fake := newFakeLive()
	defer fake.close()
	provider, _ := s.connect(fake, Options{})
	defer func() { _ = provider.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := provider.Start(ctx)
	s.Require().Error(err)
	s.ErrorContains(err, "already started")
}

func (s *GeminiSocketSuite) TestSendFailsBeforeStartAndAfterClose() {
	provider, err := New(Options{APIKey: "test-key"})
	s.Require().NoError(err)

	err = provider.SendText("hello", sts.Participant{ID: "alice"})
	s.Require().Error(err)
	s.ErrorContains(err, "not started")

	s.Require().NoError(provider.Close())
	err = provider.SendText("hello", sts.Participant{ID: "alice"})
	s.Require().Error(err)
	s.ErrorContains(err, "session closed")
}

func (s *GeminiSocketSuite) TestConcurrentCallsDuringResumeAndCloseAreRaceFree() {
	fake := newFakeLive()
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	<-fake.setups

	s.serve(conn, serverMessage{SessionResumptionUpdate: &resumptionUpdate{NewHandle: "h-1", Resumable: true}})
	s.serve(conn, spoke(240))
	s.nextEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.AudioChunk); return ok })

	pcm := sts.PcmData{Samples: make([]int16, 160), SampleRate: sts.InputSampleRate, Channels: 1}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				_ = provider.ProcessAudio(pcm, sts.Participant{ID: "alice"})
				_ = provider.Interrupt(0)
				_ = provider.SendText("ping", sts.Participant{ID: "alice"})
			}
		}()
	}

	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "drop"), time.Now().Add(time.Second))
	conn.Close()

	// Concurrent Close calls from multiple goroutines.
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = provider.Close()
		}()
	}
	wg.Wait()

	for range provider.Events() {
	}
}
