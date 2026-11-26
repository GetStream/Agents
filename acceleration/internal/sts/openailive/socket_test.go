package openailive

import (
	"context"
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
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sts"
)

// OpenAILiveSocketSuite exercises the provider against a Live API that is answering, which
// is where session.start, the audio encoding and the backend round trips live.
type OpenAILiveSocketSuite struct {
	suite.Suite
}

func TestOpenAILiveSocketSuite(t *testing.T) {
	suite.Run(t, new(OpenAILiveSocketSuite))
}

// fakeLive is a Live API that accepts one session. It answers session.start itself, or
// refuses it when told to, and keeps what it was sent and how it was authenticated.
type fakeLive struct {
	server *httptest.Server
	conns  chan *websocket.Conn
	starts chan map[string]any
	auth   chan string
	refuse string
	done   chan struct{}
}

func newFakeLive(refuse string) *fakeLive {
	fake := &fakeLive{
		conns:  make(chan *websocket.Conn, 1),
		starts: make(chan map[string]any, 1),
		auth:   make(chan string, 1),
		refuse: refuse,
		done:   make(chan struct{}),
	}

	upgrader := websocket.Upgrader{}
	fake.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.auth <- r.Header.Get("Authorization")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var start map[string]any
		if err := json.Unmarshal(raw, &start); err != nil {
			return
		}
		fake.starts <- start

		reply := `{"type":"session.started","session":{"id":"sess_123","model":"gpt-live-1"}}`
		if fake.refuse != "" {
			reply = fake.refuse
		}
		if err := conn.WriteMessage(websocket.TextMessage, []byte(reply)); err != nil {
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
func (s *OpenAILiveSocketSuite) connect(fake *fakeLive, settings Options) (*STS, *websocket.Conn) {
	settings.URL = fake.endpoint()
	settings.APIKey = "test-key"
	settings.HandshakeTimeout = 5 * time.Second
	provider, err := New(settings)
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	s.T().Cleanup(cancel)
	s.Require().NoError(provider.Start(ctx))
	select {
	case conn := <-fake.conns:
		return provider, conn
	case <-time.After(5 * time.Second):
		s.FailNow("the provider never connected")
		return nil, nil
	}
}

// nextFrame reads the next message the provider sent upstream.
func (s *OpenAILiveSocketSuite) nextFrame(conn *websocket.Conn) map[string]any {
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(5 * time.Second)))
	_, raw, err := conn.ReadMessage()
	s.Require().NoError(err)

	var frame map[string]any
	s.Require().NoError(json.Unmarshal(raw, &frame))
	return frame
}

func (s *OpenAILiveSocketSuite) TestSessionStartNamesTheModelTheAudioAndTheBackend() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{
		Voice:        "quartz",
		Instructions: "Be brief.",
		Tools:        []llm.Tool{{Name: "get_weather", Description: "Weather.", Parameters: map[string]any{"type": "object"}}},
		Backend:      "gpt-5.6-luna",
	})
	defer s.hangUp(provider, conn)

	s.Equal("Bearer test-key", <-fake.auth)
	start := <-fake.starts
	s.Equal("session.start", start["type"])
	session := start["session"].(map[string]any)
	s.Equal("gpt-live-1", session["model"], "the model goes in the frame, not on the URL")
	s.Equal("Be brief.", session["instructions"])

	audioConfig := session["audio"].(map[string]any)
	s.Equal(map[string]any{"type": "audio/pcm", "rate": float64(24000)}, audioConfig["format"])
	s.Equal(map[string]any{"voice": "quartz"}, audioConfig["output"])

	delegated := session["delegation"].(map[string]any)
	s.Equal("responses", delegated["type"], "tools only reach the model through a Responses backend")
	responses := delegated["responses"].(map[string]any)
	s.Equal("gpt-5.6-luna", responses["model"])
	s.Equal("Be brief.", responses["instructions"], "the backend is the one that chooses the tools")
	s.Equal("auto", responses["tool_choice"])
	s.Equal(false, responses["parallel_tool_calls"])
	tools := responses["tools"].([]any)
	s.Require().Len(tools, 1)
	s.Equal(map[string]any{
		"type":        "function",
		"name":        "get_weather",
		"description": "Weather.",
		"parameters":  map[string]any{"type": "object"},
	}, tools[0], "the Responses shape is flat")
}

// sendMessage is Slack's MCP slack_send_message input schema, as on the text path (AI-969):
// two required properties and four optional ones.
var sendMessage = llm.Tool{
	Name:        "slack__slack_send_message",
	Description: "Sends a message to a Slack channel or user.",
	Parameters: map[string]any{
		"type": "object",
		"properties": map[string]any{
			"channel_id":       map[string]any{"type": "string"},
			"message":          map[string]any{"type": "string"},
			"thread_ts":        map[string]any{"type": "string"},
			"draft_id":         map[string]any{"type": "string"},
			"reply_broadcast":  map[string]any{"type": "boolean"},
			"unfurl_app_links": map[string]any{"type": "boolean"},
		},
		"required": []any{"channel_id", "message"},
	},
}

// backendTools are the tools a session.start or session.update frame gave the backend.
func backendTools(frame map[string]any) []any {
	session := frame["session"].(map[string]any)
	return session["delegation"].(map[string]any)["responses"].(map[string]any)["tools"].([]any)
}

// TestABackendToolsOptionalArgumentsStayOptional is AI-993: the backend is a Responses model,
// which turns a tool with strict left out into strict mode, so the model filled thread_ts with
// "" and reply_broadcast with false.
func (s *OpenAILiveSocketSuite) TestABackendToolsOptionalArgumentsStayOptional() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{Tools: []llm.Tool{sendMessage}})
	defer s.hangUp(provider, conn)

	started := backendTools(<-fake.starts)[0].(map[string]any)
	s.Equal(false, started["strict"], "an omitted strict is strict mode on the Responses API")
	s.Equal([]any{"channel_id", "message"}, started["parameters"].(map[string]any)["required"])

	s.Require().NoError(provider.SetTools([]llm.Tool{sendMessage}))
	updated := backendTools(s.nextFrame(conn))[0].(map[string]any)
	s.Equal(false, updated["strict"], "tools set mid-session follow the same rule")
}

// TestABackendToolWithEveryPropertyRequiredIsSentAsBefore leaves strict out, as on base:
// OpenAI's strict normalization takes nothing from a schema with no optional property.
func (s *OpenAILiveSocketSuite) TestABackendToolWithEveryPropertyRequiredIsSentAsBefore() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{Tools: []llm.Tool{{Name: "get_weather", Parameters: map[string]any{
		"type":       "object",
		"properties": map[string]any{"city": map[string]any{"type": "string"}},
		"required":   []string{"city"},
	}}}})
	defer s.hangUp(provider, conn)

	s.NotContains(backendTools(<-fake.starts)[0].(map[string]any), "strict")
}

// TestABackendToolDecodedFromJSONWithEveryPropertyRequiredIsSentAsBefore: required as []any, as
// a decoded MCP schema has it, counts as much as []string.
func (s *OpenAILiveSocketSuite) TestABackendToolDecodedFromJSONWithEveryPropertyRequiredIsSentAsBefore() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{Tools: []llm.Tool{{Name: "get_weather", Parameters: map[string]any{
		"type":       "object",
		"properties": map[string]any{"city": map[string]any{"type": "string"}},
		"required":   []any{"city"},
	}}}})
	defer s.hangUp(provider, conn)

	s.NotContains(backendTools(<-fake.starts)[0].(map[string]any), "strict")
}

func (s *OpenAILiveSocketSuite) TestASessionAskingForNothingExtraSendsNothingExtra() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer s.hangUp(provider, conn)

	session := (<-fake.starts)["session"].(map[string]any)
	s.NotContains(session, "instructions")
	s.NotContains(session["audio"].(map[string]any), "output", "no voice, so the model's own")
	responses := session["delegation"].(map[string]any)["responses"].(map[string]any)
	s.Equal(map[string]any{"model": DefaultBackend}, responses, "a backend with no tools is told nothing about them")
}

func (s *OpenAILiveSocketSuite) TestARefusedSessionFailsStart() {
	fake := newFakeLive(`{"type":"error","error":{"type":"invalid_request_error","code":"unknown_parameter","message":"Unknown parameter: 'session.turn_detection'."}}`)
	defer fake.close()
	provider, err := New(Options{APIKey: "test-key", URL: fake.endpoint(), HandshakeTimeout: 5 * time.Second})
	s.Require().NoError(err)

	err = provider.Start(context.Background())
	s.ErrorContains(err, "session rejected")
	s.ErrorContains(err, "unknown_parameter")
}

func (s *OpenAILiveSocketSuite) TestTheCallersAudioGoesUpAtTheSessionRate() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer s.hangUp(provider, conn)

	pcm := sts.PcmData{Samples: make([]int16, 1600), SampleRate: sts.InputSampleRate, Channels: 1}
	s.Require().NoError(provider.ProcessAudio(pcm, sts.Participant{ID: "alice"}))

	frame := s.nextFrame(conn)
	s.Equal("session.input_audio.append", frame["type"])
	decoded, err := base64.StdEncoding.DecodeString(frame["audio"].(string))
	s.Require().NoError(err)
	s.Len(decoded, len(audio.Resample(pcm, SampleRate, 1).Bytes()), "100ms at 16 kHz goes up as 100ms at 24 kHz")
}

func (s *OpenAILiveSocketSuite) TestCallerAudioPassesThroughDuringAnOpenReply() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{ReplyGap: 5 * time.Second})
	defer func() {
		_ = conn.Close()
		_ = provider.Close()
	}()

	var transcript string
	nextAudio := func() sts.AudioChunk {
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case event, open := <-provider.Events():
				if !open {
					s.FailNow("provider closed its event stream")
				}
				switch typed := event.(type) {
				case sts.AudioChunk:
					return typed
				case sts.InputTranscript:
					transcript += typed.Text
				case sts.Error:
					s.FailNowf("provider rejected response audio", "context=%q", typed.Context)
				case sts.ResponseComplete:
					s.FailNow("response completed before the next output chunk")
				}
			case <-timer.C:
				s.FailNow("timed out waiting for provider audio")
			}
		}
	}

	firstAudio := sts.PcmData{Samples: []int16{400, -401}, SampleRate: SampleRate, Channels: 1}
	s.Require().NoError(conn.WriteJSON(serverEvent{Type: eventAudioDelta, Delta: base64.StdEncoding.EncodeToString(firstAudio.Bytes())}))
	first := nextAudio()
	s.Equal(firstAudio.Samples, first.Audio.Samples)

	caller := sts.PcmData{Samples: make([]int16, 1600), SampleRate: sts.InputSampleRate, Channels: 1}
	for i := range caller.Samples {
		caller.Samples[i] = int16(700 - i%1300)
	}
	processDone := make(chan error, 1)
	go func() { processDone <- provider.ProcessAudio(caller, sts.Participant{ID: "caller"}) }()
	input := s.nextFrame(conn)
	select {
	case err := <-processDone:
		s.Require().NoError(err)
	case <-time.After(5 * time.Second):
		s.FailNow("caller audio processing did not finish after forwarding the chunk")
	}
	s.Require().Equal(eventAudioAppend, input["type"])
	decoded, err := base64.StdEncoding.DecodeString(input["audio"].(string))
	s.Require().NoError(err)
	s.Equal(audio.Resample(caller, SampleRate, 1).Bytes(), decoded, "the complete caller chunk must reach the provider while its reply is open")

	const callerTranscript = "I have a new question for you."
	s.Require().NoError(conn.WriteJSON(serverEvent{Type: eventInputTranscript, Delta: callerTranscript}))
	secondAudio := sts.PcmData{Samples: []int16{-900, 901}, SampleRate: SampleRate, Channels: 1}
	s.Require().NoError(conn.WriteJSON(serverEvent{Type: eventAudioDelta, Delta: base64.StdEncoding.EncodeToString(secondAudio.Bytes())}))
	second := nextAudio()

	s.Equal(callerTranscript, transcript)
	s.Equal(first.ResponseID, second.ResponseID, "caller audio must not roll the live response over")
	s.Equal(first.Generation, second.Generation, "caller audio must not start a new generation")
	s.Equal(first.Index+1, second.Index, "the next output chunk keeps its place in the original response")
	s.Equal(secondAudio.Samples, second.Audio.Samples, "the distinctive second chunk must arrive intact")
}

func (s *OpenAILiveSocketSuite) TestAToolAnswerContinuesTheBackend() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer s.hangUp(provider, conn)

	s.Require().NoError(provider.Answer("call_1", `{"forecast":"sunny"}`, nil))
	result := s.nextFrame(conn)
	s.Equal("response.item.create", result["type"])
	s.Equal(map[string]any{"type": "function_call_output", "call_id": "call_1", "output": `{"forecast":"sunny"}`}, result["item"])
	s.Equal(map[string]any{"type": "response.create"}, s.nextFrame(conn), "a result alone does not carry the backend on")
}

func (s *OpenAILiveSocketSuite) TestATypedTurnGoesToTheBackendAndAPromptToTheLiveModel() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer s.hangUp(provider, conn)

	s.Require().NoError(provider.SendText("My order number is A0042.", sts.Participant{ID: "alice"}))
	typed := s.nextFrame(conn)
	s.Equal("response.item.create", typed["type"])
	s.Equal(map[string]any{
		"type":    "message",
		"role":    "user",
		"content": []any{map[string]any{"type": "input_text", "text": "My order number is A0042."}},
	}, typed["item"])
	s.Equal("response.create", s.nextFrame(conn)["type"])

	s.Require().NoError(provider.Prompt("Greet the caller."))
	prompt := s.spokenTo(conn)
	s.Equal("session.instructions.append", prompt["type"])
	s.Contains(prompt, "delegation_id", "required even when it names no delegation")
	s.Nil(prompt["delegation_id"])
	s.True(strings.HasPrefix(prompt["content"].(string), "Greet the caller."))

	// An instruction only steers the speech a turn would have produced. The commentary is
	// what starts the model talking when no turn is under way.
	commentary := s.spokenTo(conn)
	s.Equal("session.commentary.append", commentary["type"])
	s.Nil(commentary["delegation_id"])
	s.Equal("Greet the caller.", commentary["content"])
}

// A Live session injects an append at a point on a clock that only input audio advances, so
// nothing appended is spoken until silence carries the session there.
func (s *OpenAILiveSocketSuite) TestSilenceCarriesTheSessionClockUntilTheModelSpeaks() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer s.hangUp(provider, conn)

	s.Require().NoError(provider.Prompt("Greet the caller."))
	s.Equal("session.instructions.append", s.spokenTo(conn)["type"])
	s.Equal("session.commentary.append", s.spokenTo(conn)["type"])

	silence := s.nextFrame(conn)
	s.Require().Equal("session.input_audio.append", silence["type"], "silence should follow a prompt")
	decoded, err := base64.StdEncoding.DecodeString(silence["audio"].(string))
	s.Require().NoError(err)
	s.Len(decoded, SampleRate/50*2, "20ms of silence at the session rate")
	for _, sample := range decoded {
		s.Zero(sample, "the filler has to be silent")
	}

	// The silence the model streams between replies is not it talking.
	var hush serverEvent
	s.Require().NoError(json.Unmarshal([]byte(quiet(240)), &hush))
	provider.handleMessage(hush)
	provider.mu.Lock()
	carrying := provider.carrying
	provider.mu.Unlock()
	s.True(carrying, "the filler goes on while the model is silent")

	// Once the model is talking, its own audio is the clock and the filler stands down.
	var reply serverEvent
	s.Require().NoError(json.Unmarshal([]byte(spoke(240)), &reply))
	provider.handleMessage(reply)
	s.Eventually(func() bool {
		provider.mu.Lock()
		defer provider.mu.Unlock()
		return !provider.carrying
	}, time.Second, 10*time.Millisecond, "the filler should stop when the model speaks")
}

// spokenTo is the next frame that is not the silence filling the gap, which may arrive at any
// point once a prompt has started the clock.
func (s *OpenAILiveSocketSuite) spokenTo(conn *websocket.Conn) map[string]any {
	for {
		frame := s.nextFrame(conn)
		if frame["type"] != "session.input_audio.append" {
			return frame
		}
	}
}

func (s *OpenAILiveSocketSuite) TestNewToolsReplaceTheWholeDelegation() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{Instructions: "Be brief."})
	defer s.hangUp(provider, conn)

	s.Require().NoError(provider.SetTools([]llm.Tool{{Name: "get_time"}}))
	update := s.nextFrame(conn)
	s.Equal("session.update", update["type"])
	session := update["session"].(map[string]any)
	s.Equal([]string{"delegation"}, keys(session), "only the delegation can change after startup")
	responses := session["delegation"].(map[string]any)["responses"].(map[string]any)
	s.Equal(DefaultBackend, responses["model"], "the delegation is replaced whole, so the model goes again")
	s.Equal("Be brief.", responses["instructions"])
	s.Equal("get_time", responses["tools"].([]any)[0].(map[string]any)["name"])
}

// TestCloseWaitsForSessionClosed: session.closed carries the session's final billed seconds
// and comes after session.close, so Close reads it before dropping the socket.
func (s *OpenAILiveSocketSuite) TestCloseWaitsForSessionClosed() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{})

	closed := make(chan error, 1)
	go func() { closed <- provider.Close() }()
	s.Equal(map[string]any{"type": "session.close"}, s.nextFrame(conn))
	time.Sleep(500 * time.Millisecond)
	s.Require().NoError(conn.WriteMessage(websocket.TextMessage,
		[]byte(`{"type":"session.closed","reason":"close_requested","usage":{"seconds":29}}`)))
	select {
	case err := <-closed:
		s.Require().NoError(err)
	case <-time.After(closeGrace):
		s.FailNow("Close kept waiting after session.closed arrived")
	}

	var disconnected []sts.Disconnected
	for event := range provider.Events() {
		if typed, ok := event.(sts.Disconnected); ok {
			disconnected = append(disconnected, typed)
		}
	}
	s.Require().Len(disconnected, 1, "session.closed is read before the socket is dropped")
	s.Equal("close_requested", disconnected[0].Reason)
	s.True(disconnected[0].Clean)
}

// TestTheSessionsSecondsAndItsBackendAreReported uses what a recorded 30-second session on
// a gpt-5.6-luna backend was sent: a backend response of 890 tokens in and 31 out, 10 of them
// thinking, the session's seconds at 13 and 28, and 29 as it closed. The seconds are sent as
// numbers with a fraction, as the Live API sent "seconds":2.0 when probed on 2026-10-23.
func (s *OpenAILiveSocketSuite) TestTheSessionsSecondsAndItsBackendAreReported() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{Backend: "gpt-5.6-luna"})

	for _, frame := range []string{
		`{"type":"session.usage.updated","usage":{"seconds":13.0},"context_window":{"usage_ratio":0.0096}}`,
		`{"type":"response.event","event":{"type":"response.completed","response":{"id":"resp_1",` +
			`"model":"gpt-5.6-luna-2026-09-30","status":"completed","service_tier":"default","usage":{"input_tokens":890,` +
			`"input_tokens_details":{"cache_write_tokens":0,"cached_tokens":0},"output_tokens":31,` +
			`"output_tokens_details":{"reasoning_tokens":10},"total_tokens":921}}}}`,
		`{"type":"session.usage.updated","usage":{"seconds":27.6}}`,
	} {
		s.Require().NoError(conn.WriteMessage(websocket.TextMessage, []byte(frame)))
	}
	closed := make(chan error, 1)
	go func() { closed <- provider.Close() }()
	s.Equal(map[string]any{"type": "session.close"}, s.nextFrame(conn))
	s.Require().NoError(conn.WriteMessage(websocket.TextMessage,
		[]byte(`{"type":"session.closed","reason":"close_requested","usage":{"seconds":29.0}}`)))
	s.Require().NoError(<-closed)

	var seconds []int64
	var final []bool
	var backend []sts.BackendUsage
	var disconnected int
	for event := range provider.Events() {
		switch typed := event.(type) {
		case sts.SessionUsage:
			seconds = append(seconds, typed.Seconds)
			final = append(final, typed.Final)
		case sts.BackendUsage:
			backend = append(backend, typed)
		case sts.Disconnected:
			disconnected++
		}
	}
	s.Equal([]int64{13, 28, 29}, seconds, "each running total as the vendor counts it, rounded, the last as it closed")
	s.Equal([]bool{false, false, true}, final, "only session.closed's report is final")
	s.Equal(1, disconnected, "session.closed is read, seconds and all")
	s.Equal([]sts.BackendUsage{{
		ResponseID:  "resp_1",
		Model:       "gpt-5.6-luna",
		ServiceTier: "default",
		Usage: llm.Usage{
			InputTokens:         890,
			OutputTokens:        31,
			OutputTokensDetails: llm.OutputTokensDetails{ReasoningTokens: 10},
			TotalTokens:         921,
		},
	}}, backend, "reported as the model the session delegated to")
	s.True(CapabilitiesFor(DefaultModel).SessionBilled)
}

// TestCloseGivesUpAfterTwoSeconds: a server that never answers session.close holds Close
// for closeGrace and no longer.
func (s *OpenAILiveSocketSuite) TestCloseGivesUpAfterTwoSeconds() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{})

	began := time.Now()
	s.Require().NoError(provider.Close())
	waited := time.Since(began)

	s.Equal(map[string]any{"type": "session.close"}, s.nextFrame(conn))
	s.GreaterOrEqual(waited, closeGrace)
	s.Less(waited, closeGrace+time.Second)
}

// hangUp drops the server's side before closing the provider. A test that is not about
// closing has no server answering session.close, and would otherwise wait out closeGrace.
func (s *OpenAILiveSocketSuite) hangUp(provider *STS, conn *websocket.Conn) {
	_ = conn.Close()
	_ = provider.Close()
}

func keys(object map[string]any) []string {
	var found []string
	for key := range object {
		found = append(found, key)
	}
	return found
}

func (s *OpenAILiveSocketSuite) TestAToolAnsweredWhileTheModelSpeaksCarriesOnOnceItStops() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{ReplyGap: time.Second})
	defer func() { _ = provider.Close() }()
	speech := sts.PcmData{Samples: []int16{400, -401}, SampleRate: SampleRate, Channels: 1}
	s.Require().NoError(conn.WriteJSON(serverEvent{Type: eventAudioDelta, Delta: base64.StdEncoding.EncodeToString(speech.Bytes())}))
	s.awaitEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.AudioChunk); return ok })

	s.Require().NoError(provider.Answer("call_1", `{"question":"Shall I book it?"}`, nil))

	s.Equal("response.item.create", s.nextFrame(conn)["type"])
	s.Equal(map[string]any{"type": "response.create"}, s.nextFrame(conn))
	s.awaitEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.ResponseComplete); return ok },
		"the answer asked for a reply while the model's was still open, which the server ignores")
}

func (s *OpenAILiveSocketSuite) TestAToolReplyTheModelTalkedOverIsAskedForOnceMore() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{ReplyGap: 100 * time.Millisecond})
	defer func() { _ = provider.Close() }()

	s.Require().NoError(provider.Answer("call_1", `{"question":"Shall I book it?"}`, nil))
	s.Equal("response.item.create", s.spokenTo(conn)["type"])
	s.Equal(map[string]any{"type": "response.create"}, s.spokenTo(conn))

	speech := sts.PcmData{Samples: []int16{400, -401}, SampleRate: SampleRate, Channels: 1}
	for range 2 {
		s.Require().NoError(conn.WriteJSON(serverEvent{Type: eventAudioDelta, Delta: base64.StdEncoding.EncodeToString(speech.Bytes())}))
		s.awaitEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.ResponseComplete); return ok })
	}
	s.Equal(map[string]any{"type": "response.create"}, s.spokenTo(conn),
		"the model began speaking just after the reply was asked for, so the server ignored it")
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)))
	for {
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			break
		}
		s.NotEqual("response.create", frame["type"], "a reply is asked for again only once")
	}
}

func (s *OpenAILiveSocketSuite) TestAToolReplyTheBackendBeganIsNotAskedForAgain() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{ReplyGap: 100 * time.Millisecond})
	defer func() { _ = provider.Close() }()

	s.Require().NoError(provider.Answer("call_1", `{"question":"Shall I book it?"}`, nil))
	s.Equal("response.item.create", s.spokenTo(conn)["type"])
	s.Equal(map[string]any{"type": "response.create"}, s.spokenTo(conn))
	s.Require().NoError(conn.WriteMessage(websocket.TextMessage,
		[]byte(`{"type":"response.event","delegation_id":"d1","event":{"type":"response.created","response":{"id":"r1"}}}`)))

	speech := sts.PcmData{Samples: []int16{400, -401}, SampleRate: SampleRate, Channels: 1}
	s.Require().NoError(conn.WriteJSON(serverEvent{Type: eventAudioDelta, Delta: base64.StdEncoding.EncodeToString(speech.Bytes())}))
	s.awaitEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.ResponseComplete); return ok })
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond)))
	for {
		var frame map[string]any
		if err := conn.ReadJSON(&frame); err != nil {
			break
		}
		s.NotEqual("response.create", frame["type"], "the backend already began the reply")
	}
}

// awaitEvent reads events until one matches, failing if none is already waiting.
func (s *OpenAILiveSocketSuite) awaitEvent(provider *STS, matches func(sts.Event) bool, message ...any) {
	timer := time.NewTimer(50 * time.Millisecond)
	if len(message) == 0 {
		timer.Reset(5 * time.Second)
	}
	defer timer.Stop()
	for {
		select {
		case event := <-provider.Events():
			if matches(event) {
				return
			}
		case <-timer.C:
			s.FailNow("the event never came", message...)
		}
	}
}

func (s *OpenAILiveSocketSuite) TestInterruptWithNoActiveTurnPreservesUnconfirmedToolReply() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{ReplyGap: 80 * time.Millisecond})
	defer s.hangUp(provider, conn)

	s.Require().NoError(provider.Answer("call_1", `{"question":"Shall I book it?"}`, nil))
	s.Equal("response.item.create", s.spokenTo(conn)["type"])
	s.Equal(map[string]any{"type": "response.create"}, s.spokenTo(conn))

	// Interrupt while no turn is active must not wipe unconfirmed.
	s.Require().NoError(provider.Interrupt(0))

	speech := sts.PcmData{Samples: []int16{400, -401}, SampleRate: SampleRate, Channels: 1}
	s.Require().NoError(conn.WriteJSON(serverEvent{Type: eventAudioDelta, Delta: base64.StdEncoding.EncodeToString(speech.Bytes())}))
	s.awaitEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.ResponseComplete); return ok })

	s.Equal(map[string]any{"type": "response.create"}, s.spokenTo(conn),
		"unconfirmed tool reply must survive an Interrupt when no turn was active")
}

func (s *OpenAILiveSocketSuite) TestClosingGraceStopsSilenceClockAndRejectsClientSends() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{})

	s.Require().NoError(provider.Prompt("Greet the caller."))
	// Wait until carryTheClock has sent at least one silence frame.
	for {
		frame := s.nextFrame(conn)
		if frame["type"] == eventAudioAppend {
			break
		}
	}

	closed := make(chan error, 1)
	go func() { closed <- provider.Close() }()

	// Drain frames until session.close arrives.
	for {
		frame := s.nextFrame(conn)
		if frame["type"] == eventSessionClose {
			break
		}
	}

	// Entering ClosingGrace must immediately clear carrying and refuse new clock starts.
	provider.mu.Lock()
	carryingAfterClose := provider.carrying
	provider.mu.Unlock()
	s.False(carryingAfterClose, "Close must clear carrying immediately on entering ClosingGrace")
	provider.carryTheClock()
	provider.mu.Lock()
	carryingAfterRetry := provider.carrying
	provider.mu.Unlock()
	s.False(carryingAfterRetry, "carryTheClock must not start during ClosingGrace")

	// While in ClosingGrace, client sends must be rejected.
	pcm := sts.PcmData{Samples: make([]int16, 1600), SampleRate: sts.InputSampleRate, Channels: 1}
	s.ErrorContains(provider.ProcessAudio(pcm, sts.Participant{ID: "alice"}), "session closed")
	s.ErrorContains(provider.Prompt("Too late."), "session closed")

	// No silence frames or other client frames may arrive after session.close.
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond)))
	var extra map[string]any
	err := conn.ReadJSON(&extra)
	s.Error(err, "no frame may be sent after session.close, got %v", extra)

	s.Require().NoError(conn.WriteMessage(websocket.TextMessage,
		[]byte(`{"type":"session.closed","reason":"close_requested","usage":{"seconds":2}}`)))
	s.Require().NoError(<-closed)
}

func (s *OpenAILiveSocketSuite) TestAFailedHandshakeAllowsStartRetry() {
	var attempts atomic.Int32
	done := make(chan struct{})
	defer close(done)

	upgrader := websocket.Upgrader{}
	conns := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		if attempts.Add(1) == 1 {
			_ = conn.WriteMessage(websocket.TextMessage,
				[]byte(`{"type":"error","error":{"type":"server_error","code":"transient","message":"try again"}}`))
			_ = conn.Close()
			return
		}
		if err := conn.WriteMessage(websocket.TextMessage,
			[]byte(`{"type":"session.started","session":{"id":"sess_retry","model":"gpt-live-1"}}`)); err != nil {
			return
		}
		conns <- conn
		<-done
	}))
	defer server.Close()

	provider, err := New(Options{
		APIKey:           "test-key",
		URL:              "ws://" + strings.TrimPrefix(server.URL, "http://"),
		HandshakeTimeout: 5 * time.Second,
	})
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.ErrorContains(provider.Start(ctx), "session rejected")

	s.Require().NoError(provider.Start(ctx), "a failed Start must roll back to Unstarted so it can be retried")
	conn := <-conns
	defer s.hangUp(provider, conn)

	pcm := sts.PcmData{Samples: make([]int16, 1600), SampleRate: sts.InputSampleRate, Channels: 1}
	s.Require().NoError(provider.ProcessAudio(pcm, sts.Participant{ID: "alice"}))
	s.Equal(eventAudioAppend, s.nextFrame(conn)["type"])
}

func (s *OpenAILiveSocketSuite) TestADialFailureAllowsStartRetry() {
	fake := newFakeLive("")
	defer fake.close()

	provider, err := New(Options{
		APIKey:           "test-key",
		URL:              fake.endpoint(),
		HandshakeTimeout: 5 * time.Second,
	})
	s.Require().NoError(err)

	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	s.ErrorContains(provider.Start(cancelled), "dial")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Require().NoError(provider.Start(ctx), "a dial failure must roll back to Unstarted so Start can be retried")
	conn := <-fake.conns
	defer s.hangUp(provider, conn)

	s.ErrorContains(provider.Start(ctx), "already started")
}

func (s *OpenAILiveSocketSuite) TestCloseDuringHandshakeAbortsStart() {
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	defer close(done)

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		close(entered)
		<-release
		_ = conn.WriteMessage(websocket.TextMessage,
			[]byte(`{"type":"session.started","session":{"id":"sess_aborted","model":"gpt-live-1"}}`))
		<-done
	}))
	defer server.Close()

	provider, err := New(Options{
		APIKey:           "test-key",
		URL:              "ws://" + strings.TrimPrefix(server.URL, "http://"),
		HandshakeTimeout: 5 * time.Second,
	})
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	startErr := make(chan error, 1)
	go func() { startErr <- provider.Start(ctx) }()

	<-entered
	s.Require().NoError(provider.Close())
	close(release)

	s.ErrorContains(<-startErr, "session closed")
}

func (s *OpenAILiveSocketSuite) TestCloseDuringFailedHandshakeKeepsSessionClosed() {
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	defer close(done)

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		close(entered)
		<-release
		_ = conn.WriteMessage(websocket.TextMessage,
			[]byte(`{"type":"error","error":{"type":"server_error","code":"transient","message":"boom"}}`))
		_ = conn.Close()
		<-done
	}))
	defer server.Close()

	provider, err := New(Options{
		APIKey:           "test-key",
		URL:              "ws://" + strings.TrimPrefix(server.URL, "http://"),
		HandshakeTimeout: 5 * time.Second,
	})
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	startErr := make(chan error, 1)
	go func() { startErr <- provider.Start(ctx) }()

	<-entered
	s.Require().NoError(provider.Close())
	close(release)

	s.ErrorContains(<-startErr, "session rejected")
	s.ErrorContains(provider.Start(ctx), "session closed")
}

func (s *OpenAILiveSocketSuite) TestConcurrentSessionAndReplyOperationsAreRaceFree() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{ReplyGap: 5 * time.Millisecond})

	// Drain client frames on the server side so writes never block.
	serverReadDone := make(chan struct{})
	go func() {
		defer close(serverReadDone)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()

	// Drain provider events and verify every ResponseStarted settles at most once.
	var (
		startedCount   = map[string]int{}
		completedCount = map[string]int{}
		eventsDone     = make(chan struct{})
	)
	go func() {
		defer close(eventsDone)
		for event := range provider.Events() {
			switch typed := event.(type) {
			case sts.ResponseStarted:
				startedCount[typed.ResponseID]++
			case sts.ResponseComplete:
				completedCount[typed.ResponseID]++
			}
		}
	}()

	speech := sts.PcmData{Samples: []int16{400, -401}, SampleRate: SampleRate, Channels: 1}
	speechDelta := base64.StdEncoding.EncodeToString(speech.Bytes())
	pcm := sts.PcmData{Samples: make([]int16, 1600), SampleRate: sts.InputSampleRate, Channels: 1}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Server audio + transcript writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = conn.WriteJSON(serverEvent{Type: eventAudioDelta, Delta: speechDelta})
				_ = conn.WriteJSON(serverEvent{Type: eventOutputTranscript, Delta: "hi"})
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Concurrent Interrupt callers racing with fellQuiet.
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = provider.Interrupt(0)
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}

	// Concurrent client writers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = provider.ProcessAudio(pcm, sts.Participant{ID: "alice"})
				_ = provider.Answer("call_1", `{"ok":true}`, nil)
				_ = provider.Prompt("go")
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Concurrent Close calls while server sends session.closed.
	var closeWG sync.WaitGroup
	for range 3 {
		closeWG.Add(1)
		go func() {
			defer closeWG.Done()
			_ = provider.Close()
		}()
	}
	_ = conn.WriteMessage(websocket.TextMessage,
		[]byte(`{"type":"session.closed","reason":"close_requested","usage":{"seconds":1}}`))
	closeWG.Wait()
	_ = conn.Close()
	<-serverReadDone
	<-eventsDone

	for id, n := range completedCount {
		s.Equal(1, n, "response %s must complete at most once", id)
		s.Equal(1, startedCount[id], "completed response %s must have been started once", id)
	}
}

func (s *OpenAILiveSocketSuite) TestFinalUsageSurvivesACloseDuringServerEnd() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{})

	// Fill the remaining emitter buffer (Connected already occupies 1 slot) so readLoop
	// blocks on emitting final SessionUsage.
	for range sts.EmitterBuffer - 1 {
		provider.emitter.Send(sts.SessionUsage{Seconds: 1})
	}

	s.Require().NoError(conn.WriteMessage(websocket.TextMessage,
		[]byte(`{"type":"session.closed","reason":"expired","usage":{"seconds":9}}`)))

	s.Eventually(func() bool {
		provider.mu.Lock()
		defer provider.mu.Unlock()
		return provider.state == sessionServerEnded
	}, 2*time.Second, time.Millisecond, "readLoop should enter sessionServerEnded")

	closed := make(chan error, 1)
	go func() { closed <- provider.Close() }()

	// Give Close a moment to run while readLoop is still blocked on the full emitter.
	time.Sleep(100 * time.Millisecond)

	var finalUsage *sts.SessionUsage
	for event := range provider.Events() {
		if usage, ok := event.(sts.SessionUsage); ok && usage.Final {
			copyUsage := usage
			finalUsage = &copyUsage
		}
	}
	s.Require().NoError(<-closed)
	s.Require().NotNil(finalUsage, "final SessionUsage must survive a concurrent Close during ServerEnded")
	s.Equal(int64(9), finalUsage.Seconds)
}

func (s *OpenAILiveSocketSuite) TestAReplyInFlightAtCloseIsSettledInterruptedAfterTheGrace() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{ReplyGap: 50 * time.Millisecond})

	speech := sts.PcmData{Samples: []int16{400, -401}, SampleRate: SampleRate, Channels: 1}
	s.Require().NoError(conn.WriteJSON(serverEvent{Type: eventAudioDelta, Delta: base64.StdEncoding.EncodeToString(speech.Bytes())}))
	s.awaitEvent(provider, func(event sts.Event) bool { _, ok := event.(sts.AudioChunk); return ok })

	closed := make(chan error, 1)
	go func() { closed <- provider.Close() }()
	s.Equal(map[string]any{"type": eventSessionClose}, s.nextFrame(conn))

	// Sleep well past ReplyGap while in ClosingGrace; the reply must not settle uninterrupted.
	time.Sleep(200 * time.Millisecond)
	s.Require().NoError(conn.WriteMessage(websocket.TextMessage,
		[]byte(`{"type":"session.closed","reason":"close_requested","usage":{"seconds":1}}`)))
	s.Require().NoError(<-closed)

	var completes []sts.ResponseComplete
	for event := range provider.Events() {
		if typed, ok := event.(sts.ResponseComplete); ok {
			completes = append(completes, typed)
		}
	}
	s.Require().Len(completes, 1)
	s.True(completes[0].Interrupted, "a reply in flight at Close must settle as interrupted, not via quiet timer")
}

func (s *OpenAILiveSocketSuite) TestASendWaitingOnTheWriterWhenCloseBeginsIsRefused() {
	fake := newFakeLive("")
	defer fake.close()
	provider, conn := s.connect(fake, Options{})
	defer func() {
		provider.mu.Lock()
		provider.state = sessionRunning
		provider.mu.Unlock()
		s.hangUp(provider, conn)
	}()

	pcm := sts.PcmData{Samples: make([]int16, 1600), SampleRate: sts.InputSampleRate, Channels: 1}

	// Hold writeMu while ProcessAudio reaches it, then transition to ClosingGrace before releasing.
	provider.writeMu.Lock()
	sendErr := make(chan error, 1)
	go func() { sendErr <- provider.ProcessAudio(pcm, sts.Participant{ID: "alice"}) }()
	time.Sleep(20 * time.Millisecond)

	provider.mu.Lock()
	provider.state = sessionClosingGrace
	provider.mu.Unlock()
	provider.writeMu.Unlock()

	s.ErrorContains(<-sendErr, "session closed")
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)))
	var extra map[string]any
	s.Error(conn.ReadJSON(&extra), "refused send must not write a frame to the socket")
}
