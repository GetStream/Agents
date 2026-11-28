package simulation

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sttrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/ttsrouter"
)

// How the caller's ears in these tests hear a run of speech: it ends after settleQuiet of
// quiet.
const (
	settleQuiet   = 300 * time.Millisecond
	sentenceSound = 100 * time.Millisecond
)

func samples(d time.Duration) int { return stt.SampleRate * int(d/time.Millisecond) / 1000 }

// tape is what the agent's voice said, a sentence at a time, and how many samples of sound
// each took, so the caller's ears can tell which sentences a run of sound was.
type tape struct {
	mu        sync.Mutex
	sentences []string
	// after is the quiet the voice leaves after a sentence, by its text.
	after map[string]time.Duration
}

func (t *tape) record(sentence string) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sentences = append(t.sentences, sentence)
	return t.after[sentence]
}

func (t *tape) sentence(i int) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sentences[i]
}

// tapedVoice is the agent's voice: each sentence is sentenceSound of sound and the quiet the
// tape gives it, as a voice ends one sentence with more quiet than another.
type tapedVoice struct {
	emitter *tts.Emitter
	tape    *tape
}

func (t *tapedVoice) Start(context.Context) error { return nil }

func (t *tapedVoice) Synthesize(request tts.Request) error {
	for _, sentence := range sentences(request.Text) {
		quiet := t.tape.record(sentence)
		pcm := make([]int16, samples(sentenceSound)+samples(quiet))
		for i := range samples(sentenceSound) {
			pcm[i] = 8000
		}
		t.emitter.Send(tts.AudioChunk{
			SynthesisID: request.ID,
			Audio:       audio.PcmData{Samples: pcm, SampleRate: stt.SampleRate, Channels: 1},
		})
	}
	if request.Final {
		t.emitter.Send(tts.SynthesisComplete{SynthesisID: request.ID})
	}
	return nil
}

func (t *tapedVoice) Interrupt() error         { return nil }
func (t *tapedVoice) Events() <-chan tts.Event { return t.emitter.Events() }
func (t *tapedVoice) Provider() string         { return "scripted" }
func (t *tapedVoice) Model() string            { return "scripted-model" }
func (t *tapedVoice) Streaming() bool          { return false }
func (t *tapedVoice) Performs() bool           { return false }
func (t *tapedVoice) Prompt() string           { return "" }

func (t *tapedVoice) Close() error {
	t.emitter.Close()
	return nil
}

func sentences(text string) []string {
	var out []string
	for part := range strings.SplitAfterSeq(strings.TrimSpace(text), ". ") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// ears is the caller's transcriber. Like a streaming one, it settles a run of speech when it
// hears the quiet after it, and only then.
type ears struct {
	emitter *stt.Emitter
	tape    *tape

	mu        sync.Mutex
	sound     int
	sentences int
	run       []string
	quiet     int
}

func (e *ears) Start(context.Context) error { return nil }

func (e *ears) ProcessAudio(pcm stt.PcmData, _ stt.Participant) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, sample := range pcm.Samples {
		if sample == 0 {
			e.quiet++
			if len(e.run) > 0 && e.quiet >= samples(settleQuiet) {
				e.send(stt.ModeFinal)
				e.run = nil
			}
			continue
		}
		e.quiet = 0
		if e.sound++; e.sound == samples(sentenceSound) {
			e.sound = 0
			e.run = append(e.run, e.tape.sentence(e.sentences))
			e.sentences++
			e.send(stt.ModeReplacement)
		}
	}
	return nil
}

func (e *ears) send(mode stt.Mode) {
	e.emitter.Send(stt.Transcript{Participant: agentParticipant, Mode: mode, Text: strings.Join(e.run, " ")})
}

func (e *ears) Events() <-chan stt.Event { return e.emitter.Events() }
func (e *ears) Provider() string         { return "scripted" }
func (e *ears) Model() string            { return "scripted-model" }

func (e *ears) Close() error {
	e.emitter.Close()
	return nil
}

type AudioSuite struct {
	suite.Suite
	ctx  context.Context
	tape *tape
	ears *ears
}

func TestAudioSuite(t *testing.T) {
	suite.Run(t, new(AudioSuite))
}

// onFakeClock runs body in a synctest bubble, so the call's pacing and the waits for the
// agent to settle take no real time.
func (s *AudioSuite) onFakeClock(body func()) {
	outer := s.T()
	synctest.Test(outer, func(t *testing.T) {
		s.SetT(t)
		defer s.SetT(outer)
		// Made in the bubble: a goroutine waiting on a channel made outside it stops its clock.
		s.ctx = context.Background()
		s.tape = &tape{after: map[string]time.Duration{}}
		s.ears = &ears{emitter: stt.NewEmitter(256), tape: s.tape}
		body()
	})
}

// call opens a conversation out loud with an agent whose voice is the tape, heard by the
// suite's ears. Everything is built here, inside the bubble, so its timers run on its clock.
func (s *AudioSuite) call(greeting string) *spoken {
	logger := slog.New(slog.DiscardHandler)

	hearing := sttrouter.NewRegistry()
	hearing.Register("scripted", func(routing.Spec) (stt.STT, error) {
		return &deafSTT{emitter: stt.NewEmitter(64)}, nil
	})
	hearer, err := sttrouter.New(sttrouter.Options{Config: scriptedConfig(), Registry: hearing, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(hearer.Close)

	caller := sttrouter.NewRegistry()
	caller.Register("scripted", func(routing.Spec) (stt.STT, error) { return s.ears, nil })
	listener, err := sttrouter.New(sttrouter.Options{Config: scriptedConfig(), Registry: caller, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(listener.Close)

	reasoning := llmrouter.NewRegistry()
	reasoning.Register("scripted", func(routing.Spec) (llmrouter.Provider, error) { return &agentModel{}, nil })
	reasoner, err := llmrouter.New(llmrouter.Options{Config: scriptedConfig(), Registry: reasoning, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(reasoner.Close)

	speech := ttsrouter.NewRegistry()
	speech.Register("scripted", func(routing.Spec) (tts.TTS, error) {
		return &tapedVoice{emitter: tts.NewEmitter(64), tape: s.tape}, nil
	})
	speaker, err := ttsrouter.New(ttsrouter.Options{Config: scriptedConfig(), Registry: speech, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(speaker.Close)

	manager, err := session.NewManager(session.ManagerOptions{
		LLM: reasoner, STT: hearer, TTS: speaker, Logger: logger,
		// The simulation hands the session its own edge, so this is never reached.
		Edge: func(context.Context, session.Spec, streamapp.Bound, *slog.Logger) (agent.Edge, error) {
			return nil, errors.New("there is no call to join")
		},
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = manager.Shutdown() })

	runner := &Runner{sessions: manager, tts: speaker, stt: listener, logger: logger}
	over, err := runner.converse(s.ctx, session.Spec{
		CustomerID: "customer-1", CallID: "call-1", Greeting: greeting,
		LLMTarget: "scripted/scripted-model", SubagentTarget: "scripted/scripted-model",
		STTTarget: "scripted/scripted-model", TTSTarget: "scripted/scripted-model",
	}, store.Simulation{
		CustomerID: "customer-1", CallerTTS: "scripted/scripted-model", CallerSTT: "scripted/scripted-model",
	}, store.SimulationCase{})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = over.Close() })
	return over.(*spoken)
}

// says has the agent say a line as a turn of the conversation, and returns what the caller
// heard of it: Say without the caller's line, which the agent here does not answer.
func (s *AudioSuite) says(held *spoken, line string) string {
	held.begin()
	s.Require().NoError(held.Session().Say(s.ctx, line))
	held.settled(s.ctx)
	heard, _, _, _, _ := held.state()
	return heard
}

func (s *AudioSuite) TestTheGreetingIsNotHeardAgainInTheFirstReply() {
	s.onFakeClock(func() {
		// The greeting ends on less quiet than the caller's transcriber needs to hear its end.
		s.tape.after["Northwind, how can I help?"] = 100 * time.Millisecond
		s.tape.after["Your table is booked."] = 400 * time.Millisecond
		held := s.call("Northwind, how can I help?")

		s.Equal("Your table is booked.", s.says(held, "Your table is booked."))
	})
}

func (s *AudioSuite) TestTheEndOfOneReplyIsNotHeardAtTheStartOfTheNext() {
	s.onFakeClock(func() {
		s.tape.after["Your table is booked."] = 100 * time.Millisecond
		s.tape.after["It is for eight."] = 400 * time.Millisecond
		held := s.call("")

		s.Equal("Your table is booked.", s.says(held, "Your table is booked."))
		s.Equal("It is for eight.", s.says(held, "It is for eight."))
	})
}
