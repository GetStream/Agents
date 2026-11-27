package agent

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

func (s *AgentSuite) TestSetSettingsWaitsForHeldGreetingToSettle() {
	s.onFakeClock(func() {
		w := s.joinSwappable()
		model := w.model("stub")
		model.mu.Lock()
		model.reply = nil
		model.mu.Unlock()

		s.speak(nash)
		s.eventually(func() bool {
			s.agent.mu.Lock()
			_, listening := s.agent.listeners[nash.ID]
			s.agent.mu.Unlock()
			return listening && len(w.listener().transcribed()) > 0
		}, "nobody listened")
		w.listener().emitter.Send(stt.Transcript{Participant: nash, Mode: stt.ModeFinal, Text: "Hello? Hello. Hello.", Language: "en", Confidence: 1})
		s.eventually(func() bool { return len(model.requests()) == 1 }, "the reply was never asked for")
		turn := model.requests()[0].ID

		// Hold the reply for a lone greeting and finish the model with no text so generating
		// is false and utterances is 0 while held and releasing remain active.
		w.listener().emitter.Send(stt.Transcript{Participant: nash, Mode: stt.ModeReplacement, Text: "Hello?", Language: "en"})
		s.eventually(func() bool { return s.holding(turn) }, "the reply was never held for the greeting")
		model.finishes(turn)
		s.eventually(func() bool {
			s.agent.mu.Lock()
			defer s.agent.mu.Unlock()
			return !s.agent.generating && s.agent.utterances == 0 && s.agent.held != nil
		}, "the model never finished while held")

		native := cascadeOnStub
		native.STSTarget = "openai/gpt-realtime-2"
		shortCtx, cancel := context.WithTimeout(s.ctx, 100*time.Millisecond)
		defer cancel()
		err := s.agent.SetSettings(shortCtx, native)
		s.Require().ErrorIs(err, context.DeadlineExceeded, "SetSettings swapped the pipeline while a reply was held for a greeting")
		s.False(s.agent.Native(), "pipeline must not switch to native while a greeting hold is active")

		// Once the greeting hold settles, SetSettings proceeds cleanly.
		s.eventually(func() bool { return !s.holding(turn) }, "the held reply never settled")
		s.Require().NoError(s.agent.SetSettings(s.ctx, native))
		s.True(s.agent.Native())
	})
}

func (s *AgentSuite) TestSetSettingsWaitsForComposingToFinish() {
	s.onFakeClock(func() {
		w := s.joinSwappable()
		model := w.model("stub")
		releaseCompose := make(chan struct{})
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(releaseCompose) })
		model.mu.Lock()
		model.holdCompose = releaseCompose
		model.composes = []string{"Still there?"}
		model.mu.Unlock()

		s.agent.checkIn(nash, idlePurpose)
		s.eventually(func() bool { return s.agent.floor().Composing && len(model.composeRequests()) == 1 }, "compose never started")

		shortCtx, cancel := context.WithTimeout(s.ctx, 100*time.Millisecond)
		defer cancel()
		err := s.agent.SetSettings(shortCtx, Settings{
			LLMTarget: "other/other-model", STTTarget: "stub/stub-model", TTSTarget: "other/other-model",
		})
		s.Require().ErrorIs(err, context.DeadlineExceeded, "SetSettings swapped the pipeline while composing a line")
		s.Same(s.agent.LLM(), s.agent.llm)

		releaseOnce.Do(func() { close(releaseCompose) })
		s.eventually(func() bool { return !s.agent.floor().Composing && !s.agent.Busy() }, "compose never finished")

		s.Require().NoError(s.agent.SetSettings(s.ctx, Settings{
			LLMTarget: "other/other-model", STTTarget: "stub/stub-model", TTSTarget: "other/other-model",
		}))
	})
}

func (s *AgentSuite) TestCloseWaitsForReleasingHeldReplyCallback() {
	s.join(true)
	turn := s.answering("Hello? Hello. Hello.")
	s.greetedOver(turn, s.mutters, "Hello?")
	s.model.writes(turn, "Hi, yes, I can hear you.")
	s.eventually(func() bool {
		s.agent.mu.Lock()
		defer s.agent.mu.Unlock()
		return s.agent.heldLocked(turn) && len(s.agent.held.unsaid) > 0
	}, "the held reply was never buffered")

	entered := make(chan struct{})
	releaseSynth := make(chan struct{})
	s.voice.mu.Lock()
	s.voice.synthesizing = entered
	s.voice.synthesisRelease = releaseSynth
	s.voice.mu.Unlock()

	// Fire the releasing timer immediately so its callback enters sayAgain -> voice.Synthesize.
	s.agent.releaseUnheard(turn, time.Now())
	select {
	case <-entered:
	case <-time.After(settleFor):
		s.FailNow("releasing timer never entered voice.Synthesize")
	}

	var closed atomic.Bool
	closeDone := make(chan error, 1)
	go func() {
		err := s.agent.Close()
		closed.Store(true)
		closeDone <- err
	}()

	// Close must wait for the in-flight releasing callback instead of returning while sayAgain runs.
	time.Sleep(50 * time.Millisecond)
	s.False(closed.Load(), "Close returned while the releasing timer callback was still running sayAgain")

	close(releaseSynth)
	select {
	case err := <-closeDone:
		s.Require().NoError(err)
	case <-time.After(settleFor):
		s.FailNow("Close did not finish after releasing callback unblocked")
	}
}

func (s *AgentSuite) TestCloseRacingWithReleasingNeverRaces() {
	for range 10 {
		s.join(true)
		s.agent.mu.Lock()
		s.agent.speakingTurn = "turn-race"
		s.agent.held = &heldReply{
			turnID:   "turn-race",
			greeting: true,
			unsaid:   []unsaidUtterance{{synthesisID: "turn-race", text: "Hello.", final: true, counts: true, counted: true}},
		}
		s.agent.utterances = 1
		s.agent.mu.Unlock()

		s.agent.releaseUnheard("turn-race", time.Now())
		s.Require().NoError(s.agent.Close())
	}
}

func (s *AgentSuite) TestApprovalQuietDoesNotExpireDuringVoiceRecoveryOrHeldReply() {
	s.onFakeClock(func() {
		s.join(true)
		call := llm.ToolCall{ID: "call-1", TurnID: "turn-1", Name: "send_email", Arguments: `{}`}
		entry := &approval{
			call:     call,
			question: "Send the email?",
			state:    approvalStateAsked,
			said:     true,
		}
		s.agent.mu.Lock()
		s.agent.approvals = map[string]*approval{call.ID: entry}
		s.agent.recoveringVoice = s.agent.tts
		s.agent.mu.Unlock()

		s.agent.tickApprovals()

		s.agent.mu.Lock()
		stateDuringRecovery := entry.state
		playedDuringRecovery := entry.played
		s.agent.recoveringVoice = nil
		s.agent.held = &heldReply{turnID: "turn-1", greeting: true}
		s.agent.mu.Unlock()

		s.Equal(approvalStateAsked, stateDuringRecovery, "approval started expiring during voice recovery")
		s.False(playedDuringRecovery, "approval was marked played during voice recovery")

		s.agent.tickApprovals()

		s.agent.mu.Lock()
		stateDuringHold := entry.state
		playedDuringHold := entry.played
		s.agent.held = nil
		s.agent.interrupting = true
		s.agent.mu.Unlock()

		s.Equal(approvalStateAsked, stateDuringHold, "approval started expiring while a reply was held")
		s.False(playedDuringHold, "approval was marked played while a reply was held")

		s.agent.tickApprovals()

		s.agent.mu.Lock()
		stateDuringInterrupt := entry.state
		playedDuringInterrupt := entry.played
		s.agent.interrupting = false
		s.agent.mu.Unlock()

		s.Equal(approvalStateAsked, stateDuringInterrupt, "approval started expiring during interruption")
		s.False(playedDuringInterrupt, "approval was marked played during interruption")

		s.agent.tickApprovals()

		s.agent.mu.Lock()
		stateWhenIdle := entry.state
		playedWhenIdle := entry.played
		s.agent.mu.Unlock()

		s.Equal(approvalStateExpiring, stateWhenIdle, "approval did not start expiring once idle")
		s.True(playedWhenIdle, "approval was not marked played once idle")
	})
}

func (s *AgentSuite) TestBusyReflectsHeldGreetingComposingAndNativeAwaiting() {
	s.onFakeClock(func() {
		s.join(true)
		s.False(s.agent.Busy())

		s.agent.mu.Lock()
		s.agent.held = &heldReply{turnID: "turn-1", greeting: true}
		s.agent.mu.Unlock()
		s.True(s.agent.Busy(), "Busy must be true while a reply is held for a greeting")

		s.agent.mu.Lock()
		s.agent.held = nil
		s.agent.composing = true
		s.agent.mu.Unlock()
		s.True(s.agent.Busy(), "Busy must be true while composing a line")

		s.agent.mu.Lock()
		s.agent.composing = false
		s.agent.nativeAwaiting = true
		s.agent.mu.Unlock()
		s.True(s.agent.Busy(), "Busy must be true while awaiting a native reply")

		s.agent.mu.Lock()
		s.agent.nativeAwaiting = false
		s.agent.mu.Unlock()
		s.False(s.agent.Busy(), "Busy must be false once idle")
	})
}

func (s *AgentSuite) TestSupersededReleasingTimerDoesNotSynthesizeBeforeNewDeadline() {
	s.join(true)
	turn := "turn-seq"
	s.agent.mu.Lock()
	s.agent.speakingTurn = turn
	s.agent.held = &heldReply{
		turnID:   turn,
		greeting: true,
		unsaid:   []unsaidUtterance{{synthesisID: turn, text: "Hello.", final: true, counts: true, counted: true}},
	}
	s.agent.utterances = 1
	s.agent.mu.Unlock()

	// Arm an expiring timer, hold a.mu until it fires and blocks on a.mu.Lock(), then
	// supersede it with a later deadline before releasing a.mu so the stale callback runs
	// while a.releasing is non-nil and a.held is unchanged.
	s.agent.releaseUnheard(turn, time.Now().Add(2*time.Millisecond))
	s.agent.mu.Lock()
	time.Sleep(20 * time.Millisecond)
	s.agent.releaseUnheardLocked(turn, time.Now().Add(time.Hour))
	s.agent.mu.Unlock()

	time.Sleep(30 * time.Millisecond)
	s.Empty(s.voice.spoken(), "a superseded releasing callback must not synthesize before the new deadline")
	s.True(s.holding(turn), "the held reply must stay held until the new deadline")
}

func (s *AgentSuite) TestSwitchingBlocksNewSpeechCompositionAndGreetingHolds() {
	s.onFakeClock(func() {
		s.join(true)
		s.agent.switching.Store(true)
		defer s.agent.switching.Store(false)

		s.agent.mu.Lock()
		s.agent.speakingTurn = "turn-sw"
		s.agent.unheard = unheardReply{turnID: "turn-sw", participant: nash.ID}
		s.agent.toolUpdates = map[string]toolUpdate{"call-1": {text: "working", verbatim: false}}
		s.agent.mu.Unlock()

		s.agent.backchannel(nash, "uh-huh")
		s.Empty(s.voice.spoken(), "backchannel must not speak while switching")

		s.agent.checkIn(nash, idlePurpose)
		s.False(s.agent.floor().Composing, "checkIn must not start composing while switching")
		s.Empty(s.model.composeRequests(), "checkIn must not call the model while switching")

		s.agent.sayComposed(nash, "Still there?", idlePurpose, time.Now())
		s.Empty(s.voice.spoken(), "sayComposed must not speak while switching")

		s.agent.sayToolUpdate()
		s.False(s.agent.floor().Composing, "sayToolUpdate must not start composing while switching")
		s.agent.mu.Lock()
		remainingUpdates := len(s.agent.toolUpdates)
		s.agent.mu.Unlock()
		s.Equal(1, remainingUpdates, "sayToolUpdate must not consume queued updates while switching")

		s.False(s.agent.holdUnheard("turn-sw"), "holdUnheard must refuse while switching")
		s.agent.mu.Lock()
		heldAfterHold := s.agent.held
		s.agent.held = &heldReply{turnID: "turn-sw", greeting: true}
		s.agent.mu.Unlock()
		s.Nil(heldAfterHold, "holdUnheard must not set held while switching")

		s.agent.releaseUnheard("turn-sw", time.Now())
		s.agent.mu.Lock()
		releasing := s.agent.releasing
		s.agent.held = nil
		s.agent.mu.Unlock()
		s.Nil(releasing, "releaseUnheard must not arm a timer while switching")
	})
}
