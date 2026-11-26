package session

import (
	"errors"
	"fmt"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	persistent "github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// transcribedCall opens a person's saved conversation on a call that writes its transcript, as
// cmd/router's calls do: what is said goes to the transcript, and only a typed turn to the
// conversation.
func (s *SessionSuite) transcribedCall() *Session {
	return s.typedCall(true)
}

// onCalls runs test on a call that writes its transcript and on one that writes none, as a call
// whose transcript failed to open, where every event of the call goes to the conversation.
func (s *SessionSuite) onCalls(test func(running *Session)) {
	for _, transcribed := range []bool{true, false} {
		s.Run(fmt.Sprintf("transcribed=%t", transcribed), func() { test(s.typedCall(transcribed)) })
	}
}

// typedCall opens a person's saved conversation on a call, which writes its transcript if transcribed.
func (s *SessionSuite) typedCall(transcribed bool) *Session {
	service := persistent.NewForChat(chattest.Client(s.T()))
	s.T().Cleanup(service.Close)
	s.conversations = service
	s.records = nil
	if transcribed {
		s.records = &stubTranscript{}
	}
	s.manages()
	created := s.joins(Spec{
		PersistConversation: true,
		Caller:              routing.Caller{UserID: "employee-1"},
		Tools:               []harness.Tool{{Name: "lookup_record", Description: "look a record up"}},
	})
	s.T().Cleanup(func() { _ = created.Close() })
	return created
}

// unpaid is the refusal the router's money check gives a model call of an organization with no
// credit left.
var unpaid = fmt.Errorf("%w: the organization has no credit left", policy.ErrNoCredits)

// A turn typed into a call whose model call is refused for money ends failed in the
// conversation, with the refusal kept beside it, so a scheduled call reads how its turn ended
// without the event, which a watcher that has fallen behind loses (AI-3904). The refusal names
// no turn, so the call's transcript took it and the conversation left the reply thinking. The
// session keeps the refusal too (UnansweredRefusal).
func (s *SessionSuite) TestARefusalForMoneyOfATurnTypedIntoACallFailsItsReply() {
	s.onCalls(func(running *Session) {
		s.model.refuses = unpaid

		s.Require().NoError(running.FollowUp(s.ctx, "task_call", "Remind them to stretch", nil))

		s.eventually(func() bool {
			state, _, _ := running.FollowUpReply("task_call")
			return state == "failed"
		}, "the refused turn's reply never failed")
		s.Equal("insufficient_credits", policy.Code(running.FollowUpFailure("task_call")))
		s.Equal("insufficient_credits", policy.Code(running.UnansweredRefusal()))
	})
}

// A transcriber failing while a turn typed into a call is still being answered is not the
// reply's failure: the reply goes on to the model's answer and keeps no failure. Only a refusal
// for money ends a typed reply from outside its turn.
func (s *SessionSuite) TestATranscriberFailingDuringATypedTurnOnACallLeavesItsReplyAnswered() {
	s.gated = newGatedLLM()
	running := s.transcribedCall()
	events, detach := running.Watch()
	defer detach()
	s.Require().NoError(running.FollowUp(s.ctx, "task_call", "Remind them to stretch", nil))
	s.asked()

	// The transcriber opens on the caller's first audio, and fails.
	s.Require().Eventually(func() bool { return len(s.edges) > 0 }, settleFor, 10*time.Millisecond)
	caller := stt.Participant{ID: "alice", Name: "Alice"}
	s.edges[len(s.edges)-1].inbound <- agent.InboundAudio{Participant: caller,
		Audio: audio.PcmData{Samples: make([]int16, 320), SampleRate: 16_000, Channels: 1}}
	glitch := errors.New("the transcriber's socket dropped a frame")
	s.ears.emitter.Send(stt.Error{Err: glitch})
	s.Require().True(sawError(events, glitch), "the transcriber's failure never reached the session")
	s.gated.answers(1)

	s.Require().True(sawResponded(events), "the typed turn never answered")
	state, _, _ := running.FollowUpReply("task_call")
	s.NotEqual("failed", state)
	s.NoError(running.FollowUpFailure("task_call"))
}

// A provider failing a turn typed into a call, not for money, is not the reply's recorded
// failure: only a refusal for money ends a typed reply from outside its turn.
func (s *SessionSuite) TestAProviderFailingATypedTurnOnACallKeepsNoFailure() {
	running := s.transcribedCall()
	events, detach := running.Watch()
	defer detach()
	down := errors.New("the provider is down")
	s.model.refuses = down

	s.Require().NoError(running.FollowUp(s.ctx, "task_call", "Remind them to stretch", nil))

	s.Require().True(sawError(events, down), "the provider's failure never reached the session")
	s.NoError(running.FollowUpFailure("task_call"))
}

// A refusal for money once the turn typed into a call has answered is a later turn's: the
// typed reply, still open while the call goes on, keeps no failure, nor does the session, so a
// scheduled call that said its task is delivered, with a transcript or without one.
func (s *SessionSuite) TestARefusalForMoneyAfterATypedTurnOnACallAnsweredLeavesItsReply() {
	s.onCalls(func(running *Session) {
		events, detach := running.Watch()
		defer detach()
		s.Require().NoError(running.FollowUp(s.ctx, "task_call", "Remind them to stretch", nil))
		s.Require().True(sawResponded(events), "the typed turn never answered")

		s.model.mu.Lock()
		s.model.refuses = unpaid
		s.model.mu.Unlock()
		s.says(running, "and what else")
		s.Require().True(sawError(events, policy.ErrNoCredits), "the later turn was never refused")

		s.NoError(running.FollowUpFailure("task_call"))
		state, _, _ := running.FollowUpReply("task_call")
		s.NotEqual("failed", state)
		s.NoError(running.UnansweredRefusal())
	})
}

// A turn typed into a call that the caller cut off before it answered leaves no turn waiting
// once a later typed turn answered: a refusal for money after that is a later turn's, so the
// later typed reply keeps no failure, nor does the session.
func (s *SessionSuite) TestARefusalForMoneyAfterATypedTurnAnsweredOneCutOffBeforeItLeavesItsReply() {
	s.gated = newGatedLLM()
	s.onCalls(func(running *Session) {
		events, detach := running.Watch()
		defer detach()
		s.Require().NoError(running.FollowUp(s.ctx, "first_call", "Remind them to stretch", nil))
		s.asked()
		running.Interrupt()
		s.eventually(func() bool {
			state, _, _ := running.FollowUpReply("first_call")
			return state == "cancelled" && !running.Busy()
		}, "the first typed turn was never cut off")
		s.Require().NoError(running.FollowUp(s.ctx, "second_call", "Remind them to drink water", nil))
		s.asked()
		s.gated.answers(1)
		s.Require().True(sawResponded(events), "the second typed turn never answered")

		// The transcriber is refused for money as the caller speaks.
		s.Require().Eventually(func() bool { return len(s.edges) > 0 }, settleFor, 10*time.Millisecond)
		caller := stt.Participant{ID: "alice", Name: "Alice"}
		s.edges[len(s.edges)-1].inbound <- agent.InboundAudio{Participant: caller,
			Audio: audio.PcmData{Samples: make([]int16, 320), SampleRate: 16_000, Channels: 1}}
		s.ears.emitter.Send(stt.Error{Err: unpaid})
		s.Require().True(sawError(events, policy.ErrNoCredits), "the refusal never reached the session")

		s.NoError(running.FollowUpFailure("second_call"))
		state, _, _ := running.FollowUpReply("second_call")
		s.NotEqual("failed", state)
		s.NoError(running.UnansweredRefusal())
	})
}

// A refusal for money of the round that goes on after a typed turn's tool call is a later
// round's: the agent answered before it called the tool, as it did when a scheduled call read
// the refusal from its events, so the typed reply keeps no failure and the task counts as said.
func (s *SessionSuite) TestARefusalForMoneyOfTheRoundAfterATypedTurnsToolCallLeavesItsReply() {
	running := s.transcribedCall()
	s.model.calls = []llm.ToolCall{{ID: "call-1", Name: "lookup_record", Arguments: "{}"}}
	s.hears(running)
	events, detach := running.Watch()
	defer detach()
	s.Require().NoError(running.FollowUp(s.ctx, "task_call", "Remind them to stretch", nil))
	asked := awaitToolCall(events)
	s.Require().NotNil(asked, "the typed turn never called its tool")

	s.model.mu.Lock()
	s.model.refuses = unpaid
	s.model.mu.Unlock()
	s.True(running.ResolveTurnTool(asked.ID, asked.TurnID, llm.TextParts("on record"), ""))
	s.Require().True(sawError(events, policy.ErrNoCredits), "the round after the tool was never refused")

	s.NoError(running.FollowUpFailure("task_call"))
}

// A turn said on a call is the transcript's to the end, after a turn was typed into the call:
// its answer does not make it a typed turn, so what it speaks once it answered is not handed to
// the conversation.
func (s *SessionSuite) TestATurnSaidOnACallAfterATypedOneStaysTheTranscripts() {
	running := s.transcribedCall()
	events, detach := running.Watch()
	defer detach()
	s.Require().NoError(running.FollowUp(s.ctx, "task_call", "Remind them to stretch", nil))
	s.Require().True(sawResponded(events), "the typed turn never answered")

	s.hears(running)

	s.eventually(func() bool {
		s.records.mu.Lock()
		defer s.records.mu.Unlock()
		for _, event := range s.records.spoken {
			if _, ok := event.(agent.Spoke); ok {
				return true
			}
		}
		return false
	}, "the turn said on the call was not spoken into its transcript")
}

// sawError waits for the session to report a failure that is want.
func sawError(events <-chan Event, want error) bool {
	return awaitEvent(events, func(event Event) bool {
		failed, ok := event.(agent.Error)
		return ok && errors.Is(failed.Err, want)
	})
}

// sawResponded waits for the agent to answer.
func sawResponded(events <-chan Event) bool {
	return awaitEvent(events, func(event Event) bool {
		_, ok := event.(agent.Responded)
		return ok
	})
}

// awaitEvent waits up to settleFor for an event that is, reporting whether one came.
func awaitEvent(events <-chan Event, is func(Event) bool) bool {
	deadline := time.After(settleFor)
	for {
		select {
		case event, open := <-events:
			if !open {
				return false
			}
			if is(event) {
				return true
			}
		case <-deadline:
			return false
		}
	}
}
