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
	service := persistent.NewForChat(chattest.Client(s.T()))
	s.T().Cleanup(service.Close)
	s.conversations = service
	s.records = &stubTranscript{}
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
// no turn, so the call's transcript took it and the conversation left the reply thinking.
func (s *SessionSuite) TestARefusalForMoneyOfATurnTypedIntoACallFailsItsReply() {
	running := s.transcribedCall()
	s.model.refuses = unpaid

	s.Require().NoError(running.FollowUp(s.ctx, "task_call", "Remind them to stretch", nil))

	s.eventually(func() bool {
		state, _, _ := running.FollowUpReply("task_call")
		return state == "failed"
	}, "the refused turn's reply never failed")
	s.Equal("insufficient_credits", policy.Code(running.FollowUpFailure("task_call")))
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

// A refusal for money once the turn typed into a call has answered is a later turn's: the
// typed reply, still open while the call goes on, keeps no failure, so a scheduled call that
// said its task is delivered.
func (s *SessionSuite) TestARefusalForMoneyAfterATypedTurnOnACallAnsweredLeavesItsReply() {
	running := s.transcribedCall()
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
