package agent

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// hasEnded tells if the agent of drained is closed.
func hasEnded(drained *collector) bool {
	select {
	case <-drained.done:
		return true
	default:
		return false
	}
}

// A panic in one conversation stops only that conversation. The other conversations
// continue.
func (s *AgentSuite) TestAPanicInOneConversationStopsItAndNoOther() {
	s.joinText()
	other, otherEvents := s.agent, s.events
	s.joinText()
	s.model.panics = "a bug in the model adapter"

	_, err := s.agent.RespondTo(s.ctx, "Hello?", nil, options.LLM{})
	s.Require().NoError(err)
	s.eventually(func() bool { return hasEnded(s.events) }, "the conversation with the panic did not stop")

	before := countOf[Responded](otherEvents.seen())
	_, err = other.RespondTo(s.ctx, "Are you still there?", nil, options.LLM{})
	s.Require().NoError(err)
	s.eventually(func() bool { return countOf[Responded](otherEvents.seen()) == before+1 }, "the other conversation did not answer")
	s.False(hasEnded(otherEvents), "the other conversation stopped")
}

// The agent starts a provider under the context of the call. Thus a panic in a goroutine
// of the provider, for example its read loop, stops that call.
func (s *AgentSuite) TestAPanicInATranscriberGoroutineStopsItsCall() {
	s.join(false)
	s.ears.mu.Lock()
	s.ears.panics = "a bug in the read loop"
	s.ears.mu.Unlock()

	s.speak(stt.Participant{ID: "alice"})

	s.eventually(func() bool { return hasEnded(s.events) }, "the call with the panic did not stop")
}

// The agent opens a floor of chorus-asr's own under the context of the call. Thus a panic in
// a goroutine of the floor, for example when it tells the transcriber to settle the words of
// an utterance, stops that call.
func (s *AgentSuite) TestActingWithAnotherTranscriberAPanicInTheFloorStopsItsCall() {
	decided := s.floorRules(endsAtOnce)
	release := make(chan struct{})
	s.earsSettlePanics = release
	s.join(true)
	caller := stt.Participant{ID: "alice"}
	line := &callerLine{s: s, participant: caller}
	line.say(300, false)
	line.say(1200, true)
	s.transcriberWrites(caller, 1, stt.ModeReplacement, "a table at seven thirty")
	line.say(600, false)
	close(release)
	s.eventually(func() bool { return decided.Load() > 0 }, "chorus-asr was never asked about the pause")

	s.eventually(func() bool { return hasEnded(s.events) }, "the call with the panic did not stop")
}
