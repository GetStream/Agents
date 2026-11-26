package agent

import (
	"sync"
	"time"
)

// A reply the model had already sent when the pipeline stops is still read by the goroutine that
// speaks, so the harness it reads must not be cleared under it: the read is a data race under
// -race, and a nil harness panics it and ends the call.
func (s *AgentSuite) TestAReplyQueuedWhenThePipelineStopsIsSpokenBeforeTheHarnessIsCleared() {
	s.join(false)
	turn := s.answering("Tell me a story.")
	synthesizing, release := make(chan struct{}), make(chan struct{})
	s.voice.mu.Lock()
	s.voice.synthesizing, s.voice.synthesisRelease = synthesizing, release
	s.voice.mu.Unlock()
	released := sync.OnceFunc(func() { close(release) })
	defer released()
	s.model.writes(turn, "Once upon a time. ")
	<-synthesizing
	s.model.writes(turn, "And then it ended. ")
	s.eventually(func() bool {
		s.agent.mu.Lock()
		defer s.agent.mu.Unlock()
		return len(s.agent.replies) > 0
	}, "the second delta never queued")

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		s.agent.stopPipeline(false)
	}()
	s.eventually(func() bool {
		s.agent.mu.Lock()
		defer s.agent.mu.Unlock()
		return s.agent.pipe == nil || s.agent.pipe.ctx.Err() != nil
	}, "the pipeline never began to stop")
	// A stop that clears the harness at once does it here, and the reply goroutine is let go after
	// it. A stop that waits for that goroutine first leaves the harness, and this ends at its limit.
	for limit := time.Now().Add(100 * time.Millisecond); time.Now().Before(limit); time.Sleep(time.Millisecond) {
		s.agent.mu.Lock()
		cleared := s.agent.harness == nil
		s.agent.mu.Unlock()
		if cleared {
			break
		}
	}
	released()
	<-stopped

	var said []string
	for _, request := range s.voice.spoken() {
		said = append(said, request.Text)
	}
	s.Contains(said, "And then it ended.", "the reply queued at the stop was not read to its end")
}

// A pipeline that is already stopped has no reply goroutine to wait for.
func (s *AgentSuite) TestStoppingAPipelineThatIsAlreadyStoppedReturns() {
	s.join(false)
	s.Empty(s.agent.stopPipeline(false))

	s.Empty(s.agent.stopPipeline(false))
}
