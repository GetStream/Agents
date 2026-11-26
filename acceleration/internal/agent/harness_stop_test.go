package agent

import (
	"bytes"
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
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

// A ruling superseded while the pipeline is stopped finds no harness and is let go.
func (s *AgentSuite) TestASupersedeAfterThePipelineStoppedIsLetGo() {
	s.join(false)
	s.agent.stopPipeline(false)

	s.NotPanics(func() { s.agent.perform(Action{Kind: ActSupersede, TurnID: "candidate"}) })
}

// A subagent asked for as the pipeline stops is told there is none, not handed to the harness
// the stop cleared.
func (s *AgentSuite) TestADelegationAsThePipelineStopsFindsNoSubagent() {
	s.join(false)
	s.agent.mu.Lock()
	unlock := sync.OnceFunc(s.agent.mu.Unlock)
	defer unlock()
	cleared := s.agent.harness
	defer cleared.Close()
	var err error
	var panicked any
	answered := make(chan struct{})
	go func() {
		defer close(answered)
		defer func() { panicked = recover() }()
		_, _, err = s.agent.nativeDelegate(s.ctx, llm.ToolCall{ID: "think-1", Name: delegateSkill, Arguments: `{"skill":"think","prompt":"Work out the booking total"}`})
	}()
	s.eventually(func() bool { return waitingOnALockIn("nativeDelegate") }, "the delegation never waited on the agent's lock")
	// The clear stopPipeline makes, while the delegation waits on the lock.
	s.agent.harness = nil
	unlock()
	<-answered

	s.Nil(panicked, "the delegation read the harness the stop cleared")
	s.EqualError(err, "agent: no subagent configured")
}

// A tool's picture that comes back as the pipeline stops is not handed to the harness the stop
// cleared.
func (s *AgentSuite) TestAToolsPictureAsThePipelineStopsIsNotHandedToAClearedHarness() {
	s.runner = &stubToolRunner{run: func(context.Context, llm.ToolCall) ([]llm.ContentPart, error) {
		return []llm.ContentPart{{Text: "the room"}, {Image: &llm.ImagePart{MIME: "image/jpeg", Data: []byte{0xff, 0xd8, 0xff}}}}, nil
	}}
	s.tools = harness.Tools{Tools: []harness.Tool{{Name: "photograph_room", Description: "photograph the room"}}}
	s.join(false)
	// Whether the model looks at the picture itself is asked after the picture came back: the
	// pipeline stops at that moment.
	s.model.readingCapabilities = sync.OnceFunc(func() { s.agent.stopPipeline(false) })
	requested := harness.ToolRequested{TurnID: "r1", Call: llm.ToolCall{ID: "photo-1", Name: "photograph_room", Arguments: "{}"}}
	ctx, cancel := s.agent.prepareTool(requested)

	s.NotPanics(func() { s.agent.executeTool(ctx, cancel, requested) })
}

// waitingOnALockIn reports whether a goroutine running function waits on a mutex.
func waitingOnALockIn(function string) bool {
	stacks := make([]byte, 1<<20)
	stacks = stacks[:runtime.Stack(stacks, true)]
	for _, goroutine := range bytes.Split(stacks, []byte("\n\n")) {
		if bytes.Contains(goroutine, []byte("[sync.Mutex.Lock")) && bytes.Contains(goroutine, []byte(")."+function+"(")) {
			return true
		}
	}
	return false
}
