package session

import (
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

func (s *SessionSuite) TestWatchRejectsAttachmentWhileClosing() {
	s.manages()
	created := s.joins(Spec{})
	edge := s.edges[len(s.edges)-1]
	edge.holdSpeech(true)

	closed := make(chan error, 1)
	go func() { closed <- created.Close() }()
	s.eventually(func() bool { return created.State() == Closing }, "the session never entered closing")

	events, detach := created.Watch()
	defer detach()

	select {
	case _, open := <-events:
		s.False(open, "a watcher attached during closing must receive a closed channel")
	case <-time.After(100 * time.Millisecond):
		s.Fail("watch admitted a new watcher while the session was closing")
	}

	created.mu.Lock()
	watcherCount := len(created.watchers)
	created.mu.Unlock()
	s.Zero(watcherCount, "a watcher attached during closing must not be registered")

	edge.holdSpeech(false)
	s.Require().NoError(<-closed)
	s.Equal(Ended, created.State())
}

func (s *SessionSuite) TestDetachingLastWatcherWhileClosingDoesNotArmUnwatchedTimer() {
	s.grace = 20 * time.Millisecond
	s.persists()
	running := s.commands()

	_, detach := running.Watch()
	_, _, err := running.RespondCommand(s.ctx, "command-closing", "Hold reply", "", nil, options.LLM{})
	s.Require().NoError(err)
	s.Equal("Hold reply", s.asked())

	closed := make(chan error, 1)
	go func() { closed <- running.Close() }()
	s.eventually(func() bool { return running.State() == Closing }, "the session never entered closing")

	detach()

	running.mu.Lock()
	armed := running.unwatched != nil
	running.mu.Unlock()
	s.False(armed, "detaching the last watcher while closing must not arm the unwatched timer")

	s.gated.answers(1)
	s.Require().NoError(<-closed)
	s.Equal(Ended, running.State())
}

func (s *SessionSuite) TestCloseStopsArmedUnwatchedTimerOnEnteringClosing() {
	s.grace = time.Hour
	s.persists()
	running := s.commands()

	_, detach := running.Watch()
	_, _, err := running.RespondCommand(s.ctx, "command-stop-timer", "Hold reply", "", nil, options.LLM{})
	s.Require().NoError(err)
	s.Equal("Hold reply", s.asked())

	detach()
	running.mu.Lock()
	armedBefore := running.unwatched != nil
	running.mu.Unlock()
	s.Require().True(armedBefore, "detaching in Live must arm the unwatched timer")

	closed := make(chan error, 1)
	go func() { closed <- running.Close() }()
	s.eventually(func() bool { return running.State() == Closing }, "the session never entered closing")

	running.mu.Lock()
	armedDuringClose := running.unwatched != nil
	running.mu.Unlock()
	s.False(armedDuringClose, "entering Closing must stop any previously armed unwatched timer")

	s.gated.answers(1)
	s.Require().NoError(<-closed)
	s.Equal(Ended, running.State())
}

func (s *SessionSuite) TestWatchAndDetachBehaviourWhileRetiring() {
	s.grace = 20 * time.Millisecond
	s.persists()
	active := s.active()
	running := s.commands()
	s.Equal(active+1, s.active())

	_, detachFirst := running.Watch()
	s.Require().True(s.manager.Retire(running))
	s.Equal(Retiring, running.State())
	s.True(running.Retiring())

	events, detachSecond := running.Watch()
	select {
	case _, open := <-events:
		s.True(open, "watch must accept a watcher while retiring")
	case <-time.After(20 * time.Millisecond):
	}

	detachFirst()
	detachSecond()

	running.mu.Lock()
	armed := running.unwatched != nil
	running.mu.Unlock()
	s.False(armed, "detaching the last watcher while retiring must not arm the unwatched timer")

	s.Never(func() bool { return running.State() == Ended },
		3*s.grace, 5*time.Millisecond, "a retiring session must not end via the unwatched timer")

	closed, err := s.manager.CloseRetired(running)
	s.Require().NoError(err)
	s.True(closed)
	s.Equal(Ended, running.State())
	s.False(running.Retiring())
	s.Equal(active, s.active(), "closing a retiring session must decrement router_sessions_active")
}

func (s *SessionSuite) TestRetireStopsPreArmedUnwatchedTimerAndEndUnwatchedIgnoresRetiring() {
	s.grace = 20 * time.Millisecond
	s.persists()
	running := s.commands()

	_, detach := running.Watch()
	detach()
	running.mu.Lock()
	armedBefore := running.unwatched != nil
	running.mu.Unlock()
	s.Require().True(armedBefore, "detaching in Live must arm the unwatched timer")

	s.Require().True(s.manager.Retire(running))
	running.mu.Lock()
	armedAfterRetire := running.unwatched != nil
	running.mu.Unlock()
	s.False(armedAfterRetire, "retiring must stop any pre-armed unwatched timer")

	s.Never(func() bool { return running.State() == Ended },
		3*s.grace, 5*time.Millisecond, "a retired session must not be abandoned by a pre-armed unwatched timer")

	// Simulates an already-fired timer callback racing with beginRetire.
	running.endUnwatched()
	s.Equal(Retiring, running.State(), "endUnwatched must not abandon a retiring session")

	closed, err := s.manager.CloseRetired(running)
	s.Require().NoError(err)
	s.True(closed)
	s.Equal(Ended, running.State())
}

func (s *SessionSuite) TestStopVoiceWhileRetiringDoesNotArmUnwatchedTimer() {
	s.grace = 20 * time.Millisecond
	s.persists()
	running, err := s.manager.Create(s.ctx, Spec{
		CustomerID:          "acme",
		CallID:              "call-retire",
		PersistConversation: true,
		LLMTarget:           "en-low-latency",
		STTTarget:           "en-low-latency",
		TTSTarget:           "en-low-latency",
		Caller:              routing.Caller{UserID: "employee-1"},
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = running.Close() })

	s.Require().True(s.manager.Retire(running))
	s.Require().NoError(running.StopVoice(s.ctx))

	running.mu.Lock()
	armed := running.unwatched != nil
	running.mu.Unlock()
	s.False(armed, "StopVoice while retiring must not arm the unwatched timer")

	closed, err := s.manager.CloseRetired(running)
	s.Require().NoError(err)
	s.True(closed)
	s.Equal(Ended, running.State())
}

func (s *SessionSuite) TestRetireOnlyTransitionsFromLive() {
	s.manages()

	// Starting -> Retire is refused.
	starting := &Session{state: Starting, watchers: map[uint64]*watcher{}}
	s.False(s.manager.Retire(starting))
	s.Equal(Starting, starting.State())

	// Closing -> Retire is refused.
	closingSession := s.joins(Spec{})
	edge := s.edges[len(s.edges)-1]
	edge.holdSpeech(true)
	closed := make(chan error, 1)
	go func() { closed <- closingSession.Close() }()
	s.eventually(func() bool { return closingSession.State() == Closing }, "the session never entered closing")
	s.False(s.manager.Retire(closingSession))
	s.Equal(Closing, closingSession.State())
	edge.holdSpeech(false)
	s.Require().NoError(<-closed)

	// Ended -> Retire is refused.
	s.False(s.manager.Retire(closingSession))
	s.Equal(Ended, closingSession.State())
}

func (s *SessionSuite) TestWatchAndActivateBehaviourWhileStarting() {
	s.grace = 20 * time.Millisecond
	s.persists()
	running := s.commands()

	// Construct a Starting session with the same persistent conversation to verify
	// Starting accepts watchers, does not arm unwatched on detach, and activates once.
	starting := &Session{
		state:         Starting,
		persisted:     running.persisted,
		detachedGrace: s.grace,
		spec:          Spec{Text: true},
		watchers:      map[uint64]*watcher{},
	}
	events, detach := starting.Watch()
	select {
	case _, open := <-events:
		s.True(open, "watch must accept a watcher while starting")
	case <-time.After(20 * time.Millisecond):
	}
	detach()

	starting.mu.Lock()
	armed := starting.unwatched != nil
	starting.mu.Unlock()
	s.False(armed, "detaching while starting must not arm the unwatched timer")

	s.True(starting.activate())
	s.Equal(Live, starting.State())
	s.False(starting.activate(), "activate must only transition from Starting")
}

func (s *SessionSuite) TestConcurrentWatchAndDetachDuringClose() {
	s.grace = 10 * time.Millisecond
	s.persists()
	running := s.commands()

	_, _, err := running.RespondCommand(s.ctx, "command-race", "Hold reply", "", nil, options.LLM{})
	s.Require().NoError(err)
	s.Equal("Hold reply", s.asked())

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10 {
				_, detach := running.Watch()
				detach()
			}
		})
	}

	closed := make(chan error, 1)
	go func() { closed <- running.Close() }()
	s.eventually(func() bool { return running.State() == Closing }, "the session never entered closing")
	wg.Wait()

	running.mu.Lock()
	armed := running.unwatched != nil
	running.mu.Unlock()
	s.False(armed)

	s.gated.answers(1)
	s.Require().NoError(<-closed)
	s.Equal(Ended, running.State())
}
