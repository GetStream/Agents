package session

import "time"

// A held conversation (Session.Hold) is not ended by its grace, which a scheduled delivery's turn
// needs in a session nobody watches; its release starts the grace again.

// holdGrace is the grace of a conversation in these tests, and heldFor how long each looks for
// a held conversation ending: past several graces, so a grace still counting would end it.
const (
	holdGrace = 20 * time.Millisecond
	heldFor   = 150 * time.Millisecond
)

// conversation opens a conversation whose grace is holdGrace. SetupTest leaves the suite's grace
// as the last test set it, so it is put back after the test.
func (s *SessionSuite) conversation() *Session {
	previous := s.grace
	s.T().Cleanup(func() { s.grace = previous })
	s.grace = holdGrace
	s.persists()
	return s.commands()
}

// heldOpen fails the test if running ends within heldFor.
func (s *SessionSuite) heldOpen(running *Session, message string) {
	s.Never(func() bool { return running.State() == Ended }, heldFor, 5*time.Millisecond, message)
}

func (s *SessionSuite) TestAHoldStopsTheGraceOfAConversationNobodyWatches() {
	running := s.conversation()
	_, detach := running.Watch()
	detach()

	release := running.Hold()

	s.heldOpen(running, "the grace counting when the hold was taken ended a held conversation")
	release()
	s.eventually(func() bool { return running.State() == Ended }, "the release did not start the grace again")
}

func (s *SessionSuite) TestAWatcherLeavingAHeldConversationStartsNoGrace() {
	running := s.conversation()
	release := running.Hold()

	_, detach := running.Watch()
	detach()

	s.heldOpen(running, "the last watcher leaving started the grace of a held conversation")
	release()
	s.eventually(func() bool { return running.State() == Ended }, "nobody watches or holds it, so it should end")
}

func (s *SessionSuite) TestAReleaseWhileSomebodyWatchesStartsNoGrace() {
	running := s.conversation()
	_, detach := running.Watch()

	running.Hold()()

	s.heldOpen(running, "a release ended a conversation somebody watches")
	detach()
	s.eventually(func() bool { return running.State() == Ended }, "the last watcher left, so it should end")
}

func (s *SessionSuite) TestEachHoldIsReleasedOnceAndTheLastStartsTheGrace() {
	running := s.conversation()
	first, second := running.Hold(), running.Hold()

	first()
	first()

	s.heldOpen(running, "a release called twice let go of another holder's hold")
	second()
	s.eventually(func() bool { return running.State() == Ended }, "the last hold was released, so it should end")
}

// A grace that fired just as the hold was taken, too late for Hold to stop its timer, finds the
// conversation held and ends nothing.
func (s *SessionSuite) TestAGraceThatFiresUnderAHoldEndsNothing() {
	running := s.conversation()
	release := running.Hold()
	defer release()

	running.endUnwatched()

	s.Equal(Live, running.State())
}

func (s *SessionSuite) TestStoppingVoiceInAHeldConversationStartsNoGrace() {
	running := s.conversation()
	release := running.Hold()
	s.Require().NoError(running.StartVoice(s.ctx, Call{}))

	s.Require().NoError(running.StopVoice(s.ctx))

	s.heldOpen(running, "stopping voice started the grace of a held conversation")
	release()
	s.eventually(func() bool { return running.State() == Ended }, "nobody watches or holds it, so it should end")
}
