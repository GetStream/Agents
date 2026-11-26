package session

import (
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

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

// graceCounting reports whether running's grace is counting down: a timer that would end it.
// endUnwatched asks again when it fires, so a session can stay open with one counting.
func graceCounting(running *Session) bool {
	running.mu.Lock()
	defer running.mu.Unlock()
	return running.unwatched != nil
}

// heldOpen fails the test if running ends within heldFor.
func (s *SessionSuite) heldOpen(running *Session, message string) {
	s.Never(func() bool { return running.State() == Ended }, heldFor, 5*time.Millisecond, message)
}

func (s *SessionSuite) TestAHoldStopsTheGraceOfAConversationNobodyWatches() {
	running := s.conversation()
	_, detach := running.Watch()
	detach()

	s.Require().True(graceCounting(running), "the last watcher left, so its grace counts")

	release := running.Hold()

	s.False(graceCounting(running), "the hold did not stop the grace")
	s.heldOpen(running, "the grace counting when the hold was taken ended a held conversation")
	release()
	s.eventually(func() bool { return running.State() == Ended }, "the release did not start the grace again")
}

func (s *SessionSuite) TestAWatcherLeavingAHeldConversationStartsNoGrace() {
	running := s.conversation()
	release := running.Hold()

	_, detach := running.Watch()
	detach()

	s.False(graceCounting(running), "the last watcher leaving started the grace of a held conversation")
	s.heldOpen(running, "the last watcher leaving ended a held conversation")
	release()
	s.eventually(func() bool { return running.State() == Ended }, "nobody watches or holds it, so it should end")
}

func (s *SessionSuite) TestAReleaseWhileSomebodyWatchesStartsNoGrace() {
	running := s.conversation()
	_, detach := running.Watch()

	running.Hold()()

	s.False(graceCounting(running), "a release started the grace of a conversation somebody watches")
	s.heldOpen(running, "a release ended a conversation somebody watches")
	detach()
	s.eventually(func() bool { return running.State() == Ended }, "the last watcher left, so it should end")
}

func (s *SessionSuite) TestEachHoldIsReleasedOnceAndTheLastStartsTheGrace() {
	running := s.conversation()
	_, detach := running.Watch()
	detach()
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

	s.False(graceCounting(running), "stopping voice started the grace of a held conversation")
	s.heldOpen(running, "stopping voice ended a held conversation")
	release()
	s.eventually(func() bool { return running.State() == Ended }, "nobody watches or holds it, so it should end")
}

// A conversation nobody ever watched has no grace counting, so a hold and its release leave it
// as they found it: open, as before holds were counted.
func (s *SessionSuite) TestAReleaseInAConversationNobodyWatchedStartsNoGrace() {
	running := s.conversation()

	running.Hold()()

	s.False(graceCounting(running), "a release started a grace nobody was owed")
	s.heldOpen(running, "a release ended a conversation nobody ever watched")
}

// A delivery that closes the session it opened lets go of its hold afterwards: the release starts
// no grace in a session that has ended.
func (s *SessionSuite) TestAReleaseAfterTheConversationEndedStartsNoGrace() {
	running := s.conversation()
	release := running.Hold()
	s.Require().NoError(running.Close())

	release()

	s.False(graceCounting(running), "a release started the grace of a session that has ended")
}

// The grace is a conversation's: a session that keeps none, and one on a call, outlive their last
// watcher, as before holds were counted.
func (s *SessionSuite) TestOnlyAConversationInWritingIsEndedByItsGrace() {
	previous := s.grace
	s.T().Cleanup(func() { s.grace = previous })
	s.grace = holdGrace
	s.persists()
	unkept := s.writes(Spec{Caller: routing.Caller{UserID: "employee-1"}})
	s.T().Cleanup(func() { _ = unkept.Close() })
	onACall := s.commands()
	s.Require().NoError(onACall.StartVoice(s.ctx, Call{}))

	for _, running := range []*Session{unkept, onACall} {
		_, detach := running.Watch()
		detach()
		s.False(graceCounting(running), "the last watcher leaving started a grace")
	}

	s.heldOpen(unkept, "a session that keeps no conversation was ended by a grace")
	s.heldOpen(onACall, "a conversation on a call was ended by a grace")
}
