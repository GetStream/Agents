//go:build integration

package api

import (
	"context"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// A scheduled delivery holds the session it carries its turn in (Session.Hold), so a session
// nobody watches is not ended by its grace in the middle of the turn, and the grace starts again
// once the delivery ends.

// ThreadHoldSuite runs tasks made in an SMS thread in sessions whose grace is holdingGrace.
type ThreadHoldSuite struct {
	telnyxLine
}

func TestThreadHoldSuite(t *testing.T) {
	runSuite(t, new(ThreadHoldSuite))
}

// holdingGrace is how long a session of the suite outlives its last watcher: long enough for a
// delivery's writes to land, short enough to end many times over while a turn is held.
const holdingGrace = 200 * time.Millisecond

// SetupSuite gives the suite a database of its own, as TelnyxChannelSuite has: a scheduled task's
// worker claims every task due in its database.
func (s *ThreadHoldSuite) SetupSuite() {
	s.database = "thread_hold"
	s.detachedGrace = holdingGrace
	s.telnyxLine.SetupSuite()
}

// keptOnThread is the session the customer's backend opened on channel, with a person watching
// it, and the way they leave.
func (s *ThreadHoldSuite) keptOnThread(channel string) (*session.Session, func()) {
	held := s.serverClient.createSession(CreateSessionRequest{Agent: &s.config.Name, AgentId: &channel})
	found, running := s.router.sessions.ByConversationWhere("agent:"+channel, nil)
	s.Require().True(running)
	s.Require().Equal(held.Id, found.ID())
	_, leave := found.Watch()
	return found, leave
}

// openThrough fails the test if found ends within ten graces.
func (s *ThreadHoldSuite) openThrough(found *session.Session, message string) {
	s.Never(func() bool { return found.State() == session.Ended }, 10*holdingGrace, 10*time.Millisecond, message)
}

// endsAfter waits for found to end, which its grace does once nobody watches or holds it, and
// fails the test if it ended sooner than a grace after since.
func (s *ThreadHoldSuite) endsAfter(found *session.Session, since time.Time) {
	s.Require().Eventually(func() bool { return found.State() == session.Ended }, deliveryWait, 10*time.Millisecond,
		"nobody watches the session, so its grace should end it")
	s.GreaterOrEqual(time.Since(since), holdingGrace, "the grace started before the delivery ended")
}

// The person watching the session the backend keeps on the thread leaves while a task's turn
// runs in it. The session stays open for the turn, the reply is texted, and the grace that ends
// the session starts once the delivery is over.
func (s *ThreadHoldSuite) TestATaskKeepsTheSessionItFoundUntilItsReplyIsTexted() {
	person := "+13125550001"
	task, channel := s.scheduledInThread(person)
	found, leave := s.keptOnThread(channel)
	sent := len(s.telnyx().sent())
	release := s.schedulingScript.holdScheduled()
	defer release()
	type delivered struct {
		to, session string
		err         error
	}
	done := make(chan delivered, 1)
	go func() {
		to, sessionID, err := s.router.DeliverScheduledTask(context.Background(),
			store.ClaimedScheduledTask{Task: task, Run: s.manualRun(task)})
		done <- delivered{to, sessionID, err}
	}()
	s.Require().Eventually(func() bool { return s.scheduledTurns() > 0 }, deliveryWait, 10*time.Millisecond, "the task's turn never started")

	leave()

	s.openThrough(found, "the session ended in the middle of the task's turn")
	answered := time.Now()
	release()
	select {
	case got := <-done:
		s.Require().NoError(got.err)
		s.Equal("sms", got.to)
		s.Equal(found.ID(), got.session, "the task ran in the session the backend keeps")
	case <-time.After(deliveryWait):
		s.FailNow("the task was not delivered once its turn was let go of")
	}
	s.Equal(telnyxMessage{from: s.line, to: person, text: "Done."}, s.took(sent + 1)[sent].withoutAuthorization())
	s.endsAfter(found, answered)
}

// A person who watches the kept session through the delivery still keeps it open after it: the
// delivery's release starts no grace while somebody watches.
func (s *ThreadHoldSuite) TestATaskLeavesASessionSomebodyWatchesOpen() {
	task, channel := s.scheduledInThread("+13125550001")
	found, leave := s.keptOnThread(channel)

	to, sessionID, err := s.router.DeliverScheduledTask(context.Background(),
		store.ClaimedScheduledTask{Task: task, Run: s.manualRun(task)})

	s.Require().NoError(err)
	s.Equal("sms", to)
	s.Equal(found.ID(), sessionID)
	s.openThrough(found, "the delivery ended a session somebody watches")
	left := time.Now()
	leave()
	s.endsAfter(found, left)
}

// A task texted from the session it finds (captureReply), as one made in the app and delivered by
// SMS is, keeps that session open while its turn runs, though the person watching it leaves.
func (s *ThreadHoldSuite) TestATextedReplyKeepsTheSessionItRunsInUntilItIsRead() {
	task, channel := s.scheduledInThread("+13125550001")
	found, leave := s.keptOnThread(channel)
	run := s.manualRun(task)
	delivery := s.router.occurrenceDelivery(context.Background(), store.ClaimedScheduledTask{Task: task, Run: run})
	owned, err := s.router.scheduledOwnerContext(context.Background(), delivery)
	s.Require().NoError(err)
	release := s.schedulingScript.holdScheduled()
	defer release()
	type captured struct {
		text string
		err  error
	}
	done := make(chan captured, 1)
	go func() {
		text, _, err := s.router.captureReply(owned, found, delivery)
		done <- captured{text, err}
	}()
	s.Require().Eventually(func() bool { return s.scheduledTurns() > 0 }, deliveryWait, 10*time.Millisecond, "the task's turn never started")

	leave()

	s.openThrough(found, "the session ended in the middle of the task's turn")
	answered := time.Now()
	release()
	select {
	case got := <-done:
		s.Require().NoError(got.err)
		s.Equal("Done.", got.text)
	case <-time.After(deliveryWait):
		s.FailNow("the reply was not read once its turn was let go of")
	}
	s.endsAfter(found, answered)
}
