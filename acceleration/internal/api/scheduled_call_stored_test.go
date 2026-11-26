//go:build integration

package api

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/scheduling"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// errUntranscribed is why a call of a test that did not ask for a transcript writes none, which
// the session manager takes as a call with no transcript.
var errUntranscribed = errors.New("this test's calls write no transcript")

// callTranscript is the transcript a call writes once the test set transcribing: one that keeps
// nothing, so a call's session gives the conversation only what cmd/router's does.
func (s *TelnyxUnpaidSuite) callTranscript(context.Context, session.Spec, streamapp.Bound, *slog.Logger) (session.Transcript, error) {
	if !s.transcribing.Load() {
		return nil, errUntranscribed
	}
	return keptNowhere{}, nil
}

// refusedOnAnswer opens a call's session as carryOutOnCall does, with a transcript as cmd/router's
// calls have, has the organization's credit run out, and has the person pick up. The test hands
// carryOutAnswered the events it watches.
func (s *TelnyxUnpaidSuite) refusedOnAnswer() (ringing, *session.Session) {
	s.transcribing.Store(true)
	call := s.callOn("noted/noted-model")
	created, err := s.router.sessions.Create(s.ownerContext(context.Background(), call.delivery), call.spec)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _, _ = s.router.sessions.Close(created.ID(), session.OwnerOf(created.Spec())) })
	s.spent(call)
	call.edge.pickUp(call.writer)
	s.Require().Eventually(created.CallerJoined, deliveryWait, 20*time.Millisecond, "nobody picked up")
	return call, created
}

// carriedOutWith is what carrying the task out on the answered call created comes to, read
// from events.
func (s *TelnyxUnpaidSuite) carriedOutWith(call ringing, created *session.Session, events <-chan session.Event) error {
	ctx, cancel := context.WithTimeout(s.ownerContext(context.Background(), call.delivery), deliveryWait)
	defer cancel()
	return s.router.carryOutAnswered(ctx, created, call.delivery, events)
}

// spent has the organization's credit run out after call was placed.
func (s *TelnyxUnpaidSuite) spent(call ringing) {
	s.usage.Credit(s.T(), s.RouterSuite.app.organization.ID, -10_000_000)
	s.refusedForMoney(call.writer)
}

// failedUnderTheMoney checks that err is a call's turn refused because the credit ran out.
func (s *TelnyxUnpaidSuite) failedUnderTheMoney(err error) {
	var failure *scheduling.Failure
	s.Require().ErrorAs(err, &failure)
	s.Equal(codeInsufficientCredits, failure.Reason)
	s.True(refusedTurn(err), "the walk over the channels ends at the call")
}

// A phone task whose answered call's turn is refused for money, with the refusal lost by the
// watch on the call, as a watcher that has fallen behind loses events, fails under the money all
// the same: the refusal is read from the conversation, not from the event (AI-3904). The watch
// still has every other event of the turn, its answer among them.
func (s *TelnyxUnpaidSuite) TestACallWhoseRefusalItsWatchLostFailsUnderTheMoney() {
	call, created := s.refusedOnAnswer()
	events, detach := created.Watch()
	defer detach()
	lost := make(chan session.Event, 256)
	go func() {
		defer close(lost)
		for event := range events {
			if _, failed := event.(agent.Error); !failed {
				lost <- event
			}
		}
	}()

	s.failedUnderTheMoney(s.carriedOutWith(call, created, lost))
}

// A phone task whose answered call's turn is refused for money while its watch gets no event at
// all fails under the money: the conversation is read on each tick too, so a watch that is never
// sent another event still ends the call (AI-3904).
func (s *TelnyxUnpaidSuite) TestACallWhoseWatchGetsNoEventFailsUnderTheMoney() {
	call, created := s.refusedOnAnswer()

	s.failedUnderTheMoney(s.carriedOutWith(call, created, make(chan session.Event)))
}

// A phone task on a transcribed call, as cmd/router's are, whose answered turn is refused for
// money fails under the refusal's code, final, and the agent hangs up: a call's transcript takes
// every event of what is said, and the refusal, which names no turn, still ends the task's
// typed turn in the conversation (AI-3904).
func (s *TelnyxUnpaidSuite) TestATranscribedCallWhoseTurnIsRefusedForMoneyFailsUnderTheMoney() {
	s.transcribing.Store(true)
	call := s.ringingOn("noted/noted-model")
	s.spent(call)

	call.edge.pickUp(call.writer)
	outcome := s.letGo(call)

	s.failedUnderTheMoney(outcome.err)
	var failure *scheduling.Failure
	s.Require().ErrorAs(outcome.err, &failure)
	s.True(failure.Final, "spent credit outlasts every retry of the run")
	_, live := s.router.sessions.Get(call.spec.ID, session.OwnerOf(call.spec))
	s.False(live, "the agent hung up")
	kept, err := s.store.ScheduledTaskRuns.Get(context.Background(), s.customerID(), call.task.ID, call.run.ID)
	s.Require().NoError(err)
	s.Require().NotNil(kept.Error)
	s.Equal(codeInsufficientCredits, *kept.Error)
}

// A phone task on a transcribed call whose turn the model answers, or fails for a reason that is
// not money, holds the call until the user hangs up and is delivered by phone (control: the same
// on the base).
func (s *TelnyxUnpaidSuite) TestATranscribedCallWhoseTurnIsNotRefusedForMoneyIsDelivered() {
	for _, model := range []string{"noted/noted-model", "failing/failing-model"} {
		s.Run(model, func() {
			s.transcribing.Store(true)
			call := s.ringingOn(model)

			call.edge.pickUp(call.writer)
			s.answeredOn(call)
			s.held(call)
			s.hangUp(call)
			outcome := s.letGo(call)

			s.Require().NoError(outcome.err)
			s.Equal(store.ScheduledTaskDeliveredPhone, outcome.deliveredTo)
		})
	}
}
