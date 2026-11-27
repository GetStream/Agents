//go:build integration

package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
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

// onCalls runs test on a call that writes a transcript, as cmd/router's calls do, and on one that
// writes none, as a call whose transcript failed to open.
func (s *TelnyxUnpaidSuite) onCalls(test func()) {
	for _, transcribed := range []bool{true, false} {
		s.Run(fmt.Sprintf("transcribed=%t", transcribed), func() {
			s.transcribing.Store(transcribed)
			test()
		})
	}
}

// refusedOnAnswer opens a call's session as carryOutOnCall does, has the organization's credit
// run out, and has the person pick up. The test hands carryOutAnswered the events it watches.
func (s *TelnyxUnpaidSuite) refusedOnAnswer() (ringing, *session.Session) {
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
// the same: the refusal is read from the session, not from the event (AI-3904). The watch still
// has every other event of the turn, its answer among them.
func (s *TelnyxUnpaidSuite) TestACallWhoseRefusalItsWatchLostFailsUnderTheMoney() {
	s.onCalls(func() {
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
	})
}

// A phone task whose answered call's turn is refused for money while its watch gets no event at
// all fails under the money: the session is read on each tick too, so a watch that is never
// sent another event still ends the call (AI-3904).
func (s *TelnyxUnpaidSuite) TestACallWhoseWatchGetsNoEventFailsUnderTheMoney() {
	s.onCalls(func() {
		call, created := s.refusedOnAnswer()

		s.failedUnderTheMoney(s.carriedOutWith(call, created, make(chan session.Event)))
	})
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

// A phone task whose call's turn the model answers, and whose later turn on the call the agent is
// refused for money as the credit ran out meanwhile, said the task: the call is held until the
// user hangs up and is delivered by phone, with a transcript or without one, where the refusal
// reaches the conversation (AI-3904).
func (s *TelnyxUnpaidSuite) TestACallWhoseLaterTurnIsRefusedForMoneyIsDelivered() {
	s.onCalls(func() {
		call := s.ringingOn("noted/noted-model")
		call.edge.pickUp(call.writer)
		s.answeredOn(call)
		held, live := s.router.sessions.Get(call.spec.ID, session.OwnerOf(call.spec))
		s.Require().True(live)
		events, detach := held.Watch()
		defer detach()
		s.spent(call)

		_, _ = held.Respond(context.Background(), "and what else", nil, options.LLM{})
		s.Require().True(sawMoneyRefusal(events), "the later turn was never refused for money")
		s.held(call)
		s.hangUp(call)
		outcome := s.letGo(call)

		s.Require().NoError(outcome.err)
		s.Equal(store.ScheduledTaskDeliveredPhone, outcome.deliveredTo)
	})
}

// A phone task whose call's turn the caller cuts off before the agent answered, and whose next
// turn on the call is refused for money, said nothing to the user: the run fails under the money,
// final, and the agent hangs up without waiting for the user, as a refusal the agent got before
// it answered anything always ended the call (AI-3904).
func (s *TelnyxUnpaidSuite) TestACallWhoseTurnIsCutOffAndWhoseNextIsRefusedForMoneyFailsUnderTheMoney() {
	s.onCalls(func() {
		call := s.ringingOn("stuck/stuck-model")
		call.edge.pickUp(call.writer)
		s.Require().Eventually(func() bool { return s.callTurn(call) == "thinking" }, deliveryWait, 20*time.Millisecond,
			"the task's turn never started")
		held, live := s.router.sessions.Get(call.spec.ID, session.OwnerOf(call.spec))
		s.Require().True(live)
		events, detach := held.Watch()
		defer detach()
		held.Interrupt()
		s.Require().Eventually(func() bool { return s.callTurn(call) == "cancelled" }, deliveryWait, 20*time.Millisecond,
			"the task's turn was never cut off")
		s.spent(call)

		_, _ = held.Respond(context.Background(), "hello? who is this?", nil, options.LLM{})
		s.Require().True(sawMoneyRefusal(events), "the next turn was never refused for money")
		outcome := s.letGo(call)

		s.failedUnderTheMoney(outcome.err)
		var failure *scheduling.Failure
		s.Require().ErrorAs(outcome.err, &failure)
		s.True(failure.Final, "spent credit outlasts every retry of the run")
		_, live = s.router.sessions.Get(call.spec.ID, session.OwnerOf(call.spec))
		s.False(live, "the agent hung up")
	})
}

// sawMoneyRefusal waits up to deliveryWait for the agent to fail on a refusal for money.
func sawMoneyRefusal(events <-chan session.Event) bool {
	deadline := time.After(deliveryWait)
	for {
		select {
		case event, open := <-events:
			if !open {
				return false
			}
			if failed, ok := event.(agent.Error); ok && policy.Code(failed.Err) != "" {
				return true
			}
		case <-deadline:
			return false
		}
	}
}
