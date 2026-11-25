package agent

import (
	"context"
	"errors"
	"testing/synctest"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing/gatetest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sttrouter"
)

// hearsOn runs body on a fake clock, in a call whose transcriber costs money its gate answers for.
func (s *AgentSuite) hearsOn(body func(gate *gatetest.Answers)) {
	s.onFakeClock(func() {
		gate := &gatetest.Answers{}
		writer := gate.Writer()
		s.T().Cleanup(func() { writer.Close(context.Background()) })
		s.hearingWith = func(hearing *sttrouter.Options) {
			hearing.Gate, hearing.Writer = gate, writer
			hearing.Config.Providers[0].Price = routing.Price{PerAudioHour: 1}
		}
		defer func() { s.hearingWith = nil }()
		s.join(true)
		body(gate)
	})
}

// talks has alice speak for that long, a frame a second, the first at once.
func (s *AgentSuite) talks(seconds int) {
	for range seconds {
		s.speak(stt.Participant{ID: "alice"})
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
	}
}

// hearingFailures are the failures of the transcriber reported.
func (s *AgentSuite) hearingFailures() []error {
	var failures []error
	for _, event := range s.reported() {
		if failure, ok := event.(Error); ok && failure.Context == "stt" {
			failures = append(failures, failure.Err)
		}
	}
	return failures
}

func (s *AgentSuite) callEnded() bool { return countOf[Left](s.reported()) == 1 }

func (s *AgentSuite) TestACallGoesOnWhenItsTranscriberIsRefusedMoneyOnce() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(15)
		gate.Refuse(policy.ErrNoCredits)
		s.talks(30)
		gate.Refuse(nil)
		s.talks(30)
		gate.Refuse(policy.ErrNoCredits)
		s.talks(30)

		s.Len(s.ears.transcribed(), 105, "every frame is heard")
		s.Empty(s.hearingFailures())
		s.False(s.callEnded())
	})
}

func (s *AgentSuite) TestACallEndsWhenItsTranscriberIsRefusedMoneyTwiceInARow() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(15)
		gate.Refuse(policy.ErrNoCredits)
		s.talks(45)
		s.False(s.callEnded())
		heard := len(s.ears.transcribed())
		s.Equal(60, heard)

		for range 10 {
			s.speak(stt.Participant{ID: "alice"})
		}
		synctest.Wait()

		s.True(s.callEnded())
		failures := s.hearingFailures()
		s.Require().Len(failures, 1, "why is reported once, not for each frame")
		s.ErrorIs(failures[0], policy.ErrNoCredits)
		s.Len(s.ears.transcribed(), heard, "the vendor hears nothing more")
		s.EqualValues(1, s.opened.Load(), "no other transcriber is opened")
	})
}

func (s *AgentSuite) TestACallEndsAtOnceWhenItsTranscriberIsRefusedForASuspension() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(15)
		gate.Refuse(policy.ErrSuspended)
		s.talks(16)

		s.True(s.callEnded())
		failures := s.hearingFailures()
		s.Require().Len(failures, 1)
		s.ErrorIs(failures[0], policy.ErrSuspended)
	})
}

func (s *AgentSuite) TestACallGoesOnWhenATranscriberRefusedMoneyIsGrantedOneSoon() {
	s.hearsOn(func(gate *gatetest.Answers) {
		for range 3 {
			gate.Refuse(policy.ErrNoCredits)
			s.talks(18)
			s.Zero(s.opened.Load())
			gate.Refuse(nil)
			s.talks(1)
			s.EqualValues(1, s.opened.Load(), "heard within a second of the grant")
			s.opened.Store(0)
			s.vendorCuts()
		}
		s.Len(s.hearingFailures(), 3, "one for each run of refusals")
		s.False(s.callEnded())
	})
}

func (s *AgentSuite) TestATranscriberRefusedMoneyIsAskedForOnceASecond() {
	s.hearsOn(func(gate *gatetest.Answers) {
		gate.Refuse(policy.ErrNoCredits)
		before := gate.Admitted()
		for range 3 {
			for range 50 {
				s.speak(stt.Participant{ID: "alice"})
			}
			time.Sleep(time.Second)
			synctest.Wait()
		}

		s.Equal(3, gate.Admitted()-before)
	})
}

func (s *AgentSuite) TestARunOfRefusalsIsOverOnceATranscriberIsGranted() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(1)
		gate.Refuse(policy.ErrNoCredits)
		s.talks(40)
		s.vendorCuts()
		gate.Refuse(nil)
		s.talks(40)
		s.vendorCuts()
		gate.Refuse(policy.ErrNoCredits)
		s.talks(20)

		s.False(s.callEnded())
	})
}

func (s *AgentSuite) TestARunOfRefusalsIsCountedFromItsFirst() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(1)
		gate.Refuse(policy.ErrNoCredits)
		// Nobody speaks through three refusals, and the vendor cuts it.
		time.Sleep(90 * time.Second)
		synctest.Wait()
		s.vendorCuts()

		s.talks(1)

		s.True(s.callEnded())
	})
}

// vendorCuts has the vendor cut the transcriber of everyone heard, and the next one opened be a
// new one.
func (s *AgentSuite) vendorCuts() {
	cut := s.ears
	s.ears = newStubSTT()
	cut.emitter.Send(stt.Disconnected{Provider: "stub", Model: "stub-stt", Reason: "idle"})
	synctest.Wait()
}

func (s *AgentSuite) leavesCall(id string) {
	s.edge.attending <- Attendance{Participant: stt.Participant{ID: id}}
	synctest.Wait()
}

func (s *AgentSuite) joinsCall(id string) {
	s.edge.attending <- Attendance{Participant: stt.Participant{ID: id}, Joined: true}
	synctest.Wait()
}

func (s *AgentSuite) TestACallEndsWhenATranscriberIsRefusedMoneyForThirtySeconds() {
	s.hearsOn(func(gate *gatetest.Answers) {
		gate.Refuse(policy.ErrNoCredits)
		s.talks(30)
		s.False(s.callEnded())

		s.talks(1)

		s.True(s.callEnded())
		failures := s.hearingFailures()
		s.Require().Len(failures, 1, "why is reported once")
		s.ErrorIs(failures[0], policy.ErrNoCredits)
		s.Zero(s.opened.Load())
	})
}

func (s *AgentSuite) TestACallEndsAtOnceWhenASuspensionRefusesATranscriber() {
	s.hearsOn(func(gate *gatetest.Answers) {
		gate.Refuse(policy.ErrSuspended)
		s.talks(1)

		s.True(s.callEnded())
		s.Len(s.hearingFailures(), 1)
	})
}

func (s *AgentSuite) TestATranscriberOpenedAgainGoesOnFromTheRefusalsOfTheLast() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(1)
		gate.Refuse(policy.ErrNoCredits)
		// Its lease is refused at 30 s, and the vendor cuts it at 50 s.
		s.talks(49)
		s.vendorCuts()
		s.talks(9)
		s.False(s.callEnded())

		s.talks(2)

		s.True(s.callEnded(), "refused twice, 30 s apart")
	})
}

func (s *AgentSuite) TestATranscriberMoneyCouldNotBeCheckedForNeverEndsACall() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(1)
		gate.Refuse(gatetest.Unchecked{})
		s.talks(120)

		s.Len(s.ears.transcribed(), 121)
		s.False(s.callEnded())
	})
}

func (s *AgentSuite) TestAParticipantWhoLeftStartsTheCountAgain() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(1)
		gate.Refuse(policy.ErrNoCredits)
		// Refused once on its lease.
		s.talks(40)
		s.leavesCall("alice")
		s.joinsCall("alice")

		s.talks(29)

		s.Len(s.hearingFailures(), 1)
		s.False(s.callEnded())
	})
}

func (s *AgentSuite) TestAParticipantRefusedOnceBeforeALongAbsenceAndOnceAfterIsNotRefusedTwiceInARow() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(1)
		gate.Refuse(policy.ErrNoCredits)
		s.talks(40)
		s.leavesCall("alice")
		gate.Refuse(nil)
		time.Sleep(10 * time.Minute)
		s.joinsCall("alice")
		gate.Refuse(policy.ErrNoCredits)

		s.talks(1)

		s.Len(s.hearingFailures(), 1)
		s.False(s.callEnded())
	})
}

func (s *AgentSuite) TestAParticipantRefusedATranscriberBeforeTheyLeftAndOnceAfterIsNotRefusedTwiceInARow() {
	for _, absence := range []time.Duration{40 * time.Second, 10 * time.Minute} {
		s.hearsOn(func(gate *gatetest.Answers) {
			gate.Refuse(policy.ErrNoCredits)
			s.talks(2)
			s.leavesCall("alice")
			gate.Refuse(nil)
			time.Sleep(absence)
			s.joinsCall("alice")
			gate.Refuse(policy.ErrNoCredits)

			s.talks(1)

			s.Len(s.hearingFailures(), 2, "each run is reported")
			s.False(s.callEnded())
		})
	}
}

func (s *AgentSuite) TestARefusalThatComesAsAParticipantLeavesIsNotKept() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.leavesCall("alice")
		report, over := s.agent.refusedToHear("alice", policy.ErrNoCredits)
		s.False(report)
		s.False(over)
		s.joinsCall("alice")
		gate.Refuse(policy.ErrNoCredits)
		time.Sleep(40 * time.Second)

		s.talks(1)

		s.Len(s.hearingFailures(), 1)
		s.False(s.callEnded())
	})
}

func (s *AgentSuite) TestATranscriberThatEndsRefusedAsItsParticipantLeavesIsNotKept() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(1)
		gate.Refuse(policy.ErrNoCredits)
		s.talks(40)
		s.agent.mu.Lock()
		s.agent.departed["alice"] = time.Time{}
		listener := s.agent.listeners["alice"]
		s.agent.mu.Unlock()

		s.agent.keepRefusals("alice", listener)

		s.agent.mu.Lock()
		defer s.agent.mu.Unlock()
		s.Empty(s.agent.unheardCallers)
	})
}

func (s *AgentSuite) TestACallEndsWhenTheOnlyOtherTranscriberIsRefusedToo() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(1)
		gate.Refuse(policy.ErrNoCredits)
		// Alice is silent from here, and her lease is refused at 30 s.
		for range 28 {
			s.speak(stt.Participant{ID: "bob"})
			time.Sleep(time.Second)
			synctest.Wait()
		}
		s.False(s.callEnded())
		for range 4 {
			s.speak(stt.Participant{ID: "bob"})
			time.Sleep(time.Second)
			synctest.Wait()
		}

		s.True(s.callEnded())
	})
}

func (s *AgentSuite) TestRefusalsMinutesApartAreNotARow() {
	s.hearsOn(func(gate *gatetest.Answers) {
		gate.Refuse(policy.ErrNoCredits)
		s.talks(1)
		time.Sleep(10 * time.Minute)
		s.talks(1)

		s.Len(s.hearingFailures(), 2)
		s.False(s.callEnded())

		s.talks(29)
		s.False(s.callEnded())
		s.talks(1)
		s.True(s.callEnded(), "a row of its own, 30 s long")
	})
}

func (s *AgentSuite) TestRefusalsWithinTwoRenewalsAreARow() {
	s.hearsOn(func(gate *gatetest.Answers) {
		gate.Refuse(policy.ErrNoCredits)
		s.talks(1)
		time.Sleep(59 * time.Second)
		s.talks(1)

		s.True(s.callEnded())
	})
}

func (s *AgentSuite) TestAWriteThatFailsDoesNotEndACall() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.ears.mu.Lock()
		s.ears.writeErr = errors.New("the write failed")
		s.ears.mu.Unlock()
		s.talks(3)

		s.Len(s.hearingFailures(), 3)
		s.False(s.callEnded())
	})
}

func (s *AgentSuite) TestACallGoesOnWhileAnotherParticipantIsHeard() {
	s.hearsOn(func(gate *gatetest.Answers) {
		s.talks(5)
		gate.Refuse(policy.ErrSuspended)
		s.speak(stt.Participant{ID: "bob"})
		synctest.Wait()

		s.Len(s.hearingFailures(), 1, "bob is refused a transcriber")
		s.False(s.callEnded(), "alice is heard")

		// Her lease is refused at 30 s, and it ends her transcription and the call.
		s.talks(27)
		s.True(s.callEnded())
	})
}

func (s *AgentSuite) TestAFailureThatIsNotAboutMoneyIsNotOne() {
	s.False(refusedMoney(nil))
	s.False(refusedMoney(errors.New("the vendor is down")))
}
