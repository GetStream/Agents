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

		s.talks(5)

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
			s.talks(20)
			s.Zero(s.opened.Load())
			gate.Refuse(nil)
			s.talks(1)
			s.EqualValues(1, s.opened.Load(), "heard within a second of the grant")
			s.opened.Store(0)
			s.ears = newStubSTT()
			s.dropsListener()
		}
		s.Len(s.hearingFailures(), 3, "one for each run of refusals")
		s.False(s.callEnded())
	})
}

// dropsListener ends alice's transcriber, as a vendor that cuts it does.
func (s *AgentSuite) dropsListener() {
	s.agent.mu.Lock()
	listener := s.agent.listeners["alice"]
	s.agent.mu.Unlock()
	s.Require().NotNil(listener)
	s.agent.dropListener("alice", listener)
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
		s.dropsListener()
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
		s.talks(49)
		s.dropsListener()
		s.edge.attending <- Attendance{Participant: stt.Participant{ID: "alice"}}
		synctest.Wait()
		s.edge.attending <- Attendance{Participant: stt.Participant{ID: "alice"}, Joined: true}
		synctest.Wait()

		s.talks(29)

		s.False(s.callEnded())
	})
}

func (s *AgentSuite) TestAFailureThatIsNotAboutMoneyIsNotOne() {
	s.False(refusedMoney(nil))
	s.False(refusedMoney(errors.New("the vendor is down")))
}
