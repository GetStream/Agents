package agent

import (
	"context"
	"testing/synctest"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/audio"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// rosterlessEdge is a call that cannot say who is in it, which is what a simulation's
// loopback and an audio socket are.
type rosterlessEdge struct{ edge *loopbackEdge }

func (e rosterlessEdge) Join(ctx context.Context) error        { return e.edge.Join(ctx) }
func (e rosterlessEdge) Audio() <-chan InboundAudio            { return e.edge.Audio() }
func (e rosterlessEdge) PublishAudio(pcm audio.PcmData) error { return e.edge.PublishAudio(pcm) }
func (e rosterlessEdge) Leave() error                          { return e.edge.Leave() }

// greeted has the agent open the call with a statement, so the silence after it is the
// longer one (defaultIdleGap).
func (s *AgentSuite) greeted() {
	s.model.composes = []string{"Are you still there?"}
	s.Require().NoError(s.agent.Say(s.ctx, "Hello, this is the front desk."))
}

// checkedIn reports whether the agent has asked for a check-in after waiting for, with
// everything the wait set going settled.
func (s *AgentSuite) checkedIn(after time.Duration) bool {
	time.Sleep(after)
	synctest.Wait()
	return len(s.model.composeRequests()) > 0
}

func (s *AgentSuite) TestNobodyIsAskedWhetherTheyAreStillThereOnACallNobodyJoined() {
	s.onFakeClock(func() {
		s.join(true)
		s.greeted()

		s.False(s.checkedIn(5*time.Minute), "there is nobody on the call to ask")
	})
}

func (s *AgentSuite) TestSomebodyWhoJoinsLateHasTheWholeSilenceBeforeTheyAreAsked() {
	s.onFakeClock(func() {
		s.join(true)
		s.greeted()
		s.False(s.checkedIn(time.Minute))

		s.edge.attending <- Attendance{Participant: stt.Participant{ID: "alice"}, Joined: true}

		s.False(s.checkedIn(defaultIdleGap-time.Second),
			"the silence before somebody arrived is not theirs")
		s.True(s.checkedIn(2*time.Second), "a person quiet for the whole gap is asked")
	})
}

func (s *AgentSuite) TestTheSilenceStartsAgainWhenTheCallerSpeaksMidCountdown() {
	s.onFakeClock(func() {
		s.join(true)
		s.flow.reply = []string{`{"disposition":"ignore","floor":"continue"}`}
		alice := stt.Participant{ID: "alice"}
		s.edge.attending <- Attendance{Participant: alice, Joined: true}
		s.speak(alice)
		s.greeted()
		s.False(s.checkedIn(defaultIdleGap / 2))

		s.says(alice, "hmm, give me a second")

		s.False(s.checkedIn(defaultIdleGap/2+time.Second), "they spoke, so the silence is new")
		s.True(s.checkedIn(defaultIdleGap/2), "a gap of silence after their words is asked about")
	})
}

func (s *AgentSuite) TestTheSilenceIsCountedFromTheEndOfALongReplyStillPlaying() {
	s.onFakeClock(func() {
		s.join(true)
		s.voice.plays = 2 * defaultIdleGap
		s.edge.attending <- Attendance{Participant: stt.Participant{ID: "alice"}, Joined: true}
		s.greeted()

		s.False(s.checkedIn(2*defaultIdleGap+defaultIdleGap/2),
			"a caller still hearing the reply is not silent, and only half the gap has passed since it ended")
		s.True(s.checkedIn(defaultIdleGap/2+time.Second), "the gap after the reply ended is asked about")
	})
}

func (s *AgentSuite) TestOnACallThatCannotSayWhoIsInItTheCallerArrivesWithTheirAudio() {
	// A simulation's caller is on a loopback, which reports nobody arriving: the first of
	// their audio is how the agent knows they are there.
	s.onFakeClock(func() {
		s.edgeFactory = func(edge *loopbackEdge) Edge { return rosterlessEdge{edge: edge} }
		s.join(true)
		s.greeted()
		s.False(s.checkedIn(time.Minute), "nobody's audio has come in")

		s.speak(stt.Participant{ID: "caller"})

		s.False(s.checkedIn(defaultIdleGap - time.Second))
		s.True(s.checkedIn(2 * time.Second))
	})
}
