//go:build integration

package agent

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sttrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/ttsrouter"
)

// AgentMoneySuite covers a call whose organization's money is refused while it goes on, its
// transcriber and voice reserving through the real enforcer.
type AgentMoneySuite struct {
	suite.Suite
	// agents is the agent fixture, not embedded so its own tests are not run again here.
	agents *AgentSuite
	money  *policytest.Money
	owner  routing.Owner
}

func TestAgentMoneySuite(t *testing.T) {
	suite.Run(t, new(AgentMoneySuite))
}

func (s *AgentMoneySuite) SetupTest() {
	s.agents = new(AgentSuite)
	s.agents.SetT(s.T())
	s.agents.SetupTest()
	s.money = policytest.Start(s.T(), "agent_money")
	s.owner = s.money.Owner(s.T(), 20_000_000)
	s.agents.owner = s.owner
}

// spend holds that much of the organization's credit.
func (s *AgentMoneySuite) spend(micros int64) {
	s.Require().NoError(s.money.Enforcer.Reserve(s.owner, uuid.Must(uuid.NewV7()), micros, time.Minute))
}

// hearsPriced has the agent's transcriber cost money, and run opening as one is opened, after
// it was admitted.
func (s *AgentMoneySuite) hearsPriced(opening func()) {
	s.agents.hearingWith = func(hearing *sttrouter.Options) {
		hearing.Gate, hearing.Writer = s.money.Enforcer, s.money.Writer
		hearing.Config.Providers[0].Price = routing.Price{PerAudioHour: 1}
		registry := sttrouter.NewRegistry()
		registry.Register("stub", func(routing.Spec) (stt.STT, error) {
			s.agents.opened.Add(1)
			opening()
			return s.agents.ears, nil
		})
		hearing.Registry = registry
	}
}

// voicePriced has every character the agent says cost $1.
func (s *AgentMoneySuite) voicePriced() {
	s.agents.voicesWith = func(saying *ttsrouter.Options) {
		saying.Gate, saying.Writer = s.money.Enforcer, s.money.Writer
		for i := range saying.Config.Providers {
			saying.Config.Providers[i].Price = routing.Price{PerMillionChars: 1_000_000}
		}
	}
}

func (s *AgentMoneySuite) failures(context string) []error {
	var failures []error
	for _, event := range s.agents.reported() {
		if failure, ok := event.(Error); ok && failure.Context == context {
			failures = append(failures, failure.Err)
		}
	}
	return failures
}

func (s *AgentMoneySuite) TestACallWhoseMoneyCannotBeCheckedGoesOn() {
	redis, sharing := policytest.SharedHolds(s.T())
	s.money = policytest.Start(s.T(), "agent_money", sharing, func(settings *policy.Settings) { settings.FlushMaxLag = 200 * time.Millisecond })
	s.owner = s.money.Owner(s.T(), 20_000_000)
	s.agents.owner = s.owner
	s.hearsPriced(func() {})
	s.agents.join(true)
	caller := stt.Participant{ID: "alice"}
	s.agents.speak(caller)
	s.agents.eventually(func() bool { return len(s.agents.ears.transcribed()) == 1 }, "the caller was not heard")

	redis.Stop()
	s.spend(1_000)
	s.agents.eventually(func() bool { return errors.Is(s.money.Spend(s.T(), s.owner), policy.ErrSpendCheckUnavailable) }, "holds go unshared")
	s.agents.speak(stt.Participant{ID: "bob"})
	s.agents.eventually(func() bool { return len(s.failures("stt")) == 1 }, "bob's transcriber is refused until it can be checked")
	s.False(refusedMoney(s.failures("stt")[0]), "the money did not run out")
	s.agents.speak(caller)

	s.agents.eventually(func() bool { return len(s.agents.ears.transcribed()) == 2 }, "the caller is still heard")
	s.Zero(countOf[Left](s.agents.reported()), "the call goes on")
}

func (s *AgentMoneySuite) TestACallEndsOnceNobodyOnItCanBeHeard() {
	s.hearsPriced(func() {})
	s.agents.join(true)
	alice, bob := stt.Participant{ID: "alice"}, stt.Participant{ID: "bob"}
	s.agents.speak(alice)
	s.agents.eventually(func() bool { return len(s.agents.ears.transcribed()) == 1 }, "alice was not heard")
	s.spend(20_000_000)
	refused := time.Now()
	s.agents.setClock(refused)
	s.agents.speak(bob)
	s.agents.eventually(func() bool { return len(s.failures("stt")) == 1 }, "bob was not refused a transcriber")

	s.agents.setClock(refused.Add(31 * time.Second))
	s.agents.speak(bob)
	s.agents.speak(alice)
	s.agents.eventually(func() bool { return len(s.agents.ears.transcribed()) == 2 }, "alice is no longer heard")
	s.Zero(countOf[Left](s.agents.reported()), "the call goes on while alice is heard")

	s.agents.edge.attending <- Attendance{Participant: alice}
	s.agents.eventually(func() bool { return countOf[ParticipantLeft](s.agents.reported()) == 1 }, "alice did not leave")
	s.agents.setClock(refused.Add(33 * time.Second))
	s.agents.speak(bob)
	s.agents.eventually(func() bool { return countOf[Left](s.agents.reported()) == 1 }, "nobody can be heard, and the call went on")
	s.Len(s.failures("stt"), 1)
}

func (s *AgentMoneySuite) TestAReplyRefusedMoreCreditSaysWhatItWasSentAndEnds() {
	s.voicePriced()
	s.agents.join(true)
	// The first sentence holds $13 of $20, the second would take it past.
	s.agents.model.reply = []string{"Hello there. ", "How are you? ", "And the rest."}
	s.spend(10_000_000)

	_, err := s.agents.agent.RespondTo(s.agents.ctx, "hello", nil, options.LLM{})
	s.Require().NoError(err)

	s.agents.eventually(func() bool { return countOf[Spoke](s.agents.reported()) == 1 }, "the first sentence never settled")
	said := s.agents.voice.spoken()
	s.Require().Len(said, 2)
	s.Equal("Hello there.", strings.TrimSpace(said[0].Text))
	s.Equal(tts.Request{ID: said[0].ID, Final: true}, said[1], "the reply is ended with what it was sent")
	spoke, _ := firstOf[Spoke](s.agents.reported())
	s.Require().NotNil(spoke.Message, "what was said is still known")
	s.Equal(MessageSpan{Start: 0, End: len("Hello there.")}, *spoke.Message)
	s.agents.eventually(func() bool { return !s.agents.agent.Busy() }, "the agent is never left talking")
	failures := s.failures("tts")
	s.Require().Len(failures, 2, "each sentence refused is reported")
	s.ErrorIs(failures[0], policy.ErrNoCredits)
	s.Zero(countOf[Left](s.agents.reported()))
	s.agents.agent.mu.Lock()
	defer s.agents.agent.mu.Unlock()
	s.Empty(s.agents.agent.heard, "nothing is left to be heard")
}

func (s *AgentMoneySuite) TestAReplyTheNextVoiceIsRefusedCreditForIsNotLeftTalking() {
	s.voicePriced()
	s.agents.join(true)
	held := s.resaying()
	s.spend(10_000_000)

	s.agents.agent.sayAgain(s.agents.agent.voice(), held)

	said := s.agents.voice.spoken()
	s.Require().Len(said, 2)
	s.Equal(tts.Request{ID: "turn-1", Final: true}, said[1])
	s.agents.eventually(func() bool { return !s.agents.agent.Busy() }, "the agent is never left talking")
	s.Require().Len(s.failures("tts"), 1)
}

// resaying holds a reply of a streaming voice for that voice to say, as one that replaced a lost
// voice does: two texts of one utterance and its end.
func (s *AgentMoneySuite) resaying() *heldReply {
	s.agents.agent.mu.Lock()
	defer s.agents.agent.mu.Unlock()
	s.agents.agent.speakingTurn, s.agents.agent.utterances = "turn-1", 1
	s.agents.agent.held = &heldReply{turnID: "turn-1", written: true, lostAt: time.Now(), unsaid: []unsaidUtterance{
		{synthesisID: "turn-1", text: "Hello there.", message: MessageSpan{End: 12}, counts: true, counted: true},
		{synthesisID: "turn-1", text: "How are you?", message: MessageSpan{Start: 13, End: 25}},
		{synthesisID: "turn-1", final: true},
	}}
	return s.agents.agent.held
}

func (s *AgentMoneySuite) TestAReplyTheNextVoiceIsRefusedAllOfIsNotEndedInIt() {
	s.voicePriced()
	s.agents.join(true)
	held := s.resaying()
	s.spend(20_000_000)

	s.agents.agent.sayAgain(s.agents.agent.voice(), held)

	s.Empty(s.agents.voice.spoken(), "there is nothing in it to end")
	s.False(s.agents.agent.Busy())
}

func (s *AgentMoneySuite) TestAReplyStillBeingWrittenIsEndedOnceByTheReplyItself() {
	s.voicePriced()
	s.agents.join(true)
	held := s.resaying()
	s.agents.agent.mu.Lock()
	held.unsaid[2] = unsaidUtterance{synthesisID: "turn-1", text: "And the rest.", message: MessageSpan{Start: 26, End: 39}}
	held.written = false
	s.agents.agent.mu.Unlock()
	s.spend(10_000_000)

	s.agents.agent.sayAgain(s.agents.agent.voice(), held)

	s.Len(s.agents.voice.spoken(), 1, "the reply ends it when it is written")
}

func (s *AgentMoneySuite) TestAReplyTheNextVoiceFailsIsNotSentMore() {
	s.agents.join(true)
	held := s.resaying()
	taken, release := make(chan struct{}), make(chan struct{})
	s.agents.voice.mu.Lock()
	s.agents.voice.synthesizing, s.agents.voice.synthesisRelease = taken, release
	s.agents.voice.mu.Unlock()
	said := make(chan struct{})
	go func() {
		defer close(said)
		s.agents.agent.sayAgain(s.agents.agent.voice(), held)
	}()
	<-taken
	s.voiceRefuses(errors.New("the voice dropped the connection"))
	close(release)
	<-said

	s.Len(s.agents.voice.spoken(), 2, "a voice that failed is asked for nothing more")
}

func (s *AgentMoneySuite) TestAReplyRefusedAllCreditLeavesNothingToBeHeard() {
	s.voicePriced()
	s.agents.join(false)
	s.spend(20_000_000)
	s.agents.model.reply = []string{"Hello there. "}

	_, err := s.agents.agent.RespondTo(s.agents.ctx, "hello", nil, options.LLM{})
	s.Require().NoError(err)

	s.agents.eventually(func() bool { return len(s.failures("tts")) == 1 }, "the refusal was not reported")
	s.agents.agent.mu.Lock()
	defer s.agents.agent.mu.Unlock()
	s.Empty(s.agents.agent.heard)
}

func (s *AgentMoneySuite) TestWhatAnUtteranceWasSentIsKeptWhenMoreIsRefused() {
	s.agents.join(true)
	s.agents.voice.silent = true
	voice := s.agents.agent.voice()
	s.Require().NoError(s.agents.agent.synthesize(voice, "turn-1", tts.Request{ID: "turn-1", Text: "Hello there."}, MessageSpan{End: 12}))
	s.voiceRefuses(policy.ErrNoCredits)
	more := tts.Request{ID: "turn-1", Text: "How are you?"}
	s.Require().ErrorIs(s.agents.agent.synthesize(voice, "turn-1", more, MessageSpan{Start: 13, End: 25}), policy.ErrNoCredits)

	s.agents.agent.mu.Lock()
	heard := s.agents.agent.heard["turn-1"]
	s.agents.agent.mu.Unlock()
	s.Require().NotNil(heard)
	s.Equal("Hello there.", heard.text)
	s.Equal(MessageSpan{End: 12}, heard.message)

	s.voiceRefuses(errors.New("the voice dropped the connection"))
	s.Require().Error(s.agents.agent.synthesize(voice, "turn-1", more, MessageSpan{Start: 13, End: 25}))
	s.agents.agent.mu.Lock()
	defer s.agents.agent.mu.Unlock()
	s.Empty(s.agents.agent.heard, "a voice that failed leaves nothing more to be heard")
}

func (s *AgentMoneySuite) voiceRefuses(err error) {
	s.agents.voice.mu.Lock()
	defer s.agents.voice.mu.Unlock()
	s.agents.voice.refuses = err
}
