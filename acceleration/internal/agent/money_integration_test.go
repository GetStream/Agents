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

func (s *AgentMoneySuite) TestACallWhoseTranscriberIsRefusedForMoneyEnds() {
	s.hearsPriced(func() { s.spend(20_000_000) })
	s.agents.join(true)
	caller := stt.Participant{ID: "alice"}

	for range 10 {
		s.agents.speak(caller)
	}

	s.agents.eventually(func() bool { return countOf[Left](s.agents.reported()) == 1 }, "the call never ended")
	failures := s.failures("stt")
	s.Require().Len(failures, 1, "why is reported once, not for each frame")
	s.ErrorIs(failures[0], policy.ErrNoCredits)
	s.Empty(s.agents.ears.transcribed(), "the vendor hears nothing")
	s.EqualValues(1, s.agents.opened.Load(), "no other transcriber is opened")
}

func (s *AgentMoneySuite) TestACallThatCannotPayForATranscriberEnds() {
	s.hearsPriced(func() {})
	s.agents.join(true)
	s.spend(20_000_000)

	for range 10 {
		s.agents.speak(stt.Participant{ID: "alice"})
	}

	s.agents.eventually(func() bool { return countOf[Left](s.agents.reported()) == 1 }, "the call never ended")
	s.Require().Len(s.failures("stt"), 1)
	s.Zero(s.agents.opened.Load())
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
	s.agents.speak(caller)

	s.agents.eventually(func() bool { return len(s.agents.ears.transcribed()) == 2 }, "the caller is still heard")
	s.Zero(countOf[Left](s.agents.reported()), "the call goes on")
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
	s.agents.replacement = newStubTTS(true)
	s.agents.join(true)
	lost := s.agents.agent.voice()
	s.agents.agent.mu.Lock()
	s.agents.agent.speakingTurn, s.agents.agent.utterances = "turn-1", 1
	held := &heldReply{turnID: "turn-1", written: true, lostAt: time.Now(), unsaid: []unsaidUtterance{
		{synthesisID: "turn-1", text: "Hello there.", message: MessageSpan{End: 12}, counts: true, counted: true},
		{synthesisID: "turn-1", text: "How are you?", message: MessageSpan{Start: 13, End: 25}},
		{synthesisID: "turn-1", final: true},
	}}
	s.agents.agent.held = held
	s.agents.agent.mu.Unlock()
	s.spend(10_000_000)

	s.agents.agent.sayAgain(lost, held)

	said := s.agents.voice.spoken()
	s.Require().Len(said, 2)
	s.Equal(tts.Request{ID: "turn-1", Final: true}, said[1])
	s.agents.eventually(func() bool { return !s.agents.agent.Busy() }, "the agent is never left talking")
	s.Require().Len(s.failures("tts"), 1)
}
