package agent

import (
	"errors"
	"log/slog"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sttrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/ttsrouter"
)

// unkeyedFlow is the flow controller's shortcut on a deployment that holds no key for the one
// hosted vendor it routes to: a self-hosted router whose only model is the customer's own.
const unkeyedFlow = "llm-flow"

// unkeyedReasoner routes the conversation to the stub and the flow controller to a vendor
// whose every session fails to open for want of a key, as a hosted one does with none set.
func (s *AgentSuite) unkeyedReasoner(logger *slog.Logger) *llmrouter.Router {
	reasoning := llmrouter.NewRegistry()
	reasoning.Register("stub", func(routing.Spec) (llmrouter.Provider, error) { return s.model, nil })
	reasoning.Register("hosted", func(routing.Spec) (llmrouter.Provider, error) {
		return nil, errors.New("HOSTED_API_KEY is required")
	})
	config := stubConfig()
	// Not realtime, so the conversation's shortcut never picks it: only the flow's does.
	config.Providers = append(config.Providers, routing.ProviderConfig{
		Provider: "hosted", Model: "flow-model", Languages: []string{"en"},
	})
	config.Aliases[unkeyedFlow] = routing.Alias{Only: []string{"hosted/flow-model"}}
	reasoner, err := llmrouter.New(llmrouter.Options{Config: config, Registry: reasoning, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(reasoner.Close)
	return reasoner
}

// A conversation held in writing never puts a turn to the flow controller, so a flow
// controller it could not open is no reason to refuse it.
func (s *AgentSuite) TestATextAgentOpensWithoutAFlowControllerItCouldNotOpen() {
	s.model = newStubLLM()
	s.model.reply = []string{"Hello there."}
	logger := slog.New(slog.DiscardHandler)

	agent, err := New(Options{
		Text: true, CustomerID: "acme", Instructions: "be brief",
		LLM: s.unkeyedReasoner(logger), LLMTarget: "en-low-latency", ControllerTarget: unkeyedFlow,
		Logger: logger,
	})
	s.Require().NoError(err)
	s.agent = agent
	s.events = collect(agent)
	s.closesAtEnd(agent)

	s.Require().NoError(agent.Join(s.ctx), "a text session does not need the flow controller's vendor")
	_, err = agent.RespondTo(s.ctx, "hello", nil, options.LLM{})
	s.Require().NoError(err)
	s.eventually(func() bool { return countOf[Responded](s.reported()) == 1 }, "the written turn was never answered")
}

// A voice agent does put turns to the flow controller, so one it cannot open still refuses
// the call when it starts, rather than leaving a caller nobody answers.
func (s *AgentSuite) TestAVoiceAgentIsRefusedAFlowControllerItCannotOpen() {
	s.model = newStubLLM()
	logger := slog.New(slog.DiscardHandler)

	transcription := sttrouter.NewRegistry()
	transcription.Register("stub", func(routing.Spec) (stt.STT, error) { return newStubSTT(), nil })
	transcriber, err := sttrouter.New(sttrouter.Options{Config: stubConfig(), Registry: transcription, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(transcriber.Close)
	speech := ttsrouter.NewRegistry()
	speech.Register("stub", func(routing.Spec) (tts.TTS, error) { return newStubTTS(true), nil })
	speaker, err := ttsrouter.New(ttsrouter.Options{Config: stubConfig(), Registry: speech, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(speaker.Close)

	agent, err := New(Options{
		Edge: newLoopbackEdge(), CustomerID: "acme", Instructions: "be brief",
		LLM: s.unkeyedReasoner(logger), LLMTarget: "en-low-latency", ControllerTarget: unkeyedFlow,
		STT: transcriber, STTTarget: "en-low-latency",
		TTS: speaker, TTSTarget: "en-low-latency",
		Logger: logger,
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = agent.Close() })

	err = agent.Join(s.ctx)
	s.Require().Error(err)
	s.ErrorContains(err, "agent: start flow controller")
	s.ErrorContains(err, "HOSTED_API_KEY is required")
}

// A text agent moved onto a flow controller it cannot open is not refused the move either.
func (s *AgentSuite) TestATextAgentMovedOntoAFlowControllerItCannotOpenIsNotRefused() {
	s.model = newStubLLM()
	s.model.reply = []string{"Hello there."}
	logger := slog.New(slog.DiscardHandler)

	agent, err := New(Options{
		Text: true, CustomerID: "acme", Instructions: "be brief",
		LLM: s.unkeyedReasoner(logger), LLMTarget: "en-low-latency",
		Logger: logger,
	})
	s.Require().NoError(err)
	s.agent = agent
	s.events = collect(agent)
	s.closesAtEnd(agent)
	s.Require().NoError(agent.Join(s.ctx))

	s.Require().NoError(agent.SetSettings(s.ctx, Settings{LLMTarget: "en-low-latency", ControllerTarget: unkeyedFlow}))
	_, err = agent.RespondTo(s.ctx, "hello", nil, options.LLM{})
	s.Require().NoError(err)
	s.eventually(func() bool { return countOf[Responded](s.reported()) == 1 }, "the written turn was never answered")
}
