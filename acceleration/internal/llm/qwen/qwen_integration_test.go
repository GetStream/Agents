//go:build integration

package qwen

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/llmsuite"
)

type QwenIntegrationSuite struct {
	llmsuite.Suite
}

func TestQwenIntegrationSuite(t *testing.T) {
	suite.Run(t, &QwenIntegrationSuite{Suite: llmsuite.Suite{
		New: func() llm.LLM {
			provider, err := New(Options{})
			require.NoError(t, err)
			return provider
		},
		Requires: []string{apiKeyEnvVar},
	}})
}

func (s *QwenIntegrationSuite) TestThinkingIsOffByDefaultSoTheAnswerArrivesFirst() {
	// Qwen4 thinks unless told not to. With a small budget, a thinking answer is spent
	// entirely on reasoning and never appears.
	complete, events := s.Ask(llm.ResponseParams{
		Input:           []llm.Message{{Role: llm.User, Content: "Say hello in five words."}},
		MaxOutputTokens: 64,
	})

	s.NotEmpty(complete.OutputText)
	s.Zero(complete.Usage.OutputTokensDetails.ReasoningTokens)
	for _, event := range events {
		_, thinking := event.(llm.ReasoningTextDelta)
		s.False(thinking, "a non-thinking request must not stream reasoning")
	}
}

func (s *QwenIntegrationSuite) TestThinkingStreamsReasoningWhenTurnedOn() {
	provider, err := New(Options{Thinking: true, ReasoningEffort: "low"})
	s.Require().NoError(err)

	complete, events := s.AskOn(provider, llm.ResponseParams{
		Input:           []llm.Message{{Role: llm.User, Content: "Is 91 prime? Answer yes or no."}},
		MaxOutputTokens: 2048,
	})

	var thinking strings.Builder
	for _, event := range events {
		if delta, ok := event.(llm.ReasoningTextDelta); ok {
			thinking.WriteString(delta.Delta)
		}
	}
	s.NotEmpty(thinking.String(), "a reasoning model should show its working")
	s.Contains(strings.ToLower(complete.OutputText), "no")
}

func (s *QwenIntegrationSuite) TestAnAnswerHeldToASchemaIsThatJSON() {
	complete, _ := s.Ask(llm.ResponseParams{
		Input: []llm.Message{{Role: llm.User, Content: "What is the capital of France?"}},
		Text: llm.TextParams{Format: llm.FormatJSONSchema, Schema: &llm.JSONSchema{
			Name: "capital",
			Schema: map[string]any{
				"type":                 "object",
				"properties":           map[string]any{"city": map[string]any{"type": "string"}},
				"required":             []string{"city"},
				"additionalProperties": false,
			},
		}},
	})

	s.JSONEq(`{"city":"Paris"}`, complete.OutputText)
}
