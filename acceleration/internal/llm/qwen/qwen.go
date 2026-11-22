// Package qwen reaches Alibaba's own Qwen models on QwenCloud's OpenAI-compatible endpoint.
//
// The open-weight Qwen models reach the router through the hosts in openweights; this is
// Alibaba's API, which serves the closed ones such as Qwen4-Max. Its keys are the same
// DashScope keys the qwen speech-to-speech provider takes.
package qwen

import (
	"errors"
	"log/slog"
	"os"

	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/openaicompat"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ProviderName is the stable name used in routing config and stats.
const ProviderName = "qwen"

// apiKeyEnvVar holds the credentials when Options does not.
const apiKeyEnvVar = "DASHSCOPE_API_KEY"

// baseURLEnvVar overrides the endpoint. QwenCloud keys belong to one region and are refused
// by every other region's endpoint, so a key made elsewhere comes with its region's URL.
const baseURLEnvVar = "QWEN_BASE_URL"

// defaultBaseURL is the Singapore (International) region, the only one with every model.
const defaultBaseURL = "https://ap-southeast-1.maas.qwencloudapi.com/compatible-mode/v1"

// defaultModel is used when the caller names no model.
const defaultModel = "qwen4-max"

// efforts are what Qwen4 accepts once thinking is on. It maps OpenAI's other words onto
// these rather than refusing them, so a request naming one would not get what it asked for.
var efforts = []string{"low", "high", "xhigh"}

// Options configures the provider.
type Options struct {
	APIKey  string
	Model   string
	BaseURL string
	// Thinking lets the model reason before answering. Qwen4 thinks by default, at high
	// effort with a 131k-token budget; it is turned off here unless asked for, because that
	// spends the latency before the first word of the answer on a live call.
	Thinking bool
	// ReasoningEffort tunes how long the model thinks when Thinking is on. Empty leaves
	// the model's own default.
	ReasoningEffort string
	Logger          *slog.Logger
}

// New builds the provider, reading the API key from the environment when it is not given.
func New(options Options) (*openaicompat.LLM, error) {
	if options.APIKey == "" {
		options.APIKey = keypool.Getenv(apiKeyEnvVar)
	}
	if options.APIKey == "" {
		return nil, stack.Wrap(errors.New("qwen: " + apiKeyEnvVar + " is required"))
	}
	if options.Model == "" {
		options.Model = defaultModel
	}
	if options.BaseURL == "" {
		options.BaseURL = os.Getenv(baseURLEnvVar)
	}
	if options.BaseURL == "" {
		options.BaseURL = defaultBaseURL
	}

	model := llm.Capabilities{
		StreamsReasoning: options.Thinking,
		InputModalities:  []string{llm.ModalityImage},
		JSONSchema:       true,
	}
	if options.Thinking {
		model.ReasoningEfforts = efforts
		model.DefaultEffort = options.ReasoningEffort
	}

	return openaicompat.New(openaicompat.Options{
		Provider:      ProviderName,
		Model:         options.Model,
		APIKey:        options.APIKey,
		BaseURL:       options.BaseURL,
		Capabilities:  model,
		RequestFields: requestFields(options.Thinking),
		Logger:        options.Logger,
	})
}

// requestFields sends the thinking switch on every request, since leaving it out turns
// thinking on.
func requestFields(thinking bool) func(llm.ResponseParams, string) map[string]any {
	return func(_ llm.ResponseParams, effort string) map[string]any {
		fields := map[string]any{"enable_thinking": thinking}
		if effort != "" {
			fields["reasoning_effort"] = effort
		}
		return fields
	}
}
