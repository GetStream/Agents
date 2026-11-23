// Package custom reaches a model a customer serves themselves: a fine-tune on a host of
// open weights, or vLLM or SGLang on their own GPUs. Each speaks OpenAI's chat completions,
// so this is openaicompat pointed at the customer's endpoint.
//
// The weights are usually one of the open families the hosts serve, under a name of the
// customer's choosing, so the family is still read out of the model id: a fine-tune of Qwen
// is told not to think the way Qwen is.
package custom

import (
	"cmp"
	"errors"
	"log/slog"
	"time"

	"github.com/openai/openai-go/v3/option"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/openaicompat"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/openweights"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ProviderName is what a customer's own model is routed and recorded under.
const ProviderName = "custom"

// firstTokenTimeout is how long a customer's endpoint may say nothing before the turn fails
// and goes to the next candidate. It is generous because the weights may run on a CPU, where
// a 7B model takes tens of seconds to read an agent's few thousand tokens of instructions and
// tools before its first word; past a minute the endpoint is hung rather than slow, and
// waiting out the whole two minute response timeout only adds silence.
const firstTokenTimeout = 60 * time.Second

// defaultMaxTokens caps a reply when the caller set no cap. llama.cpp's server, started with
// its defaults, otherwise generates until the context is full. It is far more than any reply
// to a caller or chat message, and small enough to fit beside a long prompt in an 8K context,
// which matters because vLLM refuses a request whose prompt and cap together exceed the
// model's length.
const defaultMaxTokens = 2048

// Options configures one of a customer's models.
type Options struct {
	// Name is what the customer calls the model, which is what stats record.
	Name string
	// Model is the id the endpoint serves the weights under.
	Model string
	// BaseURL is the endpoint root, up to and including /v1.
	BaseURL string
	// APIKey is empty for an endpoint that takes none, which is then sent no Authorization
	// header at all.
	APIKey     string
	HTTPClient option.HTTPClient
	Logger     *slog.Logger
}

// New builds the provider. It performs no network access.
func New(options Options) (*openaicompat.LLM, error) {
	if options.Model == "" || options.BaseURL == "" {
		return nil, stack.Wrap(errors.New("custom: a model and a base url are required"))
	}

	return openaicompat.New(openaicompat.Options{
		Provider:     ProviderName,
		Model:        options.Model,
		StatsModel:   options.Name,
		APIKey:       options.APIKey,
		Keyless:      true,
		BaseURL:      options.BaseURL,
		Capabilities: openweights.Capabilities(options.Model, false, ""),
		// The endpoint renders whatever chat template the weights came with, and Mistral's
		// refuse anything but one system message first and turns that alternate.
		StrictRoles:       true,
		FirstTokenTimeout: firstTokenTimeout,
		RequestFields:     requestFields(options.Model),
		HTTPClient:        options.HTTPClient,
		Logger:            options.Logger,
	})
}

// requestFields adds the reply's cap to the family's thinking switch. It goes as max_tokens,
// the field vLLM, llama.cpp, SGLang and Ollama all read: Ollama ignores the newer
// max_completion_tokens, which is all a capped request sends otherwise.
//
// The thinking switch, chat_template_kwargs for Qwen, is only sent to a family that has one,
// and a template that does not read it ignores it: Qwen 2.5's renders the same either way,
// and Qwen 3's would think before every answer without it.
func requestFields(model string) func(llm.ResponseParams, string) map[string]any {
	thinking := openweights.RequestFields(model, false)
	return func(params llm.ResponseParams, effort string) map[string]any {
		fields := map[string]any{}
		if thinking != nil {
			fields = thinking(params, effort)
		}
		fields["max_tokens"] = cmp.Or(params.MaxOutputTokens, defaultMaxTokens)
		return fields
	}
}
