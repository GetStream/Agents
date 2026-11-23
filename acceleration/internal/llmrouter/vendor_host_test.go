package llmrouter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

// decoratedEndpoint is what an operator may paste as an endpoint: the vendor's own address with
// credentials, a path and a query on it. Only its host belongs in a log line.
func decoratedEndpoint(t *testing.T, vendor *httptest.Server) (endpoint, host string) {
	t.Helper()
	address, err := url.Parse(vendor.URL)
	require.NoError(t, err)
	address.User = url.UserPassword("operator", "hunter2")
	address.Path = "/gateway/v1"
	address.RawQuery = "token=hunter2"
	return address.String(), address.Host
}

// callThrough builds a provider the way a router does, from the registry and an instance
// endpoint, makes one call through a session and returns the line the call logged.
func callThrough(t *testing.T, ctx context.Context, name, endpoint string) loggedTiming {
	t.Helper()
	logs := logTimings(t)
	provider, err := DefaultRegistry().Build(name, routing.Spec{Model: "some-model", APIKey: "sk-test", BaseURL: endpoint})
	require.NoError(t, err)
	session := newSession(provider, routing.ProviderConfig{Provider: name, Model: "some-model"},
		routing.Owner{CustomerID: "acme"}, nil, nil)
	session.recorder = &statSink{}
	t.Cleanup(func() { _ = session.Close() })
	stream, err := session.Create(ctx, llm.ResponseParams{ID: "r1", Input: prompt()})
	if err == nil {
		drain(stream)
	}
	return timingLogged(t, logs)
}

// refusingVendor answers every request with a 400, which no SDK retries.
func refusingVendor(t *testing.T) *httptest.Server {
	t.Helper()
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":{"message":"refused"}}`, http.StatusBadRequest)
	}))
	t.Cleanup(vendor.Close)
	return vendor
}

func TestTheTimingLineNamesTheHostOfAnInstanceEndpoint(t *testing.T) {
	// openai and anthropic are the native adapters; deepseek is built on openaicompat and
	// named its host before, so its line is the control.
	for _, name := range []string{"openai", "anthropic", "deepseek"} {
		t.Run(name, func(t *testing.T) {
			endpoint, host := decoratedEndpoint(t, refusingVendor(t))
			logged := callThrough(t, context.Background(), name, endpoint)
			require.Equal(t, host, logged.VendorHost, "the host and port alone: no credentials, path or query")
		})
	}
}

func TestTheTimingLineNamesTheHostOfTheDefaultEndpoint(t *testing.T) {
	// The caller has gone before the request leaves, so the call reaches no vendor.
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	for name, host := range map[string]string{
		"openai":    "api.openai.com",
		"anthropic": "api.anthropic.com",
		"deepseek":  "inference.baseten.co",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("DEEPSEEK_BASE_URL", "")
			t.Setenv("ANTHROPIC_OPENAI_COMPAT", "")
			require.Equal(t, host, callThrough(t, gone, name, "").VendorHost)
		})
	}
}

func TestTheTimingLineNamesTheHostOfAnAnthropicCompatEndpoint(t *testing.T) {
	t.Setenv("ANTHROPIC_OPENAI_COMPAT", "true")
	endpoint, host := decoratedEndpoint(t, refusingVendor(t))
	require.Equal(t, host, callThrough(t, context.Background(), "anthropic", endpoint).VendorHost)
}
