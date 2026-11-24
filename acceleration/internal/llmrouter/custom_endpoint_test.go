package llmrouter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/openaicompat"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

// servedAt is a customer whose own model, own, is served at baseURL.
type servedAt struct{ baseURL string }

func (s servedAt) CustomModel(_ context.Context, _, name string) (routing.ProviderConfig, routing.Endpoint, error) {
	if name != "own" {
		return routing.ProviderConfig{}, routing.Endpoint{}, errors.New("routing: not one of this customer's models")
	}
	return routing.ProviderConfig{Provider: routing.CustomProvider, Model: name},
		routing.Endpoint{BaseURL: s.baseURL, Model: "acme/tuned-7b"}, nil
}

func TestACustomModelAnsweringWithAWebPageIsServedByTheNextEntry(t *testing.T) {
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html>not json</html>")
	}))
	t.Cleanup(page.Close)
	steady := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":"stop"}]}`+"\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(steady.Close)

	registry := DefaultRegistry()
	registry.Register("beta", func(spec routing.Spec) (Provider, error) {
		return Started(openaicompat.New(openaicompat.Options{Provider: "beta", Model: spec.Model, APIKey: "key", BaseURL: steady.URL}))
	})
	config := routing.ModalityConfig{Providers: []routing.ProviderConfig{
		{Provider: "beta", Model: "steady", Languages: []string{"en"}},
	}}
	router, err := New(Options{Config: config, Registry: registry, Models: servedAt{baseURL: page.URL}})
	require.NoError(t, err)
	session, err := router.Start(context.Background(), Request{CustomerID: "acme", Target: "custom/own", Providers: []string{"custom/own", "beta/steady"}})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, session.Close())
		router.Close()
	})

	stream, err := session.Create(context.Background(), llm.ResponseParams{Input: []llm.Message{{Role: llm.User, Content: "hi"}}})
	require.NoError(t, err)
	response, err := llm.Collect(stream)
	require.NoError(t, err)

	require.Equal(t, "beta", response.Provider, "a 200 that is no stream is a failure, and the turn goes on to the next entry")
	require.Equal(t, "Hello", response.OutputText)
}
