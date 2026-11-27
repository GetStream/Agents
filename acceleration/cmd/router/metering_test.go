package main

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/getsentry/sentry-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool"
	"github.com/GetStream/Vision-Agents/acceleration/internal/keypool/keypooltest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/memory"
	"github.com/GetStream/Vision-Agents/acceleration/internal/memory/mem0"
)

// RefusalAlertsSuite runs the router's metering alerts as its start does, against the
// process's Sentry hub, with a client that keeps each event and drops it.
type RefusalAlertsSuite struct {
	suite.Suite

	mu   sync.Mutex
	sent []*sentry.Event
}

func TestRefusalAlertsSuite(t *testing.T) {
	suite.Run(t, new(RefusalAlertsSuite))
}

func (s *RefusalAlertsSuite) SetupTest() {
	client, err := sentry.NewClient(sentry.ClientOptions{Environment: "test", BeforeSend: func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.sent = append(s.sent, event)
		return nil
	}})
	s.Require().NoError(err)
	hub := sentry.CurrentHub()
	previous := hub.Client()
	hub.BindClient(client)
	s.T().Cleanup(func() { hub.BindClient(previous) })
}

func (s *RefusalAlertsSuite) fingerprints() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var fingerprints []string
	for _, event := range s.sent {
		fingerprints = append(fingerprints, event.Fingerprint...)
	}
	return fingerprints
}

// mem0Refusals is how many calls to mem0 the process counted as refused with a 429 on search.
func (s *RefusalAlertsSuite) mem0Refusals() float64 {
	families, err := prometheus.DefaultGatherer.Gather()
	s.Require().NoError(err)
	for _, family := range families {
		if family.GetName() != "router_mem0_calls_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["op"] == "search" && labels["status"] == "429" && labels["outcome"] == "http_error" {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// Datadog alerts on a refused key and on a mem0 refusal from their counters, so the router sends
// no Sentry event for either, and the counters still count them.
func (s *RefusalAlertsSuite) TestAMem0RefusalAndARefusedKeySendNoAlertButAreStillCounted() {
	quota := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	s.T().Cleanup(quota.Close)
	store, err := mem0.New(mem0.Options{APIKey: "test-key", BaseURL: quota.URL, Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)
	keyVar := "REFUSAL_ALERTS_" + rand.Text() + "_API_KEY"
	key := "refusal-alerts-" + rand.Text()
	s.T().Setenv(keyVar, key)
	pool := keypool.FromEnv("", keyVar)
	mem0Before := s.mem0Refusals()
	stop := startMeteringAlerts(context.Background(), "router/test", "test", slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, err = store.Recall(context.Background(), memory.Query{Scope: memory.Scope{AppID: "router", UserID: "acme", AgentID: "support", RunID: "session-1"}, Text: "anything"})
	s.Require().ErrorContains(err, "429")
	pool.QuotaExceeded(s.T().Context(), key)
	stop()

	s.Equal(1.0, s.mem0Refusals()-mem0Before, "the mem0 refusal is still counted")
	s.Equal(1.0, keypooltest.Refused(s.T(), keyVar, "quota_exceeded"), "the key's refusal is still counted")
	s.NotContains(s.fingerprints(), "usage_mem0_refused:test:search/429")
	s.NotContains(s.fingerprints(), "usage_key_refused:test:"+keyVar+"/quota_exceeded")
}
