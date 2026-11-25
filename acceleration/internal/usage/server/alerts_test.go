package server

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/alerts"
)

// AlertTextSuite sends each of the service's conditions through a real Sentry client with no
// DSN, whose BeforeSend keeps each event and drops it.
type AlertTextSuite struct {
	suite.Suite
}

func TestAlertTextSuite(t *testing.T) {
	suite.Run(t, new(AlertTextSuite))
}

// sentBy is the event each condition sends when it holds, by name. The checks are replaced by
// ones that hold: what makes each hold is not what is under test.
func (s *AlertTextSuite) sentBy() map[string]*sentry.Event {
	var mu sync.Mutex
	events := map[string]*sentry.Event{}
	client, err := sentry.NewClient(sentry.ClientOptions{
		BeforeSend: func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
			mu.Lock()
			defer mu.Unlock()
			events[event.Tags["usage_alert"]] = event
			return nil
		},
	})
	s.Require().NoError(err)
	conditions := Conditions()
	for i := range conditions {
		conditions[i].Check = func(time.Time) (bool, map[string]any) { return true, nil }
	}
	loop := alerts.New(alerts.Options{
		Hub: sentry.NewHub(client, sentry.NewScope()), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Source: "usage/test", Every: time.Hour, Limit: time.Hour,
	}, conditions...)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	loop.Run(ctx)
	mu.Lock()
	defer mu.Unlock()
	return events
}

// The fingerprints are what the alerts sent before they had a summary, read on the base commit:
// a change in one is a new Sentry issue, which the rules and the people who muted the old one
// do not know.
func (s *AlertTextSuite) TestEveryAlertKeepsItsSentryIssue() {
	events := s.sentBy()

	fingerprints := map[string][]string{}
	for name, event := range events {
		fingerprints[name] = event.Fingerprint
	}
	s.Equal(map[string][]string{
		"service_rejected":   {"usage_alert:development:service_rejected"},
		"batch_refused":      {"usage_alert:development:batch_refused"},
		"batch_unauthorized": {"usage_alert:development:batch_unauthorized"},
		"ingest_failing":     {"usage_alert:development:ingest_failing"},
		"event_lag":          {"usage_alert:development:event_lag"},
		"mirror_failing":     {"usage_alert:development:mirror_failing"},
	}, fingerprints)
}

// What a person reads in Slack after the name: what the condition means and the first thing to
// check. The level is what pages and what only warns.
func (s *AlertTextSuite) TestEveryAlertSaysWhatItMeans() {
	events := s.sentBy()

	messages := map[string]string{}
	levels := map[string]sentry.Level{}
	for name, event := range events {
		messages[name] = event.Message
		levels[name] = event.Level
	}
	s.Equal(map[string]string{
		"service_rejected": "usage alert: service_rejected: the usage service refused events for breaking its contract; " +
			"they stay unbilled until the producer is fixed and ships them again " +
			"(look at the reason label of usage_events_rejected_total)",
		"batch_refused": "usage alert: batch_refused: the usage service refused whole batches for what they hold " +
			"(too big, or not a batch); none of their events are recorded until the producer ships them right " +
			"(look at the status label of usage_batches_refused_total)",
		"batch_unauthorized": "usage alert: batch_unauthorized: a caller sent usage events with a wrong or missing token, " +
			"so none were recorded (check that every router has the usage service's token)",
		"ingest_failing": "usage alert: ingest_failing: the usage service keeps failing to record batches, answering 503, " +
			"so their events wait at the producer until it sends them again (look for the error in the service's log)",
		"event_lag": "usage alert: event_lag: over 1% of the routers' events in the last 10 minutes reached the usage service " +
			"more than 600 seconds after they happened, so balances are stale (check whether the routers are shipping)",
		"mirror_failing": "usage alert: mirror_failing: the usage service keeps failing to write window totals to Redis, " +
			"so admission reads stale totals (check Redis)",
	}, messages)
	s.Equal(map[string]sentry.Level{
		"service_rejected": sentry.LevelFatal, "batch_refused": sentry.LevelFatal, "batch_unauthorized": sentry.LevelError,
		"ingest_failing": sentry.LevelFatal, "event_lag": sentry.LevelError, "mirror_failing": sentry.LevelFatal,
	}, levels)
}
