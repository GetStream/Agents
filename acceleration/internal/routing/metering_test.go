package routing

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/alerts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// MeteringAlertTextSuite raises each metering alert by recording billable work the way the
// router does, and reads what the alert loop sends to Sentry.
type MeteringAlertTextSuite struct {
	suite.Suite
	recorder *Recorder
	config   ProviderConfig
}

func TestMeteringAlertTextSuite(t *testing.T) {
	suite.Run(t, new(MeteringAlertTextSuite))
}

func (s *MeteringAlertTextSuite) SetupTest() {
	s.recorder = &Recorder{modality: LLM, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	// A model of this test's own: the counters are the process's, and other suites move them.
	s.config = ProviderConfig{Provider: "vendor", Model: "m-" + strings.ToLower(rand.Text())}
}

// sent is the events the metering alerts send now about this test's model, by alert name.
func (s *MeteringAlertTextSuite) sent() map[string]*sentry.Event {
	var mu sync.Mutex
	events := map[string]*sentry.Event{}
	client, err := sentry.NewClient(sentry.ClientOptions{
		BeforeSend: func(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
			mu.Lock()
			defer mu.Unlock()
			if strings.Contains(event.Tags["item"], s.config.Model) {
				events[event.Tags["usage_alert"]] = event
			}
			return nil
		},
	})
	s.Require().NoError(err)
	loop := alerts.New(alerts.Options{
		Hub: sentry.NewHub(client, sentry.NewScope()), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Source: "router/test", Every: time.Hour, Limit: time.Hour,
	}, MeteringAlerts(time.Hour)...)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	loop.Run(ctx)
	mu.Lock()
	defer mu.Unlock()
	return events
}

// raiseAll records one row for each of the four alerts.
func (s *MeteringAlertTextSuite) raiseAll() {
	billable := store.Request{Billable: true, Success: true}
	// The vendor's total is 100 and 10 plus 20 were billed.
	s.recorder.watch(s.config, Stat{VendorTotalTokens: 100}, store.Request{Billable: true, InputTokens: 10, OutputTokens: 20}, pricing.Quote{})
	s.recorder.watch(s.config, Stat{}, billable, pricing.Quote{})
	s.recorder.watch(s.config, Stat{Attributes: map[string]string{AttributeUsageEstimated: "true"}}, billable, pricing.Quote{Rated: true})
	s.recorder.watch(s.config, Stat{}, billable, pricing.Quote{Unpriced: true})
}

// The fingerprints are what the alerts sent before they had a description, read on the base
// commit: a change in one is a new Sentry issue for the rules and the people who muted the old.
func (s *MeteringAlertTextSuite) TestEveryAlertKeepsItsSentryIssue() {
	s.raiseAll()

	events := s.sent()

	fingerprints := map[string][]string{}
	for name, event := range events {
		fingerprints[name] = event.Fingerprint
	}
	item := "llm/vendor/" + s.config.Model // unpriced adds its region and BYOK state
	s.Equal(map[string][]string{
		"missing":   {"usage_missing:" + item},
		"estimated": {"usage_estimated:" + item},
		"unpriced":  {"usage_unpriced:" + item + "/us/false"},
		"mismatch":  {"usage_mismatch:" + item},
	}, fingerprints)
}

// What a person reads in Slack after the item: what the alert means and who acts.
func (s *MeteringAlertTextSuite) TestEveryAlertSaysWhatItMeansAfterTheItem() {
	s.raiseAll()

	events := s.sent()

	item := "llm/vendor/" + s.config.Model
	messages := map[string]string{}
	for name, event := range events {
		messages[name] = event.Message
	}
	s.Equal(map[string]string{
		"missing": "usage alert: missing " + item + ": billable work finished and recorded none of what its price rates, " +
			"so it was written at nothing; the vendor's adapter or the session lost the usage and needs a fix",
		"estimated": "usage alert: estimated " + item + ": billable work finished and the vendor never said its usage, " +
			"so it was billed on the router's own count; the vendor's adapter needs a fix",
		"unpriced": "usage alert: unpriced " + item + "/us/false: billable work succeeded with no price and was written " +
			"at nothing until the item is priced; add its price for this region and BYOK state",
		"mismatch": "usage alert: mismatch " + item + ": the billed input and output tokens do not add up to the vendor's total, " +
			"so part of the bill is missed; the vendor's adapter needs a fix",
	}, messages)
}
