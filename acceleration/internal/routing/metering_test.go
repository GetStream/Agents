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
	"github.com/prometheus/client_golang/prometheus"
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

// meteringRow is one billable row and what it raises: the alert, and the text it is sent with.
type meteringRow struct {
	name    string
	alert   string
	stat    Stat
	request store.Request
	quote   pricing.Quote
	// suffix is what the item carries after its model: unpriced adds its region and BYOK state.
	suffix string
	text   string
}

func meteringRows() []meteringRow {
	billable := store.Request{Billable: true, Success: true}
	return []meteringRow{
		{name: "missing", alert: "missing", request: billable,
			text: "billable work finished and recorded none of what its price rates, " +
				"so it was written at nothing; the vendor's adapter or the session lost the usage and needs a fix"},
		{name: "estimated", alert: "estimated", request: billable, quote: pricing.Quote{Rated: true},
			stat: Stat{Attributes: map[string]string{AttributeUsageEstimated: "true"}},
			text: "billable work finished and the vendor never said its usage, " +
				"so it was billed on the router's own count; the vendor's adapter needs a fix"},
		{name: "unpriced", alert: "unpriced", request: billable, quote: pricing.Quote{Unpriced: true}, suffix: "/us/false",
			text: "billable work succeeded with no price and was written " +
				"at nothing until the item is priced; add its price for this region and BYOK state"},
		// The vendor's total is 100 and 10 plus 20 were billed.
		{name: "mismatch under-billed", alert: "mismatch", stat: Stat{VendorTotalTokens: 100},
			request: store.Request{Billable: true, InputTokens: 10, OutputTokens: 20},
			text: "the billed input and output tokens do not add up to the vendor's total, " +
				"so the row is billed wrong; the vendor's adapter needs a fix"},
		// The vendor's total is 20 and 10 plus 20 were billed: more than the vendor counted.
		{name: "mismatch over-billed", alert: "mismatch", stat: Stat{VendorTotalTokens: 20},
			request: store.Request{Billable: true, InputTokens: 10, OutputTokens: 20},
			text: "the billed input and output tokens do not add up to the vendor's total, " +
				"so the row is billed wrong; the vendor's adapter needs a fix"},
	}
}

// each records every row alone, on a model of its own, so that an alert sent is the alert its
// row raises, and runs check on what the alerts sent.
func (s *MeteringAlertTextSuite) each(check func(r meteringRow, item string, events map[string]*sentry.Event)) {
	for _, r := range meteringRows() {
		s.Run(r.name, func() {
			s.SetupTest()
			s.recorder.watch(s.config, r.stat, r.request, r.quote)
			check(r, "llm/vendor/"+s.config.Model, s.sent())
		})
	}
}

// The Help is what Prometheus shows beside usage_mismatch_total: it must say the row is wrong
// whichever way the tokens miss the vendor's total.
func (s *MeteringAlertTextSuite) TestUsageMismatchHelpSaysItRunsEitherWay() {
	descs := make(chan *prometheus.Desc, 1)
	usageMismatch.Describe(descs)
	s.Contains(
		(<-descs).String(),
		`help: "Billable work whose input and output tokens do not add up to the vendor's own total, either way: the row is billed wrong and the adapter needs a fix"`,
	)
}

// The fingerprints are what the alerts sent before they had a description, read on the base
// commit: a change in one is a new Sentry issue for the rules and the people who muted the old.
func (s *MeteringAlertTextSuite) TestEveryAlertKeepsItsSentryIssue() {
	s.each(func(r meteringRow, item string, events map[string]*sentry.Event) {
		s.Require().Len(events, 1, "a row raises only its own alert")
		s.Require().Contains(events, r.alert)
		s.Equal([]string{"usage_" + r.alert + ":" + item + r.suffix}, events[r.alert].Fingerprint)
	})
}

// What a person reads in Slack after the item: what the alert means and who acts.
func (s *MeteringAlertTextSuite) TestEveryAlertSaysWhatItMeansAfterTheItem() {
	s.each(func(r meteringRow, item string, events map[string]*sentry.Event) {
		s.Require().Len(events, 1, "a row raises only its own alert")
		s.Require().Contains(events, r.alert)
		s.Equal("usage alert: "+r.alert+" "+item+r.suffix+": "+r.text, events[r.alert].Message)
	})
}
