package server

import (
	"fmt"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/alerts"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage/ledger"
)

const (
	// lagAlertSeconds is the event lag past which a router is not shipping.
	lagAlertSeconds = 600
	// failingFor is how long errors must keep coming before they page.
	failingFor = 2 * time.Minute
	// lagSpan is how far back the event lag's percentile is read.
	lagSpan = 10 * time.Minute
)

// Conditions are the service's alerts: a rejected event, a batch refused whole for what it
// holds (too big or not a batch), a batch refused for its token, ingest failing, a router's
// events arriving late, and Redis not taking the totals admission reads.
func Conditions() []alerts.Condition {
	unauthorized := func() float64 { return alerts.Total(BatchesRefused.WithLabelValues("401")) }
	return []alerts.Condition{
		{Name: "service_rejected", Level: alerts.Page, Check: alerts.Increased(func() float64 { return alerts.Total(EventsRejected) }),
			Summary: "the usage service refused events for breaking its contract; they stay unbilled until the producer is fixed and ships them again. Look at the reason label of usage_events_rejected_total"},
		{Name: "batch_refused", Level: alerts.Page, Check: alerts.Increased(func() float64 { return alerts.Total(BatchesRefused) - unauthorized() }),
			Summary: "the usage service refused whole batches for what they hold (too big, or not a batch); none of their events are recorded until the producer ships them right. Look at the status label of usage_batches_refused_total"},
		// A producer without the token is misconfigured rather than shipping a broken batch,
		// and a caller that is no producer at all looks the same.
		{Name: "batch_unauthorized", Level: alerts.Warn, Check: alerts.Increased(unauthorized),
			Summary: "a caller sent usage events with a wrong or missing token, so none were recorded. Check that every router has the usage service's token"},
		{Name: "ingest_failing", Level: alerts.Page, Check: alerts.IncreasingFor(func() float64 { return alerts.Total(IngestErrors) }, failingFor),
			Summary: "the usage service keeps failing to record batches, answering 503, so their events wait at the producer until it sends them again. Look for the error in the service's log"},
		{Name: "event_lag", Level: alerts.Warn, Check: alerts.ShareAbove(EventLag, lagAlertSeconds, 0.01, lagSpan),
			Summary: fmt.Sprintf("over 1%% of a router's events reached the usage service more than %d seconds after they happened, so balances are stale. Check whether the routers are shipping", lagAlertSeconds)},
		{Name: "mirror_failing", Level: alerts.Page, Check: alerts.IncreasingFor(func() float64 { return alerts.Total(ledger.MirrorFailures) }, failingFor),
			Summary: "the usage service keeps failing to write window totals to Redis, so admission reads stale totals. Check Redis"},
	}
}
