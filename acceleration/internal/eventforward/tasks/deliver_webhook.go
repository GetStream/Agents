// Package tasks defines typed async tasks for outbound customer webhook forwarding.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

const (
	// TypeDeliverDue drains due webhook deliveries from the eventforward store.
	TypeDeliverDue = "eventforward:deliver_due"

	deliverDueTimeout = 2 * time.Minute
	deliverDueRetries = 3
)

// DeliverDue triggers a worker pass that claims and delivers due customer webhooks
// while respecting per-destination concurrency limits and dual-secret signing.
type DeliverDue struct {
	DestinationID string `json:"destination_id,omitempty"`
}

func (DeliverDue) TaskType() string   { return TypeDeliverDue }
func (DeliverDue) Queue() async.Queue { return async.QueueWebhooks }
func (DeliverDue) Scope() async.Scope { return async.ScopeGlobal }
func (DeliverDue) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(deliverDueTimeout),
		async.WithMaxRetry(deliverDueRetries),
	}
}

// Processor delivers due eventforward webhooks.
type Processor interface {
	DeliverDueOnce(ctx context.Context) error
}

// DeliverDueHandler adapts Processor to async.Handler[DeliverDue].
type DeliverDueHandler struct {
	Processor Processor
}

func (h *DeliverDueHandler) Handle(ctx context.Context, _ DeliverDue) error {
	return h.Processor.DeliverDueOnce(ctx)
}
