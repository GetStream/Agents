// Package tasks defines typed async tasks for MCP event subscriptions and deliveries.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

const (
	// TypeHandleEvent handles a verified incoming MCP webhook delivery by running its agent conversation.
	TypeHandleEvent = "mcpevents:handle_event"
	// TypeReconcileSubscriptions runs a periodic pass over due MCP event subscriptions.
	TypeReconcileSubscriptions = "mcpevents:reconcile_subscriptions"

	handleEventTimeout = 15 * time.Minute
	handleEventRetries = 3

	reconcileTimeout = 2 * time.Minute
	reconcileRetries = 1
)

// HandleEvent runs the agent conversation triggered by a verified MCP event delivery.
type HandleEvent struct {
	SubscriptionID string `json:"subscription_id"`
	Token          string `json:"token"`
	EventID        string `json:"event_id"`
	EventType      string `json:"event_type"`
	Payload        []byte `json:"payload"`
}

func (HandleEvent) TaskType() string   { return TypeHandleEvent }
func (HandleEvent) Queue() async.Queue { return async.QueueEvents }
func (HandleEvent) Scope() async.Scope { return async.ScopeShard }
func (HandleEvent) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(handleEventTimeout),
		async.WithMaxRetry(handleEventRetries),
	}
}

// ReconcileSubscriptions processes due MCP event subscriptions (subscribe / refresh / unsubscribe).
type ReconcileSubscriptions struct{}

func (ReconcileSubscriptions) TaskType() string   { return TypeReconcileSubscriptions }
func (ReconcileSubscriptions) Queue() async.Queue { return async.QueueWebhooks }
func (ReconcileSubscriptions) Scope() async.Scope { return async.ScopeGlobal }
func (ReconcileSubscriptions) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(reconcileTimeout),
		async.WithMaxRetry(reconcileRetries),
	}
}

// Runner executes MCP event tasks.
type Runner interface {
	HandleAsyncEvent(ctx context.Context, task HandleEvent) error
	ReconcileDueOnce(ctx context.Context) error
}

// HandleEventHandler adapts Runner to async.Handler[HandleEvent].
type HandleEventHandler struct {
	Runner Runner
}

func (h *HandleEventHandler) Handle(ctx context.Context, task HandleEvent) error {
	return h.Runner.HandleAsyncEvent(ctx, task)
}

// ReconcileSubscriptionsHandler adapts Runner to async.Handler[ReconcileSubscriptions].
type ReconcileSubscriptionsHandler struct {
	Runner Runner
}

func (h *ReconcileSubscriptionsHandler) Handle(ctx context.Context, _ ReconcileSubscriptions) error {
	return h.Runner.ReconcileDueOnce(ctx)
}
