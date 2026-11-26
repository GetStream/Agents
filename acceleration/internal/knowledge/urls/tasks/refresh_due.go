package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// RefreshDue sweeps subscribed URLs that are due for periodic re-indexing and enqueues
// an IndexURL task for each one.
type RefreshDue struct{}

func (RefreshDue) TaskType() string   { return TypeRefreshDue }
func (RefreshDue) Queue() async.Queue { return async.QueueMaintenance }
func (RefreshDue) Scope() async.Scope { return async.ScopeGlobal }
func (RefreshDue) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(time.Minute),
		async.WithMaxRetry(1),
		async.WithUnique(55 * time.Second),
	}
}

// Refresher sweeps due URL subscriptions.
type Refresher interface {
	SweepDue(ctx context.Context) error
}

// RefreshDueHandler adapts Refresher to async.Handler[RefreshDue].
type RefreshDueHandler struct {
	Refresher Refresher
}

func (h *RefreshDueHandler) Handle(ctx context.Context, _ RefreshDue) error {
	return h.Refresher.SweepDue(ctx)
}
