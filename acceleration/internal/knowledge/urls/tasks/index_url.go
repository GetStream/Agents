// Package tasks defines the typed async tasks for knowledge URL crawling and periodic refresh.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

const (
	// TypeIndexURL is the stable wire name for reading one subscribed page.
	TypeIndexURL = "knowledge:url:index"
	// TypeRefreshDue is the periodic task that enqueues due URL subscriptions.
	TypeRefreshDue = "knowledge:url:refresh_due"

	indexTimeout = 2 * time.Minute
	maxRetry     = 3
)

// IndexURL reads one subscribed URL into its knowledge base.
type IndexURL struct {
	CustomerID  string               `json:"customer_id"`
	ID          string               `json:"id"`
	Attribution *routing.Attribution `json:"attribution,omitempty"`
}

func (IndexURL) TaskType() string   { return TypeIndexURL }
func (IndexURL) Queue() async.Queue { return async.QueueKnowledge }
func (IndexURL) Scope() async.Scope { return async.ScopeGlobal }
func (IndexURL) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(indexTimeout),
		async.WithMaxRetry(maxRetry),
	}
}

// Indexer processes an IndexURL task.
type Indexer interface {
	ProcessIndex(ctx context.Context, task IndexURL) error
}

// IndexURLHandler adapts Indexer to async.Handler[IndexURL].
type IndexURLHandler struct {
	Indexer Indexer
}

func (h *IndexURLHandler) Handle(ctx context.Context, task IndexURL) error {
	return h.Indexer.ProcessIndex(ctx, task)
}
