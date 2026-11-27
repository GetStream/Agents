// Package tasks defines typed async tasks owned by internal/session.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

const (
	// TypeReviewCall runs the post-call LLM review for a completed session.
	TypeReviewCall = "session:review_call"

	reviewCallTimeout = 90 * time.Second
	reviewCallRetries = 2
)

// SpokenLine is one turn of a call transcript serialized for post-call review.
type SpokenLine struct {
	Agent bool   `json:"agent"`
	Text  string `json:"text"`
}

// ReviewCall scores and summarizes a finished call and persists the review row.
type ReviewCall struct {
	Call        store.Call          `json:"call"`
	Attribution routing.Attribution `json:"attribution"`
	Region      string              `json:"region,omitempty"`
	Target      string              `json:"target,omitempty"`
	Guide       string              `json:"guide,omitempty"`
	Said        []SpokenLine        `json:"said"`
}

func (ReviewCall) TaskType() string   { return TypeReviewCall }
func (ReviewCall) Queue() async.Queue { return async.QueueMedia }
func (ReviewCall) Scope() async.Scope { return async.ScopeShard }
func (ReviewCall) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(reviewCallTimeout),
		async.WithMaxRetry(reviewCallRetries),
	}
}

// Reviewer executes a post-call LLM review task.
type Reviewer interface {
	RunReviewTask(ctx context.Context, task ReviewCall) error
}

// ReviewCallHandler adapts Reviewer to async.Handler[ReviewCall].
type ReviewCallHandler struct {
	Reviewer Reviewer
}

func (h *ReviewCallHandler) Handle(ctx context.Context, task ReviewCall) error {
	return h.Reviewer.RunReviewTask(ctx, task)
}
