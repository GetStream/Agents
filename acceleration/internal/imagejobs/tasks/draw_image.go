// Package tasks defines typed async tasks for asynchronous image generation and posting.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

const (
	// TypeDrawImage generates an image for a queued image job (capped on the image-draw queue).
	TypeDrawImage = "imagejobs:draw_image"
	// TypePostImage uploads and posts a drawn image (or failure message) into Stream Chat.
	TypePostImage = "imagejobs:post_image"
	// TypeProcessDue sweeps due image jobs for retry/recovery.
	TypeProcessDue = "imagejobs:process_due"

	drawTimeout = 5 * time.Minute
	drawRetries = 2

	postTimeout = 2 * time.Minute
	postRetries = 10

	dueTimeout = 5 * time.Minute
	dueRetries = 1
)

// DrawImage draws a queued image job on the concurrency-capped `image-draw` queue.
type DrawImage struct {
	JobID string `json:"job_id,omitempty"`
}

func (DrawImage) TaskType() string   { return TypeDrawImage }
func (DrawImage) Queue() async.Queue { return async.QueueImageDraw }
func (DrawImage) Scope() async.Scope { return async.ScopeGlobal }
func (DrawImage) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(drawTimeout),
		async.WithMaxRetry(drawRetries),
	}
}

// PostImage uploads and posts a decided image job on the `image-post` queue.
type PostImage struct {
	JobID string `json:"job_id,omitempty"`
}

func (PostImage) TaskType() string   { return TypePostImage }
func (PostImage) Queue() async.Queue { return async.QueueImagePost }
func (PostImage) Scope() async.Scope { return async.ScopeGlobal }
func (PostImage) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(postTimeout),
		async.WithMaxRetry(postRetries),
	}
}

// ProcessDue sweeps due/unposted image jobs periodically.
type ProcessDue struct{}

func (ProcessDue) TaskType() string   { return TypeProcessDue }
func (ProcessDue) Queue() async.Queue { return async.QueueImageDraw }
func (ProcessDue) Scope() async.Scope { return async.ScopeGlobal }
func (ProcessDue) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(dueTimeout),
		async.WithMaxRetry(dueRetries),
	}
}

// Processor runs image job steps.
type Processor interface {
	ProcessDueOnce(ctx context.Context) error
}

// DrawImageHandler adapts Processor to async.Handler[DrawImage].
type DrawImageHandler struct {
	Processor Processor
}

func (h *DrawImageHandler) Handle(ctx context.Context, _ DrawImage) error {
	return h.Processor.ProcessDueOnce(ctx)
}

// PostImageHandler adapts Processor to async.Handler[PostImage].
type PostImageHandler struct {
	Processor Processor
}

func (h *PostImageHandler) Handle(ctx context.Context, _ PostImage) error {
	return h.Processor.ProcessDueOnce(ctx)
}

// ProcessDueHandler adapts Processor to async.Handler[ProcessDue].
type ProcessDueHandler struct {
	Processor Processor
}

func (h *ProcessDueHandler) Handle(ctx context.Context, _ ProcessDue) error {
	return h.Processor.ProcessDueOnce(ctx)
}
