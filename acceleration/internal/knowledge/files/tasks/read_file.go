// Package tasks defines the typed async tasks for uploaded knowledge file ingestion.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

const (
	// TypeReadFile is the stable wire name for extracting and indexing one uploaded file.
	TypeReadFile = "knowledge:file:read"

	readTimeout = 30 * time.Minute
	maxRetry    = 3
)

// ReadFile reads one uploaded file from the shard's knowledge bucket into its knowledge base.
type ReadFile struct {
	CustomerID  string               `json:"customer_id"`
	ID          string               `json:"id"`
	ObjectKey   string               `json:"object_key"`
	Attribution *routing.Attribution `json:"attribution,omitempty"`
}

func (ReadFile) TaskType() string   { return TypeReadFile }
func (ReadFile) Queue() async.Queue { return async.QueueKnowledgeFiles }
func (ReadFile) Scope() async.Scope { return async.ScopeShard }
func (ReadFile) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(readTimeout),
		async.WithMaxRetry(maxRetry),
	}
}

// FileReader processes a ReadFile task.
type FileReader interface {
	ProcessRead(ctx context.Context, task ReadFile) error
}

// ReadFileHandler adapts FileReader to async.Handler[ReadFile].
type ReadFileHandler struct {
	Reader FileReader
}

func (h *ReadFileHandler) Handle(ctx context.Context, task ReadFile) error {
	return h.Reader.ProcessRead(ctx, task)
}
