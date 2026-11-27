// Package tasks defines the typed async tasks for Stream Chat inbound message dispatching.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

const (
	// TypeDispatchMessage is the stable wire name for answering a Stream Chat message.new event.
	TypeDispatchMessage = "chatinbound:dispatch_message"

	dispatchTimeout = 5 * time.Minute
	dispatchRetries = 5
)

// DispatchMessage carries a verified Stream Chat message.new delivery onto the owning
// shard's `events` queue for in-process event handling.
type DispatchMessage struct {
	StreamAppPK int64  `json:"stream_app_pk"`
	MessageID   string `json:"message_id"`
	CustomerID  string `json:"customer_id,omitempty"`
	Deployment  bool   `json:"deployment,omitempty"`
	APIKey      string `json:"api_key,omitempty"`
	Body        []byte `json:"body"`
	// TargetNode is an optional hint for the pod holding a live session for this channel.
	TargetNode string `json:"target_node,omitempty"`
}

func (DispatchMessage) TaskType() string   { return TypeDispatchMessage }
func (DispatchMessage) Queue() async.Queue { return async.QueueEvents }
func (DispatchMessage) Scope() async.Scope { return async.ScopeShard }
func (DispatchMessage) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(dispatchTimeout),
		async.WithMaxRetry(dispatchRetries),
	}
}

// Dispatcher handles a queued DispatchMessage on the owning shard.
type Dispatcher interface {
	AnswerAsyncChatInbound(ctx context.Context, task DispatchMessage) error
}

// DispatchMessageHandler adapts Dispatcher to async.Handler[DispatchMessage].
type DispatchMessageHandler struct {
	Dispatcher Dispatcher
}

func (h *DispatchMessageHandler) Handle(ctx context.Context, task DispatchMessage) error {
	return h.Dispatcher.AnswerAsyncChatInbound(ctx, task)
}
