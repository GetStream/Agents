// Package tasks defines typed async tasks owned by internal/channels.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

const (
	// TypeAnswerMessage processes one inbound channel message (WhatsApp/SMS/iMessage)
	// after the webhook request has acknowledged receipt with 202 Accepted.
	TypeAnswerMessage = "channels:answer_message"

	answerMessageTimeout = 3 * time.Minute
	answerMessageRetries = 3
)

// AnswerMessage carries the webhook token (no raw provider credentials) and the parsed
// inbound message for background answering.
type AnswerMessage struct {
	WebhookToken string `json:"webhook_token"`
	Kind         string `json:"kind"`
	MessageID    string `json:"message_id"`
	From         string `json:"from"`
	To           string `json:"to"`
	Thread       string `json:"thread,omitempty"`
	Group        bool   `json:"group,omitempty"`
	Name         string `json:"name,omitempty"`
	Text         string `json:"text"`
	Media        int    `json:"media,omitempty"`
}

func (AnswerMessage) TaskType() string   { return TypeAnswerMessage }
func (AnswerMessage) Queue() async.Queue { return async.QueueEvents }
func (AnswerMessage) Scope() async.Scope { return async.ScopeShard }
func (AnswerMessage) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(answerMessageTimeout),
		async.WithMaxRetry(answerMessageRetries),
	}
}

// Processor answers an inbound channel message task.
type Processor interface {
	AnswerAsyncMessage(ctx context.Context, task AnswerMessage) error
}

// AnswerMessageHandler adapts Processor to async.Handler[AnswerMessage].
type AnswerMessageHandler struct {
	Processor Processor
}

func (h *AnswerMessageHandler) Handle(ctx context.Context, task AnswerMessage) error {
	return h.Processor.AnswerAsyncMessage(ctx, task)
}
