// Package tasks defines typed async tasks for the external channel bridge.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
)

const (
	// TypeSendReply is the stable wire name for delivering a finished agent reply to an external thread.
	TypeSendReply = "channelbridge:send_reply"

	sendReplyTimeout = time.Minute
	sendReplyRetries = 4
)

// SendReply delivers an agent's finished reply to the linked external channel (Slack, WhatsApp, SMS, etc.).
type SendReply struct {
	Customer  string               `json:"customer"`
	CID       string               `json:"cid"`
	MessageID string               `json:"message_id"`
	CommandID string               `json:"command_id,omitempty"`
	Text      string               `json:"text"`
	Files     []sandbox.Attachment `json:"files,omitempty"`
}

func (SendReply) TaskType() string   { return TypeSendReply }
func (SendReply) Queue() async.Queue { return async.QueueWebhooks }
func (SendReply) Scope() async.Scope { return async.ScopeGlobal }
func (SendReply) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(sendReplyTimeout),
		async.WithMaxRetry(sendReplyRetries),
	}
}

// FinishedReply converts the wire task back to conversation.FinishedReply.
func (t SendReply) FinishedReply() conversation.FinishedReply {
	return conversation.FinishedReply{
		Customer:  t.Customer,
		CID:       t.CID,
		MessageID: t.MessageID,
		CommandID: t.CommandID,
		Text:      t.Text,
		Files:     t.Files,
	}
}

// ReplySender sends a finished reply to its external provider thread.
type ReplySender interface {
	SendAsyncReply(ctx context.Context, reply conversation.FinishedReply) error
}

// SendReplyHandler adapts ReplySender to async.Handler[SendReply].
type SendReplyHandler struct {
	Sender ReplySender
}

func (h *SendReplyHandler) Handle(ctx context.Context, task SendReply) error {
	return h.Sender.SendAsyncReply(ctx, task.FinishedReply())
}
