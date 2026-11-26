package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	bridgetasks "github.com/GetStream/Vision-Agents/acceleration/internal/channelbridge/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
)

type fakeSender struct {
	sent []conversation.FinishedReply
}

func (f *fakeSender) SendAsyncReply(_ context.Context, reply conversation.FinishedReply) error {
	f.sent = append(f.sent, reply)
	return nil
}

func TestSendReplyWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	fake := &fakeSender{}
	bridgetasks.Register(tr.Registry(), bridgetasks.Deps{Sender: fake})

	_, err := async.Enqueue(context.Background(), tr, bridgetasks.SendReply{
		Customer:  "cust-1",
		CID:       "messaging:thread-1",
		MessageID: "m-1",
		Text:      "hello external",
	})
	require.NoError(t, err)

	require.Len(t, fake.sent, 1)
	assert.Equal(t, "hello external", fake.sent[0].Text)
	recs := asynctest.Recorded[bridgetasks.SendReply](tr)
	require.Len(t, recs, 1)
	assert.Equal(t, async.QueueWebhooks, recs[0].Queue)
	assert.Equal(t, async.ScopeGlobal, recs[0].Scope)
}
