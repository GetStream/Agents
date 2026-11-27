package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	chatinboundtasks "github.com/GetStream/Vision-Agents/acceleration/internal/chatinbound/tasks"
)

type fakeDispatcher struct {
	handled []chatinboundtasks.DispatchMessage
}

func (f *fakeDispatcher) AnswerAsyncChatInbound(ctx context.Context, task chatinboundtasks.DispatchMessage) error {
	f.handled = append(f.handled, task)
	async.RecordResult(ctx, "answered:"+task.MessageID)
	return nil
}

func TestDispatchMessageTaskWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	fake := &fakeDispatcher{}
	chatinboundtasks.Register(tr.Registry(), chatinboundtasks.Deps{Dispatcher: fake})

	_, err := async.Enqueue(context.Background(), tr, chatinboundtasks.DispatchMessage{
		StreamAppPK: 42,
		MessageID:   "msg-1",
		CustomerID:  "cust-1",
		Body:        []byte(`{"type":"message.new"}`),
	}, async.WithTaskID("chatinbound:42:msg-1"))
	require.NoError(t, err)

	require.Len(t, fake.handled, 1)
	recs := asynctest.Recorded[chatinboundtasks.DispatchMessage](tr)
	require.Len(t, recs, 1)
	assert.Equal(t, int64(42), recs[0].Args.StreamAppPK)
	assert.Equal(t, "msg-1", recs[0].Args.MessageID)
	assert.Equal(t, async.QueueEvents, recs[0].Queue)
	assert.Equal(t, async.ScopeShard, recs[0].Scope)
	assert.Equal(t, "answered:msg-1", recs[0].Result)
}
