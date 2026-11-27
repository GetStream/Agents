package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	channelstasks "github.com/GetStream/Vision-Agents/acceleration/internal/channels/tasks"
)

type fakeProcessor struct {
	got []channelstasks.AnswerMessage
}

func (f *fakeProcessor) AnswerAsyncMessage(_ context.Context, task channelstasks.AnswerMessage) error {
	f.got = append(f.got, task)
	return nil
}

func TestAnswerMessageTaskWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	proc := &fakeProcessor{}
	channelstasks.Register(tr.Registry(), channelstasks.Deps{Processor: proc})

	_, err := async.Enqueue(context.Background(), tr, channelstasks.AnswerMessage{
		WebhookToken: "tok-1",
		Kind:         "whatsapp",
		MessageID:    "msg-1",
		From:         "+15125550100",
		To:           "+15125550199",
		Text:         "hello",
	})
	require.NoError(t, err)

	require.Len(t, proc.got, 1)
	assert.Equal(t, "msg-1", proc.got[0].MessageID)

	recs := asynctest.Recorded[channelstasks.AnswerMessage](tr)
	require.Len(t, recs, 1)
	assert.Equal(t, async.QueueEvents, recs[0].Queue)
	assert.True(t, recs[0].Ran)
	assert.NoError(t, recs[0].Err)
}
