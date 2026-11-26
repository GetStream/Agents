package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	eftasks "github.com/GetStream/Vision-Agents/acceleration/internal/eventforward/tasks"
)

type fakeProcessor struct {
	calls int
}

func (f *fakeProcessor) DeliverDueOnce(_ context.Context) error {
	f.calls++
	return nil
}

func TestEventForwardTasksWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	fake := &fakeProcessor{}
	eftasks.Register(tr.Registry(), eftasks.Deps{Processor: fake})

	_, err := async.Enqueue(context.Background(), tr, eftasks.DeliverDue{DestinationID: "dest-1"})
	require.NoError(t, err)
	assert.Equal(t, 1, fake.calls)
	recs := asynctest.Recorded[eftasks.DeliverDue](tr)
	require.Len(t, recs, 1)
	assert.Equal(t, async.QueueWebhooks, recs[0].Queue)
}
