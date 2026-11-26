package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	imgtasks "github.com/GetStream/Vision-Agents/acceleration/internal/imagejobs/tasks"
)

type fakeProcessor struct {
	calls int
}

func (f *fakeProcessor) ProcessDueOnce(_ context.Context) error {
	f.calls++
	return nil
}

func TestImageJobsTasksWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	fake := &fakeProcessor{}
	imgtasks.Register(tr.Registry(), imgtasks.Deps{Processor: fake})

	_, err := async.Enqueue(context.Background(), tr, imgtasks.DrawImage{JobID: "job-1"})
	require.NoError(t, err)
	_, err = async.Enqueue(context.Background(), tr, imgtasks.PostImage{JobID: "job-1"})
	require.NoError(t, err)
	assert.Equal(t, 2, fake.calls)

	drawRecs := asynctest.Recorded[imgtasks.DrawImage](tr)
	require.Len(t, drawRecs, 1)
	assert.Equal(t, async.QueueImageDraw, drawRecs[0].Queue)

	postRecs := asynctest.Recorded[imgtasks.PostImage](tr)
	require.Len(t, postRecs, 1)
	assert.Equal(t, async.QueueImagePost, postRecs[0].Queue)
}
