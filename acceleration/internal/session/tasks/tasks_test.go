package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	sessiontasks "github.com/GetStream/Vision-Agents/acceleration/internal/session/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

type fakeReviewer struct {
	got []sessiontasks.ReviewCall
}

func (f *fakeReviewer) RunReviewTask(_ context.Context, task sessiontasks.ReviewCall) error {
	f.got = append(f.got, task)
	return nil
}

func TestReviewCallTaskWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	rev := &fakeReviewer{}
	sessiontasks.Register(tr.Registry(), sessiontasks.Deps{Reviewer: rev})

	_, err := async.Enqueue(context.Background(), tr, sessiontasks.ReviewCall{
		Call:   store.Call{ID: "call-1", CustomerID: "cust-1"},
		Region: "us-east1",
		Said:   []sessiontasks.SpokenLine{{Agent: false, Text: "hi"}, {Agent: true, Text: "hello"}},
	})
	require.NoError(t, err)

	require.Len(t, rev.got, 1)
	assert.Equal(t, "call-1", rev.got[0].Call.ID)

	recs := asynctest.Recorded[sessiontasks.ReviewCall](tr)
	require.Len(t, recs, 1)
	assert.Equal(t, async.QueueMedia, recs[0].Queue)
	assert.True(t, recs[0].Ran)
	assert.NoError(t, recs[0].Err)
}
