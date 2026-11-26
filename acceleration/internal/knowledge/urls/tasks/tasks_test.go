package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	urlstasks "github.com/GetStream/Vision-Agents/acceleration/internal/knowledge/urls/tasks"
)

type fakeURLs struct {
	indexed []urlstasks.IndexURL
	swept   int
}

func (f *fakeURLs) ProcessIndex(ctx context.Context, task urlstasks.IndexURL) error {
	f.indexed = append(f.indexed, task)
	async.RecordResult(ctx, "indexed:"+task.ID)
	return nil
}

func (f *fakeURLs) SweepDue(_ context.Context) error {
	f.swept++
	return nil
}

func TestKnowledgeURLTasksWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	fake := &fakeURLs{}
	require.NoError(t, urlstasks.Register(tr.Registry(), urlstasks.Deps{
		Indexer:   fake,
		Refresher: fake,
	}))

	_, err := async.Enqueue(context.Background(), tr, urlstasks.IndexURL{
		CustomerID: "cust-1",
		ID:         "page-1",
	}, async.WithTaskID("page-1"))
	require.NoError(t, err)

	require.Len(t, fake.indexed, 1)
	recs := asynctest.Recorded[urlstasks.IndexURL](tr)
	require.Len(t, recs, 1)
	assert.Equal(t, "cust-1", recs[0].Args.CustomerID)
	assert.Equal(t, "page-1", recs[0].Args.ID)
	assert.Equal(t, async.QueueKnowledge, recs[0].Queue)
	assert.Equal(t, async.ScopeGlobal, recs[0].Scope)
	assert.Equal(t, "indexed:page-1", recs[0].Result)

	_, err = async.Enqueue(context.Background(), tr, urlstasks.RefreshDue{})
	require.NoError(t, err)
	assert.Equal(t, 1, fake.swept)
	assert.Len(t, tr.Registry().Periodics(async.ScopeGlobal), 1)
}
