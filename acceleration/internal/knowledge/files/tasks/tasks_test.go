package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	filestasks "github.com/GetStream/Vision-Agents/acceleration/internal/knowledge/files/tasks"
)

type fakeFiles struct {
	read []filestasks.ReadFile
}

func (f *fakeFiles) ProcessRead(ctx context.Context, task filestasks.ReadFile) error {
	f.read = append(f.read, task)
	async.RecordResult(ctx, task.ObjectKey)
	return nil
}

func TestKnowledgeFileTaskWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	fake := &fakeFiles{}
	filestasks.Register(tr.Registry(), filestasks.Deps{Reader: fake})

	_, err := async.Enqueue(context.Background(), tr, filestasks.ReadFile{
		CustomerID: "cust-1",
		ID:         "doc-1",
		ObjectKey:  "cust-1/abc/manual.pdf",
	}, async.WithTaskID("cust-1/abc/manual.pdf"))
	require.NoError(t, err)

	require.Len(t, fake.read, 1)
	recs := asynctest.Recorded[filestasks.ReadFile](tr)
	require.Len(t, recs, 1)
	assert.Equal(t, "doc-1", recs[0].Args.ID)
	assert.Equal(t, async.QueueKnowledgeFiles, recs[0].Queue)
	assert.Equal(t, async.ScopeShard, recs[0].Scope)
	assert.Equal(t, "cust-1/abc/manual.pdf", recs[0].Result)
}
