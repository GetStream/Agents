package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	simtasks "github.com/GetStream/Vision-Agents/acceleration/internal/simulation/tasks"
)

type fakeExecutor struct {
	runs []string
}

func (f *fakeExecutor) ExecuteAsyncRun(_ context.Context, customerID, runID string) error {
	f.runs = append(f.runs, customerID+":"+runID)
	return nil
}

func TestSimulationTasksWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	fake := &fakeExecutor{}
	simtasks.Register(tr.Registry(), simtasks.Deps{Executor: fake})

	_, err := async.Enqueue(context.Background(), tr, simtasks.RunSimulation{
		CustomerID: "cust-1",
		RunID:      "run-42",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"cust-1:run-42"}, fake.runs)

	recs := asynctest.Recorded[simtasks.RunSimulation](tr)
	require.Len(t, recs, 1)
	assert.Equal(t, async.QueueSimulations, recs[0].Queue)
	assert.Equal(t, async.ScopeShard, recs[0].Scope)
}
