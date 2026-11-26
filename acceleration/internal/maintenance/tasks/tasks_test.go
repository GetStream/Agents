package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	mainttasks "github.com/GetStream/Vision-Agents/acceleration/internal/maintenance/tasks"
)

func TestMaintenanceTasksWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	var dataPruned, schedPruned, redacted int
	mainttasks.Register(tr.Registry(), mainttasks.Deps{
		PruneDataChanges:    func(context.Context) error { dataPruned++; return nil },
		PruneScheduledTasks: func(context.Context) error { schedPruned++; return nil },
		RedactToolOutput:    func(context.Context) error { redacted++; return nil },
	})

	_, err := async.Enqueue(context.Background(), tr, mainttasks.PruneDataChanges{})
	require.NoError(t, err)
	_, err = async.Enqueue(context.Background(), tr, mainttasks.PruneScheduledTasks{})
	require.NoError(t, err)
	_, err = async.Enqueue(context.Background(), tr, mainttasks.RedactToolOutput{})
	require.NoError(t, err)

	assert.Equal(t, 1, dataPruned)
	assert.Equal(t, 1, schedPruned)
	assert.Equal(t, 1, redacted)
	assert.Len(t, tr.Registry().Periodics(async.ScopeGlobal), 3)
}
