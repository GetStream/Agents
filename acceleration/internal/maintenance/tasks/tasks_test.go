package tasks_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	mainttasks "github.com/GetStream/Vision-Agents/acceleration/internal/maintenance/tasks"
)

func TestMaintenanceTasksWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	var dataPruned, schedPruned, redacted, phoneSwept, legsSwept, legsHealed, dlcPolled, daytonaSwept int
	mainttasks.Register(tr.Registry(), mainttasks.Deps{
		PruneDataChanges:    func(context.Context) error { dataPruned++; return nil },
		PruneScheduledTasks: func(context.Context) error { schedPruned++; return nil },
		RedactToolOutput:    func(context.Context) error { redacted++; return nil },
		SweepPhone:          func(context.Context) error { phoneSwept++; return nil },
		SweepPhoneEvery:     5 * time.Minute,
		SweepCallLegs:       func(context.Context) error { legsSwept++; return nil },
		HealOpenLegs:        func(context.Context) error { legsHealed++; return nil },
		PollDLC:             func(context.Context) error { dlcPolled++; return nil },
		SweepDaytona:        func(context.Context) error { daytonaSwept++; return nil },
	})

	for _, run := range []func() error{
		func() error { _, err := async.Enqueue(context.Background(), tr, mainttasks.PruneDataChanges{}); return err },
		func() error { _, err := async.Enqueue(context.Background(), tr, mainttasks.PruneScheduledTasks{}); return err },
		func() error { _, err := async.Enqueue(context.Background(), tr, mainttasks.RedactToolOutput{}); return err },
		func() error { _, err := async.Enqueue(context.Background(), tr, mainttasks.SweepPhone{}); return err },
		func() error { _, err := async.Enqueue(context.Background(), tr, mainttasks.SweepCallLegs{}); return err },
		func() error { _, err := async.Enqueue(context.Background(), tr, mainttasks.HealOpenLegs{}); return err },
		func() error { _, err := async.Enqueue(context.Background(), tr, mainttasks.PollDLC{}); return err },
		func() error { _, err := async.Enqueue(context.Background(), tr, mainttasks.SweepDaytonaSecrets{}); return err },
	} {
		require.NoError(t, run())
	}

	assert.Equal(t, 1, dataPruned)
	assert.Equal(t, 1, schedPruned)
	assert.Equal(t, 1, redacted)
	assert.Equal(t, 1, phoneSwept)
	assert.Equal(t, 1, legsSwept)
	assert.Equal(t, 1, legsHealed)
	assert.Equal(t, 1, dlcPolled)
	assert.Equal(t, 1, daytonaSwept)
	assert.Len(t, tr.Registry().Periodics(async.ScopeGlobal), 6)
	assert.Len(t, tr.Registry().Periodics(async.ScopeShard), 2)
}
