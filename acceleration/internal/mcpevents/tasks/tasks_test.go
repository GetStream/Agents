package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	mcptasks "github.com/GetStream/Vision-Agents/acceleration/internal/mcpevents/tasks"
)

type fakeRunner struct {
	events      []mcptasks.HandleEvent
	reconciled  int
}

func (f *fakeRunner) HandleAsyncEvent(_ context.Context, task mcptasks.HandleEvent) error {
	f.events = append(f.events, task)
	return nil
}

func (f *fakeRunner) ReconcileDueOnce(_ context.Context) error {
	f.reconciled++
	return nil
}

func TestMCPEventsTasksWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	fake := &fakeRunner{}
	mcptasks.Register(tr.Registry(), mcptasks.Deps{Runner: fake})

	_, err := async.Enqueue(context.Background(), tr, mcptasks.HandleEvent{
		SubscriptionID: "sub-1",
		EventID:        "evt-1",
		EventType:      "issue.created",
		Payload:        []byte(`{"ok":true}`),
	})
	require.NoError(t, err)
	require.Len(t, fake.events, 1)
	assert.Equal(t, "sub-1", fake.events[0].SubscriptionID)

	_, err = async.Enqueue(context.Background(), tr, mcptasks.ReconcileSubscriptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, fake.reconciled)
}
