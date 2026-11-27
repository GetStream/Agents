package asynctest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
)

type greetTask struct {
	Name string `json:"name"`
}

func (greetTask) TaskType() string  { return "test:greet" }
func (greetTask) Queue() async.Queue { return async.QueueEvents }
func (greetTask) Scope() async.Scope { return async.ScopeShard }

func TestEagerModeAwaitsAndInspectsArgsAndResults(t *testing.T) {
	tr := asynctest.New(t)
	var sideEffect string
	async.RegisterFunc(tr.Registry(), func(ctx context.Context, task greetTask) error {
		sideEffect = "hello " + task.Name
		async.RecordResult(ctx, sideEffect)
		return nil
	})

	// Simulate F() which internally enqueues T().
	runF := func(ctx context.Context, d async.Dispatcher) error {
		_, err := async.Enqueue(ctx, d, greetTask{Name: "world"}, async.WithTaskID("greet-1"))
		return err
	}

	require.NoError(t, runF(context.Background(), tr))
	// T() was awaited before runF returned.
	assert.Equal(t, "hello world", sideEffect)

	recs := asynctest.Recorded[greetTask](tr)
	require.Len(t, recs, 1)
	assert.Equal(t, "world", recs[0].Args.Name)
	assert.Equal(t, "greet-1", recs[0].ID)
	assert.Equal(t, async.QueueEvents, recs[0].Queue)
	assert.True(t, recs[0].Ran)
	assert.NoError(t, recs[0].Err)
	assert.Equal(t, "hello world", recs[0].Result)
}

func TestManualModeStepsDelaysAndRetries(t *testing.T) {
	tr := asynctest.New(t, asynctest.Manual())
	calls := 0
	async.RegisterFunc(tr.Registry(), func(ctx context.Context, task greetTask) error {
		calls++
		if calls == 1 {
			return async.RetryAfter(errors.New("try again"), 30*time.Second)
		}
		async.RecordResult(ctx, "ok:"+task.Name)
		return nil
	})

	_, err := async.Enqueue(context.Background(), tr, greetTask{Name: "retry"}, async.WithProcessIn(10*time.Second))
	require.NoError(t, err)
	assert.Equal(t, 1, tr.PendingCount())

	// Not due yet.
	_, ran := tr.RunNext(context.Background())
	assert.False(t, ran)

	tr.Advance(10 * time.Second)
	rec, ran := tr.RunNext(context.Background())
	require.True(t, ran)
	require.Error(t, rec.Err)
	assert.Equal(t, 1, rec.Attempt)
	assert.Equal(t, 1, tr.PendingCount(), "re-queued for retry")

	tr.Advance(30 * time.Second)
	require.NoError(t, tr.Drain(context.Background()))
	assert.Equal(t, 0, tr.PendingCount())

	recs := asynctest.Recorded[greetTask](tr)
	require.Len(t, recs, 2)
	assert.Error(t, recs[0].Err)
	assert.NoError(t, recs[1].Err)
	assert.Equal(t, 2, recs[1].Attempt)
	assert.Equal(t, "ok:retry", recs[1].Result)
}

func TestRecordOnlyCapturesArgsWithoutRunningHandler(t *testing.T) {
	tr := asynctest.New(t, asynctest.RecordOnly(greetTask{}))
	called := false
	async.RegisterFunc(tr.Registry(), func(ctx context.Context, task greetTask) error {
		called = true
		return nil
	})

	_, err := async.Enqueue(context.Background(), tr, greetTask{Name: "captured"})
	require.NoError(t, err)
	assert.False(t, called)

	recs := asynctest.Recorded[greetTask](tr)
	require.Len(t, recs, 1)
	assert.Equal(t, "captured", recs[0].Args.Name)
	assert.False(t, recs[0].Ran)
}
