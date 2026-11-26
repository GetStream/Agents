package async_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
)

type sampleTask struct {
	ID    string `json:"id"`
	Value int    `json:"value"`
}

func (sampleTask) TaskType() string  { return "test:sample" }
func (sampleTask) Queue() async.Queue { return async.QueueEvents }
func (sampleTask) Scope() async.Scope { return async.ScopeShard }
func (sampleTask) TaskOptions() []async.Option {
	return []async.Option{
		async.WithMaxRetry(2),
		async.WithTimeout(5 * time.Second),
	}
}

func TestParseQueuesAndSplitGroups(t *testing.T) {
	shardQueues, err := async.ParseQueues([]string{"all"}, async.ScopeShard)
	require.NoError(t, err)
	assert.Contains(t, shardQueues, async.QueueEvents)
	assert.Contains(t, shardQueues, async.QueueKnowledgeFiles)
	assert.NotContains(t, shardQueues, async.QueueKnowledge, "global-only queue excluded from shard scope")

	globalQueues, err := async.ParseQueues([]string{"all"}, async.ScopeGlobal)
	require.NoError(t, err)
	assert.Contains(t, globalQueues, async.QueueKnowledge)
	assert.NotContains(t, globalQueues, async.QueueEvents)

	weighted, err := async.ParseQueues([]string{"events:20", "knowledge-files:1"}, async.ScopeShard)
	require.NoError(t, err)
	assert.Equal(t, map[async.Queue]int{
		async.QueueEvents:         20,
		async.QueueKnowledgeFiles: 1,
	}, weighted)

	groups := async.SplitQueueGroups(weighted, map[string]int{"knowledge-files": 2}, 16)
	require.Len(t, groups, 2)
	assert.Equal(t, "general", groups[0].Name)
	assert.Equal(t, 16, groups[0].Concurrency)
	assert.Equal(t, map[async.Queue]int{async.QueueEvents: 20}, groups[0].Queues)
	assert.Equal(t, "knowledge-files", groups[1].Name)
	assert.Equal(t, 2, groups[1].Concurrency)
}

func TestInProcBackendExecutesRetriesAndExportsMetrics(t *testing.T) {
	reg := async.NewRegistry()
	var attempts atomic.Int32
	done := make(chan string, 1)

	async.RegisterFunc(reg, func(ctx context.Context, task sampleTask) error {
		n := attempts.Add(1)
		retried, ok := async.RetryCount(ctx)
		assert.True(t, ok)
		if n == 1 {
			assert.Equal(t, 0, retried)
			return async.RetryAfter(errors.New("transient"), 20*time.Millisecond)
		}
		assert.Equal(t, 1, retried)
		done <- task.ID
		return nil
	})

	promReg := prometheus.NewPedanticRegistry()
	cfg := config.Defaults().Async
	cfg.Backend = config.AsyncBackendInProcess
	cfg.Worker.Concurrency = 4
	cfg.Scheduler.Enabled = false

	rt, err := async.New(async.RuntimeOptions{
		Config:     cfg,
		Registry:   reg,
		Metrics:    async.NewMetrics(promReg),
		Registerer: promReg,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, rt.Start(ctx))
	defer func() { require.NoError(t, rt.Close()) }()

	info, err := async.Enqueue(ctx, rt.Dispatcher(), sampleTask{ID: "abc", Value: 42}, async.WithTaskID("task-abc"))
	require.NoError(t, err)
	assert.Equal(t, "task-abc", info.ID)

	select {
	case got := <-done:
		assert.Equal(t, "abc", got)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for in-process task completion")
	}

	assert.Equal(t, int32(2), attempts.Load())
	mfs, err := promReg.Gather()
	require.NoError(t, err)
	names := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	assert.True(t, names["router_async_tasks_enqueued_total"])
	assert.True(t, names["router_async_tasks_total"])
	assert.True(t, names["router_async_task_duration_seconds"])
}

func TestRuntimeRefusesDisabledBackend(t *testing.T) {
	cfg := config.Defaults().Async
	cfg.Backend = "disabled"
	_, err := async.New(async.RuntimeOptions{Config: cfg})
	require.Error(t, err)
	assert.ErrorContains(t, err, "unsupported backend")
}
