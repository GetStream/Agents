package asynctest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/rueidis"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
)

// RedisRuntime wraps a real Redis-backed async.AsynqBackend isolated by a unique per-test
// queue prefix (safe on both standalone Redis and Redis Cluster, without DB number
// collisions).
type RedisRuntime struct {
	Backend  *async.AsynqBackend
	Prefix   string
	Registry *async.Registry
}

// Redis builds and starts an isolated Redis-backed async backend for integration tests,
// registering t.Cleanup to shut down workers and delete temporary queues.
func Redis(t testing.TB, redisCfg config.Redis, shared rueidis.Client, reg *async.Registry) *RedisRuntime {
	t.Helper()
	if reg == nil {
		reg = async.NewRegistry()
	}
	prefix := "test-" + randomHex(4) + ":"
	cfg := config.Defaults().Async
	cfg.Backend = config.AsyncBackendAsynq
	cfg.Worker.Enabled = true
	cfg.Worker.Queues = []string{"all"}
	cfg.Worker.Concurrency = 4
	cfg.Worker.ShutdownTimeout = 5 * time.Second
	cfg.Scheduler.Enabled = false

	conn := async.QueueRedis(redisCfg, shared)
	backend, err := async.NewAsynqBackend(async.AsynqOptions{
		Config:        cfg,
		Registry:      reg,
		GlobalRedis:   conn,
		ShardRedis:    conn,
		QueuePrefix:   prefix,
		CheckInterval: 50 * time.Millisecond,
		Metrics:       async.NewMetrics(nil),
	})
	if err != nil {
		t.Fatalf("asynctest.Redis: build backend: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := backend.Start(ctx); err != nil {
		cancel()
		_ = backend.Close()
		t.Fatalf("asynctest.Redis: start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		if insp := backend.Inspector(async.ScopeShard); insp != nil {
			for q := range async.QueueCatalog {
				_ = insp.DeleteQueue(prefix+string(q), true)
			}
		}
		_ = backend.Close()
	})
	return &RedisRuntime{
		Backend:  backend,
		Prefix:   prefix,
		Registry: reg,
	}
}

// Dispatcher returns the async.Dispatcher backed by Redis.
func (r *RedisRuntime) Dispatcher() async.Dispatcher { return r.Backend }

// WaitIdle waits until the named queue has zero pending and zero active tasks.
func (r *RedisRuntime) WaitIdle(ctx context.Context, queue async.Queue) error {
	insp := r.Backend.Inspector(async.ScopeShard)
	if insp == nil {
		return fmt.Errorf("asynctest.Redis: no inspector")
	}
	wireQ := r.Prefix + string(queue)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		info, err := insp.GetQueueInfo(wireQ)
		if err == asynq.ErrQueueNotFound {
			return nil
		}
		if err == nil && info.Pending == 0 && info.Active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("asynctest.Redis: wait idle %s: %w", queue, ctx.Err())
		case <-ticker.C:
		}
	}
}

func randomHex(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
