package async

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/rueidis"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
)

// QueueRedis returns the asynq Redis connection option matching how rueidis connected
// (standalone vs Redis Cluster). asynq does not auto-detect cluster mode and would hit
// MOVED errors on a cluster if given RedisClientOpt.
func QueueRedis(redis config.Redis, shared rueidis.Client) asynq.RedisConnOpt {
	if shared != nil && shared.Mode() == rueidis.ClientModeCluster {
		return asynq.RedisClusterClientOpt{
			Addrs:    []string{redis.Addr},
			Username: redis.Username,
			Password: redis.Password,
		}
	}
	return asynq.RedisClientOpt{
		Addr:     redis.Addr,
		Username: redis.Username,
		Password: redis.Password,
	}
}

// ShardRedis returns the shard's Redis config, or the shared config when no separate
// shard Redis is configured.
func ShardRedis(redis config.Redis) config.Redis {
	if redis.Shard.Addr == "" {
		return redis
	}
	return config.Redis{
		Addr:     redis.Shard.Addr,
		Username: redis.Shard.Username,
		Password: redis.Shard.Password,
	}
}

// AsynqOptions configures the Redis-backed asynq runtime.
type AsynqOptions struct {
	Config      config.Async
	Registry    *Registry
	GlobalRedis asynq.RedisConnOpt
	ShardRedis  asynq.RedisConnOpt
	// QueuePrefix optionally prefixes every Redis queue name (used by asynctest.Redis
	// to isolate tests without colliding on Redis DB numbers).
	QueuePrefix string
	// CheckInterval overrides asynq polling and default retry intervals (used in tests).
	CheckInterval time.Duration
	Metrics       *Metrics
	Logger        *slog.Logger
}

type asynqScope struct {
	scope      Scope
	conn       asynq.RedisConnOpt
	client     *asynq.Client
	inspector  *asynq.Inspector
	servers    []*asynq.Server
	scheduler  *asynq.Scheduler
	queues     map[Queue]int
	wireQueues []string
}

// AsynqBackend runs async tasks on Redis via hibiken/asynq, supporting separate shard and
// global Redis instances, per-queue concurrency groups, and cron scheduling.
type AsynqBackend struct {
	cfg         config.Async
	registry    *Registry
	prefix      string
	checkInt    time.Duration
	metrics     *Metrics
	logger      *slog.Logger
	global      *asynqScope
	shard       *asynqScope
	sameRedis   bool
	inFlight    atomic.Int64
	mu          sync.Mutex
	started     bool
	closed      bool
}

// NewAsynqBackend constructs an AsynqBackend. Call Start() to begin processing workers
// and periodic schedules.
func NewAsynqBackend(opts AsynqOptions) (*AsynqBackend, error) {
	if opts.Registry == nil {
		return nil, errors.New("async: registry is required")
	}
	if opts.GlobalRedis == nil && opts.ShardRedis == nil {
		return nil, errors.New("async: at least one Redis connection is required for asynq backend")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Metrics == nil {
		opts.Metrics = DefaultMetrics()
	}
	globalConn := opts.GlobalRedis
	shardConn := opts.ShardRedis
	sameRedis := false
	if globalConn == nil {
		globalConn = shardConn
		sameRedis = true
	} else if shardConn == nil {
		shardConn = globalConn
		sameRedis = true
	}

	b := &AsynqBackend{
		cfg:       opts.Config,
		registry:  opts.Registry,
		prefix:    opts.QueuePrefix,
		checkInt:  opts.CheckInterval,
		metrics:   opts.Metrics,
		logger:    opts.Logger,
		sameRedis: sameRedis,
	}

	var err error
	if b.global, err = b.buildScope(ScopeGlobal, globalConn); err != nil {
		return nil, err
	}
	if sameRedis {
		// Still track shard queues distinctly when global & shard share one Redis.
		if b.shard, err = b.buildScope(ScopeShard, shardConn); err != nil {
			_ = b.global.closeClients()
			return nil, err
		}
	} else {
		if b.shard, err = b.buildScope(ScopeShard, shardConn); err != nil {
			_ = b.global.closeClients()
			return nil, err
		}
	}
	return b, nil
}

func (b *AsynqBackend) wireQueue(q Queue) string {
	return b.prefix + string(q)
}

func (b *AsynqBackend) scopeEnabled(scope Scope) bool {
	switch b.cfg.Scope {
	case "", config.AsyncScopeBoth:
		return true
	case config.AsyncScopeShard:
		return scope == ScopeShard
	case config.AsyncScopeGlobal:
		return scope == ScopeGlobal
	default:
		return false
	}
}

func (b *AsynqBackend) buildScope(scope Scope, conn asynq.RedisConnOpt) (*asynqScope, error) {
	sc := &asynqScope{
		scope:     scope,
		conn:      conn,
		client:    asynq.NewClient(conn),
		inspector: asynq.NewInspector(conn),
	}
	if !b.cfg.Worker.Enabled || !b.scopeEnabled(scope) {
		return sc, nil
	}
	queues, err := ParseQueues(b.cfg.Worker.Queues, scope)
	if err != nil {
		_ = sc.closeClients()
		return nil, err
	}
	// When global and shard share one Redis (single-shard deployment), avoid running two
	// servers polling the exact same queue name (e.g. webhooks / maintenance) on the same
	// Redis by running shared-scope queues on the shard server.
	if b.sameRedis && scope == ScopeGlobal {
		for q := range queues {
			if QueueSupportsScope(q, ScopeShard) {
				delete(queues, q)
			}
		}
	}
	sc.queues = queues
	for _, q := range slices.Sorted(maps.Keys(queues)) {
		sc.wireQueues = append(sc.wireQueues, b.wireQueue(q))
	}

	groups := SplitQueueGroups(queues, b.cfg.Worker.QueueConcurrency, b.cfg.Worker.Concurrency)
	for _, grp := range groups {
		wireWeights := make(map[string]int, len(grp.Queues))
		for q, w := range grp.Queues {
			wireWeights[b.wireQueue(q)] = w
		}
		shutdownTimeout := b.cfg.Worker.ShutdownTimeout
		if shutdownTimeout <= 0 {
			shutdownTimeout = 30 * time.Second
		}
		asynqCfg := asynq.Config{
			Concurrency:     grp.Concurrency,
			Queues:          wireWeights,
			StrictPriority:  b.cfg.Worker.StrictPriority,
			ShutdownTimeout: shutdownTimeout,
			LogLevel:        asynq.WarnLevel,
			RetryDelayFunc: func(n int, err error, t *asynq.Task) time.Duration {
				if delay, ok := RetryDelayOf(err); ok {
					return delay
				}
				if b.checkInt > 0 {
					return b.checkInt
				}
				return asynq.DefaultRetryDelayFunc(n, err, t)
			},
		}
		if b.checkInt > 0 {
			asynqCfg.TaskCheckInterval = b.checkInt
			asynqCfg.DelayedTaskCheckInterval = b.checkInt
		}
		sc.servers = append(sc.servers, asynq.NewServer(conn, asynqCfg))
	}
	return sc, nil
}

func (sc *asynqScope) closeClients() error {
	if sc == nil {
		return nil
	}
	return errors.Join(sc.inspector.Close(), sc.client.Close())
}

func (b *AsynqBackend) forScope(scope Scope) *asynqScope {
	if scope == ScopeGlobal {
		return b.global
	}
	return b.shard
}

// EnqueueRaw implements Dispatcher.
func (b *AsynqBackend) EnqueueRaw(ctx context.Context, taskType string, payload []byte, scope Scope, opts EnqueueOptions) (TaskInfo, error) {
	sc := b.forScope(scope)
	if sc == nil || sc.client == nil {
		return TaskInfo{}, fmt.Errorf("async: no Redis client configured for scope %s", scope)
	}
	wireQ := b.wireQueue(opts.Queue)
	aopts := []asynq.Option{
		asynq.Queue(wireQ),
		asynq.MaxRetry(opts.MaxRetry),
	}
	if opts.Timeout > 0 {
		aopts = append(aopts, asynq.Timeout(opts.Timeout))
	}
	if opts.TaskID != "" {
		aopts = append(aopts, asynq.TaskID(opts.TaskID))
	}
	if opts.ProcessIn > 0 {
		aopts = append(aopts, asynq.ProcessIn(opts.ProcessIn))
	} else if !opts.ProcessAt.IsZero() {
		aopts = append(aopts, asynq.ProcessAt(opts.ProcessAt))
	}
	if opts.UniqueTTL > 0 {
		aopts = append(aopts, asynq.Unique(opts.UniqueTTL))
	}
	if opts.Retention > 0 {
		aopts = append(aopts, asynq.Retention(opts.Retention))
	}

	info, err := sc.client.EnqueueContext(ctx, asynq.NewTask(taskType, payload), aopts...)
	if err != nil {
		return TaskInfo{}, err
	}
	b.metrics.ObserveEnqueue(opts.Queue, taskType, scope)
	return fromAsynqInfo(info, opts.Queue), nil
}

// TaskState implements Dispatcher.
func (b *AsynqBackend) TaskState(_ context.Context, scope Scope, queue Queue, taskID string) (TaskState, error) {
	sc := b.forScope(scope)
	if sc == nil || sc.inspector == nil {
		return StateUnknown, ErrTaskNotFound
	}
	info, err := sc.inspector.GetTaskInfo(b.wireQueue(queue), taskID)
	if err != nil {
		return StateUnknown, err
	}
	return mapAsynqState(info.State), nil
}

// Cancel implements Dispatcher.
func (b *AsynqBackend) Cancel(_ context.Context, scope Scope, queue Queue, taskID string) error {
	sc := b.forScope(scope)
	if sc == nil || sc.inspector == nil {
		return nil
	}
	// Try deleting queued task first; if active, cancel processing.
	err := sc.inspector.DeleteTask(b.wireQueue(queue), taskID)
	if err == nil || errors.Is(err, asynq.ErrTaskNotFound) {
		return nil
	}
	return sc.inspector.CancelProcessing(taskID)
}

// Start begins all configured asynq servers and periodic schedulers.
func (b *AsynqBackend) Start(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return nil
	}
	for _, sc := range []*asynqScope{b.shard, b.global} {
		if sc == nil {
			continue
		}
		if len(sc.servers) > 0 {
			mux := b.buildMux(sc.scope)
			for _, srv := range sc.servers {
				if err := srv.Start(mux); err != nil {
					b.shutdownLocked()
					return fmt.Errorf("async: start %s worker: %w", sc.scope, err)
				}
			}
			b.logger.Info("async worker started",
				"backend", config.AsyncBackendAsynq,
				"scope", sc.scope,
				"queues", sc.queues,
				"pools", len(sc.servers),
			)
		}
		if b.cfg.Scheduler.Enabled && b.scopeEnabled(sc.scope) {
			if err := b.startScheduler(sc); err != nil {
				b.shutdownLocked()
				return err
			}
		}
	}
	b.started = true
	return nil
}

func (b *AsynqBackend) buildMux(scope Scope) *asynq.ServeMux {
	mux := asynq.NewServeMux()
	for _, taskType := range b.registry.TaskTypes() {
		handler, _, defaultQueue, ok := b.registry.Lookup(taskType)
		if !ok {
			continue
		}
		tt := taskType
		h := handler
		dq := defaultQueue
		mux.HandleFunc(tt, func(ctx context.Context, t *asynq.Task) error {
			b.inFlight.Add(1)
			defer b.inFlight.Add(-1)
			retried, _ := asynq.GetRetryCount(ctx)
			maxRetry, _ := asynq.GetMaxRetry(ctx)
			taskID, _ := asynq.GetTaskID(ctx)
			meta := Meta{
				TaskID:   taskID,
				TaskType: tt,
				Queue:    dq,
				Scope:    scope,
				Retried:  retried,
				MaxRetry: maxRetry,
			}
			return execute(ctx, h, t.Payload(), meta, b.logger, b.metrics)
		})
	}
	return mux
}

func (b *AsynqBackend) startScheduler(sc *asynqScope) error {
	entries := b.registry.Periodics(sc.scope)
	if len(entries) == 0 {
		return nil
	}
	scheduler := asynq.NewScheduler(sc.conn, &asynq.SchedulerOpts{
		Location: time.UTC,
		LogLevel: asynq.WarnLevel,
	})
	for _, entry := range entries {
		aopts := []asynq.Option{
			asynq.Queue(b.wireQueue(entry.Options.Queue)),
			asynq.MaxRetry(entry.Options.MaxRetry),
		}
		if entry.Options.Timeout > 0 {
			aopts = append(aopts, asynq.Timeout(entry.Options.Timeout))
		}
		uniqueTTL := entry.Options.UniqueTTL
		if uniqueTTL <= 0 {
			// Deduplicate identical periodic runs enqueued by multiple pods within the same tick.
			uniqueTTL = 55 * time.Second
		}
		aopts = append(aopts, asynq.Unique(uniqueTTL))
		if _, err := scheduler.Register(entry.Spec, asynq.NewTask(entry.TaskType, entry.Payload), aopts...); err != nil {
			return fmt.Errorf("async: register periodic %s (%s): %w", entry.TaskType, entry.Spec, err)
		}
	}
	if err := scheduler.Start(); err != nil {
		return fmt.Errorf("async: start %s scheduler: %w", sc.scope, err)
	}
	sc.scheduler = scheduler
	b.logger.Info("async scheduler started", "scope", sc.scope, "jobs", len(entries))
	return nil
}

// Running returns the number of tasks currently executing in this process.
func (b *AsynqBackend) Running() int {
	return int(b.inFlight.Load())
}

// QueueDepths inspects active queues for Prometheus metrics.
func (b *AsynqBackend) QueueDepths() []QueueDepth {
	var out []QueueDepth
	for _, sc := range []*asynqScope{b.shard, b.global} {
		if sc == nil || sc.inspector == nil {
			continue
		}
		queues := slices.Sorted(maps.Keys(sc.queues))
		if len(queues) == 0 {
			// Even API-only pods with worker.enabled=false can report depths for known queues
			// in their enabled scope if queried.
			continue
		}
		for _, q := range queues {
			info, err := sc.inspector.GetQueueInfo(b.wireQueue(q))
			if err != nil {
				continue
			}
			out = append(out, QueueDepth{
				Queue:     q,
				Scope:     sc.scope,
				Pending:   info.Pending,
				Active:    info.Active,
				Retry:     info.Retry,
				Scheduled: info.Scheduled,
				Archived:  info.Archived,
			})
		}
	}
	return out
}

// Healthy verifies Redis connectivity via the inspector.
func (b *AsynqBackend) Healthy(_ context.Context) error {
	for _, sc := range []*asynqScope{b.shard, b.global} {
		if sc == nil || sc.inspector == nil {
			continue
		}
		if _, err := sc.inspector.Queues(); err != nil {
			return fmt.Errorf("async: %s redis unhealthy: %w", sc.scope, err)
		}
	}
	return nil
}

// StopClaiming stops pulling new tasks from Redis while allowing active tasks to finish.
func (b *AsynqBackend) StopClaiming() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, sc := range []*asynqScope{b.shard, b.global} {
		if sc == nil {
			continue
		}
		if sc.scheduler != nil {
			sc.scheduler.Shutdown()
			sc.scheduler = nil
		}
		for _, srv := range sc.servers {
			srv.Stop()
		}
	}
}

// Close stops claiming new tasks, waits for in-flight tasks to finish, and closes Redis clients.
func (b *AsynqBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	return b.shutdownLocked()
}

func (b *AsynqBackend) shutdownLocked() error {
	var errs []error
	for _, sc := range []*asynqScope{b.shard, b.global} {
		if sc == nil {
			continue
		}
		if sc.scheduler != nil {
			sc.scheduler.Shutdown()
			sc.scheduler = nil
		}
		for _, srv := range sc.servers {
			srv.Shutdown()
		}
		sc.servers = nil
		if err := sc.closeClients(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Inspector returns the underlying asynq.Inspector for scope (used by asynctest.Redis).
func (b *AsynqBackend) Inspector(scope Scope) *asynq.Inspector {
	sc := b.forScope(scope)
	if sc == nil {
		return nil
	}
	return sc.inspector
}

func fromAsynqInfo(info *asynq.TaskInfo, queue Queue) TaskInfo {
	if info == nil {
		return TaskInfo{Queue: queue}
	}
	return TaskInfo{
		ID:            info.ID,
		Queue:         queue,
		Type:          info.Type,
		State:         mapAsynqState(info.State),
		MaxRetry:      info.MaxRetry,
		Retried:       info.Retried,
		NextProcessAt: info.NextProcessAt,
	}
}

func mapAsynqState(state asynq.TaskState) TaskState {
	switch state {
	case asynq.TaskStateActive:
		return StateActive
	case asynq.TaskStatePending:
		return StatePending
	case asynq.TaskStateScheduled:
		return StateScheduled
	case asynq.TaskStateRetry:
		return StateRetry
	case asynq.TaskStateArchived:
		return StateArchived
	case asynq.TaskStateCompleted:
		return StateCompleted
	default:
		return StateUnknown
	}
}
