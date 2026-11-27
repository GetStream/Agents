package async

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
)

// InProcOptions configures the in-memory worker backend (default in local development).
type InProcOptions struct {
	Config   config.Async
	Registry *Registry
	Metrics  *Metrics
	Logger   *slog.Logger
}

type inProcTask struct {
	id         string
	taskType   string
	payload    []byte
	scope      Scope
	queue      Queue
	maxRetry   int
	retried    int
	timeout    time.Duration
	processAt  time.Time
	uniqueKey  string
	uniqueExp  time.Time
	state      TaskState
	cancelFunc context.CancelFunc
	index      int
}

type inProcHeap []*inProcTask

func (h inProcHeap) Len() int           { return len(h) }
func (h inProcHeap) Less(i, j int) bool { return h[i].processAt.Before(h[j].processAt) }
func (h inProcHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *inProcHeap) Push(x any) {
	t := x.(*inProcTask)
	t.index = len(*h)
	*h = append(*h, t)
}
func (h *inProcHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	t.index = -1
	*h = old[:n-1]
	return t
}

// InProcBackend implements Dispatcher + Runner entirely in-process using goroutines,
// honoring Celery-style queue selection, per-queue concurrency caps, delays, retries,
// TaskID deduplication, periodic jobs, and graceful drain.
type InProcBackend struct {
	cfg      config.Async
	registry *Registry
	metrics  *Metrics
	logger   *slog.Logger

	mu          sync.Mutex
	tasks       map[string]*inProcTask // keyed by queue+":"+id
	uniqueKeys  map[string]time.Time   // uniqueKey -> expiresAt
	pending     inProcHeap
	queueLimits map[Queue]int
	queueActive map[Queue]int
	allowed     map[Queue]bool
	totalLimit  int
	totalActive int
	archived    map[Queue]int

	notify   chan struct{}
	stopCh   chan struct{}
	doneCh   chan struct{}
	wg       sync.WaitGroup
	inFlight atomic.Int64
	started  bool
	draining bool
	closed   bool
}

// NewInProcBackend constructs an in-process worker backend.
func NewInProcBackend(opts InProcOptions) (*InProcBackend, error) {
	if opts.Registry == nil {
		return nil, errors.New("async: registry is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Metrics == nil {
		opts.Metrics = DefaultMetrics()
	}
	allowed := map[Queue]bool{}
	for _, sc := range []Scope{ScopeShard, ScopeGlobal} {
		if opts.Config.Scope != "" && opts.Config.Scope != config.AsyncScopeBoth && string(sc) != opts.Config.Scope {
			continue
		}
		parsed, err := ParseQueues(opts.Config.Worker.Queues, sc)
		if err != nil {
			return nil, err
		}
		for q := range parsed {
			allowed[q] = true
		}
	}
	limits := map[Queue]int{}
	for q, n := range opts.Config.Worker.QueueConcurrency {
		if n > 0 {
			limits[Queue(q)] = n
		}
	}
	conc := opts.Config.Worker.Concurrency
	if conc <= 0 {
		conc = 8
	}
	return &InProcBackend{
		cfg:         opts.Config,
		registry:    opts.Registry,
		metrics:     opts.Metrics,
		logger:      opts.Logger,
		tasks:       make(map[string]*inProcTask),
		uniqueKeys:  make(map[string]time.Time),
		queueLimits: limits,
		queueActive: make(map[Queue]int),
		allowed:     allowed,
		totalLimit:  conc,
		archived:    make(map[Queue]int),
		notify:      make(chan struct{}, 1),
	}, nil
}

func taskKey(q Queue, id string) string { return string(q) + ":" + id }

func uniqueDigest(q Queue, taskType string, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(q))
	h.Write([]byte{0})
	h.Write([]byte(taskType))
	h.Write([]byte{0})
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// EnqueueRaw implements Dispatcher.
func (b *InProcBackend) EnqueueRaw(_ context.Context, taskType string, payload []byte, scope Scope, opts EnqueueOptions) (TaskInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return TaskInfo{}, errors.New("async: in-process backend is closed")
	}

	now := time.Now()
	id := opts.TaskID
	if id == "" {
		id = uuid.NewString()
	}
	key := taskKey(opts.Queue, id)
	if existing, exists := b.tasks[key]; exists && existing.state != StateCompleted && existing.state != StateArchived {
		return TaskInfo{}, ErrTaskIDConflict
	}

	var ukey string
	var uexp time.Time
	if opts.UniqueTTL > 0 {
		ukey = uniqueDigest(opts.Queue, taskType, payload)
		if exp, ok := b.uniqueKeys[ukey]; ok && now.Before(exp) {
			return TaskInfo{}, ErrDuplicateTask
		}
		uexp = now.Add(opts.UniqueTTL)
		b.uniqueKeys[ukey] = uexp
	}

	runAt := now
	state := StatePending
	if opts.ProcessIn > 0 {
		runAt = now.Add(opts.ProcessIn)
		state = StateScheduled
	} else if !opts.ProcessAt.IsZero() && opts.ProcessAt.After(now) {
		runAt = opts.ProcessAt
		state = StateScheduled
	}

	cp := make([]byte, len(payload))
	copy(cp, payload)
	item := &inProcTask{
		id:        id,
		taskType:  taskType,
		payload:   cp,
		scope:     scope,
		queue:     opts.Queue,
		maxRetry:  opts.MaxRetry,
		timeout:   opts.Timeout,
		processAt: runAt,
		uniqueKey: ukey,
		uniqueExp: uexp,
		state:     state,
		index:     -1,
	}
	b.tasks[key] = item
	heap.Push(&b.pending, item)
	b.metrics.ObserveEnqueue(opts.Queue, taskType, scope)
	b.signal()

	return TaskInfo{
		ID:            item.id,
		Queue:         item.queue,
		Type:          item.taskType,
		State:         item.state,
		MaxRetry:      item.maxRetry,
		Retried:       item.retried,
		NextProcessAt: item.processAt,
	}, nil
}

// TaskState implements Dispatcher.
func (b *InProcBackend) TaskState(_ context.Context, _ Scope, queue Queue, taskID string) (TaskState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	item, ok := b.tasks[taskKey(queue, taskID)]
	if !ok {
		return StateUnknown, ErrTaskNotFound
	}
	return item.state, nil
}

// Cancel implements Dispatcher.
func (b *InProcBackend) Cancel(_ context.Context, _ Scope, queue Queue, taskID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := taskKey(queue, taskID)
	item, ok := b.tasks[key]
	if !ok {
		return nil
	}
	if item.index >= 0 {
		heap.Remove(&b.pending, item.index)
	}
	if item.cancelFunc != nil {
		item.cancelFunc()
	}
	delete(b.tasks, key)
	if item.uniqueKey != "" {
		delete(b.uniqueKeys, item.uniqueKey)
	}
	return nil
}

// Start launches the in-process dispatcher loop and optional periodic scheduler.
func (b *InProcBackend) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return nil
	}
	b.started = true
	b.stopCh = make(chan struct{})
	b.doneCh = make(chan struct{})
	if b.cfg.Worker.Enabled {
		go b.loop(ctx)
		b.logger.Info("async worker started",
			"backend", config.AsyncBackendInProcess,
			"scope", b.cfg.Scope,
			"concurrency", b.totalLimit,
		)
	} else {
		close(b.doneCh)
	}
	if b.cfg.Scheduler.Enabled {
		b.startPeriodics(ctx)
	}
	return nil
}

func (b *InProcBackend) startPeriodics(ctx context.Context) {
	var scopes []Scope
	switch b.cfg.Scope {
	case "", config.AsyncScopeBoth:
		scopes = []Scope{ScopeShard, ScopeGlobal}
	case config.AsyncScopeShard:
		scopes = []Scope{ScopeShard}
	case config.AsyncScopeGlobal:
		scopes = []Scope{ScopeGlobal}
	}
	for _, sc := range scopes {
		for _, entry := range b.registry.Periodics(sc) {
			interval, ok := parseSimpleInterval(entry.Spec)
			if !ok {
				b.logger.Debug("skipping non-interval periodic spec in in_process mode",
					"task_type", entry.TaskType, "spec", entry.Spec)
				continue
			}
			e := entry
			b.wg.Add(1)
			go func() {
				defer b.wg.Done()
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-b.stopCh:
						return
					case <-ticker.C:
						_, _ = b.EnqueueRaw(ctx, e.TaskType, e.Payload, e.Scope, e.Options)
					}
				}
			}()
		}
	}
}

func parseSimpleInterval(spec string) (time.Duration, bool) {
	spec = strings.TrimSpace(spec)
	if after, ok := strings.CutPrefix(spec, "@every "); ok {
		d, err := time.ParseDuration(strings.TrimSpace(after))
		return d, err == nil && d > 0
	}
	switch spec {
	case "@hourly":
		return time.Hour, true
	case "@daily", "@midnight":
		return 24 * time.Hour, true
	}
	return 0, false
}

func (b *InProcBackend) signal() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (b *InProcBackend) loop(ctx context.Context) {
	defer close(b.doneCh)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		wait := b.dispatchDue(ctx)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if wait > 0 {
			timer.Reset(wait)
		} else {
			timer.Reset(time.Hour)
		}

		select {
		case <-ctx.Done():
			return
		case <-b.stopCh:
			return
		case <-b.notify:
		case <-timer.C:
		}
	}
}

func (b *InProcBackend) dispatchDue(ctx context.Context) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.draining || b.closed {
		return 0
	}

	now := time.Now()
	var skipped []*inProcTask
	var nextWake time.Duration

	for len(b.pending) > 0 {
		head := b.pending[0]
		if head.processAt.After(now) {
			d := head.processAt.Sub(now)
			if nextWake == 0 || d < nextWake {
				nextWake = d
			}
			break
		}
		heap.Pop(&b.pending)
		if !b.allowed[head.queue] || !b.canRunLocked(head.queue) {
			skipped = append(skipped, head)
			continue
		}
		b.totalActive++
		b.queueActive[head.queue]++
		head.state = StateActive
		b.inFlight.Add(1)
		b.wg.Add(1)
		go b.runOne(ctx, head)
	}
	for _, s := range skipped {
		heap.Push(&b.pending, s)
	}
	return nextWake
}

func (b *InProcBackend) canRunLocked(q Queue) bool {
	if capN, capped := b.queueLimits[q]; capped {
		return b.queueActive[q] < capN
	}
	return b.totalActive < b.totalLimit
}

func (b *InProcBackend) runOne(parent context.Context, item *inProcTask) {
	defer b.wg.Done()
	defer b.inFlight.Add(-1)

	runCtx := parent
	var cancel context.CancelFunc
	if item.timeout > 0 {
		runCtx, cancel = context.WithTimeout(parent, item.timeout)
	} else {
		runCtx, cancel = context.WithCancel(parent)
	}
	defer cancel()

	b.mu.Lock()
	item.cancelFunc = cancel
	b.mu.Unlock()

	handler, _, _, ok := b.registry.Lookup(item.taskType)
	var err error
	if !ok {
		err = SkipRetry(fmt.Errorf("async: no handler registered for %q", item.taskType))
	} else {
		meta := Meta{
			TaskID:   item.id,
			TaskType: item.taskType,
			Queue:    item.queue,
			Scope:    item.scope,
			Retried:  item.retried,
			MaxRetry: item.maxRetry,
		}
		err = execute(runCtx, handler, item.payload, meta, b.logger, b.metrics)
	}

	b.mu.Lock()
	item.cancelFunc = nil
	b.totalActive--
	b.queueActive[item.queue]--

	key := taskKey(item.queue, item.id)
	if err == nil {
		item.state = StateCompleted
		delete(b.tasks, key)
	} else if IsSkipRetry(err) || item.retried >= item.maxRetry {
		item.state = StateArchived
		b.archived[item.queue]++
		delete(b.tasks, key)
	} else {
		item.retried++
		delay := backoffDelay(item.retried, err)
		item.processAt = time.Now().Add(delay)
		item.state = StateRetry
		if !b.closed {
			heap.Push(&b.pending, item)
		}
	}
	b.mu.Unlock()
	b.signal()
}

func backoffDelay(attempt int, err error) time.Duration {
	if d, ok := RetryDelayOf(err); ok {
		return d
	}
	secs := math.Pow(float64(attempt), 4) + 15
	if secs > 3600 {
		secs = 3600
	}
	return time.Duration(secs) * time.Second
}

// Running returns the number of tasks currently executing.
func (b *InProcBackend) Running() int {
	return int(b.inFlight.Load())
}

// QueueDepths reports current in-memory queue counts.
func (b *InProcBackend) QueueDepths() []QueueDepth {
	b.mu.Lock()
	defer b.mu.Unlock()
	byQueue := map[Queue]*QueueDepth{}
	ensure := func(q Queue, sc Scope) *QueueDepth {
		if d, ok := byQueue[q]; ok {
			return d
		}
		d := &QueueDepth{Queue: q, Scope: sc}
		byQueue[q] = d
		return d
	}
	for _, item := range b.tasks {
		d := ensure(item.queue, item.scope)
		switch item.state {
		case StatePending:
			d.Pending++
		case StateActive:
			d.Active++
		case StateRetry:
			d.Retry++
		case StateScheduled:
			d.Scheduled++
		}
	}
	for q, count := range b.archived {
		d := ensure(q, ScopeShard)
		d.Archived = count
	}
	out := make([]QueueDepth, 0, len(byQueue))
	for _, d := range byQueue {
		out = append(out, *d)
	}
	return out
}

// Healthy always returns nil for the in-process backend until closed.
func (b *InProcBackend) Healthy(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return errors.New("async: in-process backend is closed")
	}
	return nil
}

// StopClaiming stops launching new tasks while allowing in-flight tasks to finish.
func (b *InProcBackend) StopClaiming() {
	b.mu.Lock()
	b.draining = true
	b.mu.Unlock()
	b.signal()
}

// Close stops claiming new tasks and waits for in-flight tasks up to ShutdownTimeout.
func (b *InProcBackend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.draining = true
	b.closed = true
	stopCh := b.stopCh
	doneCh := b.doneCh
	b.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	if doneCh != nil {
		<-doneCh
	}

	timeout := b.cfg.Worker.ShutdownTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	finished := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("async: in-process shutdown timed out after %s with %d tasks running", timeout, b.Running())
	}
}
