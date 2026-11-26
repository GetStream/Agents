// Package asynctest provides deterministic, Redis-free test helpers for code that
// enqueues or handles internal/async tasks, plus an isolated Redis helper for
// integration tests.
//
// By default, New(t) runs in Eager mode: when tested function F() enqueues task T(),
// T() is JSON-round-tripped, executed inline, and awaited before Enqueue returns. Every
// invocation's typed arguments, options, error, and optional recorded result are stored
// for assertion via Recorded[T](runner).
package asynctest

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Option configures a test Runner.
type Option func(*Runner)

// Manual switches the runner from Eager (execute & await inline on Enqueue) to Manual
// mode, where enqueued tasks wait in an in-memory priority queue until RunNext or Drain
// is called. Use Manual mode to test delays, retries, ordering, or pre-execution state.
func Manual() Option {
	return func(r *Runner) { r.manual = true }
}

// AllowUnregistered records enqueues whose task type has no registered handler without
// failing the test.
func AllowUnregistered() Option {
	return func(r *Runner) { r.allowUnregistered = true }
}

// RecordOnly records enqueues for the listed task types without invoking their handlers
// (even if registered), letting caller tests inspect task args in isolation.
func RecordOnly(tasks ...async.Task) Option {
	return func(r *Runner) {
		for _, task := range tasks {
			r.recordOnly[task.TaskType()] = true
		}
	}
}

// WithNow sets the starting fake clock time (primarily used with Manual mode).
func WithNow(t time.Time) Option {
	return func(r *Runner) { r.now = t }
}

// RawRecord stores one captured task enqueue / execution.
type RawRecord struct {
	ID           string
	TaskType     string
	Payload      []byte
	Queue        async.Queue
	Scope        async.Scope
	Options      async.EnqueueOptions
	Attempt      int
	Ran          bool
	Err          error
	SkippedRetry bool
	Result       any
	Duration     time.Duration
}

// Record is the typed view of a RawRecord returned by Recorded[T].
type Record[T async.Task] struct {
	ID           string
	Args         T
	Queue        async.Queue
	Scope        async.Scope
	Options      async.EnqueueOptions
	Attempt      int
	Ran          bool
	Err          error
	SkippedRetry bool
	Result       any
	Duration     time.Duration
}

type queuedItem struct {
	id        string
	taskType  string
	payload   []byte
	scope     async.Scope
	opts      async.EnqueueOptions
	retried   int
	processAt time.Time
	seq       uint64
	index     int
}

type itemHeap []*queuedItem

func (h itemHeap) Len() int { return len(h) }
func (h itemHeap) Less(i, j int) bool {
	if h[i].processAt.Equal(h[j].processAt) {
		return h[i].seq < h[j].seq
	}
	return h[i].processAt.Before(h[j].processAt)
}
func (h itemHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *itemHeap) Push(x any) {
	item := x.(*queuedItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *itemHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

// Runner implements async.Dispatcher for unit and suite tests.
type Runner struct {
	t                 testing.TB
	registry          *async.Registry
	manual            bool
	allowUnregistered bool
	recordOnly        map[string]bool

	mu      sync.Mutex
	now     time.Time
	seq     uint64
	depth   int
	pending itemHeap
	liveIDs map[string]async.TaskState
	records []RawRecord
}

const maxEagerDepth = 32

// New creates a Runner in Eager mode by default (pass Manual() for deferred execution).
func New(t testing.TB, opts ...Option) *Runner {
	t.Helper()
	r := &Runner{
		t:          t,
		registry:   async.NewRegistry(),
		recordOnly: make(map[string]bool),
		now:        time.Now().UTC(),
		liveIDs:    make(map[string]async.TaskState),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

// Registry returns the test runner's task registry for registering handlers.
func (r *Runner) Registry() *async.Registry { return r.registry }

// EnqueueRaw implements async.Dispatcher.
func (r *Runner) EnqueueRaw(ctx context.Context, taskType string, payload []byte, scope async.Scope, opts async.EnqueueOptions) (async.TaskInfo, error) {
	r.mu.Lock()
	id := opts.TaskID
	if id == "" {
		id = uuid.NewString()
	}
	idKey := string(opts.Queue) + ":" + id
	if state, exists := r.liveIDs[idKey]; exists && state != async.StateCompleted && state != async.StateArchived {
		r.mu.Unlock()
		return async.TaskInfo{}, async.ErrTaskIDConflict
	}

	cp := make([]byte, len(payload))
	copy(cp, payload)

	runAt := r.now
	state := async.StatePending
	if opts.ProcessIn > 0 {
		runAt = r.now.Add(opts.ProcessIn)
		state = async.StateScheduled
	} else if !opts.ProcessAt.IsZero() && opts.ProcessAt.After(r.now) {
		runAt = opts.ProcessAt
		state = async.StateScheduled
	}

	r.seq++
	item := &queuedItem{
		id:        id,
		taskType:  taskType,
		payload:   cp,
		scope:     scope,
		opts:      opts,
		processAt: runAt,
		seq:       r.seq,
		index:     -1,
	}
	info := async.TaskInfo{
		ID:            id,
		Queue:         opts.Queue,
		Type:          taskType,
		State:         state,
		MaxRetry:      opts.MaxRetry,
		NextProcessAt: runAt,
	}

	if r.recordOnly[taskType] {
		r.records = append(r.records, RawRecord{
			ID:       id,
			TaskType: taskType,
			Payload:  cp,
			Queue:    opts.Queue,
			Scope:    scope,
			Options:  opts,
			Ran:      false,
		})
		r.mu.Unlock()
		return info, nil
	}

	if r.manual {
		r.liveIDs[idKey] = state
		heap.Push(&r.pending, item)
		r.mu.Unlock()
		return info, nil
	}

	// Eager mode: execute inline and await completion.
	if r.depth >= maxEagerDepth {
		r.mu.Unlock()
		return async.TaskInfo{}, fmt.Errorf("asynctest: eager recursion depth exceeded %d on %s", maxEagerDepth, taskType)
	}
	r.liveIDs[idKey] = async.StateActive
	r.depth++
	r.mu.Unlock()

	rec := r.executeItem(ctx, item)

	r.mu.Lock()
	r.depth--
	delete(r.liveIDs, idKey)
	r.records = append(r.records, rec)
	r.mu.Unlock()

	return info, nil
}

func (r *Runner) executeItem(ctx context.Context, item *queuedItem) RawRecord {
	rec := RawRecord{
		ID:       item.id,
		TaskType: item.taskType,
		Payload:  item.payload,
		Queue:    item.opts.Queue,
		Scope:    item.scope,
		Options:  item.opts,
		Attempt:  item.retried + 1,
	}
	handler, _, _, ok := r.registry.Lookup(item.taskType)
	if !ok {
		if r.allowUnregistered {
			return rec
		}
		err := fmt.Errorf("asynctest: no handler registered for task %q", item.taskType)
		rec.Err = err
		r.t.Errorf("%v", err)
		return rec
	}

	meta := async.Meta{
		TaskID:   item.id,
		TaskType: item.taskType,
		Queue:    item.opts.Queue,
		Scope:    item.scope,
		Retried:  item.retried,
		MaxRetry: item.opts.MaxRetry,
	}
	runCtx := async.WithMeta(ctx, meta)
	runCtx = async.WithResultRecorder(runCtx, func(v any) {
		rec.Result = v
	})
	var cancel context.CancelFunc
	if item.opts.Timeout > 0 {
		runCtx, cancel = context.WithTimeout(runCtx, item.opts.Timeout)
		defer cancel()
	}

	start := time.Now()
	err := handler(runCtx, item.payload, meta)
	rec.Duration = time.Since(start)
	rec.Ran = true
	rec.Err = err
	rec.SkippedRetry = async.IsSkipRetry(err)
	return rec
}

// TaskState implements async.Dispatcher.
func (r *Runner) TaskState(_ context.Context, _ async.Scope, queue async.Queue, taskID string) (async.TaskState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.liveIDs[string(queue)+":"+taskID]
	if !ok {
		return async.StateUnknown, async.ErrTaskNotFound
	}
	return state, nil
}

// Cancel implements async.Dispatcher.
func (r *Runner) Cancel(_ context.Context, _ async.Scope, queue async.Queue, taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.liveIDs, string(queue)+":"+taskID)
	for i, item := range r.pending {
		if item.opts.Queue == queue && item.id == taskID {
			heap.Remove(&r.pending, i)
			break
		}
	}
	return nil
}

// Close implements async.Dispatcher.
func (r *Runner) Close() error { return nil }

// Advance moves the fake clock forward by d (used in Manual mode for ProcessIn / retries).
func (r *Runner) Advance(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.now = r.now.Add(d)
}

// PendingCount returns how many tasks are waiting in Manual mode.
func (r *Runner) PendingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

// RunNext executes the earliest due task in Manual mode and returns its RawRecord.
// Returns (zero, false) if no task is currently due.
func (r *Runner) RunNext(ctx context.Context) (RawRecord, bool) {
	r.mu.Lock()
	if len(r.pending) == 0 || r.pending[0].processAt.After(r.now) {
		r.mu.Unlock()
		return RawRecord{}, false
	}
	item := heap.Pop(&r.pending).(*queuedItem)
	idKey := string(item.opts.Queue) + ":" + item.id
	r.liveIDs[idKey] = async.StateActive
	r.mu.Unlock()

	rec := r.executeItem(ctx, item)

	r.mu.Lock()
	r.records = append(r.records, rec)
	if rec.Err != nil && !rec.SkippedRetry && item.retried < item.opts.MaxRetry {
		item.retried++
		delay := 15 * time.Second
		if custom, ok := async.RetryDelayOf(rec.Err); ok {
			delay = custom
		}
		item.processAt = r.now.Add(delay)
		r.liveIDs[idKey] = async.StateRetry
		heap.Push(&r.pending, item)
	} else {
		delete(r.liveIDs, idKey)
	}
	r.mu.Unlock()
	return rec, true
}

// Drain runs all currently due tasks in Manual mode (including nested tasks enqueued
// during execution) until none are due. Returns the first terminal error encountered, if any.
func (r *Runner) Drain(ctx context.Context) error {
	var firstErr error
	for steps := 0; steps < 1000; steps++ {
		rec, ok := r.RunNext(ctx)
		if !ok {
			return firstErr
		}
		if rec.Err != nil && firstErr == nil {
			firstErr = rec.Err
		}
	}
	return errors.New("asynctest: Drain exceeded 1000 steps (possible infinite retry loop)")
}

// RawRecords returns a copy of all recorded task invocations.
func (r *Runner) RawRecords() []RawRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RawRecord, len(r.records))
	copy(out, r.records)
	return out
}

// Reset clears all recorded invocations and pending manual tasks.
func (r *Runner) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = nil
	r.pending = nil
	r.liveIDs = make(map[string]async.TaskState)
}

// Recorded returns all captured invocations of task type T with decoded Args, options,
// execution status, error, and optional recorded result.
func Recorded[T async.Task](r *Runner) []Record[T] {
	var zero T
	wantType := zero.TaskType()
	raw := r.RawRecords()
	var out []Record[T]
	for _, rec := range raw {
		if rec.TaskType != wantType {
			continue
		}
		var args T
		if len(rec.Payload) > 0 {
			if err := json.Unmarshal(rec.Payload, &args); err != nil {
				r.t.Errorf("asynctest: unmarshal recorded %s: %v", wantType, err)
			}
		}
		out = append(out, Record[T]{
			ID:           rec.ID,
			Args:         args,
			Queue:        rec.Queue,
			Scope:        rec.Scope,
			Options:      rec.Options,
			Attempt:      rec.Attempt,
			Ran:          rec.Ran,
			Err:          rec.Err,
			SkippedRetry: rec.SkippedRetry,
			Result:       rec.Result,
			Duration:     rec.Duration,
		})
	}
	return out
}
