// Package async provides the unified background task and event-dispatching framework for
// acceleration/, built on asynq (Redis) with an in-process worker backend for local
// development and deterministic test helpers in internal/async/asynctest.
//
// Every async task is a JSON-serializable Go struct implementing Task, placed in a
// `tasks/` subpackage of the domain package that owns it. Payloads must never contain
// secrets or large binary blobs; pass row IDs or object keys instead.
package async

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
)

// Queue identifies a logical async work queue.
type Queue string

// Scope identifies whether a task runs on the owning shard's Redis or the global Redis.
type Scope string

const (
	// ScopeShard runs on the owning shard's Redis (falling back to shared Redis on
	// single-shard deployments). Every task that reads or writes shard Postgres or a
	// shard bucket must use ScopeShard.
	ScopeShard Scope = "shard"
	// ScopeGlobal runs on the deployment's shared/global Redis.
	ScopeGlobal Scope = "global"
)

// Task is implemented by every typed background task struct.
type Task interface {
	// TaskType returns the stable wire identifier, e.g. "chatinbound:dispatch_message".
	TaskType() string
	// Queue returns the default queue for this task, e.g. QueueEvents.
	Queue() Queue
	// Scope returns ScopeShard or ScopeGlobal.
	Scope() Scope
}

// Configurable is optionally implemented by tasks that specify default enqueue options
// (timeout, max retries, retention, uniqueness). Caller-supplied options override defaults.
type Configurable interface {
	TaskOptions() []Option
}

// Handler processes one typed task.
type Handler[T Task] interface {
	Handle(ctx context.Context, task T) error
}

// HandlerFunc adapts a plain function to Handler[T].
type HandlerFunc[T Task] func(ctx context.Context, task T) error

// Handle invokes the underlying function.
func (f HandlerFunc[T]) Handle(ctx context.Context, task T) error { return f(ctx, task) }

// TaskState describes the lifecycle state of an enqueued task.
type TaskState string

const (
	StateUnknown   TaskState = "unknown"
	StatePending   TaskState = "pending"
	StateActive    TaskState = "active"
	StateScheduled TaskState = "scheduled"
	StateRetry     TaskState = "retry"
	StateArchived  TaskState = "archived"
	StateCompleted TaskState = "completed"
)

// TaskInfo is returned when a task is enqueued or inspected.
type TaskInfo struct {
	ID            string
	Queue         Queue
	Type          string
	State         TaskState
	MaxRetry      int
	Retried       int
	NextProcessAt time.Time
}

// EnqueueOptions holds resolved options for one task enqueue.
type EnqueueOptions struct {
	Queue     Queue
	TaskID    string
	MaxRetry  int
	HasRetry  bool
	Timeout   time.Duration
	ProcessIn time.Duration
	ProcessAt time.Time
	UniqueTTL time.Duration
	Retention time.Duration
}

// Option configures a single task enqueue or task defaults.
type Option func(*EnqueueOptions)

// WithQueue overrides the task's default queue.
func WithQueue(q Queue) Option {
	return func(o *EnqueueOptions) { o.Queue = q }
}

// WithTaskID sets a deterministic task ID; enqueueing a second live task with the same ID
// returns ErrTaskIDConflict.
func WithTaskID(id string) Option {
	return func(o *EnqueueOptions) { o.TaskID = id }
}

// WithMaxRetry sets the maximum number of retry attempts after the initial attempt.
func WithMaxRetry(n int) Option {
	return func(o *EnqueueOptions) {
		o.MaxRetry = n
		o.HasRetry = true
	}
}

// WithTimeout bounds a single attempt's context deadline.
func WithTimeout(d time.Duration) Option {
	return func(o *EnqueueOptions) { o.Timeout = d }
}

// WithProcessIn delays execution by d from now.
func WithProcessIn(d time.Duration) Option {
	return func(o *EnqueueOptions) {
		o.ProcessIn = d
		o.ProcessAt = time.Time{}
	}
}

// WithProcessAt schedules execution at t.
func WithProcessAt(t time.Time) Option {
	return func(o *EnqueueOptions) {
		o.ProcessAt = t
		o.ProcessIn = 0
	}
}

// WithUnique deduplicates identical (type, payload, queue) tasks for ttl.
func WithUnique(ttl time.Duration) Option {
	return func(o *EnqueueOptions) { o.UniqueTTL = ttl }
}

// WithRetention keeps the completed task record for ttl after completion.
func WithRetention(ttl time.Duration) Option {
	return func(o *EnqueueOptions) { o.Retention = ttl }
}

// ResolveOptions merges a task's defaults, optional Configurable.TaskOptions(), and
// caller-supplied options into one EnqueueOptions struct.
func ResolveOptions(task Task, opts ...Option) EnqueueOptions {
	resolved := EnqueueOptions{
		Queue:    task.Queue(),
		MaxRetry: 3,
		Timeout:  2 * time.Minute,
	}
	if configurable, ok := task.(Configurable); ok {
		for _, opt := range configurable.TaskOptions() {
			if opt != nil {
				opt(&resolved)
			}
		}
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}
	return resolved
}

// Dispatcher enqueues and inspects async tasks. Both production backends (asynq,
// in_process) and asynctest.Runner implement this interface.
type Dispatcher interface {
	EnqueueRaw(ctx context.Context, taskType string, payload []byte, scope Scope, opts EnqueueOptions) (TaskInfo, error)
	TaskState(ctx context.Context, scope Scope, queue Queue, taskID string) (TaskState, error)
	Cancel(ctx context.Context, scope Scope, queue Queue, taskID string) error
	Close() error
}

// Enqueue serializes task to JSON and submits it to d with resolved options.
func Enqueue[T Task](ctx context.Context, d Dispatcher, task T, opts ...Option) (TaskInfo, error) {
	if d == nil {
		return TaskInfo{}, errors.New("async: dispatcher is nil")
	}
	if task.TaskType() == "" {
		return TaskInfo{}, errors.New("async: task type is empty")
	}
	resolved := ResolveOptions(task, opts...)
	if !ValidQueue(string(resolved.Queue)) {
		return TaskInfo{}, fmt.Errorf("async: unknown queue %q for task %s", resolved.Queue, task.TaskType())
	}
	scope := task.Scope()
	if scope != ScopeShard && scope != ScopeGlobal {
		return TaskInfo{}, fmt.Errorf("async: invalid scope %q for task %s", scope, task.TaskType())
	}
	payload, err := json.Marshal(task)
	if err != nil {
		return TaskInfo{}, fmt.Errorf("async: marshal %s: %w", task.TaskType(), err)
	}
	return d.EnqueueRaw(ctx, task.TaskType(), payload, scope, resolved)
}

var (
	// ErrDuplicateTask is returned when a unique task is already enqueued.
	ErrDuplicateTask = asynq.ErrDuplicateTask
	// ErrTaskIDConflict is returned when a task with the same TaskID is already queued/active.
	ErrTaskIDConflict = asynq.ErrTaskIDConflict
	// ErrTaskNotFound is returned when inspecting or cancelling an unknown task ID.
	ErrTaskNotFound = asynq.ErrTaskNotFound
)

// SkipRetry marks err as permanent so neither asynq nor the in-process runner retries it.
func SkipRetry(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", err, asynq.SkipRetry)
}

// IsSkipRetry reports whether err was marked permanent via SkipRetry or asynq.SkipRetry.
func IsSkipRetry(err error) bool {
	return errors.Is(err, asynq.SkipRetry)
}

type retryAfterError struct {
	err   error
	delay time.Duration
}

func (e *retryAfterError) Error() string { return e.err.Error() }
func (e *retryAfterError) Unwrap() error { return e.err }

// RetryAfter asks the worker to retry err after delay instead of the default exponential
// backoff (useful for per-destination fairness or per-thread FIFO serialization).
func RetryAfter(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{err: err, delay: delay}
}

// RetryDelayOf extracts the explicit retry delay from RetryAfter, if present.
func RetryDelayOf(err error) (time.Duration, bool) {
	var target *retryAfterError
	if errors.As(err, &target) && target.delay > 0 {
		return target.delay, true
	}
	return 0, false
}

// Meta holds per-attempt execution metadata injected into handler contexts across all
// backends (asynq, in_process, and asynctest).
type Meta struct {
	TaskID   string
	TaskType string
	Queue    Queue
	Scope    Scope
	Retried  int
	MaxRetry int
}

type metaKey struct{}
type resultKey struct{}

// WithMeta attaches attempt metadata to ctx.
func WithMeta(ctx context.Context, meta Meta) context.Context {
	return context.WithValue(ctx, metaKey{}, meta)
}

// MetaFrom extracts attempt metadata from ctx.
func MetaFrom(ctx context.Context) (Meta, bool) {
	m, ok := ctx.Value(metaKey{}).(Meta)
	return m, ok
}

// RetryCount returns how many times the current task has already been retried (0 on first
// attempt). Works uniformly across asynq, in_process, and asynctest.
func RetryCount(ctx context.Context) (int, bool) {
	if m, ok := MetaFrom(ctx); ok {
		return m.Retried, true
	}
	return asynq.GetRetryCount(ctx)
}

// MaxRetry returns the configured max retry limit for the current task.
func MaxRetry(ctx context.Context) (int, bool) {
	if m, ok := MetaFrom(ctx); ok {
		return m.MaxRetry, true
	}
	return asynq.GetMaxRetry(ctx)
}

// TaskID returns the current task's ID, if available.
func TaskID(ctx context.Context) string {
	if m, ok := MetaFrom(ctx); ok && m.TaskID != "" {
		return m.TaskID
	}
	id, _ := asynq.GetTaskID(ctx)
	return id
}

// WithResultRecorder installs a callback used by asynctest to capture a handler's result.
func WithResultRecorder(ctx context.Context, fn func(any)) context.Context {
	return context.WithValue(ctx, resultKey{}, fn)
}

// RecordResult records an optional structured result from a task handler so tests using
// asynctest can inspect task results directly without re-querying storage. It is a no-op
// in production.
func RecordResult(ctx context.Context, value any) {
	if fn, ok := ctx.Value(resultKey{}).(func(any)); ok && fn != nil {
		fn(value)
	}
}
