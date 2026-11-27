package async

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// RawHandler handles a raw JSON task payload with execution metadata.
type RawHandler func(ctx context.Context, payload []byte, meta Meta) error

// PeriodicEntry defines a cron-scheduled task registered with the framework.
type PeriodicEntry struct {
	Name     string
	Spec     string
	TaskType string
	Scope    Scope
	Options  EnqueueOptions
	Payload  []byte
}

// Registry maps task type names to typed handlers and holds periodic cron registrations.
type Registry struct {
	mu        sync.RWMutex
	handlers  map[string]registeredHandler
	periodics []PeriodicEntry
}

type registeredHandler struct {
	scope   Scope
	queue   Queue
	handler RawHandler
}

// NewRegistry constructs an empty task registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]registeredHandler),
	}
}

// Register binds a typed Handler[T] to T's TaskType in r. Panics if the task type is
// empty or registered twice, catching wiring mistakes at startup.
func Register[T Task](r *Registry, h Handler[T]) {
	if r == nil {
		panic("async: nil registry")
	}
	if h == nil {
		panic("async: nil handler")
	}
	var zero T
	taskType := zero.TaskType()
	if taskType == "" {
		panic("async: cannot register task with empty TaskType()")
	}
	scope := zero.Scope()
	if scope != ScopeShard && scope != ScopeGlobal {
		panic(fmt.Sprintf("async: task %s has invalid scope %q", taskType, scope))
	}
	if !ValidQueue(string(zero.Queue())) {
		panic(fmt.Sprintf("async: task %s has unknown queue %q", taskType, zero.Queue()))
	}

	raw := func(ctx context.Context, payload []byte, meta Meta) error {
		var typed T
		if len(payload) > 0 {
			if err := json.Unmarshal(payload, &typed); err != nil {
				return SkipRetry(fmt.Errorf("async: unmarshal %s: %w", taskType, err))
			}
		}
		return h.Handle(ctx, typed)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[taskType]; exists {
		panic(fmt.Sprintf("async: duplicate handler registration for %q", taskType))
	}
	r.handlers[taskType] = registeredHandler{
		scope:   scope,
		queue:   zero.Queue(),
		handler: raw,
	}
}

// RegisterFunc binds a plain function as the handler for task type T.
func RegisterFunc[T Task](r *Registry, fn func(context.Context, T) error) {
	Register[T](r, HandlerFunc[T](fn))
}

// RegisterPeriodic registers a cron-scheduled task (standard 5-field cron or "@every 10m").
func RegisterPeriodic[T Task](r *Registry, spec string, task T, opts ...Option) error {
	if r == nil {
		return fmt.Errorf("async: nil registry")
	}
	if spec == "" {
		return fmt.Errorf("async: periodic spec is empty for %s", task.TaskType())
	}
	payload, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("async: marshal periodic %s: %w", task.TaskType(), err)
	}
	resolved := ResolveOptions(task, opts...)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.periodics = append(r.periodics, PeriodicEntry{
		Name:     task.TaskType(),
		Spec:     spec,
		TaskType: task.TaskType(),
		Scope:    task.Scope(),
		Options:  resolved,
		Payload:  payload,
	})
	return nil
}

// Lookup returns the handler registered for taskType.
func (r *Registry) Lookup(taskType string) (RawHandler, Scope, Queue, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.handlers[taskType]
	if !ok {
		return nil, "", "", false
	}
	return entry.handler, entry.scope, entry.queue, true
}

// TaskTypes returns all registered task type names in sorted order.
func (r *Registry) TaskTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Sorted(maps.Keys(r.handlers))
}

// Periodics returns all registered periodic entries matching scope (or all if scope == "").
func (r *Registry) Periodics(scope Scope) []PeriodicEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]PeriodicEntry, 0, len(r.periodics))
	for _, entry := range r.periodics {
		if scope == "" || entry.Scope == scope {
			out = append(out, entry)
		}
	}
	return out
}
