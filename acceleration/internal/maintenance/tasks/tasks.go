// Package tasks defines periodic maintenance tasks (pruning, redaction, sweepers).
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

const (
	TypePruneDataChanges    = "maintenance:prune_data_changes"
	TypePruneScheduledTasks = "maintenance:prune_scheduled_tasks"
	TypeRedactToolOutput    = "maintenance:redact_tool_output"

	maintenanceTimeout = 5 * time.Minute
	maintenanceRetries = 1
)

// PruneDataChanges removes expired rows from the data-change log.
type PruneDataChanges struct{}

func (PruneDataChanges) TaskType() string   { return TypePruneDataChanges }
func (PruneDataChanges) Queue() async.Queue { return async.QueueMaintenance }
func (PruneDataChanges) Scope() async.Scope { return async.ScopeGlobal }
func (PruneDataChanges) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(maintenanceTimeout),
		async.WithMaxRetry(maintenanceRetries),
	}
}

// PruneScheduledTasks removes completed/expired scheduled tasks past retention.
type PruneScheduledTasks struct{}

func (PruneScheduledTasks) TaskType() string   { return TypePruneScheduledTasks }
func (PruneScheduledTasks) Queue() async.Queue { return async.QueueMaintenance }
func (PruneScheduledTasks) Scope() async.Scope { return async.ScopeGlobal }
func (PruneScheduledTasks) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(maintenanceTimeout),
		async.WithMaxRetry(maintenanceRetries),
	}
}

// RedactToolOutput redacts expired tool outputs according to retention rules.
type RedactToolOutput struct{}

func (RedactToolOutput) TaskType() string   { return TypeRedactToolOutput }
func (RedactToolOutput) Queue() async.Queue { return async.QueueMaintenance }
func (RedactToolOutput) Scope() async.Scope { return async.ScopeGlobal }
func (RedactToolOutput) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(maintenanceTimeout),
		async.WithMaxRetry(maintenanceRetries),
	}
}

// Deps supplies maintenance callbacks wired from the process store.
type Deps struct {
	PruneDataChanges    func(ctx context.Context) error
	PruneScheduledTasks func(ctx context.Context) error
	RedactToolOutput    func(ctx context.Context) error
}

// Register binds maintenance tasks and their periodic schedules to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.PruneDataChanges != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ PruneDataChanges) error {
			return deps.PruneDataChanges(ctx)
		})
		async.RegisterPeriodic(reg, "@hourly", PruneDataChanges{})
	}
	if deps.PruneScheduledTasks != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ PruneScheduledTasks) error {
			return deps.PruneScheduledTasks(ctx)
		})
		async.RegisterPeriodic(reg, "@hourly", PruneScheduledTasks{})
	}
	if deps.RedactToolOutput != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ RedactToolOutput) error {
			return deps.RedactToolOutput(ctx)
		})
		async.RegisterPeriodic(reg, "@hourly", RedactToolOutput{})
	}
}
