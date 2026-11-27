// Package tasks defines periodic maintenance tasks (pruning, redaction, telephony/DLC/sandbox sweepers).
package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

const (
	TypePruneDataChanges    = "maintenance:prune_data_changes"
	TypePruneScheduledTasks = "maintenance:prune_scheduled_tasks"
	TypeRedactToolOutput    = "maintenance:redact_tool_output"
	TypeSweepPhone          = "maintenance:sweep_phone"
	TypeSweepCallLegs       = "maintenance:sweep_call_legs"
	TypeHealOpenLegs        = "maintenance:heal_open_legs"
	TypePollDLC             = "maintenance:poll_dlc"
	TypeSweepDaytonaSecrets = "maintenance:sweep_daytona_secrets"

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

// SweepPhone sweeps stale call trunks, unanswered call bridges, unsettled phone number
// orders, and expired status URL tokens.
type SweepPhone struct{}

func (SweepPhone) TaskType() string   { return TypeSweepPhone }
func (SweepPhone) Queue() async.Queue { return async.QueueMaintenance }
func (SweepPhone) Scope() async.Scope { return async.ScopeGlobal }
func (SweepPhone) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(maintenanceTimeout),
		async.WithMaxRetry(maintenanceRetries),
	}
}

// SweepCallLegs sweeps call legs that never reported their end on this shard.
type SweepCallLegs struct{}

func (SweepCallLegs) TaskType() string   { return TypeSweepCallLegs }
func (SweepCallLegs) Queue() async.Queue { return async.QueueMaintenance }
func (SweepCallLegs) Scope() async.Scope { return async.ScopeShard }
func (SweepCallLegs) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(maintenanceTimeout),
		async.WithMaxRetry(maintenanceRetries),
	}
}

// HealOpenLegs reconciles this shard's open call leg counts against active legs.
type HealOpenLegs struct{}

func (HealOpenLegs) TaskType() string   { return TypeHealOpenLegs }
func (HealOpenLegs) Queue() async.Queue { return async.QueueMaintenance }
func (HealOpenLegs) Scope() async.Scope { return async.ScopeShard }
func (HealOpenLegs) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(2 * time.Minute),
		async.WithMaxRetry(maintenanceRetries),
	}
}

// PollDLC polls 10DLC vendors for campaign status updates whose webhook was missed.
type PollDLC struct{}

func (PollDLC) TaskType() string   { return TypePollDLC }
func (PollDLC) Queue() async.Queue { return async.QueueMaintenance }
func (PollDLC) Scope() async.Scope { return async.ScopeGlobal }
func (PollDLC) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(maintenanceTimeout),
		async.WithMaxRetry(maintenanceRetries),
	}
}

// SweepDaytonaSecrets deletes orphaned Daytona sandbox secrets whose session has ended.
type SweepDaytonaSecrets struct{}

func (SweepDaytonaSecrets) TaskType() string   { return TypeSweepDaytonaSecrets }
func (SweepDaytonaSecrets) Queue() async.Queue { return async.QueueMaintenance }
func (SweepDaytonaSecrets) Scope() async.Scope { return async.ScopeGlobal }
func (SweepDaytonaSecrets) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(10 * time.Minute),
		async.WithMaxRetry(maintenanceRetries),
	}
}

// Deps supplies maintenance callbacks and optional periodic intervals wired from the router.
type Deps struct {
	PruneDataChanges    func(ctx context.Context) error
	PruneScheduledTasks func(ctx context.Context) error
	RedactToolOutput    func(ctx context.Context) error

	SweepPhone         func(ctx context.Context) error
	SweepPhoneEvery    time.Duration
	SweepCallLegs      func(ctx context.Context) error
	SweepCallLegsEvery time.Duration
	HealOpenLegs       func(ctx context.Context) error
	HealOpenLegsEvery  time.Duration
	PollDLC            func(ctx context.Context) error
	PollDLCEvery       time.Duration
	SweepDaytona       func(ctx context.Context) error
	SweepDaytonaEvery  time.Duration
}

func everySpec(d time.Duration, fallback string) string {
	if d > 0 {
		return fmt.Sprintf("@every %s", d)
	}
	return fallback
}

// Register binds maintenance tasks and their periodic schedules to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.PruneDataChanges != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ PruneDataChanges) error {
			return deps.PruneDataChanges(ctx)
		})
		_ = async.RegisterPeriodic(reg, "@hourly", PruneDataChanges{})
	}
	if deps.PruneScheduledTasks != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ PruneScheduledTasks) error {
			return deps.PruneScheduledTasks(ctx)
		})
		_ = async.RegisterPeriodic(reg, "@hourly", PruneScheduledTasks{})
	}
	if deps.RedactToolOutput != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ RedactToolOutput) error {
			return deps.RedactToolOutput(ctx)
		})
		_ = async.RegisterPeriodic(reg, "@hourly", RedactToolOutput{})
	}
	if deps.SweepPhone != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ SweepPhone) error {
			return deps.SweepPhone(ctx)
		})
		_ = async.RegisterPeriodic(reg, everySpec(deps.SweepPhoneEvery, "@every 10m"), SweepPhone{})
	}
	if deps.SweepCallLegs != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ SweepCallLegs) error {
			return deps.SweepCallLegs(ctx)
		})
		_ = async.RegisterPeriodic(reg, everySpec(deps.SweepCallLegsEvery, "@every 10m"), SweepCallLegs{})
	}
	if deps.HealOpenLegs != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ HealOpenLegs) error {
			return deps.HealOpenLegs(ctx)
		})
		_ = async.RegisterPeriodic(reg, everySpec(deps.HealOpenLegsEvery, "@every 1m"), HealOpenLegs{})
	}
	if deps.PollDLC != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ PollDLC) error {
			return deps.PollDLC(ctx)
		})
		_ = async.RegisterPeriodic(reg, everySpec(deps.PollDLCEvery, "@every 15m"), PollDLC{})
	}
	if deps.SweepDaytona != nil {
		async.RegisterFunc(reg, func(ctx context.Context, _ SweepDaytonaSecrets) error {
			return deps.SweepDaytona(ctx)
		})
		_ = async.RegisterPeriodic(reg, everySpec(deps.SweepDaytonaEvery, "@hourly"), SweepDaytonaSecrets{})
	}
}
