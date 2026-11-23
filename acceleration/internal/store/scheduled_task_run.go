package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ErrNoScheduledTaskRun says a customer's task has no run by that id.
var ErrNoScheduledTaskRun = errors.New("store: no such scheduled task run")

// The states a scheduled task run is in, and the channels a run is delivered to.
const (
	ScheduledTaskRunRunning   = "running"
	ScheduledTaskRunDelivered = "delivered"
	ScheduledTaskRunFailed    = "failed"
	ScheduledTaskRunMissed    = "missed"

	ScheduledTaskTriggerSchedule = "schedule"
	ScheduledTaskTriggerManual   = "manual"

	ScheduledTaskDeliveredConversation = "conversation"
	ScheduledTaskDeliveredCall         = "call"
	ScheduledTaskDeliveredPhone        = "phone"
)

// ScheduledTaskRun is one occurrence of a task that came due (or was run by
// hand), and what became of it.
type ScheduledTaskRun struct {
	bun.BaseModel `bun:"table:scheduled_task_runs,alias:str"`

	ID           string     `bun:"id,pk"`
	TaskID       string     `bun:"task_id,notnull"`
	CustomerID   string     `bun:"customer_id,notnull"`
	ScheduledFor time.Time  `bun:"scheduled_for,notnull"`
	Trigger      string     `bun:"trigger,notnull"`
	Prompt       string     `bun:"prompt,notnull"`
	State        string     `bun:"state,notnull"`
	DeliveredTo  string     `bun:"delivered_to,notnull"`
	SessionID    string     `bun:"session_id,notnull"`
	Attempts     int        `bun:"attempts,notnull"`
	Error        *string    `bun:"error"`
	StartedAt    *time.Time `bun:"started_at"`
	FinishedAt   *time.Time `bun:"finished_at"`
	// PostponedUntil is when a hold on this run ends (quiet hours), recorded
	// when the worker holds it. The next claim counts the occurrence's age
	// from here, so a held occurrence is delivered, not missed.
	PostponedUntil *time.Time `bun:"postponed_until"`
	CreatedAt      time.Time  `bun:"created_at,notnull"`
}

// Validate refuses a run the scheduler could not read back: one naming no customer or task, or
// a trigger or state it does not know.
func (r *ScheduledTaskRun) Validate() error {
	return validate("scheduled task run").
		required("customer_id", r.CustomerID).
		required("task_id", r.TaskID).
		oneOf("trigger", r.Trigger, ScheduledTaskTriggerSchedule, ScheduledTaskTriggerManual).
		oneOf("state", r.State, ScheduledTaskRunRunning, ScheduledTaskRunDelivered, ScheduledTaskRunFailed, ScheduledTaskRunMissed).
		done()
}

// ScheduledTaskRunFilter picks a task's runs, a page at a time.
type ScheduledTaskRunFilter struct {
	TaskID string
	Limit  int
	// After is the last run of the previous page.
	After *ScheduledTaskPosition
}

// ScheduledTaskRuns are the occurrences of scheduled tasks that came due or were run by hand,
// in the shared database, and what became of each.
type ScheduledTaskRuns struct {
	store *Store
}

// Insert stores a run a caller made by hand (trigger manual)
// or a missed occurrence. A scheduled occurrence that already has its run is
// left as it is.
func (r *ScheduledTaskRuns) Insert(ctx context.Context, run *ScheduledTaskRun) error {
	if run.ID == "" {
		run.ID = newID()
	}
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now().UTC()
	}
	if run.Trigger == "" {
		run.Trigger = ScheduledTaskTriggerSchedule
	}
	if run.State == "" {
		run.State = ScheduledTaskRunRunning
	}
	if err := run.Validate(); err != nil {
		return stack.Wrap(err)
	}
	_, err := r.store.global.NewInsert().Model(run).
		On("CONFLICT DO NOTHING").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: insert scheduled task run: %w", err))
	}
	return nil
}

// FinishManual records the outcome of a run made by hand.
func (r *ScheduledTaskRuns) FinishManual(ctx context.Context, customerID, runID, state, deliveredTo, sessionID, failure string) error {
	if customerID == "" || runID == "" {
		return stack.Wrap(errors.New("store: a customer and a run id are required"))
	}
	now := time.Now().UTC()
	update := r.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("state = ?", state).
		Set("delivered_to = ?", deliveredTo).
		Set("session_id = ?", sessionID).
		Set("finished_at = ?", now).
		Where("customer_id = ?", customerID).
		Where("id = ?", runID)
	if failure != "" {
		update = update.Set("error = ?", failure)
	} else {
		update = update.Set("error = NULL")
	}
	if _, err := update.Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: finish scheduled task run: %w", err))
	}
	return nil
}

// TakeCall records on a running run the session of the call
// about to be placed for it, and reports false when the run already has one:
// an earlier attempt, or a worker that stopped, placed a call for this
// occurrence, so the user is not rung again. reason is then the one the run
// records, such as how that call ended (SetCallReason), if any.
//
// Taking the call clears the run's error: with no call placed, the only error a running run
// holds is the reason an earlier attempt failed under (SetAttemptReason), and once a call is
// taken, how that call ends is the run's reason, as on a run that never had an earlier attempt.
// A call given back (ReleaseCall) leaves the run with no reason until the next failed attempt
// records its own.
func (r *ScheduledTaskRuns) TakeCall(ctx context.Context, customerID, runID, sessionID string) (took bool, reason string, err error) {
	result, err := r.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("session_id = ?", sessionID).
		Set("error = NULL").
		Where("customer_id = ?", customerID).
		Where("id = ?", runID).
		Where("state = ?", ScheduledTaskRunRunning).
		Where("session_id = ''").
		Exec(ctx)
	if err != nil {
		return false, "", stack.Wrap(fmt.Errorf("store: take scheduled task run call: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, "", stack.Wrap(fmt.Errorf("store: take scheduled task run call: %w", err))
	}
	if affected > 0 {
		return true, "", nil
	}
	err = r.store.global.NewSelect().Model((*ScheduledTaskRun)(nil)).
		ColumnExpr("coalesce(error, '')").
		Where("customer_id = ?", customerID).
		Where("id = ?", runID).
		Scan(ctx, &reason)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, "", stack.Wrap(fmt.Errorf("store: take scheduled task run call: %w", err))
	}
	return false, reason, nil
}

// SetCallReason records on a running run the reason the call
// sessionID, which it took, ended under, so a worker that takes the run over
// records that reason, not only that a call was placed.
func (r *ScheduledTaskRuns) SetCallReason(ctx context.Context, customerID, runID, sessionID, reason string) error {
	_, err := r.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("error = ?", reason).
		Where("customer_id = ?", customerID).
		Where("id = ?", runID).
		Where("state = ?", ScheduledTaskRunRunning).
		Where("session_id = ?", sessionID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set scheduled task run call reason: %w", err))
	}
	return nil
}

// SetAttemptReason records on a running run the reason an attempt failed under, for the
// attempt after it: a worker that takes the run over reads it from the row, as the worker that
// made the attempt reads it from memory. It writes only under the claim leasedBy, so a worker
// that lost its claim cannot overwrite the next one's, as far as the claim is checked
// when the statement starts (a write that waits on the run's row behind a takeover's claim still
// lands after it, with a reason of the same occurrence), and only while no call is placed for
// the run: the reason that call ended under is the run's (SetCallReason).
func (r *ScheduledTaskRuns) SetAttemptReason(ctx context.Context, customerID, runID, leasedBy, reason string) error {
	claimed := r.store.global.NewSelect().Model((*ScheduledTask)(nil)).
		ColumnExpr("1").
		Where("st.customer_id = str.customer_id").
		Where("st.id = str.task_id").
		Where("st.leased_by = ?", leasedBy)
	_, err := r.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("error = ?", reason).
		Where("customer_id = ?", customerID).
		Where("id = ?", runID).
		Where("state = ?", ScheduledTaskRunRunning).
		Where("session_id = ''").
		Where("EXISTS (?)", claimed).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set scheduled task run attempt reason: %w", err))
	}
	return nil
}

// ReleaseCall gives back a call TakeCall
// took and could not place, so a later attempt may ring.
func (r *ScheduledTaskRuns) ReleaseCall(ctx context.Context, customerID, runID, sessionID string) error {
	_, err := r.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("session_id = ''").
		Where("customer_id = ?", customerID).
		Where("id = ?", runID).
		Where("state = ?", ScheduledTaskRunRunning).
		Where("session_id = ?", sessionID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: release scheduled task run call: %w", err))
	}
	return nil
}

// Get returns the run by id of a customer's task.
func (r *ScheduledTaskRuns) Get(ctx context.Context, customerID, taskID, id string) (ScheduledTaskRun, error) {
	if customerID == "" || taskID == "" || id == "" {
		return ScheduledTaskRun{}, stack.Wrap(errors.New("store: a customer, a task and a run id are required"))
	}
	var run ScheduledTaskRun
	err := r.store.global.NewSelect().Model(&run).
		Where("customer_id = ?", customerID).
		Where("task_id = ?", taskID).
		Where("id = ?", id).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ScheduledTaskRun{}, stack.Wrap(fmt.Errorf("%w: %s", ErrNoScheduledTaskRun, id))
	}
	if err != nil {
		return ScheduledTaskRun{}, stack.Wrap(fmt.Errorf("store: scheduled task run: %w", err))
	}
	return run, nil
}

// Select returns a page of a task's runs, newest first, one more than
// ScheduledTaskLimit(filter.Limit).
func (r *ScheduledTaskRuns) Select(ctx context.Context, customerID string, filter ScheduledTaskRunFilter) ([]ScheduledTaskRun, error) {
	if customerID == "" || filter.TaskID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a task id are required"))
	}
	var runs []ScheduledTaskRun
	query := r.store.global.NewSelect().Model(&runs).
		Where("customer_id = ?", customerID).
		Where("task_id = ?", filter.TaskID)
	if after := filter.After; after != nil {
		query = query.Where("(created_at, id) < (?, ?)", after.CreatedAt, after.ID)
	}
	err := query.Order("created_at DESC", "id DESC").Limit(ScheduledTaskLimit(filter.Limit) + 1).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: scheduled task runs: %w", err))
	}
	return runs, nil
}

// LastFinishedAt is when each of a customer's tasks last finished a
// run, by task id, in one query; a task that never finished one is left out.
func (r *ScheduledTaskRuns) LastFinishedAt(ctx context.Context, customerID string, taskIDs []string) (map[string]time.Time, error) {
	var rows []struct {
		TaskID     string    `bun:"task_id"`
		FinishedAt time.Time `bun:"finished_at"`
	}
	err := r.store.global.NewSelect().Model((*ScheduledTaskRun)(nil)).
		Column("task_id").
		ColumnExpr("max(finished_at) AS finished_at").
		Where("customer_id = ?", customerID).
		Where("task_id IN (?)", bun.In(taskIDs)).
		Where("finished_at IS NOT NULL").
		Group("task_id").
		Scan(ctx, &rows)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: last scheduled task runs: %w", err))
	}
	last := make(map[string]time.Time, len(rows))
	for _, row := range rows {
		last[row.TaskID] = row.FinishedAt
	}
	return last, nil
}
