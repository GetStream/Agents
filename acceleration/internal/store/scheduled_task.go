package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ErrNoScheduledTask says a customer holds no task by that id.
var ErrNoScheduledTask = errors.New("store: no such scheduled task")

// ErrScheduledTaskIDTaken says a create named a task id the customer already holds.
var ErrScheduledTaskIDTaken = errors.New("store: a scheduled task with that id already exists")

// ErrScheduledTaskChanged says a save was of a copy of a task read before another write to it:
// saving it would undo that write, a pause or a worker's advance of the schedule.
var ErrScheduledTaskChanged = errors.New("store: the scheduled task changed since it was read")

// ErrTooManyScheduledTasks says a create or resume would put a user over their
// active-task limit on the task's agent config.
var ErrTooManyScheduledTasks = errors.New("store: too many scheduled tasks")

// The states a scheduled task is in.
const (
	ScheduledTaskActive    = "active"
	ScheduledTaskPaused    = "paused"
	ScheduledTaskCompleted = "completed"
	ScheduledTaskFailed    = "failed"
)

// scheduledTaskStaleRun is how long a run may sit in running before another
// worker takes it over: the worker that claimed it renews the task's lease as
// it goes, so a run this old in running belongs to one that died.
const scheduledTaskStaleRun = 10 * time.Minute

// scheduledTaskManualRunLost is how long a run made by hand may sit in running before Prune
// writes it missed. No lease covers it: only the router that took the request finishes it, so
// one that stopped mid-delivery leaves it running for good, and its task with it. Every wait a
// delivery makes is bounded (a call rung and held, a reply, a busy conversation), together by
// hours at most, so a run a day old in running belongs to a router that stopped.
const scheduledTaskManualRunLost = 24 * time.Hour

// maxScheduledTaskRunAttempts is how many times one occurrence is claimed
// before it is failed rather than taken over again: the first claim, the one
// taking it up after a quiet-hours hold, and one after a lost worker. Each
// takeover follows a worker that died mid-delivery, so a third one says the
// delivery is what stops them, and taking it up again would stop another
// router every scheduledTaskStaleRun until the occurrence's grace ran out.
const maxScheduledTaskRunAttempts = 3

// scheduledTaskRunWorkersLost is the reason a run failed at
// maxScheduledTaskRunAttempts with no call placed is recorded under:
// scheduling.ReasonFailed, one of the codes a device is shown, which this
// package cannot import.
const scheduledTaskRunWorkersLost = "delivery_failed"

// scheduledTaskRunPaused is the reason the cut-over of a move settles the source's run of an
// occurrence it skips under: scheduling.ReasonPaused, which this package cannot import.
const scheduledTaskRunPaused = "task_paused"

// scheduledTaskRunRungAlready is the reason a run failed at
// maxScheduledTaskRunAttempts is recorded under when a call was placed for it
// and no end recorded: scheduling.ReasonRungAlready, which leaves the run out
// of the task's failure streak.
const scheduledTaskRunRungAlready = "already_called"

// scheduledTaskLimitLockSeed is the second argument to hashtextextended for the lock a user's
// creates on one config take turns under while they count the active limit. The value only has
// to differ from the other seeds.
const scheduledTaskLimitLockSeed = 3496

// ScheduledTask is something the agent agreed to do for one end user at a
// wall-clock time in that user's zone, once or on a repeat.
type ScheduledTask struct {
	bun.BaseModel `bun:"table:scheduled_tasks,alias:st"`

	ID         string `bun:"id,pk,notnull"`
	CustomerID string `bun:"customer_id,pk,notnull"`
	ConfigID   string `bun:"config_id,notnull"`
	UserID     string `bun:"user_id,notnull"`
	CallerKind string `bun:"caller_kind,notnull"`
	// APIKey and UsageCategories are those of whoever scheduled the task, the key a session's
	// was opened with when its agent scheduled it: every occurrence is owned by them.
	APIKey          string   `bun:"api_key,notnull"`
	UsageCategories []string `bun:"usage_categories,array,nullzero"`
	// OriginSessionID is the session the task was created in, SessionID the one
	// it follows up in, ConversationID that session's Chat conversation.
	OriginSessionID string `bun:"origin_session_id,notnull"`
	SessionID       string `bun:"session_id,notnull"`
	ConversationID  string `bun:"conversation_id,notnull"`
	Title           string `bun:"title,notnull"`
	Instructions    string `bun:"instructions,notnull"`
	// ChannelThread is the thread channel of the external thread the task was
	// made in (channel_threads.channel_id): an SMS, WhatsApp or 1:1 iMessage thread.
	// Empty for a task made anywhere else.
	ChannelThread string `bun:"channel_thread,notnull"`
	// StartLocal is the first occurrence as the user's clock reads it, held in
	// UTC so its fields are the wall clock's; Timezone says which clock.
	StartLocal time.Time  `bun:"start_local,notnull"`
	Timezone   string     `bun:"timezone,notnull"`
	RRule      string     `bun:"rrule,nullzero"`
	UntilDate  *time.Time `bun:"until_date"`
	// NextRunAt is the next occurrence as an instant. Nil when there is none.
	NextRunAt *time.Time `bun:"next_run_at"`
	State     string     `bun:"state,notnull"`
	// MovePaused is set on a task a data move imported paused because it was active where
	// it came from: the cut-over resumes it (ResumeMovedRows). A pause or a resume by the
	// user or the agent clears it, since they have decided; any other change keeps it.
	MovePaused bool     `bun:"move_paused,notnull"`
	Channels   []string `bun:"channels,type:jsonb,notnull"`
	// QuietHours is the span of the user's local day an occurrence is held
	// through rather than delivered into. Nil is no hold.
	QuietHours *ScheduledTaskQuietHours `bun:"quiet_hours,type:jsonb"`
	Custom     map[string]any           `bun:"custom,type:jsonb,nullzero"`
	LeaseUntil *time.Time               `bun:"lease_until"`
	LeasedBy   string                   `bun:"leased_by,notnull"`
	// ConsecutiveFailures is how many occurrences in a row failed to deliver.
	ConsecutiveFailures int       `bun:"consecutive_failures,notnull"`
	CreatedBy           string    `bun:"created_by,notnull"`
	CreatedAt           time.Time `bun:"created_at,notnull"`
	UpdatedAt           time.Time `bun:"updated_at,notnull"`
}

// ScheduledTaskQuietHours is the span of the user's local day an occurrence
// is held through rather than delivered into, as the clock reads it in the
// task's zone: "22:00" to "07:00" holds the night.
type ScheduledTaskQuietHours struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// ScheduledTaskPosition is where a page of tasks or runs ends.
type ScheduledTaskPosition struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"id"`
}

const (
	defaultScheduledTaskLimit = 25
	maxScheduledTaskLimit     = 200
)

// ScheduledTaskLimit is the page size a task or run list uses.
func ScheduledTaskLimit(asked int) int {
	return ClampLimit(asked, defaultScheduledTaskLimit, maxScheduledTaskLimit)
}

// ScheduledTaskScope narrows a list to one user's tasks as one kind of caller
// may reach them: Kinds are the stored caller kinds in reach (a verified user
// reaches their own kind and the server's). An empty UserID is every user's.
type ScheduledTaskScope struct {
	UserID string
	Kinds  []string
}

// ScheduledTaskFilter narrows a task list, a page at a time.
type ScheduledTaskFilter struct {
	Scope     ScheduledTaskScope
	State     string
	ConfigID  string
	SessionID string
	// Configs narrows the list to the tasks on these configs and those on none, when not nil.
	Configs *[]string
	Limit   int
	// After is the last task of the previous page.
	After *ScheduledTaskPosition
}

// Validate refuses a task no worker could deliver: one naming no customer, user or title, or a
// state the scheduler does not know.
func (t *ScheduledTask) Validate() error {
	return validate("scheduled task").
		required("customer_id", t.CustomerID).
		required("id", t.ID).
		required("user_id", t.UserID).
		required("title", t.Title).
		oneOf("state", t.State, ScheduledTaskActive, ScheduledTaskPaused, ScheduledTaskCompleted, ScheduledTaskFailed).
		done()
}

// ScheduledTasks are what agents agreed to do for their end users later, in the shared
// database, and the claims workers hold on them while they deliver an occurrence.
type ScheduledTasks struct {
	store *Store
}

// Insert stores a new task, refusing one over the user's active limit on the task's config or
// under an id already taken. It fills in the id when none is named.
func (r *ScheduledTasks) Insert(ctx context.Context, task *ScheduledTask, maxActive int) error {
	if task.ID == "" {
		task.ID = newID()
	}
	if task.State == "" {
		task.State = ScheduledTaskActive
	}
	if task.CreatedBy == "" {
		task.CreatedBy = "api"
	}
	if len(task.Channels) == 0 {
		task.Channels = []string{ScheduledTaskDeliveredConversation}
	}
	if err := task.Validate(); err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC()
	task.CreatedAt = now
	task.UpdatedAt = now

	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := refuseDeletedConfig(ctx, tx, task.CustomerID, task.ConfigID); err != nil {
			return err
		}
		if maxActive > 0 {
			if err := refuseOverLimit(ctx, tx, task, maxActive); err != nil {
				return err
			}
		}
		if _, err := tx.NewInsert().Model(task).Exec(ctx); err != nil {
			if constraint(err) == "scheduled_tasks_pkey" {
				return ErrScheduledTaskIDTaken
			}
			return fmt.Errorf("store: create scheduled task: %w", err)
		}
		return nil
	}))
}

// refuseOverLimit refuses one more active task for the task's user on its config when they hold
// maxActive already. Two creates or resumes at once would otherwise both count room for one more,
// so the count is taken under a lock on the user and config held until the transaction ends.
func refuseOverLimit(ctx context.Context, tx bun.Tx, task *ScheduledTask, maxActive int) error {
	if err := lockTaskLimits(ctx, tx, task.CustomerID, []scheduledTaskHolder{{task.UserID, task.ConfigID}}); err != nil {
		return err
	}
	count, err := activeScheduledTasks(ctx, tx, task.CustomerID, task.UserID, task.ConfigID)
	if err != nil {
		return err
	}
	if count >= maxActive {
		return ErrTooManyScheduledTasks
	}
	return nil
}

// lockTaskLimits takes the lock each of a customer's users on a config counts their active
// limit under, held until tx ends. Sorted, so two transactions taking several wait for each
// other instead of deadlocking.
func lockTaskLimits(ctx context.Context, tx bun.Tx, customerID string, holders []scheduledTaskHolder) error {
	keys := make([]string, 0, len(holders))
	for _, holder := range holders {
		keys = append(keys, customerID+"/"+holder.user+"/"+holder.config)
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	// unnest reads the array in order, so the locks are taken in it.
	if _, err := tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock(hashtextextended(key, ?)) FROM unnest(?::text[]) AS key",
		scheduledTaskLimitLockSeed, pgdialect.Array(keys)); err != nil {
		return fmt.Errorf("store: lock scheduled task limit: %w", err)
	}
	return nil
}

// refuseDeletedConfig refuses an active task on a deleted config, and returns the config's
// scheduling settings (the zero value when there is no row). The caller read
// the config live; a delete landing after that read pauses the config's tasks
// once its own write is in (ScheduledTasks.PauseForConfig), so a task written
// meanwhile would be left active on nothing. The config row is locked FOR SHARE
// until the task's transaction ends, which makes the two wait for each other:
//
//   - A delete that wrote first is waited for, and under READ COMMITTED the row
//     is read as the delete left it, deleted, so the task is refused
//     (https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED).
//   - A delete that comes second waits for the task to commit, and its pause
//     then finds the task.
//
// FOR SHARE, not FOR KEY SHARE as lockBoundConnections takes: the delete is a
// plain UPDATE of deleted_at, whose FOR NO KEY UPDATE conflicts only with
// FOR SHARE and stronger (table 13.3,
// https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS).
// A config is only ever soft-deleted (AgentConfigs.Delete), so an id with no row
// is not a delete and is left to the caller, which resolved it.
func refuseDeletedConfig(ctx context.Context, tx bun.Tx, customerID, configID string) (AgentScheduling, error) {
	var configs []AgentConfig
	err := tx.NewSelect().Model(&configs).
		Column("deleted_at", "scheduling").
		Where("customer_id = ?", customerID).
		Where("id = ?", configID).
		For("SHARE").
		Scan(ctx)
	if err != nil {
		return AgentScheduling{}, fmt.Errorf("store: lock scheduled task config: %w", err)
	}
	if len(configs) == 0 {
		return AgentScheduling{}, nil
	}
	if configs[0].DeletedAt != nil {
		return AgentScheduling{}, unknownAgentConfig(configID)
	}
	return configs[0].Scheduling, nil
}

// lockScheduledTask locks a task's row before its transaction writes the
// task's runs. Every transaction that writes both a task and its runs takes
// the task row first: the claim (FOR UPDATE SKIP LOCKED), a postpone and the
// pause of a config's tasks (ScheduledTasks.PauseForConfig) write the task
// before its runs, so one writing the runs first would wait for them while
// holding what they wait for, and Postgres would abort one of the two as a
// deadlock (SQLSTATE 40P01). FOR NO KEY UPDATE is what the task's own UPDATE
// takes, so it does not hold back a run insert's foreign key check (table
// 13.3, https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS).
// It returns the task's stored state, empty when there is no such task.
func lockScheduledTask(ctx context.Context, tx bun.Tx, customerID, id string) (string, error) {
	var locked []string
	err := tx.NewSelect().Model((*ScheduledTask)(nil)).
		Column("state").
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		For("NO KEY UPDATE").
		Scan(ctx, &locked)
	if err != nil {
		return "", fmt.Errorf("store: lock scheduled task: %w", err)
	}
	if len(locked) == 0 {
		return "", nil
	}
	return locked[0], nil
}

// Get returns one of a customer's tasks by id.
func (r *ScheduledTasks) Get(ctx context.Context, customerID, id string) (ScheduledTask, error) {
	if customerID == "" || id == "" {
		return ScheduledTask{}, stack.Wrap(errors.New("store: a customer and a task id are required"))
	}
	var task ScheduledTask
	err := r.store.global.NewSelect().Model(&task).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ScheduledTask{}, stack.Wrap(fmt.Errorf("%w: %s", ErrNoScheduledTask, id))
	}
	if err != nil {
		return ScheduledTask{}, stack.Wrap(fmt.Errorf("store: scheduled task: %w", err))
	}
	return task, nil
}

// Select returns a page of a customer's tasks, newest first, one more than
// ScheduledTaskLimit(filter.Limit).
func (r *ScheduledTasks) Select(ctx context.Context, customerID string, filter ScheduledTaskFilter) ([]ScheduledTask, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}
	var tasks []ScheduledTask
	query := scopedTasks(r.store.global.NewSelect().Model(&tasks).Where("customer_id = ?", customerID), filter.Scope)
	if filter.Configs != nil {
		query = query.WhereGroup(" AND ", func(q *bun.SelectQuery) *bun.SelectQuery {
			q = q.Where("config_id = ''")
			if len(*filter.Configs) > 0 {
				q = q.WhereOr("config_id IN (?)", bun.In(*filter.Configs))
			}
			return q
		})
	}
	if filter.State != "" {
		query = query.Where("state = ?", filter.State)
	}
	if filter.ConfigID != "" {
		query = query.Where("config_id = ?", filter.ConfigID)
	}
	if filter.SessionID != "" {
		query = query.Where("session_id = ?", filter.SessionID)
	}
	if after := filter.After; after != nil {
		query = query.Where("(created_at, id) < (?, ?)", after.CreatedAt, after.ID)
	}
	err := query.Order("created_at DESC", "id DESC").Limit(ScheduledTaskLimit(filter.Limit) + 1).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: scheduled tasks: %w", err))
	}
	return tasks, nil
}

// Configs are the live agent configs the tasks in scope are on, each once, with their tags:
// what a list narrowed to the configs a user may reach (ScheduledTaskFilter.Configs) is
// decided by.
func (r *ScheduledTasks) Configs(ctx context.Context, customerID string, scope ScheduledTaskScope) ([]AgentConfig, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}
	tasks := scopedTasks(r.store.global.NewSelect().Model((*ScheduledTask)(nil)).Column("config_id").
		Where("customer_id = ?", customerID), scope)
	var configs []AgentConfig
	err := r.store.global.NewSelect().Model(&configs).Column("id", "tags").
		Where("customer_id = ?", customerID).
		Where("id IN (?)", tasks).
		Where("deleted_at IS NULL").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: scheduled task configs: %w", err))
	}
	return configs, nil
}

// scopedTasks narrows a query of tasks to scope's.
func scopedTasks(query *bun.SelectQuery, scope ScheduledTaskScope) *bun.SelectQuery {
	if scope.UserID != "" {
		query = query.Where("user_id = ?", scope.UserID)
		if len(scope.Kinds) > 0 {
			query = query.Where("caller_kind IN (?)", bun.In(scope.Kinds))
		}
	}
	return query
}

// Update replaces a task's editable fields and clears any claim, so
// a worker finishing an older claim cannot advance what this save changed.
// An occurrence still running (held by quiet hours, or cut short when its
// worker lost the claim) is written missed: the change decides what happens
// next, and the occurrence is neither left running for good nor taken up
// later with the instructions it had before the change. An active one-shot
// whose occurrence that was is due again at the save, so it is delivered as
// it now reads (and held again if quiet hours still hold); a repeating task
// goes on to its next occurrence. An active one-shot whose occurrence a
// worker is delivering under a live lease, not held, keeps that claim and
// run: the delivery under way completes with the text it has, since bringing
// the one-shot forward would deliver it a second time. The caller reads the
// task, changes it, and saves it back. An active one-shot whose settled
// occurrence placed a call is completed instead: a second call is worse than
// an edit missed. A copy read before another write to the task is refused
// (ErrScheduledTaskChanged): saving it would undo that write. A save that makes
// a task active again is refused over the user's active limit on the config
// (ErrTooManyScheduledTasks).
func (r *ScheduledTasks) Update(ctx context.Context, task *ScheduledTask) error {
	return r.update(ctx, task, nil)
}

// Close saves a task whose one-shot slept through its time while paused, with
// the missed run of that occurrence, in one transaction: a save refused leaves
// no run behind.
func (r *ScheduledTasks) Close(ctx context.Context, task *ScheduledTask, missed *ScheduledTaskRun) error {
	return r.update(ctx, task, missed)
}

func (r *ScheduledTasks) update(ctx context.Context, task *ScheduledTask, missed *ScheduledTaskRun) error {
	if err := task.Validate(); err != nil {
		return stack.Wrap(err)
	}
	read := task.UpdatedAt
	task.UpdatedAt = time.Now().UTC()
	task.LeasedBy = ""
	task.LeaseUntil = nil
	if len(task.Channels) == 0 {
		task.Channels = []string{ScheduledTaskDeliveredConversation}
	}
	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var scheduling AgentScheduling
		if task.State == ScheduledTaskActive {
			var err error
			if scheduling, err = refuseDeletedConfig(ctx, tx, task.CustomerID, task.ConfigID); err != nil {
				return err
			}
		}
		stored, err := lockScheduledTask(ctx, tx, task.CustomerID, task.ID)
		if err != nil {
			return err
		}
		if task.State == ScheduledTaskActive && stored != "" && stored != ScheduledTaskActive {
			if err := refuseOverLimit(ctx, tx, task, scheduling.MaxActive()); err != nil {
				return err
			}
		}
		oneShot := task.RRule == "" && task.State == ScheduledTaskActive && task.NextRunAt != nil
		// The run of a one-shot's occurrence a worker holds: its lease is live
		// and no quiet-hours hold is on it. The task row is locked, so no
		// renewal or claim moves the lease meanwhile.
		var delivering []string
		if oneShot {
			if err := tx.NewSelect().Model((*ScheduledTaskRun)(nil)).
				Column("str.id").
				Join("JOIN scheduled_tasks AS st ON st.customer_id = str.customer_id AND st.id = str.task_id").
				Where("st.customer_id = ?", task.CustomerID).
				Where("st.id = ?", task.ID).
				Where("st.lease_until > now()").
				Where("str.trigger = ?", ScheduledTaskTriggerSchedule).
				Where("str.scheduled_for = ?", *task.NextRunAt).
				Where("(str.postponed_until IS NULL OR str.postponed_until <= now())").
				For("UPDATE OF st").
				Scan(ctx, &delivering); err != nil {
				return fmt.Errorf("store: save scheduled task: %w", err)
			}
		}
		// A run made by hand is left to the delivery that made it, which
		// records its outcome, or to Prune once its router stopped.
		settle := tx.NewUpdate().Model((*ScheduledTaskRun)(nil)).
			Set("state = ?", ScheduledTaskRunMissed).
			Set("finished_at = now()").
			Where("customer_id = ?", task.CustomerID).
			Where("task_id = ?", task.ID).
			Where("trigger = ?", ScheduledTaskTriggerSchedule).
			Where("state = ?", ScheduledTaskRunRunning)
		if len(delivering) > 0 {
			settle = settle.Where("id NOT IN (?)", bun.In(delivering))
		}
		var settled []ScheduledTaskRun
		if err := settle.Returning("scheduled_for, session_id").Scan(ctx, &settled); err != nil {
			return fmt.Errorf("store: settle scheduled task runs: %w", err)
		}
		// The occurrence keeps its run, so a one-shot left at it would only
		// be advanced past it: it comes due again at the save instead, on the
		// database's clock, which the claim compares against.
		if oneShot {
			for _, run := range settled {
				if !run.ScheduledFor.Equal(*task.NextRunAt) {
					continue
				}
				if run.SessionID != "" {
					task.State, task.NextRunAt = ScheduledTaskCompleted, nil
					break
				}
				again, err := transactionStart(ctx, tx)
				if err != nil {
					return fmt.Errorf("store: save scheduled task: %w", err)
				}
				task.NextRunAt = &again
				break
			}
		}
		// Who scheduled the task is fixed when it is created.
		save := tx.NewUpdate().Model(task).ExcludeColumn("customer_id", "created_at", "api_key", "usage_categories")
		if len(delivering) > 0 {
			save = save.ExcludeColumn("leased_by", "lease_until")
		}
		result, err := save.
			Where("customer_id = ?", task.CustomerID).
			Where("id = ?", task.ID).
			Where("updated_at = ?", read).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: save scheduled task: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: save scheduled task: %w", err)
		}
		if affected == 0 {
			// The row is locked, so it is either there and changed or not there at all.
			found, err := tx.NewSelect().Model((*ScheduledTask)(nil)).
				Where("customer_id = ?", task.CustomerID).
				Where("id = ?", task.ID).
				Exists(ctx)
			if err != nil {
				return fmt.Errorf("store: save scheduled task: %w", err)
			}
			if found {
				return fmt.Errorf("%w: %s", ErrScheduledTaskChanged, task.ID)
			}
			return fmt.Errorf("%w: %s", ErrNoScheduledTask, task.ID)
		}
		if missed != nil {
			return insertScheduledTaskRun(ctx, tx, missed)
		}
		return nil
	}))
}

// Delete removes a task and, by cascade, its runs. The changes recorded of them hold the user's
// words, so each becomes a delete with no payload, as forgetting the user makes them (deleteTasks).
func (r *ScheduledTasks) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a task id are required"))
	}
	deleted, err := deleteTasks(ctx, r.store.global, customerID, func(q bun.QueryBuilder) bun.QueryBuilder {
		return q.Where("id = ?", id)
	}, true, deletedTaskChanges)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete scheduled task: %w", err))
	}
	if deleted.Tasks == 0 {
		return stack.Wrap(fmt.Errorf("%w: %s", ErrNoScheduledTask, id))
	}
	return nil
}

// DeleteForSession removes every task that follows up in a
// session, as deleting that session must: a user who deleted a conversation
// is not contacted from it again. The changes recorded of those tasks and
// their runs hold the user's words, so each becomes a delete with no payload,
// as forgetting the user makes them (deleteTasks).
func (r *ScheduledTasks) DeleteForSession(ctx context.Context, customerID, sessionID string) error {
	if customerID == "" || sessionID == "" {
		return nil
	}
	_, err := deleteTasks(ctx, r.store.global, customerID, func(q bun.QueryBuilder) bun.QueryBuilder {
		return q.Where("session_id = ? OR origin_session_id = ?", sessionID, sessionID)
	}, true, deletedTaskChanges)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete scheduled tasks for session: %w", err))
	}
	return nil
}

// deletedTaskChanges picks, as dc, the recorded changes of the tasks deleteTasks deletes and of
// their runs, by id: a task's changes from before it followed up in a session do not name it.
// data_changes_tasks_idx serves it.
const deletedTaskChanges = changedTask + ` IN (SELECT id FROM deleted)`

// changedTask is the id of the task that a recorded change dc of a task or of a run is of, as
// data_changes_tasks_idx reads it.
const changedTask = `COALESCE(dc.payload->>'task_id', dc.payload->>'id')`

// SetSession records the session a task follows up in, once its
// first occurrence has opened one. It touches nothing else: the task may be
// claimed when this runs, and clearing the claim would fence the worker out
// of finishing the run.
func (r *ScheduledTasks) SetSession(ctx context.Context, customerID, taskID, sessionID string) error {
	_, err := r.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("session_id = ?", sessionID).
		Set("updated_at = now()").
		Where("customer_id = ?", customerID).
		Where("id = ?", taskID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set scheduled task session: %w", err))
	}
	return nil
}

// CountActive is how many active tasks a user holds on one agent
// config, which is what that config's limit counts.
func (r *ScheduledTasks) CountActive(ctx context.Context, customerID, userID, configID string) (int, error) {
	count, err := activeScheduledTasks(ctx, r.store.global, customerID, userID, configID)
	return count, stack.Wrap(err)
}

func activeScheduledTasks(ctx context.Context, db bun.IDB, customerID, userID, configID string) (int, error) {
	count, err := db.NewSelect().Model((*ScheduledTask)(nil)).
		Where("customer_id = ?", customerID).
		Where("user_id = ?", userID).
		Where("config_id = ?", configID).
		Where("state = ?", ScheduledTaskActive).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: count scheduled tasks: %w", err)
	}
	return count, nil
}

// ClaimedScheduledTask is a task a worker has claimed, with the run it is to
// deliver. AlreadySettled says the occurrence's run is finished already and
// only the task's advance was lost; the worker settles the task without
// delivering again. WorkersLost says this claim failed the run itself, every
// worker it was started under having been lost, which the worker logs.
type ClaimedScheduledTask struct {
	Task           ScheduledTask
	Run            ScheduledTaskRun
	LeasedBy       string
	AlreadySettled bool
	WorkersLost    bool
}

// Claim claims up to limit due tasks of shard under a lease of the
// given length, in one transaction, comparing against the database's own
// clock. Each claim inserts the occurrence's run, or takes over a stale one.
func (r *ScheduledTasks) Claim(ctx context.Context, limit int, lease time.Duration, shard ShardClaim) ([]ClaimedScheduledTask, error) {
	if limit < 1 {
		limit = 1
	}
	claimed := []ClaimedScheduledTask{}
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		now, err := transactionStart(ctx, tx)
		if err != nil {
			return fmt.Errorf("store: claim scheduled tasks: %w", err)
		}
		var tasks []ScheduledTask
		due := tx.NewSelect().Model(&tasks).
			Where("state = ?", ScheduledTaskActive).
			Where("next_run_at <= now()").
			Where("(lease_until IS NULL OR lease_until < now())")
		if shard.filtered() {
			claimable, args := shard.scheduledTaskClaimable()
			due = due.Where(claimable, args...)
		}
		err = due.
			Order("next_run_at ASC").
			Limit(limit).
			For("UPDATE SKIP LOCKED").
			Scan(ctx)
		if err != nil {
			return fmt.Errorf("store: claim scheduled tasks: %w", err)
		}
		runs, err := occurrenceRuns(ctx, tx, tasks)
		if err != nil {
			return fmt.Errorf("store: claim scheduled tasks: %w", err)
		}
		leaseUntil := now.Add(lease)
		var inserted []ScheduledTaskRun
		var leased []ScheduledTask
		for _, task := range tasks {
			if task.NextRunAt == nil {
				continue
			}
			scheduledFor := *task.NextRunAt
			leasedBy := newID()

			run, found := runs[occurrenceOf(task.CustomerID, task.ID, scheduledFor)]
			if found {
				lost := false
				switch {
				case run.State != ScheduledTaskRunRunning:
					// The occurrence was settled and the advance was lost.
				case run.StartedAt != nil && now.Sub(*run.StartedAt) < scheduledTaskStaleRun &&
					!heldRun(run, now):
					// A worker had it recently and its lease lapsed without a
					// renewal only just now; park the task until the run is
					// stale enough to take over.
					park := run.StartedAt.Add(scheduledTaskStaleRun)
					if _, err := tx.NewUpdate().Model((*ScheduledTask)(nil)).
						Set("lease_until = ?", park).
						Set("updated_at = ?", now).
						Where("customer_id = ?", task.CustomerID).
						Where("id = ?", task.ID).
						Exec(ctx); err != nil {
						return fmt.Errorf("store: claim scheduled tasks: %w", err)
					}
					continue
				case run.Attempts >= maxScheduledTaskRunAttempts:
					failure := capFailure(run)
					if _, err := tx.NewUpdate().Model((*ScheduledTaskRun)(nil)).
						Set("state = ?", ScheduledTaskRunFailed).
						Set("error = ?", failure).
						Set("finished_at = ?", now).
						Where("id = ?", run.ID).
						Exec(ctx); err != nil {
						return fmt.Errorf("store: fail scheduled task run: %w", err)
					}
					run.State, run.Error, run.FinishedAt = ScheduledTaskRunFailed, &failure, &now
					lost = true
				default:
					// Stale: take it over under the same run, so the same
					// command id, so a copy that did land is refused. The
					// reason a call it placed ended under stays for the
					// worker taking over (ScheduledTaskRuns.SetCallReason).
					if _, err := tx.NewUpdate().Model((*ScheduledTaskRun)(nil)).
						Set("attempts = attempts + 1").
						Set("started_at = ?", now).
						Where("id = ?", run.ID).
						Exec(ctx); err != nil {
						return fmt.Errorf("store: reclaim scheduled task run: %w", err)
					}
					run.Attempts++
					run.StartedAt = &now
				}
				if run.State != ScheduledTaskRunRunning {
					// The worker settles the task from the run as it stands,
					// without delivering.
					task.LeaseUntil = &leaseUntil
					task.LeasedBy = leasedBy
					leased = append(leased, task)
					claimed = append(claimed, ClaimedScheduledTask{Task: task, Run: run, LeasedBy: leasedBy, AlreadySettled: true, WorkersLost: lost})
					continue
				}
			} else {
				run = ScheduledTaskRun{
					ID:           newID(),
					TaskID:       task.ID,
					CustomerID:   task.CustomerID,
					ScheduledFor: scheduledFor,
					Trigger:      ScheduledTaskTriggerSchedule,
					Prompt:       task.Instructions,
					State:        ScheduledTaskRunRunning,
					Attempts:     1,
					StartedAt:    &now,
					CreatedAt:    now,
				}
				inserted = append(inserted, run)
			}
			task.LeaseUntil = &leaseUntil
			task.LeasedBy = leasedBy
			leased = append(leased, task)
			claimed = append(claimed, ClaimedScheduledTask{Task: task, Run: run, LeasedBy: leasedBy})
		}
		if len(inserted) > 0 {
			if _, err := tx.NewInsert().Model(&inserted).Exec(ctx); err != nil {
				return fmt.Errorf("store: insert scheduled task run: %w", err)
			}
		}
		return leaseScheduledTasks(ctx, tx, leased, leaseUntil, now)
	})
	if err != nil {
		return nil, stack.Wrap(err)
	}
	return claimed, nil
}

// occurrenceKey names the run of one occurrence of a task: one per task and time it was due.
type occurrenceKey struct {
	customerID, taskID string
	scheduledFor       int64
}

func occurrenceOf(customerID, taskID string, scheduledFor time.Time) occurrenceKey {
	return occurrenceKey{customerID, taskID, scheduledFor.UnixMicro()}
}

// occurrenceRuns reads the scheduled runs of the occurrences the tasks are due at, in one query.
func occurrenceRuns(ctx context.Context, tx bun.Tx, tasks []ScheduledTask) (map[occurrenceKey]ScheduledTaskRun, error) {
	var customers, ids, due []string
	for _, task := range tasks {
		if task.NextRunAt != nil {
			customers = append(customers, task.CustomerID)
			ids = append(ids, task.ID)
			due = append(due, task.NextRunAt.UTC().Format(time.RFC3339Nano))
		}
	}
	found := map[occurrenceKey]ScheduledTaskRun{}
	if len(ids) == 0 {
		return found, nil
	}
	var runs []ScheduledTaskRun
	err := tx.NewSelect().Model(&runs).
		Where("trigger = ?", ScheduledTaskTriggerSchedule).
		Where("(customer_id, task_id, scheduled_for) IN (SELECT * FROM unnest(?::text[], ?::text[], ?::timestamptz[]))",
			pgdialect.Array(customers), pgdialect.Array(ids), pgdialect.Array(due)).
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	for _, run := range runs {
		found[occurrenceOf(run.CustomerID, run.TaskID, run.ScheduledFor)] = run
	}
	return found, nil
}

// leaseScheduledTasks puts the claimed tasks under their claims' leases, in one statement.
func leaseScheduledTasks(ctx context.Context, tx bun.Tx, tasks []ScheduledTask, until, now time.Time) error {
	if len(tasks) == 0 {
		return nil
	}
	customers, ids, leasedBy := make([]string, len(tasks)), make([]string, len(tasks)), make([]string, len(tasks))
	for i, task := range tasks {
		customers[i], ids[i], leasedBy[i] = task.CustomerID, task.ID, task.LeasedBy
	}
	_, err := tx.NewUpdate().Model((*ScheduledTask)(nil)).
		TableExpr("unnest(?::text[], ?::text[], ?::text[]) AS claim (customer_id, id, leased_by)",
			pgdialect.Array(customers), pgdialect.Array(ids), pgdialect.Array(leasedBy)).
		Set("lease_until = ?", until).
		Set("leased_by = claim.leased_by").
		Set("updated_at = ?", now).
		Where("st.customer_id = claim.customer_id").
		Where("st.id = claim.id").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("store: claim scheduled tasks: %w", err)
	}
	return nil
}

// capFailure is the reason a run failed at maxScheduledTaskRunAttempts is
// recorded under. A run with a call placed for it says what the attempt
// after it would have (deliverByPhone): the reason the call ended under, or
// already_called when none was recorded, that call having maybe carried the
// task out (AI-2404). Any other run failed under delivery_failed.
func capFailure(run ScheduledTaskRun) string {
	if run.SessionID == "" {
		return scheduledTaskRunWorkersLost
	}
	if run.Error != nil && *run.Error != "" {
		return *run.Error
	}
	return scheduledTaskRunRungAlready
}

// heldRun reports whether a running run is a held one whose hold has ended:
// nobody is delivering it, however recently it started, so it is taken over
// rather than parked. A run another worker took a moment ago has no hold
// recorded, or one still to run out, or one that ended before that worker
// took it up: its lease lapsing is a worker lost, parked like any other. The
// run has started.
func heldRun(run ScheduledTaskRun, now time.Time) bool {
	return run.PostponedUntil != nil && !run.PostponedUntil.After(now) &&
		run.PostponedUntil.After(*run.StartedAt)
}

// Postpone holds a claimed task until until, under its claim:
// the lease moves to the hold's end and the claim stays, so no other worker
// takes the occurrence meanwhile, and the run stays running with the hold
// recorded on it. False is a claim an update or pause fenced meanwhile: the
// change that fenced it decides what happens instead.
func (r *ScheduledTasks) Postpone(ctx context.Context, customerID, taskID, runID, leasedBy string, until time.Time) (bool, error) {
	held := false
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		result, err := tx.NewUpdate().Model((*ScheduledTask)(nil)).
			Set("lease_until = ?", until).
			Set("updated_at = now()").
			Where("customer_id = ?", customerID).
			Where("id = ?", taskID).
			Where("leased_by = ?", leasedBy).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: postpone scheduled task: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: postpone scheduled task: %w", err)
		}
		held = affected > 0
		if !held {
			return nil
		}
		if _, err := tx.NewUpdate().Model((*ScheduledTaskRun)(nil)).
			Set("postponed_until = ?", until).
			Where("id = ?", runID).
			Exec(ctx); err != nil {
			return fmt.Errorf("store: postpone scheduled task run: %w", err)
		}
		return nil
	})
	if err != nil {
		return false, stack.Wrap(err)
	}
	return held, nil
}

// Renew extends a claim the worker still holds, never to earlier than it is: a renewal still
// on its way when Postpone held the task for quiet hours would cut the hold to one lease.
// False is a claim it no longer holds: an update, pause or delete took it.
func (r *ScheduledTasks) Renew(ctx context.Context, customerID, taskID, leasedBy string, lease time.Duration) (bool, error) {
	result, err := r.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = GREATEST(lease_until, now() + make_interval(secs => ?))", lease.Seconds()).
		Where("customer_id = ?", customerID).
		Where("id = ?", taskID).
		Where("leased_by = ?", leasedBy).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: renew scheduled task lease: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: renew scheduled task lease: %w", err))
	}
	return affected > 0, nil
}

// ScheduledTaskOutcome is what a worker reports of an occurrence.
type ScheduledTaskOutcome struct {
	// RunState is delivered or failed.
	RunState    string
	DeliveredTo string
	SessionID   string
	Error       string
	// NextRunAt and State are the task's after this occurrence; the finish
	// applies them only under the claim that delivered it.
	NextRunAt           *time.Time
	State               string
	ConsecutiveFailures int
}

// Finish records an occurrence's outcome and, under the
// claim, advances the task. applied is false when the claim was fenced by an
// update or pause: the run is still recorded, the schedule is left alone.
func (r *ScheduledTasks) Finish(ctx context.Context, customerID, runID, leasedBy string, outcome ScheduledTaskOutcome) (bool, error) {
	applied := false
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var run ScheduledTaskRun
		if err := tx.NewSelect().Model(&run).Where("id = ?", runID).Limit(1).Scan(ctx); err != nil {
			return fmt.Errorf("store: finish scheduled task run: %w", err)
		}
		if _, err := lockScheduledTask(ctx, tx, customerID, run.TaskID); err != nil {
			return err
		}
		now := time.Now().UTC()
		finished := now
		run.State = outcome.RunState
		run.DeliveredTo = outcome.DeliveredTo
		if outcome.SessionID != "" {
			run.SessionID = outcome.SessionID
		}
		run.FinishedAt = &finished
		if outcome.Error != "" {
			run.Error = &outcome.Error
		} else {
			run.Error = nil
		}
		if _, err := tx.NewUpdate().Model(&run).
			Column("state", "delivered_to", "session_id", "finished_at", "error").
			Where("id = ?", run.ID).
			Exec(ctx); err != nil {
			return fmt.Errorf("store: finish scheduled task run: %w", err)
		}

		result, err := tx.NewUpdate().Model((*ScheduledTask)(nil)).
			Set("next_run_at = ?", outcome.NextRunAt).
			Set("state = ?", outcome.State).
			Set("consecutive_failures = ?", outcome.ConsecutiveFailures).
			Set("lease_until = NULL").
			Set("leased_by = ''").
			Set("updated_at = ?", now).
			Where("customer_id = ?", customerID).
			Where("id = ?", run.TaskID).
			Where("leased_by = ?", leasedBy).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: finish scheduled task run: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: finish scheduled task run: %w", err)
		}
		applied = affected > 0
		return nil
	})
	if err != nil {
		return false, stack.Wrap(err)
	}
	return applied, nil
}

// NextDue is the soonest an active task may be claimed, for a
// worker deciding how long to sleep: its next run, or the end of the lease or
// hold on it when that is later. GREATEST skips a null lease
// (https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-GREATEST-LEAST),
// and is written as scheduled_tasks_wake_idx indexes it.
func (r *ScheduledTasks) NextDue(ctx context.Context) (time.Time, bool, error) {
	var wake time.Time
	err := r.store.global.NewSelect().Model((*ScheduledTask)(nil)).
		ColumnExpr("GREATEST(next_run_at, lease_until)").
		Where("state = ?", ScheduledTaskActive).
		Where("next_run_at IS NOT NULL").
		OrderExpr("GREATEST(next_run_at, lease_until) ASC").
		Limit(1).
		Scan(ctx, &wake)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, stack.Wrap(fmt.Errorf("store: next due scheduled task: %w", err))
	}
	return wake, true, nil
}

const (
	// ScheduledTasksKept is how long a finished run, and a task that completed or failed, is
	// kept: each row copies the user's instructions, and a daily task adds a run a day.
	ScheduledTasksKept = 90 * 24 * time.Hour
	// scheduledTaskRunsPruned and scheduledTasksPruned are how many rows one statement of
	// Prune deletes. A task takes its runs with it, so fewer tasks go at once.
	scheduledTaskRunsPruned = 5_000
	scheduledTasksPruned    = 500
)

// What ScheduledTasks.Prune sends for a batch of each table. The picks read
// scheduled_task_runs_finished_idx and scheduled_tasks_pruned_idx, whose state test must imply
// that index's predicate (TestPruneReadsItsIndexes). The deletes also make each change recorded
// of the batch's rows a delete with no payload, as deleteTasks does, by data_changes_tasks_idx
// (TestPruneRedactsByTheChangesOfItsOwnRows).
const (
	pickScheduledTaskRunsSQL = `
SELECT id FROM scheduled_task_runs
WHERE finished_at < now() - make_interval(secs => ?)
LIMIT ? FOR UPDATE SKIP LOCKED`
	deleteScheduledTaskRunsSQL = `
WITH deleted AS (
    DELETE FROM scheduled_task_runs WHERE id = ANY(?::text[]) RETURNING customer_id, task_id, id
), redacted AS (
    UPDATE data_changes AS dc SET op = 'delete', payload = NULL FROM deleted AS d
    WHERE dc.customer_id = d.customer_id AND dc.table_name = 'scheduled_task_runs' AND dc.op <> 'delete'
    AND ` + changedTask + ` = d.task_id AND dc.payload->>'id' = d.id)
SELECT count(*) FROM deleted`
	pickScheduledTasksSQL = `
SELECT customer_id, id FROM scheduled_tasks
WHERE state IN ('completed', 'failed') AND updated_at < now() - make_interval(secs => ?0)
AND NOT EXISTS (
    SELECT 1 FROM scheduled_task_runs r
    WHERE r.customer_id = scheduled_tasks.customer_id AND r.task_id = scheduled_tasks.id
    AND (r.finished_at IS NULL OR r.finished_at >= now() - make_interval(secs => ?0)))
LIMIT ?1 FOR UPDATE SKIP LOCKED`
	lockScheduledTaskRunsSQL = `
SELECT r.id FROM scheduled_task_runs AS r, unnest(?0::text[], ?1::text[]) AS batch (customer_id, id)
WHERE r.customer_id = batch.customer_id AND r.task_id = batch.id
FOR UPDATE OF r`
	deleteScheduledTasksSQL = `
WITH deleted AS (
    DELETE FROM scheduled_tasks AS t USING unnest(?0::text[], ?1::text[]) AS batch (customer_id, id)
    WHERE t.customer_id = batch.customer_id AND t.id = batch.id
    RETURNING t.customer_id, t.id
), redacted AS (
    UPDATE data_changes AS dc SET op = 'delete', payload = NULL FROM deleted AS d
    WHERE dc.customer_id = d.customer_id AND dc.table_name IN ('scheduled_tasks', 'scheduled_task_runs') AND dc.op <> 'delete'
    AND ` + changedTask + ` = d.id)
SELECT count(*) FROM deleted`
)

// settleLostManualRunsSQL writes missed the runs made by hand still running a while after they
// started, before ScheduledTasks.Prune deletes. It reads scheduled_task_runs_finished_idx by its
// unfinished runs, the few in flight (TestPruneReadsItsIndexes).
const settleLostManualRunsSQL = `
UPDATE scheduled_task_runs SET state = 'missed', finished_at = now()
WHERE finished_at IS NULL AND trigger = 'manual' AND state = 'running'
AND started_at < now() - make_interval(secs => ?)`

// Prune deletes the runs that finished, and the tasks that completed or failed,
// more than ScheduledTasksKept ago, in batches until none is left, and says how many of each.
// A task counts from updated_at, which the finish of its last run sets as the task leaves
// active, but is kept while a run of it is unfinished or finished within ScheduledTasksKept:
// a completed task can be run by hand without its row changing. An active or paused task is
// never deleted, whatever its age; its old runs are. A run made by hand still running
// scheduledTaskManualRunLost after it started is written missed first, so it finishes.
//
// SKIP LOCKED keeps two routers pruning at once off each other's rows. The runs batch locks
// only run rows, and the tasks batch takes the task row before the runs it cascades to, as
// every writer of both does (lockScheduledTask), so neither can deadlock with
// ScheduledTasks.Finish. The tasks batch takes those runs before it rewrites their changes, as
// another router's runs batch does, so the two cannot deadlock either.
func (r *ScheduledTasks) Prune(ctx context.Context) (runs, tasks int64, err error) {
	if _, err = r.store.global.ExecContext(ctx, settleLostManualRunsSQL, scheduledTaskManualRunLost.Seconds()); err != nil {
		return 0, 0, stack.Wrap(fmt.Errorf("store: settle lost scheduled task runs: %w", err))
	}
	runs, err = r.pruneInBatches(ctx, scheduledTaskRunsPruned, func(ctx context.Context, tx bun.Tx) (picked int, removed int64, err error) {
		var ids []string
		if err := tx.NewRaw(pickScheduledTaskRunsSQL, ScheduledTasksKept.Seconds(), scheduledTaskRunsPruned).Scan(ctx, &ids); err != nil || len(ids) == 0 {
			return 0, 0, err
		}
		err = tx.NewRaw(deleteScheduledTaskRunsSQL, pgdialect.Array(ids)).Scan(ctx, &removed)
		return len(ids), removed, err
	})
	if err != nil {
		return runs, 0, stack.Wrap(fmt.Errorf("store: prune scheduled task runs: %w", err))
	}
	tasks, err = r.pruneInBatches(ctx, scheduledTasksPruned, func(ctx context.Context, tx bun.Tx) (picked int, removed int64, err error) {
		var customers, ids []string
		if err := tx.NewRaw(pickScheduledTasksSQL, ScheduledTasksKept.Seconds(), scheduledTasksPruned).Scan(ctx, &customers, &ids); err != nil || len(ids) == 0 {
			return 0, 0, err
		}
		if _, err := tx.ExecContext(ctx, lockScheduledTaskRunsSQL, pgdialect.Array(customers), pgdialect.Array(ids)); err != nil {
			return 0, 0, err
		}
		err = tx.NewRaw(deleteScheduledTasksSQL, pgdialect.Array(customers), pgdialect.Array(ids)).Scan(ctx, &removed)
		return len(ids), removed, err
	})
	if err != nil {
		return runs, tasks, stack.Wrap(fmt.Errorf("store: prune scheduled tasks: %w", err))
	}
	return runs, tasks, nil
}

// pruneInBatches runs prune, which picks and locks a batch and then deletes it, each time in a
// transaction of its own, until it picks a short batch. The delete is a statement after the locks,
// so it sees the changes recorded by a write that committed while they waited.
func (r *ScheduledTasks) pruneInBatches(ctx context.Context, batch int, prune func(context.Context, bun.Tx) (picked int, removed int64, err error)) (int64, error) {
	var pruned int64
	for {
		var picked int
		var removed int64
		err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) (err error) {
			picked, removed, err = prune(ctx, tx)
			return err
		})
		if err != nil {
			return pruned, err
		}
		pruned += removed
		if picked < batch {
			return pruned, nil
		}
	}
}

// PauseForConfig pauses a config's active tasks, as deleting that
// config must: an occurrence of one would only fail. It clears their claims, so
// a worker finishing an occurrence in flight cannot make one active again, and
// writes their occurrences still running missed, as a pause by hand does
// (ScheduledTasks.Update): a held one is otherwise taken up by no worker, since
// only an active task is claimed, and stays running for good.
func (r *ScheduledTasks) PauseForConfig(ctx context.Context, customerID, configID string) error {
	if configID == "" {
		return nil
	}
	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var paused []string
		err := tx.NewUpdate().Model((*ScheduledTask)(nil)).
			Set("state = ?", ScheduledTaskPaused).
			Set("leased_by = ''").
			Set("lease_until = NULL").
			Set("updated_at = now()").
			Where("customer_id = ?", customerID).
			Where("config_id = ?", configID).
			Where("state = ?", ScheduledTaskActive).
			Returning("id").
			Scan(ctx, &paused)
		if err != nil {
			return fmt.Errorf("store: pause scheduled tasks for config: %w", err)
		}
		// A run made by hand is left to the delivery that made it, which
		// records its outcome, or to Prune once its router stopped.
		if _, err := tx.NewUpdate().Model((*ScheduledTaskRun)(nil)).
			Set("state = ?", ScheduledTaskRunMissed).
			Set("finished_at = now()").
			Where("customer_id = ?", customerID).
			Where("task_id IN (?)", bun.In(paused)).
			Where("trigger = ?", ScheduledTaskTriggerSchedule).
			Where("state = ?", ScheduledTaskRunRunning).
			Exec(ctx); err != nil {
			return fmt.Errorf("store: settle scheduled task runs for config: %w", err)
		}
		return nil
	}))
}
