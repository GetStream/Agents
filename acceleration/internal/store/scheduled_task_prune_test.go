//go:build integration

package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"
)

const day = 24 * time.Hour

// occurrence gives each run a time of its own, so two runs of a task never share an occurrence.
var occurrence atomic.Int64

// aged stores a task in a state and makes its last change age ago.
func (s *StoreSuite) aged(id, customer, state string, age time.Duration) {
	s.task(id, func(task *ScheduledTask) { task.CustomerID, task.State = customer, state })
	_, err := s.store.global.ExecContext(s.ctx,
		"UPDATE scheduled_tasks SET updated_at = now() - make_interval(secs => ?) WHERE customer_id = ? AND id = ?",
		age.Seconds(), customer, id)
	s.Require().NoError(err)
}

// ranFor stores a run of a task in a state, that finished age ago, or has not finished for
// a negative age.
func (s *StoreSuite) ranFor(id, customer, task, trigger, state string, finished time.Duration) {
	var at any
	if finished >= 0 {
		at = time.Now().Add(-finished)
	}
	_, err := s.store.global.ExecContext(s.ctx, `
INSERT INTO scheduled_task_runs (id, task_id, customer_id, scheduled_for, trigger, state, finished_at, created_at)
VALUES (?, ?, ?, now() - interval '400 days' + make_interval(secs => ?), ?, ?, ?, now() - interval '400 days')`,
		id, task, customer, float64(occurrence.Add(1)), trigger, state, at)
	s.Require().NoError(err)
}

// startedFor stores a run of a task, still running, that started age ago.
func (s *StoreSuite) startedFor(id, task, trigger string, age time.Duration) {
	_, err := s.store.global.ExecContext(s.ctx, `
INSERT INTO scheduled_task_runs (id, task_id, customer_id, scheduled_for, trigger, state, started_at)
VALUES (?, ?, 'acme', now() - make_interval(secs => ?), ?, 'running', now() - make_interval(secs => ?))`,
		id, task, age.Seconds()+float64(occurrence.Add(1)), trigger, age.Seconds())
	s.Require().NoError(err)
}

func (s *StoreSuite) ids(query string) []string {
	var ids []string
	s.Require().NoError(s.store.global.NewRaw(query).Scan(s.ctx, &ids))
	return ids
}

func (s *StoreSuite) taskIDs() []string {
	return s.ids("SELECT customer_id || '/' || id FROM scheduled_tasks ORDER BY 1")
}

func (s *StoreSuite) runIDs() []string { return s.ids("SELECT id FROM scheduled_task_runs ORDER BY 1") }

func (s *StoreSuite) TestPruneDeletesRunsThatFinishedMoreThanNinetyDaysAgo() {
	s.task("t", nil)
	s.ranFor("old-delivered", "acme", "t", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, 91*day)
	s.ranFor("old-failed", "acme", "t", ScheduledTaskTriggerSchedule, ScheduledTaskRunFailed, 91*day)
	s.ranFor("old-missed", "acme", "t", ScheduledTaskTriggerSchedule, ScheduledTaskRunMissed, 91*day)
	s.ranFor("old-manual", "acme", "t", ScheduledTaskTriggerManual, ScheduledTaskRunDelivered, 91*day)
	s.ranFor("recent", "acme", "t", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, 89*day)
	s.ranFor("running", "acme", "t", ScheduledTaskTriggerSchedule, ScheduledTaskRunRunning, -1)

	runs, tasks, err := s.store.ScheduledTasks.Prune(s.ctx)

	s.Require().NoError(err)
	s.EqualValues(4, runs)
	s.Zero(tasks)
	s.Equal([]string{"recent", "running"}, s.runIDs(),
		"a run counts from when it finished, and one that has not finished is never pruned")
}

func (s *StoreSuite) TestPruneKeepsTheTasksThatAreActiveOrPausedAndTakesTheirOldRuns() {
	s.aged("active", "acme", ScheduledTaskActive, 400*day)
	s.aged("paused", "acme", ScheduledTaskPaused, 400*day)
	s.ranFor("active-old", "acme", "active", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, 200*day)
	s.ranFor("active-new", "acme", "active", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, day)
	s.ranFor("paused-old", "acme", "paused", ScheduledTaskTriggerSchedule, ScheduledTaskRunMissed, 200*day)

	runs, tasks, err := s.store.ScheduledTasks.Prune(s.ctx)

	s.Require().NoError(err)
	s.EqualValues(2, runs)
	s.Zero(tasks)
	s.Equal([]string{"acme/active", "acme/paused"}, s.taskIDs())
	s.Equal([]string{"active-new"}, s.runIDs())
}

func (s *StoreSuite) TestPruneDeletesCompletedAndFailedTasksPastRetentionWithTheirRuns() {
	s.aged("completed-old", "acme", ScheduledTaskCompleted, 91*day)
	s.aged("failed-old", "globex", ScheduledTaskFailed, 91*day)
	s.aged("completed-new", "acme", ScheduledTaskCompleted, 89*day)
	s.aged("failed-new", "acme", ScheduledTaskFailed, 89*day)
	s.ranFor("of-completed-old", "acme", "completed-old", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, 91*day)
	s.ranFor("of-completed-new", "acme", "completed-new", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, 10*day)

	runs, tasks, err := s.store.ScheduledTasks.Prune(s.ctx)

	s.Require().NoError(err)
	s.EqualValues(1, runs)
	s.EqualValues(2, tasks)
	s.Equal([]string{"acme/completed-new", "acme/failed-new"}, s.taskIDs())
	s.Equal([]string{"of-completed-new"}, s.runIDs())
}

// A completed task can be run by hand without its row changing, so it is kept while a run of
// it is unfinished or finished within the retention, and goes once its last run is pruned.
func (s *StoreSuite) TestPruneKeepsAnOldCompletedTaskWhileARunOfItIsUnfinishedOrRecent() {
	s.aged("in-flight", "acme", ScheduledTaskFailed, 100*day)
	s.aged("yesterday", "acme", ScheduledTaskCompleted, 100*day)
	s.aged("long-ago", "acme", ScheduledTaskCompleted, 100*day)
	s.aged("other-customer", "globex", ScheduledTaskCompleted, 100*day)
	s.aged("other-customer", "acme", ScheduledTaskCompleted, 100*day)
	s.ranFor("running", "acme", "in-flight", ScheduledTaskTriggerManual, ScheduledTaskRunRunning, -1)
	s.ranFor("recent", "acme", "yesterday", ScheduledTaskTriggerManual, ScheduledTaskRunDelivered, day)
	s.ranFor("old", "acme", "long-ago", ScheduledTaskTriggerManual, ScheduledTaskRunDelivered, 100*day)
	// A recent run of the same task id under another customer does not keep this one.
	s.ranFor("not-theirs", "acme", "other-customer", ScheduledTaskTriggerManual, ScheduledTaskRunDelivered, 100*day)
	s.ranFor("theirs", "globex", "other-customer", ScheduledTaskTriggerManual, ScheduledTaskRunDelivered, day)

	runs, tasks, err := s.store.ScheduledTasks.Prune(s.ctx)

	s.Require().NoError(err)
	s.EqualValues(2, runs)
	s.EqualValues(2, tasks)
	s.Equal([]string{"acme/in-flight", "acme/yesterday", "globex/other-customer"}, s.taskIDs())
	s.Equal([]string{"recent", "running", "theirs"}, s.runIDs())
}

// Only the router that took a run by hand finishes it, so one that stopped mid-delivery left it
// running, and its task, kept while a run of it is unfinished, could never be pruned. A run by
// hand still being delivered, and an occurrence a worker holds, stay running.
func (s *StoreSuite) TestPruneWritesMissedARunByHandItsRouterLeftRunning() {
	s.aged("t", "acme", ScheduledTaskCompleted, 100*day)
	s.startedFor("lost", "t", ScheduledTaskTriggerManual, scheduledTaskManualRunLost+time.Minute)
	s.startedFor("delivering", "t", ScheduledTaskTriggerManual, time.Hour)
	s.startedFor("held", "t", ScheduledTaskTriggerSchedule, 2*day)

	_, _, err := s.store.ScheduledTasks.Prune(s.ctx)

	s.Require().NoError(err)
	s.Equal([]string{"delivering=running", "held=running", "lost=missed"},
		s.ids("SELECT id || '=' || state FROM scheduled_task_runs ORDER BY 1"))
	s.Equal([]string{"lost"}, s.ids("SELECT id FROM scheduled_task_runs WHERE finished_at IS NOT NULL"),
		"a run written missed has finished, so its task is pruned once the run is")
}

// backlog stores a batch and one more of old completed tasks, done-1 on, and of old runs of
// the active task live, run-1 on, so the prune of each takes two statements.
func (s *StoreSuite) backlog() {
	_, err := s.store.global.ExecContext(s.ctx, fmt.Sprintf(`
INSERT INTO scheduled_tasks (id, customer_id, user_id, title, instructions, start_local, timezone, state, updated_at)
SELECT 'done-' || n, 'acme', 'sam', 't', 'i', now()::timestamp, 'UTC', 'completed', now() - interval '91 days'
FROM generate_series(1, %d) AS n`, scheduledTasksPruned+1))
	s.Require().NoError(err)
	s.task("live", nil)
	_, err = s.store.global.ExecContext(s.ctx, fmt.Sprintf(`
INSERT INTO scheduled_task_runs (id, task_id, customer_id, scheduled_for, trigger, state, finished_at)
SELECT 'run-' || n, 'live', 'acme', now() - make_interval(mins => n), 'manual', 'delivered', now() - interval '91 days'
FROM generate_series(1, %d) AS n`, scheduledTaskRunsPruned+1))
	s.Require().NoError(err)
}

func (s *StoreSuite) TestPruneGoesOnUntilNoBatchIsLeft() {
	s.backlog()

	runs, tasks, err := s.store.ScheduledTasks.Prune(s.ctx)

	s.Require().NoError(err)
	s.EqualValues(scheduledTaskRunsPruned+1, runs)
	s.EqualValues(scheduledTasksPruned+1, tasks)
	s.Equal([]string{"acme/live"}, s.taskIDs())
	s.Empty(s.runIDs())
}

// One statement of the prune deletes a batch at most, so a backlog is never locked whole in one
// transaction. A snapshot taken before the prune still sees each row it deletes, with xmax the
// transaction that deleted it.
func (s *StoreSuite) TestPruneDeletesNoMoreThanABatchInOneStatement() {
	s.backlog()
	before, err := s.store.global.BeginTx(s.ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	s.Require().NoError(err)
	defer before.Rollback() //nolint:errcheck // read only
	// A repeatable read transaction takes its snapshot at its first statement.
	_, err = before.ExecContext(s.ctx, "SELECT 1")
	s.Require().NoError(err)

	_, _, err = s.store.ScheduledTasks.Prune(s.ctx)
	s.Require().NoError(err)

	perStatement := func(query string) []int {
		var deleted []int
		s.Require().NoError(before.NewRaw(query).Scan(s.ctx, &deleted))
		return deleted
	}
	s.Equal([]int{scheduledTaskRunsPruned, 1}, perStatement(
		"SELECT count(*) FROM scheduled_task_runs WHERE id LIKE 'run-%' GROUP BY xmax::text ORDER BY 1 DESC"))
	s.Equal([]int{scheduledTasksPruned, 1}, perStatement(
		"SELECT count(*) FROM scheduled_tasks WHERE id LIKE 'done-%' GROUP BY xmax::text ORDER BY 1 DESC"))
}

// Old finished runs of active tasks are deleted while their workers finish new ones, and old
// finished tasks are deleted meanwhile: a deadlock would fail a finish or the prune.
func (s *StoreSuite) TestPruneRunsBesideFinishesWithoutDeadlock() {
	const live = 12
	due := s.base.Add(-time.Minute)
	for i := range live {
		s.task(fmt.Sprintf("live-%d", i), func(task *ScheduledTask) { task.NextRunAt = &due })
	}
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, live, time.Hour)
	s.Require().NoError(err)
	s.Require().Len(claimed, live)
	for _, c := range claimed {
		for j := range 30 {
			s.ranFor(fmt.Sprintf("%s-old-%d", c.Task.ID, j), "acme", c.Task.ID,
				ScheduledTaskTriggerManual, ScheduledTaskRunDelivered, 91*day)
		}
	}
	for i := range 40 {
		id := fmt.Sprintf("done-%d", i)
		s.aged(id, "acme", ScheduledTaskCompleted, 91*day)
		s.ranFor(id+"-run", "acme", id, ScheduledTaskTriggerManual, ScheduledTaskRunDelivered, 91*day)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2*live+8)
	for _, c := range claimed {
		wg.Go(func() {
			applied, err := s.store.ScheduledTasks.Finish(s.ctx, "acme", c.Run.ID, c.LeasedBy, ScheduledTaskOutcome{
				RunState: ScheduledTaskRunDelivered, DeliveredTo: ScheduledTaskDeliveredConversation,
				State: ScheduledTaskActive, NextRunAt: &due,
			})
			if err == nil && !applied {
				err = fmt.Errorf("finish of %s was not applied", c.Task.ID)
			}
			errs <- err
		})
	}
	var runs, tasks atomic.Int64
	for range 4 {
		wg.Go(func() {
			r, t, err := s.store.ScheduledTasks.Prune(s.ctx)
			runs.Add(r)
			tasks.Add(t)
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		s.Require().NoError(err)
	}

	s.EqualValues(live*30+40, runs.Load(), "every old run goes once, whichever router's prune takes it")
	s.EqualValues(40, tasks.Load())
	s.Len(s.runIDs(), live, "the finishes of this moment stay")
}

// A prune that finds a row another router's prune holds leaves it to that router and goes on,
// where waiting for it would stall both.
func (s *StoreSuite) TestPruneLeavesTheRowsAnotherPruneHoldsAndDoesNotWait() {
	s.aged("held-task", "acme", ScheduledTaskCompleted, 91*day)
	s.aged("free-task", "acme", ScheduledTaskCompleted, 91*day)
	s.ranFor("held-run", "acme", "held-task", ScheduledTaskTriggerManual, ScheduledTaskRunDelivered, 91*day)
	s.ranFor("free-run", "acme", "held-task", ScheduledTaskTriggerManual, ScheduledTaskRunDelivered, 91*day)

	other, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer other.Rollback() //nolint:errcheck // the test commits or has failed
	_, err = other.ExecContext(s.ctx, "SELECT 1 FROM scheduled_task_runs WHERE id = 'held-run' FOR UPDATE")
	s.Require().NoError(err)
	_, err = other.ExecContext(s.ctx, "SELECT 1 FROM scheduled_tasks WHERE id = 'held-task' FOR UPDATE")
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	runs, tasks, err := s.store.ScheduledTasks.Prune(ctx)

	s.Require().NoError(err, "the prune waited for rows it could have left")
	s.EqualValues(1, runs)
	s.EqualValues(1, tasks)
	s.Equal([]string{"held-run"}, s.runIDs())
	s.Equal([]string{"acme/held-task"}, s.taskIDs())

	s.Require().NoError(other.Commit())
	runs, tasks, err = s.store.ScheduledTasks.Prune(s.ctx)
	s.Require().NoError(err)
	s.EqualValues(1, runs)
	s.EqualValues(1, tasks)
	s.Empty(s.taskIDs())
}

// The prune reads each table through an index of its own, with the cut as the index's
// condition, not a filter on a scan of the whole index. Sequential scans are off so the plan
// shows which index the planner can use: the tables of a test are too small for it to prefer one.
func (s *StoreSuite) TestPruneReadsItsIndexes() {
	for _, c := range []struct {
		name, query string
		batch       int
		index, cond string
	}{
		{"runs", pickScheduledTaskRunsSQL, scheduledTaskRunsPruned, "scheduled_task_runs_finished_idx", "(finished_at <"},
		{"tasks", pickScheduledTasksSQL, scheduledTasksPruned, "scheduled_tasks_pruned_idx", "(updated_at <"},
		{"lost runs by hand", settleLostManualRunsSQL, 0, "scheduled_task_runs_finished_idx", "(finished_at IS NULL)"},
	} {
		var plan []string
		err := s.store.global.RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
				return err
			}
			return tx.NewRaw("EXPLAIN "+c.query, ScheduledTasksKept.Seconds(), c.batch).Scan(ctx, &plan)
		})
		s.Require().NoError(err, c.name)
		read := strings.Join(plan, "\n")
		s.Contains(read, c.index, "%s: %s", c.name, read)
		s.Contains(read, "Index Cond: "+c.cond, "the index is walked by the cut, not filtered after it: %s: %s", c.name, read)
	}
}

// explainedChanges keeps the statements sent that rewrite recorded changes.
type explainedChanges struct {
	active atomic.Bool
	sent   []string
}

func (*explainedChanges) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}
func (e *explainedChanges) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if e.active.Load() && strings.Contains(event.Query, "data_changes") {
		e.sent = append(e.sent, event.Query)
	}
}

// Of a customer with many recorded changes, each delete reads those of its own tasks and runs
// by their index, and its rows by their keys.
func (s *StoreSuite) TestPruneRedactsByTheChangesOfItsOwnRows() {
	_, err := s.store.global.ExecContext(s.ctx, `
INSERT INTO scheduled_tasks (id, customer_id, user_id, title, instructions, start_local, timezone, state, updated_at)
SELECT 'task-' || n, 'acme', 'sam', 't', 'i', now()::timestamp, 'UTC', CASE WHEN n <= 100 THEN 'completed' ELSE 'active' END, now() - interval '91 days'
FROM generate_series(1, 20000) AS n;
INSERT INTO scheduled_task_runs (id, task_id, customer_id, scheduled_for, trigger, state, finished_at)
SELECT 'run-' || n, 'task-' || n, 'acme', now(), 'manual', 'delivered', now() - CASE WHEN n <= 200 THEN interval '91 days' ELSE interval '1 day' END
FROM generate_series(1, 20000) AS n;
INSERT INTO data_changes (customer_id, table_name, op, key, payload)
SELECT 'acme', CASE n % 3 WHEN 0 THEN 'scheduled_tasks' WHEN 1 THEN 'scheduled_task_runs' ELSE 'campaigns' END, 'insert', '{}',
	CASE n % 3 WHEN 1 THEN jsonb_build_object('id', 'run-' || n / 3, 'task_id', 'task-' || n / 3, 'prompt', repeat('word ', 100))
		ELSE jsonb_build_object('id', 'task-' || n / 3, 'instructions', repeat('word ', 100)) END
FROM generate_series(3, 60000) AS n;
ANALYZE scheduled_tasks, scheduled_task_runs, data_changes`)
	s.Require().NoError(err)
	watch := &explainedChanges{}
	s.store.global.AddQueryHook(watch)
	watch.active.Store(true)

	s.Require().NoError(s.store.ScheduledTasks.Delete(s.ctx, "acme", "task-19000"))
	runs, tasks, err := s.store.ScheduledTasks.Prune(s.ctx)

	watch.active.Store(false)
	s.Require().NoError(err)
	s.EqualValues(200, runs)
	s.EqualValues(100, tasks)
	s.Require().Len(watch.sent, 3, "the delete of one task, a batch of runs and a batch of tasks")
	var plans []string
	for _, query := range watch.sent {
		var plan []string
		s.Require().NoError(s.store.global.NewRaw("EXPLAIN "+query).Scan(s.ctx, &plan))
		read := strings.Join(plan, "\n")
		plans = append(plans, read)
		s.Contains(read, "Index Scan using data_changes_tasks_idx on data_changes", read)
		s.Regexp(`Index Cond: \(\(customer_id = .*COALESCE\(\(payload ->> 'task_id'::text\), \(payload ->> 'id'::text\)\) = `, read, read)
		s.NotRegexp(`Seq Scan on (scheduled_tasks|data_changes)`, read, read)
	}
	s.Regexp(`Index Cond: .*\(\(payload ->> 'id'::text\) = d\.id\)`, plans[1], "a run's changes are found by the run, not among all of its task's")
	// The runs fit in too few pages for a plan not to read them all, unless it may not.
	var plan []string
	s.Require().NoError(s.store.global.RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			return err
		}
		return tx.NewRaw("EXPLAIN "+watch.sent[1]).Scan(ctx, &plan)
	}))
	s.Contains(strings.Join(plan, "\n"), "Index Cond: (id = ANY (", "the delete reads the batch's runs by key")
}

func (s *StoreSuite) TestPruneRedactsTheChangesOfTheTasksAndRunsItDeletes() {
	for _, customer := range []string{"acme", "globex"} {
		s.Require().NoError(s.store.StartDataCapture(s.ctx, customer, time.Hour, time.Second))
	}
	s.aged("done", "acme", ScheduledTaskCompleted, 91*day)
	s.ranFor("done-run", "acme", "done", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, 91*day)
	s.aged("live", "acme", ScheduledTaskActive, 91*day)
	s.ranFor("live-old", "acme", "live", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, 91*day)
	s.ranFor("live-new", "acme", "live", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, day)
	// Another app's rows under the same ids, kept.
	s.aged("done", "globex", ScheduledTaskCompleted, 89*day)
	s.ranFor("globex-run", "globex", "done", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, 89*day)
	_, err := s.store.global.ExecContext(s.ctx, "UPDATE scheduled_task_runs SET prompt = 'words' WHERE id = 'live-old'")
	s.Require().NoError(err)

	runs, tasks, err := s.store.ScheduledTasks.Prune(s.ctx)

	s.Require().NoError(err)
	s.EqualValues(2, runs)
	s.EqualValues(1, tasks)
	s.ElementsMatch([]string{
		"scheduled_tasks delete done", "scheduled_tasks delete done", "scheduled_task_runs delete ",
		"scheduled_tasks delete done", "scheduled_task_runs delete ",
		"scheduled_tasks insert live", "scheduled_tasks update live", "scheduled_task_runs insert live",
		"scheduled_task_runs delete ", "scheduled_task_runs delete ", "scheduled_task_runs delete ",
	}, s.taskChanges("acme"), "each change of a pruned row is a delete under its own number, and those of the rows kept stay")
	changes, _, err := s.store.Changes(s.ctx, "acme", 0, 1000)
	s.Require().NoError(err)
	for _, change := range changes {
		if change.Op == ChangeInsert && change.Table == "scheduled_task_runs" {
			s.JSONEq(`{"id": "live-new"}`, string(change.Key), "the run kept is the one whose change is")
		}
	}
	s.ElementsMatch([]string{"scheduled_tasks insert done", "scheduled_tasks update done", "scheduled_task_runs insert done"},
		s.taskChanges("globex"), "another app's task by the same id keeps its changes")
}

// A run pruned before the task, by a release whose prune redacted nothing, is redacted with it.
func (s *StoreSuite) TestPruneRedactsTheChangesOfRunsDeletedBeforeTheirTask() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	s.aged("done", "acme", ScheduledTaskCompleted, 91*day)
	s.ranFor("gone", "acme", "done", ScheduledTaskTriggerSchedule, ScheduledTaskRunDelivered, 91*day)
	_, err := s.store.global.ExecContext(s.ctx, "DELETE FROM scheduled_task_runs WHERE id = 'gone'")
	s.Require().NoError(err)

	_, _, err = s.store.ScheduledTasks.Prune(s.ctx)

	s.Require().NoError(err)
	s.NotContains(s.taskChanges("acme"), "scheduled_task_runs insert done")
}

// As another router's batch of runs does, the holder has the run and then rewrites its changes:
// a prune holding those changes while it waits for the run would deadlock with it.
func (s *StoreSuite) TestARunWriteTheTaskPruneWaitsOnIsRedactedTooWithoutDeadlock() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	s.aged("done", "acme", ScheduledTaskCompleted, 91*day)
	s.ranFor("run", "acme", "done", ScheduledTaskTriggerManual, ScheduledTaskRunDelivered, 91*day)
	writing, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer writing.Rollback() //nolint:errcheck // committed below; this only ends a failed test's transaction
	_, err = writing.ExecContext(s.ctx, "UPDATE scheduled_task_runs SET prompt = 'words' WHERE id = 'run'")
	s.Require().NoError(err)

	pruned := make(chan error, 1)
	go func() {
		_, _, err := s.store.ScheduledTasks.Prune(s.ctx)
		pruned <- err
	}()
	s.Require().Eventually(s.forgetWaiting, 10*time.Second, 10*time.Millisecond, "the prune waits on the run's row")
	_, err = writing.ExecContext(s.ctx, "UPDATE data_changes SET at = at WHERE table_name = 'scheduled_task_runs'")
	s.Require().NoError(err)
	s.Require().NoError(writing.Commit())
	s.Require().NoError(<-pruned)

	s.ElementsMatch([]string{
		"scheduled_tasks delete done", "scheduled_tasks delete done", "scheduled_tasks delete done",
		"scheduled_task_runs delete ", "scheduled_task_runs delete ", "scheduled_task_runs delete ",
	}, s.taskChanges("acme"), "the run write's change is emptied too")
}

func (s *StoreSuite) TestAFollowerOfAnotherAppReadsOnFromItsCursorAfterAPrune() {
	for _, customer := range []string{"acme", "globex"} {
		s.Require().NoError(s.store.StartDataCapture(s.ctx, customer, time.Hour, time.Second))
	}
	s.task("first", func(task *ScheduledTask) { task.CustomerID = "globex" })
	_, cursor, err := s.store.Changes(s.ctx, "globex", 0, 1000)
	s.Require().NoError(err)
	s.aged("done", "acme", ScheduledTaskCompleted, 91*day)
	s.task("second", func(task *ScheduledTask) { task.CustomerID = "globex" })
	_, err = s.store.global.NewDelete().TableExpr("data_changes").Where("seq <= ?", cursor).Exec(s.ctx)
	s.Require().NoError(err)

	_, _, err = s.store.ScheduledTasks.Prune(s.ctx)
	s.Require().NoError(err)

	changes, _, err := s.store.Changes(s.ctx, "globex", cursor, 1000)
	s.Require().NoError(err, "the follower's cursor has not expired")
	s.Require().Len(changes, 1)
	s.JSONEq(`{"customer_id": "globex", "id": "second"}`, string(changes[0].Key))
}
