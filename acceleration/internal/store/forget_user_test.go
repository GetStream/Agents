//go:build integration

package store

import (
	"encoding/json"
	"maps"
	"slices"
	"time"

	"github.com/uptrace/bun"
)

// forgetSam forgets the tasks of acme's user sam.
func (s *StoreSuite) forgetSam() UserTasks {
	counts, err := s.store.ForgetUserTasks(s.ctx, "acme", "sam")
	s.Require().NoError(err)
	return counts
}

// taskChanges are the changes recorded for a customer of scheduled tasks and their runs, each as
// its table, op and the task it is of: the task's id, or the run's task_id. A delete holds no
// payload, so its task is read from the key, and a run's delete names no task.
func (s *StoreSuite) taskChanges(customerID string) []string {
	changes, _, err := s.store.Changes(s.ctx, customerID, 0, 1000)
	s.Require().NoError(err)
	var found []string
	for _, change := range changes {
		var row struct {
			ID     string `json:"id"`
			TaskID string `json:"task_id"`
		}
		if change.Op == ChangeDelete {
			s.Empty(change.Payload, "a delete holds no row")
		}
		switch change.Table {
		case "scheduled_tasks":
			body := change.Payload
			if change.Op == ChangeDelete {
				body = change.Key
			}
			s.Require().NoError(json.Unmarshal(body, &row))
			found = append(found, change.Table+" "+change.Op+" "+row.ID)
		case "scheduled_task_runs":
			if change.Op != ChangeDelete {
				s.Require().NoError(json.Unmarshal(change.Payload, &row))
			}
			found = append(found, change.Table+" "+change.Op+" "+row.TaskID)
		}
	}
	return found
}

func (s *StoreSuite) TestForgettingAUserDeletesTheirTasksAndRuns() {
	due := s.base.Add(-time.Minute)
	s.task("reminder", func(task *ScheduledTask) { task.NextRunAt = &due })
	_, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.task("later", func(task *ScheduledTask) { task.NextRunAt = &due })
	s.task("graces", func(task *ScheduledTask) { task.UserID = "grace"; task.NextRunAt = nil })
	s.task("reminder", func(task *ScheduledTask) { task.CustomerID = "globex"; task.NextRunAt = nil })

	counts := s.forgetSam()

	s.Equal(int64(2), counts.Tasks)
	for _, id := range []string{"reminder", "later"} {
		_, err := s.store.ScheduledTasks.Get(s.ctx, "acme", id)
		s.ErrorIs(err, ErrNoScheduledTask, id)
	}
	runs, err := s.store.ScheduledTaskRuns.Select(s.ctx, "acme", ScheduledTaskRunFilter{TaskID: "reminder", Limit: 25})
	s.Require().NoError(err)
	s.Empty(runs)
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Empty(claimed, "no task of theirs is due any more")
	_, err = s.store.ScheduledTasks.Get(s.ctx, "acme", "graces")
	s.Require().NoError(err, "another user's task stays")
	_, err = s.store.ScheduledTasks.Get(s.ctx, "globex", "reminder")
	s.Require().NoError(err, "a user of another app by the same id keeps theirs")
	left, err := s.store.CountUserTasks(s.ctx, "acme", "sam")
	s.Require().NoError(err)
	s.Equal(UserTasks{}, left)
}

func (s *StoreSuite) TestADryRunCountsTheUsersTasks() {
	s.task("reminder", nil)
	s.task("later", nil)
	s.task("graces", func(task *ScheduledTask) { task.UserID = "grace" })
	s.task("reminder", func(task *ScheduledTask) { task.CustomerID = "globex" })

	counts, err := s.store.CountUserTasks(s.ctx, "acme", "sam")

	s.Require().NoError(err)
	s.Equal(int64(2), counts.Tasks)
}

func (s *StoreSuite) TestForgettingAUserRedactsTheChangesRecordedOfTheirTasks() {
	for _, customer := range []string{"acme", "globex"} {
		s.Require().NoError(s.store.StartDataCapture(s.ctx, customer, time.Hour, time.Second))
	}
	due := s.base.Add(-time.Minute)
	s.task("reminder", func(task *ScheduledTask) { task.NextRunAt = &due })
	s.task("graces", func(task *ScheduledTask) { task.UserID = "grace"; task.NextRunAt = &due })
	// Another app's task of a user by the same id, under the id of another user's task here.
	s.task("graces", func(task *ScheduledTask) { task.CustomerID = "globex"; task.NextRunAt = nil })
	_, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Contains(s.taskChanges("acme"), "scheduled_task_runs insert reminder", "the run's insert was recorded")
	counted, err := s.store.CountUserTasks(s.ctx, "acme", "sam")
	s.Require().NoError(err)

	counts := s.forgetSam()

	s.Equal(UserTasks{Tasks: 1, Changes: 3}, counts, "their insert, the claim's update and the run's insert")
	s.Equal(counts, counted)
	s.ElementsMatch([]string{
		"scheduled_tasks insert graces", "scheduled_tasks update graces", "scheduled_task_runs insert graces",
		"scheduled_tasks delete reminder", "scheduled_tasks delete reminder", "scheduled_task_runs delete ",
		"scheduled_tasks delete reminder", "scheduled_task_runs delete ",
	}, s.taskChanges("acme"), "each change of theirs is a delete under its own number, and the deletes are recorded after them")
	s.Equal([]string{"scheduled_tasks insert graces"}, s.taskChanges("globex"))
}

func (s *StoreSuite) TestForgettingAUserRedactsTheChangesOfTheRunsOfATaskTheyDeletedBefore() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	due := s.base.Add(-time.Minute)
	s.task("reminder", func(task *ScheduledTask) { task.NextRunAt = &due })
	_, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().NoError(s.store.ScheduledTasks.Delete(s.ctx, "acme", "reminder"))

	counts := s.forgetSam()

	s.ElementsMatch([]string{
		"scheduled_tasks delete reminder", "scheduled_tasks delete reminder", "scheduled_task_runs delete ",
		"scheduled_tasks delete reminder", "scheduled_task_runs delete ",
	}, s.taskChanges("acme"))
	s.Equal(UserTasks{Changes: 3}, counts, "the deletes already recorded are not counted")
}

func (s *StoreSuite) TestForgettingAUserRedactsTheChangesOfTheirRunsWhenThoseOfTheTaskWerePruned() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	due := s.base.Add(-time.Minute)
	s.task("reminder", func(task *ScheduledTask) { task.NextRunAt = &due })
	_, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	_, err = s.store.global.NewDelete().TableExpr("data_changes").Where("table_name = 'scheduled_tasks'").Exec(s.ctx)
	s.Require().NoError(err)
	s.Require().Equal([]string{"scheduled_task_runs insert reminder"}, s.taskChanges("acme"))

	s.forgetSam()

	s.ElementsMatch([]string{"scheduled_task_runs delete ", "scheduled_task_runs delete ", "scheduled_tasks delete reminder"}, s.taskChanges("acme"))
}

func (s *StoreSuite) TestForgettingAUserKeepsTheRunsOfAnotherUsersTaskUnderAnIDTheyUsedBefore() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	due := s.base.Add(-time.Minute)
	s.task("reminder", func(task *ScheduledTask) { task.NextRunAt = &due })
	_, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().NoError(s.store.ScheduledTasks.Delete(s.ctx, "acme", "reminder"))
	s.task("reminder", func(task *ScheduledTask) { task.UserID = "grace"; task.NextRunAt = &due })
	_, err = s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)

	s.forgetSam()

	s.ElementsMatch([]string{
		"scheduled_tasks delete reminder", "scheduled_tasks delete reminder", "scheduled_task_runs delete ",
		"scheduled_tasks delete reminder", "scheduled_task_runs delete ",
		"scheduled_tasks insert reminder", "scheduled_tasks update reminder", "scheduled_task_runs insert reminder",
	}, s.taskChanges("acme"), "sam's run goes by the task it was a run of, and grace's task and run keep their changes")
	_, err = s.store.ScheduledTasks.Get(s.ctx, "acme", "reminder")
	s.Require().NoError(err)
}

func (s *StoreSuite) TestForgettingAUserKeepsTheRunsOfAnotherUsersTaskWhoseChangesWerePruned() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	due := s.base.Add(-time.Minute)
	s.task("x", func(task *ScheduledTask) { task.UserID = "grace"; task.NextRunAt = &due })
	s.task("x", func(task *ScheduledTask) { task.CustomerID = "globex"; task.NextRunAt = nil })
	_, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	_, err = s.store.global.NewDelete().TableExpr("data_changes").Where("table_name = 'scheduled_tasks'").Exec(s.ctx)
	s.Require().NoError(err)

	s.forgetSam()

	s.Equal([]string{"scheduled_task_runs insert x"}, s.taskChanges("acme"), "a task by the same id in another app is not this one")
}

func (s *StoreSuite) TestForgettingAUserKeepsTheRunsOfAnotherUsersTaskNamedLikeTheirSession() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	due := s.base.Add(-time.Minute)
	s.task("x", func(task *ScheduledTask) { task.UserID = "grace"; task.NextRunAt = &due })
	// Opened after the task, so the session's change is the latest one by that id before the run's.
	s.opened("x", "acme", s.base, func(session *AgentSession) { session.UserID = "sam" })
	_, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)

	s.forgetSam()

	s.ElementsMatch([]string{"scheduled_tasks insert x", "scheduled_tasks update x", "scheduled_task_runs insert x"}, s.taskChanges("acme"))
}

func (s *StoreSuite) TestATaskWriteTheForgetWaitsOnIsRedactedToo() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	s.task("reminder", func(task *ScheduledTask) { task.NextRunAt = nil })
	writing, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer writing.Rollback() //nolint:errcheck // committed below; this only ends a failed test's transaction
	_, err = writing.ExecContext(s.ctx, "UPDATE scheduled_tasks SET title = 'Stretch twice' WHERE customer_id = 'acme' AND id = 'reminder'")
	s.Require().NoError(err)

	forgotten := make(chan error, 1)
	go func() {
		_, err := s.store.ForgetUserTasks(s.ctx, "acme", "sam")
		forgotten <- err
	}()
	s.Require().Eventually(func() bool {
		var waiting bool
		s.Require().NoError(s.store.global.NewRaw(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%scheduled_tasks%')`).Scan(s.ctx, &waiting))
		return waiting
	}, 10*time.Second, 10*time.Millisecond, "the forget waits on the task's row")
	s.Require().NoError(writing.Commit())
	s.Require().NoError(<-forgotten)

	s.Equal([]string{"scheduled_tasks delete reminder", "scheduled_tasks delete reminder", "scheduled_tasks delete reminder"},
		s.taskChanges("acme"), "the write's change names them no more")
}

// forgetWaiting reports whether a ForgetUserTasks is waiting on a lock: a row, or an advisory one
// a test holds.
func (s *StoreSuite) forgetWaiting() bool {
	var waiting bool
	s.Require().NoError(s.store.global.NewRaw(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event_type = 'Lock'
	AND (query LIKE '%scheduled_task%' OR query LIKE '%data_changes%'))`).Scan(s.ctx, &waiting))
	return waiting
}

func (s *StoreSuite) TestARunWriteTheForgetWaitsOnIsRedactedToo() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	due := s.base.Add(-time.Minute)
	s.task("reminder", func(task *ScheduledTask) { task.NextRunAt = &due })
	_, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	// As TakeCall writes it: the run's row alone, not its task's.
	writing, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer writing.Rollback() //nolint:errcheck // committed below; this only ends a failed test's transaction
	_, err = writing.ExecContext(s.ctx, "UPDATE scheduled_task_runs SET session_id = 'call' WHERE customer_id = 'acme' AND task_id = 'reminder'")
	s.Require().NoError(err)

	forgotten := make(chan error, 1)
	go func() {
		_, err := s.store.ForgetUserTasks(s.ctx, "acme", "sam")
		forgotten <- err
	}()
	s.Require().Eventually(s.forgetWaiting, 10*time.Second, 10*time.Millisecond, "the forget waits on the run's row")
	s.Require().NoError(writing.Commit())
	s.Require().NoError(<-forgotten)

	s.ElementsMatch([]string{
		"scheduled_tasks delete reminder", "scheduled_tasks delete reminder", "scheduled_tasks delete reminder",
		"scheduled_task_runs delete ", "scheduled_task_runs delete ", "scheduled_task_runs delete ",
	}, s.taskChanges("acme"), "the run write's change, which holds the task's instructions, names them no more")
}

func (s *StoreSuite) TestATaskOfTheUsersMadeWhileTheForgetRedactsIsKeptWithItsChanges() {
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	s.task("reminder", func(task *ScheduledTask) { task.NextRunAt = nil })
	// The forget stops once it has redacted, until this connection lets it go on.
	_, err := s.store.global.ExecContext(s.ctx, `CREATE FUNCTION forget_paused() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_advisory_xact_lock(hashtextextended('forget_paused', 0)); RETURN NULL; END $$`)
	s.Require().NoError(err)
	defer func() {
		_, err := s.store.global.ExecContext(s.ctx, "DROP FUNCTION forget_paused() CASCADE")
		s.Require().NoError(err)
	}()
	_, err = s.store.global.ExecContext(s.ctx,
		"CREATE TRIGGER forget_paused AFTER UPDATE ON data_changes FOR EACH STATEMENT EXECUTE FUNCTION forget_paused()")
	s.Require().NoError(err)
	holding, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer holding.Rollback() //nolint:errcheck // rolled back below; this only ends a failed test's transaction
	_, err = holding.ExecContext(s.ctx, "SELECT pg_advisory_xact_lock(hashtextextended('forget_paused', 0))")
	s.Require().NoError(err)

	forgotten := make(chan error, 1)
	go func() {
		_, err := s.store.ForgetUserTasks(s.ctx, "acme", "sam")
		forgotten <- err
	}()
	s.Require().Eventually(s.forgetWaiting, 10*time.Second, 10*time.Millisecond, "the forget waits once it has redacted")
	s.task("late", func(task *ScheduledTask) { task.NextRunAt = nil })
	s.Require().NoError(holding.Rollback())
	s.Require().NoError(<-forgotten)

	_, err = s.store.ScheduledTasks.Get(s.ctx, "acme", "late")
	s.Require().NoError(err, "a task the forget's snapshot did not see is not deleted")
	s.ElementsMatch([]string{
		"scheduled_tasks delete reminder", "scheduled_tasks delete reminder", "scheduled_tasks insert late",
	}, s.taskChanges("acme"), "no task is deleted with a change naming them kept")
}

func (s *StoreSuite) TestAFollowerOfAnotherAppReadsOnFromItsCursorAfterAUserIsForgotten() {
	for _, customer := range []string{"acme", "globex"} {
		s.Require().NoError(s.store.StartDataCapture(s.ctx, customer, time.Hour, time.Second))
	}
	s.task("first", func(task *ScheduledTask) { task.CustomerID = "globex"; task.UserID = "kim"; task.NextRunAt = nil })
	_, cursor, err := s.store.Changes(s.ctx, "globex", 0, 1000)
	s.Require().NoError(err)
	s.task("reminder", func(task *ScheduledTask) { task.NextRunAt = nil })
	s.task("second", func(task *ScheduledTask) { task.CustomerID = "globex"; task.UserID = "kim"; task.NextRunAt = nil })
	// The follower's own change has aged out, as the prune drops the oldest changes kept.
	_, err = s.store.global.NewDelete().TableExpr("data_changes").Where("seq <= ?", cursor).Exec(s.ctx)
	s.Require().NoError(err)

	s.forgetSam()

	changes, _, err := s.store.Changes(s.ctx, "globex", cursor, 1000)
	s.Require().NoError(err, "the follower's cursor has not expired")
	s.Require().Len(changes, 1)
	s.Equal(ChangeInsert, changes[0].Op)
	s.JSONEq(`{"customer_id": "globex", "id": "second"}`, string(changes[0].Key))
}

func (s *StoreSuite) TestForgettingAUserKeepsTheChangesOfOtherTablesThatNameThem() {
	if s.store.Sharded() {
		s.T().Skip("a shard's own tables record no changes (watchDataChanges)")
	}
	s.Require().NoError(s.store.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	s.opened("one", "acme", s.base, func(session *AgentSession) { session.UserID = "sam" })

	s.forgetSam()

	changes, _, err := s.store.Changes(s.ctx, "acme", 0, 1000)
	s.Require().NoError(err)
	s.True(slices.ContainsFunc(changes, func(change DataChange) bool {
		return change.Table == "agent_sessions" && change.Op == ChangeInsert
	}), "the session stays, and so does its change")
}

func (s *StoreSuite) TestEveryTableWithAUserIDIsForgottenOrKeptForAReason() {
	var found []string
	for _, db := range []*bun.DB{s.store.global, s.store.shard} {
		var tables []string
		s.Require().NoError(db.NewRaw(`SELECT c.relname FROM pg_attribute AS a
JOIN pg_class AS c ON c.oid = a.attrelid
WHERE a.attname = 'user_id' AND NOT a.attisdropped AND c.relkind IN ('r', 'p') AND NOT c.relispartition
	AND c.relnamespace = current_schema()::regnamespace`).Scan(s.ctx, &tables))
		found = append(found, tables...)
	}
	slices.Sort(found)
	found = slices.Compact(found)

	for table, reason := range keptUserTables {
		s.NotEmpty(reason, table)
		s.NotContains(forgottenUserTables, table, "a table is forgotten or kept, not both")
	}
	listed := slices.Sorted(slices.Values(append(slices.Clone(forgottenUserTables), slices.Collect(maps.Keys(keptUserTables))...)))
	s.Equal(found, listed, "a table with a user_id column goes in forgottenUserTables or keptUserTables")
}

// tasksOn counts a customer's scheduled tasks and runs on a deployment.
func (s *DataMoveSuite) tasksOn(store *Store, customerID string) (tasks, runs int) {
	s.Require().NoError(store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT (SELECT count(*) FROM scheduled_tasks WHERE customer_id = ?), (SELECT count(*) FROM scheduled_task_runs WHERE customer_id = ?)",
		customerID, customerID).Scan(&tasks, &runs))
	return tasks, runs
}

func (s *DataMoveSuite) TestAMoveFollowingAForgottenUserEndsWithNoneOfTheirTasks() {
	s.capture("acme")
	s.seedScheduledTask("st-before", "acme", ScheduledTaskActive)
	// Into the same app id, as a move between deployments is: a task's key names its customer.
	cursor := s.move("acme", "acme")
	s.seedScheduledTask("st-after", "acme", ScheduledTaskActive)
	_, err := s.source.ForgetUserTasks(s.ctx, "acme", "sam")
	s.Require().NoError(err)

	changes, _, err := s.source.Changes(s.ctx, "acme", cursor, 100)
	s.Require().NoError(err)
	s.Require().NoError(s.destination.ApplyChanges(s.ctx, "acme", changes))
	tasks, runs := s.tasksOn(s.destination, "acme")
	s.Zero(tasks)
	s.Zero(runs)

	every, _, err := s.source.Changes(s.ctx, "acme", 0, 100)
	s.Require().NoError(err)
	s.Require().NoError(s.destination.ApplyChanges(s.ctx, "acme", every), "a follower starting over replays the redacted changes")
	tasks, runs = s.tasksOn(s.destination, "acme")
	s.Zero(tasks)
	s.Zero(runs)
}
