//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/uptrace/bun"
)

// task stores a scheduled task for acme's user sam, due in an hour, defaulting
// the fields a test does not care about.
func (s *StoreSuite) task(id string, change func(*ScheduledTask)) *ScheduledTask {
	task := &ScheduledTask{
		ID:           id,
		CustomerID:   "acme",
		ConfigID:     "config-1",
		UserID:       "sam",
		CallerKind:   "authenticated",
		Title:        "Stretch",
		Instructions: "Remind Sam to stretch.",
		StartLocal:   s.base,
		Timezone:     "Europe/Amsterdam",
		State:        ScheduledTaskActive,
		CreatedBy:    "api",
	}
	next := s.base.Add(time.Hour)
	task.NextRunAt = &next
	if change != nil {
		change(task)
	}
	s.Require().NoError(s.store.ScheduledTasks.Insert(s.ctx, task, 25))
	return task
}

func (s *StoreSuite) TestAScheduledTaskRoundTrips() {
	s.task("one", nil)

	found, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal("Stretch", found.Title)
	s.Equal("Europe/Amsterdam", found.Timezone)
	s.Require().NotNil(found.NextRunAt)
	s.Equal(s.base.Add(time.Hour), *found.NextRunAt)
}

func (s *StoreSuite) TestAScheduledTaskIsOnlyItsOwnCustomers() {
	s.task("one", nil)

	_, err := s.store.ScheduledTasks.Get(s.ctx, "globex", "one")
	s.ErrorIs(err, ErrNoScheduledTask)

	tasks, err := s.store.ScheduledTasks.Select(s.ctx, "globex", ScheduledTaskFilter{Limit: 25})
	s.Require().NoError(err)
	s.Empty(tasks)
}

func (s *StoreSuite) TestACreateUnderATakenIDIsRefused() {
	s.task("one", nil)

	dupe := *s.task("two", nil)
	dupe.ID = "one"
	err := s.store.ScheduledTasks.Insert(s.ctx, &dupe, 25)
	s.ErrorIs(err, ErrScheduledTaskIDTaken)
}

func (s *StoreSuite) TestTheActiveLimitIsEnforced() {
	s.task("one", nil)

	over := &ScheduledTask{
		CustomerID: "acme", ConfigID: "config-1", UserID: "sam", CallerKind: "authenticated",
		Title: "Second", Instructions: "Again.", StartLocal: s.base, Timezone: "UTC",
	}
	err := s.store.ScheduledTasks.Insert(s.ctx, over, 1)
	s.ErrorIs(err, ErrTooManyScheduledTasks)

	// A paused task does not count against the limit.
	s.task("paused", func(task *ScheduledTask) { task.State = ScheduledTaskPaused })
	s.Require().NoError(s.store.ScheduledTasks.Insert(s.ctx, &ScheduledTask{
		CustomerID: "acme", ConfigID: "config-1", UserID: "sam", CallerKind: "authenticated",
		Title: "Third", Instructions: "Again.", StartLocal: s.base, Timezone: "UTC",
	}, 2))
}

func (s *StoreSuite) TestTheActiveLimitCountsTheTasksOnTheSameConfigOnly() {
	s.task("one", nil)

	s.Require().NoError(s.store.ScheduledTasks.Insert(s.ctx, &ScheduledTask{
		CustomerID: "acme", ConfigID: "config-2", UserID: "sam", CallerKind: "authenticated",
		Title: "Elsewhere", Instructions: "Again.", StartLocal: s.base, Timezone: "UTC",
	}, 1), "a task on another config does not count against this one's limit")

	for config, want := range map[string]int{"config-1": 1, "config-2": 1, "config-3": 0} {
		count, err := s.store.ScheduledTasks.CountActive(s.ctx, "acme", "sam", config)
		s.Require().NoError(err)
		s.Equal(want, count, config)
	}
	count, err := s.store.ScheduledTasks.CountActive(s.ctx, "acme", "kim", "config-1")
	s.Require().NoError(err)
	s.Zero(count, "another user's tasks are theirs")
	count, err = s.store.ScheduledTasks.CountActive(s.ctx, "globex", "sam", "config-1")
	s.Require().NoError(err)
	s.Zero(count, "another customer's tasks are theirs")
	s.task("paused", func(task *ScheduledTask) { task.State = ScheduledTaskPaused })
	count, err = s.store.ScheduledTasks.CountActive(s.ctx, "acme", "sam", "config-1")
	s.Require().NoError(err)
	s.Equal(1, count, "a paused task does not count")
}

func (s *StoreSuite) TestClaimingGivesEachDueTaskToOneWorker() {
	s.task("one", func(task *ScheduledTask) {
		due := s.base.Add(-time.Minute)
		task.NextRunAt = &due
	})
	other, err := s.openRouter()
	s.Require().NoError(err)
	defer other.Close()

	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Equal("one", claimed[0].Task.ID)
	s.Equal(ScheduledTaskRunRunning, claimed[0].Run.State)
	s.Equal("Remind Sam to stretch.", claimed[0].Run.Prompt)

	// The lease keeps the second worker out.
	again, err := other.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Empty(again)
}

func (s *StoreSuite) TestAClaimInsertsOneRunPerOccurrence() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })

	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	runs, err := s.store.ScheduledTaskRuns.Select(s.ctx, "acme", ScheduledTaskRunFilter{TaskID: "one", Limit: 25})
	s.Require().NoError(err)
	s.Require().Len(runs, 1)
	s.Equal(due, runs[0].ScheduledFor)
}

func (s *StoreSuite) TestAStaleRunIsReclaimedUnderTheSameRun() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })

	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	first := claimed[0]

	// The worker dies: its run ages past the stale window and its lease lapses.
	_, err = s.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("started_at = ?", s.base.Add(-time.Hour)).
		Where("id = ?", first.Run.ID).Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = ?", s.base.Add(-time.Minute)).
		Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)

	reclaimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(reclaimed, 1)
	s.Equal(first.Run.ID, reclaimed[0].Run.ID, "the same run, so the same command id")
	s.Equal(2, reclaimed[0].Run.Attempts)
}

func (s *StoreSuite) TestARenewalExtendsOnlyTheClaimItHolds() {
	due := s.base.Add(-time.Minute)
	s.task("st-renew", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 10, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	held, err := s.store.ScheduledTasks.Renew(s.ctx, "acme", "st-renew", claimed[0].LeasedBy, time.Hour)
	s.Require().NoError(err)
	s.True(held)
	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "st-renew")
	s.Require().NoError(err)
	s.Require().NotNil(task.LeaseUntil)
	s.WithinDuration(time.Now().Add(time.Hour), *task.LeaseUntil, time.Minute)

	// A pause clears the claim, so the worker that had it holds nothing.
	task.State = ScheduledTaskPaused
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))
	held, err = s.store.ScheduledTasks.Renew(s.ctx, "acme", "st-renew", claimed[0].LeasedBy, time.Hour)
	s.Require().NoError(err)
	s.False(held)
}

func (s *StoreSuite) TestAFinishUnderAStolenClaimDoesNotAdvanceTheTask() {
	due := s.base.Add(-time.Minute)
	// A repeat, whose edit keeps its next run (an edited one-shot comes due
	// again at the edit instead).
	s.task("one", func(task *ScheduledTask) { task.NextRunAt, task.RRule = &due, "FREQ=DAILY" })

	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	// Somebody edits the task while it is claimed: the claim is fenced.
	stored, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	stored.Title = "Edited"
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &stored))

	next := s.base.Add(24 * time.Hour)
	applied, err := s.store.ScheduledTasks.Finish(s.ctx, "acme", claimed[0].Run.ID, claimed[0].LeasedBy, ScheduledTaskOutcome{
		RunState: ScheduledTaskRunDelivered, DeliveredTo: ScheduledTaskDeliveredConversation,
		NextRunAt: &next, State: ScheduledTaskActive,
	})
	s.Require().NoError(err)
	s.False(applied, "a fenced finish must not advance the schedule")

	after, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Require().NotNil(after.NextRunAt)
	s.Equal(due, *after.NextRunAt, "the edited task keeps its own next run")

	// The run is still recorded.
	runs, err := s.store.ScheduledTaskRuns.Select(s.ctx, "acme", ScheduledTaskRunFilter{TaskID: "one", Limit: 25})
	s.Require().NoError(err)
	s.Require().Len(runs, 1)
	s.Equal(ScheduledTaskRunDelivered, runs[0].State)
}

func (s *StoreSuite) TestAFinishAdvancesTheTask() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })

	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	next := s.base.Add(24 * time.Hour)
	applied, err := s.store.ScheduledTasks.Finish(s.ctx, "acme", claimed[0].Run.ID, claimed[0].LeasedBy, ScheduledTaskOutcome{
		RunState: ScheduledTaskRunDelivered, DeliveredTo: ScheduledTaskDeliveredConversation,
		NextRunAt: &next, State: ScheduledTaskActive,
	})
	s.Require().NoError(err)
	s.True(applied)

	after, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Require().NotNil(after.NextRunAt)
	s.Equal(next, *after.NextRunAt)
	s.Empty(after.LeasedBy)
}

func (s *StoreSuite) TestDeletingASessionDeletesItsTasks() {
	s.task("one", func(task *ScheduledTask) { task.SessionID = "session-1" })
	s.task("two", func(task *ScheduledTask) { task.SessionID = "session-2" })

	s.Require().NoError(s.store.ScheduledTasks.DeleteForSession(s.ctx, "acme", "session-1"))

	_, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.ErrorIs(err, ErrNoScheduledTask)
	_, err = s.store.ScheduledTasks.Get(s.ctx, "acme", "two")
	s.Require().NoError(err)
}

func (s *StoreSuite) TestDeletingATaskDeletesItsRuns() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	_, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)

	s.Require().NoError(s.store.ScheduledTasks.Delete(s.ctx, "acme", "one"))

	runs, err := s.store.ScheduledTaskRuns.Select(s.ctx, "acme", ScheduledTaskRunFilter{TaskID: "one", Limit: 25})
	s.Require().NoError(err)
	s.Empty(runs)
}

func (s *StoreSuite) TestQuietHoursRoundTrip() {
	s.task("one", func(task *ScheduledTask) {
		task.QuietHours = &ScheduledTaskQuietHours{Start: "22:00", End: "07:00"}
	})

	found, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Require().NotNil(found.QuietHours)
	s.Equal("22:00", found.QuietHours.Start)
	s.Equal("07:00", found.QuietHours.End)
}

func (s *StoreSuite) TestARenewalAfterAHoldDoesNotCutItShort() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	until := time.Now().Add(2 * time.Hour)
	held, err := s.store.ScheduledTasks.Postpone(s.ctx, "acme", "one", claimed[0].Run.ID, claimed[0].LeasedBy, until)
	s.Require().NoError(err)
	s.Require().True(held)

	held, err = s.store.ScheduledTasks.Renew(s.ctx, "acme", "one", claimed[0].LeasedBy, time.Minute)
	s.Require().NoError(err)
	s.True(held, "the claim is still the worker's")

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Require().NotNil(task.LeaseUntil)
	s.WithinDuration(until, *task.LeaseUntil, time.Second, "the hold outlasts the renewal's minute")
}

func (s *StoreSuite) TestAPostponedTaskIsHeldUnderItsClaimUntilTheHoldEnds() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })

	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	until := time.Now().Add(2 * time.Hour)
	held, err := s.store.ScheduledTasks.Postpone(s.ctx, "acme", "one", claimed[0].Run.ID, claimed[0].LeasedBy, until)
	s.Require().NoError(err)
	s.True(held)

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Require().NotNil(task.LeaseUntil)
	s.True(task.LeaseUntil.After(time.Now().Add(time.Hour)), "the lease is the hold, not the claim's minute")
	s.Equal(claimed[0].LeasedBy, task.LeasedBy, "the claim is kept, so no other worker takes the occurrence")

	runs, err := s.store.ScheduledTaskRuns.Select(s.ctx, "acme", ScheduledTaskRunFilter{TaskID: "one", Limit: 25})
	s.Require().NoError(err)
	s.Require().Len(runs, 1)
	s.Equal(ScheduledTaskRunRunning, runs[0].State, "a held run is still running")
	s.Require().NotNil(runs[0].PostponedUntil)
	s.WithinDuration(until, *runs[0].PostponedUntil, time.Second)

	// Nobody claims it while the hold stands.
	again, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Empty(again)

	// The hold ends: the run is taken up at once under the same run, however
	// recently it started, because nobody is delivering it. It started before
	// the hold was recorded, as every held run did.
	ended := time.Now().Add(-time.Minute)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = ?", ended).
		Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("postponed_until = ?", ended).
		Set("started_at = ?", ended.Add(-time.Second)).
		Where("id = ?", claimed[0].Run.ID).Exec(s.ctx)
	s.Require().NoError(err)
	taken, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(taken, 1)
	s.Equal(claimed[0].Run.ID, taken[0].Run.ID)
	s.Require().NotNil(taken[0].Run.PostponedUntil, "the claim remembers the hold, so the age counts from its end")
}

func (s *StoreSuite) TestAPostponeUnderAStolenClaimHoldsNothing() {
	due := s.base.Add(-time.Minute)
	// A repeat: an edit to a one-shot being delivered leaves it that delivery.
	s.task("one", func(task *ScheduledTask) { task.NextRunAt, task.RRule = &due, "FREQ=DAILY" })

	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	stored, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	stored.Title = "Edited"
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &stored))

	held, err := s.store.ScheduledTasks.Postpone(s.ctx, "acme", "one", claimed[0].Run.ID, claimed[0].LeasedBy, time.Now().Add(time.Hour))
	s.Require().NoError(err)
	s.False(held, "a fenced claim holds nothing: the edit decides what happens")
}

func (s *StoreSuite) TestAUserTimezoneIsStoredAndMoved() {
	s.Require().NoError(s.store.Users.SetTimezone(s.ctx, "acme", "sam", "authenticated", "Europe/Amsterdam"))
	zone, err := s.store.Users.GetTimezone(s.ctx, "acme", "sam")
	s.Require().NoError(err)
	s.Equal("Europe/Amsterdam", zone)

	// The latest explicit zone wins, and the row's kind is left alone.
	s.Require().NoError(s.store.Users.SetTimezone(s.ctx, "acme", "sam", "guest", "Asia/Tokyo"))
	zone, err = s.store.Users.GetTimezone(s.ctx, "acme", "sam")
	s.Require().NoError(err)
	s.Equal("Asia/Tokyo", zone)

	var user User
	s.Require().NoError(s.store.global.NewSelect().Model(&user).
		Where("customer_id = ?", "acme").Where("id = ?", "sam").Scan(s.ctx))
	s.Equal("authenticated", user.Kind, "moving a zone never rewrites what the credential proved")
}

// held claims the due task id and holds its occurrence until until, as quiet
// hours do, returning the claim.
func (s *StoreSuite) held(id string, until time.Time) ClaimedScheduledTask {
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Require().Equal(id, claimed[0].Task.ID)
	held, err := s.store.ScheduledTasks.Postpone(s.ctx, "acme", id, claimed[0].Run.ID, claimed[0].LeasedBy, until)
	s.Require().NoError(err)
	s.Require().True(held)
	return claimed[0]
}

// run reads one run back.
func (s *StoreSuite) run(id string) ScheduledTaskRun {
	var run ScheduledTaskRun
	s.Require().NoError(s.store.global.NewSelect().Model(&run).Where("id = ?", id).Scan(s.ctx))
	return run
}

func (s *StoreSuite) TestPausingATaskWritesItsHeldOccurrenceMissed() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	s.task("two", func(task *ScheduledTask) { task.NextRunAt = nil })
	s.Require().NoError(s.store.ScheduledTasks.Insert(s.ctx, &ScheduledTask{
		ID: "one", CustomerID: "globex", UserID: "sam", Title: "Theirs", Instructions: "Again.",
		StartLocal: s.base, Timezone: "UTC",
	}, 0))
	claim := s.held("one", time.Now().Add(8*time.Hour))
	// What the pause must leave alone: an earlier occurrence already delivered,
	// a run made by hand, and the running runs of another task and of another
	// customer's task under the same id.
	delivered := &ScheduledTaskRun{TaskID: "one", CustomerID: "acme", ScheduledFor: due.Add(-24 * time.Hour), State: ScheduledTaskRunDelivered}
	manual := &ScheduledTaskRun{TaskID: "one", CustomerID: "acme", ScheduledFor: due, Trigger: ScheduledTaskTriggerManual}
	other := &ScheduledTaskRun{TaskID: "two", CustomerID: "acme", ScheduledFor: due}
	theirs := &ScheduledTaskRun{TaskID: "one", CustomerID: "globex", ScheduledFor: due}
	for _, run := range []*ScheduledTaskRun{delivered, manual, other, theirs} {
		s.Require().NoError(s.store.ScheduledTaskRuns.Insert(s.ctx, run))
	}

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	task.State = ScheduledTaskPaused
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))

	missed := s.run(claim.Run.ID)
	s.Equal(ScheduledTaskRunMissed, missed.State, "the held occurrence is settled, not left running for good")
	s.NotNil(missed.FinishedAt)
	s.Equal(ScheduledTaskRunDelivered, s.run(delivered.ID).State)
	s.Equal(ScheduledTaskRunRunning, s.run(manual.ID).State, "a run by hand records its own outcome")
	s.Equal(ScheduledTaskRunRunning, s.run(other.ID).State)
	s.Equal(ScheduledTaskRunRunning, s.run(theirs.ID).State)
}

func (s *StoreSuite) TestAnEditKeepingTheNextRunSettlesARepeatsOccurrenceRatherThanDeliveringItAgain() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt, task.RRule = &due, "FREQ=DAILY" })
	claim := s.held("one", time.Now().Add(8*time.Hour))

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	task.Instructions = "Remind Sam to drink water."
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))
	s.True(task.NextRunAt.Equal(due), "a repeat keeps its schedule")

	// The edit cleared the claim and kept the next run, so the task is due
	// again at once: its occurrence is only advanced past.
	again, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(again, 1)
	s.True(again[0].AlreadySettled, "the old instructions are not delivered")
	s.Equal(claim.Run.ID, again[0].Run.ID)
	s.Equal(ScheduledTaskRunMissed, again[0].Run.State)
	s.False(again[0].WorkersLost, "the run was settled by the edit, not failed by this claim")
}

func (s *StoreSuite) TestAnEditedOneShotWhoseOccurrenceWasHeldComesDueAgainAsItNowReads() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claim := s.held("one", time.Now().Add(8*time.Hour))
	// Not this occurrence's delivery under way: a run made by hand for the
	// same time, and the deliveries of another task and of another
	// customer's task under the same id.
	s.Require().NoError(s.store.ScheduledTaskRuns.Insert(s.ctx, &ScheduledTaskRun{
		TaskID: "one", CustomerID: "acme", ScheduledFor: due, Trigger: ScheduledTaskTriggerManual,
	}))
	s.task("two", func(task *ScheduledTask) { task.NextRunAt = &due })
	s.Require().NoError(s.store.ScheduledTasks.Insert(s.ctx, &ScheduledTask{
		ID: "one", CustomerID: "globex", UserID: "sam", Title: "Theirs", Instructions: "Again.",
		StartLocal: s.base, Timezone: "UTC", State: ScheduledTaskActive, NextRunAt: &due,
	}, 0))
	others, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(others, 2)

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	task.Instructions = "Remind Sam to drink water."
	before := time.Now()
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))

	stored, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(ScheduledTaskActive, stored.State)
	s.Require().NotNil(stored.NextRunAt)
	s.WithinDuration(before, *stored.NextRunAt, 5*time.Second, "due again at the save")
	s.True(task.NextRunAt.Equal(*stored.NextRunAt), "the caller sees the next run that was saved")
	s.Equal(ScheduledTaskRunMissed, s.run(claim.Run.ID).State, "the held occurrence is settled")

	again, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(again, 1)
	s.False(again[0].AlreadySettled, "the reminder is delivered, not lost")
	s.NotEqual(claim.Run.ID, again[0].Run.ID)
	s.Equal("Remind Sam to drink water.", again[0].Run.Prompt)
	s.Nil(again[0].Run.PostponedUntil, "the new occurrence is held only if quiet hours hold it when it is delivered")
}

func (s *StoreSuite) TestAPausedOrMovedOneShotIsNotBroughtForwardBySettlingItsHeldOccurrence() {
	due := s.base.Add(-time.Minute)
	s.task("paused", func(task *ScheduledTask) { task.NextRunAt = &due })
	s.held("paused", time.Now().Add(8*time.Hour))
	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "paused")
	s.Require().NoError(err)
	task.State = ScheduledTaskPaused
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))
	stored, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "paused")
	s.Require().NoError(err)
	s.True(stored.NextRunAt.Equal(due), "a pause decides what comes next, not the save")

	s.task("moved", func(task *ScheduledTask) { task.NextRunAt = &due })
	s.held("moved", time.Now().Add(8*time.Hour))
	task, err = s.store.ScheduledTasks.Get(s.ctx, "acme", "moved")
	s.Require().NoError(err)
	later := time.Now().Add(3 * time.Hour).UTC().Truncate(time.Second)
	task.NextRunAt = &later
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))
	stored, err = s.store.ScheduledTasks.Get(s.ctx, "acme", "moved")
	s.Require().NoError(err)
	s.True(stored.NextRunAt.Equal(later), "a one-shot moved by the edit runs when it was moved to")
}

func (s *StoreSuite) TestAnEditedOneShotBeingDeliveredKeepsItsClaimAndItsRun() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	task.Instructions = "Remind Sam to drink water."
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))

	stored, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal("Remind Sam to drink water.", stored.Instructions)
	s.Equal(claimed[0].LeasedBy, stored.LeasedBy, "the worker delivering it keeps its claim")
	s.True(stored.NextRunAt.Equal(due), "the one-shot is not brought forward to be delivered again")
	s.Equal(ScheduledTaskRunRunning, s.run(claimed[0].Run.ID).State, "the delivery under way is not written missed")
	again, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Empty(again, "nothing starts a second delivery")

	applied, err := s.store.ScheduledTasks.Finish(s.ctx, "acme", claimed[0].Run.ID, claimed[0].LeasedBy, ScheduledTaskOutcome{
		RunState: ScheduledTaskRunDelivered, DeliveredTo: ScheduledTaskDeliveredConversation,
		State: ScheduledTaskCompleted,
	})
	s.Require().NoError(err)
	s.True(applied, "the delivery completes the one-shot")
}

func (s *StoreSuite) TestAnEditedOneShotTakenUpAgainAfterItsHoldKeepsItsClaim() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claim := s.held("one", time.Now().Add(8*time.Hour))
	// The hold ended a moment ago, and a worker takes the occurrence up again.
	_, err := s.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("started_at = now() - interval '2 seconds'").
		Set("postponed_until = now() - interval '1 second'").
		Where("id = ?", claim.Run.ID).Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = now() - interval '1 second'").
		Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)
	taken, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(taken, 1)
	s.Require().Equal(claim.Run.ID, taken[0].Run.ID)

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	task.Instructions = "Remind Sam to drink water."
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))

	stored, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(taken[0].LeasedBy, stored.LeasedBy, "a hold that has ended no longer holds the delivery")
	s.Equal(ScheduledTaskRunRunning, s.run(claim.Run.ID).State)
}

func (s *StoreSuite) TestAnEditedOneShotWhoseWorkerLeaseLapsedComesDueAgain() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = now() - interval '1 second'").
		Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	task.Instructions = "Remind Sam to drink water."
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))

	s.Equal(ScheduledTaskRunMissed, s.run(claimed[0].Run.ID).State, "nobody holds it: the edit settles it")
	again, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(again, 1, "the one-shot is due again at the edit")
	s.False(again[0].AlreadySettled)
	s.Equal("Remind Sam to drink water.", again[0].Run.Prompt)
}

func (s *StoreSuite) TestAnEditedOneShotWhoseLostWorkerPlacedACallIsCompletedNotRungAgain() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	took, _, err := s.store.ScheduledTaskRuns.TakeCall(s.ctx, "acme", claimed[0].Run.ID, "call-1")
	s.Require().NoError(err)
	s.Require().True(took)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = now() - interval '1 second'").
		Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	task.Instructions = "Remind Sam to drink water."
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))

	s.Equal(ScheduledTaskCompleted, task.State, "the caller reads back what was saved")
	stored, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(ScheduledTaskCompleted, stored.State)
	s.Nil(stored.NextRunAt)
	s.Equal(ScheduledTaskRunMissed, s.run(claimed[0].Run.ID).State)
	again, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Empty(again, "the user is not rung a second time")
}

func (s *StoreSuite) TestAOneShotMovedOffAnOccurrenceWhoseLostWorkerPlacedACallStaysAtItsNewTime() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	took, _, err := s.store.ScheduledTaskRuns.TakeCall(s.ctx, "acme", claimed[0].Run.ID, "call-1")
	s.Require().NoError(err)
	s.Require().True(took)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = now() - interval '1 second'").
		Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	later := s.base.Add(time.Hour)
	task.NextRunAt = &later
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))

	stored, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(ScheduledTaskActive, stored.State, "the call was for the occurrence the task moved off")
	s.Require().NotNil(stored.NextRunAt)
	s.True(later.Equal(*stored.NextRunAt), "%v", stored.NextRunAt)
	s.Equal(ScheduledTaskRunMissed, s.run(claimed[0].Run.ID).State)
}

func (s *StoreSuite) TestAOneShotMovedWhileBeingDeliveredLetsThatDeliveryGo() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	later := time.Now().Add(3 * time.Hour).UTC().Truncate(time.Second)
	task.NextRunAt = &later
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &task))

	stored, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Empty(stored.LeasedBy, "the move decides what comes next, not the worker")
	s.True(stored.NextRunAt.Equal(later))
	s.Equal(ScheduledTaskRunMissed, s.run(claimed[0].Run.ID).State)
}

// lapse ages a run past the stale window and lets its task's lease run out,
// as a worker that died leaves them.
func (s *StoreSuite) lapse(taskID, runID string) {
	_, err := s.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("started_at = ?", time.Now().Add(-time.Hour)).
		Where("id = ?", runID).Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = ?", time.Now().Add(-time.Minute)).
		Where("id = ?", taskID).Exec(s.ctx)
	s.Require().NoError(err)
}

func (s *StoreSuite) TestAnOccurrenceWhoseWorkersKeepDyingIsFailedAtTheAttemptCap() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	runID := claimed[0].Run.ID

	for attempts := 2; attempts <= maxScheduledTaskRunAttempts; attempts++ {
		s.lapse("one", runID)
		taken, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
		s.Require().NoError(err)
		s.Require().Len(taken, 1)
		s.False(taken[0].AlreadySettled, "attempt %d is still taken over", attempts)
		s.Equal(attempts, taken[0].Run.Attempts)
	}

	s.lapse("one", runID)
	failed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(failed, 1)
	s.True(failed[0].AlreadySettled, "the worker settles the task without delivering again")
	s.True(failed[0].WorkersLost, "the worker says why")
	s.Equal(ScheduledTaskRunFailed, failed[0].Run.State)
	stored := s.run(runID)
	s.Equal(ScheduledTaskRunFailed, stored.State)
	s.Equal(3, stored.Attempts, "the first claim and two takeovers")
	s.Require().NotNil(stored.Error)
	s.Equal("delivery_failed", *stored.Error, "a code a device is shown, not the story")
	s.NotNil(stored.FinishedAt)
	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(failed[0].LeasedBy, task.LeasedBy, "the settle runs under a claim of its own")
}

func (s *StoreSuite) TestTheAttemptCapRecordsWhatThePlacedCallLeft() {
	due := s.base.Add(-time.Minute)
	started := time.Now().Add(-time.Hour)
	notAnswered, empty := "not_answered", ""
	cases := []struct {
		task    string
		session string
		reason  *string
		want    string
	}{
		{task: "no-call", want: "delivery_failed"},
		{task: "no-call-with-text", reason: &notAnswered, want: "delivery_failed"},
		{task: "call-no-end", session: "call-1", want: "already_called"},
		{task: "call-empty-end", session: "call-2", reason: &empty, want: "already_called"},
		{task: "call-ended", session: "call-3", reason: &notAnswered, want: "not_answered"},
	}
	runs := map[string]string{}
	for _, c := range cases {
		s.task(c.task, func(task *ScheduledTask) { task.NextRunAt = &due })
		// Three workers took it up and died: the store's cap.
		run := &ScheduledTaskRun{TaskID: c.task, CustomerID: "acme", ScheduledFor: due,
			Attempts: maxScheduledTaskRunAttempts, StartedAt: &started,
			SessionID: c.session, Error: c.reason}
		s.Require().NoError(s.store.ScheduledTaskRuns.Insert(s.ctx, run))
		runs[c.task] = run.ID
	}

	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, len(cases))
	for _, claim := range claimed {
		s.True(claim.WorkersLost, claim.Task.ID)
	}
	for _, c := range cases {
		stored := s.run(runs[c.task])
		s.Equal(ScheduledTaskRunFailed, stored.State, c.task)
		s.Require().NotNil(stored.Error, c.task)
		s.Equal(c.want, *stored.Error, c.task)
		s.Equal(c.session, stored.SessionID, "%s: the call stays on the run", c.task)
	}
}

func (s *StoreSuite) TestALeaseLapsingAfterAHoldWasTakenUpIsParkedLikeAnyOther() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claim := s.held("one", time.Now().Add(time.Hour))
	ended := time.Now().Add(-time.Minute)
	_, err := s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = ?", ended).Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("postponed_until = ?", ended).Set("started_at = ?", ended.Add(-time.Second)).
		Where("id = ?", claim.Run.ID).Exec(s.ctx)
	s.Require().NoError(err)
	taken, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(taken, 1, "the hold ended, so the run is taken up at once")

	// The worker that took it up stops renewing a moment later: it may only be
	// slow, so the run waits out the stale window like any other.
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = now() - interval '1 second'").Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)
	again, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Empty(again)
	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Require().NotNil(task.LeaseUntil)
	s.WithinDuration(taken[0].Run.StartedAt.Add(scheduledTaskStaleRun), *task.LeaseUntil, time.Second, "parked until the run is stale")
	s.Equal(2, s.run(claim.Run.ID).Attempts)
}

func (s *StoreSuite) TestALastAttemptWhoseLeaseLapsesAMomentIsParkedNotFailed() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	runID := claimed[0].Run.ID
	// Two takeovers ago, and the worker on the third started a moment ago: it
	// may only be slow, so its delivery is not cut by the attempt cap.
	started := time.Now().Add(-time.Minute)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("attempts = ?", maxScheduledTaskRunAttempts).Set("started_at = ?", started).
		Where("id = ?", runID).Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = now() - interval '1 second'").Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)

	again, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Empty(again)
	stored := s.run(runID)
	s.Equal(ScheduledTaskRunRunning, stored.State)
	s.Equal(maxScheduledTaskRunAttempts, stored.Attempts)
	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Require().NotNil(task.LeaseUntil)
	s.WithinDuration(started.Add(scheduledTaskStaleRun), *task.LeaseUntil, time.Second, "parked until the run is stale")
}

func (s *StoreSuite) TestTheNextWakeIsWhenATaskMayBeClaimedNotWhenItWasDue() {
	at := func(offset time.Duration) *time.Time {
		when := s.base.Add(offset)
		return &when
	}
	lease := func(id string, until *time.Time) {
		_, err := s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
			Set("lease_until = ?", until).Where("id = ?", id).Exec(s.ctx)
		s.Require().NoError(err)
	}
	wake := func() time.Time {
		next, due, err := s.store.ScheduledTasks.NextDue(s.ctx)
		s.Require().NoError(err)
		s.Require().True(due)
		return next
	}
	// Neither a paused task nor one with no next run wakes anybody.
	s.task("paused", func(task *ScheduledTask) { task.NextRunAt, task.State = at(-3*time.Hour), ScheduledTaskPaused })
	s.task("over", func(task *ScheduledTask) { task.NextRunAt = nil })
	lease("over", at(-3*time.Hour))
	s.task("held", func(task *ScheduledTask) { task.NextRunAt = at(-time.Minute) })
	lease("held", at(2*time.Hour))

	s.Equal(s.base.Add(2*time.Hour), wake(), "a held task wakes the worker when its hold ends")

	// A lease that ran out before the next run leaves the next run.
	s.task("lapsed", func(task *ScheduledTask) { task.NextRunAt = at(30 * time.Minute) })
	lease("lapsed", at(-time.Hour))
	s.Equal(s.base.Add(30*time.Minute), wake())
}

// queries keeps the text of every query a pool sends.
type queries struct{ sent []string }

func (*queries) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context { return ctx }
func (q *queries) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	q.sent = append(q.sent, event.Query)
}

// The worker asks when to wake after every look, so the soonest wake is read
// off scheduled_tasks_wake_idx in order, not by sorting every active task.
func (s *StoreSuite) TestTheNextWakeIsReadOffItsIndex() {
	sent := &queries{}
	watched := &Store{global: s.store.global.WithQueryHook(sent)}
	watched.wire()
	_, _, err := watched.ScheduledTasks.NextDue(s.ctx)
	s.Require().NoError(err)
	s.Require().Len(sent.sent, 1)
	s.Contains(sent.sent[0], "ORDER BY GREATEST(next_run_at, lease_until) ASC LIMIT 1")

	var plan []string
	err = s.store.global.RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// On a table this small the planner may read another index and sort: which is cheaper moves
		// with the statistics other tests leave. With sorts off, one shows only if no index reads
		// in order.
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off; SET LOCAL enable_sort = off"); err != nil {
			return err
		}
		return tx.NewRaw("EXPLAIN "+sent.sent[0]).Scan(ctx, &plan)
	})
	s.Require().NoError(err)
	read := strings.Join(plan, "\n")
	s.Contains(read, "Index Scan using scheduled_tasks_wake_idx", read)
	s.NotContains(read, "Sort", read)
	s.NotContains(read, "Filter", "the index holds only the tasks that wake the worker: %s", read)
}

func (s *StoreSuite) TestDeletingAConfigPausesOnlyItsActiveTasks() {
	// Its last change a day ago, read back as Postgres keeps it: the pause stamps updated_at with
	// Postgres's clock, which need not agree with this process's to the microsecond.
	s.aged("one", "acme", ScheduledTaskActive, day)
	created, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.task("done", func(task *ScheduledTask) { task.State = ScheduledTaskCompleted })
	s.task("other-config", func(task *ScheduledTask) { task.ConfigID = "config-2" })
	s.task("no-config", func(task *ScheduledTask) { task.ConfigID = "" })
	s.task("other-customer", func(task *ScheduledTask) { task.CustomerID = "globex" })

	s.Require().NoError(s.store.ScheduledTasks.PauseForConfig(s.ctx, "acme", "config-1"))
	s.Require().NoError(s.store.ScheduledTasks.PauseForConfig(s.ctx, "acme", ""))

	for id, want := range map[string]string{
		"one":          ScheduledTaskPaused,
		"done":         ScheduledTaskCompleted,
		"other-config": ScheduledTaskActive,
		"no-config":    ScheduledTaskActive,
	} {
		task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", id)
		s.Require().NoError(err)
		s.Equal(want, task.State, id)
	}
	paused, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.True(paused.UpdatedAt.After(created.UpdatedAt), "the pause is a change to the task")
	other, err := s.store.ScheduledTasks.Get(s.ctx, "globex", "other-customer")
	s.Require().NoError(err)
	s.Equal(ScheduledTaskActive, other.State, "another customer's config of the same id is not this one")
}

func (s *StoreSuite) TestDeletingAConfigWritesItsTasksHeldOccurrenceMissed() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	s.task("elsewhere", func(task *ScheduledTask) { task.ConfigID, task.NextRunAt = "config-2", nil })
	s.task("one", func(task *ScheduledTask) { task.CustomerID, task.NextRunAt = "globex", nil })
	claim := s.held("one", time.Now().Add(8*time.Hour))
	// What the pause must leave alone: an earlier occurrence already delivered,
	// a run made by hand, the running run of a task on another config, and that
	// of another customer's task under the same id and config.
	delivered := &ScheduledTaskRun{TaskID: "one", CustomerID: "acme", ScheduledFor: due.Add(-24 * time.Hour), State: ScheduledTaskRunDelivered}
	manual := &ScheduledTaskRun{TaskID: "one", CustomerID: "acme", ScheduledFor: due, Trigger: ScheduledTaskTriggerManual}
	elsewhere := &ScheduledTaskRun{TaskID: "elsewhere", CustomerID: "acme", ScheduledFor: due}
	theirs := &ScheduledTaskRun{TaskID: "one", CustomerID: "globex", ScheduledFor: due}
	for _, run := range []*ScheduledTaskRun{delivered, manual, elsewhere, theirs} {
		s.Require().NoError(s.store.ScheduledTaskRuns.Insert(s.ctx, run))
	}

	s.Require().NoError(s.store.ScheduledTasks.PauseForConfig(s.ctx, "acme", "config-1"))

	missed := s.run(claim.Run.ID)
	s.Equal(ScheduledTaskRunMissed, missed.State, "no worker claims a paused task, so the held occurrence is settled now")
	s.NotNil(missed.FinishedAt)
	s.Equal(ScheduledTaskRunDelivered, s.run(delivered.ID).State)
	s.Equal(ScheduledTaskRunRunning, s.run(manual.ID).State, "a run by hand records its own outcome")
	s.Equal(ScheduledTaskRunRunning, s.run(elsewhere.ID).State)
	s.Equal(ScheduledTaskRunRunning, s.run(theirs.ID).State)
	s.Require().NoError(s.store.ScheduledTasks.PauseForConfig(s.ctx, "acme", "config-1"),
		"a config with no active task left pauses nothing")
}

func (s *StoreSuite) TestPausingAConfigsTasksLeavesAHeldTaskNoLease() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	s.held("one", time.Now().Add(8*time.Hour))

	s.Require().NoError(s.store.ScheduledTasks.PauseForConfig(s.ctx, "acme", "config-1"))

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Empty(task.LeasedBy)
	s.Nil(task.LeaseUntil, "a task resumed later is claimed at once, not after the hold's eight hours")
}

func (s *StoreSuite) TestASaveOfACopyReadBeforeAPauseIsRefused() {
	s.task("one", nil)
	stale, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	pause, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	pause.State = ScheduledTaskPaused
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &pause))

	stale.Title = "Stretch twice"
	s.ErrorIs(s.store.ScheduledTasks.Update(s.ctx, &stale), ErrScheduledTaskChanged)

	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(ScheduledTaskPaused, task.State)
	s.Equal("Stretch", task.Title)
}

// dueRepeat stores a daily task due a minute ago and returns a copy read before anything else
// writes to it.
func (s *StoreSuite) dueRepeat() ScheduledTask {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt, task.RRule = &due, "FREQ=DAILY" })
	read, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	return read
}

// claimOne claims the one due task.
func (s *StoreSuite) claimOne() ClaimedScheduledTask {
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	return claimed[0]
}

// refusesStale saves a copy read before a worker's write and expects the save refused.
func (s *StoreSuite) refusesStale(stale ScheduledTask) {
	stale.Title = "Stretch twice"
	s.ErrorIs(s.store.ScheduledTasks.Update(s.ctx, &stale), ErrScheduledTaskChanged)
	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal("Stretch", task.Title)
}

func (s *StoreSuite) TestASaveOfACopyReadBeforeAClaimIsRefused() {
	stale := s.dueRepeat()
	s.claimOne()
	s.refusesStale(stale)
}

func (s *StoreSuite) TestASaveOfACopyReadBeforeAHoldIsRefused() {
	s.dueRepeat()
	claim := s.claimOne()
	stale, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	held, err := s.store.ScheduledTasks.Postpone(s.ctx, "acme", "one", claim.Run.ID, claim.LeasedBy, time.Now().Add(2*time.Hour))
	s.Require().NoError(err)
	s.Require().True(held)
	s.refusesStale(stale)
}

func (s *StoreSuite) TestASaveOfACopyReadBeforeItsSessionWasSetIsRefused() {
	stale := s.dueRepeat()
	s.Require().NoError(s.store.ScheduledTasks.SetSession(s.ctx, "acme", "one", "session-2"))
	s.refusesStale(stale)
}

func (s *StoreSuite) TestASaveOfACopyReadBeforeAFinishIsRefused() {
	s.dueRepeat()
	claim := s.claimOne()
	stale, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	next := s.base.Add(24 * time.Hour)
	finished, err := s.store.ScheduledTasks.Finish(s.ctx, "acme", claim.Run.ID, claim.LeasedBy, ScheduledTaskOutcome{
		RunState: ScheduledTaskRunDelivered, DeliveredTo: ScheduledTaskDeliveredConversation,
		NextRunAt: &next, State: ScheduledTaskActive,
	})
	s.Require().NoError(err)
	s.Require().True(finished)
	s.refusesStale(stale)
}

func (s *StoreSuite) TestASavedCopyCanBeSavedAgain() {
	task := s.task("one", nil)
	task.Title = "Stretch twice"
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, task), "the copy a create wrote is the stored one")
	task.Title = "Stretch three times"
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, task), "the copy a save wrote is the stored one")
}

func (s *StoreSuite) TestASaveOfADeletedTaskSaysItIsNotThere() {
	task := s.task("one", nil)
	s.Require().NoError(s.store.ScheduledTasks.Delete(s.ctx, "acme", "one"))

	s.ErrorIs(s.store.ScheduledTasks.Update(s.ctx, task), ErrNoScheduledTask)
}

func (s *StoreSuite) TestCreatesAtOnceStopAtTheActiveLimit() {
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Go(func() {
			errs <- s.store.ScheduledTasks.Insert(s.ctx, &ScheduledTask{
				ID: fmt.Sprintf("task-%d", i), CustomerID: "acme", ConfigID: "config-1", UserID: "sam",
				CallerKind: "authenticated", Title: "Stretch", Instructions: "Remind Sam to stretch.",
				StartLocal: s.base, Timezone: "UTC",
			}, 5)
		})
	}
	wg.Wait()
	close(errs)
	created, refused := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			created++
		case errors.Is(err, ErrTooManyScheduledTasks):
			refused++
		default:
			s.Require().NoError(err)
		}
	}
	s.Equal(5, created)
	s.Equal(15, refused)
	active, err := s.store.ScheduledTasks.CountActive(s.ctx, "acme", "sam", "config-1")
	s.Require().NoError(err)
	s.Equal(5, active)
}

func (s *StoreSuite) TestASaveOfACopyReadBeforeARenewalIsKept() {
	s.dueRepeat()
	claim := s.claimOne()
	read, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	renewed, err := s.store.ScheduledTasks.Renew(s.ctx, "acme", "one", claim.LeasedBy, time.Minute)
	s.Require().NoError(err)
	s.Require().True(renewed)
	read.Title = "Stretch twice"
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &read), "a renewal is not a change the edit undoes")
	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal("Stretch twice", task.Title)
}

// limitedConfig stores a config of acme's whose users may hold limit active tasks.
func (s *StoreSuite) limitedConfig(limit int) AgentConfig {
	config := &AgentConfig{CustomerID: "acme", Name: "agent-" + newID(), Scheduling: AgentScheduling{MaxTasksPerUser: limit}}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))
	return *config
}

func (s *StoreSuite) TestResumesAtOnceStopAtTheConfigsActiveLimit() {
	config := s.limitedConfig(5)
	for i := range 20 {
		s.task(fmt.Sprintf("task-%d", i), func(task *ScheduledTask) { task.ConfigID, task.State = config.ID, ScheduledTaskPaused })
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		wg.Go(func() {
			task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", fmt.Sprintf("task-%d", i))
			if err != nil {
				errs <- err
				return
			}
			task.State = ScheduledTaskActive
			errs <- s.store.ScheduledTasks.Update(s.ctx, &task)
		})
	}
	wg.Wait()
	close(errs)
	resumed, refused := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			resumed++
		case errors.Is(err, ErrTooManyScheduledTasks):
			refused++
		default:
			s.Require().NoError(err)
		}
	}
	s.Equal(5, resumed)
	s.Equal(15, refused)
	active, err := s.store.ScheduledTasks.CountActive(s.ctx, "acme", "sam", config.ID)
	s.Require().NoError(err)
	s.Equal(5, active)
}

func (s *StoreSuite) TestACreateAndAResumeAtOnceStopAtTheActiveLimit() {
	for round := range 40 {
		config := s.limitedConfig(1)
		user := fmt.Sprintf("user-%d", round)
		paused := s.task(fmt.Sprintf("paused-%d", round), func(task *ScheduledTask) {
			task.ConfigID, task.UserID, task.State = config.ID, user, ScheduledTaskPaused
		})
		resumed := *paused
		resumed.State = ScheduledTaskActive
		created := &ScheduledTask{ID: fmt.Sprintf("new-%d", round), CustomerID: "acme", ConfigID: config.ID, UserID: user,
			CallerKind: "authenticated", Title: "Stretch", Instructions: "Remind Sam to stretch.",
			StartLocal: s.base, Timezone: "UTC"}
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, 2)
		wg.Go(func() { <-start; errs <- s.store.ScheduledTasks.Insert(s.ctx, created, 1) })
		wg.Go(func() { <-start; errs <- s.store.ScheduledTasks.Update(s.ctx, &resumed) })
		close(start)
		wg.Wait()
		close(errs)
		won, refused := 0, 0
		for err := range errs {
			switch {
			case err == nil:
				won++
			case errors.Is(err, ErrTooManyScheduledTasks):
				refused++
			default:
				s.Require().NoError(err, "round %d", round)
			}
		}
		s.Require().Equal(1, won, "round %d", round)
		s.Require().Equal(1, refused, "round %d", round)
		active, err := s.store.ScheduledTasks.CountActive(s.ctx, "acme", user, config.ID)
		s.Require().NoError(err)
		s.Require().Equal(1, active, "round %d", round)
	}
}

func (s *StoreSuite) TestAFailedTaskResumedAtTheLimitIsRefused() {
	config := s.limitedConfig(1)
	s.task("one", func(task *ScheduledTask) { task.ConfigID = config.ID })
	failed := s.task("two", func(task *ScheduledTask) { task.ConfigID, task.State = config.ID, ScheduledTaskFailed })
	failed.State = ScheduledTaskActive
	s.ErrorIs(s.store.ScheduledTasks.Update(s.ctx, failed), ErrTooManyScheduledTasks)
}

func (s *StoreSuite) TestAnEditOfAnActiveTaskAtTheLimitIsSaved() {
	config := s.limitedConfig(1)
	task := s.task("one", func(task *ScheduledTask) { task.ConfigID = config.ID })
	task.Title = "Stretch twice"
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, task), "the task is counted already, not one more")
	paused := s.task("two", func(task *ScheduledTask) { task.ConfigID, task.State = config.ID, ScheduledTaskPaused })
	paused.Title = "Walk"
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, paused), "a task left paused takes no room")
	paused.State = ScheduledTaskActive
	s.ErrorIs(s.store.ScheduledTasks.Update(s.ctx, paused), ErrTooManyScheduledTasks)
}

// sleptThrough stores a paused one-shot whose time passed, and the missed run a resume records.
func (s *StoreSuite) sleptThrough() (ScheduledTask, *ScheduledTaskRun) {
	due := s.base.Add(-time.Hour)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt, task.State = &due, ScheduledTaskPaused })
	read, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	read.State, read.NextRunAt = ScheduledTaskCompleted, nil
	reason, finished := scheduledTaskRunPaused, time.Now().UTC()
	return read, &ScheduledTaskRun{TaskID: "one", CustomerID: "acme", ScheduledFor: due,
		State: ScheduledTaskRunMissed, Error: &reason, FinishedAt: &finished}
}

func (s *StoreSuite) TestClosingATaskRecordsTheOccurrenceItSleptThrough() {
	task, missed := s.sleptThrough()
	s.Require().NoError(s.store.ScheduledTasks.Close(s.ctx, &task, missed))

	runs, err := s.store.ScheduledTaskRuns.Select(s.ctx, "acme", ScheduledTaskRunFilter{TaskID: "one"})
	s.Require().NoError(err)
	s.Require().Len(runs, 1)
	s.Equal(ScheduledTaskRunMissed, runs[0].State)
	closed, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(ScheduledTaskCompleted, closed.State)
}

func (s *StoreSuite) TestAClosingRefusedAsChangedRecordsNoRun() {
	stale, missed := s.sleptThrough()
	edit, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	edit.Title = "Stretch twice"
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &edit))

	s.ErrorIs(s.store.ScheduledTasks.Close(s.ctx, &stale, missed), ErrScheduledTaskChanged)
	runs, err := s.store.ScheduledTaskRuns.Select(s.ctx, "acme", ScheduledTaskRunFilter{TaskID: "one"})
	s.Require().NoError(err)
	s.Empty(runs, "the refused resume left no run behind")
}

func (s *StoreSuite) TestATaskOnADeletedConfigIsRefused() {
	deleted := s.boundConfig("acme")
	s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, "acme", deleted.ID))

	err := s.store.ScheduledTasks.Insert(s.ctx, &ScheduledTask{
		CustomerID: "acme", ConfigID: deleted.ID, UserID: "sam", CallerKind: "authenticated",
		Title: "Stretch", Instructions: "Remind Sam to stretch.", StartLocal: s.base, Timezone: "UTC",
	}, 25)
	s.ErrorIs(err, ErrNoAgentConfig)
	tasks, err := s.store.ScheduledTasks.Select(s.ctx, "acme", ScheduledTaskFilter{Limit: 25})
	s.Require().NoError(err)
	s.Empty(tasks)

	live := s.boundConfig("acme")
	s.task("live", func(task *ScheduledTask) { task.ConfigID = live.ID })
	s.task("theirs", func(task *ScheduledTask) { task.CustomerID, task.ConfigID = "globex", deleted.ID })
}

func (s *StoreSuite) TestACreateWaitsForAConfigDeleteUnderWayAndIsRefused() {
	config := s.boundConfig("acme")
	deleting, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer func() { _ = deleting.Rollback() }()
	_, err = deleting.ExecContext(s.ctx, "UPDATE agent_configs SET deleted_at = now() WHERE id = ?", config.ID)
	s.Require().NoError(err)

	created := make(chan error, 1)
	go func() {
		created <- s.store.ScheduledTasks.Insert(s.ctx, &ScheduledTask{
			CustomerID: "acme", ConfigID: config.ID, UserID: "sam", CallerKind: "authenticated",
			Title: "Stretch", Instructions: "Remind Sam to stretch.", StartLocal: s.base, Timezone: "UTC",
		}, 25)
	}()
	// The suite's database is its own, so a backend waiting on a lock in it is the create.
	s.Require().Eventually(func() bool {
		var waiting int
		s.Require().NoError(s.store.global.QueryRowContext(s.ctx, "SELECT count(*) FROM pg_stat_activity"+
			" WHERE datname = current_database() AND wait_event_type = 'Lock'").Scan(&waiting))
		return waiting > 0
	}, 5*time.Second, 10*time.Millisecond, "the create read the config without waiting for the delete under way")
	s.Require().NoError(deleting.Commit())

	s.ErrorIs(<-created, ErrNoAgentConfig)
}

func (s *StoreSuite) TestATaskOfADeletedConfigIsNotMadeActiveAgain() {
	config := s.boundConfig("acme")
	s.task("one", func(task *ScheduledTask) { task.ConfigID, task.State = config.ID, ScheduledTaskPaused })
	s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, "acme", config.ID))
	stored, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)

	stored.Title = "Renamed"
	s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, &stored), "a paused task's edit stands")
	stored.State = ScheduledTaskActive
	s.ErrorIs(s.store.ScheduledTasks.Update(s.ctx, &stored), ErrNoAgentConfig)

	after, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(ScheduledTaskPaused, after.State)
	s.Equal("Renamed", after.Title)
}

func (s *StoreSuite) TestAClaimInFlightCannotResumeADeletedConfigsTask() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	s.Require().NoError(s.store.ScheduledTasks.PauseForConfig(s.ctx, "acme", "config-1"))

	next := s.base.Add(24 * time.Hour)
	applied, err := s.store.ScheduledTasks.Finish(s.ctx, "acme", claimed[0].Run.ID, claimed[0].LeasedBy, ScheduledTaskOutcome{
		RunState: ScheduledTaskRunDelivered, DeliveredTo: ScheduledTaskDeliveredConversation,
		NextRunAt: &next, State: ScheduledTaskActive,
	})
	s.Require().NoError(err)
	s.False(applied, "the pause fenced the claim")
	after, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(ScheduledTaskPaused, after.State)
}

// holdRun claims the due task one and locks its run's row in a transaction
// left open, so a write of that run waits until release is called.
func (s *StoreSuite) holdRun() (ClaimedScheduledTask, func()) {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	holder, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	_, err = holder.ExecContext(s.ctx, "SELECT id FROM scheduled_task_runs WHERE id = ? FOR UPDATE", claimed[0].Run.ID)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = holder.Rollback() })
	return claimed[0], func() { s.Require().NoError(holder.Rollback()) }
}

func (s *StoreSuite) TestAConfigDeleteWhileAWorkerFinishesTheRunStillPausesTheTask() {
	claim, release := s.holdRun()
	finished := make(chan error, 1)
	go func() {
		next := s.base.Add(24 * time.Hour)
		_, err := s.store.ScheduledTasks.Finish(s.ctx, "acme", claim.Run.ID, claim.LeasedBy, ScheduledTaskOutcome{
			RunState: ScheduledTaskRunDelivered, DeliveredTo: ScheduledTaskDeliveredConversation,
			NextRunAt: &next, State: ScheduledTaskActive,
		})
		finished <- err
	}()
	s.waitForLockWaiters(1, "the write of the held run waits")
	paused := make(chan error, 1)
	go func() { paused <- s.store.ScheduledTasks.PauseForConfig(s.ctx, "acme", "config-1") }()
	s.waitForLockWaiters(2, "the pause waits too")
	release()

	s.Require().NoError(<-finished)
	s.Require().NoError(<-paused, "the pause is not aborted as a deadlock with the finish")
	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(ScheduledTaskPaused, task.State, "the deleted config's task is not left active")
	s.Equal(ScheduledTaskRunDelivered, s.run(claim.Run.ID).State, "the finish recorded the delivery")
}

func (s *StoreSuite) TestAConfigDeleteWhileATaskIsPausedByHandStillPausesTheTask() {
	claim, release := s.holdRun()
	task, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	task.State = ScheduledTaskPaused
	saved := make(chan error, 1)
	go func() { saved <- s.store.ScheduledTasks.Update(s.ctx, &task) }()
	s.waitForLockWaiters(1, "the write of the held run waits")
	paused := make(chan error, 1)
	go func() { paused <- s.store.ScheduledTasks.PauseForConfig(s.ctx, "acme", "config-1") }()
	s.waitForLockWaiters(2, "the pause waits too")
	release()

	s.Require().NoError(<-saved)
	s.Require().NoError(<-paused, "the pause is not aborted as a deadlock with the save")
	after, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "one")
	s.Require().NoError(err)
	s.Equal(ScheduledTaskPaused, after.State)
	s.Equal(ScheduledTaskRunMissed, s.run(claim.Run.ID).State, "the pause by hand settled the occurrence")
}

func (s *StoreSuite) TestAnUnchangedUserTimezoneIsNotWrittenAgain() {
	s.Require().NoError(s.store.Users.SetTimezone(s.ctx, "acme", "sam", "authenticated", "Europe/Amsterdam"))
	written := s.userRowVersion("sam")

	s.Require().NoError(s.store.Users.SetTimezone(s.ctx, "acme", "sam", "authenticated", "Europe/Amsterdam"))
	s.Equal(written, s.userRowVersion("sam"), "the same zone again leaves the row alone")

	s.Require().NoError(s.store.Users.SetTimezone(s.ctx, "acme", "sam", "authenticated", "Asia/Tokyo"))
	s.NotEqual(written, s.userRowVersion("sam"))
	zone, err := s.store.Users.GetTimezone(s.ctx, "acme", "sam")
	s.Require().NoError(err)
	s.Equal("Asia/Tokyo", zone)
}

// userRowVersion is the transaction that last wrote acme's user's row, which moves on
// every write to it, even one that sets a column to what it held.
func (s *StoreSuite) userRowVersion(userID string) string {
	var version string
	s.Require().NoError(s.store.global.QueryRowContext(s.ctx,
		"SELECT xmin::text FROM users WHERE customer_id = ? AND id = ?", "acme", userID).Scan(&version))
	return version
}

func (s *StoreSuite) TestAUserWithNoZoneYetIsGivenOne() {
	// A row written before zones were kept holds none, not an empty one.
	_, err := s.store.global.ExecContext(s.ctx,
		"INSERT INTO users (customer_id, id, kind, name, timezone) VALUES ('acme', 'sam', 'authenticated', '', NULL)")
	s.Require().NoError(err)

	s.Require().NoError(s.store.Users.SetTimezone(s.ctx, "acme", "sam", "authenticated", "Europe/Amsterdam"))

	zone, err := s.store.Users.GetTimezone(s.ctx, "acme", "sam")
	s.Require().NoError(err)
	s.Equal("Europe/Amsterdam", zone)
}

func (s *StoreSuite) TestScheduledTaskQueryBudgets() {
	due := s.base.Add(-time.Minute)
	for _, id := range []string{"st-1", "st-2", "st-3"} {
		s.task(id, func(task *ScheduledTask) { task.NextRunAt = &due; task.OriginSessionID = "session-1" })
	}
	added := &ScheduledTask{ID: "st-new", CustomerID: "acme", ConfigID: "config-1", UserID: "sam", CallerKind: "authenticated",
		Title: "Stretch", Instructions: "Remind Sam to stretch.", StartLocal: s.base, Timezone: "Europe/Amsterdam",
		State: ScheduledTaskActive, CreatedBy: "api"}

	// The transaction, the config locked, the limit locked, the user's active tasks counted and the insert.
	s.Equal(6, s.countQueries(func() { s.Require().NoError(s.store.ScheduledTasks.Insert(s.ctx, added, 25)) }), "Insert")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ScheduledTasks.Get(s.ctx, "acme", "st-new")
		s.Require().NoError(err)
	}), "Get")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.ScheduledTasks.Select(s.ctx, "acme", ScheduledTaskFilter{Limit: 2})
		s.Require().NoError(err)
		s.Len(listed, 3)
	}), "Select")
	added.Title = "Stretch twice"
	// The transaction, the config and the task locked, its running occurrence settled and the save.
	s.Equal(6, s.countQueries(func() { s.Require().NoError(s.store.ScheduledTasks.Update(s.ctx, added)) }), "Update")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ScheduledTasks.SetSession(s.ctx, "acme", "st-new", "session-2"))
	}), "SetSession")
	s.Equal(1, s.countQueries(func() {
		count, err := s.store.ScheduledTasks.CountActive(s.ctx, "acme", "sam", "config-1")
		s.Require().NoError(err)
		s.Equal(4, count)
	}), "CountActive")
	s.Equal(1, s.countQueries(func() {
		_, _, err := s.store.ScheduledTasks.NextDue(s.ctx)
		s.Require().NoError(err)
	}), "NextDue")

	// The transaction, the clock, the due tasks, their runs, the new runs and the leases: the same
	// however many tasks came due.
	var claimed []ClaimedScheduledTask
	s.Equal(7, s.countQueries(func() {
		var err error
		claimed, err = s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
		s.Require().NoError(err)
		s.Len(claimed, 3)
	}), "Claim")
	first, second := claimed[0], claimed[1]
	s.Equal(1, s.countQueries(func() {
		held, err := s.store.ScheduledTasks.Renew(s.ctx, "acme", first.Task.ID, first.LeasedBy, time.Minute)
		s.Require().NoError(err)
		s.True(held)
	}), "Renew")
	// The transaction, the task and its run.
	s.Equal(4, s.countQueries(func() {
		held, err := s.store.ScheduledTasks.Postpone(s.ctx, "acme", second.Task.ID, second.Run.ID, second.LeasedBy, time.Now().Add(time.Hour))
		s.Require().NoError(err)
		s.True(held)
	}), "Postpone")
	next := time.Now().Add(24 * time.Hour)
	// The transaction, the run read, its task locked, the run and the task written.
	s.Equal(6, s.countQueries(func() {
		applied, err := s.store.ScheduledTasks.Finish(s.ctx, "acme", first.Run.ID, first.LeasedBy, ScheduledTaskOutcome{
			RunState: ScheduledTaskRunDelivered, DeliveredTo: "conversation", NextRunAt: &next, State: ScheduledTaskActive,
		})
		s.Require().NoError(err)
		s.True(applied)
	}), "Finish")
	// The transaction, the tasks and their runs.
	s.Equal(4, s.countQueries(func() {
		s.Require().NoError(s.store.ScheduledTasks.PauseForConfig(s.ctx, "acme", "config-1"))
	}), "PauseForConfig")
	// The transaction, the tasks and their runs locked, and the delete with its changes redacted.
	s.Equal(5, s.countQueries(func() {
		s.Require().NoError(s.store.ScheduledTasks.DeleteForSession(s.ctx, "acme", "session-1"))
	}), "DeleteForSession")
	// The transaction and the lock, which finds none.
	s.Equal(3, s.countQueries(func() {
		s.Require().NoError(s.store.ScheduledTasks.DeleteForSession(s.ctx, "acme", "session-none"))
	}), "DeleteForSession of a session with no tasks")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ScheduledTasks.Delete(s.ctx, "acme", "st-new")) }), "Delete")
	// The lost runs by hand settled, then one batch of runs and one of tasks, with nothing to prune.
	s.Equal(3, s.countQueries(func() {
		_, _, err := s.store.ScheduledTasks.Prune(s.ctx)
		s.Require().NoError(err)
	}), "Prune")
}
