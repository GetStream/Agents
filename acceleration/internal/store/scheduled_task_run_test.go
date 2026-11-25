//go:build integration

package store

import "time"

func (s *StoreSuite) TestARunTakesOneCallUntilItIsGivenBack() {
	s.task("one", nil)
	run := ScheduledTaskRun{
		TaskID: "one", CustomerID: "acme", ScheduledFor: s.base, Trigger: ScheduledTaskTriggerManual,
		Prompt: "Remind Sam to stretch.", State: ScheduledTaskRunRunning, Attempts: 1, StartedAt: &s.base,
	}
	s.Require().NoError(s.store.ScheduledTaskRuns.Insert(s.ctx, &run))
	take := func(customer, call string) bool {
		took, _, err := s.store.ScheduledTaskRuns.TakeCall(s.ctx, customer, run.ID, call)
		s.Require().NoError(err)
		return took
	}

	s.False(take("globex", "call-0"), "another customer's run is not taken")
	s.True(take("acme", "call-1"))
	s.False(take("acme", "call-2"), "a run that placed a call places no second one")

	s.Require().NoError(s.store.ScheduledTaskRuns.ReleaseCall(s.ctx, "acme", run.ID, "call-2"))
	s.Require().NoError(s.store.ScheduledTaskRuns.ReleaseCall(s.ctx, "globex", run.ID, "call-1"))
	s.False(take("acme", "call-3"), "only the call taken, on the customer's own run, is given back")

	s.Require().NoError(s.store.ScheduledTaskRuns.ReleaseCall(s.ctx, "acme", run.ID, "call-1"))
	s.True(take("acme", "call-3"), "a call given back may be placed again")

	s.Require().NoError(s.store.ScheduledTaskRuns.FinishManual(s.ctx, run.CustomerID, run.ID, ScheduledTaskRunFailed, "", "call-3", "not_answered"))
	s.Require().NoError(s.store.ScheduledTaskRuns.ReleaseCall(s.ctx, "acme", run.ID, "call-3"))
	s.Require().NoError(s.store.ScheduledTaskRuns.SetCallReason(s.ctx, "acme", run.ID, "call-3", "delivery_failed"))
	runs, err := s.store.ScheduledTaskRuns.Select(s.ctx, "acme", ScheduledTaskRunFilter{TaskID: "one", Limit: 25})
	s.Require().NoError(err)
	s.Require().Len(runs, 1)
	s.Equal("call-3", runs[0].SessionID, "a finished run keeps the call it made")
	s.Equal("not_answered", *runs[0].Error, "and its outcome: a call ending late writes no reason over it")

	s.Require().NoError(s.store.ScheduledTaskRuns.FinishManual(s.ctx, run.CustomerID, run.ID, ScheduledTaskRunFailed, "", "", "delivery_failed"))
	s.False(take("acme", "call-4"), "a finished run places no call")
}

func (s *StoreSuite) TestARunTakesEachTextChannelOnceUntilItIsGivenBack() {
	s.task("one", nil)
	run := ScheduledTaskRun{
		TaskID: "one", CustomerID: "acme", ScheduledFor: s.base, Trigger: ScheduledTaskTriggerManual,
		Prompt: "Remind Sam to stretch.", State: ScheduledTaskRunRunning, Attempts: 1, StartedAt: &s.base,
	}
	s.Require().NoError(s.store.ScheduledTaskRuns.Insert(s.ctx, &run))
	take := func(customer, channel string) bool {
		took, err := s.store.ScheduledTaskRuns.TakeText(s.ctx, customer, run.ID, channel)
		s.Require().NoError(err)
		return took
	}

	s.False(take("globex", "sms"), "another customer's run is not taken")
	s.True(take("acme", "sms"))
	s.False(take("acme", "sms"), "a run that texted on a channel texts there no second time")
	s.True(take("acme", "whatsapp"), "each channel is taken on its own")

	s.Require().NoError(s.store.ScheduledTaskRuns.ReleaseText(s.ctx, "globex", run.ID, "sms"))
	s.False(take("acme", "sms"), "only the customer's own run is given back")

	s.Require().NoError(s.store.ScheduledTaskRuns.ReleaseText(s.ctx, "acme", run.ID, "sms"))
	s.True(take("acme", "sms"), "a text given back may be sent again")
	s.False(take("acme", "whatsapp"), "giving one channel back leaves the other taken")
}

func (s *StoreSuite) TestAnotherCustomersFinishOfARunIdChangesNothing() {
	s.task("one", nil)
	run := ScheduledTaskRun{
		TaskID: "one", CustomerID: "acme", ScheduledFor: s.base, Trigger: ScheduledTaskTriggerManual,
		Prompt: "Remind Sam to stretch.", State: ScheduledTaskRunRunning, Attempts: 1, StartedAt: &s.base,
	}
	s.Require().NoError(s.store.ScheduledTaskRuns.Insert(s.ctx, &run))
	before, err := s.store.ScheduledTaskRuns.Get(s.ctx, "acme", "one", run.ID)
	s.Require().NoError(err)

	s.Require().NoError(s.store.ScheduledTaskRuns.FinishManual(s.ctx, "globex", run.ID, ScheduledTaskRunFailed, "", "their-session", "delivery_failed"))

	after, err := s.store.ScheduledTaskRuns.Get(s.ctx, "acme", "one", run.ID)
	s.Require().NoError(err)
	s.Equal(before, after, "acme's run is still running")
}

func (s *StoreSuite) TestARunsCallKeepsTheReasonItEndedUnderForTheWorkerTakingItOver() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute, ShardClaim{})
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	run := claimed[0].Run
	took, _, err := s.store.ScheduledTaskRuns.TakeCall(s.ctx, "acme", run.ID, "call-1")
	s.Require().NoError(err)
	s.Require().True(took)
	s.Require().NoError(s.store.ScheduledTaskRuns.SetCallReason(s.ctx, "acme", run.ID, "call-1", "not_answered"))
	// Only the call the run took, on the customer's own run, records how it ended.
	s.Require().NoError(s.store.ScheduledTaskRuns.SetCallReason(s.ctx, "acme", run.ID, "call-2", "delivery_failed"))
	s.Require().NoError(s.store.ScheduledTaskRuns.SetCallReason(s.ctx, "globex", run.ID, "call-1", "delivery_failed"))
	took, reason, err := s.store.ScheduledTaskRuns.TakeCall(s.ctx, "globex", run.ID, "call-3")
	s.Require().NoError(err)
	s.False(took)
	s.Empty(reason, "another customer's run tells nothing")

	// The worker dies: its run ages past the stale window and its lease lapses.
	_, err = s.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("started_at = ?", s.base.Add(-time.Hour)).
		Where("id = ?", run.ID).Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = ?", s.base.Add(-time.Minute)).
		Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)
	reclaimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute, ShardClaim{})
	s.Require().NoError(err)
	s.Require().Len(reclaimed, 1)

	took, reason, err = s.store.ScheduledTaskRuns.TakeCall(s.ctx, "acme", run.ID, "call-4")
	s.Require().NoError(err)
	s.False(took, "the run rang already")
	s.Equal("not_answered", reason, "the taking over keeps how the call went")
}

// staleRun ages a claimed run past the stale window and lapses the lease of acme's task, as a worker that
// died leaves them, and returns the claim that takes the run over.
func (s *StoreSuite) staleRun(taskID, runID string) ClaimedScheduledTask {
	_, err := s.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("started_at = ?", s.base.Add(-time.Hour)).
		Where("id = ?", runID).Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = ?", s.base.Add(-time.Minute)).
		Where("customer_id = ?", "acme").
		Where("id = ?", taskID).Exec(s.ctx)
	s.Require().NoError(err)
	reclaimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(reclaimed, 1)
	s.Require().Equal(runID, reclaimed[0].Run.ID, "taken over under the same run")
	return reclaimed[0]
}

func (s *StoreSuite) TestAnAttemptsReasonIsReadByTheWorkerTakingTheRunOver() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	s.task("two", func(task *ScheduledTask) { task.NextRunAt = &due })
	s.task("one", func(task *ScheduledTask) { task.CustomerID, task.NextRunAt = "globex", &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 3)
	claims := map[string]ClaimedScheduledTask{}
	for _, claim := range claimed {
		claims[claim.Task.CustomerID+"/"+claim.Task.ID] = claim
	}
	mine := claims["acme/one"]
	run := mine.Run
	s.Nil(run.Error, "a new run has no reason")
	// Only the claim that holds the run's own task records a reason: not a lost claim, another
	// task's, or another customer's task of the same id.
	for _, leasedBy := range []string{"a-lost-claim", claims["acme/two"].LeasedBy, claims["globex/one"].LeasedBy} {
		s.Require().NoError(s.store.ScheduledTaskRuns.SetAttemptReason(s.ctx, "acme", run.ID, leasedBy, "user_on_call"))
	}
	s.Require().NoError(s.store.ScheduledTaskRuns.SetAttemptReason(s.ctx, "globex", run.ID, mine.LeasedBy, "user_on_call"))
	s.Nil(s.run(run.ID).Error)

	s.Require().NoError(s.store.ScheduledTaskRuns.SetAttemptReason(s.ctx, "acme", run.ID, mine.LeasedBy, "spend_check_unavailable"))
	reclaimed := s.staleRun("one", run.ID)

	s.Require().NotNil(reclaimed.Run.Error)
	s.Equal("spend_check_unavailable", *reclaimed.Run.Error, "the worker taking over reads why the last attempt failed")
	s.Require().NoError(s.store.ScheduledTaskRuns.SetAttemptReason(s.ctx, "acme", run.ID, mine.LeasedBy, "user_on_call"))
	s.Equal("spend_check_unavailable", *s.run(run.ID).Error, "the claim taken over records nothing more")
}

func (s *StoreSuite) TestAnAttemptsReasonLeavesTheReasonAPlacedCallEndedUnder() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	run := claimed[0].Run
	took, _, err := s.store.ScheduledTaskRuns.TakeCall(s.ctx, "acme", run.ID, "call-1")
	s.Require().NoError(err)
	s.Require().True(took)
	s.Require().NoError(s.store.ScheduledTaskRuns.SetCallReason(s.ctx, "acme", run.ID, "call-1", "not_answered"))

	s.Require().NoError(s.store.ScheduledTaskRuns.SetAttemptReason(s.ctx, "acme", run.ID, claimed[0].LeasedBy, "channel_unreachable"))

	took, reason, err := s.store.ScheduledTaskRuns.TakeCall(s.ctx, "acme", run.ID, "call-2")
	s.Require().NoError(err)
	s.False(took)
	s.Equal("not_answered", reason, "how the call ended is the run's reason")
}

// callTakenAfterAnAttemptFailed claims a due task, records an earlier attempt's reason on its run
// and takes a call for it, as a worker does on the attempt that rings after one that failed.
func (s *StoreSuite) callTakenAfterAnAttemptFailed() ClaimedScheduledTask {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Require().NoError(s.store.ScheduledTaskRuns.SetAttemptReason(s.ctx, "acme", claimed[0].Run.ID, claimed[0].LeasedBy, "tier_limit_reached"))
	took, _, err := s.store.ScheduledTaskRuns.TakeCall(s.ctx, "acme", claimed[0].Run.ID, "call-1")
	s.Require().NoError(err)
	s.Require().True(took)
	return claimed[0]
}

func (s *StoreSuite) TestAnEarlierAttemptsReasonIsNotTheReasonOfTheCallTakenAfterIt() {
	claimed := s.callTakenAfterAnAttemptFailed()

	// The worker stops mid-call, before it records how the call ended.
	reclaimed := s.staleRun("one", claimed.Run.ID)

	s.Nil(reclaimed.Run.Error, "the call is the run's, the earlier attempt's reason is not")
	took, reason, err := s.store.ScheduledTaskRuns.TakeCall(s.ctx, "acme", claimed.Run.ID, "call-2")
	s.Require().NoError(err)
	s.False(took, "the user is not rung again")
	s.Empty(reason, "so the delivery fails as already called, not under the earlier attempt's reason")
}

func (s *StoreSuite) TestARunTakenOverMidCallAfterAFailedAttemptEndsAsAlreadyCalled() {
	claimed := s.callTakenAfterAnAttemptFailed()
	_, err := s.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("attempts = ?", maxScheduledTaskRunAttempts).
		Set("started_at = ?", s.base.Add(-time.Hour)).
		Where("id = ?", claimed.Run.ID).Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.store.global.NewUpdate().Model((*ScheduledTask)(nil)).
		Set("lease_until = ?", s.base.Add(-time.Minute)).
		Where("customer_id = ?", "acme").
		Where("id = ?", "one").Exec(s.ctx)
	s.Require().NoError(err)

	_, err = s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)

	run := s.run(claimed.Run.ID)
	s.Equal(ScheduledTaskRunFailed, run.State)
	s.Require().NotNil(run.Error)
	s.Equal(scheduledTaskRunRungAlready, *run.Error)
}

func (s *StoreSuite) TestAFinishedRunTakesNoAttemptsReason() {
	due := s.base.Add(-time.Minute)
	s.task("one", func(task *ScheduledTask) { task.NextRunAt = &due })
	claimed, err := s.store.ScheduledTasks.Claim(s.ctx, 16, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	run := claimed[0].Run
	_, err = s.store.global.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("state = ?", ScheduledTaskRunDelivered).
		Where("id = ?", run.ID).Exec(s.ctx)
	s.Require().NoError(err)

	s.Require().NoError(s.store.ScheduledTaskRuns.SetAttemptReason(s.ctx, "acme", run.ID, claimed[0].LeasedBy, "user_on_call"))

	s.Nil(s.run(run.ID).Error, "a delivered run says nothing failed")
}

func (s *StoreSuite) TestTheLastRunOfEveryTaskIsReadAtOnce() {
	for _, id := range []string{"one", "two", "three", "four"} {
		s.task(id, nil)
	}
	s.Require().NoError(s.store.ScheduledTasks.Insert(s.ctx, &ScheduledTask{
		ID: "one", CustomerID: "globex", UserID: "sam", Title: "Theirs", Instructions: "Again.",
		StartLocal: s.base, Timezone: "UTC",
	}, 0))
	finished := func(customer, task string, at time.Time) {
		run := &ScheduledTaskRun{TaskID: task, CustomerID: customer, ScheduledFor: at,
			State: ScheduledTaskRunDelivered, FinishedAt: &at}
		s.Require().NoError(s.store.ScheduledTaskRuns.Insert(s.ctx, run))
	}
	finished("acme", "one", s.base.Add(-2*time.Hour))
	finished("acme", "one", s.base.Add(-time.Hour))
	finished("globex", "one", s.base)
	finished("acme", "four", s.base)
	// Two's only run is still going.
	s.Require().NoError(s.store.ScheduledTaskRuns.Insert(s.ctx, &ScheduledTaskRun{TaskID: "two", CustomerID: "acme", ScheduledFor: s.base}))

	last, err := s.store.ScheduledTaskRuns.LastFinishedAt(s.ctx, "acme", []string{"one", "two", "three"})
	s.Require().NoError(err)
	s.Equal(map[string]time.Time{"one": s.base.Add(-time.Hour)}, last,
		"the newest finished run of each task asked for, of this customer only")

	none, err := s.store.ScheduledTaskRuns.LastFinishedAt(s.ctx, "acme", nil)
	s.Require().NoError(err)
	s.Empty(none)
}

func (s *StoreSuite) TestScheduledTaskRunQueryBudgets() {
	task := s.task("st-1", nil)
	run := &ScheduledTaskRun{CustomerID: "acme", TaskID: task.ID, Trigger: ScheduledTaskTriggerManual, State: ScheduledTaskRunRunning,
		ScheduledFor: s.base, Prompt: task.Instructions}

	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ScheduledTaskRuns.Insert(s.ctx, run)) }), "Insert")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ScheduledTaskRuns.Get(s.ctx, "acme", task.ID, run.ID)
		s.Require().NoError(err)
	}), "Get")
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.ScheduledTaskRuns.Select(s.ctx, "acme", ScheduledTaskRunFilter{TaskID: task.ID})
		s.Require().NoError(err)
		s.Len(listed, 1)
	}), "Select")
	s.Equal(1, s.countQueries(func() {
		took, _, err := s.store.ScheduledTaskRuns.TakeCall(s.ctx, "acme", run.ID, "call-1")
		s.Require().NoError(err)
		s.True(took)
	}), "TakeCall")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ScheduledTaskRuns.SetCallReason(s.ctx, "acme", run.ID, "call-1", "no_answer"))
	}), "SetCallReason")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ScheduledTaskRuns.ReleaseCall(s.ctx, "acme", run.ID, "call-1"))
	}), "ReleaseCall")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ScheduledTaskRuns.SetAttemptReason(s.ctx, "acme", run.ID, "claim-1", "user_on_call"))
	}), "SetAttemptReason")
	s.Equal(1, s.countQueries(func() {
		took, err := s.store.ScheduledTaskRuns.TakeText(s.ctx, "acme", run.ID, "sms")
		s.Require().NoError(err)
		s.True(took)
	}), "TakeText")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ScheduledTaskRuns.ReleaseText(s.ctx, "acme", run.ID, "sms"))
	}), "ReleaseText")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ScheduledTaskRuns.FinishManual(s.ctx, run.CustomerID, run.ID, ScheduledTaskRunDelivered, "conversation", "session-1", ""))
	}), "FinishManual")
	s.Equal(1, s.countQueries(func() {
		finished, err := s.store.ScheduledTaskRuns.LastFinishedAt(s.ctx, "acme", []string{task.ID})
		s.Require().NoError(err)
		s.Contains(finished, task.ID)
	}), "LastFinishedAt")
}
