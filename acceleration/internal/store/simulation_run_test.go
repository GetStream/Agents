//go:build integration

package store

import (
	"time"
)

func (s *StoreSuite) TestAnotherCustomersFinishOfASimulationRunIdChangesNothing() {
	run := s.startedRun("app", time.Minute)
	before, err := s.store.SimulationRuns.Get(s.ctx, "app", run.ID)
	s.Require().NoError(err)

	s.Require().NoError(s.store.SimulationRuns.Finish(s.ctx, SimulationRun{ID: run.ID, CustomerID: "somebody-else", State: SimulationPassed, Passed: 3}))

	after, err := s.store.SimulationRuns.Get(s.ctx, "app", run.ID)
	s.Require().NoError(err)
	s.Equal(before, after, "app's run is still running")
}

func (s *StoreSuite) TestARunItsRouterStillRenewsIsNotWrittenOff() {
	// However long ago it started: a second router booting must not abandon what the first
	// is still having.
	run := s.startedRun("app", time.Minute)
	s.Require().NotNil(run.LeaseUntil)
	s.InDelta(time.Minute, s.leftOnTheDatabasesClock(*run.LeaseUntil), float64(time.Second), "a minute on Postgres's clock")
	s.leasesPass("simulation_runs", 55*time.Second)

	s.Require().NoError(s.store.SimulationRuns.Abandon(s.ctx, time.Now()))

	found, err := s.store.SimulationRuns.Get(s.ctx, "app", run.ID)
	s.Require().NoError(err)
	s.Equal(SimulationRunning, found.State)
}

func (s *StoreSuite) TestARunWhoseRouterStoppedRenewingIsWrittenOff() {
	run := s.startedRun("app", time.Minute)
	s.leasesPass("simulation_runs", time.Minute+5*time.Second)

	s.Require().NoError(s.store.SimulationRuns.Abandon(s.ctx, time.Now()))

	found, err := s.store.SimulationRuns.Get(s.ctx, "app", run.ID)
	s.Require().NoError(err)
	s.Equal(SimulationErrored, found.State)
	cases, err := s.store.SimulationCases.Select(s.ctx, run.ID)
	s.Require().NoError(err)
	s.Equal(SimulationCancelled, cases[0].State, "nobody is going to have the conversation")
}

func (s *StoreSuite) TestRenewingARunKeepsItFromBeingWrittenOff() {
	run := s.startedRun("app", time.Second)

	stop, err := s.store.SimulationRuns.Renew(s.ctx, []string{run.ID}, time.Hour)
	s.Require().NoError(err)
	s.Empty(stop)
	s.leasesPass("simulation_runs", time.Minute)
	s.Require().NoError(s.store.SimulationRuns.Abandon(s.ctx, time.Now()))

	found, err := s.store.SimulationRuns.Get(s.ctx, "app", run.ID)
	s.Require().NoError(err)
	s.Equal(SimulationRunning, found.State)
	s.Require().NotNil(found.LeaseUntil)
	s.InDelta(59*time.Minute, s.leftOnTheDatabasesClock(*found.LeaseUntil), float64(time.Second), "renewed for an hour on Postgres's clock")
}

func (s *StoreSuite) TestARunAskedToStopIsNotRenewedAndIsToldToStop() {
	run := s.startedRun("app", time.Minute)
	s.Require().NoError(s.store.SimulationRuns.Stop(s.ctx, "app", run.ID))

	stop, err := s.store.SimulationRuns.Renew(s.ctx, []string{run.ID}, time.Hour)
	s.Require().NoError(err)

	s.Equal([]string{run.ID}, stop)
}

func (s *StoreSuite) TestAFinishedRunIsToldToStop() {
	run := s.startedRun("app", time.Minute)
	run.State = SimulationPassed
	s.Require().NoError(s.store.SimulationRuns.Finish(s.ctx, run))

	stop, err := s.store.SimulationRuns.Renew(s.ctx, []string{run.ID}, time.Hour)
	s.Require().NoError(err)

	s.Equal([]string{run.ID}, stop)
}

func (s *StoreSuite) TestAnotherCustomerCannotStopARun() {
	run := s.startedRun("app", time.Minute)
	s.Require().NoError(s.store.SimulationRuns.Stop(s.ctx, "somebody-else", run.ID))

	stop, err := s.store.SimulationRuns.Renew(s.ctx, []string{run.ID}, time.Hour)
	s.Require().NoError(err)

	s.Empty(stop)
}

// startedRun starts a run of one conversation for customerID, with a lease for the time in lease.
func (s *StoreSuite) startedRun(customerID string, lease time.Duration) SimulationRun {
	simulation := s.simulation(customerID)
	run := SimulationRun{CustomerID: customerID, SimulationID: simulation.ID, Mode: SimulationText}
	s.Require().NoError(s.store.SimulationRuns.Start(s.ctx, &run,
		[]SimulationCase{{Scenario: simulation.Scenario}}, lease))
	return run
}

func (s *StoreSuite) TestARunIsOnlyItsOwnCustomers() {
	run := s.startedRun("app", time.Minute)
	s.startedRun("other", time.Minute)

	_, err := s.store.SimulationRuns.Get(s.ctx, "other", run.ID)
	s.ErrorIs(err, ErrNoSimulationRun)
	runs, err := s.store.SimulationRuns.Select(s.ctx, "app", SimulationRunFilter{})
	s.Require().NoError(err)
	s.Require().Len(runs, 1)
	s.Equal(run.ID, runs[0].ID)
}

func (s *StoreSuite) TestRunsAreSelectedBySimulationAndState() {
	first := s.startedRun("app", time.Minute)
	second := s.startedRun("app", time.Minute)
	second.State = SimulationPassed
	s.Require().NoError(s.store.SimulationRuns.Finish(s.ctx, second))

	bySimulation, err := s.store.SimulationRuns.Select(s.ctx, "app", SimulationRunFilter{SimulationID: first.SimulationID})
	s.Require().NoError(err)
	s.Require().Len(bySimulation, 1)
	s.Equal(first.ID, bySimulation[0].ID)
	byState, err := s.store.SimulationRuns.Select(s.ctx, "app", SimulationRunFilter{State: SimulationPassed})
	s.Require().NoError(err)
	s.Require().Len(byState, 1)
	s.Equal(second.ID, byState[0].ID)
	limited, err := s.store.SimulationRuns.Select(s.ctx, "app", SimulationRunFilter{Limit: 1})
	s.Require().NoError(err)
	s.Require().Len(limited, 1)
	s.Equal(second.ID, limited[0].ID, "newest first")
}

func (s *StoreSuite) TestARunWithoutConversationsIsNotStarted() {
	simulation := s.simulation("app")
	run := SimulationRun{CustomerID: "app", SimulationID: simulation.ID, Mode: SimulationText}

	var invalid *ValidationError
	s.Require().ErrorAs(s.store.SimulationRuns.Start(s.ctx, &run, nil, time.Minute), &invalid)
	s.Equal("cases", invalid.Field)
	runless := SimulationRun{CustomerID: "app", Mode: SimulationText}
	s.Require().ErrorAs(s.store.SimulationRuns.Start(s.ctx, &runless, []SimulationCase{{}}, time.Minute), &invalid)
	s.Equal("simulation_id", invalid.Field)

	runs, err := s.store.SimulationRuns.Select(s.ctx, "app", SimulationRunFilter{})
	s.Require().NoError(err)
	s.Empty(runs)
}

func (s *StoreSuite) TestSimulationRunQueryBudgets() {
	simulation := s.simulation("app")
	for range 20 {
		s.startedRun("app", time.Minute)
	}
	run := SimulationRun{CustomerID: "app", SimulationID: simulation.ID, Mode: SimulationText}
	cases := make([]SimulationCase, maxVariations)
	for i := range cases {
		cases[i] = SimulationCase{Variation: i, Scenario: simulation.Scenario}
	}

	s.Equal(4, s.countQueries(func() {
		s.Require().NoError(s.store.SimulationRuns.Start(s.ctx, &run, cases, time.Minute))
	}), "begin, the run, every case at once and commit")
	s.Equal(1, s.countQueries(func() {
		runs, err := s.store.SimulationRuns.Select(s.ctx, "app", SimulationRunFilter{})
		s.Require().NoError(err)
		s.Len(runs, 21)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.SimulationRuns.Get(s.ctx, "app", run.ID)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.SimulationRuns.Renew(s.ctx, []string{run.ID}, time.Hour)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.SimulationRuns.Stop(s.ctx, "app", run.ID)) }))
	s.Equal(4, s.countQueries(func() { s.Require().NoError(s.store.SimulationRuns.Abandon(s.ctx, time.Now())) }),
		"begin, the cases, the runs and commit")
	run.State = SimulationPassed
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.SimulationRuns.Finish(s.ctx, run)) }))
}
