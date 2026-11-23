package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// How a run or one of its conversations ended.
const (
	// SimulationPending is a case not yet started.
	SimulationPending = "pending"
	// SimulationRunning means the conversations are still being had.
	SimulationRunning = "running"
	// SimulationPassed means the judge was satisfied. A run passed only if every one of
	// its cases did.
	SimulationPassed = "passed"
	// SimulationFailed means the judge was not.
	SimulationFailed = "failed"
	// SimulationErrored means it never got as far as a ruling.
	SimulationErrored = "errored"
	// SimulationCancelled means somebody stopped it, or the process did.
	SimulationCancelled = "cancelled"
)

// SimulationRun is one press of Run, and the parent of however many conversations the
// variations asked for.
//
// What was run is copied onto the row rather than referenced, because editing a simulation
// must not rewrite what an old run tested.
type SimulationRun struct {
	bun.BaseModel `bun:"table:simulation_runs,alias:smr"`

	ID           string `bun:"id,pk"`
	CustomerID   string `bun:"customer_id,notnull"`
	SimulationID string `bun:"simulation_id,notnull"`
	State        string `bun:"state,notnull"`
	// Cases, Passed and Failed are the tally, so the log can list a run without reading
	// its conversations.
	Cases       int        `bun:"cases,notnull"`
	Passed      int        `bun:"passed,notnull"`
	Failed      int        `bun:"failed,notnull"`
	Mode        string     `bun:"mode,notnull"`
	ConfigID    string     `bun:"config_id,notnull"`
	Scenario    string     `bun:"scenario,notnull"`
	Assertion   string     `bun:"assertion,notnull"`
	JudgeTarget string     `bun:"judge_target,notnull"`
	Error       string     `bun:"error,nullzero"`
	StartedAt   time.Time  `bun:"started_at,notnull"`
	FinishedAt  *time.Time `bun:"finished_at"`
	// LeaseUntil is when the router having the conversations stops being their owner unless
	// it renews, and StopRequested a cancel asked of a router that may not be that one.
	LeaseUntil    *time.Time `bun:"lease_until"`
	StopRequested bool       `bun:"stop_requested,notnull"`
}

// SimulationRunFilter narrows which runs are listed. Every field is optional, and an empty
// filter is the customer's most recent runs.
type SimulationRunFilter struct {
	SimulationID string
	State        string
	// Limit caps how many come back. Zero leaves the store's own default.
	Limit int
}

// defaultRunLimit is how many runs are listed when nobody said.
const defaultRunLimit = 50

// Validate refuses a run the table would refuse.
func (r *SimulationRun) Validate() error {
	return validate("simulation run").
		required("customer_id", r.CustomerID).
		required("simulation_id", r.SimulationID).
		oneOf("state", r.State, SimulationRunning, SimulationPassed, SimulationFailed, SimulationErrored, SimulationCancelled).
		oneOf("mode", r.Mode, "", SimulationText, SimulationAudio).
		check(r.Cases >= 0 && r.Passed >= 0 && r.Failed >= 0, "cases", "are negative").
		done()
}

// ErrNoSimulationRun is a run id the customer holds no run by.
var ErrNoSimulationRun = errors.New("store: there is no simulation run")

// SimulationRuns are the runs of customers' simulations, in the shared database. A run is held
// by the router having its conversations until a time it renews, so another router can tell
// one whose router died from one still going.
type SimulationRuns struct {
	store *Store
}

// Start writes a run and all of its conversations at once, so a run that has only just begun
// already shows how many ways it is going to ask. It is leased until leaseUntil, and Renew keeps
// it held after that.
func (r *SimulationRuns) Start(ctx context.Context, run *SimulationRun, cases []SimulationCase, leaseUntil time.Time) error {
	if len(cases) == 0 {
		return stack.Wrap(&ValidationError{Model: "simulation run", Field: "cases", Reason: "is empty: a run needs at least one conversation to have"})
	}

	run.ID = newID()
	run.State = SimulationRunning
	run.Cases = len(cases)
	run.Passed = 0
	run.Failed = 0
	run.StartedAt = time.Now().UTC()
	run.FinishedAt = nil
	leased := leaseUntil.UTC()
	run.LeaseUntil = &leased
	run.StopRequested = false
	if err := run.Validate(); err != nil {
		return stack.Wrap(err)
	}

	for i := range cases {
		cases[i].ID = newID()
		cases[i].RunID = run.ID
		cases[i].State = SimulationPending
		cases[i].StartedAt = run.StartedAt
		if cases[i].Transcript == nil {
			cases[i].Transcript = []SimulationLine{}
		}
		if err := cases[i].Validate(); err != nil {
			return stack.Wrap(err)
		}
	}

	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.NewInsert().Model(run).Exec(ctx); err != nil {
			return err
		}
		_, err := tx.NewInsert().Model(&cases).Exec(ctx)
		return err
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: start simulation run: %w", err))
	}
	return nil
}

// Finish records how a run ended and what its conversations came to.
func (r *SimulationRuns) Finish(ctx context.Context, run SimulationRun) error {
	if run.CustomerID == "" || run.ID == "" {
		return stack.Wrap(errors.New("store: a customer and a run id are required"))
	}

	now := time.Now().UTC()
	run.FinishedAt = &now

	_, err := r.store.global.NewUpdate().Model(&run).
		Column("state", "passed", "failed", "error", "finished_at").
		Where("id = ?", run.ID).
		Where("customer_id = ?", run.CustomerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: finish simulation run: %w", err))
	}
	return nil
}

// Get returns one run a customer holds.
func (r *SimulationRuns) Get(ctx context.Context, customerID, id string) (SimulationRun, error) {
	if customerID == "" || id == "" {
		return SimulationRun{}, stack.Wrap(errors.New("store: a customer and a run id are required"))
	}

	var run SimulationRun
	err := r.store.global.NewSelect().Model(&run).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return SimulationRun{}, unknownSimulationRun(id)
	}
	if err != nil {
		return SimulationRun{}, stack.Wrap(fmt.Errorf("store: simulation run: %w", err))
	}
	return run, nil
}

// Select lists a customer's runs, newest first. It answers both what one simulation has come
// to and what every simulation has come to lately, which is the same question asked twice.
func (r *SimulationRuns) Select(ctx context.Context, customerID string, filter SimulationRunFilter) ([]SimulationRun, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultRunLimit
	}

	var runs []SimulationRun
	query := r.store.global.NewSelect().Model(&runs).
		Where("customer_id = ?", customerID)
	if filter.SimulationID != "" {
		query = query.Where("simulation_id = ?", filter.SimulationID)
	}
	if filter.State != "" {
		query = query.Where("state = ?", filter.State)
	}

	if err := query.Order("started_at DESC").Limit(limit).Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: simulation runs: %w", err))
	}
	return runs, nil
}

// Renew holds the runs a router is having until a later time, and returns the ones among them
// it should stop: asked to from anywhere, or no longer running.
func (r *SimulationRuns) Renew(ctx context.Context, ids []string, until time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	var renewed []string
	err := r.store.global.NewUpdate().Model((*SimulationRun)(nil)).
		Set("lease_until = ?", until.UTC()).
		Where("id IN (?)", bun.In(ids)).
		Where("state = ?", SimulationRunning).
		Where("NOT stop_requested").
		Returning("id").
		Scan(ctx, &renewed)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: renew simulation runs: %w", err))
	}
	return slices.DeleteFunc(slices.Clone(ids), func(id string) bool {
		return slices.Contains(renewed, id)
	}), nil
}

// Stop asks whichever router is holding a run to stop it.
func (r *SimulationRuns) Stop(ctx context.Context, customerID, id string) error {
	_, err := r.store.global.NewUpdate().Model((*SimulationRun)(nil)).
		Set("stop_requested = true").
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("state = ?", SimulationRunning).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: stop simulation run: %w", err))
	}
	return nil
}

// Abandon marks as errored the runs whose router stopped renewing them by now, and cancels
// their conversations. Nothing else will finish them: the conversations were held in that
// router, and it is gone. A run another router is still renewing is left alone, however old
// it is.
func (r *SimulationRuns) Abandon(ctx context.Context, now time.Time) error {
	now = now.UTC()

	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewUpdate().Model((*SimulationCase)(nil)).
			Set("state = ?", SimulationCancelled).
			Set("finished_at = ?", now).
			Where("state IN (?)", bun.In([]string{SimulationPending, SimulationRunning})).
			Where("run_id IN (SELECT id FROM simulation_runs WHERE state = ? AND (lease_until IS NULL OR lease_until < ?))",
				SimulationRunning, now).
			Exec(ctx)
		if err != nil {
			return err
		}

		_, err = tx.NewUpdate().Model((*SimulationRun)(nil)).
			Set("state = ?", SimulationErrored).
			Set("error = ?", "the router having this run stopped while it was still going").
			Set("finished_at = ?", now).
			Where("state = ?", SimulationRunning).
			Where("lease_until IS NULL OR lease_until < ?", now).
			Exec(ctx)
		return err
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: abandon simulation runs: %w", err))
	}
	return nil
}

func unknownSimulationRun(id string) error {
	return stack.Wrap(fmt.Errorf("%w %s", ErrNoSimulationRun, id))
}
