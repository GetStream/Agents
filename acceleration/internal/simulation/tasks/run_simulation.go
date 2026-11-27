// Package tasks defines typed async tasks for agent simulations.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

const (
	// TypeRunSimulation executes a simulation run across its scenarios on the simulations queue.
	TypeRunSimulation = "simulation:run_simulation"

	runSimulationTimeout = 30 * time.Minute
	runSimulationRetries = 0
)

// RunSimulation executes a persisted simulation run.
type RunSimulation struct {
	CustomerID string `json:"customer_id"`
	RunID      string `json:"run_id"`
}

func (RunSimulation) TaskType() string   { return TypeRunSimulation }
func (RunSimulation) Queue() async.Queue { return async.QueueSimulations }
func (RunSimulation) Scope() async.Scope { return async.ScopeShard }
func (RunSimulation) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(runSimulationTimeout),
		async.WithMaxRetry(runSimulationRetries),
	}
}

// Executor runs a single queued simulation run.
type Executor interface {
	ExecuteAsyncRun(ctx context.Context, customerID, runID string) error
}

// RunSimulationHandler adapts Executor to async.Handler[RunSimulation].
type RunSimulationHandler struct {
	Executor Executor
}

func (h *RunSimulationHandler) Handle(ctx context.Context, task RunSimulation) error {
	return h.Executor.ExecuteAsyncRun(ctx, task.CustomerID, task.RunID)
}
