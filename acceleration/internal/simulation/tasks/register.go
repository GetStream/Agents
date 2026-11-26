package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the handler dependencies for simulation tasks.
type Deps struct {
	Executor Executor
}

// Register binds simulation tasks to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Executor != nil {
		async.Register(reg, &RunSimulationHandler{Executor: deps.Executor})
	}
}
