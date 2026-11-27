package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the handler dependencies for imagejobs tasks.
type Deps struct {
	Processor Processor
}

// Register binds imagejobs tasks and periodic recovery sweep to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Processor != nil {
		async.Register(reg, &DrawImageHandler{Processor: deps.Processor})
		async.Register(reg, &PostImageHandler{Processor: deps.Processor})
		async.Register(reg, &ProcessDueHandler{Processor: deps.Processor})
		async.RegisterPeriodic(reg, "@every 1m", ProcessDue{})
	}
}
