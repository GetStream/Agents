package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the handler dependencies for eventforward tasks.
type Deps struct {
	Processor Processor
}

// Register binds eventforward tasks and periodic due-delivery sweep to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Processor != nil {
		async.Register(reg, &DeliverDueHandler{Processor: deps.Processor})
		async.RegisterPeriodic(reg, "@every 1m", DeliverDue{})
	}
}
