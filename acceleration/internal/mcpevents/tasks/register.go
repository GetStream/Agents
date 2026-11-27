package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the handler dependencies for MCP event tasks.
type Deps struct {
	Runner Runner
}

// Register binds MCP event tasks and the periodic subscription reconciliation sweep to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Runner != nil {
		async.Register(reg, &HandleEventHandler{Runner: deps.Runner})
		async.Register(reg, &ReconcileSubscriptionsHandler{Runner: deps.Runner})
		async.RegisterPeriodic(reg, "@every 1m", ReconcileSubscriptions{})
	}
}
