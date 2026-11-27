package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the handler dependencies for limitnotice tasks.
type Deps struct {
	Sender  Sender
	Checker AnomalyChecker
}

// Register binds limitnotice tasks and periodic schedules to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Sender != nil {
		async.Register(reg, &PostNoticeHandler{Sender: deps.Sender})
		async.Register(reg, &SweepNoticesHandler{Sender: deps.Sender})
		async.RegisterPeriodic(reg, "@every 1m", SweepNotices{})
	}
	if deps.Checker != nil {
		async.Register(reg, &CheckAnomaliesHandler{Checker: deps.Checker})
		async.RegisterPeriodic(reg, "@every 15m", CheckAnomalies{})
	}
}
