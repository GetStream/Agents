package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the service methods invoked by knowledge URL tasks.
type Deps struct {
	Indexer   Indexer
	Refresher Refresher
}

// Register binds knowledge URL tasks and the periodic refresh sweep to reg.
func Register(reg *async.Registry, deps Deps) error {
	if deps.Indexer != nil {
		async.Register(reg, &IndexURLHandler{Indexer: deps.Indexer})
	}
	if deps.Refresher != nil {
		async.Register(reg, &RefreshDueHandler{Refresher: deps.Refresher})
		if err := async.RegisterPeriodic(reg, "@every 1m", RefreshDue{}); err != nil {
			return err
		}
	}
	return nil
}
