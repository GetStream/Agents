package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the handler dependencies for Stream Chat inbound tasks.
type Deps struct {
	Dispatcher Dispatcher
}

// Register binds Stream Chat inbound tasks to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Dispatcher != nil {
		async.Register(reg, &DispatchMessageHandler{Dispatcher: deps.Dispatcher})
	}
}
