package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the handler dependencies for channels tasks.
type Deps struct {
	Processor Processor
}

// Register binds channels async task handlers to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Processor != nil {
		async.Register(reg, &AnswerMessageHandler{Processor: deps.Processor})
	}
}
