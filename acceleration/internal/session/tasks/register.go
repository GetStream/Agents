package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the handler dependencies for session tasks.
type Deps struct {
	Reviewer Reviewer
}

// Register binds session async task handlers to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Reviewer != nil {
		async.Register(reg, &ReviewCallHandler{Reviewer: deps.Reviewer})
	}
}
