package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the service methods invoked by knowledge file tasks.
type Deps struct {
	Reader FileReader
}

// Register binds knowledge file tasks to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Reader != nil {
		async.Register(reg, &ReadFileHandler{Reader: deps.Reader})
	}
}
