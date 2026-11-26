package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the handler dependencies for channelbridge tasks.
type Deps struct {
	Sender ReplySender
}

// Register binds channelbridge tasks to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Sender != nil {
		async.Register(reg, &SendReplyHandler{Sender: deps.Sender})
	}
}
