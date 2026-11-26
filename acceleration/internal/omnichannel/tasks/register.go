package tasks

import (
	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
)

// Deps supplies the handler dependencies for omnichannel tasks.
type Deps struct {
	Closer Closer
}

// Register binds omnichannel tasks and periodic idle-episode sweep to reg.
func Register(reg *async.Registry, deps Deps) {
	if deps.Closer != nil {
		async.Register(reg, &SummarizeEpisodeHandler{Closer: deps.Closer})
		async.Register(reg, &CloseIdleEpisodesHandler{Closer: deps.Closer})
		async.RegisterPeriodic(reg, "@every 1m", CloseIdleEpisodes{})
	}
}
