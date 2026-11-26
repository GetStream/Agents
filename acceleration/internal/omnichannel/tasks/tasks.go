// Package tasks defines typed async tasks for omnichannel episode closing and summarization.
package tasks

import (
	"context"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

const (
	// TypeSummarizeEpisode summarizes a single closed episode and updates its Stream Chat card.
	TypeSummarizeEpisode = "omnichannel:summarize_episode"
	// TypeCloseIdleEpisodes closes idle thread episodes and summarizes them.
	TypeCloseIdleEpisodes = "omnichannel:close_idle_episodes"

	summarizeTimeout = 2 * time.Minute
	summarizeRetries = 3

	sweepTimeout = 2 * time.Minute
	sweepRetries = 1
)

// SummarizeEpisode generates and writes the summary for a single closed episode.
type SummarizeEpisode struct {
	Episode store.ClosedEpisode `json:"episode"`
	Author  string              `json:"author"`
}

func (SummarizeEpisode) TaskType() string   { return TypeSummarizeEpisode }
func (SummarizeEpisode) Queue() async.Queue { return async.QueueMaintenance }
func (SummarizeEpisode) Scope() async.Scope { return async.ScopeGlobal }
func (SummarizeEpisode) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(summarizeTimeout),
		async.WithMaxRetry(summarizeRetries),
	}
}

// CloseIdleEpisodes sweeps idle episodes and re-claims any whose summary lease expired.
type CloseIdleEpisodes struct{}

func (CloseIdleEpisodes) TaskType() string   { return TypeCloseIdleEpisodes }
func (CloseIdleEpisodes) Queue() async.Queue { return async.QueueMaintenance }
func (CloseIdleEpisodes) Scope() async.Scope { return async.ScopeGlobal }
func (CloseIdleEpisodes) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(sweepTimeout),
		async.WithMaxRetry(sweepRetries),
	}
}

// Closer performs episode sweeping and summarization.
type Closer interface {
	SummarizeAsyncEpisode(ctx context.Context, episode store.ClosedEpisode, author string) error
	Sweep(ctx context.Context) error
}

// SummarizeEpisodeHandler adapts Closer to async.Handler[SummarizeEpisode].
type SummarizeEpisodeHandler struct {
	Closer Closer
}

func (h *SummarizeEpisodeHandler) Handle(ctx context.Context, task SummarizeEpisode) error {
	return h.Closer.SummarizeAsyncEpisode(ctx, task.Episode, task.Author)
}

// CloseIdleEpisodesHandler adapts Closer to async.Handler[CloseIdleEpisodes].
type CloseIdleEpisodesHandler struct {
	Closer Closer
}

func (h *CloseIdleEpisodesHandler) Handle(ctx context.Context, _ CloseIdleEpisodes) error {
	return h.Closer.Sweep(ctx)
}
