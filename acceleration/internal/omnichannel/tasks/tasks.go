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
	// TypeWriteCard writes an opened episode's Stream Chat card off the request path.
	TypeWriteCard = "omnichannel:write_card"

	summarizeTimeout = 2 * time.Minute
	summarizeRetries = 3

	writeCardTimeout = 45 * time.Second
	writeCardRetries = 3

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

// WriteCard writes an opened episode's card into the person's omni-channel.
type WriteCard struct {
	Episode   store.Episode         `json:"episode"`
	Contact   store.ContactMapEntry `json:"contact"`
	AgentName string                `json:"agent_name"`
	New       bool                  `json:"new"`
}

func (WriteCard) TaskType() string   { return TypeWriteCard }
func (WriteCard) Queue() async.Queue { return async.QueueMedia }
func (WriteCard) Scope() async.Scope { return async.ScopeShard }
func (WriteCard) TaskOptions() []async.Option {
	return []async.Option{
		async.WithTimeout(writeCardTimeout),
		async.WithMaxRetry(writeCardRetries),
	}
}

// CardWriter persists an opened episode's card into Stream Chat.
type CardWriter interface {
	WriteAsyncCard(ctx context.Context, task WriteCard) error
}

// WriteCardHandler adapts CardWriter to async.Handler[WriteCard].
type WriteCardHandler struct {
	Writer CardWriter
}

func (h *WriteCardHandler) Handle(ctx context.Context, task WriteCard) error {
	return h.Writer.WriteAsyncCard(ctx, task)
}
