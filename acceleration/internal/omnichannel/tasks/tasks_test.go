package tasks_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/async"
	"github.com/GetStream/Vision-Agents/acceleration/internal/async/asynctest"
	omnitasks "github.com/GetStream/Vision-Agents/acceleration/internal/omnichannel/tasks"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

type fakeCloser struct {
	summarized []string
	sweeps     int
}

func (f *fakeCloser) SummarizeAsyncEpisode(_ context.Context, ep store.ClosedEpisode, author string) error {
	f.summarized = append(f.summarized, ep.ID+":"+author)
	return nil
}

func (f *fakeCloser) Sweep(_ context.Context) error {
	f.sweeps++
	return nil
}

type fakeCards struct {
	written []string
}

func (f *fakeCards) WriteAsyncCard(_ context.Context, task omnitasks.WriteCard) error {
	f.written = append(f.written, task.Episode.ID)
	return nil
}

func TestOmnichannelTasksWithAsynctest(t *testing.T) {
	tr := asynctest.New(t)
	fake := &fakeCloser{}
	cards := &fakeCards{}
	omnitasks.Register(tr.Registry(), omnitasks.Deps{Closer: fake, Cards: cards})

	_, err := async.Enqueue(context.Background(), tr, omnitasks.SummarizeEpisode{
		Episode: store.ClosedEpisode{Episode: store.Episode{ID: "ep-1"}},
		Author:  "agent-1",
	})
	require.NoError(t, err)
	_, err = async.Enqueue(context.Background(), tr, omnitasks.CloseIdleEpisodes{})
	require.NoError(t, err)
	_, err = async.Enqueue(context.Background(), tr, omnitasks.WriteCard{
		Episode: store.Episode{ID: "ep-2"},
		New:     true,
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"ep-1:agent-1"}, fake.summarized)
	assert.Equal(t, 1, fake.sweeps)
	assert.Equal(t, []string{"ep-2"}, cards.written)
}
