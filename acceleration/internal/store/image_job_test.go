//go:build integration

package store

import (
	"encoding/json"
	"time"
)

// queuedImageJob queues a picture in a conversation of its own and returns it as queued.
func (s *StoreSuite) queuedImageJob() ImageJob {
	job := ImageJob{
		ID: "job-" + time.Now().Format("150405.000000000"), CustomerID: "acme", CID: "agent:a", AgentUser: "illustrator",
		SessionID: "session-1", ToolCallID: "draw-1", RequestKey: "key-draw-1", Target: "image-fast", Attribution: json.RawMessage(`{}`), Prompt: "a field",
	}
	queued, err := s.store.ImageJobs.Queue(s.ctx, &job)
	s.Require().NoError(err)
	s.Require().True(queued)
	return job
}

func (s *StoreSuite) TestASlowClaimCannotFinishAnImageJobAnotherClaimTook() {
	// A router that ran past its lease and the one that claimed the job after it would both
	// post, and both record, without the claim being part of every write.
	s.queuedImageJob()
	start := time.Now()
	first, err := s.store.ImageJobs.Claim(s.ctx, start, 1, start.Add(time.Millisecond), false)
	s.Require().NoError(err)
	s.Require().Len(first, 1)
	second, err := s.store.ImageJobs.Claim(s.ctx, start.Add(time.Second), 1, start.Add(time.Minute), false)
	s.Require().NoError(err)
	s.Require().Len(second, 1, "due again once the first lease ran out")
	s.Equal(2, second[0].Attempts)

	finished, err := s.store.ImageJobs.Finish(s.ctx, first[0], ImageJobDone, "", time.Now())
	s.Require().NoError(err)
	s.False(finished, "the first claim no longer holds it")
	kept, err := s.store.ImageJobs.KeepDrawing(s.ctx, first[0], []byte("png"), "image/png", "Here is the picture.")
	s.Require().NoError(err)
	s.False(kept)

	finished, err = s.store.ImageJobs.Finish(s.ctx, second[0], ImageJobDone, "", time.Now())
	s.Require().NoError(err)
	s.True(finished)
}

func (s *StoreSuite) TestAnImageJobUnderItsLeaseIsNotClaimedAgain() {
	s.queuedImageJob()
	now := time.Now()
	claimed, err := s.store.ImageJobs.Claim(s.ctx, now, 1, now.Add(time.Minute), false)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)

	again, err := s.store.ImageJobs.Claim(s.ctx, now.Add(time.Second), 1, now.Add(2*time.Minute), false)
	s.Require().NoError(err)
	s.Empty(again)
}

func (s *StoreSuite) TestAnImageJobWithItsMessageDecidedIsClaimedOnlyToPost() {
	// A router's draw slots are kept for the jobs still to draw: a decided one only posts.
	s.queuedImageJob()
	now := time.Now()
	claimed, err := s.store.ImageJobs.Claim(s.ctx, now, 1, now, false)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	kept, err := s.store.ImageJobs.KeepDrawing(s.ctx, claimed[0], []byte("png"), "image/png", "Here is the picture.")
	s.Require().NoError(err)
	s.Require().True(kept)

	toDraw, err := s.store.ImageJobs.Claim(s.ctx, now.Add(time.Second), 1, now.Add(time.Minute), false)
	s.Require().NoError(err)
	s.Empty(toDraw)
	toPost, err := s.store.ImageJobs.Claim(s.ctx, now.Add(time.Second), 1, now.Add(time.Minute), true)
	s.Require().NoError(err)
	s.Require().Len(toPost, 1)
	s.Nil(toPost[0].Drawn, "a claim of up to 16 jobs loads none of their pictures")
	drawn, err := s.store.ImageJobs.Drawn(s.ctx, toPost[0].ID)
	s.Require().NoError(err)
	s.Equal([]byte("png"), drawn, "read when the upload needs it")
}

func (s *StoreSuite) TestTheNextImageJobDueIsToldApartForDrawingAndPosting() {
	// A router whose draw slots are full waits for a decided job's post, not for a draw it has
	// no slot for, which would have it ask again every poll.
	toDraw := s.queuedImageJob()
	_, err := s.store.GlobalDB().NewUpdate().Model((*ImageJob)(nil)).Set("next_attempt_at = ?", time.Now().Add(-time.Minute)).
		Where("id = ?", toDraw.ID).Exec(s.ctx)
	s.Require().NoError(err)

	_, queued, err := s.store.ImageJobs.NextDue(s.ctx, true)
	s.Require().NoError(err)
	s.False(queued, "no decided job is queued")
	next, queued, err := s.store.ImageJobs.NextDue(s.ctx, false)
	s.Require().NoError(err)
	s.True(queued)
	s.True(next.Before(time.Now()))
}

func (s *StoreSuite) TestPostedImageJobsAreDeletedAfterADayAndFailedOnesAfter30Days() {
	// They hold the prompt: kept as long as someone may look for why, and no longer.
	now := time.Now()
	for _, finished := range []struct {
		call, state string
		at          time.Time
	}{
		{"done-old", ImageJobDone, now.Add(-25 * time.Hour)},
		{"done-new", ImageJobDone, now.Add(-time.Hour)},
		{"failed-old", ImageJobFailed, now.Add(-31 * 24 * time.Hour)},
		{"failed-new", ImageJobFailed, now.Add(-29 * 24 * time.Hour)},
	} {
		job := ImageJob{ID: finished.call, CustomerID: "acme", CID: "agent:a", AgentUser: "illustrator", SessionID: "session-1",
			ToolCallID: finished.call, RequestKey: "key-" + finished.call, Target: "image-fast", Attribution: json.RawMessage(`{}`), Prompt: "a field"}
		_, err := s.store.ImageJobs.Queue(s.ctx, &job)
		s.Require().NoError(err)
		_, err = s.store.GlobalDB().NewUpdate().Model((*ImageJob)(nil)).Set("state = ?", finished.state).
			Set("finished_at = ?", finished.at).Where("id = ?", finished.call).Exec(s.ctx)
		s.Require().NoError(err)
	}
	s.queuedImageJob()

	pruned, err := s.store.ImageJobs.Prune(s.ctx, now)

	s.Require().NoError(err)
	s.EqualValues(2, pruned)
	var left []string
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model((*ImageJob)(nil)).Column("tool_call_id").Order("tool_call_id").Scan(s.ctx, &left))
	s.Equal([]string{"done-new", "draw-1", "failed-new"}, left, "the queued one and the ones within their keep stay")
}

func (s *StoreSuite) TestImageJobQueryBudgets() {
	var job ImageJob
	s.Equal(1, s.countQueries(func() { job = s.queuedImageJob() }), "Queue")
	now := time.Now()
	s.Equal(1, s.countQueries(func() {
		_, found, err := s.store.ImageJobs.NextDue(s.ctx, false)
		s.Require().NoError(err)
		s.True(found)
	}), "NextDue")
	s.Equal(1, s.countQueries(func() {
		claimed, err := s.store.ImageJobs.Claim(s.ctx, now, 10, now.Add(time.Minute), false)
		s.Require().NoError(err)
		s.Require().Len(claimed, 1)
		job = claimed[0]
	}), "Claim")
	held := func(name string, write func() (bool, error)) {
		s.Equal(1, s.countQueries(func() {
			changed, err := write()
			s.Require().NoError(err)
			s.True(changed, name)
		}), name)
	}
	held("StartDraw", func() (bool, error) { return s.store.ImageJobs.StartDraw(s.ctx, job) })
	held("KeepDrawing", func() (bool, error) {
		return s.store.ImageJobs.KeepDrawing(s.ctx, job, []byte("png"), "image/png", "Here it is.")
	})
	s.Equal(1, s.countQueries(func() {
		drawn, err := s.store.ImageJobs.Drawn(s.ctx, job.ID)
		s.Require().NoError(err)
		s.Equal([]byte("png"), drawn)
	}), "Drawn")
	held("SavePicture", func() (bool, error) { return s.store.ImageJobs.SavePicture(s.ctx, job, json.RawMessage(`{}`)) })
	held("DecideFailure", func() (bool, error) { return s.store.ImageJobs.DecideFailure(s.ctx, job, "No picture.") })
	held("RetryPost", func() (bool, error) { return s.store.ImageJobs.RetryPost(s.ctx, job, now) })
	held("Release", func() (bool, error) { return s.store.ImageJobs.Release(s.ctx, job, true, now) })
	held("Finish", func() (bool, error) { return s.store.ImageJobs.Finish(s.ctx, job, ImageJobDone, "", now) })
	s.Equal(1, s.countQueries(func() {
		pruned, err := s.store.ImageJobs.Prune(s.ctx, now.Add(30*24*time.Hour))
		s.Require().NoError(err)
		s.Equal(int64(1), pruned)
	}), "Prune")
}

// PostedImageJobsAreDeletedAfterADayAndFailedOnesAfter30Days, with a backlog of more than one statement's worth.
func (s *StoreSuite) TestABacklogOfFinishedImageJobsIsPrunedWhole() {
	s.inSmallBatches()
	deletes := s.statementsLike("DELETE FROM image_jobs", s.TestPostedImageJobsAreDeletedAfterADayAndFailedOnesAfter30Days)
	s.Equal(3, deletes, "a row per statement, then the one that finds none")
}
