//go:build integration

package store

import (
	"encoding/json"
	"fmt"
)

// A failed job's reason is read back by its caller, so the job keeps the one it was finished
// with, and a job finished with none completed.
func (s *StoreSuite) TestAFinishedRecordingKeepsTheReasonItFailedWith() {
	for reason, status := range map[string]string{
		RecordingFailure:                         RecordingFailed,
		"tts: there is no voice called narrator": RecordingFailed,
		"":                                       RecordingCompleted,
	} {
		job := Recording{CustomerID: "acme", Modality: "stt"}
		s.Require().NoError(s.store.Recordings.Insert(s.ctx, &job))

		s.Require().NoError(s.store.Recordings.Finish(s.ctx, job.CustomerID, job.ID, "deepgram", "nova", nil, reason))

		found, err := s.store.Recordings.Get(s.ctx, "acme", job.ID)
		s.Require().NoError(err)
		s.Equal(status, found.Status, reason)
		s.Equal(reason, found.Error)
	}
}

func (s *StoreSuite) TestRecordingQueryBudgets() {
	for i := range 30 {
		s.Require().NoError(s.store.Recordings.Insert(s.ctx, &Recording{ID: fmt.Sprintf("recording-%02d", i),
			CustomerID: "app", Modality: "stt", Status: RecordingQueued}))
	}
	recording := &Recording{CustomerID: "app", Modality: "tts"}
	s.withinQueryBudgets([]queryBudget{
		{"Recordings.Insert", 1, 0, func() error { return s.store.Recordings.Insert(s.ctx, recording) }},
		{"Recordings.Finish", 1, 0, func() error {
			return s.store.Recordings.Finish(s.ctx, recording.CustomerID, recording.ID, "stub", "stub-tts", json.RawMessage(`{}`), "")
		}},
		{"Recordings.Get", 1, 0, func() error { _, err := s.store.Recordings.Get(s.ctx, "app", recording.ID); return err }},
	})
}

func (s *StoreSuite) TestAnotherCustomersWriteToARecordingIdChangesNothing() {
	writes := map[string]func(id string) error{
		"Finish": func(id string) error {
			return s.store.Recordings.Finish(s.ctx, "somebody-else", id, "their-stt", "their-model", json.RawMessage(`{"text":"theirs"}`), "")
		},
	}
	for name, write := range writes {
		s.Run(name, func() {
			job := Recording{CustomerID: "app", Modality: "stt"}
			s.Require().NoError(s.store.Recordings.Insert(s.ctx, &job))
			before, err := s.store.Recordings.Get(s.ctx, "app", job.ID)
			s.Require().NoError(err)

			s.Require().NoError(write(job.ID))

			after, err := s.store.Recordings.Get(s.ctx, "app", job.ID)
			s.Require().NoError(err)
			s.Equal(before, after, "app's recording is as it was")
		})
	}
}

func (s *StoreSuite) TestARecordingOfNoSpeechModalityIsNotWritten() {
	err := s.store.Recordings.Insert(s.ctx, &Recording{CustomerID: "app", Modality: "llm"})
	s.refusedUnwritten(err, "recordings", "customer_id", "app")
}
