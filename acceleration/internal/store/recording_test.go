//go:build integration

package store

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// A failed job's error is a provider's text, which can echo the key it was sent, and the
// caller reads it back: it is kept without the key, and cut as the requests table cuts an
// error. One with no key is kept as it was.
func (s *StoreSuite) TestAFailedRecordingsErrorIsKeptWithoutTheKeyItEchoed() {
	key := "sk-test-" + rand.Text()
	for failure, kept := range map[string]string{
		"deepgram: 401 Unauthorized\nAuthorization: Bearer " + key: "deepgram: 401 Unauthorized\n[redacted]",
		"deepgram: 503 Service Unavailable":                        "deepgram: 503 Service Unavailable",
		strings.Repeat("x", 9000):                                  strings.Repeat("x", 2048) + "… [truncated]",
	} {
		job := Recording{CustomerID: "acme", Modality: "stt"}
		s.Require().NoError(s.store.Recordings.Insert(s.ctx, &job))

		s.Require().NoError(s.store.Recordings.Finish(s.ctx, job.CustomerID, job.ID, "deepgram", "nova", nil, errors.New(failure)))

		found, err := s.store.Recordings.Get(s.ctx, "acme", job.ID)
		s.Require().NoError(err)
		s.Equal(RecordingFailed, found.Status)
		s.Equal(kept, found.Error)
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
		{"Recordings.Start", 1, 0, func() error {
			return s.store.Recordings.Start(s.ctx, recording.CustomerID, recording.ID, "stub", "stub-tts")
		}},
		{"Recordings.Finish", 1, 0, func() error {
			return s.store.Recordings.Finish(s.ctx, recording.CustomerID, recording.ID, "stub", "stub-tts", json.RawMessage(`{}`), nil)
		}},
		{"Recordings.Get", 1, 0, func() error { _, err := s.store.Recordings.Get(s.ctx, "app", recording.ID); return err }},
	})
}

func (s *StoreSuite) TestAnotherCustomersWriteToARecordingIdChangesNothing() {
	writes := map[string]func(id string) error{
		"Start": func(id string) error {
			return s.store.Recordings.Start(s.ctx, "somebody-else", id, "their-stt", "their-model")
		},
		"Finish": func(id string) error {
			return s.store.Recordings.Finish(s.ctx, "somebody-else", id, "their-stt", "their-model", json.RawMessage(`{"text":"theirs"}`), nil)
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
