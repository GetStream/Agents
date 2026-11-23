package ttsrouter

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts"
)

// timedRecorder takes took over a text, then answers its characters and 800 ms of audio, or
// fails with failure when it is set.
type timedRecorder struct {
	took    time.Duration
	failure error
}

func (t timedRecorder) Record(_ context.Context, recording tts.Recording) (tts.Recorded, error) {
	time.Sleep(t.took)
	if t.failure != nil {
		return tts.Recorded{}, t.failure
	}
	return tts.Recorded{Audio: []byte("audio"), Characters: int64(len(recording.Text)), AudioDurationMs: 800}, nil
}
func (timedRecorder) Start(context.Context) error { return nil }
func (timedRecorder) Close() error                { return nil }
func (timedRecorder) Provider() string            { return "voicy" }
func (timedRecorder) Model() string               { return "whole" }

// recordLogged speaks "hello" through a priority list of two voices, voicy speaking with first
// and spoken with next, each priced at 30 US dollars a million characters (30 micro-dollars a
// character), on a fake clock so the times logged are exact, and answers the "speech
// synthesis timing" lines of its app.
func (s *RecordingsSuite) recordLogged(app string, first, next timedRecorder) []map[string]any {
	var logs bytes.Buffer
	outer := s.T()
	synctest.Test(outer, func(t *testing.T) {
		s.SetT(t)
		defer s.SetT(outer)
		registry := NewRecorderRegistry()
		registry.Register("voicy", func(routing.Spec) (tts.Recorder, error) { return first, nil })
		registry.Register("spoken", func(routing.Spec) (tts.Recorder, error) { return next, nil })
		price := routing.Price{PerMillionChars: 30}
		config := routing.ModalityConfig{Providers: []routing.ProviderConfig{
			{Provider: "voicy", Model: "whole", Languages: []string{"en"}, Price: price},
			{Provider: "spoken", Model: "whole", Languages: []string{"en"}, Price: price},
		}}
		recordings, err := NewRecordings(Options{Config: config, Recorders: registry,
			Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
		s.Require().NoError(err)
		defer recordings.Close()

		_, _, _ = recordings.Record(context.Background(), Recording{CustomerID: app,
			Options: options.TTS{Providers: []string{"voicy/whole", "spoken/whole"}}, Text: "hello"})
	})
	var lines []map[string]any
	for decoder := json.NewDecoder(&logs); decoder.More(); {
		var line map[string]any
		s.Require().NoError(decoder.Decode(&line))
		if line["msg"] == "speech synthesis timing" && line["app_id"] == app {
			delete(line, "time")
			lines = append(lines, line)
		}
	}
	return lines
}

func (s *RecordingsSuite) TestASpokenRecordingLogsOneBatchLineWithWhatItCost() {
	app := "acme-" + rand.Text()

	lines := s.recordLogged(app, timedRecorder{took: 300 * time.Millisecond}, timedRecorder{})

	s.Equal([]map[string]any{{
		"level": "INFO", "msg": "speech synthesis timing",
		"call": "", "app_id": app, "provider": "voicy", "model": "whole",
		"batch": true, "characters": 5.0, "audio_ms": 800.0, "duration_ms": 300.0, "cost_micros": 150.0,
		"success": true, "error_code": "",
	}}, lines, "no ttfb_ms: one answer for the whole text has no first audio to time")
}

func (s *RecordingsSuite) TestEachCandidateARecordingFallsBackFromLogsItsOwnLine() {
	app := "acme-" + rand.Text()
	failure := errors.New("voicy: 500 for api_key=sk-live-0123456789abcdef")

	lines := s.recordLogged(app, timedRecorder{took: 120 * time.Millisecond, failure: failure},
		timedRecorder{took: 300 * time.Millisecond})

	s.Require().Len(lines, 2, "the voice that failed and the one that spoke")
	failed := lines[0]
	s.Require().IsType("", failed["error"])
	s.Contains(failed["error"], "voicy: 500 for")
	s.NotContains(failed["error"], "sk-live-0123456789abcdef")
	delete(failed, "error")
	s.Equal(map[string]any{
		"level": "INFO", "msg": "speech synthesis timing",
		"call": "", "app_id": app, "provider": "voicy", "model": "whole",
		"batch": true, "characters": 0.0, "audio_ms": 0.0, "duration_ms": 120.0, "cost_micros": 0.0,
		"success": false, "error_code": "record_failed",
	}, failed)
	s.Equal(map[string]any{
		"level": "INFO", "msg": "speech synthesis timing",
		"call": "", "app_id": app, "provider": "spoken", "model": "whole",
		"batch": true, "characters": 5.0, "audio_ms": 800.0, "duration_ms": 300.0, "cost_micros": 150.0,
		"success": true, "error_code": "",
	}, lines[1])
}
