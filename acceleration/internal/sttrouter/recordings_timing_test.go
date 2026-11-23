package sttrouter

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
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

// timedTranscriber takes a quarter of a second over a recording, then answers a second and a
// half of audio, or fails with failure when it is set.
type timedTranscriber struct{ failure error }

func (t timedTranscriber) Transcribe(context.Context, stt.Recording) (stt.Transcription, error) {
	time.Sleep(250 * time.Millisecond)
	if t.failure != nil {
		return stt.Transcription{}, t.failure
	}
	return stt.Transcription{Text: "hello there", AudioDurationMs: 1_500}, nil
}
func (timedTranscriber) Start(context.Context) error { return nil }
func (timedTranscriber) Close() error                { return nil }
func (timedTranscriber) Provider() string            { return "batchy" }
func (timedTranscriber) Model() string               { return "whole" }

// transcribeLogged transcribes one recording with a timedTranscriber priced at 3.60 US dollars
// an audio hour (a millisecond of audio costs one micro-dollar), on a fake clock so the times
// logged are exact, and answers the "speech transcription timing" lines of its app.
func (s *RecordingsSuite) transcribeLogged(transcriber timedTranscriber, recording Recording) []map[string]any {
	var logs bytes.Buffer
	outer := s.T()
	synctest.Test(outer, func(t *testing.T) {
		s.SetT(t)
		defer s.SetT(outer)
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
		defer slog.SetDefault(previous)
		transcribers := NewTranscriberRegistry()
		transcribers.Register("batchy", func(routing.Spec) (stt.Transcriber, error) { return transcriber, nil })
		config := routing.ModalityConfig{Providers: []routing.ProviderConfig{{Provider: "batchy", Model: "whole",
			Languages: []string{"en"}, Price: routing.Price{PerAudioHour: 3.6}}}}
		recordings, err := NewRecordings(Options{Config: config, Transcribers: transcribers})
		s.Require().NoError(err)
		defer recordings.Close()

		recording.Options = options.STT{Target: "batchy/whole"}
		_, _, _ = recordings.Transcribe(context.Background(), recording)
	})
	var lines []map[string]any
	for decoder := json.NewDecoder(&logs); decoder.More(); {
		var line map[string]any
		s.Require().NoError(decoder.Decode(&line))
		if line["msg"] == "speech transcription timing" && line["app_id"] == recording.CustomerID {
			delete(line, "time")
			lines = append(lines, line)
		}
	}
	return lines
}

func (s *RecordingsSuite) TestATranscribedRecordingLogsOneBatchLineWithWhatItCost() {
	app := "acme-" + rand.Text()

	lines := s.transcribeLogged(timedTranscriber{}, Recording{CustomerID: app, AgentID: "concierge", CallID: "default:c1"})

	s.Equal([]map[string]any{{
		"level": "INFO", "msg": "speech transcription timing",
		"call": "default:c1", "app_id": app, "agent_id": "concierge", "provider": "batchy", "model": "whole",
		"batch": true, "audio_ms": 1_500.0, "duration_ms": 250.0, "cost_micros": 1_500.0,
		"success": true, "error_code": "",
	}}, lines, "no ttfb_ms: one answer for the whole file has no first transcript to time")
}

func (s *RecordingsSuite) TestAFailedTranscriptionLogsItsCodeAndItsErrorWithNoCredential() {
	app := "acme-" + rand.Text()
	failure := errors.New("deepgram: 401 for api_key=sk-live-0123456789abcdef")

	lines := s.transcribeLogged(timedTranscriber{failure: failure}, Recording{CustomerID: app})

	s.Require().Len(lines, 1)
	line := lines[0]
	s.Require().IsType("", line["error"])
	s.Contains(line["error"], "deepgram: 401 for")
	s.NotContains(line["error"], "sk-live-0123456789abcdef")
	delete(line, "error")
	s.Equal(map[string]any{
		"level": "INFO", "msg": "speech transcription timing",
		"call": "", "app_id": app, "provider": "batchy", "model": "whole",
		"batch": true, "audio_ms": 0.0, "duration_ms": 250.0, "cost_micros": 0.0,
		"success": false, "error_code": "transcribe_failed",
	}, line, "a recording outside a call names no agent and an empty call")
}
