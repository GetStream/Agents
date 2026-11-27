package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Recording is one non-realtime job: a recording to transcribe or a text to speak.
//
// It is stored rather than held in memory because it outlives the request that created
// it. A caller is handed an id and comes back for the result, and an hour of audio takes
// minutes to transcribe, so there is nowhere else the answer could wait.
type Recording struct {
	bun.BaseModel `bun:"table:recordings,alias:rec"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	// Modality is stt for a transcription and tts for a recorded voice.
	Modality string `bun:"modality,notnull"`
	// Status is where the job has got to: RecordingQueued, RecordingCompleted or
	// RecordingFailed.
	Status string `bun:"status,notnull"`
	// Provider and Model are what served it, once routing has picked.
	Provider string `bun:"provider,nullzero"`
	Model    string `bun:"model,nullzero"`
	// Text is what to say, for a voice job.
	Text string `bun:"text,nullzero"`
	// STT is what a transcription was asked for.
	STT options.STT `bun:"stt,type:jsonb"`
	// Callback is a URL the finished job is POSTed to. Empty means the caller polls.
	Callback string            `bun:"callback,nullzero"`
	Tags     map[string]string `bun:"tags,type:jsonb"`
	// Result is the finished transcript or the finished audio, as JSON. It is one column
	// rather than a table per modality because nothing queries inside it: a result is
	// written once and read whole.
	Result json.RawMessage `bun:"result,type:jsonb,nullzero"`
	// Error is why the job failed, if it did: RecordingFailure, or a refusal the caller can
	// correct.
	Error       string     `bun:"error,nullzero"`
	CreatedAt   time.Time  `bun:"created_at,notnull"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull"`
	CompletedAt *time.Time `bun:"completed_at"`
}

// Where a recording job has got to.
const (
	// RecordingQueued is accepted and not yet started.
	RecordingQueued = "queued"
	// RecordingCompleted has a result.
	RecordingCompleted = "completed"
	// RecordingFailed has a reason instead.
	RecordingFailed = "failed"
)

// RecordingFailure is the reason a failed job gives its caller. The failure itself can
// name hosts, URLs and a provider's body, so it goes only to the log.
const RecordingFailure = "the recording could not be completed"

// Validate refuses a job with no customer, or for anything but transcribing or speaking.
func (r *Recording) Validate() error {
	return validate("recording").
		required("customer_id", r.CustomerID).
		oneOf("modality", r.Modality, "stt", "tts").
		done()
}

// Recordings are the non-realtime jobs customers asked for, in the shard that took them.
type Recordings struct {
	store *Store
}

// Insert accepts a job and fills in its id and timestamps. It is written before anything is
// sent to a provider, so a caller handed an id can always ask about it.
func (r *Recordings) Insert(ctx context.Context, recording *Recording) error {
	if err := recording.Validate(); err != nil {
		return stack.Wrap(err)
	}

	recording.ID = newID()
	now := time.Now().UTC()
	recording.CreatedAt = now
	recording.UpdatedAt = now
	recording.Status = RecordingQueued
	recording.Tags = orEmptyMap(recording.Tags)

	if _, err := r.store.shard.NewInsert().Model(recording).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: create recording: %w", err))
	}
	return nil
}

// Finish writes the result a job produced, or, when reason is set, that it failed and why.
// The caller reads reason back, so it is one written for them, RecordingFailure unless the
// failure is theirs to correct.
func (r *Recordings) Finish(ctx context.Context, customerID, id, provider, model string, result json.RawMessage, reason string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a recording id are required"))
	}
	now := time.Now().UTC()
	status := RecordingCompleted
	if reason != "" {
		status = RecordingFailed
	}

	_, err := r.store.shard.NewUpdate().Model((*Recording)(nil)).
		Set("status = ?", status).
		Set("provider = ?", provider).
		Set("model = ?", model).
		Set("result = ?", result).
		Set("error = ?", reason).
		Set("updated_at = ?", now).
		Set("completed_at = ?", now).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: finish recording: %w", err))
	}
	return nil
}

// ErrNoRecording is a recording id that the customer has no job for.
var ErrNoRecording = errors.New("store: there is no recording")

// Get returns one job a customer holds.
func (r *Recordings) Get(ctx context.Context, customerID, id string) (Recording, error) {
	if customerID == "" || id == "" {
		return Recording{}, stack.Wrap(errors.New("store: a customer and a recording id are required"))
	}

	var recording Recording
	err := r.store.shard.NewSelect().Model(&recording).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Recording{}, stack.Wrap(fmt.Errorf("%w %s", ErrNoRecording, id))
	}
	if err != nil {
		return Recording{}, stack.Wrap(fmt.Errorf("store: recording: %w", err))
	}
	return recording, nil
}
