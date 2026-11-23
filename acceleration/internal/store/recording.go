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
	// Status is where the job has got to: RecordingQueued, RecordingRunning,
	// RecordingCompleted or RecordingFailed.
	Status string `bun:"status,notnull"`
	// Provider and Model are what served it, once routing has picked.
	Provider string `bun:"provider,nullzero"`
	Model    string `bun:"model,nullzero"`
	// Source is the recording to transcribe: a URL, or empty when the audio was sent
	// inline, which is not kept.
	Source string `bun:"source,nullzero"`
	// Text is what to say, for a voice job.
	Text string `bun:"text,nullzero"`
	// Options are what was asked for. Only the block for this modality is set.
	STT options.STT `bun:"stt,type:jsonb"`
	TTS options.TTS `bun:"tts,type:jsonb"`
	// Callback is a URL the finished job is POSTed to. Empty means the caller polls.
	Callback string            `bun:"callback,nullzero"`
	Tags     map[string]string `bun:"tags,type:jsonb"`
	// Result is the finished transcript or the finished audio, as JSON. It is one column
	// rather than a table per modality because nothing queries inside it: a result is
	// written once and read whole.
	Result json.RawMessage `bun:"result,type:jsonb,nullzero"`
	// Error is why the job failed, if it did.
	Error       string     `bun:"error,nullzero"`
	CreatedAt   time.Time  `bun:"created_at,notnull"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull"`
	CompletedAt *time.Time `bun:"completed_at"`
}

// Where a recording job has got to.
const (
	// RecordingQueued is accepted and not yet started.
	RecordingQueued = "queued"
	// RecordingRunning is at the provider.
	RecordingRunning = "running"
	// RecordingCompleted has a result.
	RecordingCompleted = "completed"
	// RecordingFailed has a reason instead.
	RecordingFailed = "failed"
)

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
	if recording.Tags == nil {
		recording.Tags = map[string]string{}
	}

	if _, err := r.store.shard.NewInsert().Model(recording).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: create recording: %w", err))
	}
	return nil
}

// Start marks a job as at the provider and records which one took it.
func (r *Recordings) Start(ctx context.Context, id, provider, model string) error {
	_, err := r.store.shard.NewUpdate().Model((*Recording)(nil)).
		Set("status = ?", RecordingRunning).
		Set("provider = ?", provider).
		Set("model = ?", model).
		Set("updated_at = ?", time.Now().UTC()).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: start recording: %w", err))
	}
	return nil
}

// Finish writes the result a job produced, or the reason it produced none.
func (r *Recordings) Finish(ctx context.Context, id, provider, model string, result json.RawMessage, failure error) error {
	now := time.Now().UTC()
	status := RecordingCompleted
	reason := ""
	if failure != nil {
		status = RecordingFailed
		// A provider's text can echo the key it was sent, and a caller reads this back.
		reason = SafeLogText(failure.Error())
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

	recording, err := whicheverShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (Recording, error) {
		var recording Recording
		err := db.NewSelect().Model(&recording).
			Where("id = ?", id).
			Where("customer_id = ?", customerID).
			Limit(1).
			Scan(ctx)
		return recording, err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Recording{}, stack.Wrap(fmt.Errorf("%w %s", ErrNoRecording, id))
	}
	if err != nil {
		return Recording{}, stack.Wrap(fmt.Errorf("store: recording: %w", err))
	}
	return recording, nil
}
