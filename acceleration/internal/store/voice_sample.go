package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// VoiceSample is one recording of the voice. The audio is in object storage, so the row
// says where it went rather than holding it.
type VoiceSample struct {
	bun.BaseModel `bun:"table:voice_samples,alias:vs"`

	ID      string `bun:"id,pk"`
	VoiceID string `bun:"voice_id,notnull"`
	// ObjectKey locates the audio in the bucket the deployment was given.
	ObjectKey   string `bun:"object_key,notnull"`
	ContentType string `bun:"content_type,notnull"`
	Bytes       int64  `bun:"bytes,notnull"`
	// Transcript is what is said in the recording, which the providers that ask for one
	// clone more faithfully with.
	Transcript string    `bun:"transcript,notnull"`
	CreatedAt  time.Time `bun:"created_at,notnull"`
}

// Validate refuses a sample the table would refuse.
func (v *VoiceSample) Validate() error {
	return validate("voice sample").
		required("voice_id", v.VoiceID).
		required("object_key", v.ObjectKey).
		check(v.Bytes >= 0, "bytes", "is negative").
		done()
}

// VoiceSamples are the recordings voices were made from, in the shared database. A sample
// belongs to its voice, whose owner the caller has checked already.
type VoiceSamples struct {
	store *Store
}

// Insert records a recording that has been stored.
func (r *VoiceSamples) Insert(ctx context.Context, sample *VoiceSample) error {
	if err := sample.Validate(); err != nil {
		return stack.Wrap(err)
	}

	sample.ID = newID()
	sample.CreatedAt = time.Now().UTC()

	if _, err := r.store.global.NewInsert().Model(sample).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: add voice sample: %w", err))
	}
	return nil
}

// Select returns a voice's recordings, oldest first, which is the order they were given and so
// the order a provider should hear them in.
func (r *VoiceSamples) Select(ctx context.Context, voiceID string) ([]VoiceSample, error) {
	if voiceID == "" {
		return nil, stack.Wrap(errors.New("store: a voice id is required"))
	}

	var samples []VoiceSample
	err := r.store.global.NewSelect().Model(&samples).
		Where("voice_id = ?", voiceID).
		Order("created_at").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: voice samples: %w", err))
	}
	return samples, nil
}
