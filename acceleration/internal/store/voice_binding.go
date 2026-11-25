package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// What a provider has made of a voice's samples.
const (
	// VoicePending means preparation has been asked for but has not finished.
	VoicePending = "pending"
	// VoiceReady means the provider has the voice and will speak in it.
	VoiceReady = "ready"
	// VoiceFailed means the provider would not take it, and Error says why.
	VoiceFailed = "failed"
)

// VoiceBinding is what one provider made of a voice's samples.
type VoiceBinding struct {
	bun.BaseModel `bun:"table:voice_bindings,alias:vb"`

	ID       string `bun:"id,pk"`
	VoiceID  string `bun:"voice_id,notnull"`
	Provider string `bun:"provider,notnull"`
	// ExternalID is what the provider calls this voice, and what a session asks it for.
	ExternalID string `bun:"external_id,notnull"`
	// State is one of VoicePending, VoiceReady or VoiceFailed.
	State     string    `bun:"state,notnull"`
	Error     string    `bun:"error,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
	// SyncedAt is when this provider last came back with a voice that can be spoken in,
	// and nil until one does. UpdatedAt cannot answer that: it moves again when the
	// binding goes back to pending.
	SyncedAt *time.Time `bun:"synced_at,nullzero"`
}

// Validate refuses a binding the table would refuse.
func (b *VoiceBinding) Validate() error {
	return validate("voice binding").
		required("voice_id", b.VoiceID).
		required("provider", b.Provider).
		oneOf("state", b.State, VoicePending, VoiceReady, VoiceFailed).
		done()
}

// VoiceBindings are what providers made of voices, in the shared database. The id a provider
// knows a voice by is read on every utterance spoken in it, so it is cached.
type VoiceBindings struct {
	store *Store
}

// Upsert records what a provider made of a voice, replacing whatever it made of it before:
// two answers to which id to use would be one too many.
func (r *VoiceBindings) Upsert(ctx context.Context, binding *VoiceBinding) error {
	if err := binding.Validate(); err != nil {
		return stack.Wrap(err)
	}

	// A provider's refusal can echo the key it was sent, and a caller reads this back.
	binding.Error = SafeLogText(binding.Error)

	now := time.Now().UTC()
	binding.UpdatedAt = now
	if binding.ID == "" {
		binding.ID = newID()
		binding.CreatedAt = now
	}
	// Only a binding that can be spoken in has been synced. A pending one keeps whatever
	// the last successful preparation wrote, so re-preparing a voice does not make the
	// provider look as though it never had it.
	if binding.State == VoiceReady {
		binding.SyncedAt = &now
	}

	// The row has no customer of its own, and the key it is cached under names the voice's.
	var customerID string
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		return tx.NewInsert().Model(binding).
			On("CONFLICT (voice_id, provider) DO UPDATE").
			Set("external_id = EXCLUDED.external_id").
			Set("state = EXCLUDED.state").
			Set("error = EXCLUDED.error").
			Set("updated_at = EXCLUDED.updated_at").
			Set("synced_at = COALESCE(EXCLUDED.synced_at, vb.synced_at)").
			Returning("(SELECT v.customer_id FROM voices AS v WHERE v.id = vb.voice_id)").
			Scan(ctx, &customerID)
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if customerID == "" {
			return nil
		}
		return []string{voiceBindingKey(customerID, binding.VoiceID, binding.Provider)}
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save voice binding: %w", err))
	}
	return nil
}

// Select returns what every provider made of a voice.
func (r *VoiceBindings) Select(ctx context.Context, voiceID string) ([]VoiceBinding, error) {
	if voiceID == "" {
		return nil, stack.Wrap(errors.New("store: a voice id is required"))
	}

	var bindings []VoiceBinding
	err := r.store.global.NewSelect().Model(&bindings).
		Where("voice_id = ?", voiceID).
		Order("provider").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: voice bindings: %w", err))
	}
	return bindings, nil
}

// GetReady returns the id a provider knows a customer's voice by. A voice that was never
// prepared with this provider, or whose preparation failed, is reported as unknown: speaking
// in the wrong voice is worse than not speaking through this provider.
func (r *VoiceBindings) GetReady(ctx context.Context, customerID, voiceID, provider string) (string, error) {
	if customerID == "" || voiceID == "" || provider == "" {
		return "", stack.Wrap(errors.New("store: a customer, a voice and a provider are required"))
	}
	return cached(ctx, r.store, voiceBindingKey(customerID, voiceID, provider), func(ctx context.Context) (string, error) {
		var binding VoiceBinding
		err := r.store.global.NewSelect().Model(&binding).
			Join("JOIN voices AS v ON v.id = vb.voice_id").
			Where("vb.voice_id = ?", voiceID).
			Where("vb.provider = ?", provider).
			Where("vb.state = ?", VoiceReady).
			Where("v.customer_id = ?", customerID).
			Where("v.deleted_at IS NULL").
			Limit(1).
			Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return "", stack.Wrap(fmt.Errorf("%w: voice %s has no ready %s binding", ErrNoVoice, voiceID, provider))
		}
		if err != nil {
			return "", stack.Wrap(fmt.Errorf("store: ready voice binding: %w", err))
		}
		return binding.ExternalID, nil
	})
}

// Elsewhere reports a provider's voice id that another customer's voice was prepared as and
// none of this customer's was. A deleted voice still counts: the provider may hold the clone yet.
func (r *VoiceBindings) Elsewhere(ctx context.Context, customerID, provider, externalID string) (bool, error) {
	if provider == "" || externalID == "" {
		return false, nil
	}
	var elsewhere bool
	err := r.store.global.NewSelect().Model((*VoiceBinding)(nil)).
		ColumnExpr("COALESCE(bool_and(v.customer_id <> ?), false)", customerID).
		Join("JOIN voices AS v ON v.id = vb.voice_id").
		Where("vb.provider = ? AND vb.external_id = ?", provider, externalID).
		Scan(ctx, &elsewhere)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: voice binding elsewhere: %w", err))
	}
	return elsewhere, nil
}

// SelectByCustomer returns what every provider made of every voice of a customer, deleted
// ones included.
func (r *VoiceBindings) SelectByCustomer(ctx context.Context, customerID string) ([]VoiceBinding, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: a customer id is required"))
	}
	var bindings []VoiceBinding
	err := r.store.global.NewSelect().Model(&bindings).
		Join("JOIN voices AS v ON v.id = vb.voice_id").
		Where("v.customer_id = ?", customerID).
		Where("vb.external_id <> ''").
		Order("vb.provider", "vb.external_id").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer voice bindings: %w", err))
	}
	return bindings, nil
}

func voiceBindingKey(customerID, voiceID, provider string) string {
	return cacheKey("voice-binding", customerID, voiceID, provider)
}
