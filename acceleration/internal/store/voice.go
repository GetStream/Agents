package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/redis/rueidis/rueidisaside"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Voice is a voice a customer brought with them, as opposed to one from a provider's own
// library.
//
// What a session asks for is this row rather than a provider's id for it, because the
// router fails over between providers mid-call and an id one provider knows means nothing
// to the next. Which id it means is worked out once the provider is chosen.
type Voice struct {
	bun.BaseModel `bun:"table:voices,alias:v"`

	ID          string     `bun:"id,pk"`
	CustomerID  string     `bun:"customer_id,notnull"`
	Name        string     `bun:"name,notnull"`
	Description string     `bun:"description,notnull"`
	CreatedAt   time.Time  `bun:"created_at,notnull"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull"`
	DeletedAt   *time.Time `bun:"deleted_at"`
}

// Validate refuses a voice the table would refuse.
func (v *Voice) Validate() error {
	return validate("voice").
		required("customer_id", v.CustomerID).
		required("name", v.Name).
		done()
}

// ErrNoVoice says a customer holds no such voice. It is a sentinel because asking for a
// voice by a name that turns out to be a provider's own is ordinary, and a caller has to
// be able to tell that apart from the database being down.
var ErrNoVoice = errors.New("store: no such voice")

// Voices are the voices customers brought with them, in the shared database. A voice is read
// on every session that speaks in it, by id or by name, so both are cached.
type Voices struct {
	store *Store
}

// Insert stores a new voice and fills in its id and timestamps.
func (r *Voices) Insert(ctx context.Context, voice *Voice) error {
	if err := voice.Validate(); err != nil {
		return stack.Wrap(err)
	}

	voice.ID = newID()
	now := time.Now().UTC()
	voice.CreatedAt = now
	voice.UpdatedAt = now
	voice.DeletedAt = nil

	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().Model(voice).Exec(ctx)
		return err
	}, func() []string { return []string{voiceNameKey(voice.CustomerID, voice.Name)} })
	if err != nil {
		if constraint(err) == "voices_name_idx" {
			return stack.Wrap(ErrNameTaken)
		}
		return stack.Wrap(fmt.Errorf("store: create voice: %w", err))
	}
	return nil
}

// Update renames or re-describes a voice a customer holds, and forgets the name it had as
// well as the one it has. The samples and the bindings are not touched: they are what the
// voice sounds like, not what it is called.
func (r *Voices) Update(ctx context.Context, voice *Voice) error {
	if voice.ID == "" {
		return stack.Wrap(&ValidationError{Model: "voice", Field: "id", Reason: "is required"})
	}
	if err := voice.Validate(); err != nil {
		return stack.Wrap(err)
	}

	voice.UpdatedAt = time.Now().UTC()
	// The name the row had is read in the same statement, locked, so the one forgotten is the
	// one a concurrent rename did not already replace.
	was := r.store.global.NewSelect().Model((*Voice)(nil)).Column("name").
		Where("id = ?", voice.ID).
		Where("customer_id = ?", voice.CustomerID).
		Where("deleted_at IS NULL").
		For("UPDATE")
	var wasName string
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		return tx.NewUpdate().Model(voice).
			With("was", was).
			TableExpr("was").
			Column("name", "description", "updated_at").
			Where("v.id = ?", voice.ID).
			Where("v.customer_id = ?", voice.CustomerID).
			Returning("was.name").
			Scan(ctx, &wasName)
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if wasName == "" {
			return nil
		}
		return []string{
			voiceKey(voice.CustomerID, voice.ID),
			voiceNameKey(voice.CustomerID, voice.Name),
			voiceNameKey(voice.CustomerID, wasName),
		}
	})
	switch {
	case constraint(err) == "voices_name_idx":
		return stack.Wrap(ErrNameTaken)
	case errors.Is(err, sql.ErrNoRows):
		return unknownVoice(voice.ID)
	case err != nil:
		return stack.Wrap(fmt.Errorf("store: update voice: %w", err))
	}
	return nil
}

// Delete marks a voice as gone, and forgets with it the bindings that spoke in it. The row
// stays, because the calls that spoke in it still name it.
func (r *Voices) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a voice id are required"))
	}

	var name string
	var providers []string
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		return tx.NewUpdate().Model((*Voice)(nil)).
			Set("deleted_at = ?", time.Now().UTC()).
			Where("id = ?", id).
			Where("customer_id = ?", customerID).
			Where("deleted_at IS NULL").
			Returning("name, ARRAY(SELECT vb.provider FROM voice_bindings AS vb WHERE vb.voice_id = v.id)").
			Scan(ctx, &name, pgdialect.Array(&providers))
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if name == "" {
			return nil
		}
		keys := []string{voiceKey(customerID, id), voiceNameKey(customerID, name)}
		for _, provider := range providers {
			keys = append(keys, voiceBindingKey(customerID, id, provider))
		}
		return keys
	})
	if errors.Is(err, sql.ErrNoRows) {
		return unknownVoice(id)
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete voice: %w", err))
	}
	return nil
}

// Get returns one voice a customer holds.
func (r *Voices) Get(ctx context.Context, customerID, id string) (Voice, error) {
	if customerID == "" || id == "" {
		return Voice{}, stack.Wrap(errors.New("store: a customer and a voice id are required"))
	}
	return cached(ctx, r.store, voiceKey(customerID, id), func(ctx context.Context) (Voice, error) {
		return r.where(ctx, customerID, "id", id)
	})
}

// GetByName returns the voice a customer calls this. It is what lets a session name a voice
// the way a person would rather than by id. A name the customer has no voice by is cached as
// that too, for missTTL, since it is usually one of a provider's own.
func (r *Voices) GetByName(ctx context.Context, customerID, name string) (Voice, error) {
	if customerID == "" || name == "" {
		return Voice{}, stack.Wrap(errors.New("store: a customer and a voice name are required"))
	}
	answer, err := cached(ctx, r.store, voiceNameKey(customerID, name), func(ctx context.Context) (found[Voice], error) {
		voice, err := r.where(ctx, customerID, "name", name)
		if errors.Is(err, ErrNoVoice) {
			rueidisaside.OverrideCacheTTL(ctx, missTTL)
			return found[Voice]{}, nil
		}
		return found[Voice]{Value: voice, Found: err == nil}, err
	})
	if err != nil {
		return Voice{}, err
	}
	if !answer.Found {
		return Voice{}, unknownVoice(name)
	}
	return answer.Value, nil
}

// Select returns every live voice a customer holds, newest first. It is not paged: a customer
// holds few, and the API lists them all.
func (r *Voices) Select(ctx context.Context, customerID string) ([]Voice, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}

	var voices []Voice
	err := r.store.global.NewSelect().Model(&voices).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Order("created_at DESC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer voices: %w", err))
	}
	return voices, nil
}

func (r *Voices) where(ctx context.Context, customerID, column, value string) (Voice, error) {
	var voice Voice
	err := r.store.global.NewSelect().Model(&voice).
		Where("customer_id = ?", customerID).
		Where("? = ?", bun.Ident(column), value).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Voice{}, unknownVoice(value)
	}
	if err != nil {
		return Voice{}, stack.Wrap(fmt.Errorf("store: voice: %w", err))
	}
	return voice, nil
}

func unknownVoice(id string) error {
	return stack.Wrap(fmt.Errorf("%w: %s", ErrNoVoice, id))
}

func voiceKey(customerID, id string) string {
	return cacheKey("voice", customerID, id)
}

func voiceNameKey(customerID, name string) string {
	return cacheKey("voice-name", customerID, name)
}
