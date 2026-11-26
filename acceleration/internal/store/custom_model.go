package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// CustomModel is a language model a customer serves themselves behind an OpenAI-compatible
// endpoint, which a session names as custom/<name>.
type CustomModel struct {
	bun.BaseModel `bun:"table:custom_models,alias:cm"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	Name       string `bun:"name,notnull"`
	// BaseURL is the endpoint root, up to and including /v1.
	BaseURL string `bun:"base_url,notnull"`
	// Model is the id the endpoint serves the weights under.
	Model string `bun:"model,notnull"`
	// APIKeySealed is the key the endpoint wants, sealed under KEKVersion and bound to the
	// customer. Nil for an endpoint that takes none.
	APIKeySealed    []byte   `bun:"api_key_sealed"`
	KEKVersion      int      `bun:"kek_version,notnull"`
	ContextWindow   int64    `bun:"context_window,notnull"`
	InputModalities []string `bun:"input_modalities,array"`
	// The prices are what the customer is billed by whoever serves the model, so usage can
	// say what a conversation cost. Zero records it as free.
	PerMillionInputTokens  float64   `bun:"per_million_input_tokens,notnull"`
	PerMillionOutputTokens float64   `bun:"per_million_output_tokens,notnull"`
	TrainsOnData           string    `bun:"trains_on_data,notnull"`
	Retention              string    `bun:"retention,notnull"`
	CreatedAt              time.Time `bun:"created_at,notnull"`
	UpdatedAt              time.Time `bun:"updated_at,notnull"`
}

// Validate refuses a model a session could not be routed to.
func (m *CustomModel) Validate() error {
	endpoint, err := url.Parse(m.BaseURL)
	return validate("custom model").
		required("customer_id", m.CustomerID).
		required("name", m.Name).
		maxLength("name", m.Name, 128).
		required("base_url", m.BaseURL).
		check(err == nil && (endpoint.Scheme == "https" || endpoint.Scheme == "http") && endpoint.Host != "", "base_url", "is not an http or https URL").
		required("model", m.Model).
		check(m.ContextWindow >= 0, "context_window", "is negative").
		nonNegative("per_million_input_tokens", m.PerMillionInputTokens).
		nonNegative("per_million_output_tokens", m.PerMillionOutputTokens).
		oneOf("trains_on_data", m.TrainsOnData, "", "yes", "no", "unknown").
		done()
}

// ErrNoCustomModel says a customer holds no model by that id or name.
var ErrNoCustomModel = errors.New("store: no such model")

const (
	defaultCustomModelLimit = 25
	maxCustomModelLimit     = 200
)

// CustomModelLimit is the page size a model list uses for the limit asked for. Select returns
// one row more than this, the same as the session queries.
func CustomModelLimit(asked int) int {
	return ClampLimit(asked, defaultCustomModelLimit, maxCustomModelLimit)
}

// CustomModelPosition is where a page of models ends.
type CustomModelPosition struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"id"`
}

// CustomModelFilter picks a page of a customer's models.
type CustomModelFilter struct {
	Limit int
	After *CustomModelPosition
}

// CustomModels are the models customers serve themselves, in the shared database. The one a
// session names is read on every session that opens, so it is cached by name.
type CustomModels struct {
	store *Store
}

// Insert stores a new model and fills in its id and timestamps.
func (r *CustomModels) Insert(ctx context.Context, model *CustomModel) error {
	if err := model.Validate(); err != nil {
		return stack.Wrap(err)
	}

	model.ID = newID()
	model.InputModalities = orEmpty(model.InputModalities)
	now := time.Now().UTC()
	model.CreatedAt = now
	model.UpdatedAt = now

	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().Model(model).Exec(ctx)
		return err
	}, func() []string { return []string{customModelKey(model.CustomerID, model.Name)} })
	if err != nil {
		if constraint(err) == "custom_models_name_idx" {
			return stack.Wrap(ErrNameTaken)
		}
		return stack.Wrap(fmt.Errorf("store: create model: %w", err))
	}
	return nil
}

// Update replaces everything about a model but its id, owner and creation, and forgets the
// name it had as well as the one it has.
func (r *CustomModels) Update(ctx context.Context, model *CustomModel) error {
	if model.ID == "" {
		return stack.Wrap(&ValidationError{Model: "custom model", Field: "id", Reason: "is required"})
	}
	if err := model.Validate(); err != nil {
		return stack.Wrap(err)
	}

	model.InputModalities = orEmpty(model.InputModalities)
	model.UpdatedAt = time.Now().UTC()
	// The name the row had is read in the same statement, locked, so the one forgotten is the
	// one a concurrent rename did not already replace.
	was := r.store.global.NewSelect().Model((*CustomModel)(nil)).Column("name").
		Where("id = ?", model.ID).
		Where("customer_id = ?", model.CustomerID).
		For("UPDATE")
	var wasName string
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		return tx.NewUpdate().Model(model).
			With("was", was).
			TableExpr("was").
			ExcludeColumn("id", "customer_id", "created_at").
			Where("cm.id = ?", model.ID).
			Where("cm.customer_id = ?", model.CustomerID).
			Returning("was.name").
			Scan(ctx, &wasName)
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if wasName == "" {
			return nil
		}
		return []string{
			customModelKey(model.CustomerID, model.Name),
			customModelKey(model.CustomerID, wasName),
		}
	})
	switch {
	case constraint(err) == "custom_models_name_idx":
		return stack.Wrap(ErrNameTaken)
	case errors.Is(err, sql.ErrNoRows):
		return stack.Wrap(fmt.Errorf("%w: %s", ErrNoCustomModel, model.ID))
	case err != nil:
		return stack.Wrap(fmt.Errorf("store: update model: %w", err))
	}
	return nil
}

// Delete removes a model. A session that names it afterwards is refused.
func (r *CustomModels) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a model id are required"))
	}

	var name string
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		return tx.NewDelete().Model((*CustomModel)(nil)).
			Where("id = ?", id).
			Where("customer_id = ?", customerID).
			Returning("name").
			Scan(ctx, &name)
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if name == "" {
			return nil
		}
		return []string{customModelKey(customerID, name)}
	})
	if errors.Is(err, sql.ErrNoRows) {
		return stack.Wrap(fmt.Errorf("%w: %s", ErrNoCustomModel, id))
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete model: %w", err))
	}
	return nil
}

// Get returns one of a customer's models by id.
func (r *CustomModels) Get(ctx context.Context, customerID, id string) (CustomModel, error) {
	return r.where(ctx, customerID, "id", id)
}

// GetByName returns the customer's model of that name, which is what a session names. It is
// read through the cache, its key staying sealed there as it is in Postgres.
func (r *CustomModels) GetByName(ctx context.Context, customerID, name string) (CustomModel, error) {
	if customerID == "" || name == "" {
		return r.where(ctx, customerID, "name", name)
	}
	return cached(ctx, r.store, customModelKey(customerID, name), func(ctx context.Context) (CustomModel, error) {
		return r.where(ctx, customerID, "name", name)
	})
}

// Select returns a page of a customer's models, newest first, one more than the limit.
func (r *CustomModels) Select(ctx context.Context, customerID string, filter CustomModelFilter) ([]CustomModel, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}

	var models []CustomModel
	query := r.store.global.NewSelect().Model(&models).Where("customer_id = ?", customerID)
	if filter.After != nil {
		query = query.Where("(created_at, id) < (?, ?)", filter.After.CreatedAt, filter.After.ID)
	}
	err := query.Order("created_at DESC", "id DESC").Limit(CustomModelLimit(filter.Limit) + 1).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: custom models: %w", err))
	}
	return models, nil
}

func (r *CustomModels) where(ctx context.Context, customerID, column, value string) (CustomModel, error) {
	if customerID == "" || value == "" {
		return CustomModel{}, stack.Wrap(errors.New("store: a customer and a model are required"))
	}

	var model CustomModel
	err := r.store.global.NewSelect().Model(&model).
		Where("customer_id = ?", customerID).
		Where("? = ?", bun.Ident(column), value).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return CustomModel{}, stack.Wrap(fmt.Errorf("%w: %s", ErrNoCustomModel, value))
	}
	if err != nil {
		return CustomModel{}, stack.Wrap(fmt.Errorf("store: custom model: %w", err))
	}
	return model, nil
}

func customModelKey(customerID, name string) string {
	return cacheKey("model-name", customerID, name)
}
