package llmrouter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/egress"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/custom"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// CustomModels resolves the models customers serve themselves, which a session names as
// custom/<name>.
type CustomModels struct {
	store   *store.Store
	secrets *auth.Sealer
	client  *http.Client
}

// NewCustomModels reads models from the store. secrets opens their keys and
// may be nil on a deployment that stores none. private lets an endpoint sit on a private
// address, which only a router nobody else's tenants share should allow.
func NewCustomModels(models *store.Store, secrets *auth.Sealer, private bool) *CustomModels {
	client := egress.NewClient(0, nil)
	if private {
		client = &http.Client{}
	}
	return &CustomModels{store: models, secrets: secrets, client: client}
}

// ErrCustomModelKey is a customer's model whose api key this deployment cannot open. It is the
// deployment's failure, not the caller's.
var ErrCustomModelKey = errors.New("llmrouter: the api key of a custom model cannot be opened")

// CustomModel returns how to route to the customer's model of that name. Naming a model the
// customer does not hold is a routing.ErrRequest; a key that cannot be opened is not, since
// the caller can do nothing about it.
func (m *CustomModels) CustomModel(ctx context.Context, customerID, name string) (routing.ProviderConfig, routing.Endpoint, error) {
	stored, err := m.store.CustomModels.GetByName(ctx, customerID, name)
	if errors.Is(err, store.ErrNoCustomModel) {
		return routing.ProviderConfig{}, routing.Endpoint{}, stack.Wrap(routing.Refused(
			fmt.Errorf("routing: %q is not one of this customer's models", routing.CustomProvider+"/"+name)))
	}
	if err != nil {
		return routing.ProviderConfig{}, routing.Endpoint{}, err
	}

	endpoint := routing.Endpoint{BaseURL: stored.BaseURL, Model: stored.Model, ContextWindow: stored.ContextWindow, Client: m.client}
	if len(stored.APIKeySealed) > 0 {
		if m.secrets == nil {
			return routing.ProviderConfig{}, routing.Endpoint{}, stack.Wrap(
				fmt.Errorf("%w: this deployment has no key to open it with", ErrCustomModelKey))
		}
		var legacy bool
		endpoint.APIKey, legacy, err = m.secrets.OpenWithLegacyAAD(stored.APIKeySealed,
			CustomModelKeyAAD(customerID, stored.ID), []byte(customerID), stored.KEKVersion, "custom_model")
		if err != nil {
			return routing.ProviderConfig{}, routing.Endpoint{}, fmt.Errorf("%w: %s: %w", ErrCustomModelKey, routing.CustomProvider+"/"+name, err)
		}
		if legacy {
			m.reseal(ctx, stored, endpoint.APIKey)
		}
	}
	return CustomModelConfig(stored), endpoint, nil
}

// reseal seals again bound to the model a key sealed bound only to its customer, as keys were
// before they were bound to their model, so that form is gone from every model in use. Failing
// to write it back costs nothing but another try on the next session, so it is logged and the
// session goes on.
func (m *CustomModels) reseal(ctx context.Context, stored store.CustomModel, key string) {
	sealed, err := m.secrets.SealWithAAD(key, CustomModelKeyAAD(stored.CustomerID, stored.ID))
	if err == nil {
		err = m.store.CustomModels.Reseal(ctx, stored, sealed, m.secrets.CurrentVersion())
	}
	if err != nil {
		slog.WarnContext(ctx, "could not seal a custom model's key again bound to the model",
			"customer", stored.CustomerID, "model", stored.ID, "error", err)
	}
}

// CustomModelKeyAAD binds a model's sealed api key to its customer and id, so a key copied to
// another model's row does not open. The id, not the name: a model can be renamed. Each part is
// length-prefixed, as pgsealed's credentialsAAD is, so no two models give the same bytes. v1
// changes with the layout.
func CustomModelKeyAAD(customerID, id string) []byte {
	return fmt.Appendf(nil, "accelerate:custom-model-key:v1:%d:%s:%d:%s", len(customerID), customerID, len(id), id)
}

// CustomModelConfig is what routing knows about a customer's model: everything but where
// it is and how to get in.
func CustomModelConfig(stored store.CustomModel) routing.ProviderConfig {
	return routing.ProviderConfig{
		Provider:        custom.ProviderName,
		Model:           stored.Name,
		Realtime:        true,
		InputModalities: stored.InputModalities,
		ContextWindow:   stored.ContextWindow,
		DataPolicy: options.DataHandling{
			TrainsOnData: options.Claim(stored.TrainsOnData),
			Retention:    options.Retention(stored.Retention),
		},
		Price: routing.Price{
			PerMillionInputTokens:  stored.PerMillionInputTokens,
			PerMillionOutputTokens: stored.PerMillionOutputTokens,
		},
	}
}
