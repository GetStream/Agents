package llmrouter

import (
	"context"
	"errors"
	"fmt"
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

	endpoint := routing.Endpoint{BaseURL: stored.BaseURL, Model: stored.Model, Client: m.client}
	if len(stored.APIKeySealed) > 0 {
		if m.secrets == nil {
			return routing.ProviderConfig{}, routing.Endpoint{}, stack.Wrap(
				fmt.Errorf("%w: this deployment has no key to open it with", ErrCustomModelKey))
		}
		endpoint.APIKey, err = m.secrets.OpenWithAADVersion(stored.APIKeySealed, []byte(customerID), stored.KEKVersion)
		if err != nil {
			return routing.ProviderConfig{}, routing.Endpoint{}, fmt.Errorf("%w: %s: %w", ErrCustomModelKey, routing.CustomProvider+"/"+name, err)
		}
	}
	return CustomModelConfig(stored), endpoint, nil
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
