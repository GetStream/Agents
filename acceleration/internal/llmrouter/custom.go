package llmrouter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
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
	// keys opens the keys sealed under an app's data key, and with sealing on moves each key that
	// opened under the router's key to the app's data key. It is nil when no KMS key wraps data
	// keys.
	keys   *appkeys.Keys
	client *http.Client
}

// KeyRecord is the record label of custom model keys in the appkeys counters.
const KeyRecord = "custom_model"

// init serves the custom_model series of the appkeys counters at 0 from the router's start, so
// dashboards and alerts find them before the first key is sealed or opened under the router's
// key.
func init() { appkeys.StartCounting(KeyRecord) }

// NewCustomModels reads models from the store. secrets opens their keys and may be nil on a
// deployment that stores none. keys opens the ones under an app's data key, and may be nil.
// private lets an endpoint sit on a private address, which only a router nobody else's tenants
// share should allow.
func NewCustomModels(models *store.Store, secrets *auth.Sealer, keys *appkeys.Keys, private bool) *CustomModels {
	client := egress.NewClient(0, nil)
	if private {
		// The default transport, with the response header cap every client of a tenant's server has.
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxResponseHeaderBytes = egress.MaxResponseHeaderBytes
		client = &http.Client{Transport: transport}
	}
	return &CustomModels{store: models, secrets: secrets, keys: keys, client: client}
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
		aad := customModelKeyAAD(customerID, stored.ID)
		dataKeyVersion := dataKeyVersion(stored)
		key, err := m.keys.OpenRecord(ctx, keyRecord(stored), dataKeyVersion, stored.APIKeySealed, aad, func() ([]byte, error) {
			key, err := m.secrets.OpenWithLegacyAAD(stored.APIKeySealed, aad, []byte(customerID), stored.KEKVersion, "custom_model")
			return []byte(key), err
		})
		// The error names the KMS key or a setting, so callers answer only that something went
		// wrong (ErrCustomModelKey is no refusal), and the cause goes to the log with the error.
		if err != nil {
			return routing.ProviderConfig{}, routing.Endpoint{}, fmt.Errorf("%w: %s: %w", ErrCustomModelKey, routing.CustomProvider+"/"+name, err)
		}
		endpoint.APIKey = string(key)
		if dataKeyVersion == nil {
			m.reseal(ctx, stored, key)
		}
	}
	return CustomModelConfig(stored), endpoint, nil
}

// SealKey seals key, the api key of model, and puts it in model with the version of the key that
// sealed it. With data key sealing on, it seals under the app's data key through keys, bound to
// the model, and falls back to sealer when KMS is unavailable (appkeys.Keys.SealRecord). Under
// sealer it is bound to the customer only, as before data keys, so a rollback still opens it.
// model must have its id, since the key is bound to it.
func SealKey(ctx context.Context, sealer *auth.Sealer, keys *appkeys.Keys, model *store.CustomModel, key string) error {
	sealed, err := keys.SealRecord(ctx, keyRecord(*model), []byte(key), customModelKeyAAD(model.CustomerID, model.ID),
		func() ([]byte, int, error) {
			blob, err := sealer.SealWithAAD(key, []byte(model.CustomerID))
			return blob, sealer.CurrentVersion(), err
		})
	if err != nil {
		return err
	}
	model.APIKeySealed, model.KEKVersion, model.DataKeyVersion = sealed.Blob, sealed.KEKVersion, sealed.DataKeyVersion
	return nil
}

// reseal moves a key that opened under the router's key to the app's data key when sealing is
// on. A failure is logged and leaves the row as it was, since the key opened and the session can
// still use it.
func (m *CustomModels) reseal(ctx context.Context, stored store.CustomModel, key []byte) {
	sealed, err := m.keys.ResealRecord(ctx, keyRecord(stored), key, customModelKeyAAD(stored.CustomerID, stored.ID))
	// ResealRecord seals nothing and returns no version with sealing off or no data keys.
	if err == nil && sealed.DataKeyVersion != nil {
		_, err = m.store.CustomModels.Reseal(ctx, stored, sealed.Blob, *sealed.DataKeyVersion)
	}
	if err != nil {
		slog.WarnContext(ctx, "left a custom model's key under the router's key, since it could not be sealed again "+
			"under the app's data key", "app_id", stored.CustomerID, "model_id", stored.ID, "error", err)
	}
}

// dataKeyVersion is the version of the app's data key that sealed the model's key, and nil when
// the router's key sealed it. A row with a data key version and a router key version is under
// the router's key: a build from before data keys put a new key over a row under the data key,
// and wrote the blob and kek_version but left the data key version as it was.
func dataKeyVersion(stored store.CustomModel) *int {
	if stored.KEKVersion != 0 {
		return nil
	}
	return stored.DataKeyVersion
}

// keyRecord is a model's key as the appkeys counters and log lines name it.
func keyRecord(model store.CustomModel) appkeys.Record {
	return appkeys.Record{Kind: KeyRecord, CustomerID: model.CustomerID, Attrs: []slog.Attr{slog.String("model_id", model.ID)}}
}

// customModelKeyAAD binds a model's sealed api key to its customer and id, so a key copied to
// another model's row does not open. The id, not the name: a model can be renamed. Each part is
// length-prefixed, as pgsealed's credentialsAAD is, so no two models give the same bytes. v1
// changes with the layout. A key under the app's data key is sealed with it, after the record's
// kind and app. A key under the router's key is still sealed bound to the customer only, and
// CustomModel tries this binding first there too.
func customModelKeyAAD(customerID, id string) []byte {
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
