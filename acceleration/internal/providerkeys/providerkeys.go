// Package providerkeys seals and opens the keys customers keep with the router for their own
// accounts at a vendor, one per vendor and region (BYOK). Work routed on one is billed to the
// customer by the vendor, and by the router as BYOK.
//
// A key is sealed under the app's data key when data key sealing is on (appkeys), and under the
// deployment's key encryption key otherwise, bound to its customer, vendor and region. It leaves
// this package opened only for the vendor it is the key of: it is never returned by the API,
// logged or put in an error.
package providerkeys

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"unicode"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// Record is the record label of provider keys in the appkeys counters.
const Record = "provider_key"

// Dashboards and alerts on the appkeys counters find a provider_key series from the router's
// start, before any key falls back or opens under the router's key.
func init() { appkeys.StartCounting(Record) }

// record is a customer's key for provider in region, as the appkeys counters and log lines name it.
func record(customerID, provider, region string) appkeys.Record {
	return appkeys.Record{Kind: Record, CustomerID: customerID,
		Attrs: []slog.Attr{slog.String("provider", provider), slog.String("region", region)}}
}

// The vendors a customer may keep a key for beside the language models whose factories build
// with one (routing.OverrideAPIKey): knowledge is kept at turbopuffer, and code runs at Daytona.
const (
	Turbopuffer = "turbopuffer"
	Daytona     = "daytona"
)

// minHinted is the shortest key a hint is given for: of a shorter one, four characters are too
// much of it. Every vendor's key that is taken is longer.
const minHinted = 16

// MaxKeyLength is the longest key that is kept. The longest a vendor issues today is a few
// hundred characters.
const MaxKeyLength = 1024

// ErrMalformedKey is a key with no character, one longer than MaxKeyLength, or one holding a
// space, a comma or a control character, which no vendor issues. A comma is refused because a
// vendor's client reads a list of keys out of one string, and a key that is only commas reads
// as none, which some clients answer with the deployment's own.
var ErrMalformedKey = errors.New("providerkeys: a key is up to 1024 characters, with no space, comma or control character")

// AAD binds a sealed key to its row, so a key copied to another customer's, vendor's or region's
// row does not open. Each string is length-prefixed, so no two rows split the same bytes two
// ways. v1 changes with the layout.
func AAD(customerID, provider, region string) []byte {
	return fmt.Appendf(nil, "accelerate:provider-key:v1:%d:%s:%d:%s:%d:%s",
		len(customerID), customerID, len(provider), provider, len(region), region)
}

// Valid reports whether a key is one that is kept (ErrMalformedKey).
func Valid(key string) bool {
	if key == "" || len(key) > MaxKeyLength {
		return false
	}
	return !strings.ContainsFunc(key, func(r rune) bool { return r == ',' || unicode.IsSpace(r) || unicode.IsControl(r) })
}

// Seal is the row a customer's key for a provider in a region is kept as, its hint and the key
// that sealed it filled in. With data key sealing on, keys seals it under the app's data key,
// and sealer seals it when KMS is unavailable (appkeys.Keys.SealRecord). A nil keys seals with
// sealer alone.
func Seal(ctx context.Context, sealer *auth.Sealer, keys *appkeys.Keys, customerID, provider, region, key string) (store.ProviderKey, error) {
	if sealer == nil {
		return store.ProviderKey{}, stack.Wrap(errors.New("providerkeys: this deployment has no key to seal a provider key with"))
	}
	if !Valid(key) {
		return store.ProviderKey{}, stack.Wrap(ErrMalformedKey)
	}
	aad := AAD(customerID, provider, region)
	sealed, err := keys.SealRecord(ctx, record(customerID, provider, region), []byte(key), aad, func() ([]byte, int, error) {
		blob, err := sealer.SealWithAAD(key, aad)
		return blob, sealer.CurrentVersion(), err
	})
	if err != nil {
		return store.ProviderKey{}, err
	}
	return store.ProviderKey{
		CustomerID: customerID, Provider: provider, Region: region,
		APIKeySealed: sealed.Blob, KEKVersion: sealed.KEKVersion, DataKeyVersion: sealed.DataKeyVersion, KeyHint: Hint(key),
	}, nil
}

// Open is the key a row keeps. A row whose key was sealed for another row does not open. A row
// under the app's data key that cannot be opened because KMS is unavailable, or because keys is
// nil, fails with routing.ErrCustomerKeyUnavailable. The error names the row, never the key.
func Open(ctx context.Context, sealer *auth.Sealer, keys *appkeys.Keys, row store.ProviderKey) (string, error) {
	if sealer == nil {
		return "", stack.Wrap(errors.New("providerkeys: this deployment has no key to open a provider key with"))
	}
	aad := AAD(row.CustomerID, row.Provider, row.Region)
	key, err := keys.OpenRecord(ctx, record(row.CustomerID, row.Provider, row.Region), row.DataKeyVersion, row.APIKeySealed, aad,
		func() ([]byte, error) {
			key, err := sealer.OpenWithAADVersion(row.APIKeySealed, aad, row.KEKVersion)
			return []byte(key), err
		})
	if errors.Is(err, appkeys.ErrUnavailable) || errors.Is(err, appkeys.ErrNoKeys) {
		return "", stack.Wrap(fmt.Errorf("providerkeys: the %s key kept for %s does not open: %w: %w",
			row.Provider, row.Region, routing.ErrCustomerKeyUnavailable, err))
	}
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("providerkeys: the %s key kept for %s does not open: %w", row.Provider, row.Region, err))
	}
	return string(key), nil
}

// Hint is the last four characters of a key, so one can be told from another, and empty for a
// key too short to show any of.
func Hint(key string) string {
	runes := []rune(key)
	if len(runes) < minHinted {
		return ""
	}
	return string(runes[len(runes)-4:])
}

// Source is where a customer's sealed keys are read: *store.ProviderKeys, through its cache.
type Source interface {
	SelectAll(ctx context.Context, customerID string) ([]store.ProviderKey, error)
	// Reseal writes a key sealed again under the app's data key over the row it opened from, and
	// false when the row changed since it was read.
	Reseal(ctx context.Context, opened store.ProviderKey, sealed []byte, dataKeyVersion int) (bool, error)
}

// Keyring opens the keys customers keep, for the work routed on them. It is a
// routing.CustomerKeys.
//
// It holds no opened key. Each is read through its source, which for an *store.Store is the
// copy rueidis keeps in this process, dropped on every replica when a key is put or deleted,
// and opened when work is routed on it: a cached read and one AES-GCM open per vendor and region
// a request could go to.
type Keyring struct {
	source Source
	sealer *auth.Sealer
	// keys opens the keys sealed under an app's data key, and with sealing on moves each key
	// that opened under the router's key to the app's data key. It is nil when no KMS key wraps
	// data keys.
	keys *appkeys.Keys
}

// NewKeyring opens the keys source holds with sealer and, for the ones under an app's data key,
// with keys. A nil keys opens with sealer alone.
func NewKeyring(source Source, sealer *auth.Sealer, keys *appkeys.Keys) *Keyring {
	return &Keyring{source: source, sealer: sealer, keys: keys}
}

// CustomerKey is the key a customer keeps for a provider in a region, and false when they keep
// none there. A key under the router's key is sealed again under the app's data key when
// sealing is on. If that fails, the key is still returned and the row stays as it was.
func (k *Keyring) CustomerKey(ctx context.Context, customerID, provider, region string) (string, bool, error) {
	if customerID == "" {
		return "", false, nil
	}
	keys, err := k.source.SelectAll(ctx, customerID)
	if err != nil {
		return "", false, err
	}
	for _, row := range keys {
		if row.Provider != provider || row.Region != region {
			continue
		}
		key, err := Open(ctx, k.sealer, k.keys, row)
		if err != nil {
			return "", false, err
		}
		if row.DataKeyVersion == nil {
			k.reseal(ctx, row, key)
		}
		return key, true, nil
	}
	return "", false, nil
}

// reseal moves a key that opened under the router's key to the app's data key when sealing is
// on. A failure is logged and leaves the row as it was, since the key opened and the work can
// still use it.
func (k *Keyring) reseal(ctx context.Context, row store.ProviderKey, key string) {
	sealed, err := k.keys.ResealRecord(ctx, record(row.CustomerID, row.Provider, row.Region), []byte(key),
		AAD(row.CustomerID, row.Provider, row.Region))
	// ResealRecord seals nothing and returns no version with sealing off or no data keys.
	if err == nil && sealed.DataKeyVersion != nil {
		_, err = k.source.Reseal(ctx, row, sealed.Blob, *sealed.DataKeyVersion)
	}
	if err != nil {
		slog.WarnContext(ctx, "left a provider key under the router's key, since it could not be sealed again "+
			"under the app's data key", "app_id", row.CustomerID, "provider", row.Provider, "region", row.Region, "error", err)
	}
}
