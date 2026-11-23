package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// APIKey is one credential belonging to an app. Several may be live at once, because
// rotation without downtime is: create the new key, deploy it, watch the old one go quiet,
// revoke it. One key per app makes every rotation an outage, so rotation stops happening.
type APIKey struct {
	bun.BaseModel `bun:"table:api_keys,alias:k"`

	ID     string `bun:"id,pk"`
	AppID  string `bun:"app_id,notnull"`
	Name   string `bun:"name,notnull"`
	Env    string `bun:"environment,notnull"`
	Sealed []byte `bun:"secret_sealed,notnull"`
	// KEKVersion names which key encryption key sealed this row.
	KEKVersion int        `bun:"kek_version,notnull"`
	Last4      string     `bun:"last4,notnull"`
	CreatedAt  time.Time  `bun:"created_at,notnull"`
	CreatedBy  string     `bun:"created_by,notnull"`
	ExpiresAt  *time.Time `bun:"expires_at"`
	LastUsedAt *time.Time `bun:"last_used_at"`
	RevokedAt  *time.Time `bun:"revoked_at"`
	RevokedBy  string     `bun:"revoked_by,notnull"`
}

// Validate refuses a key with no id, app or sealed secret. The id is minted by the caller,
// because the key is what the caller is handed and it has to be well formed.
func (k *APIKey) Validate() error {
	return validate("api key").
		required("id", k.ID).
		required("app_id", k.AppID).
		check(len(k.Sealed) > 0, "secret_sealed", "is required").
		done()
}

// APIKeyAAD binds a sealed key secret to its app and key id, the row it is stored on, so a blob
// copied onto another key's row, or a row whose app was changed, does not open. Each part is
// length-prefixed, as pgsealed's credentialsAAD is, so no two pairs give the same bytes. v1
// changes with the layout.
func APIKeyAAD(appID, keyID string) []byte {
	return fmt.Appendf(nil, "accelerate:api-key-secret:v1:%d:%s:%d:%s", len(appID), appID, len(keyID), keyID)
}

// unboundOpens counts the key secrets that opened only without APIKeyAAD: rows sealed before
// secrets were bound to their row. Each is sealed again with it on that use, so once this
// stays at zero the fallback in openSecret can be deleted.
var unboundOpens = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "router",
	Name:      "api_key_unbound_opens_total",
	Help:      "API key secrets that opened only without the AAD binding them to their row, each then sealed again with it",
})

// ErrNoAPIKey says no live key has that id. It is one error for every reason a key might
// not be usable — unknown, revoked, expired — because a caller that can tell those apart
// can find out which keys exist.
var ErrNoAPIKey = errors.New("store: no such api key")

// APIKeyOwner is the app a live key belongs to, the sealed secret to verify the caller's
// token with, and what that app has turned on. It is one row rather than three lookups
// because it is read on every authenticated request.
type APIKeyOwner struct {
	AppID          string      `bun:"app_id"`
	OrganizationID string      `bun:"organization_id"`
	Sealed         []byte      `bun:"secret_sealed"`
	KEKVersion     int         `bun:"kek_version"`
	Settings       AppSettings `bun:"settings,type:jsonb"`
	// ExpiresAt is when the key stops working, nil for one that never does. The query
	// already leaves a lapsed key out; it is carried so that a caller holding this row
	// for a while can tell when it stopped being true.
	ExpiresAt *time.Time `bun:"expires_at"`
}

// KeyUseInterval throttles how often a key's use is recorded. Writing on every request
// would double the writes of a busy key, and recording nothing means nobody can answer
// whether a key is still in use, so nobody ever revokes one.
const KeyUseInterval = time.Minute

// APIKeys are the apps' credentials, in the shared database. The owner of a live key is read
// on every authenticated request, so it is cached by key id.
type APIKeys struct {
	store *Store
	// touched is when this process last recorded each key's use, keyed by key id. It is per
	// process rather than shared because it guards a write nobody reads in anger.
	touched sync.Map
}

// Insert stores a credential and fills in its timestamp. A fresh id is one nothing can have
// cached, since a lookup that found no key caches nothing.
func (r *APIKeys) Insert(ctx context.Context, key *APIKey) error {
	if err := key.Validate(); err != nil {
		return stack.Wrap(err)
	}

	key.CreatedAt = time.Now().UTC()

	if _, err := r.store.global.NewInsert().Model(key).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: create api key: %w", err))
	}
	return nil
}

// GetLive returns the owner of the key with that id, provided it has not been revoked and has
// not expired. Every other outcome is ErrNoAPIKey.
//
// The expiry is checked here as well as in the query: a key cached while it was live must
// stop working when it lapses, not when the cached copy does.
func (r *APIKeys) GetLive(ctx context.Context, id string) (APIKeyOwner, error) {
	owner, err := cachedFor(ctx, r.store, cacheKey("key", id), keyCacheTTL, func(ctx context.Context) (APIKeyOwner, error) {
		var owner APIKeyOwner
		err := r.store.global.NewSelect().Model((*APIKey)(nil)).
			ColumnExpr("k.app_id, k.secret_sealed, k.kek_version, k.expires_at").
			ColumnExpr("ap.organization_id, ap.settings").
			Join("JOIN apps AS ap ON ap.id = k.app_id").
			Where("k.id = ?", id).
			Where("k.revoked_at IS NULL").
			Where("k.expires_at IS NULL OR k.expires_at > ?", time.Now().UTC()).
			Scan(ctx, &owner)
		if errors.Is(err, sql.ErrNoRows) {
			return APIKeyOwner{}, ErrNoAPIKey
		}
		if err != nil {
			return APIKeyOwner{}, stack.Wrap(fmt.Errorf("store: live api key: %w", err))
		}
		return owner, nil
	})
	if err != nil {
		return APIKeyOwner{}, err
	}
	if owner.ExpiresAt != nil && !owner.ExpiresAt.After(time.Now().UTC()) {
		return APIKeyOwner{}, stack.Wrap(ErrNoAPIKey)
	}
	return owner, nil
}

// Touch records that a key was used, but only when the last record is older than interval.
// This process remembers what it last wrote, so the common case is no query at all; the
// query refuses an older record too, for the other replicas' writes.
func (r *APIKeys) Touch(ctx context.Context, id string, interval time.Duration) error {
	now := time.Now().UTC()
	if last, ok := r.touched.Load(id); ok && now.Sub(last.(time.Time)) < interval {
		return nil
	}
	r.touched.Store(id, now)
	_, err := r.store.global.NewUpdate().Model((*APIKey)(nil)).
		Set("last_used_at = ?", now).
		Where("id = ?", id).
		Where("last_used_at IS NULL OR last_used_at < ?", now.Add(-interval)).
		Exec(ctx)
	if err != nil {
		r.touched.Delete(id)
		return stack.Wrap(fmt.Errorf("store: touch api key: %w", err))
	}
	return nil
}

// Revoke stops one of an app's keys working, here and on every other replica. A key of
// another app is ErrNoAPIKey. The row survives, because the audit trail of which key made
// which call has to outlive the key. told says whether the replicas' cached copy was
// dropped; when it was not, they take the key until that copy lapses.
func (r *APIKeys) Revoke(ctx context.Context, appID, id, by string) (told bool, err error) {
	now := time.Now().UTC()
	result, err := r.store.global.NewUpdate().Model((*APIKey)(nil)).
		Set("revoked_at = ?", now).
		Set("revoked_by = ?", by).
		Where("id = ?", id).
		Where("app_id = ?", appID).
		Where("revoked_at IS NULL").
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: revoke api key: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: revoke api key: %w", err))
	}
	if affected == 0 {
		return false, stack.Wrap(ErrNoAPIKey)
	}
	told = r.store.forget(ctx, cacheKey("key", id))
	r.touched.Delete(id)
	return told, nil
}

// Select returns an app's keys, revoked ones included, newest first.
func (r *APIKeys) Select(ctx context.Context, appID string) ([]APIKey, error) {
	var keys []APIKey
	err := r.store.global.NewSelect().Model(&keys).
		Where("app_id = ?", appID).
		Order("created_at DESC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: list api keys: %w", err))
	}
	return keys, nil
}

// Lookup resolves an API key to the app holding it, which is what the api_key mode of
// authentication is built on. The router and its test harness resolve a key the same way,
// down to the cache in front of it, so a measurement of what authentication costs measures
// the real thing.
func (r *APIKeys) Lookup(sealer *auth.Sealer) auth.Lookup {
	return func(ctx context.Context, key string) (auth.App, error) {
		// The shape of the key is checked before the database is, so a truncated paste
		// costs nothing to reject.
		if !auth.ValidKey(key) {
			return auth.App{}, auth.ErrUnauthenticated
		}
		owner, err := r.GetLive(ctx, key)
		if errors.Is(err, ErrNoAPIKey) {
			return auth.App{}, auth.ErrUnauthenticated
		}
		if err != nil {
			return auth.App{}, err
		}
		secret, err := r.openSecret(ctx, sealer, key, owner)
		if err != nil {
			return auth.App{}, stack.Wrap(fmt.Errorf("store: unseal key %s: %w", key, err))
		}
		if err := r.Touch(ctx, key, KeyUseInterval); err != nil {
			slog.Default().Debug("could not record key use", "key", key, "error", err)
		}
		// The app's settings came back on the same row, so which levels of end user it
		// admits costs nothing beyond the lookup that was already happening. They are
		// inverted on the way across because auth measures a caller against a zero value
		// in the three modes that resolve no app at all, and that zero value has to admit
		// everybody.
		return auth.App{
			OrganizationID: owner.OrganizationID,
			AppID:          owner.AppID,
			Secret:         secret,
			Levels: auth.Levels{
				NoAnonymous: !owner.Settings.AnonymousAllowed(),
				NoGuest:     !owner.Settings.GuestAllowed(),
			},
		}, nil
	}
}

// openSecret opens a key's secret with the AAD of its row. A row sealed before secrets were
// bound to their row has no AAD: it still opens, so no key stops working, and it is sealed
// again with the AAD, so the unbound form ends for every key still in use.
func (r *APIKeys) openSecret(ctx context.Context, sealer *auth.Sealer, id string, owner APIKeyOwner) (string, error) {
	aad := APIKeyAAD(owner.AppID, id)
	secret, err := sealer.OpenWithAADVersion(owner.Sealed, aad, owner.KEKVersion)
	if err == nil {
		return secret, nil
	}
	secret, unboundErr := sealer.OpenWithAADVersion(owner.Sealed, nil, owner.KEKVersion)
	if unboundErr != nil {
		return "", err
	}
	unboundOpens.Inc()
	r.reseal(ctx, sealer, id, owner.Sealed, secret, aad)
	return secret, nil
}

// reseal writes a secret opened without its AAD back sealed with it, under the current key
// version. It is a chore, not a condition of the request: the old seal still opens, so a
// failure is logged and the next use tries again. The write applies only while the row holds
// the blob that was opened, so a secret replaced meanwhile stays as it is.
func (r *APIKeys) reseal(ctx context.Context, sealer *auth.Sealer, id string, was []byte, secret string, aad []byte) {
	sealed, err := sealer.SealWithAAD(secret, aad)
	if err == nil {
		_, err = r.store.global.NewUpdate().Model((*APIKey)(nil)).
			Set("secret_sealed = ?", sealed).
			Set("kek_version = ?", sealer.CurrentVersion()).
			Where("id = ?", id).
			Where("secret_sealed = ?", was).
			Exec(ctx)
	}
	if err != nil {
		slog.WarnContext(ctx, "could not seal an api key's secret again with its row's binding", "key", id, "error", err)
		return
	}
	// The cached owner still holds the unbound blob. Dropped, the next lookup reads the new one
	// instead of opening the old one and writing again.
	r.store.forget(ctx, cacheKey("key", id))
	slog.InfoContext(ctx, "sealed an api key's secret again with its row's binding", "key", id)
}
