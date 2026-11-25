package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/shardmap"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// App is the tenant. Its id is what every other table's customer_id holds.
type App struct {
	bun.BaseModel `bun:"table:apps,alias:ap"`

	ID             string      `bun:"id,pk"`
	OrganizationID string      `bun:"organization_id,notnull"`
	Name           string      `bun:"name,notnull"`
	Settings       AppSettings `bun:"settings,type:jsonb,notnull"`
	CreatedAt      time.Time   `bun:"created_at,notnull"`
}

// AppSettings is what an app has turned on or off.
//
// Every field is a pointer because the document is sparse and the absent key has to mean
// something other than false. A key nobody has written is a feature nobody has expressed
// an opinion on, and reading that as "off" would turn every app that predates a setting
// into one that has opted out of it.
type AppSettings struct {
	// AllowAnonymous and AllowGuest are which levels of end user this app admits. Nil is
	// yes for both: an app that has never been configured takes everybody, and a
	// deployment turning authentication on for the first time must not lock its users
	// out on the way.
	AllowAnonymous *bool `json:"allow_anonymous,omitempty"`
	AllowGuest     *bool `json:"allow_guest,omitempty"`
	// InternalConnectors is whether the app sees the connectors whose manifest says internal
	// (visibleTo in connector_definition.go). Stream sets it on its own apps; no operation writes
	// it, so an app cannot set it on itself. Nil is no.
	InternalConnectors *bool `json:"internal_connectors,omitempty"`
	// AgentTeams is whether an end user opens only the agent configs of a team they are in,
	// by Chat's shared-team rule (api.teamsGate). Stream sets it, as it does
	// InternalConnectors. Nil is no: an app that never asked keeps every config open to
	// every user, as before the setting existed.
	AgentTeams *bool `json:"agent_teams,omitempty"`
	// RoutingPolicy is which shards the app's work may be placed in, a shardmap.Policy, unless
	// its agent sets its own. Empty is shardmap.PolicyClosest. The app writes it itself
	// (SetRoutingPolicy).
	RoutingPolicy string `json:"routing_policy,omitempty"`
}

// AgentTeamsChecked reports whether the app keeps each team's agent configs to its own users.
func (s AppSettings) AgentTeamsChecked() bool {
	return s.AgentTeams != nil && *s.AgentTeams
}

// AnonymousAllowed reports whether the app takes callers who presented no token.
func (s AppSettings) AnonymousAllowed() bool {
	return s.AllowAnonymous == nil || *s.AllowAnonymous
}

// GuestAllowed reports whether the app takes callers holding a guest token.
func (s AppSettings) GuestAllowed() bool {
	return s.AllowGuest == nil || *s.AllowGuest
}

// Validate refuses an app outside an organization or with no name.
func (a *App) Validate() error {
	return validate("app").
		required("organization_id", a.OrganizationID).
		required("name", a.Name).
		done()
}

// ErrNoApp says no app has that id.
var ErrNoApp = errors.New("store: no such app")

// Apps are the tenants, in the shared database. An app's settings are read on every session
// an end user starts, so they are cached.
type Apps struct {
	store *Store
	// guests is what GuestAllowed read, in a router with no Redis to hold it.
	guests *seen
}

// Insert stores a new app and fills in its timestamp. An id the caller already chose is
// kept, which is what moving a deployment needs: every other table's customer_id holds the
// app id, so the rows only mean the same thing somewhere else if the app does.
func (r *Apps) Insert(ctx context.Context, app *App) error {
	if err := app.Validate(); err != nil {
		return stack.Wrap(err)
	}

	if app.ID == "" {
		app.ID = newID()
	}
	app.CreatedAt = time.Now().UTC()

	if _, err := r.store.global.NewInsert().Model(app).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: create app: %w", err))
	}
	return nil
}

// Get returns one app, or ErrNoApp.
func (r *Apps) Get(ctx context.Context, id string) (App, error) {
	var app App
	err := r.store.global.NewSelect().Model(&app).Where("id = ?", id).Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return App{}, stack.Wrap(ErrNoApp)
	}
	if err != nil {
		return App{}, stack.Wrap(fmt.Errorf("store: app: %w", err))
	}
	return app, nil
}

// GetSettings returns what an app has turned on or off, through the cache.
//
// An app with no row gets the zero settings, which admit everybody, rather than an error.
// That is the same reading the authenticator takes: a deployment naming a customer without
// having written an app row for it must not have every end user turned away because of it.
// Stream writes settings in Postgres, and nothing forgets the key of such a change: it is seen
// once the cache's TTL has passed, which is an hour. SetRoutingPolicy forgets it. The settings
// an API key is read with (APIKeys.GetLive) are cached for a minute only.
func (r *Apps) GetSettings(ctx context.Context, appID string) (AppSettings, error) {
	if appID == "" {
		return AppSettings{}, stack.Wrap(errors.New("store: an app id is required"))
	}
	return cached(ctx, r.store, cacheKey("app-settings", appID), func(ctx context.Context) (AppSettings, error) {
		return r.readSettings(ctx, appID)
	})
}

func (r *Apps) readSettings(ctx context.Context, appID string) (AppSettings, error) {
	var app App
	err := r.store.global.NewSelect().Model(&app).
		Column("settings").
		Where("id = ?", appID).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AppSettings{}, nil
	}
	if err != nil {
		return AppSettings{}, stack.Wrap(fmt.Errorf("store: app settings: %w", err))
	}
	return app.Settings, nil
}

// SetRoutingPolicy writes an app's routing policy, empty for shardmap.PolicyClosest, and leaves
// the rest of its settings as they are, dropping what every replica had cached of them
// (detachedWrite). ErrNoApp is an app with no row.
func (r *Apps) SetRoutingPolicy(ctx context.Context, appID string, policy shardmap.Policy) error {
	if err := validate("app").check(policy == "" || policy.Valid(), "routing_policy", "is not a routing policy").done(); err != nil {
		return stack.Wrap(err)
	}
	var found bool
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		update := tx.NewUpdate().Model((*App)(nil)).Where("id = ?", appID)
		// A change of the one key, so a concurrent write of another setting is kept.
		if policy == "" {
			update.Set("settings = settings - 'routing_policy'")
		} else {
			update.Set("settings = settings || jsonb_build_object('routing_policy', ?::text)", string(policy))
		}
		result, err := update.Exec(ctx)
		if err != nil {
			return err
		}
		updated, err := result.RowsAffected()
		found = updated > 0
		return err
	}, func() []string {
		if !found {
			return nil
		}
		return []string{cacheKey("app-settings", appID)}
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: app routing policy: %w", err))
	}
	if !found {
		return stack.Wrap(ErrNoApp)
	}
	return nil
}

// RoutingPolicy is the policy work for customerID under configID is placed by: the agent's when
// it sets one, else the app's (shardmap.Effective). configID "" reads the app's alone. Both are
// read through their caches (GetSettings, AgentConfigs.Get).
func (r *Apps) RoutingPolicy(ctx context.Context, customerID, configID string) (shardmap.Policy, error) {
	var agentPolicy string
	if configID != "" {
		config, err := r.store.AgentConfigs.Get(ctx, customerID, configID)
		if err != nil {
			return "", err
		}
		agentPolicy = config.RoutingPolicy
	}
	if agentPolicy != "" {
		return shardmap.Policy(agentPolicy), nil
	}
	settings, err := r.GetSettings(ctx, customerID)
	if err != nil {
		return "", err
	}
	return shardmap.Effective(settings.RoutingPolicy, ""), nil
}

// GuestAllowed reports whether an app takes guests, as its settings say and GetSettings reads
// them: an app with no row does. It is asked on each request of a guest's behind a proxy, so it
// is held for a minute, as a key's levels are (APIKeys.GetLive), and that long in this router
// with no Redis.
func (r *Apps) GuestAllowed(ctx context.Context, appID string) (bool, error) {
	if appID == "" {
		return false, stack.Wrap(errors.New("store: an app id is required"))
	}
	return cachedFact(ctx, r.store, r.guests, appID, "", cacheKey("app-guests", appID), keyCacheTTL, keyCacheTTL,
		func(ctx context.Context) (bool, error) {
			settings, err := r.readSettings(ctx, appID)
			return settings.GuestAllowed(), err
		})
}

// Known is which of ids this deployment knows: an app registered here, or one a request
// named with its organization. An id it has never seen is another deployment's.
func (r *Apps) Known(ctx context.Context, ids []string) (map[string]bool, error) {
	known := make(map[string]bool, len(ids))
	if len(ids) == 0 {
		return known, nil
	}
	var found []string
	seen := r.store.global.NewSelect().Model((*AppOrganization)(nil)).Column("app_id").Where("app_id IN (?)", bun.In(ids))
	err := r.store.global.NewSelect().Model((*App)(nil)).Column("id").Where("id IN (?)", bun.In(ids)).
		Union(seen).
		Scan(ctx, &found)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: known apps: %w", err))
	}
	for _, id := range found {
		known[id] = true
	}
	return known, nil
}
