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
	// settings, so an app cannot set it on itself. Nil is no.
	InternalConnectors *bool `json:"internal_connectors,omitempty"`
	// AgentTeams is whether an end user opens only the agent configs of a team they are in,
	// by Chat's shared-team rule (api.teamsGate). Stream sets it, as it does
	// InternalConnectors. Nil is no: an app that never asked keeps every config open to
	// every user, as before the setting existed.
	AgentTeams *bool `json:"agent_teams,omitempty"`
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
// Stream writes settings in Postgres and no operation of the router does, so nothing forgets
// the key: a change is seen once the cache's TTL has passed, which is an hour. The settings
// an API key is read with (APIKeys.GetLive) are cached for a minute only.
func (r *Apps) GetSettings(ctx context.Context, appID string) (AppSettings, error) {
	if appID == "" {
		return AppSettings{}, stack.Wrap(errors.New("store: an app id is required"))
	}
	return cached(ctx, r.store, cacheKey("app-settings", appID), func(ctx context.Context) (AppSettings, error) {
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
