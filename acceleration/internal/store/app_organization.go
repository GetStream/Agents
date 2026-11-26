package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// AppOrganization records which organization an app was last seen under.
type AppOrganization struct {
	bun.BaseModel `bun:"table:app_organizations,alias:ao"`

	AppID          string    `bun:"app_id,pk"`
	OrganizationID string    `bun:"organization_id,notnull"`
	SeenAt         time.Time `bun:"seen_at,notnull"`
}

// Validate refuses a membership without an app or an organization.
func (m *AppOrganization) Validate() error {
	return validate("app organization").
		required("app_id", m.AppID).
		required("organization_id", m.OrganizationID).
		done()
}

// AppOrganizations are which organization each app was last seen under, in the shared
// database. Every request that resolves a policy reads its app's, so they are cached by app.
type AppOrganizations struct {
	store *Store
}

// Upsert records that an app was seen under an organization now, in a write that outlives ctx
// within the store's write timeout, and drops what every replica had cached of the app's
// (detachedWrite).
func (r *AppOrganizations) Upsert(ctx context.Context, membership *AppOrganization) error {
	if err := membership.Validate(); err != nil {
		return stack.Wrap(err)
	}
	membership.SeenAt = time.Now().UTC()
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().Model(membership).
			On("CONFLICT (app_id) DO UPDATE").
			Set("organization_id = EXCLUDED.organization_id").
			Set("seen_at = EXCLUDED.seen_at").
			Exec(ctx)
		return err
	}, func() []string {
		return []string{appOrganizationKey(membership.AppID), appPoliciesKey(membership.AppID)}
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: join organization: %w", err))
	}
	return nil
}

// Get returns the organization an app was last seen under, through the cache, or the zero
// membership for an app that has never named one.
func (r *AppOrganizations) Get(ctx context.Context, appID string) (AppOrganization, error) {
	return cached(ctx, r.store, appOrganizationKey(appID), func(ctx context.Context) (AppOrganization, error) {
		var membership AppOrganization
		err := r.store.global.NewSelect().Model(&membership).Where("app_id = ?", appID).Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return AppOrganization{}, nil
		}
		if err != nil {
			return AppOrganization{}, stack.Wrap(fmt.Errorf("store: organization of: %w", err))
		}
		return membership, nil
	})
}

// Holds reports whether an app is the organization's: last seen under it by a request, or,
// for an app never seen under any, registered under it.
func (r *AppOrganizations) Holds(ctx context.Context, organizationID, appID string) (bool, error) {
	if appID == "" || organizationID == "" {
		return false, nil
	}
	owner, err := r.OrganizationOf(ctx, appID)
	if err != nil {
		return false, err
	}
	return owner == organizationID, nil
}

// OrganizationOf is the organization an app was last seen under by a request, or, for an app
// never seen under any, the one it is registered under. It is empty for an app neither names.
func (r *AppOrganizations) OrganizationOf(ctx context.Context, appID string) (string, error) {
	seen, err := r.Get(ctx, appID)
	if err != nil {
		return "", err
	}
	if seen.OrganizationID != "" {
		return seen.OrganizationID, nil
	}
	registered, err := r.registered(ctx, []string{appID})
	if err != nil {
		return "", err
	}
	return registered[appID], nil
}

// OrganizationsOf is OrganizationOf of every app, read from Postgres rather than the cache, in
// at most two statements however many there are. An app neither names is left out.
func (r *AppOrganizations) OrganizationsOf(ctx context.Context, appIDs []string) (map[string]string, error) {
	var seen []AppOrganization
	err := r.store.global.NewSelect().Model(&seen).Column("app_id", "organization_id").
		Where("app_id IN (?)", bun.In(appIDs)).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: organizations of apps: %w", err))
	}
	owners := make(map[string]string, len(appIDs))
	for _, membership := range seen {
		owners[membership.AppID] = membership.OrganizationID
	}
	var unseen []string
	for _, app := range appIDs {
		if _, ok := owners[app]; !ok {
			unseen = append(unseen, app)
		}
	}
	if len(unseen) == 0 {
		return owners, nil
	}
	registered, err := r.registered(ctx, unseen)
	if err != nil {
		return nil, err
	}
	maps.Copy(owners, registered)
	return owners, nil
}

// registered is the organization each app is registered under, leaving out an app registered
// under none.
func (r *AppOrganizations) registered(ctx context.Context, appIDs []string) (map[string]string, error) {
	var apps []App
	err := r.store.global.NewSelect().Model(&apps).Column("id", "organization_id").Where("id IN (?)", bun.In(appIDs)).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: organization of app: %w", err))
	}
	owners := make(map[string]string, len(apps))
	for _, app := range apps {
		if app.OrganizationID != "" {
			owners[app.ID] = app.OrganizationID
		}
	}
	return owners, nil
}

// Apps is every app of an organization: its Members, and the app whose id is the
// organization's, for an app that is its own.
func (r *AppOrganizations) Apps(ctx context.Context, organizationID string) ([]string, error) {
	members, err := r.Members(ctx, organizationID)
	if err != nil {
		return nil, err
	}
	apps := []string{organizationID}
	for _, app := range members {
		if app != organizationID {
			apps = append(apps, app)
		}
	}
	return apps, nil
}

// Members is every app of an organization, in order: those requests last named it for, and
// those registered under it that no request named another for.
func (r *AppOrganizations) Members(ctx context.Context, organizationID string) ([]string, error) {
	// One statement, so an app a request records under the organization meanwhile is in one
	// half or the other.
	namedOther := r.store.global.NewSelect().Model((*AppOrganization)(nil)).
		ColumnExpr("1").
		Where("ao.app_id = ap.id AND ao.organization_id <> ?", organizationID)
	named := r.store.global.NewSelect().Model((*AppOrganization)(nil)).Column("app_id").Where("organization_id = ?", organizationID).
		Union(r.store.global.NewSelect().Model((*App)(nil)).
			Column("id").
			Where("ap.organization_id = ?", organizationID).
			Where("NOT EXISTS (?)", namedOther))
	var members []string
	err := r.store.global.NewSelect().TableExpr("(?) AS member", named).Column("member.app_id").OrderExpr("1").Scan(ctx, &members)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: organization apps: %w", err))
	}
	return members, nil
}

// ActivityApps is Apps for a read of what an organization did: the
// app whose id is the organization's counts only when the organization is the one the latest
// request named it for, or, when none did, the one it is registered under, so an app of
// another organization with that id is not told as this one's.
func (r *AppOrganizations) ActivityApps(ctx context.Context, organizationID string) ([]string, error) {
	apps, err := r.Apps(ctx, organizationID)
	if err != nil {
		return nil, err
	}
	var own bool
	err = r.store.global.NewRaw(`SELECT EXISTS (SELECT 1 FROM app_organizations WHERE app_id = ?0 AND organization_id = ?0)
  OR (NOT EXISTS (SELECT 1 FROM app_organizations WHERE app_id = ?0)
      AND NOT EXISTS (SELECT 1 FROM apps WHERE id = ?0 AND organization_id <> ?0))`, organizationID).Scan(ctx, &own)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: organization activity apps: %w", err))
	}
	if !own {
		// Apps puts the organization's own id first.
		return apps[1:], nil
	}
	return apps, nil
}

func appOrganizationKey(appID string) string { return cacheKey("app-organization", appID) }
