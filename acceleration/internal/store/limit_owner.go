package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// appOwnerPrefix starts the owner of an app in no organization. No id holds a NUL.
const appOwnerPrefix = "\x00app\x00"

// AppOwner is who limits, resets and grants belong to for an app in no organization: the app
// itself, which is not the organization of the same id. An organization's owner is its id.
func AppOwner(appID string) string { return appOwnerPrefix + appID }

// LimitOwner is the owner of an organization's rows, or with billed false of those of the app in
// no organization whose id it is.
func LimitOwner(id string, billed bool) string {
	if billed {
		return id
	}
	return AppOwner(id)
}

// OwnerID is the id an owner's rows hold as organization_id, and whether it is an
// organization's, which the usage service bills.
func OwnerID(owner string) (id string, billed bool) {
	app, own := strings.CutPrefix(owner, appOwnerPrefix)
	return app, !own
}

// ownedRow is a limit, a reset or a grant: the id of who it belongs to, and whether that is an
// organization, an app in none, or not known (nil), as a row is that a release from before the
// column wrote and AttributeLimits has not attributed.
type ownedRow interface {
	owner() (id string, billed *bool)
}

func (l Limit) owner() (string, *bool)      { return l.OrganizationID, l.Billed }
func (r UsageReset) owner() (string, *bool) { return r.OrganizationID, r.Billed }
func (g LimitGrant) owner() (string, *bool) { return g.OrganizationID, g.Billed }

// OwnedBy keeps the rows known to be the owner's, which it may read and change.
func OwnedBy[T ownedRow](owner string, rows []T) []T {
	owned := []T{}
	for _, row := range rows {
		if id, billed := row.owner(); billed != nil && LimitOwner(id, *billed) == owner {
			owned = append(owned, row)
		}
	}
	return owned
}

// HeldTo keeps the rows the owner's work is held to: its own, and those of its id not known to
// be another's, so no limit lapses while whose it is is not known.
func HeldTo[T ownedRow](owner string, rows []T) []T {
	held := []T{}
	for _, row := range rows {
		id, billed := row.owner()
		if ownerID, ownerBilled := OwnerID(owner); id == ownerID && (billed == nil || *billed == ownerBilled) {
			held = append(held, row)
		}
	}
	return held
}

// Holders are the owners whose work the row holds: both of its id when it is not known whose it is.
func Holders[T ownedRow](row T) []string {
	id, billed := row.owner()
	if billed == nil {
		return []string{id, AppOwner(id)}
	}
	return []string{LimitOwner(id, *billed)}
}

// whereOwner keeps the owner's rows, or with unattributed also those of its id not known to be
// anyone's, which only staff are shown.
func whereOwner(query *bun.SelectQuery, owner string, unattributed bool) *bun.SelectQuery {
	id, billed := OwnerID(owner)
	query = query.Where("organization_id = ?", id)
	if unattributed {
		return query.Where("billed = ? OR billed IS NULL", billed)
	}
	return query.Where("billed = ?", billed)
}

// organizationOwned is billed as a write keeps it: a row that does not say is an organization's.
func organizationOwned(billed *bool) *bool {
	if billed != nil {
		return billed
	}
	yes := true
	return &yes
}

// attributeLimits attributes the rows of one table, whose audit entry is named by its prefix and
// its id, to who wrote them, where the audit log and what is recorded of the app say so:
//   - one another app wrote, or staff for an organization the router knows, is the organization's;
//   - one the app of that id wrote is the app's, or its organization's when it is the one the app
//     is recorded or registered under.
//
// Any other is left, as is an app's that is recorded under another organization, since it may
// have been in the one of its id when it wrote. An app's is not attributed for a minute, in
// which the organization its request named is recorded.
const attributeLimits = `
UPDATE ?4 AS row SET billed = said.billed
FROM (
    SELECT r.id, CASE
        WHEN al.source = ?0 THEN CASE WHEN EXISTS (SELECT 1 FROM app_organizations o WHERE o.organization_id = r.organization_id)
            OR EXISTS (SELECT 1 FROM organizations o WHERE o.id = r.organization_id) THEN true END
        WHEN al.customer_id <> r.organization_id THEN true
        WHEN r.created_at > now() - interval '1 minute' THEN NULL
        WHEN named.organization_id = r.organization_id THEN true
        WHEN named.organization_id IS NULL THEN false
    END AS billed
    FROM ?4 AS r
    JOIN audit_log al ON al.resource_type = ?1 AND al.resource_id = ?2::text || r.id::text AND al.action = ?3
        AND al.changes @> jsonb_build_array(jsonb_build_object('field', 'organization_id', 'after', r.organization_id))
    LEFT JOIN LATERAL (
        SELECT coalesce(
            (SELECT o.organization_id FROM app_organizations o WHERE o.app_id = r.organization_id),
            (SELECT nullif(p.organization_id, '') FROM apps p WHERE p.id = r.organization_id)) AS organization_id
    ) named ON true
    WHERE r.billed IS NULL
) said
WHERE row.id = said.id AND row.billed IS NULL AND said.billed IS NOT NULL`

// AttributeLimits attributes the limits, resets and grants not known to be anyone's, which a
// release from before they said whose they are wrote, where it can be told. It says how many.
func (s *Store) AttributeLimits(ctx context.Context) (int64, error) {
	var attributed int64
	for _, table := range []struct{ name, prefix string }{{"limits", ""}, {"usage_resets", "reset:"}, {"limit_grants", "grant:"}} {
		result, err := s.global.NewRaw(attributeLimits,
			AuditSourceManage, AuditSpendLimit, table.prefix, AuditCreated, bun.Ident(table.name)).Exec(ctx)
		if err != nil {
			return attributed, stack.Wrap(fmt.Errorf("store: attribute %s: %w", table.name, err))
		}
		n, err := result.RowsAffected()
		if err != nil {
			return attributed, stack.Wrap(fmt.Errorf("store: attribute %s: %w", table.name, err))
		}
		attributed += n
	}
	return attributed, nil
}
