package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// MaxLiveGrants is the most grants one owner has live at once, so admission's search
// for one stays a short scan.
const MaxLiveGrants = 20

// ErrNoGrant is a grant that is not there, or is another owner's.
var ErrNoGrant = errors.New("store: no such grant")

// ErrTooManyGrants is a grant past MaxLiveGrants live in its organization.
var ErrTooManyGrants = errors.New("store: the organization has as many live grants as it may")

// ErrGrantExpired is an unrevoke of a grant past its expires_at, which could never be drawn.
var ErrGrantExpired = errors.New("store: the grant has expired")

// LimitGrant is a one-time amount one subject may spend past its own spent limits, until it is
// used up, expires or is revoked. UsedMicros is what it paid for: the billable cost of the
// request rows that name it, added as the writer writes them (LimitGrantUses.InsertMany).
type LimitGrant struct {
	bun.BaseModel `bun:"table:limit_grants,alias:lg"`

	ID             int64  `bun:"id,pk,autoincrement"`
	OrganizationID string `bun:"organization_id,notnull"`
	// Billed is whether OrganizationID is an organization's id, or an app's in none. Nil is a row
	// from before it was kept (AttributeLimits).
	Billed       *bool         `bun:"billed"`
	Subject      usage.Subject `bun:"subject,notnull"`
	AppID        string        `bun:"app_id,notnull"`
	APIKey       string        `bun:"api_key,notnull"`
	UserID       string        `bun:"user_id,notnull"`
	Category     string        `bun:"category,notnull"`
	AmountMicros int64         `bun:"amount_micros,notnull"`
	UsedMicros   int64         `bun:"used_micros,notnull"`
	ExpiresAt    *time.Time    `bun:"expires_at"`
	RevokedAt    *time.Time    `bun:"revoked_at"`
	RevokedBy    string        `bun:"revoked_by,notnull"`
	Reason       string        `bun:"reason,notnull"`
	Actor        string        `bun:"actor,notnull"`
	CreatedAt    time.Time     `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}

// Validate refuses a grant its table's checks would: one outside an organization, for a
// subject there is none of, naming an id its subject does not or leaving out one it does, of
// nothing, with no reason, or expiring before it is made.
func (g *LimitGrant) Validate() error {
	idsMatch := (g.AppID != "") == (g.Subject == usage.SubjectApp || g.Subject == usage.SubjectAPIKey ||
		g.Subject == usage.SubjectUser || g.Subject == usage.SubjectAppCategory) &&
		(g.APIKey != "") == (g.Subject == usage.SubjectAPIKey) &&
		(g.UserID != "") == (g.Subject == usage.SubjectUser)
	categorized := g.Subject == usage.SubjectCategory || g.Subject == usage.SubjectAppCategory
	return validate("limit grant").
		required("organization_id", g.OrganizationID).
		check(len(limitLevels[g.Subject]) > 0, "subject", fmt.Sprintf("is %q, which no grant is for", g.Subject)).
		check(idsMatch, "app_id", "does not match the subject: an app, key, user or app category names an app, and only they do").
		check((g.Category != "") == categorized, "category", "is named by a category subject, and only by one").
		check(g.AmountMicros > 0, "amount_micros", "is not positive").
		check(g.UsedMicros >= 0, "used_micros", "is negative").
		required("reason", g.Reason).
		check(g.ExpiresAt == nil || g.ExpiresAt.After(time.Now()), "expires_at", "is not in the future").
		done()
}

// Ref is the subject the grant is for.
func (g LimitGrant) Ref() usage.SubjectRef {
	return usage.SubjectRef{OrganizationID: g.OrganizationID, Subject: g.Subject, AppID: g.AppID, APIKey: g.APIKey, UserID: g.UserID, Category: g.Category}
}

// LiveAt reports whether the grant can be drawn at now: not revoked, and not expired.
func (g LimitGrant) LiveAt(now time.Time) bool {
	return g.RevokedAt == nil && (g.ExpiresAt == nil || now.Before(*g.ExpiresAt))
}

// LimitGrants are the one-time amounts subjects may spend past their limits, in the shared
// database. Every write records its audit row in the transaction it commits in.
type LimitGrants struct {
	store *Store
}

// whereLive keeps the grants that can still be drawn at now: not revoked, not expired, and not
// used up.
func whereLive(query *bun.SelectQuery, now any) *bun.SelectQuery {
	return query.Where("revoked_at IS NULL").Where("expires_at IS NULL OR expires_at > ?", now).
		Where("used_micros < amount_micros")
}

// grantFields is a grant as its audit rows name it.
func grantFields(g *LimitGrant) map[string]any {
	fields := map[string]any{
		"subject": g.Subject, "organization_id": g.OrganizationID, "amount_micros": g.AmountMicros, "reason": g.Reason,
	}
	for name, value := range map[string]string{"app_id": g.AppID, "api_key": g.APIKey, "user_id": g.UserID, "category": g.Category} {
		if value != "" {
			fields[name] = value
		}
	}
	if g.ExpiresAt != nil {
		fields["expires_at"] = g.ExpiresAt.UTC().Format(time.RFC3339)
	}
	if g.RevokedAt != nil {
		fields["revoked_at"] = g.RevokedAt.UTC().Format(time.RFC3339)
		fields["revoked_by"] = g.RevokedBy
	}
	return fields
}

// SelectLive is the grants whose organization id is one of organizationIDs, whoever they belong
// to (OwnedBy, HeldTo), that are neither revoked, expired at now, nor used up.
func (r *LimitGrants) SelectLive(ctx context.Context, organizationIDs []string, now time.Time) ([]LimitGrant, error) {
	grants := []LimitGrant{}
	if len(organizationIDs) == 0 {
		return grants, nil
	}
	query := r.store.global.NewSelect().Model(&grants).Where("organization_id IN (?)", bun.In(organizationIDs))
	err := whereLive(query, now.UTC()).Order("id").Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: live grants: %w", err))
	}
	return grants, nil
}

// Insert writes a grant and its audit row, refusing one past MaxLiveGrants live in the
// organization with ErrTooManyGrants; a used-up grant is not live. The count and the write hold
// the organization's lock, so two grants made at once cannot both be the twenty-first.
func (r *LimitGrants) Insert(ctx context.Context, grant *LimitGrant, audit LimitAudit) error {
	grant.UsedMicros = 0
	grant.Billed = organizationOwned(grant.Billed)
	if err := grant.Validate(); err != nil {
		return stack.Wrap(err)
	}
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockUntilCommit(ctx, tx, "limit_grants:"+grant.OrganizationID, 0); err != nil {
			return err
		}
		query := tx.NewSelect().Model((*LimitGrant)(nil))
		query = whereOwner(query, LimitOwner(grant.OrganizationID, *grant.Billed), false)
		live, err := whereLive(query, bun.Safe("now()")).Count(ctx)
		if err != nil {
			return err
		}
		if live >= MaxLiveGrants {
			return ErrTooManyGrants
		}
		if _, err := tx.NewInsert().Model(grant).Returning("*").Exec(ctx); err != nil {
			return err
		}
		return recordAuditEntry(ctx, tx, audit.entry("grant:"+strconv.FormatInt(grant.ID, 10), string(grant.Subject), AuditCreated,
			fieldChanges(nil, grantFields(grant))))
	})
	if errors.Is(err, ErrTooManyGrants) {
		return ErrTooManyGrants
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: add grant: %w", err))
	}
	return nil
}

// Get is one grant by id, of the owner, or of any when owner is empty.
func (r *LimitGrants) Get(ctx context.Context, owner string, id int64) (LimitGrant, error) {
	return grantOf(ctx, r.store.global, owner, id, false)
}

func grantOf(ctx context.Context, db bun.IDB, owner string, id int64, lock bool) (LimitGrant, error) {
	var grant LimitGrant
	query := db.NewSelect().Model(&grant).Where("id = ?", id)
	if owner != "" {
		query = whereOwner(query, owner, false)
	}
	if lock {
		query = query.For("UPDATE")
	}
	err := query.Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return LimitGrant{}, ErrNoGrant
	}
	if err != nil {
		return LimitGrant{}, stack.Wrap(fmt.Errorf("store: grant: %w", err))
	}
	return grant, nil
}

// Revoke ends a grant of the owner, or of any when owner is empty, and records it with the reason. Revoking one already revoked changes nothing and records
// nothing. The grant and what it paid for stay, for the history.
func (r *LimitGrants) Revoke(ctx context.Context, owner string, id int64, by, reason string, audit LimitAudit) (LimitGrant, error) {
	var revoked LimitGrant
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		before, err := grantOf(ctx, tx, owner, id, true)
		if err != nil {
			return err
		}
		revoked = before
		if before.RevokedAt != nil {
			return nil
		}
		if _, err := tx.NewUpdate().Model(&revoked).Set("revoked_at = now()").Set("revoked_by = ?", by).
			WherePK().Returning("*").Exec(ctx); err != nil {
			return err
		}
		if audit.CustomerID == "" {
			audit.CustomerID = revoked.OrganizationID
		}
		after := grantFields(&revoked)
		after["revoke_reason"] = reason
		return recordAuditEntry(ctx, tx, audit.entry("grant:"+strconv.FormatInt(id, 10), string(revoked.Subject), AuditUpdated,
			fieldChanges(grantFields(&before), after)))
	})
	if errors.Is(err, ErrNoGrant) {
		return LimitGrant{}, ErrNoGrant
	}
	if err != nil {
		return LimitGrant{}, stack.Wrap(fmt.Errorf("store: revoke grant: %w", err))
	}
	return revoked, nil
}

// Unrevoke makes a revoked grant of the owner, or of any when owner is empty, live again, and records it with the reason. Unrevoking one that is not revoked
// changes nothing and records nothing. An expired grant is ErrGrantExpired, and one that would
// be past MaxLiveGrants live in its organization ErrTooManyGrants: the count and the write
// hold the organization's lock, as Insert's do.
func (r *LimitGrants) Unrevoke(ctx context.Context, owner string, id int64, reason string, audit LimitAudit) (LimitGrant, error) {
	var unrevoked LimitGrant
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// The organization's lock is taken before the row's, as Insert takes it before it
		// writes, and an organization id is never changed.
		found, err := grantOf(ctx, tx, owner, id, false)
		if err != nil {
			return err
		}
		if err := lockUntilCommit(ctx, tx, "limit_grants:"+found.OrganizationID, 0); err != nil {
			return err
		}
		before, err := grantOf(ctx, tx, owner, id, true)
		if err != nil {
			return err
		}
		unrevoked = before
		if before.RevokedAt == nil {
			return nil
		}
		if before.ExpiresAt != nil && !time.Now().Before(*before.ExpiresAt) {
			return ErrGrantExpired
		}
		if before.UsedMicros < before.AmountMicros {
			query := tx.NewSelect().Model((*LimitGrant)(nil))
			query = whereOwner(query, Holders(before)[0], false)
			live, err := whereLive(query, bun.Safe("now()")).Count(ctx)
			if err != nil {
				return err
			}
			if live >= MaxLiveGrants {
				return ErrTooManyGrants
			}
		}
		unrevoked.RevokedAt, unrevoked.RevokedBy = nil, ""
		if _, err := tx.NewUpdate().Model(&unrevoked).Column("revoked_at", "revoked_by").
			WherePK().Returning("*").Exec(ctx); err != nil {
			return err
		}
		if audit.CustomerID == "" {
			audit.CustomerID = unrevoked.OrganizationID
		}
		after := grantFields(&unrevoked)
		after["unrevoke_reason"] = reason
		return recordAuditEntry(ctx, tx, audit.entry("grant:"+strconv.FormatInt(id, 10), string(unrevoked.Subject), AuditUpdated,
			fieldChanges(grantFields(&before), after)))
	})
	for _, known := range []error{ErrNoGrant, ErrGrantExpired, ErrTooManyGrants} {
		if errors.Is(err, known) {
			return LimitGrant{}, known
		}
	}
	if err != nil {
		return LimitGrant{}, stack.Wrap(fmt.Errorf("store: unrevoke grant: %w", err))
	}
	return unrevoked, nil
}

// GrantFilter picks one owner's grants. Each empty field is every grant.
type GrantFilter struct {
	Owner string
	// Unattributed also keeps the grants of the owner's id not known to be anyone's.
	Unattributed bool
	Subject      usage.Subject
	AppID        string
	APIKey       string
	UserID       string
	Category     string
	// CategoryPrefix and NotCategoryPrefix keep the grants whose category does, or does not,
	// start with them.
	CategoryPrefix    string
	NotCategoryPrefix string
	// Revoked, when set, keeps only the revoked grants or only the others; Expired, likewise,
	// the grants expired at now or the others; Used the grants used up or the others.
	Revoked *bool
	Expired *bool
	Used    *bool
	Now     time.Time
	// Before is the id of the last grant of the page before, and Limit the page size asked for.
	Before int64
	Limit  int
}

// Select lists an organization's grants newest first, before filter.Before when it is not 0,
// LimitPage(filter.Limit) and one more. Revoked and expired grants are listed too.
func (r *LimitGrants) Select(ctx context.Context, filter GrantFilter) ([]LimitGrant, error) {
	if filter.Owner == "" {
		return nil, stack.Wrap(errors.New("store: listing grants needs an owner"))
	}
	grants := []LimitGrant{}
	query := whereOwner(r.store.global.NewSelect().Model(&grants), filter.Owner, filter.Unattributed)
	for column, value := range map[string]string{
		"subject": string(filter.Subject), "app_id": filter.AppID, "api_key": filter.APIKey, "user_id": filter.UserID, "category": filter.Category,
	} {
		if value != "" {
			query = query.Where("? = ?", bun.Ident(column), value)
		}
	}
	if filter.CategoryPrefix != "" {
		query = query.Where("starts_with(category, ?)", filter.CategoryPrefix)
	}
	if filter.NotCategoryPrefix != "" {
		query = query.Where("NOT starts_with(category, ?)", filter.NotCategoryPrefix)
	}
	if filter.Revoked != nil {
		if *filter.Revoked {
			query = query.Where("revoked_at IS NOT NULL")
		} else {
			query = query.Where("revoked_at IS NULL")
		}
	}
	if filter.Expired != nil {
		if *filter.Expired {
			query = query.Where("expires_at IS NOT NULL AND expires_at <= ?", filter.Now.UTC())
		} else {
			query = query.Where("expires_at IS NULL OR expires_at > ?", filter.Now.UTC())
		}
	}
	if filter.Used != nil {
		if *filter.Used {
			query = query.Where("used_micros >= amount_micros")
		} else {
			query = query.Where("used_micros < amount_micros")
		}
	}
	if filter.Before > 0 {
		query = query.Where("id < ?", filter.Before)
	}
	if err := query.Order("id DESC").Limit(LimitPage(filter.Limit) + 1).Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: list grants: %w", err))
	}
	return grants, nil
}
