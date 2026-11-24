package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// LimitLevel is whose ids a limit names: the subject's own, or a level above it, where the row
// is a default for every subject of its kind there.
type LimitLevel string

const (
	LevelOrganization LimitLevel = "organization"
	LevelApp          LimitLevel = "app"
	LevelAPIKey       LimitLevel = "api_key"
	LevelUser         LimitLevel = "user"
)

// limitLevels is each subject's levels, most specific first, which is the order a subject's
// limit is resolved in (limits_level_check).
var limitLevels = map[usage.Subject][]LimitLevel{
	usage.SubjectOrganization: {LevelOrganization},
	usage.SubjectApp:          {LevelApp},
	usage.SubjectAPIKey:       {LevelAPIKey, LevelApp, LevelOrganization},
	usage.SubjectUser:         {LevelUser, LevelApp, LevelOrganization},
	usage.SubjectCategory:     {LevelOrganization},
	usage.SubjectAppCategory:  {LevelApp},
}

// LimitLevels is the levels a subject can be limited at, most specific first. A subject that
// has none is no subject a limit can name.
func LimitLevels(subject usage.Subject) []LimitLevel {
	return limitLevels[subject]
}

// Limit is one of the customer's spend limits: what Subject may spend in each Interval window,
// at Level, whose ids it names.
type Limit struct {
	bun.BaseModel `bun:"table:limits,alias:lim"`

	ID             int64          `bun:"id,pk,autoincrement"`
	Subject        usage.Subject  `bun:"subject,notnull"`
	Level          LimitLevel     `bun:"level,notnull"`
	OrganizationID string         `bun:"organization_id,notnull"`
	AppID          string         `bun:"app_id,notnull"`
	APIKey         string         `bun:"api_key,notnull"`
	UserID         string         `bun:"user_id,notnull"`
	Category       string         `bun:"category,notnull"`
	Interval       usage.Interval `bun:"interval,notnull"`
	LimitMicros    int64          `bun:"limit_micros,notnull"`
	Note           string         `bun:"note,notnull"`
	UpdatedBy      string         `bun:"updated_by,notnull"`
	CreatedAt      time.Time      `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	UpdatedAt      time.Time      `bun:"updated_at,nullzero,notnull,default:current_timestamp"`
}

// Validate refuses a limit its table's checks would: a subject limited at a level it has
// none of, an id its level does not name or one it names left out, an interval spend is not
// kept in, or a negative amount.
func (l *Limit) Validate() error {
	idsMatch := (l.AppID != "") == (l.Level == LevelApp || l.Level == LevelAPIKey || l.Level == LevelUser) &&
		(l.APIKey != "") == (l.Level == LevelAPIKey) &&
		(l.UserID != "") == (l.Level == LevelUser)
	categorized := l.Subject == usage.SubjectCategory || l.Subject == usage.SubjectAppCategory
	return validate("limit").
		required("organization_id", l.OrganizationID).
		check(len(limitLevels[l.Subject]) > 0, "subject", fmt.Sprintf("is %q, which no limit names", l.Subject)).
		check(slices.Contains(limitLevels[l.Subject], l.Level), "level", fmt.Sprintf("is %q, which %s is not limited at", l.Level, l.Subject)).
		check(idsMatch, "app_id", "does not match the level: an app, key or user level names an app, and only they do").
		check((l.Category != "") == categorized, "category", "is named by a category subject, and only by one").
		check(l.Interval.Valid(), "interval", fmt.Sprintf("is %q, want daily, weekly, biweekly or monthly", l.Interval)).
		check(l.LimitMicros >= 0, "limit_micros", "is negative").
		done()
}

// ErrNoLimit is a limit that is not there, or is another organization's.
var ErrNoLimit = errors.New("store: no such limit")

// ErrLimitExists is a limit written for a subject, level and interval that already have one.
type ErrLimitExists struct{ ID int64 }

func (e *ErrLimitExists) Error() string {
	return "store: the subject already has a limit at that level and interval: " + strconv.FormatInt(e.ID, 10)
}

// LimitAudit is who a write of a limit, a reset or a grant is recorded as made by, which the
// store records in the transaction the write commits in.
type LimitAudit struct {
	CustomerID string
	Source     string
	ActorID    string
	ActorName  string
	RequestID  string
}

func (a LimitAudit) entry(resourceID, name, action string, changes []AuditChange) *AuditEntry {
	return &AuditEntry{
		CustomerID: a.CustomerID, ResourceType: AuditSpendLimit, ResourceID: resourceID, ResourceName: name,
		Action: action, Source: a.Source, ActorID: a.ActorID, ActorName: a.ActorName, RequestID: a.RequestID,
		Changes: changes,
	}
}

// limitFields is a limit as its audit rows name it.
func limitFields(l *Limit) map[string]any {
	if l == nil {
		return map[string]any{}
	}
	fields := map[string]any{
		"subject": l.Subject, "level": l.Level, "organization_id": l.OrganizationID, "interval": l.Interval,
		"limit_micros": l.LimitMicros,
	}
	for name, value := range map[string]string{"app_id": l.AppID, "api_key": l.APIKey, "user_id": l.UserID, "category": l.Category, "note": l.Note} {
		if value != "" {
			fields[name] = value
		}
	}
	return fields
}

// fieldChanges is every field whose value differs between before and after, in name order.
func fieldChanges(before, after map[string]any) []AuditChange {
	names := map[string]bool{}
	for name := range before {
		names[name] = true
	}
	for name := range after {
		names[name] = true
	}
	changes := []AuditChange{}
	for _, name := range slices.Sorted(maps.Keys(names)) {
		was, had := before[name]
		now, has := after[name]
		if had == has && fmt.Sprint(was) == fmt.Sprint(now) {
			continue
		}
		changes = append(changes, AuditChange{Field: name, Before: was, After: now})
	}
	return changes
}

// LimitFilter picks one organization's limits. Each empty field is every row.
type LimitFilter struct {
	OrganizationID string
	Subject        usage.Subject
	Level          LimitLevel
	AppID          string
	APIKey         string
	UserID         string
	Category       string
	Interval       usage.Interval
	// CategoryPrefix keeps only the rows whose category starts with it, and
	// NotCategoryPrefix only the ones whose category does not.
	CategoryPrefix    string
	NotCategoryPrefix string
	// After is the id of the last limit of the page before, and Limit the page size asked for.
	After int64
	Limit int
}

// How many limits are listed at once.
const (
	defaultLimitPage = 50
	maxLimitPage     = 200
)

// LimitPage is the page size Limits.Select uses for the size asked for. It returns one row more.
func LimitPage(asked int) int { return ClampLimit(asked, defaultLimitPage, maxLimitPage) }

// LimitChange is what an update of a limit changes: its amount and its note, nil leaving each
// as it is. What a row limits never changes.
type LimitChange struct {
	LimitMicros *int64
	Note        *string
	UpdatedBy   string
}

// Limits are the organizations' spend limits, in the shared database. Every write records its
// audit row in the transaction it commits in.
type Limits struct {
	store *Store
}

// SelectOf is every limit of the organizations.
func (r *Limits) SelectOf(ctx context.Context, organizationIDs []string) ([]Limit, error) {
	limits := []Limit{}
	if len(organizationIDs) == 0 {
		return limits, nil
	}
	if err := r.store.global.NewSelect().Model(&limits).Where("organization_id IN (?)", bun.In(organizationIDs)).Order("id").Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: limits for: %w", err))
	}
	return limits, nil
}

// Get is one limit by id, of the organization, or of any when organizationID is empty.
func (r *Limits) Get(ctx context.Context, organizationID string, id int64) (Limit, error) {
	return limitOf(ctx, r.store.global, organizationID, id, false)
}

func limitOf(ctx context.Context, db bun.IDB, organizationID string, id int64, lock bool) (Limit, error) {
	var limit Limit
	query := db.NewSelect().Model(&limit).Where("id = ?", id)
	if organizationID != "" {
		query = query.Where("organization_id = ?", organizationID)
	}
	if lock {
		query = query.For("UPDATE")
	}
	err := query.Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Limit{}, ErrNoLimit
	}
	if err != nil {
		return Limit{}, stack.Wrap(fmt.Errorf("store: limit: %w", err))
	}
	return limit, nil
}

// Insert writes a limit and its audit row. A subject that has one at the level and
// interval already is ErrLimitExists, naming it.
func (r *Limits) Insert(ctx context.Context, limit *Limit, audit LimitAudit) error {
	if err := limit.Validate(); err != nil {
		return stack.Wrap(err)
	}
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.NewInsert().Model(limit).Returning("*").Exec(ctx); err != nil {
			return err
		}
		return recordAuditEntry(ctx, tx, audit.entry(strconv.FormatInt(limit.ID, 10), string(limit.Subject), AuditCreated,
			fieldChanges(nil, limitFields(limit))))
	})
	if isViolation(err, uniqueViolation) {
		var existing Limit
		found := r.store.global.NewSelect().Model(&existing).
			Where("organization_id = ?", limit.OrganizationID).Where("subject = ?", limit.Subject).
			Where("level = ?", limit.Level).Where("interval = ?", limit.Interval).
			Where("app_id = ?", limit.AppID).Where("api_key = ?", limit.APIKey).
			Where("user_id = ?", limit.UserID).Where("category = ?", limit.Category).Scan(ctx)
		if found != nil {
			return stack.Wrap(fmt.Errorf("store: create limit: %w", found))
		}
		return &ErrLimitExists{ID: existing.ID}
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: create limit: %w", err))
	}
	return nil
}

// Update changes a limit of the organization, or of any when organizationID is empty, and
// records what moved. A change that moves nothing writes nothing.
func (r *Limits) Update(ctx context.Context, organizationID string, id int64, change LimitChange, audit LimitAudit) (Limit, error) {
	var updated Limit
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		before, err := limitOf(ctx, tx, organizationID, id, true)
		if err != nil {
			return err
		}
		updated = before
		if change.LimitMicros != nil {
			updated.LimitMicros = *change.LimitMicros
		}
		if change.Note != nil {
			updated.Note = *change.Note
		}
		changes := fieldChanges(limitFields(&before), limitFields(&updated))
		if len(changes) == 0 {
			return nil
		}
		updated.UpdatedBy = change.UpdatedBy
		if _, err := tx.NewUpdate().Model(&updated).Column("limit_micros", "note", "updated_by").
			Set("updated_at = now()").WherePK().Returning("*").Exec(ctx); err != nil {
			return err
		}
		return recordAuditEntry(ctx, tx, audit.entry(strconv.FormatInt(id, 10), string(updated.Subject), AuditUpdated, changes))
	})
	if errors.Is(err, ErrNoLimit) {
		return Limit{}, ErrNoLimit
	}
	if err != nil {
		return Limit{}, stack.Wrap(fmt.Errorf("store: update limit: %w", err))
	}
	return updated, nil
}

// Delete removes a limit of the organization, or of any when organizationID is empty, and
// records it.
func (r *Limits) Delete(ctx context.Context, organizationID string, id int64, audit LimitAudit) error {
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		before, err := limitOf(ctx, tx, organizationID, id, true)
		if err != nil {
			return err
		}
		if _, err := tx.NewDelete().Model(&before).WherePK().Exec(ctx); err != nil {
			return err
		}
		return recordAuditEntry(ctx, tx, audit.entry(strconv.FormatInt(id, 10), string(before.Subject), AuditDeleted,
			fieldChanges(limitFields(&before), nil)))
	})
	if errors.Is(err, ErrNoLimit) {
		return ErrNoLimit
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete limit: %w", err))
	}
	return nil
}

// Select lists an organization's limits by id, after filter.After, LimitPage(filter.Limit) and
// one more.
func (r *Limits) Select(ctx context.Context, filter LimitFilter) ([]Limit, error) {
	if filter.OrganizationID == "" {
		return nil, stack.Wrap(errors.New("store: listing limits needs an organization"))
	}
	limits := []Limit{}
	query := r.store.global.NewSelect().Model(&limits).Where("organization_id = ?", filter.OrganizationID)
	for column, value := range map[string]string{
		"subject": string(filter.Subject), "level": string(filter.Level), "app_id": filter.AppID, "api_key": filter.APIKey,
		"user_id": filter.UserID, "category": filter.Category, "interval": string(filter.Interval),
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
	if filter.After > 0 {
		query = query.Where("id > ?", filter.After)
	}
	if err := query.Order("id").Limit(LimitPage(filter.Limit) + 1).Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: list limits: %w", err))
	}
	return limits, nil
}
