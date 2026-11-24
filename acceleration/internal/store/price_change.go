package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/prices"
)

// What wrote a price change (PriceChange.Kind).
const (
	// PriceChangeSeed is a database's first load, from the router's price files.
	PriceChangeSeed = "seed"
	// PriceChangeRelease is the items a release's price files had that the database lacked.
	PriceChangeRelease = "release"
	// PriceChangeAPI is a change made through the management API.
	PriceChangeAPI = "api"
)

// ErrNoPriceChange is a price change nobody wrote.
var ErrNoPriceChange = errors.New("store: there is no such price change")

// PriceChange is one write to the price list. It is never deleted; one not yet in force can
// be cancelled, once.
type PriceChange struct {
	bun.BaseModel `bun:"table:price_changes,alias:pc"`

	// Name is <UTC yyyymmddhhmmss>_<snake_case>.
	Name string `bun:"name,pk"`
	// Kind is PriceChangeSeed, PriceChangeRelease or PriceChangeAPI.
	Kind   string `bun:"kind,notnull"`
	Reason string `bun:"reason,notnull"`
	// Source is the vendor's page and the date it was read, the operator's decision, or the
	// files and release a load came from.
	Source         string     `bun:"source,notnull"`
	EffectiveAt    time.Time  `bun:"effective_at,notnull"`
	CreatedBy      string     `bun:"created_by,notnull"`
	IdempotencyKey string     `bun:"idempotency_key,nullzero"`
	CreatedAt      time.Time  `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	CancelledAt    *time.Time `bun:"cancelled_at"`
	CancelledBy    string     `bun:"cancelled_by,nullzero"`
	CancelReason   string     `bun:"cancel_reason,nullzero"`
}

// Validate refuses a change the table would: one with a name, a kind it knows and a time it
// takes effect.
func (c *PriceChange) Validate() error {
	return validate("price change").
		required("name", c.Name).
		oneOf("kind", c.Kind, PriceChangeSeed, PriceChangeRelease, PriceChangeAPI).
		check(!c.EffectiveAt.IsZero(), "effective_at", "is required").
		done()
}

// PriceChanges are the writes to the price list, in the shared database: never deleted, and
// cancelled at most once before they take effect.
type PriceChanges struct {
	store *Store
}

// priceChangeColumns are PriceChange's columns, returned by name as priceEntryColumns are.
const priceChangeColumns = "name, kind, reason, source, effective_at, created_by, idempotency_key, created_at, " +
	"cancelled_at, cancelled_by, cancel_reason"

// PriceLoad is what a router's start loads into the price list: its price files, merged.
type PriceLoad struct {
	// Entries are every file's, the first file's entry winning for an item, region, BYOK state
	// and When. One with no price is priced at its vendor rates times pricing.PriceFactorBps,
	// or pricing.BYOKFeeBps for a BYOK one, as it is written, and one with no SKU is given one
	// (AssignSKUs).
	Entries []pricing.Entry
	// Source names the files, for the change's source.
	Source string
	// Release is the router's, for the change's name and author.
	Release string
}

// pricesLock serialises loads of the price list, so routers starting at once write it once.
// pricesLockTimeout bounds the wait for it: a load runs before a router serves, and one held
// up by a stuck session fails and is tried again later rather than keeping the router down.
const (
	pricesLock        = "SELECT pg_advisory_xact_lock(hashtextextended('router.prices', 0))"
	pricesLockTimeout = "SET LOCAL lock_timeout = '5s'"
)

// ToLoad is what Sync would write now, writing nothing: whether it would be the
// database's first load, and the entries it would add.
func (r *PriceChanges) ToLoad(ctx context.Context, entries []pricing.Entry) (bool, []pricing.Entry, error) {
	return pricesToLoad(ctx, r.store.global, entries)
}

// Sync loads the router's price files into the price list, in one transaction. A
// database with no change yet gets them all as its seed. Otherwise only the entries for an
// item, region, BYOK state and When no change that stands has ever priced are added, as one
// release change: an item the database has priced keeps its price and its withdrawal, whatever
// the files say now. It reports the change it wrote, the entries in it as written, and whether
// it wrote one.
func (r *PriceChanges) Sync(ctx context.Context, load PriceLoad) (PriceChange, []pricing.Entry, bool, error) {
	var change PriceChange
	var added []pricing.Entry
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockPrices(ctx, tx); err != nil {
			return err
		}
		var err error
		change, added, err = syncPrices(ctx, tx, load)
		return err
	})
	if err != nil {
		return PriceChange{}, nil, false, stack.Wrap(fmt.Errorf("store: sync prices: %w", err))
	}
	return change, added, added != nil, nil
}

// lockPrices takes the lock every write to the price list takes, giving up after
// pricesLockTimeout.
func lockPrices(ctx context.Context, tx bun.Tx) error {
	if _, err := tx.ExecContext(ctx, pricesLockTimeout); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, pricesLock)
	return err
}

// syncPrices is Sync in a transaction that holds the prices lock. It returns nil
// entries when it wrote nothing.
func syncPrices(ctx context.Context, tx bun.Tx, load PriceLoad) (PriceChange, []pricing.Entry, error) {
	seed, entries, err := pricesToLoad(ctx, tx, load.Entries)
	if err != nil || len(entries) == 0 {
		return PriceChange{}, nil, err
	}
	// The database's clock, so routers whose clocks disagree still name and time their
	// changes in one order.
	now, err := transactionStart(ctx, tx)
	if err != nil {
		return PriceChange{}, nil, err
	}
	change := PriceChange{
		EffectiveAt: now,
		Source:      load.Source + ", router " + load.Release,
		CreatedBy:   "router " + load.Release,
	}
	stamp := now.UTC().Format("20060102150405")
	if seed {
		change.Name, change.Kind, change.Reason = stamp+"_seed", PriceChangeSeed, "The first price list"
	} else {
		change.Name, change.Kind = stamp+"_release_"+ReleaseName(load.Release), PriceChangeRelease
		change.Reason = "Items new in " + load.Release
	}
	history, err := priceSKUs(ctx, tx)
	if err != nil {
		return PriceChange{}, nil, err
	}
	writes := make([]PriceEntryWrite, 0, len(entries))
	for _, entry := range entries {
		writes = append(writes, PriceEntryWrite{Entry: pricing.Priced(entry)})
	}
	if writes, err = AssignSKUs(history, now, writes); err != nil {
		return PriceChange{}, nil, err
	}
	rows := make([]PriceEntry, 0, len(writes))
	entries = make([]pricing.Entry, 0, len(writes))
	names := make([]string, 0, len(writes))
	for _, write := range writes {
		rows = append(rows, priceEntryOf(change.Name, write.Entry))
		entries = append(entries, write.Entry)
		names = append(names, prices.Name(write.Entry))
	}
	if err := validatePriceChange(&change, rows); err != nil {
		return PriceChange{}, nil, err
	}
	if _, err := tx.NewInsert().Model(&change).Exec(ctx); err != nil {
		return PriceChange{}, nil, err
	}
	if _, err := tx.NewInsert().Model(&rows).Exec(ctx); err != nil {
		return PriceChange{}, nil, err
	}
	changes := []AuditChange{{Field: "entries", After: names}}
	err = recordAuditEntry(ctx, tx, &AuditEntry{
		ResourceType: AuditResourcePriceList,
		ResourceID:   change.Name,
		ResourceName: change.Reason,
		Action:       AuditCreated,
		Source:       AuditSourceCLI,
		ActorID:      change.CreatedBy,
		ActorName:    "router",
		Changes:      changes,
	})
	if err != nil {
		return PriceChange{}, nil, err
	}
	return change, entries, nil
}

// PriceReload is what a reload of the price list found in it, and the seed it wrote in its
// place.
type PriceReload struct {
	// At is the database's clock when the reload read the list.
	At time.Time
	// Changes are every change the list held, newest first: cancelled ones and those not yet
	// in force among them. Entries is how many entries they wrote.
	Changes []PriceChange
	Entries int
	// Active is the list in force at At, before the reload.
	Active []PriceEntry
	// Seed is the change the reload wrote and Loaded its entries; both are empty on a dry run.
	Seed   PriceChange
	Loaded []pricing.Entry
}

// Reload empties the price list and seeds it from load, as a router's first start on a
// fresh database does, in one transaction under the lock every load takes. Every change goes:
// those scheduled and cancelled with the rest, so the list holds what the files say and
// nothing else. TRUNCATE is what removes them, because the tables' triggers
// refuse a DELETE; no table references either one. The audit log keeps its rows, and gains
// one saying what the reload removed. dryRun reads the list under the lock and writes nothing.
//
// It is for a development database only; the caller decides that it is one.
func (r *PriceChanges) Reload(ctx context.Context, load PriceLoad, dryRun bool) (PriceReload, error) {
	if len(load.Entries) == 0 {
		return PriceReload{}, stack.Wrap(errors.New("store: reload prices: the price files have no entry, so the list would be empty"))
	}
	var reload PriceReload
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockPrices(ctx, tx); err != nil {
			return err
		}
		var err error
		if reload.At, err = transactionStart(ctx, tx); err != nil {
			return err
		}
		reload.Changes = []PriceChange{}
		if err := tx.NewSelect().Model(&reload.Changes).Order("created_at DESC", "name DESC").Scan(ctx); err != nil {
			return err
		}
		if reload.Entries, err = tx.NewSelect().Model((*PriceEntry)(nil)).Count(ctx); err != nil {
			return err
		}
		if reload.Active, err = activePriceEntries(ctx, tx, reload.At); err != nil {
			return err
		}
		if dryRun {
			return nil
		}
		if _, err := tx.ExecContext(ctx, "TRUNCATE price_list_entries, price_changes"); err != nil {
			return err
		}
		// The removal is recorded before the seed, under a name of its own, so the log reads
		// in the order the reload did them.
		if len(reload.Changes) > 0 {
			names := make([]string, 0, len(reload.Changes))
			for _, change := range reload.Changes {
				names = append(names, change.Name)
			}
			err := recordAuditEntry(ctx, tx, &AuditEntry{
				ResourceType: AuditResourcePriceList,
				ResourceID:   reload.At.UTC().Format("20060102150405") + "_reload",
				ResourceName: "Every change, removed by a development reload",
				Action:       AuditDeleted,
				Source:       AuditSourceCLI,
				ActorID:      "router " + load.Release,
				ActorName:    "router prices reload",
				Changes: []AuditChange{
					{Field: "changes", Before: names},
					{Field: "entries", Before: reload.Entries},
				},
			})
			if err != nil {
				return err
			}
		}
		reload.Seed, reload.Loaded, err = syncPrices(ctx, tx, load)
		return err
	})
	if err != nil {
		return PriceReload{}, stack.Wrap(fmt.Errorf("store: reload prices: %w", busyOr(err)))
	}
	return reload, nil
}

// pricesToLoad is whether the database has no change yet, and the entries a load would add:
// all of them to an empty one, else those for an item, region, BYOK state and When no change
// that stands wrote.
func pricesToLoad(ctx context.Context, db bun.IDB, entries []pricing.Entry) (bool, []pricing.Entry, error) {
	var changes int
	if err := db.NewSelect().Model((*PriceChange)(nil)).ColumnExpr("count(*)").Scan(ctx, &changes); err != nil {
		return false, nil, stack.Wrap(fmt.Errorf("store: prices to load: %w", err))
	}
	if changes == 0 {
		return true, slices.Clone(entries), nil
	}
	var priced []PriceEntry
	err := db.NewSelect().Model(&priced).
		ColumnExpr("pe.modality, pe.provider, pe.model, pe.region, pe.byok, pe.attributes").
		Join("JOIN price_changes AS pc ON pc.name = pe.change AND pc.cancelled_at IS NULL").
		Distinct().
		Scan(ctx)
	if err != nil {
		return false, nil, stack.Wrap(fmt.Errorf("store: prices to load: %w", err))
	}
	known := make(map[string]bool, len(priced))
	for _, row := range priced {
		known[prices.Name(row.Entry())] = true
	}
	var added []pricing.Entry
	for _, entry := range entries {
		if !known[prices.Name(entry)] {
			added = append(added, entry)
		}
	}
	return false, added, nil
}

var notInName = regexp.MustCompile(`[^a-z0-9_]`)

// ReleaseName is a release as a change's name carries it: lowercased, with every character
// outside [a-z0-9_] an underscore, so v1.4.0 is v1_4_0.
func ReleaseName(release string) string {
	return notInName.ReplaceAllString(strings.ToLower(release), "_")
}

// nextPriceChangeAt is when the next change not cancelled takes effect after after, and
// whether one is scheduled, so a list in memory can switch at that moment.
func nextPriceChangeAt(ctx context.Context, db bun.IDB, after time.Time) (time.Time, bool, error) {
	var next sql.NullTime
	err := db.NewSelect().Model((*PriceChange)(nil)).ColumnExpr("min(effective_at)").
		Where("cancelled_at IS NULL").
		Where("effective_at > ?", after).
		Scan(ctx, &next)
	if err != nil {
		return time.Time{}, false, stack.Wrap(fmt.Errorf("store: next price change: %w", err))
	}
	return next.Time, next.Valid, nil
}

// Get is the change of that name and the entries it wrote, or ErrNoPriceChange.
func (r *PriceChanges) Get(ctx context.Context, name string) (PriceChange, []PriceEntry, error) {
	var change PriceChange
	err := r.store.global.NewSelect().Model(&change).Where("name = ?", name).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return PriceChange{}, nil, ErrNoPriceChange
	}
	if err != nil {
		return PriceChange{}, nil, stack.Wrap(fmt.Errorf("store: price change: %w", err))
	}
	entries, err := priceEntriesOf(ctx, r.store.global, name)
	return change, entries, err
}

// How many price changes are handed back at once. Neither is measured; they are the sizes
// every other list here uses.
const (
	defaultPriceChangeLimit = 25
	maxPriceChangeLimit     = 200
)

// PriceChangeLimit is the page size a list of price changes uses for the limit asked for.
// PriceChanges.Select returns one row more than this.
func PriceChangeLimit(asked int) int {
	return ClampLimit(asked, defaultPriceChangeLimit, maxPriceChangeLimit)
}

// PriceChangePosition is the last change of a page.
type PriceChangePosition struct {
	CreatedAt time.Time `json:"c"`
	Name      string    `json:"n"`
}

// PriceChangeFilter picks price changes, a page at a time.
type PriceChangeFilter struct {
	// Kind keeps only changes of that kind. Empty is every kind.
	Kind  string
	Limit int
	// After is the last change of the previous page.
	After *PriceChangePosition
}

// Select lists the price changes, newest first, one more than
// PriceChangeLimit(filter.Limit).
func (r *PriceChanges) Select(ctx context.Context, filter PriceChangeFilter) ([]PriceChange, error) {
	changes := []PriceChange{}
	query := r.store.global.NewSelect().Model(&changes)
	if filter.Kind != "" {
		query = query.Where("kind = ?", filter.Kind)
	}
	if after := filter.After; after != nil {
		query = query.Where("(created_at, name) < (?, ?)", after.CreatedAt, after.Name)
	}
	err := query.
		Order("created_at DESC", "name DESC").
		Limit(PriceChangeLimit(filter.Limit) + 1).
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: price changes: %w", err))
	}
	return changes, nil
}

// What a price change made through the management API is refused for.
var (
	// ErrPriceListNotSeeded is a change asked for before the router's first start loaded the
	// price files: a change written first would stop that load from seeding the list.
	ErrPriceListNotSeeded = errors.New("store: the price list is not seeded yet")
	// ErrPriceChangeKeyReused is an idempotency key that already holds another change.
	ErrPriceChangeKeyReused = errors.New("store: the idempotency key holds another price change")
	// ErrPriceChangeExists is a change named as one already made in the same second.
	ErrPriceChangeExists = errors.New("store: a price change of that name was made this second")
	// ErrPriceChangeInForce is a cancel of a change already in force, or one the API did not
	// make: neither can be cancelled, only followed by a newer change.
	ErrPriceChangeInForce = errors.New("store: the price change is in force or was not made through the API")
	// ErrPriceChangeInPast is an effective time more than PriceChangeGrace ago.
	ErrPriceChangeInPast = errors.New("store: a price change cannot take effect in the past")
	// ErrPriceListBusy is a write that waited its lock timeout for another write to the price
	// list, or a router's start loading its price files, to finish: one to send again shortly.
	ErrPriceListBusy = errors.New("store: another write to the price list held it past the lock timeout")
)

// lockNotAvailable is the SQLSTATE a statement that waited past lock_timeout fails with.
const lockNotAvailable = "55P03"

// busyOr is ErrPriceListBusy for a write that gave up waiting for the price list's lock, and
// err for any other failure.
func busyOr(err error) error {
	if isViolation(err, lockNotAvailable) {
		return ErrPriceListBusy
	}
	return err
}

// PriceChangeGrace is how far in the past a change may say it takes effect: one up to this
// long ago is taken as now, which absorbs a caller's clock running behind the database's, and
// one further back is refused, so no change rewrites what a row already billed.
const PriceChangeGrace = 5 * time.Minute

// PriceChangeWrite is a change made through the management API.
type PriceChangeWrite struct {
	// Name is the caller's, of [a-z0-9_]; the change is named <UTC yyyymmddhhmmss>_<Name>.
	Name   string
	Reason string
	Source string
	// EffectiveAt nil, or no later than now, is now.
	EffectiveAt *time.Time
	Entries     []PriceEntryWrite
	// Expand, when set, is the entries the change writes on the list it lands on, Entries and
	// those written beside them, before their SKUs are assigned. A change sent again under its
	// idempotency key is matched on Entries alone.
	Expand         func(PriceListAt, []PriceEntryWrite) []PriceEntryWrite
	IdempotencyKey string
	ActorID        string
	ActorName      string
}

// effectiveAt is when a change asked to take effect at asked does, by the database's clock.
func effectiveAt(now time.Time, asked *time.Time) (time.Time, error) {
	if asked == nil {
		return now, nil
	}
	if asked.Before(now.Add(-PriceChangeGrace)) {
		return time.Time{}, ErrPriceChangeInPast
	}
	if !asked.After(now) {
		return now, nil
	}
	return asked.UTC().Truncate(time.Microsecond), nil
}

// Insert writes a change, its entries and its audit row in one transaction, under
// the lock a router's start loads its price files under. Each entry is given its SKU
// (AssignSKUs), which refuses the change with ErrSKUInUse or ErrSKUChanged. check is handed
// the list the change lands on and the entries with their SKUs, and refuses the change by
// returning an error, which Insert returns as it is. A change already written under
// the write's idempotency key is answered instead, with replayed true, when it is the same
// change, and ErrPriceChangeKeyReused when it is not.
func (r *PriceChanges) Insert(ctx context.Context, write PriceChangeWrite,
	check func(PriceListAt, []PriceEntryWrite) error) (PriceChange, []PriceEntry, bool, error) {
	var change PriceChange
	var rows []PriceEntry
	var replayed bool
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.ExecContext(ctx, pricesLockTimeout); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, pricesLock); err != nil {
			return err
		}
		if write.IdempotencyKey != "" {
			err := tx.NewSelect().Model(&change).Where("idempotency_key = ?", write.IdempotencyKey).Scan(ctx)
			switch {
			case err == nil:
				written, err := priceEntriesOf(ctx, tx, change.Name)
				if err != nil {
					return err
				}
				if !samePriceChange(change, written, write) {
					return ErrPriceChangeKeyReused
				}
				rows, replayed = written, true
				return nil
			case !errors.Is(err, sql.ErrNoRows):
				return err
			}
		}
		seeded, err := tx.NewSelect().Model((*PriceChange)(nil)).Exists(ctx)
		if err != nil {
			return err
		}
		if !seeded {
			return ErrPriceListNotSeeded
		}
		before, err := priceListAt(ctx, tx, write.EffectiveAt)
		if err != nil {
			return err
		}
		entries := write.Entries
		if write.Expand != nil {
			entries = write.Expand(before, entries)
		}
		writes, err := before.Assign(entries)
		if err != nil {
			return refusedByCheck{err}
		}
		if err := check(before, writes); err != nil {
			return refusedByCheck{err}
		}
		change = PriceChange{
			Name:           before.Now.UTC().Format("20060102150405") + "_" + write.Name,
			Kind:           PriceChangeAPI,
			Reason:         write.Reason,
			Source:         write.Source,
			EffectiveAt:    before.At,
			CreatedBy:      write.ActorID,
			IdempotencyKey: write.IdempotencyKey,
		}
		taken, err := tx.NewSelect().Model((*PriceChange)(nil)).Where("name = ?", change.Name).Exists(ctx)
		if err != nil {
			return err
		}
		if taken {
			return ErrPriceChangeExists
		}
		rows = make([]PriceEntry, 0, len(writes))
		names := make([]string, 0, len(writes))
		for _, entry := range writes {
			row := priceEntryOf(change.Name, entry.Entry)
			row.Withdrawn = entry.Withdrawn
			rows = append(rows, row)
			name := prices.Name(entry.Entry) + " (" + entry.SKU + ")"
			if entry.Withdrawn {
				name += " (withdrawn)"
			}
			names = append(names, name)
		}
		if err := validatePriceChange(&change, rows); err != nil {
			return refusedByCheck{err}
		}
		if _, err := tx.NewInsert().Model(&change).Returning(priceChangeColumns).Exec(ctx); err != nil {
			return err
		}
		if len(rows) > 0 {
			if _, err := tx.NewInsert().Model(&rows).Returning(priceEntryColumns).Exec(ctx); err != nil {
				return err
			}
		}
		changes := []AuditChange{{Field: "effective_at", After: change.EffectiveAt}}
		if len(names) > 0 {
			changes = append(changes, AuditChange{Field: "entries", After: names})
		}
		return recordAuditEntry(ctx, tx, &AuditEntry{
			ResourceType: AuditResourcePriceList,
			ResourceID:   change.Name,
			ResourceName: change.Reason,
			Action:       AuditCreated,
			Source:       AuditSourceManage,
			ActorID:      write.ActorID,
			ActorName:    write.ActorName,
			Changes:      changes,
		})
	})
	var refused refusedByCheck
	switch {
	case errors.As(err, &refused):
		return PriceChange{}, nil, false, refused.err
	case errors.Is(err, ErrPriceListNotSeeded), errors.Is(err, ErrPriceChangeKeyReused),
		errors.Is(err, ErrPriceChangeExists), errors.Is(err, ErrPriceChangeInPast):
		return PriceChange{}, nil, false, err
	case isViolation(err, lockNotAvailable):
		return PriceChange{}, nil, false, ErrPriceListBusy
	case err != nil:
		return PriceChange{}, nil, false, stack.Wrap(fmt.Errorf("store: create price change: %w", err))
	}
	return change, rows, replayed, nil
}

// validatePriceChange checks a change and every entry it writes before any is written.
func validatePriceChange(change *PriceChange, rows []PriceEntry) error {
	if err := change.Validate(); err != nil {
		return stack.Wrap(err)
	}
	for i := range rows {
		if err := rows[i].Validate(); err != nil {
			return stack.Wrap(err)
		}
	}
	return nil
}

// refusedByCheck carries the error a change's check refused it with out of the transaction,
// to be returned as it is rather than as the database failing.
type refusedByCheck struct{ err error }

func (r refusedByCheck) Error() string { return r.err.Error() }

// samePriceChange reports whether a change already written is the one write asks for, so a
// resubmit is answered with it. The effective time asked for matches when it is the one
// written, or when it was no later than the moment the change was made, which it then took.
// Entries are compared in the order they were written, so a resubmit must list them in the
// order the first did: the same entries in another order are another body. An entry that
// asked for no SKU matches the one it was given. With Expand, the entries it wrote beside
// those asked for are left out of the comparison.
func samePriceChange(change PriceChange, rows []PriceEntry, write PriceChangeWrite) bool {
	_, name, _ := cutStamp(change.Name)
	if write.Expand != nil {
		asked := map[string]bool{}
		for _, entry := range write.Entries {
			asked[prices.Name(entry.Entry)] = true
		}
		rows = slices.DeleteFunc(slices.Clone(rows), func(row PriceEntry) bool { return !asked[prices.Name(row.Entry())] })
	}
	if change.Kind != PriceChangeAPI || name != write.Name || change.Reason != write.Reason || change.Source != write.Source ||
		len(rows) != len(write.Entries) {
		return false
	}
	if write.EffectiveAt != nil {
		asked := write.EffectiveAt.UTC().Truncate(time.Microsecond)
		if asked.Before(change.CreatedAt) {
			asked = change.CreatedAt
		}
		if !asked.Equal(change.EffectiveAt) {
			return false
		}
	}
	for i, row := range rows {
		asked := priceEntryOf(change.Name, write.Entries[i].Entry)
		asked.Withdrawn = write.Entries[i].Withdrawn
		if asked.SKU == "" {
			asked.SKU = row.SKU
		}
		if row.Withdrawn != asked.Withdrawn || !reflect.DeepEqual(row.Entry(), asked.Entry()) {
			return false
		}
	}
	return true
}

// cutStamp splits a change's name into the time it was made and the name it was given.
func cutStamp(name string) (stamp, given string, ok bool) {
	const stampLength = len("20060102150405")
	if len(name) <= stampLength || name[stampLength] != '_' {
		return "", name, false
	}
	return name[:stampLength], name[stampLength+1:], true
}

// Cancel cancels a change made through the API that is not yet in force, writing
// its cancel and an audit row in one transaction. A change already cancelled is answered as
// it stands, with cancelled false and nothing written, so a cancel sent again changes nothing.
// A change in force, or one the API did not make, is ErrPriceChangeInForce, and one nobody
// made ErrNoPriceChange.
func (r *PriceChanges) Cancel(ctx context.Context, name, reason, actorID, actorName string) (PriceChange, []PriceEntry, bool, error) {
	var change PriceChange
	var rows []PriceEntry
	var cancelled bool
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.ExecContext(ctx, pricesLockTimeout); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, pricesLock); err != nil {
			return err
		}
		err := tx.NewSelect().Model(&change).Where("name = ?", name).For("UPDATE").Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoPriceChange
		}
		if err != nil {
			return err
		}
		if rows, err = priceEntriesOf(ctx, tx, name); err != nil {
			return err
		}
		if change.CancelledAt != nil {
			return nil
		}
		// The clock now, not the transaction's start: the cancel may have waited for the lock
		// while the change came into force.
		var now time.Time
		if err := tx.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
			return err
		}
		if change.Kind != PriceChangeAPI || !change.EffectiveAt.After(now) {
			return ErrPriceChangeInForce
		}
		change.CancelledAt, change.CancelledBy, change.CancelReason = &now, actorID, reason
		if _, err := tx.NewUpdate().Model(&change).Column("cancelled_at", "cancelled_by", "cancel_reason").WherePK().Exec(ctx); err != nil {
			return err
		}
		cancelled = true
		return recordAuditEntry(ctx, tx, &AuditEntry{
			ResourceType: AuditResourcePriceList,
			ResourceID:   change.Name,
			ResourceName: change.Reason,
			Action:       AuditCancelled,
			Source:       AuditSourceManage,
			ActorID:      actorID,
			ActorName:    actorName,
			Changes: []AuditChange{
				{Field: "cancelled_at", After: now},
				{Field: "cancel_reason", After: reason},
			},
		})
	})
	if err = busyOr(err); errors.Is(err, ErrNoPriceChange) || errors.Is(err, ErrPriceChangeInForce) || errors.Is(err, ErrPriceListBusy) {
		return PriceChange{}, nil, false, err
	}
	if err != nil {
		return PriceChange{}, nil, false, stack.Wrap(fmt.Errorf("store: cancel price change: %w", err))
	}
	return change, rows, cancelled, nil
}
