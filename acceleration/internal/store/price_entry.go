package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// PriceEntry is one item's price, as a change wrote it. Rows are only ever added: the entry
// in force for an item is the newest whose EffectiveAt has passed under a change that was not
// cancelled, and a newer row replaces an older one whole.
type PriceEntry struct {
	bun.BaseModel `bun:"table:price_list_entries,alias:pe"`

	ID     int64  `bun:"id,pk,autoincrement"`
	Change string `bun:"change,notnull"`
	// SKU names the item across every change of its price (AssignSKUs).
	SKU string `bun:"sku,notnull"`
	// Modality, Provider, Model, Region and BYOK are the item; Attributes is the entry's When:
	// the item attributes it applies to, such as {"service_tier": "flex"}. Empty is the default
	// entry.
	Modality   string            `bun:"modality,notnull"`
	Provider   string            `bun:"provider,notnull"`
	Model      string            `bun:"model,notnull"`
	Region     string            `bun:"region,notnull"`
	BYOK       bool              `bun:"byok,notnull"`
	Attributes map[string]string `bun:"attributes,type:jsonb,nullzero"`
	Free       bool              `bun:"free,notnull"`
	// Rates are the vendor's, for reference: nothing is charged from them.
	Rates map[pricing.Meter]pricing.Rate `bun:"rates,type:jsonb,nullzero"`
	// Tiers are the vendor's context tiers: a longer prompt costed at other rates, whole.
	Tiers []pricing.Tier `bun:"tiers,type:jsonb,nullzero"`
	// Prices are what the work is charged, by the meters of Rates; PriceTiers are its context
	// tiers, at the thresholds of Tiers.
	Prices     map[pricing.Meter]pricing.Rate `bun:"prices,type:jsonb,nullzero"`
	PriceTiers []pricing.Tier                 `bun:"price_tiers,type:jsonb,nullzero"`
	// Withdrawn stops pricing the item under these Attributes from EffectiveAt on; its
	// entries under other Attributes are untouched.
	Withdrawn bool `bun:"withdrawn,notnull"`
	// EffectiveAt is stamped from the change by the price_entries_guard trigger, so an insert
	// leaves it zero and no entry can backdate itself.
	EffectiveAt time.Time `bun:"effective_at,nullzero"`
	CreatedAt   time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}

// Validate refuses an entry the table would: one naming its change, a SKU and a region, and a
// price unless it is free, withdrawn or has no vendor rates to price from.
func (e *PriceEntry) Validate() error {
	return validate("price entry").
		required("change", e.Change).
		required("sku", e.SKU).
		required("region", e.Region).
		check(e.Free || e.Withdrawn || len(e.Rates) == 0 || len(e.Prices) > 0, "prices", "is empty for an entry with vendor rates").
		done()
}

// PriceEntries are the price list's entries, in the shared database: what each change wrote,
// read as the list in force at a moment.
type PriceEntries struct {
	store *Store
}

// priceEntryColumns are PriceEntry's columns, read and returned by name rather than as *, so
// a column a later release adds or drops does not break the read.
const priceEntryColumns = "id, change, sku, modality, provider, model, region, byok, attributes, free, rates, tiers, prices, " +
	"price_tiers, withdrawn, effective_at, created_at"

// Entry is the row as a price list prices with it, its Version the change that wrote it.
func (e PriceEntry) Entry() pricing.Entry {
	entry := pricing.Entry{
		Modality:   e.Modality,
		Provider:   e.Provider,
		Model:      e.Model,
		Region:     e.Region,
		BYOK:       e.BYOK,
		SKU:        e.SKU,
		Free:       e.Free,
		Rates:      e.Rates,
		Tiers:      e.Tiers,
		Prices:     e.Prices,
		PriceTiers: e.PriceTiers,
		Version:    e.Change,
	}
	if len(e.Attributes) > 0 {
		entry.When = e.Attributes
	}
	if len(e.Rates) == 0 {
		entry.Rates = nil
	}
	if len(e.Tiers) == 0 {
		entry.Tiers = nil
	}
	if len(e.Prices) == 0 {
		entry.Prices = nil
	}
	if len(e.PriceTiers) == 0 {
		entry.PriceTiers = nil
	}
	return entry
}

// priceEntryOf is the row a change writes for an entry. Local and Version have no column:
// local is the routing config's to say, and the version is the change itself.
func priceEntryOf(change string, entry pricing.Entry) PriceEntry {
	return PriceEntry{
		Change:     change,
		SKU:        entry.SKU,
		Modality:   entry.Modality,
		Provider:   entry.Provider,
		Model:      entry.Model,
		Region:     entry.Region,
		BYOK:       entry.BYOK,
		Attributes: entry.When,
		Free:       entry.Free,
		Rates:      entry.Rates,
		Tiers:      entry.Tiers,
		Prices:     entry.Prices,
		PriceTiers: entry.PriceTiers,
	}
}

// Active is the entry in force at at for every item, region, BYOK state and When:
// the newest that has taken effect under a change not cancelled, ties going to the later
// change. An item whose newest entry withdrew it is left out.
func (r *PriceEntries) Active(ctx context.Context, at time.Time) ([]PriceEntry, error) {
	return activePriceEntries(ctx, r.store.global, at)
}

func activePriceEntries(ctx context.Context, db bun.IDB, at time.Time) ([]PriceEntry, error) {
	var newest []PriceEntry
	err := db.NewSelect().Model(&newest).
		DistinctOn("pe.modality, pe.provider, pe.model, pe.region, pe.byok, pe.attributes").
		Join("JOIN price_changes AS pc ON pc.name = pe.change AND pc.cancelled_at IS NULL").
		Where("pe.effective_at <= ?", at).
		Order("pe.modality", "pe.provider", "pe.model", "pe.region", "pe.byok", "pe.attributes").
		OrderExpr("pe.effective_at DESC, pe.change DESC, pe.id DESC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: active price entries: %w", err))
	}
	var active []PriceEntry
	for _, entry := range newest {
		if !entry.Withdrawn {
			active = append(active, entry)
		}
	}
	return active, nil
}

// UpcomingPriceEntry is an entry a change not yet in force will set, with what the change
// says about it.
type UpcomingPriceEntry struct {
	PriceEntry
	Reason string
}

// upcomingPriceEntries is every entry of a change not cancelled that takes effect after
// after, soonest first.
func upcomingPriceEntries(ctx context.Context, db bun.IDB, after time.Time) ([]UpcomingPriceEntry, error) {
	var changes []PriceChange
	err := db.NewSelect().Model(&changes).
		Where("cancelled_at IS NULL").
		Where("effective_at > ?", after).
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: upcoming price entries: %w", err))
	}
	if len(changes) == 0 {
		return nil, nil
	}
	reasons := make(map[string]string, len(changes))
	for _, change := range changes {
		reasons[change.Name] = change.Reason
	}
	var entries []PriceEntry
	err = db.NewSelect().Model(&entries).
		Where("change IN (?)", bun.In(slices.Collect(maps.Keys(reasons)))).
		Order("effective_at", "change", "id").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: upcoming price entries: %w", err))
	}
	upcoming := make([]UpcomingPriceEntry, 0, len(entries))
	for _, entry := range entries {
		upcoming = append(upcoming, UpcomingPriceEntry{PriceEntry: entry, Reason: reasons[entry.Change]})
	}
	return upcoming, nil
}

// priceEntriesOf is the entries a change wrote, in the order it wrote them.
func priceEntriesOf(ctx context.Context, db bun.IDB, name string) ([]PriceEntry, error) {
	entries := []PriceEntry{}
	if err := db.NewSelect().Model(&entries).Where("change = ?", name).Order("id").Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: price change: %w", err))
	}
	return entries, nil
}

// Source is the price list in force as pricing.Active reads it.
func (r *PriceEntries) Source() pricing.Source {
	return priceSource{store: r.store}
}

type priceSource struct {
	store *Store
}

// Active reads the entries and the next change in one snapshot, so a change committed between
// the reads cannot leave the list with half of it.
func (p priceSource) Active(ctx context.Context, at time.Time) ([]pricing.Entry, time.Time, error) {
	var entries []pricing.Entry
	var next time.Time
	err := p.store.global.RunInTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		rows, err := activePriceEntries(ctx, tx, at)
		if err != nil {
			return err
		}
		entries = make([]pricing.Entry, 0, len(rows))
		for _, row := range rows {
			entry := row.Entry()
			entry.EffectiveAt = row.EffectiveAt.UTC()
			entries = append(entries, entry)
		}
		found, ok, err := nextPriceChangeAt(ctx, tx, at)
		if ok {
			next = found
		}
		return err
	})
	if err != nil {
		return nil, time.Time{}, stack.Wrap(fmt.Errorf("store: the price list in force: %w", err))
	}
	return entries, next, nil
}

func (p priceSource) Upcoming(ctx context.Context, after time.Time) ([]pricing.UpcomingEntry, error) {
	rows, err := upcomingPriceEntries(ctx, p.store.global, after)
	if err != nil {
		return nil, err
	}
	upcoming := make([]pricing.UpcomingEntry, 0, len(rows))
	for _, row := range rows {
		upcoming = append(upcoming, pricing.UpcomingEntry{
			Entry: row.Entry(), EffectiveAt: row.EffectiveAt, Withdrawn: row.Withdrawn, Reason: row.Reason,
		})
	}
	return upcoming, nil
}

// PriceListSnapshot is the price list in force at a moment and what is scheduled after it,
// read in one snapshot so a change committed between the reads cannot show half of itself.
type PriceListSnapshot struct {
	Entries []PriceEntry
	// Upcoming are the entries of the changes not cancelled that take effect later, soonest
	// first.
	Upcoming []UpcomingPriceEntry
}

// At reads the price list in force at at, and what is scheduled after it.
func (r *PriceEntries) At(ctx context.Context, at time.Time) (PriceListSnapshot, error) {
	var list PriceListSnapshot
	err := r.store.global.RunInTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		var err error
		if list.Entries, err = activePriceEntries(ctx, tx, at); err != nil {
			return err
		}
		list.Upcoming, err = upcomingPriceEntries(ctx, tx, at)
		return err
	})
	if err != nil {
		return PriceListSnapshot{}, stack.Wrap(fmt.Errorf("store: price list at %s: %w", at.Format(time.RFC3339), err))
	}
	return list, nil
}

// PriceEntryWrite is an entry a change writes: the item's whole entry under its When, its
// vendor's rates and its price, or its withdrawal. An Entry.SKU left empty is assigned
// (AssignSKUs).
type PriceEntryWrite struct {
	pricing.Entry
	// Withdrawn stops pricing the item under Entry.When from the change's effective time on;
	// its entries under other Whens are untouched.
	Withdrawn bool
}

// PriceListAt is the price list in force at the moment a change would take effect, before it.
type PriceListAt struct {
	// Now is the database's clock, and At when the change would take effect.
	Now, At time.Time
	Entries []PriceEntry
	// SKUs is the SKU every entry of a change not cancelled gave its item, for AssignSKUs.
	SKUs []PriceSKU
}

// Assign is AssignSKUs of writes on the list a change lands on.
func (l PriceListAt) Assign(writes []PriceEntryWrite) ([]PriceEntryWrite, error) {
	return AssignSKUs(l.SKUs, l.At, writes)
}

// priceListAt reads the list a change asked to take effect at asked lands on.
func priceListAt(ctx context.Context, db bun.IDB, asked *time.Time) (PriceListAt, error) {
	var list PriceListAt
	var err error
	// Read once Insert holds the lock, which it may have waited for.
	if list.Now, err = databaseClock(ctx, db); err != nil {
		return PriceListAt{}, err
	}
	if list.At, err = effectiveAt(list.Now, asked); err != nil {
		return PriceListAt{}, err
	}
	if list.Entries, err = activePriceEntries(ctx, db, list.At); err != nil {
		return PriceListAt{}, err
	}
	if list.SKUs, err = priceSKUs(ctx, db); err != nil {
		return PriceListAt{}, err
	}
	return list, nil
}

// Before is the list a change asked to take effect at asked would land on, writing
// nothing: what a preview compares the change with.
func (r *PriceEntries) Before(ctx context.Context, asked *time.Time) (PriceListAt, error) {
	var list PriceListAt
	err := r.store.global.RunInTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		var err error
		list, err = priceListAt(ctx, tx, asked)
		return err
	})
	if errors.Is(err, ErrPriceChangeInPast) {
		return PriceListAt{}, err
	}
	if err != nil {
		return PriceListAt{}, stack.Wrap(fmt.Errorf("store: price list before a change: %w", err))
	}
	return list, nil
}
