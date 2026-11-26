package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

const (
	// statsShipEvery is how often this shard's stats of the hour are shipped, and so how old
	// what a stats page reads can be.
	statsShipEvery = time.Minute
	// statsShipTimeout bounds one ship, so a holder whose database stopped answering lets go of
	// the lock rather than keep it while nobody ships. A backfill longer than one ship is
	// carried on by the next.
	statsShipTimeout = 2 * time.Minute
	// statsLockTimeout bounds taking or checking the lock.
	statsLockTimeout = 10 * time.Second
	// statsShipBatch is how many rows one insert of a ship writes into the shared database, so
	// a busy day is several statements well inside the statement limit rather than one.
	statsShipBatch = 1000
)

// StatsShipped reports whether the stats, spend and activity pages read the rollups each shard
// ships to the shared database (ShipStats) rather than this shard: a deployment of named
// shards keeps each one's requests and sessions in it alone.
func (s *Store) StatsShipped() bool { return s.homed }

// StatsShipLock makes one router of this shard the one that ships its stats.
func (s *Store) StatsShipLock() *ShipLock {
	return &ShipLock{db: s.shard, key: "router.stats.ship"}
}

// shippedTable is a shard's rollup table as its copy in the shared database is written.
type shippedTable struct {
	// name is the copy's: the shard's table's, after shard_.
	name string
	// key is the shard's table's primary key. The copy's adds the shard.
	key    []string
	values []string
}

var requestStatsValues = []string{
	"audio_ms_total", "characters_total", "input_tokens_total", "cached_input_tokens_total", "output_tokens_total",
	"images_total", "cost_micros_total", "request_count", "error_count", "latency_p50_ms", "latency_p95_ms",
}

func shippedStats(granularity Granularity) shippedTable {
	return shippedTable{name: "shard_" + granularity.table(),
		key: []string{"modality", "customer_id", "provider", "model", "bucket"}, values: requestStatsValues}
}

func shippedTagStats(granularity Granularity) shippedTable {
	return shippedTable{name: "shard_" + granularity.tagTable(),
		key: []string{"modality", "customer_id", "tag_key", "tag_value", "bucket"}, values: requestStatsValues}
}

var (
	shippedTurns = shippedTable{name: "shard_turn_stats_hourly",
		key: []string{"customer_id", "agent_id", "bucket"}, values: []string{"turn_count", "interrupted_count", "audio_out_ms_total"}}
	shippedLatency = shippedTable{name: "shard_turn_latency_hourly",
		key: []string{"customer_id", "agent_id", "bucket", "leg", "bin"}, values: []string{"count"}}
)

// statsShipped is how far a shard has shipped its stats.
type statsShipped struct {
	bun.BaseModel `bun:"table:shard_stats_shipped"`

	Shard          string    `bun:"shard,pk"`
	ShippedThrough time.Time `bun:"shipped_through"`
}

// ShipStats writes this shard's stats into the shared database, where the stats pages read
// every shard's: the hour now is in and the one before, and the UTC days they are in. The hour
// before is shipped again once it has closed, for the requests and turns written after it
// ended: a request is recorded as it finishes. A shard that has never shipped ships every hour
// it has first, and one that stopped shipping for a while every hour since. Each UTC day is
// written before the next and how far it got is kept, so a ship cut short carries on where it
// stopped. A shipped bucket replaces what the shard shipped of it before, so shipping it again
// changes nothing.
//
// The request stats are aggregated from the requests rather than read from the shard's rollup
// tables, which only an operator's rollup writes and whose keys read no window of time.
func (s *Store) ShipStats(ctx context.Context, now time.Time) error {
	shard := s.shardName
	current := now.UTC().Truncate(time.Hour)
	from := current.Add(-time.Hour)
	var mark statsShipped
	err := s.global.NewSelect().Model(&mark).Where("shard = ?", shard).Scan(ctx)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		earliest, err := s.earliestStats(ctx)
		if err != nil {
			return err
		}
		if earliest.Valid && earliest.Time.Before(from) {
			from = earliest.Time.UTC().Truncate(time.Hour)
		}
	case err != nil:
		return stack.Wrap(fmt.Errorf("store: ship stats: %w", err))
	case mark.ShippedThrough.Before(from):
		from = mark.ShippedThrough
	}

	end := current.Add(time.Hour)
	for start := from; start.Before(end); {
		until := Daily.next(start)
		if until.After(end) {
			until = end
		}
		if err := s.shipStatsOf(ctx, shard, start, until); err != nil {
			return err
		}
		through := until
		if through.After(current) {
			through = current
		}
		_, err := s.global.NewInsert().Model(&statsShipped{Shard: shard, ShippedThrough: through}).
			On("CONFLICT (shard) DO UPDATE").
			Set("shipped_through = EXCLUDED.shipped_through").
			Set("shipped_at = now()").
			Exec(ctx)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: ship stats: mark: %w", err))
		}
		start = until
	}
	return nil
}

// earliestStats is the start of the first request, turn, session or call this shard counted,
// not valid for a shard that counted none.
func (s *Store) earliestStats(ctx context.Context) (sql.NullTime, error) {
	var earliest sql.NullTime
	err := s.shard.NewSelect().
		ColumnExpr("LEAST((?), (?), (?), (?))",
			s.shard.NewSelect().TableExpr("requests").ColumnExpr("min(started_at)"),
			s.shard.NewSelect().TableExpr("turn_stats_hourly").ColumnExpr("min(bucket)"),
			s.shard.NewSelect().TableExpr("agent_sessions").ColumnExpr("min(created_at)"),
			s.shard.NewSelect().TableExpr("calls").ColumnExpr("min(started_at)")).
		Scan(ctx, &earliest)
	if err != nil {
		return earliest, stack.Wrap(fmt.Errorf("store: ship stats: earliest: %w", err))
	}
	return earliest, nil
}

// shipStatsOf ships every rollup's buckets [from, to) touches, a window inside one UTC day.
func (s *Store) shipStatsOf(ctx context.Context, shard string, from, to time.Time) error {
	for _, granularity := range []Granularity{Hourly, Daily} {
		if err := s.shipRows(ctx, shard, shippedStats(granularity), rollupSelect(granularity), from, to); err != nil {
			return err
		}
		if err := s.shipRows(ctx, shard, shippedTagStats(granularity), tagRollupSelect(granularity), from, to); err != nil {
			return err
		}
	}
	for table, copied := range map[string]shippedTable{"turn_stats_hourly": shippedTurns, "turn_latency_hourly": shippedLatency} {
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
			strings.Join(slices.Concat(copied.key, copied.values), ", "), table, wholeBuckets(Hourly, "bucket"))
		if err := s.shipRows(ctx, shard, copied, query, from, to); err != nil {
			return err
		}
	}
	for _, table := range replacedRollups {
		if err := s.replaceRows(ctx, shard, table, from, to); err != nil {
			return err
		}
	}
	return nil
}

// shipRows writes the rows query reads from this shard, given from and to, into table of the
// shared database as the shard's, each replacing the one the shard shipped under its key. The
// rows cross between the two databases as JSON, which keeps every column's type as Postgres
// has it.
func (s *Store) shipRows(ctx context.Context, shard string, table shippedTable, query string, from, to time.Time) error {
	var encoded []byte
	err := s.shard.NewSelect().
		ColumnExpr("COALESCE(json_agg(shipped), '[]')").
		TableExpr("("+query+") AS shipped", from, to).
		Scan(ctx, &encoded)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: ship %s: read: %w", table.name, err))
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(encoded, &rows); err != nil {
		return stack.Wrap(fmt.Errorf("store: ship %s: decode: %w", table.name, err))
	}

	columns := strings.Join(slices.Concat(table.key, table.values), ", ")
	set := make([]string, 0, len(table.values))
	for _, column := range table.values {
		set = append(set, column+" = EXCLUDED."+column)
	}
	// INSERT ... SELECT from the JSON, which the builders do not write.
	insert := fmt.Sprintf(`INSERT INTO %[1]s (shard, %[2]s)
SELECT ?, %[2]s FROM jsonb_populate_recordset(NULL::%[1]s, ?::jsonb)
ON CONFLICT (shard, %[3]s) DO UPDATE SET %[4]s`, table.name, columns, strings.Join(table.key, ", "), strings.Join(set, ", "))
	for batch := range slices.Chunk(rows, statsShipBatch) {
		encoded, err := json.Marshal(batch)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: ship %s: encode: %w", table.name, err))
		}
		if _, err := s.global.ExecContext(ctx, insert, shard, string(encoded)); err != nil {
			return stack.Wrap(fmt.Errorf("store: ship %s: %w", table.name, err))
		}
	}
	return nil
}

// Shipper ships what this shard keeps that the shared database answers for every shard: its
// stats every minute (Store.ShipStats), and its sessions' homes and list rows every few seconds
// (Store.ShipIndex). Every router of a deployment with shards runs one of each, and the one
// holding the shard's lock for it ships; the others try the lock again at the next tick.
type Shipper struct {
	lock    *ShipLock
	logger  *slog.Logger
	every   time.Duration
	timeout time.Duration
	what    string
	run     func(context.Context, time.Time) error
	// holding is whether the last tick held the lock and shipped.
	holding atomic.Bool
}

// StatsShipper returns a shipper of this shard's stats, shipping every interval, or every
// minute for none; Run starts it.
func (s *Store) StatsShipper(logger *slog.Logger, every time.Duration) *Shipper {
	return &Shipper{lock: s.StatsShipLock(), logger: logger, every: cmp.Or(every, statsShipEvery),
		timeout: statsShipTimeout, what: "stats", run: s.ShipStats}
}

// IndexShipper returns a shipper of this shard's session homes and list rows, shipping every
// interval, or every indexShipEvery for none; Run starts it.
func (s *Store) IndexShipper(logger *slog.Logger, every time.Duration) *Shipper {
	return &Shipper{lock: &ShipLock{db: s.shard, key: "router.index.ship"}, logger: logger,
		every: cmp.Or(every, indexShipEvery), timeout: statsShipTimeout, what: "index", run: s.ShipIndex}
}

// Run ships at once and then every interval until ctx is done, and then gives the lock back.
func (p *Shipper) Run(ctx context.Context) {
	defer p.lock.Release()
	ticker := time.NewTicker(p.every)
	defer ticker.Stop()
	for {
		if err := p.ship(ctx); err != nil && ctx.Err() == nil {
			p.logger.Error("could not ship this shard's "+p.what+", trying again", "in", p.every, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Holding reports whether this shipper held its shard's lock and shipped at its last tick.
func (p *Shipper) Holding() bool { return p.holding.Load() }

// ship ships when this router holds the lock. A holder whose ship fails lets the lock go, so
// that a router that can ship takes it at its next tick.
func (p *Shipper) ship(ctx context.Context) error {
	locking, cancel := context.WithTimeout(ctx, statsLockTimeout)
	held, err := p.lock.Hold(locking)
	cancel()
	p.holding.Store(held)
	if err != nil || !held {
		return err
	}
	shipping, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if err := p.run(shipping, time.Now()); err != nil {
		p.lock.Release()
		p.holding.Store(false)
		return err
	}
	return nil
}
