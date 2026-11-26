package store

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/redis/rueidis/rueidishook"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// A deployment may keep the rows every part of it agrees on in one database and the calls each
// part runs in a database of its own, the shard's. Calls, sessions and their turns, the
// requests a call made and the rollups of them are the shard's: written on a call's hot path,
// close to where it ran. Everything else is shared.
//
// A deployment of one shard sets one DSN, and every handle below is the same database.

// Options says where a store's databases are.
type Options struct {
	// DSN is the database every shard shares.
	DSN string
	// Shard names this shard, as the router's shard setting does. Empty names it by a digest of
	// its DSN.
	Shard string
	// ShardDSN is this shard's, where the calls it runs are written. Empty is DSN.
	ShardDSN string
	// Shards names every shard, this one's included, for the work each shard runs on its own
	// rows (ShardJobs). Empty is this shard alone.
	Shards []string
	// MigrateDSN and ShardMigrateDSN are what Migrate connects with to DSN's database and to
	// ShardDSN's, as the role that owns the schema. Empty is DSN and ShardDSN.
	MigrateDSN      string
	ShardMigrateDSN string
	// Redis is where rows read on every request are cached. Empty reads Postgres every time.
	Redis Redis
	// ShardRedis is where this shard's running calls keep their state. Empty is Redis.
	ShardRedis Redis
	// redisHook sees every Redis command, for the tests counting round trips.
	redisHook rueidishook.Hook
	// writeTimeout bounds a write of a cached row. Zero is writeTimeout; a test shortens it.
	writeTimeout time.Duration
}

// OpenShards connects to the shared database and this shard's.
func OpenShards(options Options) (*Store, error) {
	if options.DSN == "" {
		return nil, errors.New("store: dsn is required")
	}
	db, err := connect(options.DSN, maxConns)
	if err != nil {
		return nil, err
	}
	locks, err := connect(options.DSN, lockConns)
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	s := &Store{global: db, shard: db, locks: locks,
		globalDSN: cmp.Or(options.MigrateDSN, options.DSN), shardDSN: cmp.Or(options.MigrateDSN, options.DSN),
		retries: make(chan struct{}, forgetSlots), lost: make(chan struct{}, forgetSlots), polls: make(chan struct{}, pollSlots), writeTimeout: cmp.Or(options.writeTimeout, writeTimeout)}
	s.closing, s.stop = context.WithCancel(context.Background())
	if options.ShardDSN != "" && options.ShardDSN != options.DSN {
		shard, err := connect(options.ShardDSN, maxConns)
		if err != nil {
			return nil, errors.Join(err, s.Close())
		}
		s.shard = shard
		s.shardDSN = cmp.Or(options.ShardMigrateDSN, options.ShardDSN)
	}
	s.shardName = cmp.Or(options.Shard, unnamedShard(cmp.Or(options.ShardDSN, options.DSN)))
	s.shardNames = append([]string{s.shardName}, slices.DeleteFunc(slices.Sorted(slices.Values(options.Shards)),
		func(name string) bool { return name == s.shardName })...)
	s.homed = options.Shard != "" || s.Sharded()
	// Wired first: the shared Redis client tells Users of stale keys as soon as it connects.
	s.wire()
	if err := s.openRedis(options); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	return s, nil
}

// unnamedShard names a shard Options.Shard does not: by a digest of its DSN, which says nothing
// of it.
func unnamedShard(dsn string) string {
	sum := sha256.Sum256([]byte(dsn))
	return "dsn:" + hex.EncodeToString(sum[:8])
}

// ShardName names this shard.
func (s *Store) ShardName() string { return s.shardName }

// Sharded reports whether this shard's calls are kept apart from the shared database.
func (s *Store) Sharded() bool { return s.shard != s.global }

// HasShards reports whether any shard's tables are kept outside the shared database: this
// shard's own, or another shard's.
func (s *Store) HasShards() bool { return s.Sharded() || len(s.shardNames) > 1 }

// A table moving from the shared database to the shards' is written in this shard's and read
// from both: this shard's, where its rows are written now, and the shared one, where they were
// written before, until its rows there are copied over and it is dropped. A database that does
// not have the table yet holds none of its rows, rather than failing the read.

// holders is every database that may hold this shard's rows of a moving table: this shard's,
// then the shared one unless it is this shard's.
func (s *Store) holders() []bun.IDB {
	if s.Sharded() {
		return []bun.IDB{s.shard, s.global}
	}
	return []bun.IDB{s.shard}
}

// everyHolder reads a moving table from every database of holders at once, in its order.
func everyHolder[T any](ctx context.Context, s *Store, read func(context.Context, bun.IDB) (T, error)) ([]T, error) {
	holders := s.holders()
	found := make([]T, len(holders))
	failed := make([]error, len(holders))
	var wg sync.WaitGroup
	for i, db := range holders {
		wg.Go(func() {
			found[i], failed[i] = read(ctx, db)
			if absentTable(failed[i]) {
				var none T
				found[i], failed[i] = none, nil
			}
		})
	}
	wg.Wait()
	if err := errors.Join(failed...); err != nil {
		return nil, err
	}
	return found, nil
}

// absentTable reports whether err is Postgres refusing a statement on a table the database
// does not have.
func absentTable(err error) bool {
	var refused pgdriver.Error
	return errors.As(err, &refused) && refused.Field('C') == undefinedTable
}

// merged puts every shard's rows into one list in the order before says, cut to limit when
// limit is above zero. The sort is stable, so rows that tie keep this shard's first.
func merged[T any](lists [][]T, before func(a, b T) int, limit int) []T {
	if len(lists) == 1 {
		return lists[0]
	}
	var all []T
	for _, list := range lists {
		all = append(all, list...)
	}
	slices.SortStableFunc(all, before)
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all
}

// distinct keeps the first row of each key. A text session carried on in another shard than
// the one it began in has a row in both, and the first, in an order newest first, is the one
// that says what it is now.
func distinct[T any](rows []T, key func(T) string) []T {
	seen := make(map[string]bool, len(rows))
	kept := rows[:0]
	for _, row := range rows {
		if !seen[key(row)] {
			seen[key(row)] = true
			kept = append(kept, row)
		}
	}
	return kept
}

// closeShards closes this shard's database when it is not the shared one.
func (s *Store) closeShards() error {
	if !s.Sharded() {
		return nil
	}
	return stack.Wrap(s.shard.Close())
}
