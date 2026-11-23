package store

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/redis/rueidis/rueidishook"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// A deployment may keep the rows every part of it agrees on in one database and the calls each
// part runs in a database of its own, the shard's. Calls, sessions and their turns, the
// requests a call made and the rollups of them are the shard's: written on a call's hot path,
// close to where it ran. Everything else is shared.
//
// A deployment of one shard sets one DSN, and every handle below is the same database.

// peerConns is how many connections a router holds to another shard's database. It only reads
// there, for a page somebody is looking at, so it needs far fewer than its own.
const peerConns = 8

// Options says where a store's databases are.
type Options struct {
	// DSN is the database every shard shares.
	DSN string
	// ShardDSN is this shard's, where the calls it runs are written. Empty is DSN.
	ShardDSN string
	// Shards is every shard's DSN by name, this one's included. Reads of calls, sessions and
	// requests cover them all. Empty is this shard's alone.
	Shards map[string]string
	// Redis is where rows read on every request are cached. Empty reads Postgres every time.
	Redis Redis
	// ShardRedis is where this shard's running calls keep their state. Empty is Redis.
	ShardRedis Redis
	// redisHook sees every Redis command, for the tests counting round trips.
	redisHook rueidishook.Hook
}

// OpenShards connects to the shared database, this shard's, and every other shard's.
func OpenShards(options Options) (*Store, error) {
	if options.DSN == "" {
		return nil, errors.New("store: dsn is required")
	}
	db := connect(options.DSN, maxConns)
	s := &Store{global: db, shard: db, globalDSN: options.DSN, shardDSN: options.DSN}
	if options.ShardDSN != "" && options.ShardDSN != options.DSN {
		s.shard = connect(options.ShardDSN, maxConns)
		s.shardDSN = options.ShardDSN
	}
	own := cmp.Or(options.ShardDSN, options.DSN)
	s.shardNames = []string{unnamedShard(own)}
	for _, name := range slices.Sorted(maps.Keys(options.Shards)) {
		switch dsn := options.Shards[name]; {
		case dsn == own:
			s.shardNames[0] = name
			s.globalIsShard = s.globalIsShard || dsn == options.DSN
		case dsn != "":
			s.globalIsShard = s.globalIsShard || dsn == options.DSN
			s.peers = append(s.peers, connect(dsn, peerConns))
			s.shardNames = append(s.shardNames, name)
		}
	}
	// Wired first: the shared Redis client tells Users of stale keys as soon as it connects.
	s.wire()
	if err := s.openRedis(options); err != nil {
		return nil, errors.Join(err, s.Close())
	}
	return s, nil
}

// unnamedShard names a shard Options.Shards does not: by a digest of its DSN, which says
// nothing of it.
func unnamedShard(dsn string) string {
	sum := sha256.Sum256([]byte(dsn))
	return "dsn:" + hex.EncodeToString(sum[:8])
}

// Sharded reports whether this shard's calls are kept apart from the shared database.
func (s *Store) Sharded() bool { return s.shard != s.global }

// HasShards reports whether any shard's tables are kept outside the shared database: this
// shard's own, or another shard's.
func (s *Store) HasShards() bool { return s.Sharded() || len(s.peers) > 0 }

// shards is every shard's database, this one's first.
func (s *Store) shards() []bun.IDB {
	all := make([]bun.IDB, 0, 1+len(s.peers))
	all = append(all, s.shard)
	for _, peer := range s.peers {
		all = append(all, peer)
	}
	return all
}

// everyShard reads every shard's database at once and returns what each answered, this
// shard's first. One shard failing fails the read: a page that silently left a shard out
// would be a list of calls missing some.
func everyShard[T any](ctx context.Context, s *Store, read func(context.Context, bun.IDB) (T, error)) ([]T, error) {
	all := s.shards()
	if len(all) == 1 {
		found, err := read(ctx, all[0])
		if err != nil {
			return nil, err
		}
		return []T{found}, nil
	}
	found := make([]T, len(all))
	failed := make([]error, len(all))
	var wg sync.WaitGroup
	for i, db := range all {
		wg.Go(func() { found[i], failed[i] = read(ctx, db) })
	}
	wg.Wait()
	if err := errors.Join(failed...); err != nil {
		return nil, err
	}
	return found, nil
}

// ErrShardUnanswered is a read no shard that answered could settle while another did not: the
// row may be on that one, so asking again once it answers may find it.
var ErrShardUnanswered = errors.New("store: a shard that may hold it did not answer")

// peerReadTimeout bounds a read of another shard's database that one shard's answer can settle
// (answeringShards). Each is one indexed lookup, answered in milliseconds by a peer that is up,
// even across regions. A peer partitioned away would otherwise hold the caller for pgdriver's
// 5 s dial timeout, or its 10 s read timeout on a host that accepts and never answers, and the
// callers here are a vendor and Stream waiting on a call.
const peerReadTimeout = 2 * time.Second

// claimReadTimeout bounds a bridge claim's read of another shard, longer than peerReadTimeout.
// The claim deletes what it reads, and a peer that commits the delete after the caller stopped
// waiting has spent the bridge with nobody to hand it to. The longer bound narrows that to a
// peer slower than pgdriver's own 5 s dial timeout, which is one that is down; a vendor's fetch
// is one per answered call, so holding a peer connection that long is not load.
const claimReadTimeout = 5 * time.Second

// answeringShards reads every shard's database at once, as everyShard does, for a read one
// shard's answer can settle: it returns what each shard answered and, apart, how each that
// did not answer failed, this shard's first. A shard down then costs only what is on it.
//
// Another shard's read is given bound. Once an answer settles the read, the others are
// cancelled and not waited for, so a slow peer gives its pool connection back at once: their
// entries stay zero. Each shard that failed before the answer settled is logged
// (settledWithout); one cut off by the settle is not known to be down, and a shard that is
// down fails the reads that do need it.
func answeringShards[T any](ctx context.Context, s *Store, read string, bound time.Duration, settles func(T) bool, query func(context.Context, bun.IDB) (T, error)) ([]T, []error) {
	all := s.shards()
	found := make([]T, len(all))
	failed := make([]error, len(all))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type answer struct {
		shard int
		found T
		err   error
	}
	// Buffered for every shard, so a read still running when the answer is settled finishes
	// into it rather than waiting on a reader that has gone.
	answers := make(chan answer, len(all))
	for i, db := range all {
		go func() {
			readCtx := ctx
			if i > 0 {
				var stop context.CancelFunc
				readCtx, stop = context.WithTimeout(ctx, bound)
				defer stop()
			}
			found, err := query(readCtx, db)
			answers <- answer{shard: i, found: found, err: err}
		}()
	}
	for range all {
		got := <-answers
		found[got.shard], failed[got.shard] = got.found, got.err
		if got.err != nil || !settles(got.found) {
			continue
		}
		for shard, err := range failed {
			s.settledWithout(ctx, read, shard, err)
		}
		break
	}
	return found, failed
}

// settledWithout logs a shard that failed a read the others' answers settled, at most once a
// minute a shard: a shard that is down fails every call's reads until it is back. A read the
// caller cancelled says nothing of its shard.
func (s *Store) settledWithout(ctx context.Context, read string, shard int, err error) {
	if err != nil && !errors.Is(err, context.Canceled) && s.shardsDown.due(s.shardNames[shard], time.Now()) {
		slog.Default().WarnContext(ctx, "a shard did not answer, settled on the others", "read", read, "shard", s.shardNames[shard], "error", err)
	}
}

// shardWarnings is when each shard was last logged as not answering. Its keys are the shards'
// names, so it holds at most one entry a shard.
type shardWarnings struct {
	mu   sync.Mutex
	last map[string]time.Time
}

// due reports whether shard may be logged at now, and counts it as logged if so.
func (w *shardWarnings) due(shard string, now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if last, ok := w.last[shard]; ok && now.Sub(last) < time.Minute {
		return false
	}
	if w.last == nil {
		w.last = map[string]time.Time{}
	}
	w.last[shard] = now
	return true
}

// whicheverShard reads a row from the shard holding it: this one first, where the row almost
// always is, then the others. read answers sql.ErrNoRows for a shard without it, and so does
// whicheverShard when no shard has it.
func whicheverShard[T any](ctx context.Context, s *Store, read func(context.Context, bun.IDB) (T, error)) (T, error) {
	found, err := read(ctx, s.shard)
	if len(s.peers) == 0 || !errors.Is(err, sql.ErrNoRows) {
		return found, err
	}
	answers := make([]T, len(s.peers))
	failed := make([]error, len(s.peers))
	var wg sync.WaitGroup
	for i, peer := range s.peers {
		wg.Go(func() { answers[i], failed[i] = read(ctx, peer) })
	}
	wg.Wait()
	for i := range s.peers {
		if failed[i] == nil {
			return answers[i], nil
		}
	}
	for _, err := range failed {
		if !errors.Is(err, sql.ErrNoRows) {
			return found, err
		}
	}
	return found, sql.ErrNoRows
}

// onEveryShard runs a write on every shard's database and returns how many rows it changed
// across them. It is for a row somebody changes from wherever they are, long after the shard
// that wrote it is known to anybody.
func onEveryShard(ctx context.Context, s *Store, write func(context.Context, bun.IDB) (sql.Result, error)) (int64, error) {
	results, err := everyShard(ctx, s, func(ctx context.Context, db bun.IDB) (int64, error) {
		result, err := write(ctx, db)
		if err != nil {
			return 0, err
		}
		return result.RowsAffected()
	})
	if err != nil {
		return 0, err
	}
	var total int64
	for _, affected := range results {
		total += affected
	}
	return total, nil
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

// closeShards closes every database the store opened past the shared one.
func (s *Store) closeShards() error {
	var failed []error
	if s.Sharded() {
		failed = append(failed, s.shard.Close())
	}
	for _, peer := range s.peers {
		failed = append(failed, peer.Close())
	}
	return stack.Wrap(errors.Join(failed...))
}
