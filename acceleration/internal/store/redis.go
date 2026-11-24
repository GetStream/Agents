package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidisaside"
	"github.com/redis/rueidis/rueidishook"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tracing"
)

var tracer = tracing.Tracer("store")

// cacheTTL is how long a cached row lives in Redis when nothing deletes it first. It is long
// because every write deletes the keys it changed, and the expiry is only the backstop for a
// deletion that did not land.
const cacheTTL = time.Hour

// forgetWait is how long the deletion of cached rows, or one try at it again, waits on Redis.
// It is longer than cacheWait: a deletion that is answered late still tells every replica,
// where a read that waits that long is better answered from Postgres.
const forgetWait = 2 * time.Second

// cacheWait is how long a read of the cache waits on one answer from Redis before going to
// Postgres instead.
// Redis answers in a millisecond or two; one that takes this long is down or cut off.
const cacheWait = 250 * time.Millisecond

// keyCacheTTL is how long a key's owner is cached. It is short because a key revoked where
// the deletion does not reach, in the database alone or when Redis drops the DEL, has to
// stop authenticating within a minute rather than an hour.
const keyCacheTTL = time.Minute

// missTTL is how long a lookup that found nothing is cached. No write deletes such a key unless
// it creates the row, so the expiry is what lets one go, and every name asked for once would
// otherwise be kept for cacheTTL.
const missTTL = time.Minute

// forgetAttempts is how many times a deletion Redis did not take is tried again, the first
// forgetBackoff after it and each one twice as long after the last. forgetSlots bounds how
// many are being retried at once, so a Redis that is down costs a few goroutines and not one
// per write.
const (
	forgetAttempts = 3
	forgetBackoff  = 100 * time.Millisecond
	forgetSlots    = 64
)

// Redis says where a Redis server is. An empty Address is none.
type Redis struct {
	// Address is a host:port, for example localhost:6379.
	Address  string
	Username string
	Password string
}

func (r Redis) option() rueidis.ClientOption {
	return rueidis.ClientOption{InitAddress: []string{r.Address}, Username: r.Username, Password: r.Password}
}

// openRedis connects the store to its Redis servers: the shared one, where rows read on every
// request are cached, and the shard's, where a running call's state is kept.
func (s *Store) openRedis(options Options) error {
	if options.Redis.Address == "" {
		return nil
	}
	build := func(option rueidis.ClientOption) (rueidis.Client, error) {
		client, err := rueidis.NewClient(option)
		if err != nil || options.redisHook == nil {
			return client, err
		}
		return rueidishook.WithHook(client, options.redisHook), nil
	}
	// The shared client tracks the keys it reads for rueidisaside and for Users, whose LRU
	// hears from Redis which users went stale.
	shared := func(option rueidis.ClientOption) (rueidis.Client, error) {
		aside := option.OnInvalidations
		option.OnInvalidations = func(messages []rueidis.RedisMessage) {
			aside(messages)
			s.Users.invalidate(messages)
		}
		client, err := build(option)
		if err != nil {
			return nil, err
		}
		return rueidishook.WithHook(client, cacheReadBound{}), nil
	}
	cache, err := rueidisaside.NewClient(rueidisaside.ClientOption{ClientOption: options.Redis.option(), ClientBuilder: shared})
	if err != nil {
		return fmt.Errorf("store: connect to redis: %w", err)
	}
	s.cache = cache
	s.redis = cache.Client()
	if options.ShardRedis.Address != "" && options.ShardRedis.Address != options.Redis.Address {
		if s.redis, err = build(options.ShardRedis.option()); err != nil {
			cache.Close()
			return fmt.Errorf("store: connect to the shard's redis: %w", err)
		}
	}
	return nil
}

// closeRedis releases the Redis connections.
func (s *Store) closeRedis() {
	if s.cache == nil {
		return
	}
	if s.redis != s.cache.Client() {
		s.redis.Close()
	}
	s.cache.Close()
}

// sharedRedis is the Redis every shard shares, or nil without Redis.
func (s *Store) sharedRedis() rueidis.Client {
	if s.cache == nil {
		return nil
	}
	return s.cache.Client()
}

// redisFamilies are the prefixes of every key the store keeps in Redis.
var redisFamilies = []string{"appcfg:", "turns:", "call_events:", "floor_checks:", refusedPrefix, usersPrefix}

// cacheKey names one cached row. Every key is prefixed, so what the store cached can be told
// from what the rest of the router keeps.
func cacheKey(parts ...string) string { return "appcfg:" + strings.Join(parts, ":") }

// cached returns what load produced, from the local cache rueidis keeps, from Redis, or from
// Postgres, in that order. Redis invalidates the local copy itself when the key is deleted,
// so every replica forgets a changed row at once.
//
// An error from load is the caller's answer, unchanged, so that ErrNoAPIKey and its siblings
// still read as themselves. An error from Redis is not: the row is loaded from Postgres
// instead, because a cache nobody can reach is a slow deployment and not a broken one.
func cached[T any](ctx context.Context, s *Store, name string, load func(context.Context) (T, error)) (T, error) {
	return cachedFor(ctx, s, name, cacheTTL, load)
}

// cachedFor is cached with a TTL of the caller's own.
//
// Each Redis command of the read waits on Redis's answer for cacheWait (cacheReadBound), so a
// Redis that cannot be reached costs that and not the caller's whole deadline. load runs on
// the caller's context, unbounded by that wait, and so does rueidisaside's wait for a load
// another caller of this router is running: the readers of a cold key share one load.
func cachedFor[T any](ctx context.Context, s *Store, name string, ttl time.Duration, load func(context.Context) (T, error)) (T, error) {
	var value T
	if s.cache == nil {
		return load(ctx)
	}

	ctx, span := tracer.Start(ctx, "store.cached")
	defer span.End()

	var refused error
	var loaded T
	var hasLoaded bool
	encoded, err := s.cache.Get(context.WithValue(ctx, cacheReadKey{}, true), ttl, name, func(ctx context.Context, _ string) (string, error) {
		var err error
		if loaded, err = load(ctx); err != nil {
			refused = err
			return "", err
		}
		hasLoaded = true
		raw, err := json.Marshal(loaded)
		return string(raw), err
	})
	switch {
	case refused != nil:
		return value, stack.Wrap(refused)
	case err != nil && hasLoaded:
		slog.Default().Error("could not cache a row in redis", "key", name, "error", err)
		return loaded, nil
	case err != nil:
		slog.Default().Error("could not read a cached row from redis, reading postgres", "key", name, "error", err)
		return load(ctx)
	}

	if err := json.Unmarshal([]byte(encoded), &value); err != nil {
		return value, stack.Wrap(fmt.Errorf("store: decode %s: %w", name, err))
	}
	return value, nil
}

// cacheReadKey marks the context of a cached read, whose Redis commands cacheReadBound bounds.
type cacheReadKey struct{}

// cacheReadBound gives cacheWait to be answered to each command sent on a cached read's
// context, and to each one sent on a context with no deadline. The second are rueidisaside's
// own, its client marker and the release of a load's lock, which a cached read and the
// readers waiting on its load wait for, and the store's sends that outlive a request (forget).
// A command on a context with a deadline of its own keeps it. A stream is read after the call
// returns, so it is left alone.
type cacheReadBound struct{}

func boundCacheRead(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, deadline := ctx.Deadline(); deadline && ctx.Value(cacheReadKey{}) == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, cacheWait)
}

func (cacheReadBound) Do(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	ctx, cancel := boundCacheRead(ctx)
	defer cancel()
	return client.Do(ctx, cmd)
}

func (cacheReadBound) DoMulti(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	ctx, cancel := boundCacheRead(ctx)
	defer cancel()
	return client.DoMulti(ctx, multi...)
}

func (cacheReadBound) DoCache(client rueidis.Client, ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	ctx, cancel := boundCacheRead(ctx)
	defer cancel()
	return client.DoCache(ctx, cmd, ttl)
}

func (cacheReadBound) DoMultiCache(client rueidis.Client, ctx context.Context, multi ...rueidis.CacheableTTL) []rueidis.RedisResult {
	ctx, cancel := boundCacheRead(ctx)
	defer cancel()
	return client.DoMultiCache(ctx, multi...)
}

func (cacheReadBound) Receive(client rueidis.Client, ctx context.Context, subscribe rueidis.Completed, fn func(rueidis.PubSubMessage)) error {
	return client.Receive(ctx, subscribe, fn)
}

func (cacheReadBound) DoStream(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResultStream {
	return client.DoStream(ctx, cmd)
}

func (cacheReadBound) DoMultiStream(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) rueidis.MultiRedisResultStream {
	return client.DoMultiStream(ctx, multi...)
}

// found is a row looked up together with whether there was one, so that a row nobody holds
// can be cached as well.
type found[T any] struct {
	Value T    `json:"value"`
	Found bool `json:"found"`
}

// forget drops cached rows from Redis, which drops them from every replica's local cache
// too, in one round trip.
//
// A deletion that does not land is tried again in the background rather than returned: the
// write it followed has already happened, and refusing it afterwards would tell the caller
// their change was rejected when it was not. It reports whether the deletion landed now, false
// without Redis, for a caller that tells a person so.
//
// The deletion goes out even when the request has been cancelled since the write: rueidis
// returns the context's error before it sends anything, so neither the DEL nor the EXISTS
// would reach Redis and every node would keep the old value until the TTL.
func (s *Store) forget(ctx context.Context, names ...string) bool {
	if s.cache == nil || len(names) == 0 {
		return false
	}
	// A deadline of its own, so cacheReadBound leaves the deletion its forgetWait rather than
	// a cached read's wait.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), forgetWait)
	defer cancel()
	client := s.cache.Client()
	// Each key is deleted on its own: a Redis Cluster refuses one DEL whose keys sit in
	// different hash slots, and the keys one write changes usually do.
	//
	// Redis sends this node the invalidation of its own local copy after the DEL's reply, so
	// a read made as soon as the DEL returns can still be answered by the old copy. The
	// client sends these commands over the connection it read the node's copies on, and reads
	// its replies in order, so once a command after the DEL is answered, the invalidation has
	// been applied. That command is an EXISTS of the same key, so that on a cluster it goes
	// to the node that holds the key, as a keyless PING would not. An EXISTS that fails needs
	// nothing more: a broken connection drops every local copy it held, and any other failure
	// is the one the TTL covers.
	commands := make(rueidis.Commands, 0, 2*len(names))
	for _, name := range names {
		commands = append(commands, client.B().Del().Key(name).Build(), client.B().Exists().Key(name).Build())
	}
	replies := client.DoMulti(ctx, commands...)
	var missed []string
	var failed error
	for i, name := range names {
		if err := replies[2*i].Error(); err != nil && !errors.Is(err, rueidis.Nil) {
			missed, failed = append(missed, name), err
		}
	}
	if len(missed) > 0 {
		s.forgetLater(missed, failed)
	}
	return len(missed) == 0
}

// forgetLater tries again, a few times and in the background, a deletion Redis did not take.
// The usual cause is a connection Redis dropped, which the client dials again at once. A
// deletion still not taken after that, or one with no slot left to retry in, is what the TTL
// covers.
func (s *Store) forgetLater(names []string, err error) {
	s.retryMu.Lock()
	defer s.retryMu.Unlock()
	if s.closed {
		slog.Default().Error("could not drop cached rows, and the store is closing", "keys", names, "error", err)
		return
	}
	select {
	case s.retries <- struct{}{}:
	default:
		slog.Default().Error("could not drop cached rows, and no retry is free", "keys", names, "error", err)
		return
	}
	s.retrying.Add(1)
	go func() {
		defer s.retrying.Done()
		defer func() { <-s.retries }()
		client := s.cache.Client()
		wait := forgetBackoff
		for range forgetAttempts {
			timer := time.NewTimer(wait)
			select {
			case <-s.closing.Done():
				timer.Stop()
				slog.Default().Error("could not drop cached rows before the store closed", "keys", names, "error", err)
				return
			case <-timer.C:
			}
			// One DEL per key, as forget sends them, since a cluster refuses one across slots.
			commands := make(rueidis.Commands, 0, len(names))
			for _, name := range names {
				commands = append(commands, client.B().Del().Key(name).Build())
			}
			var missed []string
			attempt, cancel := context.WithTimeout(s.closing, forgetWait)
			for i, reply := range client.DoMulti(attempt, commands...) {
				if failed := reply.Error(); failed != nil && !errors.Is(failed, rueidis.Nil) {
					missed, err = append(missed, names[i]), failed
				}
			}
			cancel()
			if names = missed; len(names) == 0 {
				return
			}
			wait *= 2
		}
		slog.Default().Error("gave up dropping cached rows", "keys", names, "attempts", forgetAttempts, "error", err)
	}()
}
