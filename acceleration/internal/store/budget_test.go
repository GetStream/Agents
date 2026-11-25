//go:build integration

package store

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidisaside"
	"github.com/uptrace/bun"
)

// redisAddressVar is where the tests look for a Redis to run against.
const redisAddressVar = "ROUTER_REDIS_ADDR"

// redisFamilies are the prefixes of every key the store keeps in Redis.
var redisFamilies = []string{"appcfg:", "turns:", "call_events:", "floor_checks:", refusedPrefix, usersPrefix}

// redis is the Redis the suite's store caches in, or none when the tests have none.
func (s *StoreSuite) redis() Redis {
	return Redis{Address: os.Getenv(redisAddressVar)}
}

// forgetRedis deletes every key the store keeps, so a row cached by one test is not read by
// the next after SetupTest deleted it.
func (s *StoreSuite) forgetRedis() {
	if s.store.cache == nil {
		return
	}
	client := s.store.cache.Client()
	for _, family := range redisFamilies {
		var cursor uint64
		for {
			entry, err := client.Do(s.ctx, client.B().Scan().Cursor(cursor).Match(family+"*").Count(1000).Build()).AsScanEntry()
			s.Require().NoError(err)
			if len(entry.Elements) > 0 {
				s.Require().NoError(client.Do(s.ctx, client.B().Del().Key(entry.Elements...).Build()).Error())
			}
			if cursor = entry.Cursor; cursor == 0 {
				break
			}
		}
	}
}

// queryCount counts the queries bun sends through it.
type queryCount struct{ sent atomic.Int64 }

func (*queryCount) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context { return ctx }
func (q *queryCount) AfterQuery(context.Context, *bun.QueryEvent)                      { q.sent.Add(1) }

// countQueries is how many statements run sent to Postgres, on every database the store holds.
func (s *StoreSuite) countQueries(run func()) int {
	before := s.queries.sent.Load()
	run()
	return int(s.queries.sent.Load() - before)
}

// countSettledReads is countQueries for a read one shard's answer settles (answeringShards).
// The reads that answer cut off are not waited for and finish after run returns, so it waits,
// up to a deadline, until every shard's statement is in before it counts.
func (s *StoreSuite) countSettledReads(run func()) int {
	before := s.queries.sent.Load()
	run()
	shards := int64(len(s.store.shards()))
	s.Eventually(func() bool { return s.queries.sent.Load()-before >= shards }, 5*time.Second, time.Millisecond,
		"a statement from every shard")
	return int(s.queries.sent.Load() - before)
}

// countEveryQuery hooks the suite's counter into every database the store holds.
func (s *StoreSuite) countEveryQuery() {
	databases := append([]*bun.DB{s.store.global, s.store.locks}, s.store.peers...)
	if s.store.Sharded() {
		databases = append(databases, s.store.shard)
	}
	for _, db := range databases {
		db.AddQueryHook(&s.queries)
	}
}

// statementsOf counts the statements starting with prefix sent while it is active.
type statementsOf struct {
	prefix string
	active atomic.Bool
	sent   atomic.Int64
}

func (c *statementsOf) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (c *statementsOf) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if c.active.Load() && strings.HasPrefix(event.Query, c.prefix) {
		c.sent.Add(1)
	}
}

// statementsLike is how many statements starting with prefix run sends, on every database the
// store holds: how many batches a batched write took.
func (s *StoreSuite) statementsLike(prefix string, run func()) int {
	counter := &statementsOf{prefix: prefix}
	databases := append([]*bun.DB{s.store.global}, s.store.peers...)
	if s.store.Sharded() {
		databases = append(databases, s.store.shard)
	}
	for _, db := range databases {
		db.AddQueryHook(counter)
	}
	counter.active.Store(true)
	run()
	counter.active.Store(false)
	return int(counter.sent.Load())
}

// roundTrips counts what the store sends Redis: a command, or a pipeline of them, is one.
type roundTrips struct{ sent atomic.Int64 }

func (r *roundTrips) Do(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	if !asideMarker(cmd) {
		r.sent.Add(1)
	}
	return client.Do(ctx, cmd)
}

// asideMarker says cmd writes the marker rueidisaside keeps for this client, which it refreshes
// on its own every few seconds: not a round trip the store makes, but one that lands in a
// counted window now and then.
func asideMarker(cmd rueidis.Completed) bool {
	parts := cmd.Commands()
	return len(parts) > 1 && parts[0] == "SET" && strings.HasPrefix(parts[1], rueidisaside.PlaceholderPrefix)
}

func (r *roundTrips) DoMulti(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	r.sent.Add(1)
	return client.DoMulti(ctx, multi...)
}

func (r *roundTrips) DoCache(client rueidis.Client, ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	result := client.DoCache(ctx, cmd, ttl)
	if !result.IsCacheHit() {
		r.sent.Add(1)
	}
	return result
}

func (r *roundTrips) DoMultiCache(client rueidis.Client, ctx context.Context, multi ...rueidis.CacheableTTL) []rueidis.RedisResult {
	results := client.DoMultiCache(ctx, multi...)
	for _, result := range results {
		if !result.IsCacheHit() {
			r.sent.Add(1)
			break
		}
	}
	return results
}

func (r *roundTrips) Receive(client rueidis.Client, ctx context.Context, subscribe rueidis.Completed, fn func(msg rueidis.PubSubMessage)) error {
	r.sent.Add(1)
	return client.Receive(ctx, subscribe, fn)
}

func (r *roundTrips) DoStream(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResultStream {
	r.sent.Add(1)
	return client.DoStream(ctx, cmd)
}

func (r *roundTrips) DoMultiStream(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) rueidis.MultiRedisResultStream {
	r.sent.Add(1)
	return client.DoMultiStream(ctx, multi...)
}

// countRedis is how many round trips run made to Redis. A read answered from the local cache
// rueidis keeps did not cross the network and is not counted.
func (s *StoreSuite) countRedis(run func()) int {
	before := s.roundTrips.sent.Load()
	run()
	return int(s.roundTrips.sent.Load() - before)
}

// redisTrips is the n round trips a method makes to Redis when the suite has one, and none
// when it has not.
func (s *StoreSuite) redisTrips(n int) int {
	if s.store.cache == nil {
		return 0
	}
	return n
}
