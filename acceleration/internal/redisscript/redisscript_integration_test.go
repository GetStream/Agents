//go:build integration

package redisscript_test

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidishook"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/redisscript"
	_ "github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
)

type ScriptSuite struct {
	suite.Suite
	ctx     context.Context
	counted *roundTrips
	redis   rueidis.Client
	// script is new to Redis in every test: a comment no other run has makes its SHA-1 one
	// Redis has not cached.
	script *redisscript.Script
	prefix string
}

func TestScriptSuite(t *testing.T) { suite.Run(t, new(ScriptSuite)) }

func (s *ScriptSuite) SetupSuite() {
	address := os.Getenv("ROUTER_REDIS_ADDR")
	if address == "" {
		s.T().Skip("ROUTER_REDIS_ADDR is not set")
	}
	s.ctx = context.Background()
	client, err := redisready.NewClient(rueidis.ClientOption{InitAddress: []string{address}, DisableCache: true})
	s.Require().NoError(err)
	s.T().Cleanup(client.Close)
	s.counted = &roundTrips{}
	s.redis = rueidishook.WithHook(client, s.counted)
}

func (s *ScriptSuite) SetupTest() {
	s.prefix = "test:redisscript:" + uuid.NewString()
	s.script = redisscript.New("-- " + uuid.NewString() + "\nreturn redis.call('INCRBY', KEYS[1], ARGV[1])")
}

// incr is a call adding by to key n.
func (s *ScriptSuite) incr(n, by int) rueidis.LuaExec {
	return rueidis.LuaExec{Keys: []string{s.prefix + ":" + strconv.Itoa(n)}, Args: []string{strconv.Itoa(by)}}
}

// sent is the round trips run takes.
func (s *ScriptSuite) sent(run func()) int64 {
	before := s.counted.sent.Load()
	run()
	return s.counted.sent.Load() - before
}

func (s *ScriptSuite) TestABatchOfAScriptRedisHasIsOneRoundTrip() {
	s.Require().NoError(s.script.Exec(s.ctx, s.redis, s.incr(0, 1).Keys, s.incr(0, 1).Args).Error())

	var results []rueidis.RedisResult
	trips := s.sent(func() {
		results = s.script.ExecMulti(s.ctx, s.redis, s.incr(1, 10), s.incr(2, 20), s.incr(1, 5))
	})

	s.Equal(int64(1), trips)
	s.Require().Len(results, 3)
	for i, want := range []int64{10, 20, 15} {
		got, err := results[i].AsInt64()
		s.Require().NoError(err)
		s.Equal(want, got, "answer %d", i)
	}
}

func (s *ScriptSuite) TestABatchOfAScriptRedisLacksRunsEveryCallOnceAndLoadsIt() {
	var results []rueidis.RedisResult
	trips := s.sent(func() {
		results = s.script.ExecMulti(s.ctx, s.redis, s.incr(1, 10), s.incr(2, 20))
	})

	s.Equal(int64(2), trips, "the batch, then the calls Redis lacked the script for")
	for i, want := range []int64{10, 20} {
		got, err := results[i].AsInt64()
		s.Require().NoError(err)
		s.Equal(want, got, "answer %d", i)
	}
	s.Equal(int64(1), s.sent(func() {
		results = s.script.ExecMulti(s.ctx, s.redis, s.incr(1, 1))
	}), "the script is loaded for the next batch")
	got, err := results[0].AsInt64()
	s.Require().NoError(err)
	s.Equal(int64(11), got)
}

// roundTrips counts what a client sends Redis: a command, or a pipeline of them, is one.
type roundTrips struct{ sent atomic.Int64 }

func (r *roundTrips) Do(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	r.sent.Add(1)
	return client.Do(ctx, cmd)
}

func (r *roundTrips) DoMulti(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	r.sent.Add(1)
	return client.DoMulti(ctx, multi...)
}

func (r *roundTrips) DoCache(client rueidis.Client, ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	r.sent.Add(1)
	return client.DoCache(ctx, cmd, ttl)
}

func (r *roundTrips) DoMultiCache(client rueidis.Client, ctx context.Context, multi ...rueidis.CacheableTTL) []rueidis.RedisResult {
	r.sent.Add(1)
	return client.DoMultiCache(ctx, multi...)
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
