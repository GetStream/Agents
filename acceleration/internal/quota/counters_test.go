//go:build integration

package quota

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
)

// slowLimiter is a Limiter whose Redis can be made to stop answering.
func (s *QuotaSuite) slowLimiter(limits Limits) (*Limiter, *slowRedis) {
	slow := newSlowRedis(s.T(), os.Getenv(AddressEnvVar))
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{slow.address()}, DisableCache: true, PipelineMultiplex: -1})
	s.Require().NoError(err)
	s.T().Cleanup(client.Close)
	limiter, err := New(client, limits, nil, slog.Default())
	s.Require().NoError(err)
	return limiter, slow
}

// briefly is Allow given little time, so one that waited on a Redis that does not answer
// would fail open rather than answer from the copy.
func (s *QuotaSuite) briefly(limiter *Limiter) (Status, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 300*time.Millisecond)
	defer cancel()
	return limiter.Allow(ctx, s.customer, s.caller)
}

func (s *QuotaSuite) TestAllowAnswersFromTheCopyWhileRedisDoesNotAnswer() {
	limiter, slow := s.slowLimiter(Limits{MessagesPerDay: 3})
	limiter.Debit(s.ctx, s.customer, s.caller, 10)
	s.written(limiter)
	s.Require().NoError(s.allow(limiter, s.customer, s.caller))
	limiter.Debit(s.ctx, s.customer, s.caller, 10)
	limiter.Debit(s.ctx, s.customer, s.caller, 10)
	s.written(limiter)
	slow.hold()
	defer slow.release()

	_, err := s.briefly(limiter)

	s.ErrorIs(err, ErrExhausted, "the copy holds all three messages")
}

func (s *QuotaSuite) TestADebitHereCountsAtOnceThoughRedisHasNotAnswered() {
	limiter, slow := s.slowLimiter(Limits{MessagesPerDay: 3})
	s.Require().NoError(s.allow(limiter, s.customer, s.caller))
	slow.hold()
	started := time.Now()

	limiter.Debit(s.ctx, s.customer, s.caller, 10)
	limiter.Debit(s.ctx, s.customer, s.caller, 10)

	s.Less(time.Since(started), 200*time.Millisecond, "a debit does not wait for Redis")
	status, err := s.briefly(limiter)
	s.Require().NoError(err)
	s.EqualValues(1, status.Remaining)
	slow.release()
	s.written(limiter)
	status, err = limiter.Allow(s.ctx, s.customer, s.caller)
	s.Require().NoError(err)
	s.EqualValues(1, status.Remaining, "once written, each debit is still counted once")
}

func (s *QuotaSuite) TestADebitOnAnotherRouterReachesThisOneWithinARefresh() {
	here, there := s.limiter(2, 0), s.limiter(2, 0)
	s.Require().NoError(s.allow(here, s.customer, s.caller))
	here.Start(s.ctx)
	defer here.Close()

	there.Debit(s.ctx, s.customer, s.caller, 10)
	there.Debit(s.ctx, s.customer, s.caller, 10)
	s.written(there)

	s.EventuallyWithT(func(c *assert.CollectT) {
		assert.ErrorIs(c, s.allow(here, s.customer, s.caller), ErrExhausted)
	}, 3*refreshEvery, 50*time.Millisecond)
}

func (s *QuotaSuite) TestARefreshThatSeesAWriteBeforeItsAnswerCountsItOnce() {
	// Redis applied this router's debit, and the refresh read it, before the write's own
	// answer was taken in: the copy still holds it as unwritten.
	limiter := s.limiter(3, 0)
	s.Require().NoError(s.allow(limiter, s.customer, s.caller))
	keys := deviceKeys(s.customer, s.caller, time.Now())
	bumped, _ := limiter.bump(keys, 10, time.Now())
	totals := make([]spend, len(keys))
	for i, key := range keys {
		messages, err := s.redis.Do(s.ctx, s.redis.B().Hincrby().Key(key).Field(messagesField).Increment(1).Build()).AsInt64()
		s.Require().NoError(err)
		tokens, err := s.redis.Do(s.ctx, s.redis.B().Hincrby().Key(key).Field(tokensField).Increment(10).Build()).AsInt64()
		s.Require().NoError(err)
		totals[i] = spend{messages: messages, tokens: tokens}
	}

	limiter.refresh(s.ctx)

	status, err := limiter.Allow(s.ctx, s.customer, s.caller)
	s.Require().NoError(err)
	s.EqualValues(2, status.Remaining)
	limiter.settle(bumped, 10, totals, time.Now())
	limiter.writes.Done()
	status, err = limiter.Allow(s.ctx, s.customer, s.caller)
	s.Require().NoError(err)
	s.EqualValues(2, status.Remaining, "and once more after its answer")
}

func (s *QuotaSuite) TestANewDayIsCountedFromNothing() {
	limiter := s.limiter(1, 0)
	evening := time.Date(2026, 3, 1, 23, 59, 30, 0, time.UTC)
	limiter.now = func() time.Time { return evening }
	limiter.Debit(s.ctx, s.customer, s.caller, 10)
	s.Require().ErrorIs(s.allow(limiter, s.customer, s.caller), ErrExhausted)

	limiter.now = func() time.Time { return evening.Add(time.Minute) }
	status, err := limiter.Allow(s.ctx, s.customer, s.caller)

	s.Require().NoError(err)
	s.EqualValues(1, status.Remaining)
	s.Equal(time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC), status.Reset)
	s.written(limiter)
}

func (s *QuotaSuite) TestACounterNobodyUsesLeavesTheCopy() {
	limiter := s.limiter(5, 0)
	started := time.Now()
	limiter.now = func() time.Time { return started }
	s.Require().NoError(s.allow(limiter, s.customer, s.caller))
	limiter.now = func() time.Time { return started.Add(idleAfter / 2) }
	s.Require().NoError(s.allow(limiter, s.customer+"-other", s.caller))
	limiter.now = func() time.Time { return started.Add(idleAfter + time.Minute) }

	limiter.refresh(s.ctx)

	s.Len(limiter.counters, 2, "the other customer's user and address, used within idleAfter")
	for key := range limiter.counters {
		s.Contains(key, s.customer+"-other")
	}
}

func (s *QuotaSuite) TestCloseWaitsForTheDebitsStillBeingWritten() {
	limiter, slow := s.slowLimiter(Limits{MessagesPerDay: 3})
	slow.hold()
	limiter.Debit(s.ctx, s.customer, s.caller, 10)
	time.AfterFunc(200*time.Millisecond, slow.release)

	limiter.Close()

	spent, err := limiter.spent(s.ctx, deviceKeys(s.customer, s.caller, time.Now())...)
	s.Require().NoError(err)
	s.Equal([]spend{{messages: 1, tokens: 10}, {messages: 1, tokens: 10}}, spent)
}
