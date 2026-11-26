package conversation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"
	"github.com/stretchr/testify/suite"
)

// OwnerKindBackfillSuite covers what BackfillOwnerKinds does without a store: how it names what it
// did to a channel, and how it waits out Stream's rate limit.
type OwnerKindBackfillSuite struct {
	suite.Suite
	service *Service
	waited  []time.Duration
}

func TestOwnerKindBackfillSuite(t *testing.T) {
	suite.Run(t, new(OwnerKindBackfillSuite))
}

func (s *OwnerKindBackfillSuite) SetupTest() {
	s.service = NewForChats(oneChat{})
	s.waited = nil
}

func limitedFor(retryAfter time.Duration, reset int64) error {
	return &getstream.StreamError{StatusCode: http.StatusTooManyRequests, RetryAfter: retryAfter,
		RateLimit: &getstream.RateLimitInfo{Reset: reset}}
}

// sleeping is a Sleep that waits for nothing and says how long it was asked to.
func (s *OwnerKindBackfillSuite) sleeping() OwnerKindRun {
	return OwnerKindRun{Sleep: func(_ context.Context, wait time.Duration) error {
		s.waited = append(s.waited, wait)
		return nil
	}}
}

func (s *OwnerKindBackfillSuite) TestALineSaysWhatWasDoneToItsChannel() {
	for _, row := range []struct {
		line OwnerKindLine
		want string
	}{
		{OwnerKindLine{Outcome: OwnerKindStamped}, "would stamp"},
		{OwnerKindLine{Outcome: OwnerKindStamped, Written: true}, "stamped"},
		{OwnerKindLine{Outcome: OwnerKindRecorded, Written: true}, "skipped:already recorded"},
		{OwnerKindLine{Outcome: OwnerKindNoOwner}, "skipped:no owner"},
		{OwnerKindLine{Outcome: OwnerKindLeftAlone}, "skipped:left alone"},
		{OwnerKindLine{Outcome: OwnerKindFailed, Err: errors.New("it broke\n\tbadly")}, "failed:error"},
		{OwnerKindLine{Outcome: OwnerKindFailed, Err: failedOn(failedStream, errors.New("it broke"))}, "failed:stream_error"},
		{OwnerKindLine{Outcome: OwnerKindFailed, Err: failedOn(failedStore, errors.New("it broke"))}, "failed:store_error"},
		{OwnerKindLine{Outcome: OwnerKindFailed, Err: failedOn(failedStream, limitedFor(time.Second, 0))}, "failed:rate_limited"},
	} {
		s.Equal(row.want, row.line.Action())
	}
}

func (s *OwnerKindBackfillSuite) TestAFailedLineNamesNeitherTheKeyNorTheErrorText() {
	client, err := getstream.NewClient("PROBEKEY123", "PROBESECRET456", getstream.WithBaseUrl("http://127.0.0.1:1"))
	s.Require().NoError(err)
	_, streamErr := client.Chat().QueryChannels(context.Background(), &getstream.QueryChannelsRequest{})
	s.Require().Error(streamErr)
	storeErr := errors.New("dial tcp db-host.internal:5432: connection refused: SELECT secret FROM sessions")
	for _, row := range []struct {
		err  error
		want string
	}{
		{failedOn(failedStream, streamErr), "failed:stream_error"},
		{failedOn(failedStore, storeErr), "failed:store_error"},
		{streamErr, "failed:error"},
	} {
		got := OwnerKindLine{CustomerID: "c", ConversationID: "x", Outcome: OwnerKindFailed, Err: row.err}.Action()

		s.Equal(row.want, got)
		s.NotContains(got, "PROBEKEY123")
		s.NotContains(got, "api_key")
		s.NotContains(got, "db-host")
	}
}

func (s *OwnerKindBackfillSuite) TestARateLimitIsWaitedOutAsLongAsStreamAsksAndTheCallRunsAgain() {
	now := time.Now()
	for name, limit := range map[string]error{
		"its Retry-After":       limitedFor(7*time.Second, 0),
		"the end of its window": limitedFor(0, now.Add(7*time.Second).Unix()),
	} {
		s.Run(name, func() {
			s.waited = nil
			calls := 0
			err := s.service.waitOutLimit(s.T().Context(), s.sleeping(), func() error {
				calls++
				if calls == 1 {
					return limit
				}
				return nil
			})

			s.Require().NoError(err)
			s.Equal(2, calls)
			s.Require().Len(s.waited, 1)
			s.InDelta(float64(7*time.Second), float64(s.waited[0]), float64(time.Second))
		})
	}
}

func (s *OwnerKindBackfillSuite) TestAWaitStreamAsksForIsHeldToMaxLimitWait() {
	calls := 0
	err := s.service.waitOutLimit(s.T().Context(), s.sleeping(), func() error {
		calls++
		if calls == 1 {
			return limitedFor(time.Hour, 0)
		}
		return nil
	})

	s.Require().NoError(err)
	s.Equal([]time.Duration{2 * time.Minute}, s.waited)
}

func (s *OwnerKindBackfillSuite) TestARateLimitThatDoesNotLiftIsWaitedOutMaxLimitWaitsTimesAndThenAnswered() {
	calls := 0
	limit := limitedFor(time.Second, 0)
	err := s.service.waitOutLimit(s.T().Context(), s.sleeping(), func() error {
		calls++
		return limit
	})

	s.Equal(limit, err)
	s.Equal(maxLimitWaits+1, calls)
	s.Len(s.waited, maxLimitWaits)
}

func (s *OwnerKindBackfillSuite) TestAFailureThatIsNoRateLimitIsNotWaitedOut() {
	for name, failure := range map[string]error{
		"another status": &getstream.StreamError{StatusCode: http.StatusInternalServerError},
		"no Stream":      errors.New("connection reset"),
		"nothing":        nil,
	} {
		s.Run(name, func() {
			s.waited = nil
			calls := 0
			err := s.service.waitOutLimit(s.T().Context(), s.sleeping(), func() error {
				calls++
				return failure
			})

			s.Equal(failure, err)
			s.Equal(1, calls)
			s.Empty(s.waited)
		})
	}
}

func (s *OwnerKindBackfillSuite) TestAWaitCutShortByItsContextEndsTheCall() {
	ctx, cancel := context.WithCancel(s.T().Context())
	calls := 0
	run := OwnerKindRun{Sleep: func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}}

	err := s.service.waitOutLimit(ctx, run, func() error {
		calls++
		return limitedFor(time.Second, 0)
	})

	s.ErrorIs(err, context.Canceled)
	s.Equal(1, calls)
}

func (s *OwnerKindBackfillSuite) TestTheDefaultWaitStopsWithItsContext() {
	ctx, cancel := context.WithCancel(s.T().Context())
	cancel()
	started := time.Now()

	err := s.service.waitOutLimit(ctx, OwnerKindRun{}, func() error { return limitedFor(time.Hour, 0) })

	s.ErrorIs(err, context.Canceled)
	s.Less(time.Since(started), 5*time.Second)
	s.NoError(sleepFor(s.T().Context(), time.Millisecond), "a wait that is not cut short ends with nothing")
}

func (s *OwnerKindBackfillSuite) TestWhatStreamSaysOfARateLimitIsRead() {
	now := time.Unix(1_790_000_000, 0)
	for _, row := range []struct {
		name    string
		err     error
		wait    time.Duration
		limited bool
	}{
		{"Retry-After first", limitedFor(3*time.Second, now.Add(time.Minute).Unix()), 3 * time.Second, true},
		{"then the window's reset", limitedFor(0, now.Add(20*time.Second).Unix()), 20 * time.Second, true},
		{"a reset already past", limitedFor(0, now.Add(-time.Minute).Unix()), 0, true},
		{"nothing said", limitedFor(0, 0), 0, true},
		{"wrapped", fmt.Errorf("read: %w", limitedFor(3*time.Second, 0)), 3 * time.Second, true},
		{"another status", &getstream.StreamError{StatusCode: http.StatusBadGateway, RetryAfter: time.Second}, 0, false},
		{"no Stream error", errors.New("nope"), 0, false},
		{"no error", nil, 0, false},
	} {
		wait, limited := rateLimitWait(row.err, now)
		s.Equal([2]any{row.wait, row.limited}, [2]any{wait, limited}, row.name)
	}
}
