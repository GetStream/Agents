//go:build integration

package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"
)

// timePasses moves every next attempt in table d back, as if d went by on Postgres's clock: a
// claim leases from now() there, so a test cannot run a lease out with a time of its own.
func (s *StoreSuite) timePasses(table string, d time.Duration) {
	_, err := s.store.GlobalDB().ExecContext(s.ctx,
		fmt.Sprintf("UPDATE %s SET next_attempt_at = next_attempt_at - make_interval(secs => ?)", table), d.Seconds())
	s.Require().NoError(err)
}

// stopAfterClaim stops its caller once a claim's statement has committed, as a router that is
// stopping does when its cancel lands after the claim was sent.
type stopAfterClaim struct{ stop context.CancelFunc }

func (*stopAfterClaim) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h *stopAfterClaim) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if strings.HasPrefix(event.Query, "WITH \"due\"") {
		h.stop()
	}
}

// A claim whose caller stopped after the claim was sent leases nothing: what it took is due
// again at once, for the next claim on any router.
func (s *StoreSuite) TestAClaimCancelledAfterItWasSentLeavesNoRowLeased() {
	s.queuedChat("msg-one")
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	stopping := &Store{global: s.store.global.WithQueryHook(&stopAfterClaim{stop: cancel})}
	stopping.wire()

	claimed, err := stopping.ChatInbounds.Claim(ctx, 10, chatLease)

	s.Require().ErrorIs(err, context.Canceled)
	s.Empty(claimed)
	again := s.claimedChat("msg-one")
	s.Equal(2, again.Attempts, "the cancelled claim's attempt stays counted, so no claim before it writes the row again")
}

// A caller that stopped before its claim takes nothing and spends no attempt: a router that
// stops does not use up a retry of every row due.
func (s *StoreSuite) TestAClaimWhoseCallerHadStoppedTakesNothing() {
	s.queuedChat("msg-one")
	ctx, cancel := context.WithCancel(s.ctx)
	cancel()

	claimed, err := s.store.ChatInbounds.Claim(ctx, 10, chatLease)

	s.Require().ErrorIs(err, context.Canceled)
	s.Empty(claimed)
	s.Equal(1, s.claimedChat("msg-one").Attempts, "the first claim that took it")
}
