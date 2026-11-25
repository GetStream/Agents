//go:build integration

package policy

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// readHolder is a query hook that keeps the answer of the next read of a table, once Postgres has
// given it, from whoever asked until let go.
type readHolder struct{ next atomic.Pointer[heldRead] }

type heldRead struct {
	table      string
	held, gate chan struct{}
}

func (h *readHolder) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context { return ctx }

func (h *readHolder) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	next := h.next.Load()
	if next != nil && strings.Contains(event.Query, `FROM "`+next.table+`" `) && h.next.CompareAndSwap(next, nil) {
		close(next.held)
		<-next.gate
	}
}

// seldom is an enforcer that reads again only what a test asks it to, on a store of its own.
func (s *PolicySuite) seldom(change ...func(*Settings)) (*Enforcer, *readHolder) {
	own, err := store.Open(os.Getenv("ROUTER_POSTGRES_DSN"))
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(own.Close()) })
	reads := &readHolder{}
	own.GlobalDB().AddQueryHook(reads)
	enforcer := s.newEnforcer(own, nil, append([]func(*Settings){func(settings *Settings) {
		settings.Fresh, settings.SumEvery, settings.MaxStale = time.Hour, time.Hour, 2*time.Hour
		settings.SpendEvery, settings.FirstRead = time.Hour, 5*time.Second
	}}, change...)...)
	return enforcer, reads
}

// holdRead starts read, whose read of the table is held with the answer it has now, and returns what
// lets it land.
func (s *PolicySuite) holdRead(reads *readHolder, table string, read func()) (land func()) {
	h := &heldRead{table: table, held: make(chan struct{}), gate: make(chan struct{})}
	reads.next.Store(h)
	done := make(chan struct{})
	go func() {
		defer close(done)
		read()
	}()
	land = sync.OnceFunc(func() {
		close(h.gate)
		<-done
	})
	s.T().Cleanup(land)
	select {
	case <-h.held:
	case <-time.After(5 * time.Second):
		s.FailNow("the read was never made")
	}
	return land
}

// spentApp is an app past its limit, as the enforcer has read it, with its grants read once. The
// enforcer's loops are stopped then, so every later read is one the test asks for.
func (s *PolicySuite) spentApp(enforcer *Enforcer) store.Limit {
	limit := s.limit(store.Limit{Subject: usage.SubjectApp, Level: store.LevelApp, OrganizationID: s.org, AppID: s.app, LimitMicros: 1_000_000})
	s.spend(s.app, 1_000_000)
	s.spendRefused(enforcer, s.app, ErrBudgetSpent)
	s.eventually(func() bool {
		enforcer.money.mu.Lock()
		defer enforcer.money.mu.Unlock()
		return !enforcer.money.accounts[s.org].grantsFrom.IsZero()
	}, 5*time.Second, 5*time.Millisecond, "its grants are read")
	enforcer.Close()
	return limit
}

func (s *PolicySuite) audit() store.LimitAudit {
	return store.LimitAudit{CustomerID: s.app, Source: store.AuditSourceAPI}
}

func (s *PolicySuite) grantApp() store.LimitGrant {
	grant := store.LimitGrant{OrganizationID: s.org, Subject: usage.SubjectApp, AppID: s.app, AmountMicros: 2_000_000, Reason: "launch", Actor: "test"}
	s.Require().NoError(s.store.LimitGrants.Insert(s.ctx, &grant, s.audit()))
	return grant
}

func (s *PolicySuite) lower(limit store.Limit, micros int64) {
	_, err := s.store.Limits.Update(s.ctx, s.org, limit.ID, store.LimitChange{LimitMicros: &micros, UpdatedBy: "test"}, s.audit())
	s.Require().NoError(err)
}

// stale has the enforcer's next read of the organization's limits due.
func (s *PolicySuite) stale(enforcer *Enforcer) {
	enforcer.money.mu.Lock()
	defer enforcer.money.mu.Unlock()
	enforcer.money.accounts[s.org].limitsRead = time.Now().Add(-90 * time.Minute)
}

func (s *PolicySuite) TestAGrantsReadAskedBeforeARevokeLeavesItRevoked() {
	enforcer, reads := s.seldom()
	s.spentApp(enforcer)
	grant := s.grantApp()
	enforcer.LimitsChanged(s.ctx, s.org)
	s.Require().Equal([]int64{grant.ID}, s.admit(enforcer, s.app).Grants)
	land := s.holdRead(reads, "limit_grants", func() { enforcer.money.grantsPass(s.ctx) })

	_, err := s.store.LimitGrants.Revoke(s.ctx, s.org, grant.ID, "test", "done", s.audit())
	s.Require().NoError(err)
	enforcer.LimitsChanged(s.ctx, s.org)
	s.Require().ErrorIs(s.admit(enforcer, s.app).Spend, ErrBudgetSpent)
	land()

	s.ErrorIs(s.admit(enforcer, s.app).Spend, ErrBudgetSpent, "the older read does not put the grant back")
}

func (s *PolicySuite) TestAGrantsReadAskedBeforeAGrantWasMadeLeavesItLive() {
	enforcer, reads := s.seldom()
	s.spentApp(enforcer)
	land := s.holdRead(reads, "limit_grants", func() { enforcer.money.grantsPass(s.ctx) })

	grant := s.grantApp()
	enforcer.LimitsChanged(s.ctx, s.org)
	s.Require().NoError(s.admit(enforcer, s.app).Spend)
	land()

	admitted := s.admit(enforcer, s.app)
	s.NoError(admitted.Spend, "the older read does not take the grant away")
	s.Equal([]int64{grant.ID}, admitted.Grants)
}

// A hold is let go of once a read of the grants has its row, so an older read of what the grant
// had paid for would count the row nowhere.
func (s *PolicySuite) TestAGrantsReadAskedBeforeARowCountedLeavesItCounted() {
	enforcer, reads := s.seldom()
	s.spentApp(enforcer)
	grant := s.grantApp()
	enforcer.LimitsChanged(s.ctx, s.org)
	s.Require().NoError(s.admit(enforcer, s.app).Spend)
	land := s.holdRead(reads, "limit_grants", func() { enforcer.money.grantsPass(s.ctx) })

	_, err := s.store.LimitGrantUses.InsertMany(s.ctx, []store.GrantUse{{GrantID: grant.ID, CustomerID: s.app, OrganizationID: s.org, UsageID: newID(), Micros: 2_000_000}})
	s.Require().NoError(err)
	enforcer.money.grantsPass(s.ctx)
	s.Require().ErrorIs(s.admit(enforcer, s.app).Spend, ErrBudgetSpent)
	land()

	s.ErrorIs(s.admit(enforcer, s.app).Spend, ErrBudgetSpent, "the grant stays used up")
}

// The read that was not kept may be the only one that had another router's write, which is then
// the next pass's to find.
func (s *PolicySuite) TestAPassAfterAnOlderGrantsReadKeepsWhatItFinds() {
	enforcer, reads := s.seldom()
	s.spentApp(enforcer)
	land := s.holdRead(reads, "limit_grants", func() { enforcer.money.grantsPass(s.ctx) })
	enforcer.LimitsChanged(s.ctx, s.org)
	land()
	grant := s.grantApp()

	enforcer.money.grantsPass(s.ctx)

	s.Equal([]int64{grant.ID}, s.admit(enforcer, s.app).Grants)
}

func (s *PolicySuite) TestALimitsPassAskedBeforeALimitWasLoweredLeavesItLowered() {
	enforcer, reads := s.seldom()
	limit := s.spentApp(enforcer)
	s.lower(limit, 3_000_000)
	enforcer.LimitsChanged(s.ctx, s.org)
	s.Require().NoError(s.admit(enforcer, s.app).Spend)
	s.stale(enforcer)
	land := s.holdRead(reads, "limits", func() { enforcer.money.readLimits(s.ctx, enforcer.config, []string{s.org}) })

	s.lower(limit, 500_000)
	enforcer.LimitsChanged(s.ctx, s.org)
	s.Require().ErrorIs(s.admit(enforcer, s.app).Spend, ErrBudgetSpent)
	read := limitsReadAt(enforcer, s.org)
	land()

	s.ErrorIs(s.admit(enforcer, s.app).Spend, ErrBudgetSpent, "the older read does not raise the limit again")
	s.Equal(read, limitsReadAt(enforcer, s.org), "an older read makes the limits no fresher")
}

func (s *PolicySuite) TestALimitsPassAskedBeforeAResetLeavesItReset() {
	enforcer, reads := s.seldom()
	limit := s.spentApp(enforcer)
	s.stale(enforcer)
	land := s.holdRead(reads, "usage_resets", func() { enforcer.money.readLimits(s.ctx, enforcer.config, []string{s.org}) })

	start, _ := usage.Window(usage.Daily, time.Now())
	s.Require().NoError(s.store.UsageResets.InsertMany(s.ctx, []store.UsageReset{{
		OrganizationID: s.org, Subject: limit.Subject, AppID: s.app, Interval: usage.Daily, WindowStart: start,
		BaselineMicros: 1_000_000, Reason: "test", Actor: "test",
	}}, s.audit()))
	enforcer.LimitsChanged(s.ctx, s.org)
	s.Require().NoError(s.admit(enforcer, s.app).Spend)
	land()

	s.NoError(s.admit(enforcer, s.app).Spend, "the older read does not undo the reset")
}

func (s *PolicySuite) TestOfTwoWritesLimitsReadsTheOneAskedLaterIsKept() {
	enforcer, reads := s.seldom()
	limit := s.spentApp(enforcer)
	s.lower(limit, 3_000_000)
	land := s.holdRead(reads, "limits", func() { enforcer.LimitsChanged(s.ctx, s.org) })

	s.lower(limit, 500_000)
	enforcer.LimitsChanged(s.ctx, s.org)
	s.Require().ErrorIs(s.admit(enforcer, s.app).Spend, ErrBudgetSpent)
	land()

	s.ErrorIs(s.admit(enforcer, s.app).Spend, ErrBudgetSpent)
}

// Which read is older goes by when each was asked, not by the clock the windows are counted in,
// which may step back.
func (s *PolicySuite) TestAClockThatStepsBackStopsNoRead() {
	var back atomic.Int64
	enforcer, _ := s.seldom(func(settings *Settings) {
		settings.Now = func() time.Time { return time.Now().Add(-time.Duration(back.Load())) }
	})
	limit := s.spentApp(enforcer)
	back.Store(int64(10 * time.Second))

	grant := s.grantApp()
	enforcer.LimitsChanged(s.ctx, s.org)
	s.Equal([]int64{grant.ID}, s.admit(enforcer, s.app).Grants)
	s.lower(limit, 3_000_000)
	enforcer.LimitsChanged(s.ctx, s.org)
	s.Empty(s.admit(enforcer, s.app).Grants, "the raised limit has room, so no grant is drawn")
	s.lower(limit, 500_000)
	s.stale(enforcer)
	enforcer.money.readLimits(s.ctx, enforcer.config, []string{s.org})
	s.Equal([]int64{grant.ID}, s.admit(enforcer, s.app).Grants, "the lowered one has not")
}
