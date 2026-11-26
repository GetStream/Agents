//go:build integration

package policy

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// policyReads is a query hook that holds each read of one app's policies, once Postgres has
// answered it, until the test lets it land: of reads under way at once, the test picks the order
// they land in.
type policyReads struct {
	app  string
	held chan chan struct{}
	done chan struct{}
}

func (*policyReads) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context { return ctx }

func (r *policyReads) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if !strings.Contains(event.Query, "LEFT JOIN app_organizations AS ao") || !strings.Contains(event.Query, "'"+r.app+"'") {
		return
	}
	gate := make(chan struct{})
	select {
	case r.held <- gate:
	case <-r.done:
		return
	}
	select {
	case <-gate:
	case <-r.done:
	}
}

// ordered is an enforcer over a store of the test's own whose every read of the app's policies
// is held (policyReads), and which reads them again only when the test asks.
func (s *PolicySuite) ordered() (*Enforcer, *policyReads) {
	db, err := store.Open(os.Getenv("ROUTER_POSTGRES_DSN"))
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })
	reads := &policyReads{app: s.app, held: make(chan chan struct{}), done: make(chan struct{})}
	db.GlobalDB().AddQueryHook(reads)
	enforcer := s.newEnforcer(db, nil, func(settings *Settings) { settings.Fresh = time.Hour })
	// Runs before the enforcer is closed, so no read is left held.
	s.T().Cleanup(func() { close(reads.done) })
	return enforcer, reads
}

// asked runs read until its read of the app's policies is held with what Postgres answered then,
// and returns what lets that read land and waits until read has returned.
func (s *PolicySuite) asked(reads *policyReads, read func()) (land func()) {
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		read()
	}()
	var gate chan struct{}
	select {
	case gate = <-reads.held:
	case <-time.After(5 * time.Second):
		s.FailNow("the policies were never read")
	}
	return func() {
		close(gate)
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			s.FailNow("the read never landed")
		}
	}
}

// join is Join naming the organization, returning once the membership it records and the read
// of the app's decision after it are done.
func (s *PolicySuite) join(enforcer *Enforcer, organizationID string) func() {
	return func() {
		enforcer.Join(s.ctx, s.app, organizationID, "")
		for running := true; running; time.Sleep(time.Millisecond) {
			enforcer.joinsMu.Lock()
			_, running = enforcer.joins[s.app]
			enforcer.joinsMu.Unlock()
		}
	}
}

// warmed is Join naming no organization, returning once the read of the app's decision it starts
// is done.
func (s *PolicySuite) warmed(enforcer *Enforcer) func() {
	return func() {
		enforcer.Join(s.ctx, s.app, "", "")
		for warming := true; warming; time.Sleep(time.Millisecond) {
			_, warming = enforcer.warming.Load(s.app)
		}
	}
}

func (s *PolicySuite) requested(enforcer *Enforcer) func() {
	return func() { enforcer.decide(s.ctx, s.app) }
}

func (s *PolicySuite) refreshed(enforcer *Enforcer) func() {
	return func() { enforcer.refresh(s.ctx, s.app) }
}

func (s *PolicySuite) move(organizationID string) {
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &store.AppOrganization{AppID: s.app, OrganizationID: organizationID}))
}

// The move's own read is asked after the refresh already under way, so it is the one kept,
// though it lands last.
func (s *PolicySuite) TestAMoveToAnotherOrganizationIsKeptOverARefreshAskedBeforeIt() {
	enforcer, reads := s.ordered()
	s.asked(reads, s.requested(enforcer))()
	moved := s.org + "-moved"
	before := s.asked(reads, s.refreshed(enforcer))
	after := s.asked(reads, s.join(enforcer, moved))

	before()
	after()

	s.Equal(moved, s.admit(enforcer, s.app).OrganizationID)
}

func (s *PolicySuite) TestARefreshAskedBeforeAMoveThatLandsAfterItChangesNothing() {
	enforcer, reads := s.ordered()
	s.asked(reads, s.requested(enforcer))()
	moved := s.org + "-moved"
	before := s.asked(reads, s.refreshed(enforcer))
	s.asked(reads, s.join(enforcer, moved))()

	before()

	s.Equal(moved, s.admit(enforcer, s.app).OrganizationID)
}

// With no decision in memory yet (a new process, or one dropped after Idle or by a Save), the
// move's refresh is still the read asked later, and is kept over the request's that lands after it.
func (s *PolicySuite) TestARefreshAskedAfterAMoveIsKeptWithNoDecisionYet() {
	enforcer, reads := s.ordered()
	before := s.asked(reads, s.requested(enforcer))
	moved := s.org + "-moved"
	s.move(moved)
	s.asked(reads, s.refreshed(enforcer))()

	before()

	s.Equal(moved, s.admit(enforcer, s.app).OrganizationID)
}

func (s *PolicySuite) TestARefreshWithNoDecisionYetKeepsWhatItReadAsUsedNow() {
	enforcer := s.newEnforcer(s.store, nil)

	enforcer.refresh(s.ctx, s.app)

	s.Contains(enforcer.inUse(time.Now()), s.app, "not dropped as unused for Idle")
}

func (s *PolicySuite) TestARefreshThatFailsWithNoDecisionYetKeepsNothing() {
	db, err := store.Open(os.Getenv("ROUTER_POSTGRES_DSN"))
	s.Require().NoError(err)
	s.Require().NoError(db.Close())
	enforcer := s.newEnforcer(db, nil)

	enforcer.refresh(s.ctx, s.app)

	_, kept := enforcer.decided(s.app)
	s.False(kept, "an empty decision would answer the next request")
}

func (s *PolicySuite) TestOfTwoRequestsReadingADecisionTheOneAskedLaterIsKept() {
	enforcer, reads := s.ordered()
	before := s.asked(reads, s.requested(enforcer))
	moved := s.org + "-moved"
	s.move(moved)
	s.asked(reads, s.requested(enforcer))()

	before()

	s.Equal(moved, s.admit(enforcer, s.app).OrganizationID)
}

func (s *PolicySuite) TestARequestsReadAskedBeforeARefreshChangesNothingWhenItLandsAfterIt() {
	enforcer, reads := s.ordered()
	before := s.asked(reads, s.requested(enforcer))
	s.asked(reads, s.requested(enforcer))()
	moved := s.org + "-moved"
	s.move(moved)
	s.asked(reads, s.refreshed(enforcer))()

	before()

	s.Equal(moved, s.admit(enforcer, s.app).OrganizationID)
}

func (s *PolicySuite) TestAWarmingAskedAfterARequestsReadIsKeptThoughItLandsLast() {
	enforcer, reads := s.ordered()
	before := s.asked(reads, s.requested(enforcer))
	moved := s.org + "-moved"
	s.move(moved)
	after := s.asked(reads, s.warmed(enforcer))

	before()
	after()

	s.Equal(moved, s.admit(enforcer, s.app).OrganizationID)
}

func (s *PolicySuite) TestAWarmingAskedBeforeARequestsReadChangesNothingWhenItLandsAfterIt() {
	enforcer, reads := s.ordered()
	before := s.asked(reads, s.warmed(enforcer))
	moved := s.org + "-moved"
	s.move(moved)
	s.asked(reads, s.requested(enforcer))()

	before()

	s.Equal(moved, s.admit(enforcer, s.app).OrganizationID)
}

// A policy saved while a read is under way still wins over it, however late that read was asked.
func (s *PolicySuite) TestAReadUnderWayWhenAPolicyIsSavedKeepsNothing() {
	enforcer, reads := s.ordered()
	before := s.asked(reads, s.requested(enforcer))
	s.Require().NoError(enforcer.Save(s.ctx, store.ScopeApp, s.app, store.PolicyDocument{}))

	before()

	_, kept := enforcer.decided(s.app)
	s.False(kept)
}

func (s *PolicySuite) TestAWarmingUnderWayWhenAPolicyIsSavedKeepsNothing() {
	enforcer, reads := s.ordered()
	before := s.asked(reads, s.warmed(enforcer))
	s.Require().NoError(enforcer.Save(s.ctx, store.ScopeApp, s.app, store.PolicyDocument{}))

	before()

	_, kept := enforcer.decided(s.app)
	s.False(kept)
}

func (s *PolicySuite) TestARefreshUnderWayWhenAPolicyIsSavedKeepsNothing() {
	enforcer, reads := s.ordered()
	s.asked(reads, s.requested(enforcer))()
	before := s.asked(reads, s.refreshed(enforcer))
	s.Require().NoError(enforcer.Save(s.ctx, store.ScopeApp, s.app, store.PolicyDocument{}))

	before()

	_, kept := enforcer.decided(s.app)
	s.False(kept)
}
