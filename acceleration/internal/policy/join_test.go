//go:build integration

package policy

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// heldMembership begins a transaction holding an app's membership row, which a write of it
// waits behind, and answers it with its backend's pid.
func (s *PolicySuite) heldMembership(appID string) (bun.Tx, int) {
	s.Require().NoError(s.store.AppOrganizations.Upsert(s.ctx, &store.AppOrganization{AppID: appID, OrganizationID: "org-before"}))
	holder, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = holder.Rollback() })
	var pid int
	s.Require().NoError(holder.QueryRowContext(s.ctx, "SELECT pg_backend_pid()").Scan(&pid))
	_, err = holder.ExecContext(s.ctx, "SELECT 1 FROM app_organizations WHERE app_id = ? FOR UPDATE", appID)
	s.Require().NoError(err)
	return holder, pid
}

// waitingBehind is how many statements wait for the holder with this pid.
func (s *PolicySuite) waitingBehind(holder int) int {
	var waiting int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT count(*) FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid))", holder).Scan(&waiting))
	return waiting
}

// organizationOf is the organization the app's row names now, read past the cache, or empty
// with no row.
func (s *PolicySuite) organizationOf(appID string) string {
	var organization string
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT coalesce((SELECT organization_id FROM app_organizations WHERE app_id = ?), '')", appID).Scan(&organization))
	return organization
}

// seenAt is when the app's row was last written.
func (s *PolicySuite) seenAt(appID string) time.Time {
	var seen time.Time
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT seen_at FROM app_organizations WHERE app_id = ?", appID).Scan(&seen))
	return seen
}

func (s *PolicySuite) TestAnAppNamingOrganizationsInTurnHasOneMembershipWrittenAtATimeAndTheLastWins() {
	enforcer := s.newEnforcer(s.store, nil)
	app := "app-" + uuid.NewString()
	holder, pid := s.heldMembership(app)

	for i := range 200 {
		enforcer.Join(s.ctx, app, fmt.Sprintf("org-%d", i%2), openTier)
	}
	enforcer.Join(s.ctx, app, "org-last", openTier)
	s.Require().Eventually(func() bool { return s.waitingBehind(pid) > 0 }, 5*time.Second, 10*time.Millisecond,
		"the first write waits behind the holder")
	s.Never(func() bool { return s.waitingBehind(pid) > 1 }, 300*time.Millisecond, 10*time.Millisecond,
		"one write of the app's membership runs at a time")

	s.Require().NoError(holder.Commit())
	s.Eventually(func() bool { return s.organizationOf(app) == "org-last" }, 5*time.Second, 10*time.Millisecond,
		"the organization named last is the one recorded")
	seen := s.seenAt(app)
	s.Never(func() bool { return !s.seenAt(app).Equal(seen) }, 300*time.Millisecond, 20*time.Millisecond,
		"the app's membership is written again once the last organization named landed")
	closed := make(chan struct{})
	go func() {
		enforcer.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(joinTimeout / 2):
		s.FailNow("the app's writes go on once the last organization named landed")
	}
}

func (s *PolicySuite) TestAnAppJoiningAnotherOrganizationLaterIsRecordedUnderIt() {
	enforcer := s.newEnforcer(s.store, nil)
	app := "app-" + uuid.NewString()
	enforcer.Join(s.ctx, app, "org-first", openTier)
	s.Require().Eventually(func() bool { return s.organizationOf(app) == "org-first" }, 5*time.Second, 10*time.Millisecond,
		"the first organization is recorded")

	enforcer.Join(s.ctx, app, "org-second", openTier)

	s.Eventually(func() bool { return s.organizationOf(app) == "org-second" }, 5*time.Second, 10*time.Millisecond,
		"an organization named after the first write ended is written too")
}

func (s *PolicySuite) TestAnOrganizationNamedBehindAWriteIsNotWrittenOnceCloseBegan() {
	enforcer, err := New(s.store, quick, nil)
	s.Require().NoError(err)
	app := "app-" + uuid.NewString()
	holder, pid := s.heldMembership(app)
	enforcer.Join(s.ctx, app, "org-first", openTier)
	s.Require().Eventually(func() bool { return s.waitingBehind(pid) > 0 }, 5*time.Second, 10*time.Millisecond,
		"the first write waits behind the holder")
	enforcer.Join(s.ctx, app, "org-second", openTier)

	closed := make(chan struct{})
	go func() {
		enforcer.Close()
		close(closed)
	}()
	s.Require().Eventually(func() bool {
		enforcer.joinsMu.Lock()
		defer enforcer.joinsMu.Unlock()
		return enforcer.joinsClosed
	}, 5*time.Second, 10*time.Millisecond, "Close began")
	s.Require().NoError(holder.Commit())
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		s.FailNow("Close never returned")
	}
	s.Equal("org-first", s.organizationOf(app), "the write under way when Close began lands, and the one behind it is not started")
}

func (s *PolicySuite) TestCloseWaitsForAMembershipBeingWritten() {
	enforcer, err := New(s.store, quick, nil)
	s.Require().NoError(err)
	app := "app-" + uuid.NewString()
	holder, pid := s.heldMembership(app)
	enforcer.Join(s.ctx, app, "org-after", openTier)
	s.Require().Eventually(func() bool { return s.waitingBehind(pid) > 0 }, 5*time.Second, 10*time.Millisecond,
		"the write waits behind the holder")

	closed := make(chan struct{})
	go func() {
		enforcer.Close()
		close(closed)
	}()
	s.Never(func() bool {
		select {
		case <-closed:
			return true
		default:
			return false
		}
	}, 300*time.Millisecond, 10*time.Millisecond, "Close returns while the membership is being written")
	s.Require().NoError(holder.Commit())
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		s.FailNow("Close never returned")
	}
	s.Equal("org-after", s.organizationOf(app), "the membership landed before Close returned")
}

func (s *PolicySuite) TestCloseStopsWaitingForAMembershipAfterJoinTimeout() {
	// A lock wait past the serving default, so only the store's write timeout ends the write.
	dsn, err := url.Parse(os.Getenv("ROUTER_POSTGRES_DSN"))
	s.Require().NoError(err)
	query := dsn.Query()
	query.Set("lock_timeout", "60000")
	dsn.RawQuery = query.Encode()
	patient, err := store.Open(dsn.String())
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(patient.Close()) })
	enforcer, err := New(patient, quick, nil)
	s.Require().NoError(err)
	app := "app-" + uuid.NewString()
	_, pid := s.heldMembership(app)
	enforcer.Join(s.ctx, app, "org-after", openTier)
	s.Require().Eventually(func() bool { return s.waitingBehind(pid) > 0 }, 5*time.Second, 10*time.Millisecond,
		"the write waits behind the holder")

	closed := make(chan struct{})
	go func() {
		enforcer.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(joinTimeout + 2*time.Second):
		s.FailNow("Close waited on the membership past joinTimeout")
	}
}

func (s *PolicySuite) TestAJoinAfterCloseWritesNoMembership() {
	enforcer, err := New(s.store, quick, nil)
	s.Require().NoError(err)
	enforcer.Close()
	app := "app-" + uuid.NewString()

	enforcer.Join(s.ctx, app, "org-after", openTier)

	s.Never(func() bool {
		var written bool
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
			"SELECT EXISTS (SELECT 1 FROM app_organizations WHERE app_id = ?)", app).Scan(&written))
		return written
	}, 300*time.Millisecond, 10*time.Millisecond, "a closed enforcer starts no write")
}
