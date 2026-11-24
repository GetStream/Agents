//go:build integration

package resolver_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/resolver"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// failingSaves is the credential store a router saves through when a test makes a save fail,
// with no DDL on the shared database. A save is an Update whose fn asks to commit: fail gets
// its number, from 1, and answers the error it fails with, nil to let it pass, and whether it
// lands first, as a save whose answer was lost. before runs ahead of each Update, numbered
// from 1, outside the lock.
type failingSaves struct {
	core.CredentialStore
	fail   func(save int) (err error, landed bool)
	before func(update int)

	mu      sync.Mutex
	saves   int
	updates int
}

func (c *failingSaves) Update(ctx context.Context, ref core.ConnectionRef,
	fn func(state *core.CredentialState, checkpoint func() error) (bool, error)) error {
	c.mu.Lock()
	c.updates++
	update := c.updates
	c.mu.Unlock()
	if c.before != nil {
		c.before(update)
	}
	return c.CredentialStore.Update(ctx, ref, func(state *core.CredentialState, checkpoint func() error) (bool, error) {
		changed, err := fn(state, checkpoint)
		if err != nil || !changed {
			return changed, err
		}
		c.mu.Lock()
		c.saves++
		save := c.saves
		c.mu.Unlock()
		failure, landed := c.fail(save)
		if failure == nil {
			return true, nil
		}
		if landed {
			if err := checkpoint(); err != nil {
				return false, err
			}
		}
		return false, failure
	})
}

// routerSaving is a router whose saves go through saves.
func (f *fixture) routerSaving(client *http.Client, saves *failingSaves) *resolver.Resolver {
	f.saves = saves
	defer func() { f.saves = nil }()
	return f.router(client)
}

// firstFails fails the first save with err, landed or not, and lets the rest pass.
func firstFails(err error, landed bool) *failingSaves {
	return &failingSaves{fail: func(save int) (error, bool) {
		if save == 1 {
			return err, landed
		}
		return nil, false
	}}
}

// dropped is what the store returns for a save on a connection the driver closed.
var dropped = fmt.Errorf("store: save connector credentials: %w", driver.ErrBadConn)

// refused is a real Postgres error with code, as the store returns it from a save.
func (s *ResolverSuite) refused(code string) error {
	_, err := s.db.GlobalDB().ExecContext(s.f.ctx,
		fmt.Sprintf("DO $$ BEGIN RAISE EXCEPTION 'refused by the test' USING ERRCODE = '%s'; END $$", code))
	s.Require().Error(err)
	return fmt.Errorf("store: save connector credentials: %w", err)
}

func (s *ResolverSuite) TestARefreshWhoseFirstSaveFailsIsSavedByTheNextAndStaysConnected() {
	ref := s.f.connected()
	before := s.f.stored(ref).Revision
	_, consented := s.f.tokens(ref)
	s.f.due()

	credential, err := s.f.routerSaving(s.f.srv.Client(), firstFails(dropped, false)).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.Require().NoError(err)
	s.True(s.f.works(credential))
	stored := s.f.stored(ref)
	s.Equal(store.ConnectionConnected, stored.Status)
	s.Empty(stored.LastError)
	s.Equal(before+1, stored.Revision)
	s.Require().NotNil(stored.ExpiresAt)
	// The credential store keeps microseconds, and the clock on Linux gives nanoseconds.
	renewed := credential.ExpiresAt.Truncate(time.Microsecond)
	s.True(stored.ExpiresAt.Equal(renewed), "the renewed expiry is stored: %v, want %v", *stored.ExpiresAt, renewed)
	_, rotated := s.f.tokens(ref)
	s.False(rotated == consented, "the rotated refresh token is stored")

	s.f.due()
	credential, err = s.f.router(s.f.srv.Client()).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.Require().NoError(err, "the stored refresh token is the one the provider still takes")
	s.True(s.f.works(credential))
	s.Equal(2, s.f.srv.Refreshes())
}

func (s *ResolverSuite) TestARefreshWhoseSavesAllFailKeepsTheCheckpointAndIsLoggedOnce() {
	ref := s.f.connected()
	before := s.f.stored(ref).Revision
	s.f.due()
	saves := &failingSaves{fail: func(int) (error, bool) { return dropped, false }}

	_, err := s.f.routerSaving(s.f.srv.Client(), saves).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.ErrorIs(err, driver.ErrBadConn)
	stored := s.f.stored(ref)
	s.Equal(store.ConnectionNeedsReauthorization, stored.Status)
	s.Equal(lostRefresh, stored.LastError)
	s.Equal(before, stored.Revision)
	line := s.line("could not save a connector credential refresh")
	s.Contains(line, "level=ERROR")
	s.Contains(line, "connection="+ref.ConnectionID)
	s.Contains(line, "attempts=3")
	access, refresh := s.f.tokens(ref)
	s.noTokenLogged(access, refresh)
}

// A refresh whose saves all failed keeps what it renewed on the router that renewed it, which
// saves it on the connection's next use once the database takes it again: the provider may
// have rotated the refresh token, and the person does not have to reconnect.
func (s *ResolverSuite) TestARefreshWhoseSavesAllFailedIsSavedOnTheNextUse() {
	ref := s.f.connected()
	s.f.due()
	var failing atomic.Bool
	failing.Store(true)
	router := s.f.routerSaving(s.f.srv.Client(), &failingSaves{fail: func(int) (error, bool) {
		if failing.Load() {
			return dropped, false
		}
		return nil, false
	}})
	_, err := router.Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.Require().ErrorIs(err, driver.ErrBadConn)
	s.Require().Equal(store.ConnectionNeedsReauthorization, s.f.stored(ref).Status)
	failing.Store(false)

	credential, err := router.Resolve(s.f.ctx, ref, core.CredentialRequest{})

	s.Require().NoError(err)
	s.True(s.f.works(credential))
	s.Equal(store.ConnectionConnected, s.f.stored(ref).Status)
	s.Empty(s.f.stored(ref).LastError)
}

func (s *ResolverSuite) TestASaveIsRetriedOnlyAfterAFailureARetryCanFix() {
	invalid := (&store.ConnectorConnection{}).Validate()
	s.Require().Error(invalid)
	cases := []struct {
		name    string
		err     error
		retried bool
	}{
		{"a dropped connection", dropped, true},
		{"a timeout", fmt.Errorf("store: save connector credentials: %w", context.DeadlineExceeded), true},
		{"a serialization failure", s.refused("40001"), true},
		{"a row that moved", store.ErrConnectorConnectionChanged, false},
		{"a deleted row", fmt.Errorf("%w: %s", store.ErrNoConnectorConnection, "gone"), false},
		{"an invalid row", invalid, false},
		{"a refused check", s.refused("23514"), false},
	}
	for _, c := range cases {
		ref := s.f.connected()
		s.f.due()

		_, err := s.f.routerSaving(s.f.srv.Client(), firstFails(c.err, false)).Resolve(s.f.ctx, ref, core.CredentialRequest{})
		stored := s.f.stored(ref)
		if c.retried {
			s.NoError(err, c.name)
			s.Equal(store.ConnectionConnected, stored.Status, c.name)
		} else {
			s.ErrorIs(err, c.err, c.name)
			s.Equal(store.ConnectionNeedsReauthorization, stored.Status, c.name)
			s.Equal(lostRefresh, stored.LastError, c.name)
		}
	}
}

func (s *ResolverSuite) TestASaveThatLandedWithItsAnswerLostIsNotSavedAgain() {
	ref := s.f.connected()
	before := s.f.stored(ref).Revision
	s.f.due()

	credential, err := s.f.routerSaving(s.f.srv.Client(), firstFails(dropped, true)).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.Require().NoError(err)
	s.True(s.f.works(credential))
	stored := s.f.stored(ref)
	s.Equal(store.ConnectionConnected, stored.Status)
	s.Equal(before+1, stored.Revision)
	s.Equal(stored.Revision, credential.Revision)
}

func (s *ResolverSuite) TestARetriedSaveLeavesAReconnectThatCameFirst() {
	ref := s.f.connected()
	s.f.due()
	var reconnected string
	saves := firstFails(dropped, false)
	saves.before = func(update int) {
		if update == 2 {
			s.f.consent(ref)
			_, reconnected = s.f.tokens(ref)
		}
	}

	_, err := s.f.routerSaving(s.f.srv.Client(), saves).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.Error(err)
	stored := s.f.stored(ref)
	s.Equal(store.ConnectionConnected, stored.Status, "the reconnect's status")
	_, refresh := s.f.tokens(ref)
	s.True(refresh == reconnected, "the reconnect's refresh token, not the renewal's")
	s.Contains(s.line("could not save a connector credential refresh"), "attempts=2", "a moved connection is not tried again")
}

func (s *ResolverSuite) TestARetriedSaveOfARefreshTheCallerGaveUpOnStillLands() {
	ref := s.f.connected()
	before := s.f.stored(ref).Revision
	ctx, cancel := context.WithCancel(s.f.ctx)
	returned := make(chan struct{})
	client := s.f.interposed(func() {
		cancel()
		<-returned
	})
	r := s.f.routerSaving(client, firstFails(dropped, false))
	s.f.due()

	_, err := r.Resolve(ctx, ref, core.CredentialRequest{})
	close(returned)
	s.ErrorIs(err, context.Canceled)

	s.Eventually(func() bool { return s.f.stored(ref).Revision == before+1 }, 5*time.Second, 10*time.Millisecond,
		"the retry runs on the refresh's detached context")
	s.Equal(store.ConnectionConnected, s.f.stored(ref).Status)
}

func (s *ResolverSuite) TestAFailedSaveWithoutACheckpointIsNotRetried() {
	ref := s.f.connected()

	_, err := s.f.routerSaving(s.f.srv.Client(), firstFails(dropped, false)).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.ErrorIs(err, driver.ErrBadConn, "a save no checkpoint went before loses nothing")
	stored := s.f.stored(ref)
	s.Equal(store.ConnectionConnected, stored.Status)
	s.Nil(stored.ExpiresAt, "the expiry the first resolve would have written")
	s.NotContains(s.f.logs.String(), "could not save a connector credential refresh")
}

func (s *ResolverSuite) TestARetriedSaveKeepsTheGrantsConnectedAt() {
	ref := s.f.connected()
	began := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	s.f.update(ref, func(state *core.CredentialState) { state.ConnectedAt = began })
	s.f.due()

	_, err := s.f.routerSaving(s.f.srv.Client(), firstFails(dropped, false)).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.Require().NoError(err)
	s.Require().NotNil(s.f.stored(ref).ConnectedAt)
	s.True(s.f.stored(ref).ConnectedAt.Equal(began), "a renewal begins no grant")
	s.Require().NoError(s.f.router(s.f.srv.Client()).Revoke(s.f.ctx, ref, core.SignalRevoked,
		time.Now().Add(-30*time.Minute).Truncate(time.Second)))
	s.Equal(store.ConnectionNeedsReauthorization, s.f.stored(ref).Status, "the provider ended this grant")
}

func (s *ResolverSuite) TestARevocationBetweenTwoSavesStands() {
	ref := s.f.connected()
	s.f.due()
	saves := firstFails(dropped, false)
	saves.before = func(update int) {
		if update == 2 {
			s.Require().NoError(s.f.router(s.f.srv.Client()).Revoke(s.f.ctx, ref, core.SignalUninstalled, time.Time{}))
		}
	}

	_, err := s.f.routerSaving(s.f.srv.Client(), saves).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.Error(err)
	stored := s.f.stored(ref)
	s.Equal(store.ConnectionNeedsReauthorization, stored.Status, "the uninstall stands")
	s.Equal("The provider says its app was uninstalled from the account; reconnect the account", stored.LastError)
	rows := s.audited(ref, 1)
	s.Equal(store.AuditGrantRevoked, rows[0].Action, "the grant the uninstall ended")
	s.Equal(string(core.SignalUninstalled), rows[0].Reason)
	s.Contains(s.line("could not save a connector credential refresh"), "attempts=2", "a revoked connection is not tried again")
}

// TestARevocationOfAnOlderGrantBetweenTwoSavesIsLeft: a reconnect began a new grant and its first
// refresh left the checkpoint when its save failed; Slack's late tokens_revoked for the grant
// before it arrives between two tries. The new grant is not the one that ended, so it stays.
func (s *ResolverSuite) TestARevocationOfAnOlderGrantBetweenTwoSavesIsLeft() {
	ref := s.f.connected()
	s.f.due()
	saves := firstFails(dropped, false)
	saves.before = func(update int) {
		if update == 2 {
			s.Require().NoError(s.f.router(s.f.srv.Client()).Revoke(s.f.ctx, ref, core.SignalUninstalled, time.Now().Add(-time.Hour).Truncate(time.Second)))
		}
	}

	_, err := s.f.routerSaving(s.f.srv.Client(), saves).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.NoError(err)
	s.Equal(store.ConnectionConnected, s.f.stored(ref).Status)
}

func (s *ResolverSuite) TestARefreshWhoseSaveLandsTakesNoSecondUpdate() {
	ref := s.f.connected()
	s.f.due()
	saves := firstFails(nil, false)
	saves.before = func(update int) {
		if update == 2 {
			s.f.consent(ref)
		}
	}

	credential, err := s.f.routerSaving(s.f.srv.Client(), saves).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.Require().NoError(err, "a save that worked is not followed by another Update")
	s.True(s.f.works(credential))
	s.Equal(store.ConnectionConnected, s.f.stored(ref).Status)
}

func (s *ResolverSuite) TestASaveThatLandedWithItsAnswerLostIsNotWrittenAgain() {
	ref := s.f.connected()
	s.f.due()
	saves := firstFails(dropped, true)
	var landedAt time.Time
	saves.before = func(update int) {
		if update == 2 {
			landedAt = s.f.stored(ref).UpdatedAt
		}
	}

	_, err := s.f.routerSaving(s.f.srv.Client(), saves).Resolve(s.f.ctx, ref, core.CredentialRequest{})
	s.Require().NoError(err)
	s.Require().False(landedAt.IsZero(), "a second Update ran")
	s.True(s.f.stored(ref).UpdatedAt.Equal(landedAt), "the landed row is left as it is")
}
