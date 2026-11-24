//go:build integration

package store

import (
	"context"
	"time"
)

// mintedBy is what Minted answers, which it must.
func (s *StoreSuite) mintedBy(users *Users, customerID, userID string) bool {
	minted, err := users.Minted(s.ctx, customerID, userID)
	s.Require().NoError(err)
	return minted
}

func (s *StoreSuite) mintGuest(users *Users, customerID, userID string) {
	s.Require().NoError(users.InsertGuest(s.ctx, &User{ID: userID, CustomerID: customerID, Name: "Guest"}))
}

func (s *StoreSuite) TestOnlyAGuestThisRouterHandedOutIsMinted() {
	app, other := s.freshApp(), s.freshApp()
	users := s.store.Users
	s.mintGuest(users, app, "guest-1")
	s.Require().NoError(users.Seen(s.ctx, app, "guest-2", UserKindGuest))
	s.Require().NoError(users.Seen(s.ctx, app, "guest-3", UserKindAuthenticated))
	s.mintGuest(users, app, "guest-4")
	s.setUserKind(app, "guest-4", UserKindAuthenticated)

	s.True(s.mintedBy(users, app, "guest-1"))
	s.False(s.mintedBy(users, other, "guest-1"), "another app's")
	s.False(s.mintedBy(users, app, "guest-2"), "a guest of the app's own")
	s.False(s.mintedBy(users, app, "guest-3"), "an account")
	s.False(s.mintedBy(users, app, "guest-4"), "an account whose row says minted")
	s.False(s.mintedBy(users, app, "guest-5"), "nobody")
}

func (s *StoreSuite) TestAClaimedGuestIsStillMinted() {
	app := s.freshApp()
	s.mintGuest(s.store.Users, app, "guest-1")
	_, err := s.store.Users.ClaimGuest(s.ctx, app, "guest-1", "alice")
	s.Require().NoError(err)

	s.True(s.mintedBy(s.store.Users, app, "guest-1"))
}

func (s *StoreSuite) TestWhetherAGuestIsMintedIsReadOnce() {
	app := s.freshApp()
	s.mintGuest(s.store.Users, app, "guest-1")
	s.Equal(1, s.countQueries(func() { s.mintedBy(s.store.Users, app, "guest-1") }))
	s.Equal(1, s.countQueries(func() { s.mintedBy(s.store.Users, app, "nobody") }))

	s.Zero(s.countQueries(func() {
		s.True(s.mintedBy(s.store.Users, app, "guest-1"))
		s.False(s.mintedBy(s.store.Users, app, "nobody"))
	}))
}

func (s *StoreSuite) TestAnAnswerThatAGuestIsNotMintedIsKeptForAMinuteAtMost() {
	if s.store.cache == nil {
		s.T().Skipf("%s not set", redisAddressVar)
	}
	app := s.freshApp()
	s.mintGuest(s.store.Users, app, "guest-1")
	s.True(s.mintedBy(s.store.Users, app, "guest-1"))
	s.False(s.mintedBy(s.store.Users, app, "nobody"))
	ttl := func(user string) time.Duration {
		client := s.store.sharedRedis()
		left, err := client.Do(s.ctx, client.B().Pttl().Key("appcfg:minted-guest:"+app+":"+user).Build()).AsInt64()
		s.Require().NoError(err)
		return time.Duration(left) * time.Millisecond
	}

	s.InDelta(60, ttl("nobody").Seconds(), 5)
	s.InDelta(3600, ttl("guest-1").Seconds(), 5)
}

func (s *StoreSuite) TestAGuestAnotherRouterMintsIsMintedHereWhateverThisOneHeld() {
	app, other := s.freshApp(), s.replicaUsers()
	s.False(s.mintedBy(s.store.Users, app, "guest-1"))

	s.mintGuest(other, app, "guest-1")

	s.Eventually(func() bool { return s.mintedBy(s.store.Users, app, "guest-1") }, settleFor, 10*time.Millisecond)
}

func (s *StoreSuite) TestAForgottenGuestIsMintedNoMoreInAnyRouter() {
	other := s.replicaUsers()
	s.mintGuest(s.store.Users, "app", "jlahey")
	s.True(s.mintedBy(s.store.Users, "app", "jlahey"))
	s.True(s.mintedBy(other, "app", "jlahey"))

	s.forget()

	s.False(s.mintedBy(s.store.Users, "app", "jlahey"))
	s.Eventually(func() bool { return !s.mintedBy(other, "app", "jlahey") }, settleFor, 10*time.Millisecond)
}

func (s *StoreSuite) TestAGuestAMoveDeletesIsMintedNoMore() {
	app := s.freshApp()
	s.mintGuest(s.store.Users, app, "guest-1")
	s.True(s.mintedBy(s.store.Users, app, "guest-1"))

	s.Require().NoError(s.store.ApplyChanges(s.ctx, app, []DataChange{{
		Seq: 1, Table: "users", Op: ChangeDelete, Key: []byte(`{"customer_id":"` + app + `","id":"guest-1"}`),
	}}))

	s.False(s.mintedBy(s.store.Users, app, "guest-1"))
}

func (s *StoreSuite) TestAGuestAMoveWritesOverAsAnAccountIsMintedNoMore() {
	app := s.freshApp()
	s.mintGuest(s.store.Users, app, "guest-1")
	s.True(s.mintedBy(s.store.Users, app, "guest-1"))

	s.Require().NoError(s.store.ImportRow(s.ctx, app, "users",
		[]byte(`{"customer_id":"`+app+`","id":"guest-1","kind":"authenticated","name":"","custom":{},"created_at":"2026-01-01T00:00:00Z"}`)))

	s.False(s.mintedBy(s.store.Users, app, "guest-1"))
}

func (s *StoreSuite) TestAFailedReadOfAMintedGuestIsAnErrorAndNotKept() {
	app := s.freshApp()
	s.mintGuest(s.store.Users, app, "guest-1")
	cancelled, cancel := context.WithDeadline(s.ctx, time.Now().Add(-time.Second))
	defer cancel()
	for _, users := range []*Users{s.store.Users, s.usersWithoutRedis(0)} {
		_, err := users.Minted(cancelled, app, "guest-1")
		s.Require().Error(err)

		s.True(s.mintedBy(users, app, "guest-1"))
	}
}

func (s *StoreSuite) TestWithNoRedisWhetherAGuestIsMintedIsHeldByTheRouterForAMinute() {
	app, users := s.freshApp(), s.usersWithoutRedis(0)
	s.False(s.mintedBy(users, app, "guest-1"))
	s.mintGuest(users, app, "guest-1")
	s.True(s.mintedBy(users, app, "guest-1"), "this router's own mint is known at once")

	s.eraseUser(app, "guest-1")
	s.True(s.mintedBy(users, app, "guest-1"), "held")
	s.True(users.minted.Holds(app, "guest-1", "true", time.Now().Add(50*time.Second)))
	s.False(users.minted.Holds(app, "guest-1", "true", time.Now().Add(61*time.Second)))
	s.False(s.mintedBy(users, app, "nobody"))
	s.False(users.minted.Holds(app, "nobody", "false", time.Now().Add(61*time.Second)))
}

func (s *StoreSuite) TestWithNoRedisAForgottenOrDeletedGuestIsMintedNoMoreHere() {
	router, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(router.Close()) })
	app := s.freshApp()
	s.mintGuest(router.Users, app, "guest-1")
	s.mintGuest(router.Users, app, "guest-2")
	s.True(s.mintedBy(router.Users, app, "guest-1"))
	s.True(s.mintedBy(router.Users, app, "guest-2"))

	_, err = router.ForgetUserSettings(s.ctx, app, "guest-1", "forgotten", &AuditEntry{
		CustomerID: app, ResourceType: AuditEndUser, ResourceID: "forgotten", Action: AuditUpdated, Source: AuditSourceCLI, ActorID: "ops",
	})
	s.Require().NoError(err)
	s.Require().NoError(router.ApplyChanges(s.ctx, app, []DataChange{{
		Seq: 1, Table: "users", Op: ChangeDelete, Key: []byte(`{"customer_id":"` + app + `","id":"guest-2"}`),
	}}))

	s.False(s.mintedBy(router.Users, app, "guest-1"))
	s.False(s.mintedBy(router.Users, app, "guest-2"))
}
