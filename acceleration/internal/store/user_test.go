//go:build integration

package store

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

// settleFor is how long an invalidation has to reach the other replica. Redis pushes it as
// soon as the key is deleted, so this is generous rather than expected.
const settleFor = 2 * time.Second

// freshApp is an app id no other test is using: the suite's store keeps its users' LRU
// between tests, so a fixed id would be held from an earlier one.
func (s *StoreSuite) freshApp() string {
	return fmt.Sprintf("app-%d", time.Now().UnixNano())
}

// replicaUsers is the Users of a second replica on the suite's databases and Redis, which
// is what two replicas of the router are. A user one of them wrote down is one the other
// must not write again, and a guest one of them claimed is one the other must stop
// trusting its cache about. Skipped without Redis.
func (s *StoreSuite) replicaUsers() *Users {
	return s.replica().Users
}

// usersWithoutRedis is the Users of a router with no Redis, holding an email for emailTTL.
func (s *StoreSuite) usersWithoutRedis(emailTTL time.Duration) *Users {
	router, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(router.Close()) })
	router.Users.emailTTL = emailTTL
	return router.Users
}

// userRow is the kind a user was written down as, and whether there is a row at all.
func (s *StoreSuite) userRow(customerID, userID string) (string, bool) {
	var kind string
	err := s.store.global.QueryRowContext(s.ctx,
		"SELECT kind FROM users WHERE customer_id = ? AND id = ?", customerID, userID).Scan(&kind)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false
	}
	s.Require().NoError(err)
	return kind, true
}

// setUserKind rewrites a user's kind behind Users' back, so that a later Seen changing it
// again is evidence that it went to Postgres.
func (s *StoreSuite) setUserKind(customerID, userID, kind string) {
	_, err := s.store.global.ExecContext(s.ctx,
		"UPDATE users SET kind = ? WHERE customer_id = ? AND id = ?", kind, customerID, userID)
	s.Require().NoError(err)
}

// userVersion is the row's xmin, which changes with every write to it.
func (s *StoreSuite) userVersion(customerID, userID string) string {
	var xmin string
	s.Require().NoError(s.store.global.QueryRowContext(s.ctx,
		"SELECT xmin::text FROM users WHERE customer_id = ? AND id = ?", customerID, userID).Scan(&xmin))
	return xmin
}

// eraseUser removes a user's row behind Users' back, so that a later Seen writing one
// again is evidence that it went to Postgres.
func (s *StoreSuite) eraseUser(customerID, userID string) {
	_, err := s.store.global.ExecContext(s.ctx,
		"DELETE FROM users WHERE customer_id = ? AND id = ?", customerID, userID)
	s.Require().NoError(err)
}

// userEmail is the address a user's row holds.
func (s *StoreSuite) userEmail(customerID, userID string) string {
	email, err := s.store.Users.GetEmail(s.ctx, customerID, userID)
	s.Require().NoError(err)
	return email
}

func (s *StoreSuite) TestAUserIsWrittenDownTheFirstTimeTheyCall() {
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))

	kind, found := s.userRow(app, "jlahey")
	s.True(found)
	s.Equal(UserKindAuthenticated, kind)
}

func (s *StoreSuite) TestAUserAlreadySeenIsNotWrittenAgain() {
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))
	s.eraseUser(app, "jlahey")

	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))

	_, found := s.userRow(app, "jlahey")
	s.False(found, "a user this process has already seen costs no write")
}

func (s *StoreSuite) TestSeeingAUserThisProcessHoldsCostsNoQueryAndNoRedis() {
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))

	var trips int
	queries := s.countQueries(func() {
		trips = s.countRedis(func() {
			s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))
		})
	})

	s.Zero(queries)
	s.Zero(trips)
}

func (s *StoreSuite) TestSeeingAUserAnotherReplicaWroteCostsNoQueryAndOneRedisRoundTrip() {
	app := s.freshApp()
	s.Require().NoError(s.replicaUsers().Seen(s.ctx, app, "jlahey", UserKindAuthenticated))

	var trips int
	queries := s.countQueries(func() {
		trips = s.countRedis(func() {
			s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))
		})
	})

	s.Zero(queries)
	s.LessOrEqual(trips, 1)
	held, found := s.store.Users.seen.Value(app, "jlahey")
	s.True(found, "what redis said is held here")
	s.Equal(UserKindAuthenticated, held)
}

func (s *StoreSuite) TestAUserOneReplicaWroteCostsTheOtherNoWrite() {
	app, other := s.freshApp(), s.replicaUsers()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))
	s.eraseUser(app, "jlahey")

	s.Require().NoError(other.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))

	_, found := s.userRow(app, "jlahey")
	s.False(found, "the key the first replica left in redis is what the second reads")
}

func (s *StoreSuite) TestAGuestWhoCallsWithAFullTokenIsAGuestNoLonger() {
	// The guest sighting is still held, which is what must not stand in for the write.
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindGuest))

	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindAuthenticated))

	kind, _ := s.userRow(app, "guest-1")
	s.Equal(UserKindAuthenticated, kind)
}

func (s *StoreSuite) TestAGuestIsAGuestNoLongerOnAFullTokenWithNoRedis() {
	// Only the LRU holds the guest sighting here, with no invalidation to drop it.
	plain, app := s.usersWithoutRedis(emailTTL), s.freshApp()
	s.Require().NoError(plain.Seen(s.ctx, app, "guest-1", UserKindGuest))

	s.Require().NoError(plain.Seen(s.ctx, app, "guest-1", UserKindAuthenticated))

	kind, _ := s.userRow(app, "guest-1")
	s.Equal(UserKindAuthenticated, kind)
}

func (s *StoreSuite) TestAGuestAnotherReplicaWroteIsAGuestNoLongerOnAFullToken() {
	app, other := s.freshApp(), s.replicaUsers()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindGuest))

	s.Require().NoError(other.Seen(s.ctx, app, "guest-1", UserKindAuthenticated))

	kind, _ := s.userRow(app, "guest-1")
	s.Equal(UserKindAuthenticated, kind, "the guest key in redis is no answer to a full token")
}

func (s *StoreSuite) TestAGuestWhoseKeyAnOlderReplicaWroteIsAGuestNoLongerOnAFullToken() {
	// A replica from before the key held a kind wrote "1", which is no full-token write.
	app, other := s.freshApp(), s.replicaUsers()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindGuest))
	client := s.store.sharedRedis()
	s.Require().NoError(client.Do(s.ctx, client.B().Set().Key(userKey(app, "guest-1")).Value("1").Build()).Error())

	s.Require().NoError(other.Seen(s.ctx, app, "guest-1", UserKindAuthenticated))

	kind, _ := s.userRow(app, "guest-1")
	s.Equal(UserKindAuthenticated, kind, "an older replica's key is no answer to a full token")
}

func (s *StoreSuite) TestAGuestWhoCallsAsAGuestAgainStaysAGuest() {
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindGuest))
	s.store.Users.Forget(s.ctx, app, "guest-1")

	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindGuest))

	kind, _ := s.userRow(app, "guest-1")
	s.Equal(UserKindGuest, kind)
}

func (s *StoreSuite) TestAGuestABackendNamesStaysAGuest() {
	// A backend may act for a guest of its own, which says nothing about who they are.
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindGuest))
	s.store.Users.Forget(s.ctx, app, "guest-1")

	s.Require().NoError(s.store.Users.Named(s.ctx, app, "guest-1"))

	kind, _ := s.userRow(app, "guest-1")
	s.Equal(UserKindGuest, kind)
}

func (s *StoreSuite) TestAUserABackendNamedFirstIsWrittenAgainOnTheirOwnFullToken() {
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Named(s.ctx, app, "jlahey"))
	kind, _ := s.userRow(app, "jlahey")
	s.Require().Equal(UserKindAuthenticated, kind, "a backend's user with no row is recorded as an account")
	s.setUserKind(app, "jlahey", UserKindGuest)

	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))

	kind, _ = s.userRow(app, "jlahey")
	s.Equal(UserKindAuthenticated, kind, "what a backend's naming left held is no answer to a full token")
}

func (s *StoreSuite) TestAFullTokenIsWrittenOnceAndThenHeld() {
	app, other := s.freshApp(), s.replicaUsers()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))
	s.setUserKind(app, "jlahey", UserKindGuest)

	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))
	s.Require().NoError(other.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))

	kind, _ := s.userRow(app, "jlahey")
	s.Equal(UserKindGuest, kind, "a full token already written costs neither replica a write")
}

func (s *StoreSuite) TestAFullTokenLeavesAnAccountsRowUnwritten() {
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))
	before := s.userVersion(app, "jlahey")
	s.store.Users.Forget(s.ctx, app, "jlahey")

	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))

	s.Equal(before, s.userVersion(app, "jlahey"), "an account's row is already what the token says")
}

func (s *StoreSuite) TestASightingNamingNoUserIsNotRecorded() {
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "", UserKindAuthenticated))
	s.Require().NoError(s.store.Users.Named(s.ctx, app, ""))
	s.Require().NoError(s.store.Users.Named(s.ctx, "", "jlahey"))

	_, found := s.userRow(app, "")
	s.False(found)
	_, found = s.userRow("", "jlahey")
	s.False(found)
}

func (s *StoreSuite) TestAGuestTheRouterMintedStaysAGuestOnTheirOwnToken() {
	// The token minted for them names no role, so it reads as a full user's.
	app := s.freshApp()
	s.Require().NoError(s.store.Users.InsertGuest(s.ctx, &User{ID: "guest-1", CustomerID: app}))

	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindAuthenticated))

	kind, _ := s.userRow(app, "guest-1")
	s.Equal(UserKindGuest, kind)
}

func (s *StoreSuite) TestAClaimedGuestStaysAGuestOnAFullToken() {
	// The claim stands: a retried claim reads the guest row.
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindGuest))
	_, err := s.store.ClaimGuest(s.ctx, app, "guest-1", "jlahey")
	s.Require().NoError(err)
	s.store.Users.Forget(s.ctx, app, "guest-1")

	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindAuthenticated))

	kind, _ := s.userRow(app, "guest-1")
	s.Equal(UserKindGuest, kind)
}

func (s *StoreSuite) TestForgettingAUserOnOneReplicaForgetsThemOnTheOther() {
	app, other := s.freshApp(), s.replicaUsers()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "guest-1", UserKindGuest))
	s.Require().NoError(other.Seen(s.ctx, app, "guest-1", UserKindGuest))

	// What a claim leaves behind: the row has changed under both of them, and the one
	// that did not make the change is the one that has to be told.
	s.eraseUser(app, "guest-1")
	s.store.Users.Forget(s.ctx, app, "guest-1")

	s.Require().Eventually(func() bool {
		if err := other.Seen(s.ctx, app, "guest-1", UserKindGuest); err != nil {
			return false
		}
		_, found := s.userRow(app, "guest-1")
		return found
	}, settleFor, 10*time.Millisecond, "the other replica is still answering from a stale cache")
}

func (s *StoreSuite) TestAnAnonymousCallerNamingNoKindIsNotRecorded() {
	app := s.freshApp()
	s.Require().NoError(s.store.Users.Seen(s.ctx, app, "whoever", ""))

	_, found := s.userRow(app, "whoever")
	s.False(found)
}

func (s *StoreSuite) TestADeploymentWithNoRedisStillWritesItsUsersDown() {
	plain, app := s.usersWithoutRedis(emailTTL), s.freshApp()
	s.Require().NoError(plain.Seen(s.ctx, app, "jlahey", UserKindAuthenticated))

	kind, found := s.userRow(app, "jlahey")
	s.True(found)
	s.Equal(UserKindAuthenticated, kind)
}

// TestAnEmailIsWrittenAgainOnceItsTTLEnds changes the row behind Users' back, so a later
// Email putting the address back is evidence that it went to Postgres.
func (s *StoreSuite) TestAnEmailIsWrittenAgainOnceItsTTLEnds() {
	users, app := s.usersWithoutRedis(500*time.Millisecond), s.freshApp()
	s.Require().NoError(users.Email(s.ctx, app, "jlahey", "jim@fake.example"))
	s.Require().NoError(s.store.Users.SetEmail(s.ctx, app, "jlahey", "behind@fake.example"))

	s.Require().NoError(users.Email(s.ctx, app, "jlahey", "jim@fake.example"))
	s.Equal("behind@fake.example", s.userEmail(app, "jlahey"), "an address just written costs no write")

	s.Eventually(func() bool {
		return users.Email(s.ctx, app, "jlahey", "jim@fake.example") == nil &&
			s.userEmail(app, "jlahey") == "jim@fake.example"
	}, settleFor, 10*time.Millisecond, "the hold outlived its ttl")
}

// TestAnEmailIsHeldForAMinuteWhateverTheTTL: the row an email hold may leave stale is what
// a first owner_email consent is checked against, so the hold is a minute even where the
// users' Redis key lives an hour.
func (s *StoreSuite) TestAnEmailIsHeldForAMinuteWhateverTheTTL() {
	app, start := s.freshApp(), time.Now()

	s.Require().NoError(s.store.Users.Email(s.ctx, app, "jlahey", "jim@fake.example"))

	s.True(s.store.Users.emails.Holds(app, "jlahey", "jim@fake.example", start))
	s.False(s.store.Users.emails.Holds(app, "jlahey", "jim@fake.example", time.Now().Add(time.Minute)))
}

// TestAnAddressAnotherReplicaReplacedIsPutBackOnceTheHoldEnds is the reviewer's probe
// (R1.1): one replica writes A, the other writes B, the first hears A again. Until the hold
// ends the row stays B, which the first consent would admit; after it, A is written again.
func (s *StoreSuite) TestAnAddressAnotherReplicaReplacedIsPutBackOnceTheHoldEnds() {
	first, other, app := s.usersWithoutRedis(500*time.Millisecond), s.usersWithoutRedis(500*time.Millisecond), s.freshApp()
	s.Require().NoError(first.Email(s.ctx, app, "jlahey", "jim@fake.example"))
	s.Require().NoError(other.Email(s.ctx, app, "jlahey", "randy@fake.example"))

	s.Require().NoError(first.Email(s.ctx, app, "jlahey", "jim@fake.example"))
	s.Equal("randy@fake.example", s.userEmail(app, "jlahey"), "the window the hold leaves")

	s.Eventually(func() bool {
		return first.Email(s.ctx, app, "jlahey", "jim@fake.example") == nil &&
			s.userEmail(app, "jlahey") == "jim@fake.example"
	}, settleFor, 10*time.Millisecond, "the replaced address outlived the hold")
}

func (s *StoreSuite) TestTheSameUserAndEmailUnderTwoAppsIsWrittenForEach() {
	users, app := s.usersWithoutRedis(time.Hour), s.freshApp()
	one, two := app+"-one", app+"-two"

	s.Require().NoError(users.Email(s.ctx, one, "jlahey", "jim@fake.example"))
	s.Require().NoError(users.Email(s.ctx, two, "jlahey", "jim@fake.example"))

	s.Equal("jim@fake.example", s.userEmail(one, "jlahey"))
	s.Equal("jim@fake.example", s.userEmail(two, "jlahey"), "the same id under two apps is two people")
}

func (s *StoreSuite) TestAnEmailForNoUserIsNotHeld() {
	users, app := s.usersWithoutRedis(time.Hour), s.freshApp()

	s.Require().NoError(users.Email(s.ctx, app, "", "jim@fake.example"))
	s.Require().NoError(users.Email(s.ctx, "", "jlahey", "jim@fake.example"))
	s.Require().NoError(users.Email(s.ctx, app, "jlahey", ""))

	s.Zero(users.emails.Len())
}

func (s *StoreSuite) TestAnEmailThatFailedToBeWrittenIsTriedAgain() {
	closed, err := s.openRouter()
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	users, app := s.usersWithoutRedis(time.Hour), s.freshApp()
	working := users.store
	users.store = closed
	s.Require().Error(users.Email(s.ctx, app, "jlahey", "jim@fake.example"))

	users.store = working
	s.Require().NoError(users.Email(s.ctx, app, "jlahey", "jim@fake.example"))

	s.Equal("jim@fake.example", s.userEmail(app, "jlahey"))
}

// TestTwoAddressesSaidAtOnceLeaveTheHeldOneTheRows: requests saying A and B for one user at
// the same time end with the row and the hold on the same address, whichever it is, so a
// later request for the row's address is not written while one for the other is. Many
// users at once give the scheduler room to pause one request between its write and its
// hold.
func (s *StoreSuite) TestTwoAddressesSaidAtOnceLeaveTheHeldOneTheRows() {
	users, app := s.usersWithoutRedis(time.Hour), s.freshApp()
	addresses := []string{"jim@fake.example", "randy@fake.example"}
	for round := range 10 {
		var group sync.WaitGroup
		for user := range 50 {
			for _, address := range addresses {
				group.Go(func() { s.NoError(users.Email(s.ctx, app, fmt.Sprintf("user-%d-%d", round, user), address)) })
			}
		}
		group.Wait()

		for user := range 50 {
			name := fmt.Sprintf("user-%d-%d", round, user)
			row := s.userEmail(app, name)
			s.Require().True(users.emails.Holds(app, name, row, time.Now()),
				"%s: the row holds %s and the cache does not", name, row)
		}
	}
}

// TestAnEmailWaitsForAWriteInFlightForTheSameUser holds the lock a write in flight holds,
// which is what makes the interleaving above certain rather than likely: a request seen
// while it is held writes nothing until it is let go.
func (s *StoreSuite) TestAnEmailWaitsForAWriteInFlightForTheSameUser() {
	users, app := s.usersWithoutRedis(time.Hour), s.freshApp()
	lock := users.emailLock(app, "jlahey")
	lock.Lock()
	done := make(chan error)
	go func() { done <- users.Email(s.ctx, app, "jlahey", "jim@fake.example") }()

	s.Never(func() bool { return s.userEmail(app, "jlahey") != "" }, 200*time.Millisecond, 10*time.Millisecond,
		"written while another write held the lock")
	lock.Unlock()

	s.Require().NoError(<-done)
	s.Equal("jim@fake.example", s.userEmail(app, "jlahey"))
}
