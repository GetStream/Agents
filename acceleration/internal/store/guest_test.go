//go:build integration

package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// guest records a guest of app holding the secret's hash, or none for an empty secret.
func (s *StoreSuite) guest(app, secret string) string {
	id := "guest-" + uuid.NewString()
	guest := User{ID: id, CustomerID: app}
	if secret != "" {
		guest.RefreshSecretHash = GuestSecretHash(secret)
	}
	s.Require().NoError(s.store.Users.InsertGuest(s.ctx, &guest))
	return id
}

func (s *StoreSuite) TestAGuestsRefreshSecretIsSpentOnce() {
	id := s.guest("app", "first")

	guest, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "second")
	s.Require().NoError(err)
	s.Equal(id, guest.ID)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "third")
	s.ErrorIs(err, ErrGuestSecretRefused, "replayed")
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "other-app", id, "second", "third")
	s.ErrorIs(err, ErrGuestSecretRefused, "another app's guest")
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "second", "third")
	s.NoError(err, "the secret it was handed last")
}

func (s *StoreSuite) TestAClaimedGuestTakesNoSecretOrProfile() {
	id := s.guest("app", "first")
	_, err := s.store.Users.ClaimGuest(s.ctx, "app", id, "ann")
	s.Require().NoError(err)

	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "second")
	s.ErrorIs(err, ErrGuestSecretRefused)
	s.Error(s.store.Users.SetGuestSecret(s.ctx, "app", id, "second"))
	s.Error(s.store.Users.SetGuestProfile(s.ctx, "app", id, "Ann", nil))
	given, err := s.store.Users.GiveGuestSecret(s.ctx, "app", id, "second")
	s.Require().NoError(err)
	s.False(given)
}

func (s *StoreSuite) TestAGuestIsGivenASecretOnlyWhenItHoldsNone() {
	bare, holding := s.guest("app", ""), s.guest("app", "kept")

	given, err := s.store.Users.GiveGuestSecret(s.ctx, "app", bare, "new")
	s.Require().NoError(err)
	s.True(given)
	given, err = s.store.Users.GiveGuestSecret(s.ctx, "app", holding, "new")
	s.Require().NoError(err)
	s.False(given)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", holding, "kept", "next")
	s.NoError(err, "the secret it held still works")

	s.Require().NoError(s.store.Users.SetGuestSecret(s.ctx, "app", holding, "replaced"))
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", holding, "replaced", "after")
	s.NoError(err, "a secret set without the old one replaces it")
	s.Error(s.store.Users.SetGuestSecret(s.ctx, "other-app", holding, "stolen"), "another app's guest")
}

func (s *StoreSuite) TestAGuestsProfileIsWhatTheBackendLastSaid() {
	id := s.guest("app", "")

	s.Require().NoError(s.store.Users.SetGuestProfile(s.ctx, "app", id, "Ann", map[string]any{"plan": "pro"}))

	var guest User
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&guest).Where("customer_id = ? AND id = ?", "app", id).Scan(s.ctx))
	s.Equal("Ann", guest.Name)
	s.Equal(map[string]any{"plan": "pro"}, guest.Custom)
	s.Error(s.store.Users.SetGuestProfile(s.ctx, "app", "never-recorded", "Bob", nil))
}

func (s *StoreSuite) TestGuestQueryBudgets() {
	id := s.guest("app", "first")
	bare := s.guest("app", "")

	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "second")
		s.Require().NoError(err)
	}))
	s.Equal(2, s.countQueries(func() {
		_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, "wrong", "next")
		s.Require().ErrorIs(err, ErrGuestSecretRefused)
	}), "a refused secret is checked against the one spent last")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Users.SetGuestSecret(s.ctx, "app", id, "third")) }))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Users.GiveGuestSecret(s.ctx, "app", bare, "given")
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Users.SetGuestProfile(s.ctx, "app", id, "Ann", nil)) }))
}

// guestSecrets reads what a guest holds of their refresh secrets.
func (s *StoreSuite) guestSecrets(app, id string) User {
	var guest User
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&guest).Where("customer_id = ? AND id = ?", app, id).Scan(s.ctx))
	return guest
}

// ageGuestSecret moves a guest's secret expiry back by age, as if it had been issued or rotated
// that much earlier.
func (s *StoreSuite) ageGuestSecret(app, id string, age time.Duration) {
	_, err := s.store.GlobalDB().NewUpdate().Model((*User)(nil)).
		Set("refresh_secret_expires_at = refresh_secret_expires_at - make_interval(secs => ?)", age.Seconds()).
		Where("customer_id = ? AND id = ?", app, id).
		Exec(s.ctx)
	s.Require().NoError(err)
}

func (s *StoreSuite) TestAnExpiredRefreshSecretIsRefused() {
	id := s.guest("app", "first")
	s.ageGuestSecret("app", id, GuestSecretTTL+time.Second)

	_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "second")

	s.ErrorIs(err, ErrGuestSecretRefused)
	s.Equal(GuestSecretHash("first"), s.guestSecrets("app", id).RefreshSecretHash, "nothing was spent")
}

func (s *StoreSuite) TestEveryWayAGuestIsHandedASecretStartsItsThirtyDays() {
	inserted, bare := s.guest("app", "first"), s.guest("app", "")
	startsNow := func(id, how string) {
		expires := s.guestSecrets("app", id).RefreshSecretExpiresAt
		s.Require().NotNil(expires, how)
		s.WithinDuration(time.Now().Add(GuestSecretTTL), *expires, time.Minute, how)
	}
	startsNow(inserted, "minted")

	s.ageGuestSecret("app", inserted, GuestSecretTTL-time.Hour)
	_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", inserted, "first", "second")
	s.Require().NoError(err)
	startsNow(inserted, "rotated")

	s.ageGuestSecret("app", inserted, GuestSecretTTL-time.Hour)
	s.Require().NoError(s.store.Users.SetGuestSecret(s.ctx, "app", inserted, "third"))
	startsNow(inserted, "set by the backend")

	s.ageGuestSecret("app", bare, GuestSecretTTL+time.Hour)
	given, err := s.store.Users.GiveGuestSecret(s.ctx, "app", bare, "given")
	s.Require().NoError(err)
	s.Require().True(given)
	startsNow(bare, "given to a guest holding none")
}

func (s *StoreSuite) TestASpentSecretPresentedAgainClearsTheGuestsSecretAndLeavesAMark() {
	id := s.guest("app", "first")
	_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "second")
	s.Require().NoError(err)
	s.ageGuestSecret("app", id, GuestSecretReplayGrace+time.Second)

	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "thief")

	s.ErrorIs(err, ErrGuestSecretReplayed)
	held := s.guestSecrets("app", id)
	s.Nil(held.RefreshSecretHash)
	s.NotNil(held.PreviousRefreshSecretHash, "the mark that keeps the guest's own token from re-arming them")
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "second", "third")
	s.ErrorIs(err, ErrGuestSecretRefused, "the secret the rotation handed out no longer works either")
	given, err := s.store.Users.GiveGuestSecret(s.ctx, "app", id, "given")
	s.Require().NoError(err)
	s.False(given, "the token asking may be the thief's")

	s.Require().NoError(s.store.Users.SetGuestSecret(s.ctx, "app", id, "from-the-backend"))
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "from-the-backend", "after")
	s.NoError(err, "the backend brings the guest back")
}

func (s *StoreSuite) TestASpentSecretPresentedRightAfterItsRotationIsOnlyRefused() {
	// The same device sending it twice at once, the second just behind the first.
	id := s.guest("app", "first")
	_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "second")
	s.Require().NoError(err)

	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "again")

	s.ErrorIs(err, ErrGuestSecretRefused)
	s.NotErrorIs(err, ErrGuestSecretReplayed)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "second", "third")
	s.NoError(err, "the secret the rotation handed out still works")
}

func (s *StoreSuite) TestConcurrentRedeemsOfOneSecretRotateItOnceAndClearNothing() {
	id := s.guest("app", "first")
	const redeems = 8
	errs := make([]error, redeems)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range redeems {
		wg.Go(func() {
			<-start
			_, errs[i] = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", fmt.Sprint("next-", i))
		})
	}
	close(start)
	wg.Wait()

	winner := -1
	for i, err := range errs {
		if err == nil {
			s.Equal(-1, winner, "only one redeem rotates the secret")
			winner = i
			continue
		}
		s.ErrorIs(err, ErrGuestSecretRefused)
		s.NotErrorIs(err, ErrGuestSecretReplayed)
	}
	s.Require().NotEqual(-1, winner)
	s.Equal(GuestSecretHash(fmt.Sprint("next-", winner)), s.guestSecrets("app", id).RefreshSecretHash)
}

func (s *StoreSuite) TestASecretTheBackendSetIsNotClearedByAnOlderOneComingBack() {
	// The backend decided anew which device is this guest; what came before is no sign of a leak.
	id := s.guest("app", "first")
	_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "second")
	s.Require().NoError(err)
	s.Require().NoError(s.store.Users.SetGuestSecret(s.ctx, "app", id, "set"))
	s.ageGuestSecret("app", id, GuestSecretReplayGrace+time.Second)

	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "first", "stale")

	s.ErrorIs(err, ErrGuestSecretRefused)
	s.NotErrorIs(err, ErrGuestSecretReplayed)
	s.Equal(GuestSecretHash("set"), s.guestSecrets("app", id).RefreshSecretHash)
}

// familyGuest records a guest of app holding a secret of a new family, and returns its id and
// the secret.
func (s *StoreSuite) familyGuest(app string) (string, string) {
	secret, hash, err := NewGuestSecret()
	s.Require().NoError(err)
	id := "guest-" + uuid.NewString()
	s.Require().NoError(s.store.Users.InsertGuest(s.ctx, &User{
		ID: id, CustomerID: app, RefreshSecretHash: hash, RefreshFamilySecretHash: GuestSecretFamilyHash(secret),
	}))
	return id, secret
}

// rotate redeems spent for the next secret of its family.
func (s *StoreSuite) rotate(app, id, spent string) string {
	next, _, err := NextGuestSecret(spent)
	s.Require().NoError(err)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, app, id, spent, next)
	s.Require().NoError(err)
	return next
}

func (s *StoreSuite) TestASecretSpentRotationsAgoIsSeenAsLeaked() {
	// A thief redeemed the victim's secret and rotated twice before the victim came back.
	id, victims := s.familyGuest("app")
	thiefs := s.rotate("app", id, s.rotate("app", id, victims))
	s.ageGuestSecret("app", id, GuestSecretReplayGrace+time.Second)

	next, _, err := NextGuestSecret(victims)
	s.Require().NoError(err)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, victims, next)

	s.ErrorIs(err, ErrGuestSecretReplayed)
	s.Nil(s.guestSecrets("app", id).RefreshSecretHash)
	next, _, err = NextGuestSecret(thiefs)
	s.Require().NoError(err)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, thiefs, next)
	s.Error(err, "the thief's secret no longer works")
}

func (s *StoreSuite) TestAnotherFamilysSecretIsOnlyWrong() {
	id, held := s.familyGuest("app")
	stranger, _, err := NewGuestSecret()
	s.Require().NoError(err)
	next, _, err := NextGuestSecret(stranger)
	s.Require().NoError(err)

	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, stranger, next)

	s.ErrorIs(err, ErrGuestSecretRefused)
	s.NotErrorIs(err, ErrGuestSecretReplayed)
	s.Equal(GuestSecretHash(held), s.guestSecrets("app", id).RefreshSecretHash)
}

func (s *StoreSuite) TestAnExpiredSecretOfTheFamilyIsOnlyRefused() {
	id, held := s.familyGuest("app")
	s.ageGuestSecret("app", id, GuestSecretTTL+time.Second)
	next, _, err := NextGuestSecret(held)
	s.Require().NoError(err)

	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, held, next)

	s.ErrorIs(err, ErrGuestSecretRefused)
	s.NotErrorIs(err, ErrGuestSecretReplayed)
}

func (s *StoreSuite) TestAGuestWhoseSecretExpiredIsGivenANewOneByTheirOwnToken() {
	id := s.guest("app", "first")
	s.rotate("app", id, "first")
	s.ageGuestSecret("app", id, GuestSecretTTL+time.Second)

	given, err := s.store.Users.GiveGuestSecret(s.ctx, "app", id, "given")

	s.Require().NoError(err)
	s.True(given)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, "given", "next")
	s.NoError(err)
}

func (s *StoreSuite) TestASecretFromBeforeTheMigrationRedeemsAndStartsAFamily() {
	// Written as a router of the release before wrote it: no expiry, no family.
	id := "guest-" + uuid.NewString()
	_, err := s.store.GlobalDB().ExecContext(s.ctx,
		"INSERT INTO users (customer_id, id, kind, minted, refresh_secret_hash) VALUES (?, ?, ?, true, ?)",
		"app", id, UserKindGuest, GuestSecretHash("from-before"))
	s.Require().NoError(err)
	expires := s.guestSecrets("app", id).RefreshSecretExpiresAt
	s.Require().NotNil(expires, "the column's default dates it")
	s.WithinDuration(time.Now().Add(GuestSecretTTL), *expires, time.Minute)

	next := s.rotate("app", id, "from-before")

	held := s.guestSecrets("app", id)
	s.Equal(GuestSecretHash(next), held.RefreshSecretHash)
	s.NotNil(held.RefreshFamilySecretHash)
	s.Equal(GuestSecretFamilyHash(next), held.RefreshFamilySecretHash)
}

func (s *StoreSuite) TestARotationThatLandsWhileARedeemOfTheSameSecretWaitsIsNoSignOfALeak() {
	// The rotation holds the row while the second redeem of the same secret waits on it, then
	// commits: that redeem misses, and the check that follows sees the rotation it just lost to.
	id, held := s.familyGuest("app")
	won, _, err := NextGuestSecret(held)
	s.Require().NoError(err)
	tx, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	_, err = tx.NewUpdate().Model((*User)(nil)).
		Set("previous_refresh_secret_hash = refresh_secret_hash").
		Set("refresh_secret_hash = ?", GuestSecretHash(won)).
		Set("refresh_secret_expires_at = now() + make_interval(secs => ?)", GuestSecretTTL.Seconds()).
		Where("customer_id = ? AND id = ?", "app", id).
		Exec(s.ctx)
	s.Require().NoError(err)

	lost := make(chan error, 1)
	go func() {
		next, _, err := NextGuestSecret(held)
		if err == nil {
			_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, held, next)
		}
		lost <- err
	}()
	s.Eventually(func() bool {
		var waiting int
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'").Scan(&waiting))
		return waiting > 0
	}, 10*time.Second, 10*time.Millisecond, "the second redeem waits on the row")
	s.Require().NoError(tx.Commit())

	err = <-lost
	s.ErrorIs(err, ErrGuestSecretRefused)
	s.NotErrorIs(err, ErrGuestSecretReplayed)
	s.Equal(GuestSecretHash(won), s.guestSecrets("app", id).RefreshSecretHash)
}
