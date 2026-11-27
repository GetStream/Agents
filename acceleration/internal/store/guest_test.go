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

	guest, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("second"))
	s.Require().NoError(err)
	s.Equal(id, guest.ID)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("third"))
	s.ErrorIs(err, ErrGuestSecretRefused, "replayed")
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "other-app", id, GuestSecretHash("second"), GuestSecretHash("third"))
	s.ErrorIs(err, ErrGuestSecretRefused, "another app's guest")
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("second"), GuestSecretHash("third"))
	s.NoError(err, "the secret it was handed last")
}

func (s *StoreSuite) TestAClaimedGuestTakesNoSecretOrProfile() {
	id := s.guest("app", "first")
	_, err := s.store.Users.ClaimGuest(s.ctx, "app", id, "ann")
	s.Require().NoError(err)

	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("second"))
	s.ErrorIs(err, ErrGuestSecretRefused)
	s.Error(s.store.Users.SetGuestSecret(s.ctx, "app", id, GuestSecretHash("second")))
	s.Error(s.store.Users.SetGuestProfile(s.ctx, "app", id, "Ann", nil))
	given, err := s.store.Users.GiveGuestSecret(s.ctx, "app", id, GuestSecretHash("second"))
	s.Require().NoError(err)
	s.False(given)
}

func (s *StoreSuite) TestAGuestIsGivenASecretOnlyWhenItHoldsNone() {
	bare, holding := s.guest("app", ""), s.guest("app", "kept")

	given, err := s.store.Users.GiveGuestSecret(s.ctx, "app", bare, GuestSecretHash("new"))
	s.Require().NoError(err)
	s.True(given)
	given, err = s.store.Users.GiveGuestSecret(s.ctx, "app", holding, GuestSecretHash("new"))
	s.Require().NoError(err)
	s.False(given)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", holding, GuestSecretHash("kept"), GuestSecretHash("next"))
	s.NoError(err, "the secret it held still works")

	s.Require().NoError(s.store.Users.SetGuestSecret(s.ctx, "app", holding, GuestSecretHash("replaced")))
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", holding, GuestSecretHash("replaced"), GuestSecretHash("after"))
	s.NoError(err, "a secret set without the old one replaces it")
	s.Error(s.store.Users.SetGuestSecret(s.ctx, "other-app", holding, GuestSecretHash("stolen")), "another app's guest")
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
		_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("second"))
		s.Require().NoError(err)
	}))
	s.Equal(2, s.countQueries(func() {
		_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("wrong"), GuestSecretHash("next"))
		s.Require().ErrorIs(err, ErrGuestSecretRefused)
	}), "a refused secret is checked against the one spent last")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Users.SetGuestSecret(s.ctx, "app", id, GuestSecretHash("third"))) }))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Users.GiveGuestSecret(s.ctx, "app", bare, GuestSecretHash("given"))
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

	_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("second"))

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
	_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", inserted, GuestSecretHash("first"), GuestSecretHash("second"))
	s.Require().NoError(err)
	startsNow(inserted, "rotated")

	s.ageGuestSecret("app", inserted, GuestSecretTTL-time.Hour)
	s.Require().NoError(s.store.Users.SetGuestSecret(s.ctx, "app", inserted, GuestSecretHash("third")))
	startsNow(inserted, "set by the backend")

	s.ageGuestSecret("app", bare, GuestSecretTTL+time.Hour)
	given, err := s.store.Users.GiveGuestSecret(s.ctx, "app", bare, GuestSecretHash("given"))
	s.Require().NoError(err)
	s.Require().True(given)
	startsNow(bare, "given to a guest holding none")
}

func (s *StoreSuite) TestASpentSecretPresentedAgainClearsBothOfTheGuestsSecrets() {
	id := s.guest("app", "first")
	_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("second"))
	s.Require().NoError(err)
	s.ageGuestSecret("app", id, GuestSecretReplayGrace+time.Second)

	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("thief"))

	s.ErrorIs(err, ErrGuestSecretReplayed)
	held := s.guestSecrets("app", id)
	s.Nil(held.RefreshSecretHash)
	s.Nil(held.PreviousRefreshSecretHash)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("second"), GuestSecretHash("third"))
	s.ErrorIs(err, ErrGuestSecretRefused, "the secret the rotation handed out no longer works either")
}

func (s *StoreSuite) TestASpentSecretPresentedRightAfterItsRotationIsOnlyRefused() {
	// The same device sending it twice at once, the second just behind the first.
	id := s.guest("app", "first")
	_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("second"))
	s.Require().NoError(err)

	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("again"))

	s.ErrorIs(err, ErrGuestSecretRefused)
	s.NotErrorIs(err, ErrGuestSecretReplayed)
	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("second"), GuestSecretHash("third"))
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
			_, errs[i] = s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash(fmt.Sprint("next-", i)))
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
	_, err := s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("second"))
	s.Require().NoError(err)
	s.Require().NoError(s.store.Users.SetGuestSecret(s.ctx, "app", id, GuestSecretHash("set")))
	s.ageGuestSecret("app", id, GuestSecretReplayGrace+time.Second)

	_, err = s.store.Users.RotateGuestSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("stale"))

	s.ErrorIs(err, ErrGuestSecretRefused)
	s.NotErrorIs(err, ErrGuestSecretReplayed)
	s.Equal(GuestSecretHash("set"), s.guestSecrets("app", id).RefreshSecretHash)
}
