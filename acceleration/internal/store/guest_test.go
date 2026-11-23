//go:build integration

package store

import (
	"github.com/google/uuid"
)

// guest records a guest of app holding the secret's hash, or none for an empty secret.
func (s *StoreSuite) guest(app, secret string) string {
	id := "guest-" + uuid.NewString()
	guest := User{ID: id, CustomerID: app}
	if secret != "" {
		guest.RefreshSecretHash = GuestSecretHash(secret)
	}
	s.Require().NoError(s.store.RecordGuest(s.ctx, &guest))
	return id
}

func (s *StoreSuite) TestAGuestsRefreshSecretIsSpentOnce() {
	id := s.guest("app", "first")

	guest, err := s.store.Guests.RotateSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("second"))
	s.Require().NoError(err)
	s.Equal(id, guest.ID)
	_, err = s.store.Guests.RotateSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("third"))
	s.ErrorIs(err, ErrGuestSecretRefused, "replayed")
	_, err = s.store.Guests.RotateSecret(s.ctx, "other-app", id, GuestSecretHash("second"), GuestSecretHash("third"))
	s.ErrorIs(err, ErrGuestSecretRefused, "another app's guest")
	_, err = s.store.Guests.RotateSecret(s.ctx, "app", id, GuestSecretHash("second"), GuestSecretHash("third"))
	s.NoError(err, "the secret it was handed last")
}

func (s *StoreSuite) TestAClaimedGuestTakesNoSecretOrProfile() {
	id := s.guest("app", "first")
	_, err := s.store.ClaimGuest(s.ctx, "app", id, "ann")
	s.Require().NoError(err)

	_, err = s.store.Guests.RotateSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("second"))
	s.ErrorIs(err, ErrGuestSecretRefused)
	s.Error(s.store.Guests.SetSecret(s.ctx, "app", id, GuestSecretHash("second")))
	s.Error(s.store.Guests.SetProfile(s.ctx, "app", id, "Ann", nil))
	given, err := s.store.Guests.GiveSecret(s.ctx, "app", id, GuestSecretHash("second"))
	s.Require().NoError(err)
	s.False(given)
}

func (s *StoreSuite) TestAGuestIsGivenASecretOnlyWhenItHoldsNone() {
	bare, holding := s.guest("app", ""), s.guest("app", "kept")

	given, err := s.store.Guests.GiveSecret(s.ctx, "app", bare, GuestSecretHash("new"))
	s.Require().NoError(err)
	s.True(given)
	given, err = s.store.Guests.GiveSecret(s.ctx, "app", holding, GuestSecretHash("new"))
	s.Require().NoError(err)
	s.False(given)
	_, err = s.store.Guests.RotateSecret(s.ctx, "app", holding, GuestSecretHash("kept"), GuestSecretHash("next"))
	s.NoError(err, "the secret it held still works")

	s.Require().NoError(s.store.Guests.SetSecret(s.ctx, "app", holding, GuestSecretHash("replaced")))
	_, err = s.store.Guests.RotateSecret(s.ctx, "app", holding, GuestSecretHash("replaced"), GuestSecretHash("after"))
	s.NoError(err, "a secret set without the old one replaces it")
	s.Error(s.store.Guests.SetSecret(s.ctx, "other-app", holding, GuestSecretHash("stolen")), "another app's guest")
}

func (s *StoreSuite) TestAGuestsProfileIsWhatTheBackendLastSaid() {
	id := s.guest("app", "")

	s.Require().NoError(s.store.Guests.SetProfile(s.ctx, "app", id, "Ann", map[string]any{"plan": "pro"}))

	var guest User
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&guest).Where("customer_id = ? AND id = ?", "app", id).Scan(s.ctx))
	s.Equal("Ann", guest.Name)
	s.Equal(map[string]any{"plan": "pro"}, guest.Custom)
	s.Error(s.store.Guests.SetProfile(s.ctx, "app", "never-recorded", "Bob", nil))
}

func (s *StoreSuite) TestGuestQueryBudgets() {
	id := s.guest("app", "first")
	bare := s.guest("app", "")

	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Guests.RotateSecret(s.ctx, "app", id, GuestSecretHash("first"), GuestSecretHash("second"))
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Guests.SetSecret(s.ctx, "app", id, GuestSecretHash("third"))) }))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Guests.GiveSecret(s.ctx, "app", bare, GuestSecretHash("given"))
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Guests.SetProfile(s.ctx, "app", id, "Ann", nil)) }))
}
