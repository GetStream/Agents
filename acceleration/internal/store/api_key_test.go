//go:build integration

package store

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
)

// key stores a credential for an app, with the expiry given.
func (s *StoreSuite) key(appID, id string, expires *time.Time) APIKey {
	key := APIKey{
		ID:         id,
		AppID:      appID,
		Name:       "ci",
		Env:        "live",
		Sealed:     []byte("sealed-" + id),
		KEKVersion: 1,
		Last4:      "cret",
		ExpiresAt:  expires,
	}
	s.Require().NoError(s.store.APIKeys.Insert(s.ctx, &key))
	return key
}

func (s *StoreSuite) TestALiveKeyNamesTheAppAndOrganizationItBelongsTo() {
	app := s.app()
	s.key(app.ID, "vak_live_aaaaaaaaaaaaaaaa00000000", nil)

	owner, err := s.store.APIKeys.GetLive(s.ctx, "vak_live_aaaaaaaaaaaaaaaa00000000")
	s.Require().NoError(err)

	s.Equal(app.ID, owner.AppID)
	s.Equal(app.OrganizationID, owner.OrganizationID)
	s.Equal([]byte("sealed-vak_live_aaaaaaaaaaaaaaaa00000000"), owner.Sealed)
	s.Equal(1, owner.KEKVersion)
}

func (s *StoreSuite) TestAKeyThatWasNeverIssuedIsNotLive() {
	_, err := s.store.APIKeys.GetLive(s.ctx, "vak_live_ffffffffffffffff00000000")
	s.ErrorIs(err, ErrNoAPIKey)
}

func (s *StoreSuite) TestARevokedKeyStopsWorkingButItsRowSurvives() {
	app := s.app()
	s.key(app.ID, "vak_live_bbbbbbbbbbbbbbbb00000000", nil)

	_, err := s.store.APIKeys.Revoke(s.ctx, app.ID, "vak_live_bbbbbbbbbbbbbbbb00000000", "thierry")
	s.Require().NoError(err)

	_, err = s.store.APIKeys.GetLive(s.ctx, "vak_live_bbbbbbbbbbbbbbbb00000000")
	s.ErrorIs(err, ErrNoAPIKey)

	// The audit trail of which key made which call has to outlive the key.
	keys, err := s.store.APIKeys.Select(s.ctx, app.ID)
	s.Require().NoError(err)
	s.Require().Len(keys, 1)
	s.NotNil(keys[0].RevokedAt)
	s.Equal("thierry", keys[0].RevokedBy)
}

func (s *StoreSuite) TestRevokingAKeyTwiceSaysItWasAlreadyGone() {
	app := s.app()
	s.key(app.ID, "vak_live_cccccccccccccccc00000000", nil)

	_, err := s.store.APIKeys.Revoke(s.ctx, app.ID, "vak_live_cccccccccccccccc00000000", "thierry")
	s.Require().NoError(err)
	_, err = s.store.APIKeys.Revoke(s.ctx, app.ID, "vak_live_cccccccccccccccc00000000", "thierry")
	s.ErrorIs(err, ErrNoAPIKey)
}

func (s *StoreSuite) TestAnExpiredKeyIsNotLive() {
	app := s.app()
	past := time.Now().UTC().Add(-time.Hour)
	future := time.Now().UTC().Add(time.Hour)
	s.key(app.ID, "vak_live_dddddddddddddddd00000000", &past)
	s.key(app.ID, "vak_live_eeeeeeeeeeeeeeee00000000", &future)

	_, err := s.store.APIKeys.GetLive(s.ctx, "vak_live_dddddddddddddddd00000000")
	s.ErrorIs(err, ErrNoAPIKey)

	_, err = s.store.APIKeys.GetLive(s.ctx, "vak_live_eeeeeeeeeeeeeeee00000000")
	s.NoError(err)
}

func (s *StoreSuite) TestAnAppKeepsSeveralLiveKeysSoRotationIsNotAnOutage() {
	app := s.app()
	s.key(app.ID, "vak_live_1111111111111111ba79ee27", nil)
	s.key(app.ID, "vak_live_2222222222222222d4c94e40", nil)

	for _, id := range []string{"vak_live_1111111111111111ba79ee27", "vak_live_2222222222222222d4c94e40"} {
		_, err := s.store.APIKeys.GetLive(s.ctx, id)
		s.NoError(err, id)
	}
}

func (s *StoreSuite) TestAKeyRevokedOnOneReplicaStopsWorkingOnTheOther() {
	replica := s.replica()
	app := s.app()
	s.key(app.ID, "vak_live_4444444444444444aaaaaaaa", nil)
	// Both replicas hold it before it is revoked, which is the state a revocation has to undo.
	for _, router := range []*Store{s.store, replica} {
		_, err := router.APIKeys.GetLive(s.ctx, "vak_live_4444444444444444aaaaaaaa")
		s.Require().NoError(err)
	}

	_, err := s.store.APIKeys.Revoke(s.ctx, app.ID, "vak_live_4444444444444444aaaaaaaa", "thierry")
	s.Require().NoError(err)

	s.Eventually(func() bool {
		_, err := replica.APIKeys.GetLive(s.ctx, "vak_live_4444444444444444aaaaaaaa")
		return errors.Is(err, ErrNoAPIKey)
	}, 2*time.Second, 10*time.Millisecond, "the other replica still takes a revoked key")
}

func (s *StoreSuite) TestAKeyCachedWhileLiveStopsWorkingWhenItLapses() {
	app := s.app()
	lapses := time.Now().UTC().Add(300 * time.Millisecond)
	s.key(app.ID, "vak_live_5555555555555555aaaaaaaa", &lapses)
	_, err := s.store.APIKeys.GetLive(s.ctx, "vak_live_5555555555555555aaaaaaaa")
	s.Require().NoError(err)

	s.Eventually(func() bool {
		_, err := s.store.APIKeys.GetLive(s.ctx, "vak_live_5555555555555555aaaaaaaa")
		return errors.Is(err, ErrNoAPIKey)
	}, 2*time.Second, 10*time.Millisecond, "a lapsed key read from the cache still works")
}

// A key revoked where nothing deletes its cached owner stops working when the cached owner
// expires, so that expiry is the bound on how long such a key keeps working.
func (s *StoreSuite) TestAKeysCachedOwnerExpiresWithinAMinute() {
	if s.store.cache == nil {
		s.T().Skip("ROUTER_REDIS_ADDR not set: the cache is what this bounds")
	}
	app := s.app()
	key := s.key(app.ID, "vak_live_6666666666666666aaaaaaaa", nil)
	_, err := s.store.APIKeys.GetLive(s.ctx, key.ID)
	s.Require().NoError(err)

	redis := s.store.sharedRedis()
	ttl, err := redis.Do(s.ctx, redis.B().Pttl().Key(cacheKey("key", key.ID)).Build()).AsInt64()
	s.Require().NoError(err)
	s.Positive(ttl, "the owner is cached")
	s.LessOrEqual(ttl, time.Minute.Milliseconds(), "a key revoked without telling the cache keeps working for as long as its owner is cached")
}

func (s *StoreSuite) TestAnAppCannotRevokeAnotherAppsKey() {
	owner, other := s.app(), s.app()
	key := s.key(owner.ID, "vak_live_7777777777777777aaaaaaaa", nil)

	_, err := s.store.APIKeys.Revoke(s.ctx, other.ID, key.ID, "thierry")
	s.ErrorIs(err, ErrNoAPIKey)

	_, err = s.store.APIKeys.GetLive(s.ctx, key.ID)
	s.NoError(err, "the key still works for the app that holds it")
	keys, err := s.store.APIKeys.Select(s.ctx, owner.ID)
	s.Require().NoError(err)
	s.Nil(keys[0].RevokedAt)
}

func (s *StoreSuite) TestAKeySealedUnderAnOlderKEKStillAuthenticatesAfterRotation() {
	before, err := auth.NewSealerWithKeyring(1, map[int]string{1: "first key"})
	s.Require().NoError(err)
	after, err := auth.NewSealerWithKeyring(2, map[int]string{1: "first key", 2: "second key"})
	s.Require().NoError(err)
	app := s.app()
	id, secret, err := auth.NewCredential(auth.Live)
	s.Require().NoError(err)
	sealed, err := before.SealWithAAD(secret, APIKeyAAD(app.ID, id))
	s.Require().NoError(err)
	s.Require().NoError(s.store.APIKeys.Insert(s.ctx, &APIKey{ID: id, AppID: app.ID, Name: "ci", Env: "live", Sealed: sealed, KEKVersion: 1, Last4: "cret"}))


	resolved, err := s.store.APIKeys.Lookup(after)(s.ctx, id)
	s.Require().NoError(err)
	s.Equal(secret, resolved.Secret)
	stored := s.storedKey(id)
	s.Equal(1, stored.KEKVersion, "a bound row is not written on use")
	s.Equal(sealed, stored.Sealed)
}

func (s *StoreSuite) TestALookupResolvesAKeyToItsAppAndTheEndUsersItAdmits() {
	sealer, err := auth.NewSealerWithKeyring(1, map[int]string{1: "store test key"})
	s.Require().NoError(err)
	app := s.app()
	_, err = s.store.GlobalDB().NewUpdate().Model((*App)(nil)).Set(`settings = '{"allow_guest": false}'`).Where("id = ?", app.ID).Exec(s.ctx)
	s.Require().NoError(err)
	id, secret, err := auth.NewCredential(auth.Live)
	s.Require().NoError(err)
	sealed, err := sealer.SealWithAAD(secret, APIKeyAAD(app.ID, id))
	s.Require().NoError(err)
	key := APIKey{ID: id, AppID: app.ID, Name: "ci", Env: "live", Sealed: sealed, KEKVersion: 1, Last4: "cret"}
	s.Require().NoError(s.store.APIKeys.Insert(s.ctx, &key))
	lookup := s.store.APIKeys.Lookup(sealer)

	resolved, err := lookup(s.ctx, key.ID)
	s.Require().NoError(err)
	s.Equal(auth.App{
		OrganizationID: app.OrganizationID, AppID: app.ID, Secret: secret,
		Levels: auth.Levels{NoGuest: true},
	}, resolved)

	unissued, _, err := auth.NewCredential(auth.Live)
	s.Require().NoError(err)
	for _, refused := range []string{id[:len(id)-1], unissued} {
		_, err := lookup(s.ctx, refused)
		s.ErrorIs(err, auth.ErrUnauthenticated, refused)
	}
}

func (s *StoreSuite) TestAPIKeyQueryBudgets() {
	app := s.app()
	key := APIKey{ID: "vak_live_8888888888888888aaaaaaaa", AppID: app.ID, Name: "ci", Env: "live", Sealed: []byte("sealed"), KEKVersion: 1, Last4: "cret"}

	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.APIKeys.Insert(s.ctx, &key)) }))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.APIKeys.Select(s.ctx, app.ID)
		s.Require().NoError(err)
	}))
	s.Equal(0, s.countQueries(func() {
		_, err := s.store.APIKeys.Lookup(nil)(s.ctx, "not-a-key")
		s.ErrorIs(err, auth.ErrUnauthenticated)
	}), "a key of the wrong shape is refused before the database is asked")
	read := func() {
		_, err := s.store.APIKeys.GetLive(s.ctx, key.ID)
		s.Require().NoError(err)
	}
	s.Equal(1, s.countQueries(read), "the owner is one join")
	if s.store.cache != nil {
		read()
		s.Equal(0, s.countQueries(func() { s.Equal(0, s.countRedis(read)) }), "then the local cache answers")
	}
	s.Equal(3+s.redisTrips(1), s.countQueries(func() {
		s.Equal(s.redisTrips(1), s.countRedis(func() {
			_, err := s.store.APIKeys.Revoke(s.ctx, app.ID, key.ID, "thierry")
			s.Require().NoError(err)
		}))
	}), "begin, the write, its transaction's id when there is a cache, and commit: a write of a cached row runs detached in a transaction of its own")
}

// storedKey is the row of a key as Postgres holds it, past any cache.
func (s *StoreSuite) storedKey(id string) APIKey {
	var key APIKey
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&key).Where("id = ?", id).Scan(s.ctx))
	return key
}

// sealedKey stores a fresh credential of the app, its secret sealed with the AAD given.
func (s *StoreSuite) sealedKey(sealer *auth.Sealer, appID string, aad func(id string) []byte) (id, secret string) {
	id, secret, err := auth.NewCredential(auth.Live)
	s.Require().NoError(err)
	sealed, err := sealer.SealWithAAD(secret, aad(id))
	s.Require().NoError(err)
	s.Require().NoError(s.store.APIKeys.Insert(s.ctx, &APIKey{
		ID: id, AppID: appID, Name: "ci", Env: "live", Sealed: sealed, KEKVersion: sealer.CurrentVersion(), Last4: "cret",
	}))
	return id, secret
}

func unbound(string) []byte { return nil }

func (s *StoreSuite) TestASecretCopiedOntoAnotherKeysRowDoesNotOpen() {
	sealer, err := auth.NewSealerWithKeyring(1, map[int]string{1: "store test key"})
	s.Require().NoError(err)
	app, other := s.app(), s.app()
	bindTo := func(appID string) func(string) []byte { return func(id string) []byte { return APIKeyAAD(appID, id) } }
	copied, secret := s.sealedKey(sealer, app.ID, bindTo(app.ID))
	sibling, _ := s.sealedKey(sealer, app.ID, bindTo(app.ID))
	stranger, _ := s.sealedKey(sealer, other.ID, bindTo(other.ID))
	blob := s.storedKey(copied).Sealed
	for _, target := range []string{sibling, stranger} {
		_, err := s.store.GlobalDB().NewUpdate().Model((*APIKey)(nil)).Set("secret_sealed = ?", blob).Where("id = ?", target).Exec(s.ctx)
		s.Require().NoError(err)
	}
	lookup := s.store.APIKeys.Lookup(sealer)

	resolved, err := lookup(s.ctx, copied)
	s.Require().NoError(err)
	s.Equal(secret, resolved.Secret, "the key the secret was sealed for still opens it")
	for _, target := range []string{sibling, stranger} {
		_, err := lookup(s.ctx, target)
		s.Error(err, "a key of the same app and a key of another app both refuse the copied secret")
		s.Equal(blob, s.storedKey(target).Sealed, "a refused secret is not written back")
	}
}

func (s *StoreSuite) TestASecretDoesNotOpenOnceItsRowNamesAnotherApp() {
	sealer, err := auth.NewSealerWithKeyring(1, map[int]string{1: "store test key"})
	s.Require().NoError(err)
	app, other := s.app(), s.app()
	id, _ := s.sealedKey(sealer, app.ID, func(id string) []byte { return APIKeyAAD(app.ID, id) })
	_, err = s.store.GlobalDB().NewUpdate().Model((*APIKey)(nil)).Set("app_id = ?", other.ID).Where("id = ?", id).Exec(s.ctx)
	s.Require().NoError(err)

	_, err = s.store.APIKeys.Lookup(sealer)(s.ctx, id)
	s.Error(err)
}

func (s *StoreSuite) TestAKeySealedBeforeSecretsWereBoundStillOpensAndIsCounted() {
	before, err := auth.NewSealerWithKeyring(1, map[int]string{1: "first key"})
	s.Require().NoError(err)
	after, err := auth.NewSealerWithKeyring(2, map[int]string{1: "first key", 2: "second key"})
	s.Require().NoError(err)
	app := s.app()
	id, secret := s.sealedKey(before, app.ID, unbound)
	was := s.storedKey(id)
	opens := testutil.ToFloat64(unboundOpens)

	resolved, err := s.store.APIKeys.Lookup(after)(s.ctx, id)
	s.Require().NoError(err)
	s.Equal(secret, resolved.Secret)
	s.GreaterOrEqual(testutil.ToFloat64(unboundOpens)-opens, 1.0, "the unbound open is counted")
	stored := s.storedKey(id)
	s.Equal(was.Sealed, stored.Sealed, "the row is not sealed again: the release before this one could not open it")
	s.Equal(1, stored.KEKVersion)
	opened, err := before.OpenWithAADVersion(stored.Sealed, nil, stored.KEKVersion)
	s.Require().NoError(err, "the release before this one, which opens without an AAD, still opens the row")
	s.Equal(secret, opened)
}
