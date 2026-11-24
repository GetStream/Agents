//go:build integration

package store

import (
	"time"
)

func (s *StoreSuite) providerKey(customer, provider, region, sealed string) ProviderKey {
	return ProviderKey{CustomerID: customer, Provider: provider, Region: region, APIKeySealed: []byte(sealed), KEKVersion: 1, KeyHint: "…" + sealed}
}

func (s *StoreSuite) TestAProviderKeyReplacesTheOneForItsProviderAndRegion() {
	first := s.providerKey("app", "openai", "global", "one")
	was, replaced, err := s.store.ProviderKeys.Upsert(s.ctx, &first)
	s.Require().NoError(err)
	s.False(replaced)
	s.Equal(ProviderKey{}, was)

	second := s.providerKey("app", "openai", "global", "two")
	was, replaced, err = s.store.ProviderKeys.Upsert(s.ctx, &second)
	s.Require().NoError(err)
	s.True(replaced)
	s.Equal([]byte("one"), was.APIKeySealed, "the row it replaced")
	s.True(first.CreatedAt.Equal(second.CreatedAt), "it keeps when the provider was first given a key")

	keys, err := s.store.ProviderKeys.SelectAll(s.ctx, "app")
	s.Require().NoError(err)
	s.Require().Len(keys, 1)
	s.Equal([]byte("two"), keys[0].APIKeySealed)
}

func (s *StoreSuite) TestAProviderKeyChangedOnOneReplicaIsRoutedOnByTheOther() {
	replica := s.replica()
	key := s.providerKey("app", "openai", "global", "one")
	_, _, err := s.store.ProviderKeys.Upsert(s.ctx, &key)
	s.Require().NoError(err)
	keys, err := replica.ProviderKeys.SelectAll(s.ctx, "app")
	s.Require().NoError(err)
	s.Require().Len(keys, 1)

	_, err = s.store.ProviderKeys.Delete(s.ctx, "app", "openai", "global")
	s.Require().NoError(err)

	s.Eventually(func() bool {
		keys, err := replica.ProviderKeys.SelectAll(s.ctx, "app")
		return err == nil && len(keys) == 0
	}, 2*time.Second, 10*time.Millisecond, "the other replica still routes on a deleted key")
}

func (s *StoreSuite) TestAProviderKeyIsTheCustomersOwn() {
	for _, key := range []ProviderKey{
		s.providerKey("app", "openai", "global", "a"),
		s.providerKey("app", "openai", "eu", "b"),
		s.providerKey("app", "deepgram", "global", "c"),
		s.providerKey("other-app", "openai", "global", "d"),
	} {
		_, _, err := s.store.ProviderKeys.Upsert(s.ctx, &key)
		s.Require().NoError(err)
	}
	named := func(keys []ProviderKey) []string {
		var out []string
		for _, key := range keys {
			out = append(out, key.CustomerID+"/"+key.Provider+"/"+key.Region)
		}
		return out
	}

	all, err := s.store.ProviderKeys.SelectAll(s.ctx, "app")
	s.Require().NoError(err)
	s.Equal([]string{"app/deepgram/global", "app/openai/eu", "app/openai/global"}, named(all), "by provider, then region")
	page, err := s.store.ProviderKeys.Select(s.ctx, "app", ProviderKeyFilter{Limit: 1})
	s.Require().NoError(err)
	s.Equal([]string{"app/deepgram/global", "app/openai/eu"}, named(page), "one more than the limit")
	page, err = s.store.ProviderKeys.Select(s.ctx, "app", ProviderKeyFilter{Limit: 1, After: &ProviderKeyPosition{Provider: "openai", Region: "eu"}})
	s.Require().NoError(err)
	s.Equal([]string{"app/openai/global"}, named(page))
	byProvider, err := s.store.ProviderKeys.SelectByProvider(s.ctx, "openai")
	s.Require().NoError(err)
	s.Equal([]string{"app/openai/eu", "app/openai/global", "other-app/openai/global"}, named(byProvider))

	_, err = s.store.ProviderKeys.Delete(s.ctx, "other-app", "openai", "eu")
	s.ErrorIs(err, ErrNoProviderKey, "another customer's region")
	deleted, err := s.store.ProviderKeys.Delete(s.ctx, "other-app", "openai", "global")
	s.Require().NoError(err)
	s.Equal([]byte("d"), deleted.APIKeySealed)
	all, err = s.store.ProviderKeys.SelectAll(s.ctx, "app")
	s.Require().NoError(err)
	s.Len(all, 3, "deleting another customer's key leaves these")
}

func (s *StoreSuite) TestProviderKeyQueryBudgets() {
	key := s.providerKey("app", "openai", "global", "one")
	s.Equal(4, s.countQueries(func() {
		s.Equal(s.redisTrips(1), s.countRedis(func() {
			_, _, err := s.store.ProviderKeys.Upsert(s.ctx, &key)
			s.Require().NoError(err)
		}))
	}), "begin, the row it replaces, the write, commit")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ProviderKeys.Select(s.ctx, "app", ProviderKeyFilter{})
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ProviderKeys.SelectByProvider(s.ctx, "openai")
		s.Require().NoError(err)
	}))
	all := func() {
		_, err := s.store.ProviderKeys.SelectAll(s.ctx, "app")
		s.Require().NoError(err)
	}
	s.Equal(1, s.countQueries(all))
	if s.store.cache != nil {
		all()
		s.Equal(0, s.countQueries(func() { s.Equal(0, s.countRedis(all)) }), "then the local cache answers")
	}
	s.Equal(3, s.countQueries(func() {
		s.Equal(s.redisTrips(1), s.countRedis(func() {
			_, err := s.store.ProviderKeys.Delete(s.ctx, "app", "openai", "global")
			s.Require().NoError(err)
		}))
	}), "begin, the write and commit: a write of a cached row runs detached in a transaction of its own")
}
