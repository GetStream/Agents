//go:build integration

package store

import (
	"time"
)

func (s *StoreSuite) TestAStreamAppTokenIsKeptOnlyWhenItOutlastsTheOneHeld() {
	hour := s.base.Add(time.Hour)
	held := StreamAppToken{StreamAppPK: 41, CustomerID: "app", APIKey: "key", Sealed: []byte("first"), KEKVersion: 1, ExpiresAt: hour}
	kept, err := s.store.StreamAppTokens.Keep(s.ctx, &held)
	s.Require().NoError(err)
	s.True(kept)

	sooner := held
	sooner.Sealed, sooner.ExpiresAt = []byte("sooner"), s.base
	kept, err = s.store.StreamAppTokens.Keep(s.ctx, &sooner)
	s.Require().NoError(err)
	s.False(kept, "a token minted earlier, written later")
	read, err := s.store.StreamAppTokens.Get(s.ctx, 41)
	s.Require().NoError(err)
	s.Equal([]byte("first"), read.Sealed)

	later := held
	later.Sealed, later.ExpiresAt = []byte("later"), hour.Add(time.Hour)
	kept, err = s.store.StreamAppTokens.Keep(s.ctx, &later)
	s.Require().NoError(err)
	s.True(kept)
	read, err = s.store.StreamAppTokens.Get(s.ctx, 41)
	s.Require().NoError(err)
	s.Equal([]byte("later"), read.Sealed)

	_, err = s.store.StreamAppTokens.Get(s.ctx, 42)
	s.ErrorIs(err, ErrNoStreamAppToken)
}

func (s *StoreSuite) TestStreamAppTokenQueryBudgets() {
	token := StreamAppToken{StreamAppPK: 43, CustomerID: "app", APIKey: "key", Sealed: []byte("sealed"), KEKVersion: 1, ExpiresAt: s.base}
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.StreamAppTokens.Keep(s.ctx, &token)
		s.Require().NoError(err)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.StreamAppTokens.Get(s.ctx, 43)
		s.Require().NoError(err)
	}))
}
