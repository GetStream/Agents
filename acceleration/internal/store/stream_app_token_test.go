//go:build integration

package store

import (
	"context"
	"crypto/rand"
	mathrand "math/rand/v2"
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

// streamAppPK is an app id no other test uses, so the suite's runs do not share rows.
func streamAppPK() int64 {
	return 1_000_000 + mathrand.Int64N(1_000_000_000_000)
}

func (s *StoreSuite) TestAStreamAppTokenKeepsTheDataKeyVersionOfTheKeyThatSealedIt() {
	pk := streamAppPK()
	token := StreamAppToken{StreamAppPK: pk, CustomerID: "app", APIKey: "key", Sealed: []byte("data-key-blob"),
		DataKeyVersion: new(1), ExpiresAt: s.base}
	_, err := s.store.StreamAppTokens.Keep(s.ctx, &token)
	s.Require().NoError(err)
	read, err := s.store.StreamAppTokens.Get(s.ctx, pk)
	s.Require().NoError(err)
	s.Equal(new(1), read.DataKeyVersion)
	s.Zero(read.KEKVersion)

	later := token
	later.Sealed, later.KEKVersion, later.DataKeyVersion, later.ExpiresAt = []byte("router-key-blob"), 1, nil, s.base.Add(time.Hour)
	_, err = s.store.StreamAppTokens.Keep(s.ctx, &later)
	s.Require().NoError(err)
	read, err = s.store.StreamAppTokens.Get(s.ctx, pk)
	s.Require().NoError(err)
	s.Nil(read.DataKeyVersion, "a token kept under the router's key names no data key")
	s.Equal(1, read.KEKVersion)
}

// resealRows are three tokens under the router's key that hold the same blob: two of one
// customer's apps and one of another customer's.
func (s *StoreSuite) resealRows() (target, sibling, other StreamAppToken) {
	keep := func(customer string) StreamAppToken {
		token := StreamAppToken{StreamAppPK: streamAppPK(), CustomerID: customer, APIKey: "key", Sealed: []byte("same-blob"),
			KEKVersion: 1, ExpiresAt: s.base}
		_, err := s.store.StreamAppTokens.Keep(s.ctx, &token)
		s.Require().NoError(err)
		read, err := s.store.StreamAppTokens.Get(s.ctx, token.StreamAppPK)
		s.Require().NoError(err)
		return read
	}
	customer := "customer-" + rand.Text()
	return keep(customer), keep(customer), keep("customer-" + rand.Text())
}

func (s *StoreSuite) streamAppToken(pk int64) StreamAppToken {
	read, err := s.store.StreamAppTokens.Get(s.ctx, pk)
	s.Require().NoError(err)
	return read
}

func (s *StoreSuite) TestAResealedStreamAppTokenIsUnderTheDataKeyAndNoOtherRowChanges() {
	target, sibling, other := s.resealRows()

	resealed, err := s.store.StreamAppTokens.Reseal(s.ctx, target, []byte("resealed-blob"), 1)

	s.Require().NoError(err)
	s.True(resealed)
	read := s.streamAppToken(target.StreamAppPK)
	s.Equal([]byte("resealed-blob"), read.Sealed)
	s.Zero(read.KEKVersion)
	s.Equal(new(1), read.DataKeyVersion)
	s.Equal(sibling, s.streamAppToken(sibling.StreamAppPK), "another app of the same customer is left as it was")
	s.Equal(other, s.streamAppToken(other.StreamAppPK), "another customer's app is left as it was")
}

func (s *StoreSuite) TestAResealOfAnotherCustomersStreamAppTokenWritesNothing() {
	_, _, other := s.resealRows()
	opened := other
	opened.CustomerID = "customer-" + rand.Text()

	resealed, err := s.store.StreamAppTokens.Reseal(s.ctx, opened, []byte("resealed-blob"), 1)

	s.Require().NoError(err)
	s.False(resealed)
	s.Equal(other, s.streamAppToken(other.StreamAppPK))
}

func (s *StoreSuite) TestAResealLosesToAStreamAppTokenKeptAfterTheOldOneWasRead() {
	target, _, _ := s.resealRows()
	newer := target
	newer.Sealed, newer.ExpiresAt = []byte("newer-blob"), s.base.Add(time.Hour)
	_, err := s.store.StreamAppTokens.Keep(s.ctx, &newer)
	s.Require().NoError(err)

	resealed, err := s.store.StreamAppTokens.Reseal(s.ctx, target, []byte("resealed-blob"), 1)

	s.Require().NoError(err)
	s.False(resealed)
	read := s.streamAppToken(target.StreamAppPK)
	s.Equal([]byte("newer-blob"), read.Sealed)
	s.Equal(1, read.KEKVersion)
	s.Nil(read.DataKeyVersion)
}

func (s *StoreSuite) TestAResealThatCannotRunFailsAndLeavesTheStreamAppTokenAsItWas() {
	target, _, _ := s.resealRows()
	cancelled, cancel := context.WithCancel(s.ctx)
	cancel()

	resealed, err := s.store.StreamAppTokens.Reseal(cancelled, target, []byte("resealed-blob"), 1)

	s.ErrorIs(err, context.Canceled)
	s.False(resealed)
	s.Equal(target, s.streamAppToken(target.StreamAppPK))
}
