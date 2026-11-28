//go:build integration

package store

import "sync"

// line connects a WhatsApp line for a customer, with sealed credentials standing in for the
// real ones.
func (s *StoreSuite) line(customerID, e164 string) ChannelAccount {
	account := ChannelAccount{
		CustomerID: customerID, Kind: "whatsapp", E164: e164,
		AccountID: "phone-number-id", Token: newID(),
		SecretsSealed: []byte("sealed"), SecretsKEKVersion: 1,
	}
	s.Require().NoError(s.store.ChannelAccounts.Upsert(s.ctx, &account))
	return account
}

func (s *StoreSuite) TestAConnectedLineIsFoundByItsNumberAndByItsWebhookToken() {
	customerID := newID()
	account := s.line(customerID, "+1555"+newID()[:7])

	byNumber, err := s.store.ChannelAccounts.GetByNumber(s.ctx, customerID, "whatsapp", account.E164)
	s.Require().NoError(err)
	byToken, err := s.store.ChannelAccounts.GetByToken(s.ctx, account.Token)
	s.Require().NoError(err)

	s.Equal(account.ID, byNumber.ID)
	s.Equal(account.ID, byToken.ID)
}

// Rotating a token must not mean setting the webhook up with the provider all over again.
func (s *StoreSuite) TestReconnectingALineKeepsItsTokenAndReplacesItsCredentials() {
	customerID := newID()
	first := s.line(customerID, "+1555"+newID()[:7])

	again := ChannelAccount{
		CustomerID: customerID, Kind: "whatsapp", E164: first.E164,
		AccountID: "another-phone-number-id", Token: newID(),
		SecretsSealed: []byte("sealed again"), SecretsKEKVersion: 2,
	}
	s.Require().NoError(s.store.ChannelAccounts.Upsert(s.ctx, &again))

	held, err := s.store.ChannelAccounts.GetByNumber(s.ctx, customerID, "whatsapp", first.E164)
	s.Require().NoError(err)
	s.Equal(first.Token, held.Token)
	s.Equal(first.ID, held.ID)
	s.Equal("another-phone-number-id", held.AccountID)
	s.Equal([]byte("sealed again"), held.SecretsSealed)
	s.Equal(first.Token, again.Token, "the caller is told the token the provider was given")
	s.Equal(first.ID, again.ID)
}

func (s *StoreSuite) TestALineConnectedTwiceAtOnceIsOneLine() {
	customerID, e164 := newID(), "+1555"+newID()[:7]
	const connects = 8
	accounts := make([]ChannelAccount, connects)
	errs := make([]error, connects)
	var wg sync.WaitGroup
	for i := range connects {
		wg.Go(func() {
			accounts[i] = ChannelAccount{CustomerID: customerID, Kind: "whatsapp", E164: e164,
				AccountID: "phone-number-id", Token: newID(), SecretsSealed: []byte("sealed"), SecretsKEKVersion: 1}
			errs[i] = s.store.ChannelAccounts.Upsert(s.ctx, &accounts[i])
		})
	}
	wg.Wait()

	held, err := s.store.ChannelAccounts.GetByNumber(s.ctx, customerID, "whatsapp", e164)
	s.Require().NoError(err)
	for i := range connects {
		s.Require().NoError(errs[i])
		s.Equal(held.ID, accounts[i].ID)
		s.Equal(held.Token, accounts[i].Token)
	}
}

// A line nobody answers on has no use for the credentials.
func (s *StoreSuite) TestDisconnectingALineDropsItsCredentials() {
	customerID := newID()
	account := s.line(customerID, "+1555"+newID()[:7])

	s.Require().NoError(s.store.ChannelAccounts.Delete(s.ctx, customerID, "whatsapp", account.E164))

	_, err := s.store.ChannelAccounts.GetByToken(s.ctx, account.Token)
	s.ErrorIs(err, ErrUnknownChannelAccount)
	s.ErrorIs(s.store.ChannelAccounts.Delete(s.ctx, customerID, "whatsapp", account.E164),
		ErrUnknownChannelAccount)
}

func (s *StoreSuite) TestAnotherAppsLineIsNotThisOnes() {
	e164 := "+1555" + newID()[:7]
	s.line(newID(), e164)

	_, err := s.store.ChannelAccounts.GetByNumber(s.ctx, newID(), "whatsapp", e164)

	s.ErrorIs(err, ErrUnknownChannelAccount)
}

// The lines are kept per channel, so a number that texts and carries WhatsApp is two.
func (s *StoreSuite) TestTheSameNumberOnAnotherChannelIsAnotherLine() {
	customerID := newID()
	e164 := "+1555" + newID()[:7]
	whatsapp := s.line(customerID, e164)
	texting := ChannelAccount{
		CustomerID: customerID, Kind: "sms", E164: e164, Token: newID(),
		SecretsSealed: []byte("sealed"), SecretsKEKVersion: 1,
	}
	s.Require().NoError(s.store.ChannelAccounts.Upsert(s.ctx, &texting))

	s.NotEqual(whatsapp.ID, texting.ID)
	s.NotEqual(whatsapp.Token, texting.Token)
}

// Catches a reseal that leaves kek_version or the data key version behind, and a reconnect that
// keeps a data key version its new credentials were not sealed under.
func (s *StoreSuite) TestAResealedLineIsWhatTheNextReadGetsAndAReconnectClearsItsDataKeyVersion() {
	customerID := newID()
	account := s.line(customerID, "+1555"+newID()[:7])
	sibling := s.line(customerID, "+1555"+newID()[:7])

	resealed, err := s.store.ChannelAccounts.Reseal(s.ctx, account, []byte("under the data key"), 1)
	s.Require().NoError(err)
	s.True(resealed)
	untouched, err := s.store.ChannelAccounts.GetByToken(s.ctx, sibling.Token)
	s.Require().NoError(err)
	s.Equal([]byte("sealed"), untouched.SecretsSealed, "another line of the app with the same blob stays")
	held, err := s.store.ChannelAccounts.GetByToken(s.ctx, account.Token)
	s.Require().NoError(err)
	s.Equal([]byte("under the data key"), held.SecretsSealed)
	s.Zero(held.SecretsKEKVersion)
	s.Equal(new(1), held.SecretsDataKeyVersion)

	again := s.line(customerID, account.E164)
	held, err = s.store.ChannelAccounts.GetByToken(s.ctx, again.Token)
	s.Require().NoError(err)
	s.Nil(held.SecretsDataKeyVersion)
	s.Equal(1, held.SecretsKEKVersion)
}

// A reconnect between the read and the reseal is newer than what was resealed, so it stays.
func (s *StoreSuite) TestAResealLosesToALineReconnectedAfterTheOldCredentialsWereRead() {
	customerID := newID()
	read := s.line(customerID, "+1555"+newID()[:7])
	reconnected := ChannelAccount{CustomerID: customerID, Kind: "whatsapp", E164: read.E164, Token: newID(),
		SecretsSealed: []byte("reconnected"), SecretsKEKVersion: 1}
	s.Require().NoError(s.store.ChannelAccounts.Upsert(s.ctx, &reconnected))

	resealed, err := s.store.ChannelAccounts.Reseal(s.ctx, read, []byte("under the data key"), 1)

	s.Require().NoError(err)
	s.False(resealed)
	held, err := s.store.ChannelAccounts.GetByToken(s.ctx, read.Token)
	s.Require().NoError(err)
	s.Equal([]byte("reconnected"), held.SecretsSealed)
}

func (s *StoreSuite) TestAResealOfALineOfAnotherCustomerWritesNothing() {
	account := s.line(newID(), "+1555"+newID()[:7])
	elsewhere := account
	elsewhere.CustomerID = newID()

	resealed, err := s.store.ChannelAccounts.Reseal(s.ctx, elsewhere, []byte("under the data key"), 1)

	s.Require().NoError(err)
	s.False(resealed)
	held, err := s.store.ChannelAccounts.GetByToken(s.ctx, account.Token)
	s.Require().NoError(err)
	s.Equal([]byte("sealed"), held.SecretsSealed)
}

func (s *StoreSuite) TestChannelAccountQueryBudgets() {
	customerID, e164 := newID(), "+1555"+newID()[:7]
	var account ChannelAccount
	s.Equal(1, s.countQueries(func() { account = s.line(customerID, e164) }), "Upsert of a new line")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ChannelAccounts.Upsert(s.ctx, &account)) }),
		"Upsert of a connected line")
	s.Equal(1, s.countQueries(func() {
		lines, err := s.store.ChannelAccounts.Select(s.ctx, customerID)
		s.Require().NoError(err)
		s.Len(lines, 1)
	}), "Select")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ChannelAccounts.GetByNumber(s.ctx, customerID, "whatsapp", e164)
		s.Require().NoError(err)
	}), "GetByNumber")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ChannelAccounts.GetByToken(s.ctx, account.Token)
		s.Require().NoError(err)
	}), "GetByToken")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ChannelAccounts.Delete(s.ctx, customerID, "whatsapp", e164)) }), "Delete")
}
