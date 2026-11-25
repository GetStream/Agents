//go:build integration

package store

import (
	"errors"
	"sync"
)

// contact is a contact map row of the acme-app customer's agent for a phone number, which a
// test changes one part of.
func contact(address, conversationID string) *ContactMapEntry {
	return &ContactMapEntry{
		CustomerID:     "acme-app",
		ConfigID:       "agent-one",
		Kind:           ContactPhone,
		Address:        address,
		ConversationID: conversationID,
		StreamAppPK:    4242,
	}
}

func (s *StoreSuite) TestANewNumberMakesOneContactRow() {
	created, err := s.store.ContactMapEntries.Map(s.ctx, contact("+15550100", "agent:omni-one"))

	s.Require().NoError(err)
	s.True(created)
	s.Equal(1, s.contactRows())
}

// An SMS and a call from one number are one person: the second sighting finds the first
// one's omni-channel and pin, whatever it proposed.
func (s *StoreSuite) TestTheSameNumberAgainKeepsItsOmniChannel() {
	_, err := s.store.ContactMapEntries.Map(s.ctx, contact("+15550100", "agent:omni-one"))
	s.Require().NoError(err)
	again := contact("+15550100", "agent:omni-two")
	again.StreamAppPK = 7

	created, err := s.store.ContactMapEntries.Map(s.ctx, again)

	s.Require().NoError(err)
	s.False(created)
	s.Equal("agent:omni-one", again.ConversationID)
	s.Equal(int64(4242), again.StreamAppPK)
	s.Equal(1, s.contactRows())
}

// The same person writing to two agents is two conversations.
func (s *StoreSuite) TestTheSameNumberForAnotherAgentIsAnotherContact() {
	_, err := s.store.ContactMapEntries.Map(s.ctx, contact("+15550100", "agent:omni-one"))
	s.Require().NoError(err)
	other := contact("+15550100", "agent:omni-two")
	other.ConfigID = "agent-two"

	created, err := s.store.ContactMapEntries.Map(s.ctx, other)

	s.Require().NoError(err)
	s.True(created)
}

func (s *StoreSuite) TestTheSameNumberOfAnotherCustomerIsAnotherContact() {
	_, err := s.store.ContactMapEntries.Map(s.ctx, contact("+15550100", "agent:omni-one"))
	s.Require().NoError(err)
	other := contact("+15550100", "agent:omni-two")
	other.CustomerID = "globex-app"

	created, err := s.store.ContactMapEntries.Map(s.ctx, other)

	s.Require().NoError(err)
	s.True(created)
}

func (s *StoreSuite) TestAContactNeedsAnAddressAndAConversation() {
	var invalid *ValidationError
	_, err := s.store.ContactMapEntries.Map(s.ctx, contact("", "agent:omni-one"))
	s.Require().ErrorAs(err, &invalid)
	s.Equal("address", invalid.Field)

	_, err = s.store.ContactMapEntries.Map(s.ctx, contact("+15550100", ""))
	s.Require().ErrorAs(err, &invalid)
	s.Equal("conversation_id", invalid.Field)
	s.Zero(s.contactRows(), "an invalid contact is not written")
}

// Two routers seeing a new number at once, an SMS and a call say, make one row and hand both
// the same omni-channel: the unique index on the address decides which proposal wins.
func (s *StoreSuite) TestTwoFirstSightingsOfOneAddressAtOnceAreOneRow() {
	pools := s.pools(8)
	var wg sync.WaitGroup
	omniChannels := make([]string, len(pools))
	for i, pool := range pools {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry := contact("+15550100", "agent:omni-"+newID())
			_, err := pool.ContactMapEntries.Map(s.ctx, entry)
			s.NoError(err)
			omniChannels[i] = entry.ConversationID
		}()
	}
	wg.Wait()

	for _, omni := range omniChannels {
		s.Equal(omniChannels[0], omni)
	}
	s.Equal(1, s.contactRows())
}

// pools are count stores on the suite's database, each its own connection pool, as count
// routers would be. The suite's own store is the first.
func (s *StoreSuite) pools(count int) []*Store {
	pools := []*Store{s.store}
	for len(pools) < count {
		pool, err := s.openRouter()
		s.Require().NoError(err)
		s.T().Cleanup(func() { _ = pool.Close() })
		pools = append(pools, pool)
	}
	return pools
}

// contactRows is how many contact map rows there are.
func (s *StoreSuite) contactRows() int {
	var count int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT count(*) FROM contact_map").Scan(&count))
	return count
}

func (s *StoreSuite) TestAnIdentityIsFoundByItsUserAndKeepsItsThread() {
	customerID, configID, userID := newID(), newID(), "user-"+newID()
	identity := ContactMapEntry{
		CustomerID: customerID, ConfigID: configID, Kind: "imessage",
		Address: "+15550001111", Thread: "chat-1", UserID: userID,
	}
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &identity))

	held, err := s.store.ContactMapEntries.GetByUser(s.ctx, customerID, configID, "imessage", userID)
	s.Require().NoError(err)
	s.Equal("+15550001111", held.Address)
	s.Equal("chat-1", held.Thread)

	identity.Thread = "chat-2"
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &identity))
	moved, err := s.store.ContactMapEntries.GetByUser(s.ctx, customerID, configID, "imessage", userID)
	s.Require().NoError(err)
	s.Equal("chat-2", moved.Thread, "the chat a reply goes to moves with the person")

	_, err = s.store.ContactMapEntries.GetByUser(s.ctx, customerID, configID, "imessage", "somebody-else")
	s.ErrorIs(err, ErrNoContact)
	_, err = s.store.ContactMapEntries.GetByUser(s.ctx, newID(), configID, "imessage", userID)
	s.ErrorIs(err, ErrNoContact, "another app's identity is not this one's")
}

func (s *StoreSuite) TestAUserKnownAtTwoNumbersIsReachedAtTheOneSettledLast() {
	customerID, configID, userID := newID(), newID(), "user-"+newID()
	first := ContactMapEntry{CustomerID: customerID, ConfigID: configID, Kind: "sms", Address: "+15550001111", UserID: userID}
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &first))
	second := ContactMapEntry{CustomerID: customerID, ConfigID: configID, Kind: "sms", Address: "+15550002222", UserID: userID}
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &second))

	held, err := s.store.ContactMapEntries.GetByUser(s.ctx, customerID, configID, "sms", userID)
	s.Require().NoError(err)
	s.Equal("+15550002222", held.Address)

	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &first))
	held, err = s.store.ContactMapEntries.GetByUser(s.ctx, customerID, configID, "sms", userID)
	s.Require().NoError(err)
	s.Equal("+15550001111", held.Address, "the number they settled on again is the one reached")
}

func (s *StoreSuite) TestWhoANumberIsCanBeRecordedAndItsConversationMovedOn() {
	customerID, configID := newID(), newID()
	address := "+1347" + newID()[:7]
	identity := ContactMapEntry{
		CustomerID: customerID, ConfigID: configID, Kind: "whatsapp",
		Address: address, UserID: "phone:" + address,
	}
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &identity))

	identity.ConversationID = "agent:support-" + newID()
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &identity))

	held, err := s.store.ContactMapEntries.GetByAddress(s.ctx, customerID, configID, "whatsapp", address)
	s.Require().NoError(err)
	s.Equal(identity.ConversationID, held.ConversationID)
	s.Equal("phone:"+address, held.UserID)
}

// Two routers answering a number's first messages at once each open a conversation: the one
// recorded first is kept, and each is handed it.
func (s *StoreSuite) TestTwoConcurrentFirstMessagesFromOneNumberEndInOneConversation() {
	customerID, configID := newID(), newID()
	address := "+1347" + newID()[:7]
	kept := make([]string, 2)
	var wg sync.WaitGroup
	for i := range kept {
		wg.Go(func() {
			identity := ContactMapEntry{CustomerID: customerID, ConfigID: configID, Kind: "sms", Address: address, UserID: "phone:" + address}
			s.NoError(s.store.ContactMapEntries.Upsert(s.ctx, &identity))
			identity.ConversationID = "agent:omni-" + newID()
			s.NoError(s.store.ContactMapEntries.Upsert(s.ctx, &identity))
			kept[i] = identity.ConversationID
		})
	}
	wg.Wait()

	held, err := s.store.ContactMapEntries.GetByAddress(s.ctx, customerID, configID, "sms", address)
	s.Require().NoError(err)
	s.Equal(held.ConversationID, kept[0])
	s.Equal(held.ConversationID, kept[1])
}

// A router that saw no row for a number writes one with no conversation: one another router
// recorded meanwhile is not wiped, and is handed back.
func (s *StoreSuite) TestAFirstSightingOfANumberKeepsTheConversationRecordedMeanwhile() {
	customerID, configID := newID(), newID()
	address := "+1347" + newID()[:7]
	recorded := ContactMapEntry{CustomerID: customerID, ConfigID: configID, Kind: "sms", Address: address,
		UserID: "phone:" + address, ConversationID: "agent:omni-first"}
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &recorded))

	late := ContactMapEntry{CustomerID: customerID, ConfigID: configID, Kind: "sms", Address: address, UserID: "phone:" + address}
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &late))
	s.Equal("agent:omni-first", late.ConversationID)
	s.Equal(recorded.ID, late.ID)

	held, err := s.store.ContactMapEntries.GetByAddress(s.ctx, customerID, configID, "sms", address)
	s.Require().NoError(err)
	s.Equal("agent:omni-first", held.ConversationID)
}

// The same person writing to two agents is two conversations.
func (s *StoreSuite) TestANumberIsIdentifiedPerAgent() {
	customerID, address := newID(), "+1347"+newID()[:7]
	first := ContactMapEntry{
		CustomerID: customerID, ConfigID: newID(), Kind: "whatsapp",
		Address: address, UserID: "someone", ConversationID: "agent:one",
	}
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &first))
	second := first
	second.ID = ""
	second.ConfigID = newID()
	second.ConversationID = "agent:two"
	s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &second))

	held, err := s.store.ContactMapEntries.GetByAddress(s.ctx, customerID, first.ConfigID, "whatsapp", address)
	s.Require().NoError(err)
	s.Equal("agent:one", held.ConversationID)
}

func (s *StoreSuite) TestANumberNobodyHasClaimedBelongsToNobody() {
	_, err := s.store.ContactMapEntries.GetByAddress(s.ctx, newID(), newID(), "whatsapp", "+13470000000")

	s.True(errors.Is(err, ErrNoContact))
}

func (s *StoreSuite) TestContactMapEntryQueryBudgets() {
	entry := contact("+15550100", "agent:omni-one")
	s.Equal(2, s.countQueries(func() {
		created, err := s.store.ContactMapEntries.Map(s.ctx, entry)
		s.Require().NoError(err)
		s.True(created)
	}), "Map of a new address: insert, and look for its link")
	s.Equal(1, s.countQueries(func() {
		created, err := s.store.ContactMapEntries.Map(s.ctx, contact("+15550100", "agent:omni-two"))
		s.Require().NoError(err)
		s.False(created)
	}), "Map of a known address")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ContactMapEntries.GetByAddress(s.ctx, entry.CustomerID, entry.ConfigID, entry.Kind, entry.Address)
		s.Require().NoError(err)
	}), "GetByAddress")
	sender := ContactMapEntry{CustomerID: "acme-app", ConfigID: "agent-one", Kind: "sms", Address: "+15550199", UserID: "user-one"}
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ContactMapEntries.Upsert(s.ctx, &sender)) }), "Upsert")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ContactMapEntries.GetByUser(s.ctx, "acme-app", "agent-one", "sms", "user-one")
		s.Require().NoError(err)
	}), "GetByUser")
	_, err := s.store.Episodes.Open(s.ctx, s.episodeOf(entry.ID, "agent:thread-one"))
	s.Require().NoError(err)
	s.Equal(1, s.countQueries(func() {
		found, err := s.store.ContactMapEntries.GetByThread(s.ctx, "acme-app", "agent-one", "agent:thread-one")
		s.Require().NoError(err)
		s.Equal(entry.ID, found.ID)
	}), "GetByThread")
}
