//go:build integration

package store

import (
	"sync"
)

// thread is a link for a Slack-shaped thread of the acme-app customer, which a test changes
// one part of.
func thread(channelID, threadKey string) *ChannelThread {
	return &ChannelThread{
		ChannelID:      channelID,
		CustomerID:     "acme-app",
		ConnectorID:    "acme",
		ProviderUnitID: "T0000TEAM",
		ThreadKey:      threadKey,
		ConnectionID:   "connection-one",
		ThreadParts:    map[string]string{"channel": "C0000CHAN", "thread_ts": "1759740000.000100"},
		StreamAppPK:    4242,
	}
}

func (s *StoreSuite) TestTheFirstMessageOfAThreadLinksItsChannel() {
	linked := thread("thread-one", "C0000CHAN:1759740000.000100")

	created, err := s.store.ChannelThreads.Link(s.ctx, linked)

	s.Require().NoError(err)
	s.True(created)
	found, err := s.store.ChannelThreads.Get(s.ctx, "thread-one")
	s.Require().NoError(err)
	s.Equal("C0000CHAN:1759740000.000100", found.ThreadKey)
	s.Equal(map[string]string{"channel": "C0000CHAN", "thread_ts": "1759740000.000100"}, found.ThreadParts)
	s.Equal(int64(4242), found.StreamAppPK)
	s.Equal("connection-one", found.ConnectionID)
}

func (s *StoreSuite) TestASecondMessageOfAThreadKeepsItsChannelAndTakesTheLatestConnection() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
	s.Require().NoError(err)
	again := thread("thread-two", "C0000CHAN:1")
	again.ConnectionID = "connection-two"

	created, err := s.store.ChannelThreads.Link(s.ctx, again)

	s.Require().NoError(err)
	s.False(created)
	s.Equal("thread-one", again.ChannelID, "the thread keeps the channel it was first written to")
	found, err := s.store.ChannelThreads.Get(s.ctx, "thread-one")
	s.Require().NoError(err)
	s.Equal("connection-two", found.ConnectionID)
	_, err = s.store.ChannelThreads.Get(s.ctx, "thread-two")
	s.ErrorIs(err, ErrNoChannelThread)
}

func (s *StoreSuite) TestAnotherThreadGetsAnotherChannel() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
	s.Require().NoError(err)

	created, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-two", "C0000CHAN:2"))

	s.Require().NoError(err)
	s.True(created)
}

func (s *StoreSuite) TestTheFirstStarterOfAThreadIsTheOneKept() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
	s.Require().NoError(err)
	starter, err := s.store.ChannelThreads.Starter(s.ctx, "thread-one")
	s.Require().NoError(err)
	s.Empty(starter, "nobody started it yet")

	s.Require().NoError(s.store.ChannelThreads.SetStarter(s.ctx, "thread-one", "+15550100"))
	s.Require().NoError(s.store.ChannelThreads.SetStarter(s.ctx, "thread-one", "+15550199"))

	starter, err = s.store.ChannelThreads.Starter(s.ctx, "thread-one")
	s.Require().NoError(err)
	s.Equal("+15550100", starter)
}

// One Slack workspace can install two customers' apps, so the same thread is two threads.
func (s *StoreSuite) TestTheSameThreadOfAnotherCustomerIsAThreadOfItsOwn() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
	s.Require().NoError(err)
	other := thread("thread-two", "C0000CHAN:1")
	other.CustomerID = "other-app"

	created, err := s.store.ChannelThreads.Link(s.ctx, other)

	s.Require().NoError(err)
	s.True(created)
	s.Equal("thread-two", other.ChannelID)
}

func (s *StoreSuite) TestTwoFirstMessagesOfOneThreadAtOnceLinkOneChannel() {
	var wg sync.WaitGroup
	channels := make([]string, 8)
	for i := range channels {
		wg.Add(1)
		go func() {
			defer wg.Done()
			linked := thread("thread-"+newID(), "C0000CHAN:1")
			_, err := s.store.ChannelThreads.Link(s.ctx, linked)
			s.NoError(err)
			channels[i] = linked.ChannelID
		}()
	}
	wg.Wait()

	for _, channel := range channels {
		s.Equal(channels[0], channel)
	}
}

func (s *StoreSuite) TestAChannelNoThreadIsLinkedToIsNoThread() {
	_, err := s.store.ChannelThreads.Get(s.ctx, "plain-conversation")

	s.ErrorIs(err, ErrNoChannelThread)
}

func (s *StoreSuite) TestOnlyTheThreadThatWasLinkedIsLinked() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
	s.Require().NoError(err)

	for name, ask := range map[string][4]string{
		"the thread":            {"acme-app", "acme", "T0000TEAM", "C0000CHAN:1"},
		"another key":           {"acme-app", "acme", "T0000TEAM", "C0000CHAN:2"},
		"another customer":      {"other-app", "acme", "T0000TEAM", "C0000CHAN:1"},
		"another connector":     {"acme-app", "other", "T0000TEAM", "C0000CHAN:1"},
		"another provider unit": {"acme-app", "acme", "T0000OTHER", "C0000CHAN:1"},
	} {
		linked, err := s.store.ChannelThreads.Linked(s.ctx, ask[0], ask[1], ask[2], ask[3])
		s.Require().NoError(err, name)
		s.Equal(name == "the thread", linked, name)
	}
}

// A chat thread's person is read with the thread, replaced by the next one recorded, and
// forgotten by an empty one.
func (s *StoreSuite) TestAChatThreadsPersonIsReadWithItUntilItIsForgotten() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "chat-1"))
	s.Require().NoError(err)
	s.Empty(s.chatPerson("thread-one"), "a thread nobody recorded a person for")

	s.Require().NoError(s.store.ChannelThreads.SetChatPerson(s.ctx, "thread-one", "+12025550199"))
	s.Equal("+12025550199", s.chatPerson("thread-one"))
	s.Require().NoError(s.store.ChannelThreads.SetChatPerson(s.ctx, "thread-one", "+12025550188"))
	s.Equal("+12025550188", s.chatPerson("thread-one"))

	s.Require().NoError(s.store.ChannelThreads.SetChatPerson(s.ctx, "thread-one", ""))
	s.Empty(s.chatPerson("thread-one"))
}

// A thread linked again keeps its person: only the bridge, for a fresh message, says who it is.
func (s *StoreSuite) TestAThreadLinkedAgainKeepsItsChatPerson() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "chat-1"))
	s.Require().NoError(err)
	s.Require().NoError(s.store.ChannelThreads.SetChatPerson(s.ctx, "thread-one", "+12025550199"))

	_, err = s.store.ChannelThreads.Link(s.ctx, thread("thread-two", "chat-1"))

	s.Require().NoError(err)
	s.Equal("+12025550199", s.chatPerson("thread-one"))
}

// chatPerson is the person store.ChannelThread reads for a thread channel.
func (s *StoreSuite) chatPerson(channelID string) string {
	found, err := s.store.ChannelThreads.Get(s.ctx, channelID)
	s.Require().NoError(err)
	return found.ChatPerson
}

func (s *StoreSuite) TestChannelThreadQueryBudgets() {
	s.Equal(1, s.countQueries(func() {
		created, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
		s.Require().NoError(err)
		s.True(created)
	}), "Link")
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.ChannelThreads.Get(s.ctx, "thread-one")
		s.Require().NoError(err)
	}), "Get")
	s.Equal(1, s.countQueries(func() {
		linked, err := s.store.ChannelThreads.Linked(s.ctx, "acme-app", "acme", "T0000TEAM", "C0000CHAN:1")
		s.Require().NoError(err)
		s.True(linked)
	}), "Linked")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ChannelThreads.SetChatPerson(s.ctx, "thread-one", "+12025550199")) }), "SetChatPerson")
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ChannelThreads.SetStarter(s.ctx, "thread-one", "U0000ADA")) }), "SetStarter")
	s.Equal(1, s.countQueries(func() {
		starter, err := s.store.ChannelThreads.Starter(s.ctx, "thread-one")
		s.Require().NoError(err)
		s.Equal("U0000ADA", starter)
	}), "Starter")
}
