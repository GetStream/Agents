//go:build integration

package store

// A chat thread is silenced by a live opt-out of a person recorded in it, on its channel or on
// all of them; not by one of another channel, another customer, a person not in it, or one
// revoked (AI-2015).
func (s *StoreSuite) TestAThreadMemberWithALiveOptOutIsFound() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-group", "chat-group"))
	s.Require().NoError(err)
	_, err = s.store.ChannelThreads.Link(s.ctx, thread("thread-other", "chat-other"))
	s.Require().NoError(err)
	s.Require().NoError(s.store.ChannelThreadMembers.Insert(s.ctx, "thread-group", "+12025550198"))
	s.Require().NoError(s.store.ChannelThreadMembers.Insert(s.ctx, "thread-group", "+12025550199"))
	s.Require().NoError(s.store.ChannelThreadMembers.Insert(s.ctx, "thread-group", "+12025550199"), "a member recorded again")
	optedOut := func(channelID string) bool {
		found, err := s.store.ChannelThreadMembers.OptedOut(s.ctx, "acme-app", channelID, "imessage")
		s.Require().NoError(err)
		return found
	}
	optOut := func(customerID, recipient, channel string) {
		s.Require().NoError(s.store.OptOuts.Insert(s.ctx, &OptOut{CustomerID: customerID, Recipient: recipient, Channel: channel, Source: "keyword"}))
	}
	s.False(optedOut("thread-group"), "nobody opted out")

	optOut("acme-app", "+12025550198", "sms")
	optOut("other-app", "+12025550198", "imessage")
	optOut("acme-app", "+12025550100", "imessage")
	s.False(optedOut("thread-group"), "another channel, another customer, a person not in the chat")

	optOut("acme-app", "+12025550198", OptOutAll)
	s.True(optedOut("thread-group"), "an opt-out of every channel")
	s.False(optedOut("thread-other"), "a chat the person is not in")
	s.Require().NoError(s.store.OptOuts.RevokeAll(s.ctx, "acme-app", "+12025550198", "imessage"))
	s.False(optedOut("thread-group"), "revoked")

	optOut("acme-app", "+12025550199", "imessage")
	s.True(optedOut("thread-group"))
	s.Error(s.store.ChannelThreadMembers.Insert(s.ctx, "thread-group", ""), "no member")
}

func (s *StoreSuite) TestChannelThreadMemberQueryBudgets() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-group", "chat-group"))
	s.Require().NoError(err)

	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ChannelThreadMembers.Insert(s.ctx, "thread-group", "+12025550199"))
	}), "Insert")
	s.Equal(1, s.countQueries(func() {
		optedOut, err := s.store.ChannelThreadMembers.OptedOut(s.ctx, "acme-app", "thread-group", "imessage")
		s.Require().NoError(err)
		s.False(optedOut)
	}), "OptedOut")
}
