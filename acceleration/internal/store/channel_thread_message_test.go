//go:build integration

package store

import "time"

func (s *StoreSuite) TestARetriedMessageIsClaimedOnce() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
	s.Require().NoError(err)

	first, err := s.store.ChannelThreadMessages.Claim(s.ctx, "thread-one", ClaimInbound, "1759740000.000200")
	s.Require().NoError(err)
	again, err := s.store.ChannelThreadMessages.Claim(s.ctx, "thread-one", ClaimInbound, "1759740000.000200")
	s.Require().NoError(err)
	next, err := s.store.ChannelThreadMessages.Claim(s.ctx, "thread-one", ClaimInbound, "1759740000.000300")
	s.Require().NoError(err)

	s.True(first)
	s.False(again)
	s.True(next)
}

// A Stream Chat id and a provider's id can be the same string; each step claims its own.
func (s *StoreSuite) TestEachStepClaimsAMessageOnce() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
	s.Require().NoError(err)

	for _, kind := range []string{ClaimInbound, ClaimTurn, ClaimReply} {
		first, err := s.store.ChannelThreadMessages.Claim(s.ctx, "thread-one", kind, "same-id")
		s.Require().NoError(err)
		again, err := s.store.ChannelThreadMessages.Claim(s.ctx, "thread-one", kind, "same-id")
		s.Require().NoError(err)
		s.True(first, kind)
		s.False(again, kind)
	}
}

func (s *StoreSuite) TestAReleasedClaimCanBeTakenAgain() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
	s.Require().NoError(err)
	_, err = s.store.ChannelThreadMessages.Claim(s.ctx, "thread-one", ClaimReply, "reply-1")
	s.Require().NoError(err)

	s.Require().NoError(s.store.ChannelThreadMessages.Release(s.ctx, "thread-one", ClaimReply, "reply-1"))

	again, err := s.store.ChannelThreadMessages.Claim(s.ctx, "thread-one", ClaimReply, "reply-1")
	s.Require().NoError(err)
	s.True(again)
}

func (s *StoreSuite) TestAMessageIdOlderThanTheKeepIsForgotten() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
	s.Require().NoError(err)
	_, err = s.store.ChannelThreadMessages.Claim(s.ctx, "thread-one", ClaimInbound, "old")
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "UPDATE channel_thread_messages SET created_at = ?", time.Now().Add(-channelMessageKeep-time.Minute))
	s.Require().NoError(err)

	_, err = s.store.ChannelThreadMessages.Claim(s.ctx, "thread-one", ClaimInbound, "new")
	s.Require().NoError(err)

	var kept int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT count(*) FROM channel_thread_messages").Scan(&kept))
	s.Equal(1, kept)
}

func (s *StoreSuite) TestChannelThreadMessageQueryBudgets() {
	_, err := s.store.ChannelThreads.Link(s.ctx, thread("thread-one", "C0000CHAN:1"))
	s.Require().NoError(err)

	s.Equal(2, s.countQueries(func() {
		claimed, err := s.store.ChannelThreadMessages.Claim(s.ctx, "thread-one", ClaimReply, "msg-one")
		s.Require().NoError(err)
		s.True(claimed)
	}), "Claim: drop the channel's old claims, claim")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ChannelThreadMessages.Release(s.ctx, "thread-one", ClaimReply, "msg-one"))
	}), "Release")
}
