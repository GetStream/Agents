//go:build integration

package store

import (
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
)

// waiting is a reply in a Slack-shaped thread of the acme connector, which a test keeps waiting.
func waiting(threadKey, messageID string) core.InboundMessage {
	return core.InboundMessage{
		ConnectorID: "acme", ProviderUnitID: "T0000TEAM", ThreadKey: threadKey, AuthorID: "U0000BOB",
		Text: "and the deploy? " + messageID, ProviderMessageID: messageID, Raw: []byte(`{"event":{"ts":"` + messageID + `"}}`),
	}
}

func (s *StoreSuite) TestTheWaitingRepliesOfAThreadAreTakenOnceOldestFirst() {
	for _, id := range []string{"1759740000.000300", "1759740000.000200"} {
		kept, err := s.store.WaitingMessages.Wait(s.ctx, "acme-app", waiting("C0000CHAN:1", id))
		s.Require().NoError(err)
		s.Require().True(kept)
	}
	_, err := s.store.WaitingMessages.Wait(s.ctx, "acme-app", waiting("C0000CHAN:2", "1759740000.000400"))
	s.Require().NoError(err)
	_, err = s.store.WaitingMessages.Wait(s.ctx, "other-app", waiting("C0000CHAN:1", "1759740000.000500"))
	s.Require().NoError(err)

	taken, err := takeWaitingMessages(s.ctx, s.store.global, "acme-app", "acme", "T0000TEAM", "C0000CHAN:1")

	s.Require().NoError(err)
	s.Equal([]core.InboundMessage{waiting("C0000CHAN:1", "1759740000.000300"), waiting("C0000CHAN:1", "1759740000.000200")}, taken,
		"the thread's own replies, in the order they waited, as they were kept")
	again, err := takeWaitingMessages(s.ctx, s.store.global, "acme-app", "acme", "T0000TEAM", "C0000CHAN:1")
	s.Require().NoError(err)
	s.Empty(again)
}

func (s *StoreSuite) TestAReplyDeliveredAgainWaitsOnce() {
	kept, err := s.store.WaitingMessages.Wait(s.ctx, "acme-app", waiting("C0000CHAN:1", "1759740000.000200"))
	s.Require().NoError(err)
	s.Require().True(kept)

	kept, err = s.store.WaitingMessages.Wait(s.ctx, "acme-app", waiting("C0000CHAN:1", "1759740000.000200"))

	s.Require().NoError(err)
	s.False(kept)
	taken, err := takeWaitingMessages(s.ctx, s.store.global, "acme-app", "acme", "T0000TEAM", "C0000CHAN:1")
	s.Require().NoError(err)
	s.Len(taken, 1)
}

func (s *StoreSuite) TestAWaitingReplyTakenBackIsNotTakenByItsThread() {
	reply := waiting("C0000CHAN:1", "1759740000.000200")
	_, err := s.store.WaitingMessages.Wait(s.ctx, "acme-app", reply)
	s.Require().NoError(err)

	back, err := s.store.WaitingMessages.Unwait(s.ctx, "acme-app", reply)

	s.Require().NoError(err)
	s.True(back)
	again, err := s.store.WaitingMessages.Unwait(s.ctx, "acme-app", reply)
	s.Require().NoError(err)
	s.False(again, "taken back once")
	taken, err := takeWaitingMessages(s.ctx, s.store.global, "acme-app", "acme", "T0000TEAM", "C0000CHAN:1")
	s.Require().NoError(err)
	s.Empty(taken)
}

// A reply whose thread nobody linked within channelWaitingKeep is not taken, and the next
// reply kept drops it.
func (s *StoreSuite) TestAReplyPastItsKeepIsNotTakenAndIsDropped() {
	_, err := s.store.WaitingMessages.Wait(s.ctx, "acme-app", waiting("C0000CHAN:1", "1759740000.000200"))
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "UPDATE channel_thread_waiting SET created_at = ? WHERE customer_id = 'acme-app'",
		time.Now().UTC().Add(-channelWaitingKeep-time.Minute))
	s.Require().NoError(err)

	taken, err := takeWaitingMessages(s.ctx, s.store.global, "acme-app", "acme", "T0000TEAM", "C0000CHAN:1")
	s.Require().NoError(err)
	s.Empty(taken)
	_, err = s.store.WaitingMessages.Wait(s.ctx, "acme-app", waiting("C0000CHAN:2", "1759740000.000300"))
	s.Require().NoError(err)

	var rows int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT count(*) FROM channel_thread_waiting WHERE customer_id = 'acme-app'").Scan(&rows))
	s.Equal(1, rows, "only the reply kept now")
}

func (s *StoreSuite) TestWaitingMessageQueryBudgets() {
	reply := waiting("C0000CHAN:1", "1759740000.000200")

	s.Equal(2, s.countQueries(func() {
		kept, err := s.store.WaitingMessages.Wait(s.ctx, "acme-app", reply)
		s.Require().NoError(err)
		s.True(kept)
	}), "Wait: drop the messages past their keep, keep it")
	s.Equal(1, s.countQueries(func() {
		taken, err := s.store.WaitingMessages.Unwait(s.ctx, "acme-app", reply)
		s.Require().NoError(err)
		s.True(taken)
	}), "Unwait")
}
