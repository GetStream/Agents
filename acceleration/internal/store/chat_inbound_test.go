//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"
)

// chatLease is the lease the tests claim under: longer than an answer may take.
const chatLease = 3 * time.Minute

// queuedChat queues a message of the app 1257545, due at s.base; change sets the fields a test
// cares about.
func (s *StoreSuite) queuedChat(messageID string, change ...func(*ChatInbound)) ChatInbound {
	delivery := s.chatInbound(messageID, change...)
	queued, err := s.store.ChatInbounds.Queue(s.ctx, delivery)
	s.Require().NoError(err)
	s.Require().True(queued)
	return delivery
}

// chatInbound is the message queuedChat queues, not yet queued.
func (s *StoreSuite) chatInbound(messageID string, change ...func(*ChatInbound)) ChatInbound {
	delivery := ChatInbound{
		StreamAppPK: 1257545, MessageID: messageID, CustomerID: "acme-app", APIKey: "key-one",
		Body: []byte(`{"type":"message.new","message":{"id":"` + messageID + `"}}`), NextAttemptAt: s.base,
	}
	for _, apply := range change {
		apply(&delivery)
	}
	return delivery
}

// claimChats claims every message due, under chatLease.
func (s *StoreSuite) claimChats() []ChatInbound {
	claimed, err := s.store.ChatInbounds.Claim(s.ctx, 10, chatLease)
	s.Require().NoError(err)
	return claimed
}

// claimedChat claims and requires the one message it takes to be messageID.
func (s *StoreSuite) claimedChat(messageID string) ChatInbound {
	claimed := s.claimChats()
	s.Require().Len(claimed, 1)
	s.Require().Equal(messageID, claimed[0].MessageID)
	return claimed[0]
}

// chatRow is the state, attempts and finish time of a message's row in the app 1257545.
func (s *StoreSuite) chatRow(messageID string) (state string, attempts int, finishedAt *time.Time) {
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT state, attempts, finished_at FROM chat_inbound WHERE stream_app_pk = 1257545 AND message_id = ?",
		messageID).Scan(&state, &attempts, &finishedAt))
	return state, attempts, finishedAt
}

// chatRows is how many rows the table holds.
func (s *StoreSuite) chatRows() int {
	var rows int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT count(*) FROM chat_inbound").Scan(&rows))
	return rows
}

func (s *StoreSuite) TestAClaimedChatInboundHasEverythingItWasQueuedWith() {
	s.queuedChat("msg-one", func(delivery *ChatInbound) { delivery.Deployment = true })

	claimed := s.claimedChat("msg-one")

	s.NotZero(claimed.Seq)
	s.Equal(int64(1257545), claimed.StreamAppPK)
	s.Equal("acme-app", claimed.CustomerID)
	s.True(claimed.Deployment)
	s.Equal("key-one", claimed.APIKey)
	s.Equal(`{"type":"message.new","message":{"id":"msg-one"}}`, string(claimed.Body))
	s.Equal(ChatInboundQueued, claimed.State)
	s.Equal(1, claimed.Attempts, "a claim counts its attempt")
	s.WithinDuration(time.Now().Add(chatLease), claimed.NextAttemptAt, 5*time.Second, "a claimed message is due at the end of the lease its claim gave")
	s.WithinDuration(time.Now(), claimed.ReceivedAt, time.Minute)
	s.Nil(claimed.FinishedAt)
}

// What a message's voice recording said is kept once, for the next try: a claim and a read by
// seq carry it, and a keep after the first, from a try that heard it again, leaves the first.
func (s *StoreSuite) TestWhatAQueuedChatRecordingSaidIsKeptOnceForTheNextTry() {
	s.queuedChat("msg-one")
	s.Nil(s.claimedChat("msg-one").Heard, "nothing is heard before a try hears it")

	s.Require().NoError(s.store.ChatInbounds.KeepHeard(s.ctx, 1257545, "msg-one", "where is my order"))
	s.Require().NoError(s.store.ChatInbounds.KeepHeard(s.ctx, 1257545, "msg-one", "where is my order now"))

	s.timePasses("chat_inbound", chatLease)
	claimed := s.claimedChat("msg-one")
	s.Require().NotNil(claimed.Heard)
	s.Equal("where is my order", *claimed.Heard)
	read, err := s.store.ChatInbounds.Get(s.ctx, claimed.Seq)
	s.Require().NoError(err)
	s.Require().NotNil(read.Heard)
	s.Equal("where is my order", *read.Heard)
}

func (s *StoreSuite) TestAChatInboundQueuedTwiceIsOneRow() {
	s.queuedChat("msg-one")

	queued, err := s.store.ChatInbounds.Queue(s.ctx, s.chatInbound("msg-one", func(delivery *ChatInbound) {
		delivery.Body = []byte(`{"again":true}`)
	}))

	s.Require().NoError(err)
	s.False(queued)
	s.Equal(1, s.chatRows())
	s.Equal(`{"type":"message.new","message":{"id":"msg-one"}}`, string(s.claimedChat("msg-one").Body), "the first delivery's row stays")
}

// Stream's message ids are unique within an app, so another app's message of the same id is a
// message of its own.
func (s *StoreSuite) TestAChatMessageOfTheSameIDInAnotherAppIsQueuedOnItsOwn() {
	s.queuedChat("msg-one")

	queued, err := s.store.ChatInbounds.Queue(s.ctx, s.chatInbound("msg-one", func(delivery *ChatInbound) { delivery.StreamAppPK = 1728173 }))

	s.Require().NoError(err)
	s.True(queued)
	s.Len(s.claimChats(), 2)
}

func (s *StoreSuite) TestAnAnsweredChatMessageIsNotQueuedAgain() {
	s.queuedChat("msg-one")
	_, err := s.store.ChatInbounds.Finish(s.ctx, s.claimedChat("msg-one"), s.base)
	s.Require().NoError(err)

	queued, err := s.store.ChatInbounds.Queue(s.ctx, s.chatInbound("msg-one"))

	s.Require().NoError(err)
	s.False(queued)
	state, _, _ := s.chatRow("msg-one")
	s.Equal(ChatInboundDone, state)
}

// queueChatInbound takes the caller's transaction: what it queues there is queued only when
// the transaction commits, and a second queue in it is still one row.
func (s *StoreSuite) TestAChatInboundQueuedInACallersTransactionIsQueuedOnlyWithIt() {
	rolledBack := errors.New("the caller's next step failed")
	err := s.store.GlobalDB().RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		queued, err := queueChatInbound(ctx, tx, s.chatInbound("msg-one"))
		s.Require().NoError(err)
		s.Require().True(queued)
		return rolledBack
	})
	s.Require().ErrorIs(err, rolledBack)
	s.Zero(s.chatRows(), "a rolled back transaction queues nothing")

	err = s.store.GlobalDB().RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		first, err := queueChatInbound(ctx, tx, s.chatInbound("msg-one"))
		s.Require().NoError(err)
		second, err := queueChatInbound(ctx, tx, s.chatInbound("msg-one"))
		s.Require().NoError(err)
		s.True(first)
		s.False(second)
		return nil
	})

	s.Require().NoError(err)
	s.Equal(1, s.chatRows())
}

// deliveryKey is a key no other test recorded: hook deliveries outlive a test.
func (s *StoreSuite) deliveryKey() string {
	return fmt.Sprintf("4242:message:msg-%d", time.Now().UnixNano())
}

// The message webhook recorded the message first: it answers it, and the delivery queues nothing.
func (s *StoreSuite) TestAChatInboundIsNotQueuedUnderAKeyRecordedBefore() {
	key := s.deliveryKey()
	first, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeHook, key)
	s.Require().NoError(err)
	s.Require().True(first)

	queued, err := s.store.ChatInbounds.QueueFirst(s.ctx, key, s.chatInbound("msg-one"))

	s.Require().NoError(err)
	s.False(queued)
	s.Zero(s.chatRows())
}

// A delivery queued first records its key, so the message webhook that comes after it, or the
// delivery made again, acts on nothing.
func (s *StoreSuite) TestAChatInboundQueuedFirstRecordsItsKey() {
	key := s.deliveryKey()

	queued, err := s.store.ChatInbounds.QueueFirst(s.ctx, key, s.chatInbound("msg-one"))
	s.Require().NoError(err)
	s.Require().True(queued)

	first, err := s.store.DeliveriesSeen.First(s.ctx, DeliveryScopeHook, key)
	s.Require().NoError(err)
	s.False(first)
	again, err := s.store.ChatInbounds.QueueFirst(s.ctx, key, s.chatInbound("msg-one"))
	s.Require().NoError(err)
	s.False(again)
	s.Equal(1, s.chatRows())
}

// The key and the row are one transaction: a delivery that could not be queued records no key,
// so when Stream Chat makes it again, it is queued then.
func (s *StoreSuite) TestAChatInboundThatCouldNotBeQueuedRecordsNoKey() {
	key := s.deliveryKey()

	_, err := s.store.ChatInbounds.QueueFirst(s.ctx, key, s.chatInbound("msg-one", func(delivery *ChatInbound) { delivery.StreamAppPK = 0 }))
	s.Require().Error(err)

	queued, err := s.store.ChatInbounds.QueueFirst(s.ctx, key, s.chatInbound("msg-one"))
	s.Require().NoError(err)
	s.True(queued)
}

// Once the message webhook is off, the endpoint is what forgets the keys older than a day.
func (s *StoreSuite) TestQueueingAChatInboundForgetsTheKeysOfADayAgo() {
	old := s.deliveryKey()
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "INSERT INTO deliveries_seen (scope, key, seen_at) VALUES (?, ?, ?)", DeliveryScopeHook, old, time.Now().Add(-deliveryKeptFor-time.Hour))
	s.Require().NoError(err)
	// As a store that has forgotten none yet.
	s.store.DeliveriesSeen.pruned.Store(0)

	_, err = s.store.ChatInbounds.QueueFirst(s.ctx, s.deliveryKey(), s.chatInbound("msg-one"))
	s.Require().NoError(err)

	kept, err := s.store.GlobalDB().NewSelect().Table("deliveries_seen").Where("scope = ? AND key = ?", DeliveryScopeHook, old).Count(s.ctx)
	s.Require().NoError(err)
	s.Zero(kept, "a key of a day ago is still recorded")
}

func (s *StoreSuite) TestAChatMessageIsNotClaimedBeforeItIsDue() {
	s.queuedChat("msg-one", func(delivery *ChatInbound) { delivery.NextAttemptAt = time.Now().Add(time.Hour) })

	s.Empty(s.claimChats())
	s.timePasses("chat_inbound", time.Hour)
	s.claimedChat("msg-one")
}

// A worker that stopped mid-answer leaves its row leased: another takes it once the lease the
// claim gave runs out, and not before.
func (s *StoreSuite) TestAClaimedChatMessageIsClaimedAgainOnceItsLeaseRunsOut() {
	s.queuedChat("msg-one")
	s.claimedChat("msg-one")

	s.timePasses("chat_inbound", chatLease-5*time.Second)
	s.Empty(s.claimChats(), "still leased")
	s.timePasses("chat_inbound", 5*time.Second)
	again := s.claimedChat("msg-one")

	s.Equal(2, again.Attempts, "the claim whose lease ran out counts too")
	s.WithinDuration(time.Now().Add(chatLease), again.NextAttemptAt, 5*time.Second)
}

func (s *StoreSuite) TestAChatClaimTakesAtMostLimitOldestDueFirst() {
	s.queuedChat("due-last", func(delivery *ChatInbound) { delivery.NextAttemptAt = s.base.Add(-time.Second) })
	s.queuedChat("due-first", func(delivery *ChatInbound) { delivery.NextAttemptAt = s.base.Add(-time.Minute) })
	s.queuedChat("due-first-too", func(delivery *ChatInbound) { delivery.NextAttemptAt = s.base.Add(-time.Minute) })

	claimed, err := s.store.ChatInbounds.Claim(s.ctx, 2, chatLease)

	s.Require().NoError(err)
	s.Require().Len(claimed, 2)
	s.ElementsMatch([]string{"due-first", "due-first-too"}, []string{claimed[0].MessageID, claimed[1].MessageID})
	s.claimedChat("due-last")
}

func (s *StoreSuite) TestAChatClaimTakesMessagesDueAtOnceInTheOrderTheyWereQueued() {
	for _, id := range []string{"msg-one", "msg-two", "msg-three"} {
		s.queuedChat(id)
	}

	for _, id := range []string{"msg-one", "msg-two", "msg-three"} {
		claimed, err := s.store.ChatInbounds.Claim(s.ctx, 1, chatLease)
		s.Require().NoError(err)
		s.Require().Len(claimed, 1)
		s.Equal(id, claimed[0].MessageID)
	}
}

func (s *StoreSuite) TestAChatClaimWithNoRoomTakesNothing() {
	s.queuedChat("msg-one")

	claimed, err := s.store.ChatInbounds.Claim(s.ctx, -1, chatLease)

	s.Require().NoError(err)
	s.Empty(claimed)
	s.claimedChat("msg-one")
}

func (s *StoreSuite) TestARetriedChatMessageIsDueAtItsNextAttempt() {
	s.queuedChat("msg-one")
	claimed := s.claimedChat("msg-one")

	retried, err := s.store.ChatInbounds.Retry(s.ctx, claimed, time.Now().Add(time.Hour))

	s.Require().NoError(err)
	s.True(retried)
	s.Empty(s.claimChats())
	s.timePasses("chat_inbound", time.Hour)
	again := s.claimedChat("msg-one")
	s.Equal(2, again.Attempts, "the retried claim and this one")
	s.Equal(ChatInboundQueued, again.State)
}

func (s *StoreSuite) TestAnAnsweredChatMessageIsDoneWithItsAttempts() {
	s.queuedChat("msg-one")
	_, err := s.store.ChatInbounds.Retry(s.ctx, s.claimedChat("msg-one"), s.base.Add(time.Second))
	s.Require().NoError(err)
	claimed := s.claimedChat("msg-one")

	finished, err := s.store.ChatInbounds.Finish(s.ctx, claimed, s.base.Add(5*time.Second))

	s.Require().NoError(err)
	s.True(finished)
	state, attempts, finishedAt := s.chatRow("msg-one")
	s.Equal(ChatInboundDone, state)
	s.Equal(2, attempts, "each claim counts, and finishing adds none")
	s.Require().NotNil(finishedAt)
	s.True(finishedAt.Equal(s.base.Add(5 * time.Second)))
	s.timePasses("chat_inbound", time.Hour)
	s.Empty(s.claimChats())
}

func (s *StoreSuite) TestAGivenUpChatMessageIsFailedWithItsLastAttemptCounted() {
	s.queuedChat("msg-one")
	claimed := s.claimedChat("msg-one")

	failed, err := s.store.ChatInbounds.Fail(s.ctx, claimed, s.base.Add(5*time.Second))

	s.Require().NoError(err)
	s.True(failed)
	state, attempts, finishedAt := s.chatRow("msg-one")
	s.Equal(ChatInboundFailed, state)
	s.Equal(1, attempts)
	s.Require().NotNil(finishedAt)
	s.True(finishedAt.Equal(s.base.Add(5 * time.Second)))
	s.timePasses("chat_inbound", time.Hour)
	s.Empty(s.claimChats())
}

// A worker whose lease ran out while it answered, and whose message another worker took since,
// must not finish, retry or give up the other worker's attempt.
func (s *StoreSuite) TestAWorkerWhoseChatLeaseWasTakenOverChangesNothing() {
	s.queuedChat("msg-one")
	stale := s.claimedChat("msg-one")
	s.timePasses("chat_inbound", chatLease)
	current := s.claimedChat("msg-one")

	retried, err := s.store.ChatInbounds.Retry(s.ctx, stale, s.base.Add(chatLease))
	s.Require().NoError(err)
	failed, err := s.store.ChatInbounds.Fail(s.ctx, stale, s.base.Add(chatLease))
	s.Require().NoError(err)
	finished, err := s.store.ChatInbounds.Finish(s.ctx, stale, s.base.Add(chatLease))
	s.Require().NoError(err)

	s.False(retried)
	s.False(failed)
	s.False(finished)
	state, attempts, _ := s.chatRow("msg-one")
	s.Equal(ChatInboundQueued, state)
	s.Equal(2, attempts, "the current claim's count, which the stale worker left alone")
	s.Empty(s.claimChats(), "the current worker's lease still holds it")
	changed, err := s.store.ChatInbounds.Finish(s.ctx, current, s.base.Add(chatLease))
	s.Require().NoError(err)
	s.True(changed)
}

func (s *StoreSuite) TestAnAnsweredChatMessageIsNotGivenUpOrRetriedAfter() {
	s.queuedChat("msg-one")
	claimed := s.claimedChat("msg-one")
	_, err := s.store.ChatInbounds.Finish(s.ctx, claimed, s.base)
	s.Require().NoError(err)

	failed, err := s.store.ChatInbounds.Fail(s.ctx, claimed, s.base)
	s.Require().NoError(err)
	retried, err := s.store.ChatInbounds.Retry(s.ctx, claimed, s.base)
	s.Require().NoError(err)

	s.False(failed)
	s.False(retried)
	state, attempts, _ := s.chatRow("msg-one")
	s.Equal(ChatInboundDone, state)
	s.Equal(1, attempts, "the one claim")
}

// claimChatsWhileAnotherRouterActs runs router A's paused claim and, while A is in its pause,
// meanwhile on this suite's store, and returns what A took.
func (s *StoreSuite) claimChatsWhileAnotherRouterActs(meanwhile func()) []ChatInbound {
	routerA, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(routerA.Close()) })
	type claim struct {
		claimed []ChatInbound
		err     error
	}
	byA := make(chan claim, 1)
	go func() {
		claimed, err := routerA.ChatInbounds.claim(s.ctx, 10, chatLease, func(due *bun.SelectQuery) *bun.SelectQuery {
			// A pause before the claim locks a row, so another router's claim commits inside it.
			return queuedChatInbound(due).Where("(SELECT 1 FROM pg_sleep(0.5)) = 1")
		})
		byA <- claim{claimed, err}
	}()
	s.Require().Eventually(func() bool {
		var sleeping int
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND datname = current_database()").Scan(&sleeping))
		return sleeping == 1
	}, 5*time.Second, 10*time.Millisecond, "router A's claim is in its pause")
	meanwhile()
	a := <-byA
	s.Require().NoError(a.err)
	return a.claimed
}

// Router B leases both messages while router A's claim, which read them as due, is paused: A
// checks each row again as it locks it and takes none of what B leased.
func (s *StoreSuite) TestTwoRoutersClaimingChatMessagesAtOnceTakeEachOnce() {
	s.queuedChat("msg-one")
	s.queuedChat("msg-two")
	var byB []ChatInbound

	byA := s.claimChatsWhileAnotherRouterActs(func() { byB = s.claimChats() })

	s.Len(byB, 2)
	s.Empty(byA, "router A takes none of what router B leased")
}

// Router B claims a message and answers it while router A's claim, which read it as queued and
// due, is paused: A must not take the answered message again. B's lease is over at once, so
// only the answer keeps A from it.
func (s *StoreSuite) TestARouterDoesNotClaimAChatMessageAnotherAnsweredDuringItsClaim() {
	s.queuedChat("msg-one")

	byA := s.claimChatsWhileAnotherRouterActs(func() {
		claimed, err := s.store.ChatInbounds.Claim(s.ctx, 1, 0)
		s.Require().NoError(err)
		s.Require().Len(claimed, 1)
		_, err = s.store.ChatInbounds.Finish(s.ctx, claimed[0], s.base)
		s.Require().NoError(err)
	})

	s.Empty(byA)
}

// A row another router holds locked, mid-claim or mid-finish, is skipped: the claim takes the
// other messages at once rather than waiting for that router's commit.
func (s *StoreSuite) TestAChatClaimSkipsAMessageAnotherRouterHoldsLocked() {
	s.queuedChat("msg-one")
	s.queuedChat("msg-two")
	held, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer held.Rollback() //nolint:errcheck // only releases the lock
	_, err = held.ExecContext(s.ctx, "SELECT 1 FROM chat_inbound WHERE message_id = 'msg-one' FOR UPDATE")
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()
	claimed, err := s.store.ChatInbounds.Claim(ctx, 10, chatLease)

	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Equal("msg-two", claimed[0].MessageID)
}

// Two routers claiming over and over at the same moment, one row at a time, take each message
// exactly once between them.
func (s *StoreSuite) TestRoutersClaimingChatMessagesConcurrentlyTakeEachExactlyOnce() {
	const messages = 40
	for i := range messages {
		s.queuedChat(fmt.Sprintf("msg-%02d", i))
	}
	routerA, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(routerA.Close()) })

	var mu sync.Mutex
	taken := map[string]int{}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, router := range []*Store{s.store, routerA} {
		wg.Go(func() {
			// Bounded, so a claim that takes a message again fails the test rather than
			// spinning on it.
			for range 2 * messages {
				claimed, err := router.ChatInbounds.Claim(s.ctx, 1, chatLease)
				if err != nil {
					errs <- err
					return
				}
				if len(claimed) == 0 {
					return
				}
				mu.Lock()
				taken[claimed[0].MessageID]++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		s.Require().NoError(err)
	}
	s.Len(taken, messages)
	for id, times := range taken {
		s.Equal(1, times, "%s taken %d times", id, times)
	}
}

func (s *StoreSuite) TestTheNextChatMessageIsTheQueuedOneDueFirst() {
	_, found, err := s.store.ChatInbounds.NextDue(s.ctx)
	s.Require().NoError(err)
	s.False(found, "none when nothing is queued")

	s.queuedChat("msg-one")
	s.queuedChat("msg-two", func(delivery *ChatInbound) { delivery.NextAttemptAt = time.Now().Add(time.Hour) })
	s.queuedChat("msg-three", func(delivery *ChatInbound) { delivery.NextAttemptAt = s.base.Add(-time.Hour) })
	claimed, err := s.store.ChatInbounds.Claim(s.ctx, 1, chatLease)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Require().Equal("msg-three", claimed[0].MessageID)
	_, err = s.store.ChatInbounds.Finish(s.ctx, claimed[0], s.base)
	s.Require().NoError(err)
	s.claimedChat("msg-one")

	next, found, err := s.store.ChatInbounds.NextDue(s.ctx)

	s.Require().NoError(err)
	s.True(found)
	s.WithinDuration(time.Now().Add(chatLease), next, 5*time.Second, "msg-one is due at the end of its lease, not an answered row's time")
}

func (s *StoreSuite) TestNoChatMessageIsNextOnceAllAreAnsweredOrGivenUp() {
	s.queuedChat("msg-one")
	s.queuedChat("msg-two")
	for _, claimed := range s.claimChats() {
		var err error
		if claimed.MessageID == "msg-one" {
			_, err = s.store.ChatInbounds.Finish(s.ctx, claimed, s.base)
		} else {
			_, err = s.store.ChatInbounds.Fail(s.ctx, claimed, s.base)
		}
		s.Require().NoError(err)
	}

	_, found, err := s.store.ChatInbounds.NextDue(s.ctx)

	s.Require().NoError(err)
	s.False(found)
}

func (s *StoreSuite) TestPruningKeepsAnAnsweredChatMessageADayAndAGivenUpOneThirtyDays() {
	for _, id := range []string{"done-old", "done-new", "failed-old", "failed-new"} {
		s.queuedChat(id, func(delivery *ChatInbound) { delivery.NextAttemptAt = time.Now().Add(time.Hour) })
	}
	s.queuedChat("queued", func(delivery *ChatInbound) { delivery.NextAttemptAt = s.base.Add(-60 * 24 * time.Hour) })
	finishedAt := map[string]time.Time{
		"done-old":   s.base.Add(-24*time.Hour - time.Second),
		"done-new":   s.base.Add(-24*time.Hour + time.Second),
		"failed-old": s.base.Add(-30*24*time.Hour - time.Second),
		"failed-new": s.base.Add(-30*24*time.Hour + time.Second),
	}
	claimed, err := s.store.ChatInbounds.Claim(s.ctx, 10, chatLease)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1, "only the long-queued row is due")
	_, err = s.store.ChatInbounds.Retry(s.ctx, claimed[0], time.Now().Add(2*time.Hour))
	s.Require().NoError(err)
	s.timePasses("chat_inbound", time.Hour)
	for _, claimed := range s.claimChats() {
		at := finishedAt[claimed.MessageID]
		if strings.HasPrefix(claimed.MessageID, "done") {
			_, err = s.store.ChatInbounds.Finish(s.ctx, claimed, at)
		} else {
			_, err = s.store.ChatInbounds.Fail(s.ctx, claimed, at)
		}
		s.Require().NoError(err)
	}

	pruned, err := s.store.ChatInbounds.Prune(s.ctx, s.base)

	s.Require().NoError(err)
	s.Equal(2, pruned)
	var kept []string
	rows, err := s.store.GlobalDB().QueryContext(s.ctx, "SELECT message_id FROM chat_inbound ORDER BY message_id")
	s.Require().NoError(err)
	defer rows.Close() //nolint:errcheck // read to the end below
	for rows.Next() {
		var id string
		s.Require().NoError(rows.Scan(&id))
		kept = append(kept, id)
	}
	s.Require().NoError(rows.Err())
	s.Equal([]string{"done-new", "failed-new", "queued"}, kept, "a queued row is never pruned, however old")
}

// A message's answer starts once: the row leaves the queue as it starts, so no later try, nor
// a later claim, answers it again, and the worker holding the lease still finishes it.
func (s *StoreSuite) TestAChatMessagesAnswerStartsOnce() {
	s.queuedChat("msg-one")
	claimed := s.claimedChat("msg-one")

	started, err := s.store.ChatInbounds.StartAnswer(s.ctx, 1257545, "msg-one", s.base.Add(time.Second))
	s.Require().NoError(err)
	again, err := s.store.ChatInbounds.StartAnswer(s.ctx, 1257545, "msg-one", s.base.Add(2*time.Second))
	s.Require().NoError(err)

	s.True(started)
	s.False(again, "an answer that started is not started again")
	s.timePasses("chat_inbound", time.Hour)
	s.Empty(s.claimChats(), "a message whose answer started is not claimed again")
	finished, err := s.store.ChatInbounds.Finish(s.ctx, claimed, s.base.Add(5*time.Second))
	s.Require().NoError(err)
	s.True(finished, "the worker holding the lease records its attempt")
	state, attempts, finishedAt := s.chatRow("msg-one")
	s.Equal(ChatInboundDone, state)
	s.Equal(1, attempts, "the one claim")
	s.Require().NotNil(finishedAt)
	s.True(finishedAt.Equal(s.base.Add(time.Second)), "it left the queue as its answer started: %s", finishedAt)
}

// The message is named by its app and its id together: either alone is another message.
func (s *StoreSuite) TestAChatAnswerStartsOnlyForItsOwnAppAndMessage() {
	s.queuedChat("msg-one")

	otherApp, err := s.store.ChatInbounds.StartAnswer(s.ctx, 1257546, "msg-one", s.base)
	s.Require().NoError(err)
	otherMessage, err := s.store.ChatInbounds.StartAnswer(s.ctx, 1257545, "msg-two", s.base)
	s.Require().NoError(err)

	s.False(otherApp)
	s.False(otherMessage)
	state, _, finishedAt := s.chatRow("msg-one")
	s.Equal(ChatInboundQueued, state)
	s.Nil(finishedAt)
}

func (s *StoreSuite) TestAGivenUpChatMessagesAnswerDoesNotStart() {
	s.queuedChat("msg-one")
	_, err := s.store.ChatInbounds.Fail(s.ctx, s.claimedChat("msg-one"), s.base)
	s.Require().NoError(err)

	started, err := s.store.ChatInbounds.StartAnswer(s.ctx, 1257545, "msg-one", s.base)

	s.Require().NoError(err)
	s.False(started)
	state, _, _ := s.chatRow("msg-one")
	s.Equal(ChatInboundFailed, state)
}

// A worker whose try failed after the answer started elsewhere, such as a peer whose reply was
// lost, neither retries nor gives up the message: it was answered.
func (s *StoreSuite) TestAChatMessageWhoseAnswerStartedIsNotRetriedOrGivenUp() {
	s.queuedChat("msg-one")
	claimed := s.claimedChat("msg-one")
	_, err := s.store.ChatInbounds.StartAnswer(s.ctx, 1257545, "msg-one", s.base)
	s.Require().NoError(err)

	retried, err := s.store.ChatInbounds.Retry(s.ctx, claimed, s.base.Add(time.Second))
	s.Require().NoError(err)
	failed, err := s.store.ChatInbounds.Fail(s.ctx, claimed, s.base.Add(time.Second))
	s.Require().NoError(err)

	s.False(retried)
	s.False(failed)
	state, attempts, _ := s.chatRow("msg-one")
	s.Equal(ChatInboundDone, state)
	s.Equal(1, attempts, "the one claim")
}

// A worker whose lease ran out does not finish the attempt of the worker that took the message
// since, even once that one's answer started.
func (s *StoreSuite) TestAStartedChatMessageIsNotFinishedByAWorkerWhoseLeaseWasTakenOver() {
	s.queuedChat("msg-one")
	stale := s.claimedChat("msg-one")
	s.timePasses("chat_inbound", chatLease)
	current := s.claimedChat("msg-one")
	_, err := s.store.ChatInbounds.StartAnswer(s.ctx, 1257545, "msg-one", s.base.Add(chatLease))
	s.Require().NoError(err)

	staleFinished, err := s.store.ChatInbounds.Finish(s.ctx, stale, s.base.Add(chatLease))
	s.Require().NoError(err)
	currentFinished, err := s.store.ChatInbounds.Finish(s.ctx, current, s.base.Add(chatLease))
	s.Require().NoError(err)

	s.False(staleFinished)
	s.True(currentFinished)
}

// A message whose answer started and that nothing then took is queued again under the same
// lease: the worker holding it retries it, and the next try starts its answer.
func (s *StoreSuite) TestAReleasedChatAnswerIsRetriedAndStartsAgain() {
	s.queuedChat("msg-one")
	claimed := s.claimedChat("msg-one")
	_, err := s.store.ChatInbounds.StartAnswer(s.ctx, 1257545, "msg-one", s.base)
	s.Require().NoError(err)

	s.Require().NoError(s.store.ChatInbounds.ReleaseAnswer(s.ctx, 1257545, "msg-one"))

	state, _, finishedAt := s.chatRow("msg-one")
	s.Equal(ChatInboundQueued, state)
	s.Nil(finishedAt)
	retried, err := s.store.ChatInbounds.Retry(s.ctx, claimed, s.base.Add(time.Second))
	s.Require().NoError(err)
	s.True(retried)
	s.claimedChat("msg-one")
	started, err := s.store.ChatInbounds.StartAnswer(s.ctx, 1257545, "msg-one", s.base.Add(time.Second))
	s.Require().NoError(err)
	s.True(started)
}

// Only an answer that started is given back: a message given up, or of another app, stays as
// it is.
func (s *StoreSuite) TestReleasingAChatAnswerLeavesAGivenUpMessageAndAnotherAppsAlone() {
	s.queuedChat("msg-one")
	_, err := s.store.ChatInbounds.Fail(s.ctx, s.claimedChat("msg-one"), s.base)
	s.Require().NoError(err)
	s.queuedChat("msg-two")
	_, err = s.store.ChatInbounds.StartAnswer(s.ctx, 1257545, "msg-two", s.base)
	s.Require().NoError(err)

	s.Require().NoError(s.store.ChatInbounds.ReleaseAnswer(s.ctx, 1257545, "msg-one"))
	s.Require().NoError(s.store.ChatInbounds.ReleaseAnswer(s.ctx, 1257546, "msg-two"))
	s.Require().NoError(s.store.ChatInbounds.ReleaseAnswer(s.ctx, 1257545, "msg-three"))

	failed, _, _ := s.chatRow("msg-one")
	s.Equal(ChatInboundFailed, failed)
	started, _, _ := s.chatRow("msg-two")
	s.Equal(ChatInboundDone, started)
}

// Routers starting one message's answer at the same moment start it once between them.
func (s *StoreSuite) TestRoutersStartingAChatAnswerAtOnceStartItOnce() {
	s.queuedChat("msg-one")
	routerA, err := s.openRouter()
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(routerA.Close()) })

	var started atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 20 {
		router := []*Store{s.store, routerA}[i%2]
		wg.Go(func() {
			ok, err := router.ChatInbounds.StartAnswer(s.ctx, 1257545, "msg-one", s.base)
			if err != nil {
				errs <- err
				return
			}
			if ok {
				started.Add(1)
			}
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		s.Require().NoError(err)
	}
	s.Equal(int32(1), started.Load())
}

func (s *StoreSuite) TestChatInboundQueryBudgets() {
	s.Equal(1, s.countQueries(func() { s.queuedChat("msg-one") }), "Queue")
	// The first delivery a store records also forgets old ones; the next within the hour does not.
	_, err := s.store.ChatInbounds.QueueFirst(s.ctx, s.deliveryKey(), s.chatInbound("msg-zero", func(delivery *ChatInbound) {
		delivery.StreamAppPK, delivery.NextAttemptAt = 7, time.Now().Add(time.Hour)
	}))
	s.Require().NoError(err)
	s.Equal(4, s.countQueries(func() {
		queued, err := s.store.ChatInbounds.QueueFirst(s.ctx, s.deliveryKey(), s.chatInbound("msg-two"))
		s.Require().NoError(err)
		s.True(queued)
	}), "QueueFirst: begin, record the delivery, queue, commit")
	s.Equal(1, s.countQueries(func() {
		_, found, err := s.store.ChatInbounds.NextDue(s.ctx)
		s.Require().NoError(err)
		s.True(found)
	}), "NextDue")
	var claimed []ChatInbound
	s.Equal(1, s.countQueries(func() { claimed = s.claimChats() }), "Claim")
	s.Require().Len(claimed, 2)
	s.Equal(1, s.countQueries(func() {
		got, err := s.store.ChatInbounds.Get(s.ctx, claimed[0].Seq)
		s.Require().NoError(err)
		s.Equal(claimed[0].MessageID, got.MessageID)
	}), "Get")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ChatInbounds.KeepHeard(s.ctx, 1257545, claimed[0].MessageID, "hello"))
	}), "KeepHeard")
	s.Equal(1, s.countQueries(func() {
		started, err := s.store.ChatInbounds.StartAnswer(s.ctx, 1257545, claimed[0].MessageID, s.base)
		s.Require().NoError(err)
		s.True(started)
	}), "StartAnswer")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ChatInbounds.ReleaseAnswer(s.ctx, 1257545, claimed[0].MessageID))
	}), "ReleaseAnswer")
	held := func(name string, write func() (bool, error)) {
		s.Equal(1, s.countQueries(func() {
			changed, err := write()
			s.Require().NoError(err)
			s.True(changed, name)
		}), name)
	}
	held("Retry", func() (bool, error) { return s.store.ChatInbounds.Retry(s.ctx, claimed[0], s.base) })
	held("Finish", func() (bool, error) { return s.store.ChatInbounds.Finish(s.ctx, claimed[0], s.base) })
	held("Fail", func() (bool, error) { return s.store.ChatInbounds.Fail(s.ctx, claimed[1], s.base) })
	s.Equal(1, s.countQueries(func() {
		pruned, err := s.store.ChatInbounds.Prune(s.ctx, time.Now().Add(31*24*time.Hour))
		s.Require().NoError(err)
		s.Equal(2, pruned)
	}), "Prune")
}

// PruningKeepsAnAnsweredChatMessageADayAndAGivenUpOneThirtyDays, with a backlog of more than one statement's worth.
func (s *StoreSuite) TestABacklogOfAnsweredChatMessagesIsPrunedWhole() {
	s.inSmallBatches()
	deletes := s.statementsLike("DELETE FROM chat_inbound", s.TestPruningKeepsAnAnsweredChatMessageADayAndAGivenUpOneThirtyDays)
	s.Equal(3, deletes, "a row per statement, then the one that finds none")
}
