//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/uptrace/bun"
)

// episodeOf is a thread episode of the acme-app customer for a contact, which a test changes
// one part of.
func (s *StoreSuite) episodeOf(contactID, threadChannel string) *Episode {
	return &Episode{
		CustomerID:    "acme-app",
		ContactID:     contactID,
		Source:        "sms",
		ThreadChannel: threadChannel,
		StartedAt:     s.base,
		StreamAppPK:   4242,
	}
}

// mapped is a contact map row of the acme-app customer, for an episode to name.
func (s *StoreSuite) mapped(address string) string {
	entry := contact(address, "agent:omni-"+address)
	_, err := s.store.ContactMapEntries.Map(s.ctx, entry)
	s.Require().NoError(err)
	return entry.ID
}

func (s *StoreSuite) TestTheFirstMessageOfAThreadOpensAnEpisodeInProgress() {
	episode := s.episodeOf(s.mapped("+15550100"), "agent:thread-one")

	opened, err := s.store.Episodes.Open(s.ctx, episode)

	s.Require().NoError(err)
	s.True(opened)
	s.Equal(episodeInProgress, episode.Status)
	s.Equal("episode-"+episode.ID, episode.CardMessageID)
	s.Equal(s.base, episode.StartedAt)
}

// Defect: a thread episode dated by Postgres's clock, as a call's is, in place of the time it is
// given: an SMS episode starts when its message was queued (channel_inbound.created_at), so a
// message the bridge writes later keeps its place against the link.
func (s *StoreSuite) TestAThreadEpisodeStartsAtTheTimeItIsGiven() {
	episode := s.episodeOf(s.mapped("+15550100"), "agent:thread-one")

	_, err := s.store.Episodes.Open(s.ctx, episode)

	s.Require().NoError(err)
	var started time.Time
	s.Require().NoError(s.store.global.QueryRowContext(s.ctx, "SELECT started_at FROM episodes WHERE id = ?", episode.ID).Scan(&started))
	s.Equal(time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC), started.UTC())
}

// Three SMS in a row are one episode, so one card.
func (s *StoreSuite) TestTheNextMessagesOfAThreadFindItsOpenEpisode() {
	person := s.mapped("+15550100")
	first := s.episodeOf(person, "agent:thread-one")
	_, err := s.store.Episodes.Open(s.ctx, first)
	s.Require().NoError(err)

	for range 2 {
		next := s.episodeOf(person, "agent:thread-one")
		opened, err := s.store.Episodes.Open(s.ctx, next)
		s.Require().NoError(err)
		s.False(opened)
		s.Equal(first.ID, next.ID)
		s.Equal(first.CardMessageID, next.CardMessageID)
	}
	s.Equal(1, s.episodeRows())
}

// Two first messages of one thread taken at once by two routers open one episode, so the
// thread gets one card: the open-thread index decides which.
func (s *StoreSuite) TestTwoFirstMessagesOfOneThreadAtOnceOpenOneEpisode() {
	person := s.mapped("+15550100")
	pools := s.pools(8)
	var wg sync.WaitGroup
	ids := make([]string, len(pools))
	opened := make([]bool, len(pools))
	for i, pool := range pools {
		wg.Add(1)
		go func() {
			defer wg.Done()
			episode := s.episodeOf(person, "agent:thread-one")
			var err error
			opened[i], err = pool.Episodes.Open(s.ctx, episode)
			s.NoError(err)
			ids[i] = episode.ID
		}()
	}
	wg.Wait()

	count := 0
	for i, id := range ids {
		s.Equal(ids[0], id)
		if opened[i] {
			count++
		}
	}
	s.Equal(1, count, "one of them opened it, so one card is written")
	s.Equal(1, s.episodeRows())
}

func (s *StoreSuite) TestAnotherThreadIsAnotherEpisode() {
	person := s.mapped("+15550100")
	_, err := s.store.Episodes.Open(s.ctx, s.episodeOf(person, "agent:thread-one"))
	s.Require().NoError(err)

	opened, err := s.store.Episodes.Open(s.ctx, s.episodeOf(person, "agent:thread-two"))

	s.Require().NoError(err)
	s.True(opened)
}

// Two calls on one call channel are two episodes: the session keys a call, not the channel.
func (s *StoreSuite) TestEachCallSessionIsAnEpisodeOfItsOwnOnOneCallChannel() {
	person := s.mapped("+15550100")
	call := func(session string) *Episode {
		episode := s.episodeOf(person, "agent:phone-15550199")
		episode.Source, episode.CallID, episode.SessionID = EpisodeCall, "phone-15550199", session
		return episode
	}
	_, err := s.store.Episodes.Open(s.ctx, call("session-one"))
	s.Require().NoError(err)

	opened, err := s.store.Episodes.Open(s.ctx, call("session-two"))
	s.Require().NoError(err)
	s.True(opened)
	again, err := s.store.Episodes.Open(s.ctx, call("session-two"))
	s.Require().NoError(err)
	s.False(again, "a session's call is one episode")
	s.Equal(2, s.episodeRows())
}

// A thread episode closed by T55 leaves the thread free for the next one.
func (s *StoreSuite) TestAThreadWhoseEpisodeEndedOpensANewOne() {
	person := s.mapped("+15550100")
	first := s.episodeOf(person, "agent:thread-one")
	_, err := s.store.Episodes.Open(s.ctx, first)
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "UPDATE episodes SET status = 'ended', ended_at = now() WHERE id = ?", first.ID)
	s.Require().NoError(err)

	next := s.episodeOf(person, "agent:thread-one")
	opened, err := s.store.Episodes.Open(s.ctx, next)

	s.Require().NoError(err)
	s.True(opened)
	s.NotEqual(first.ID, next.ID)
}

func (s *StoreSuite) TestOnlyACallEpisodeNamesASession() {
	person := s.mapped("+15550100")
	thread := s.episodeOf(person, "agent:thread-one")
	thread.SessionID = "session-one"
	_, err := s.store.Episodes.Open(s.ctx, thread)
	s.ErrorContains(err, "session_id is named by a call episode, and only by one")

	call := s.episodeOf(person, "agent:call-one")
	call.Source = EpisodeCall
	_, err = s.store.Episodes.Open(s.ctx, call)
	s.ErrorContains(err, "session_id is named by a call episode, and only by one")
}

// threadAt is a thread episode of person in thread, opened at at.
func (s *StoreSuite) threadAt(person, thread string, at time.Time) *Episode {
	episode := s.episodeOf(person, thread)
	episode.StartedAt = at
	_, err := s.store.Episodes.Open(s.ctx, episode)
	s.Require().NoError(err)
	return episode
}

// closeIdle closes the thread episodes with no message for an hour before now, leased for
// five minutes.
func (s *StoreSuite) closeIdle(store *Store, now time.Time) []ClosedEpisode {
	closed, err := store.Episodes.CloseIdle(s.ctx, now.Add(-time.Hour), now, 50, 5*time.Minute, "router-a")
	s.Require().NoError(err)
	return closed
}

// episodeStatus is an episode's status as stored.
func (s *StoreSuite) episodeStatus(id string) string {
	var status string
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT status FROM episodes WHERE id = ?", id).Scan(&status))
	return status
}

func (s *StoreSuite) TestAThreadQuietForTheIdlePeriodIsClosedAndLeasedToItsCloser() {
	person := s.mapped("+15550100")
	episode := s.threadAt(person, "agent:thread-one", s.base)
	now := s.base.Add(time.Hour)

	closed := s.closeIdle(s.store, now)

	s.Require().Len(closed, 1)
	s.Equal(episode.ID, closed[0].ID)
	s.Equal(EpisodeEnded, closed[0].Status)
	s.Require().NotNil(closed[0].EndedAt)
	s.True(now.Equal(*closed[0].EndedAt))
	s.Require().NotNil(closed[0].LeaseUntil)
	s.InDelta(5*time.Minute, s.leftOnTheDatabasesClock(*closed[0].LeaseUntil), float64(time.Second), "five minutes on Postgres's clock")
	s.Equal("router-a", closed[0].LeasedBy, "the router that closed it")
	s.Equal("agent:omni-+15550100", closed[0].ConversationID, "the omni-channel the card is in")
	s.Equal("agent-one", closed[0].ConfigID, "the config whose LLM writes the summary")
	s.Equal(EpisodeEnded, s.episodeStatus(episode.ID))
}

// Each message of a thread is its last one, so a thread that is talking is not idle.
func (s *StoreSuite) TestAThreadWithAMessageInTheIdlePeriodStaysOpen() {
	person := s.mapped("+15550100")
	episode := s.threadAt(person, "agent:thread-one", s.base)
	s.threadAt(person, "agent:thread-one", s.base.Add(50*time.Minute))

	closed := s.closeIdle(s.store, s.base.Add(90*time.Minute))

	s.Empty(closed)
	s.Equal(episodeInProgress, s.episodeStatus(episode.ID))
}

// A call ends when its call does, never for being quiet.
func (s *StoreSuite) TestACallIsNeverClosedAsIdle() {
	call := s.episodeOf(s.mapped("+15550100"), "agent:call-one")
	call.Source, call.CallID, call.SessionID = EpisodeCall, "call-one", "session-one"
	_, err := s.store.Episodes.Open(s.ctx, call)
	s.Require().NoError(err)

	closed := s.closeIdle(s.store, s.base.Add(48*time.Hour))

	s.Empty(closed)
	s.Equal(episodeInProgress, s.episodeStatus(call.ID))
}

// Several routers sweep at once: each idle episode is closed by one of them, once, so one
// summary is written for it.
func (s *StoreSuite) TestTwoSweepersAtOnceCloseEachIdleEpisodeOnce() {
	person := s.mapped("+15550100")
	for i := range 20 {
		s.threadAt(person, fmt.Sprintf("agent:thread-%d", i), s.base)
	}
	pools := s.pools(8)
	now := s.base.Add(time.Hour)
	var wg sync.WaitGroup
	closed := make([][]ClosedEpisode, len(pools))
	for i, pool := range pools {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			closed[i], err = pool.Episodes.CloseIdle(s.ctx, now.Add(-time.Hour), now, 50, 5*time.Minute, "router-a")
			s.NoError(err)
		}()
	}
	wg.Wait()

	seen := map[string]int{}
	for _, swept := range closed {
		for _, episode := range swept {
			seen[episode.ID]++
		}
	}
	s.Len(seen, 20)
	for id, times := range seen {
		s.Equal(1, times, "episode %s was closed by more than one sweeper", id)
	}
}

// A router that is closing an episode holds its row until it commits. Another sweep leaves
// that row to it, rather than waiting and closing it a second time.
func (s *StoreSuite) TestAnEpisodeAnotherRouterIsClosingIsLeftToIt() {
	episode := s.threadAt(s.mapped("+15550100"), "agent:thread-one", s.base)
	closing, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	_, err = closing.ExecContext(s.ctx, "UPDATE episodes SET status = 'ended', ended_at = ? WHERE id = ?", s.base.Add(time.Hour), episode.ID)
	s.Require().NoError(err)

	swept := make(chan []ClosedEpisode, 1)
	go func() {
		now := s.base.Add(time.Hour)
		closed, err := s.store.Episodes.CloseIdle(s.ctx, now.Add(-time.Hour), now, 50, 5*time.Minute, "router-a")
		s.NoError(err)
		swept <- closed
	}()
	// A sweep that waited for the row would go on once the other router commits: give it
	// the time to reach the row before that.
	var closed []ClosedEpisode
	select {
	case closed = <-swept:
	case <-time.After(200 * time.Millisecond):
		s.Require().NoError(closing.Commit())
		closed = <-swept
	}
	_ = closing.Rollback()

	s.Empty(closed, "the episode was closed twice")
}

// A call id is unique only in its app: the event from one app ends that app's call only.
func (s *StoreSuite) TestACallsEpisodesEndWithItInItsOwnAppOnly() {
	person := s.mapped("+15550100")
	call := func(callID, session string) *Episode {
		episode := s.episodeOf(person, "agent:"+callID)
		episode.Source, episode.CallID, episode.SessionID = EpisodeCall, callID, session
		_, err := s.store.Episodes.Open(s.ctx, episode)
		s.Require().NoError(err)
		return episode
	}
	first, second, other := call("call-one", "session-one"), call("call-one", "session-two"), call("call-two", "session-three")
	now := s.base.Add(10 * time.Minute)

	elsewhere, err := s.store.Episodes.EndCall(s.ctx, AppScope{App: 7}, "call-one", now, 5*time.Minute, "router-a")
	s.Require().NoError(err)
	s.Empty(elsewhere, "another app's call of the same id")
	ended, err := s.store.Episodes.EndCall(s.ctx, AppScope{App: 4242}, "call-one", now, 5*time.Minute, "router-a")
	s.Require().NoError(err)

	s.ElementsMatch([]string{first.ID, second.ID}, []string{ended[0].ID, ended[1].ID})
	s.Require().NotNil(ended[0].LeaseUntil)
	s.InDelta(5*time.Minute, s.leftOnTheDatabasesClock(*ended[0].LeaseUntil), float64(time.Second), "held for its summary five minutes on Postgres's clock")
	s.Equal(episodeInProgress, s.episodeStatus(other.ID))
	again, err := s.store.Episodes.EndCall(s.ctx, AppScope{App: 4242}, "call-one", now, 5*time.Minute, "router-a")
	s.Require().NoError(err)
	s.Empty(again, "an event delivered twice ends each episode once")
}

// A router that closed an episode and stopped before its summary leaves it ended; once the
// lease runs out the next sweep takes it, once.
func (s *StoreSuite) TestAnEpisodeWhoseSummaryLeaseRanOutIsTakenAgainOnce() {
	episode := s.threadAt(s.mapped("+15550100"), "agent:thread-one", s.base)
	s.closeIdle(s.store, s.base.Add(time.Hour))

	s.leasesPass("episodes", 4*time.Minute)
	early, err := s.store.Episodes.ClaimSummaries(s.ctx, 10, time.Hour, "router-a")
	s.Require().NoError(err)
	s.Empty(early, "its closer still holds it")
	s.leasesPass("episodes", 2*time.Minute)
	taken, err := s.store.Episodes.ClaimSummaries(s.ctx, 10, 5*time.Minute, "router-b")
	s.Require().NoError(err)
	s.Require().Len(taken, 1)
	s.Equal(episode.ID, taken[0].ID)
	s.Equal("agent:omni-+15550100", taken[0].ConversationID)
	s.Require().NotNil(taken[0].LeaseUntil)
	s.InDelta(5*time.Minute, s.leftOnTheDatabasesClock(*taken[0].LeaseUntil), float64(time.Second), "the new lease, five minutes on Postgres's clock")
	s.Equal("router-b", taken[0].LeasedBy, "the router that took it over")
	again, err := s.store.Episodes.ClaimSummaries(s.ctx, 10, 5*time.Minute, "router-a")
	s.Require().NoError(err)
	s.Empty(again, "the new lease holds it")
}

func (s *StoreSuite) TestAFinishedSummaryIsNeverTakenAgain() {
	episode := s.threadAt(s.mapped("+15550100"), "agent:thread-one", s.base)
	s.closeIdle(s.store, s.base.Add(time.Hour))

	s.Require().NoError(s.store.Episodes.FinishSummary(s.ctx, "acme-app", episode.ID, EpisodeSummarized))

	s.Equal(EpisodeSummarized, s.episodeStatus(episode.ID))
	s.leasesPass("episodes", 48*time.Hour)
	taken, err := s.store.Episodes.ClaimSummaries(s.ctx, 10, time.Hour, "router-a")
	s.Require().NoError(err)
	s.Empty(taken)
}

// Only an ended episode is finished: one still in progress has no summary to finish.
func (s *StoreSuite) TestASummaryFinishesOnlyAnEndedEpisode() {
	episode := s.threadAt(s.mapped("+15550100"), "agent:thread-one", s.base)

	s.Require().NoError(s.store.Episodes.FinishSummary(s.ctx, "acme-app", episode.ID, EpisodeSummaryFailed))

	s.Equal(episodeInProgress, s.episodeStatus(episode.ID))
}

// anyToSweep is Episodes.AnyToSweep's answer.
func (s *StoreSuite) anyToSweep() bool {
	due, err := s.store.Episodes.AnyToSweep(s.ctx)
	s.Require().NoError(err)
	return due
}

func (s *StoreSuite) TestAnyEpisodeToSweepIsWhetherAConfigTurnedTheCardsOn() {
	off := &AgentConfig{CustomerID: "acme-app", Name: "plain"}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, off))
	s.False(s.anyToSweep(), "a config that never had the cards on, and no episode, has nothing to sweep")

	on := &AgentConfig{CustomerID: "acme-app", Name: "carded", EpisodeCards: AgentEpisodeCards{On: true}}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, on))
	s.True(s.anyToSweep())

	s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, "acme-app", on.ID))
	s.False(s.anyToSweep(), "a deleted config writes no card")
}

// Catches a sweeper that never starts for a config recording the cards: record opens call and
// app chat episodes as on does, and they must close and be summarized (AI-2856).
func (s *StoreSuite) TestAnyEpisodeToSweepCountsAConfigThatRecordsTheCards() {
	off := &AgentConfig{CustomerID: "acme-app", Name: "plain"}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, off))
	s.Require().False(s.anyToSweep())

	record := &AgentConfig{CustomerID: "acme-app", Name: "recording", EpisodeCards: AgentEpisodeCards{Record: true}}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, record))
	s.True(s.anyToSweep())

	s.Require().NoError(s.store.AgentConfigs.Delete(s.ctx, "acme-app", record.ID))
	s.False(s.anyToSweep(), "a deleted config writes no card")
}

// Catches an update that drops episode_cards_record (configColumns): a record config saved
// again would turn off.
func (s *StoreSuite) TestAnUpdateStoresWhetherAConfigRecordsTheCards() {
	config := &AgentConfig{CustomerID: "acme-app", Name: "recording"}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, config))
	config.EpisodeCards.Record = true
	s.Require().NoError(s.store.AgentConfigs.Update(s.ctx, config))

	stored, err := s.store.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(err)
	s.True(stored.EpisodeCards.Record)
	s.False(stored.EpisodeCards.On)
}

// Catches AI-2771: a config turned the cards off while its chat episode was open, and the
// router started again with no config's cards on; the episode was never closed.
func (s *StoreSuite) TestAThreadEpisodeLeftInProgressAfterTheCardsTurnedOffIsToSweep() {
	off := &AgentConfig{CustomerID: "acme-app", Name: "was carded"}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, off))
	episode := s.threadAt(s.mapped("+15550100"), "agent:thread-one", s.base)
	s.True(s.anyToSweep())

	s.Require().Len(s.closeIdle(s.store, s.base.Add(2*time.Hour)), 1)
	s.Require().NoError(s.store.Episodes.FinishSummary(s.ctx, "acme-app", episode.ID, EpisodeSummarized))
	s.False(s.anyToSweep(), "a summarized episode is nothing to sweep")
}

// Catches a sweeper started for a call alone, which its call hook ends, not the sweeper.
func (s *StoreSuite) TestACallEpisodeInProgressIsNothingToSweep() {
	call := s.episodeOf(s.mapped("+15550100"), "agent:call-one")
	call.Source, call.CallID, call.SessionID = EpisodeCall, "call-one", "session-one"
	_, err := s.store.Episodes.Open(s.ctx, call)
	s.Require().NoError(err)

	s.False(s.anyToSweep())
}

// Catches an ended episode whose router stopped before its summary, under a config that
// turned the cards off since: its lease is taken by a sweep only. The lease is still running,
// as when a router starts while another is summarizing and then dies: no sweeper would take it.
func (s *StoreSuite) TestAnEndedEpisodeWithItsSummaryLeaseHeldIsToSweep() {
	episode := s.threadAt(s.mapped("+15550100"), "agent:thread-one", s.base)
	now := s.base.Add(2 * time.Hour)
	closed, err := s.store.Episodes.CloseIdle(s.ctx, now.Add(-time.Hour), now, 50, 5*time.Minute, "router-a")
	s.Require().NoError(err)
	s.Require().Len(closed, 1)

	s.True(s.anyToSweep(), "a lease still running is to sweep once it runs out")

	s.Require().NoError(s.store.Episodes.FinishSummary(s.ctx, "acme-app", episode.ID, EpisodeSummaryFailed))
	s.False(s.anyToSweep(), "a failed summary drops its lease, and the episode is nothing to sweep")
}

// Catches an episode closed early by a message tried again: a message is touched at the time
// it was made or queued, so one tried again after a later message is older than the episode's
// last message, and the episode stays open from that later one on.
func (s *StoreSuite) TestAMessageTriedAgainAfterALaterOneKeepsTheEpisodeOpenFromTheLaterOne() {
	person := s.mapped("+15550100")
	episode := s.threadAt(person, "agent:thread-one", s.base)
	s.threadAt(person, "agent:thread-one", s.base.Add(30*time.Minute))
	s.threadAt(person, "agent:thread-one", s.base.Add(time.Minute))

	s.Empty(s.closeIdle(s.store, s.base.Add(80*time.Minute)), "closed an hour after the message tried again, not the later one")
	s.Equal(episodeInProgress, s.episodeStatus(episode.ID))
	s.Len(s.closeIdle(s.store, s.base.Add(90*time.Minute)), 1, "the later message's hour is over")
}

// A message being written holds its episode open: a sweep that runs meanwhile leaves it to
// the message, however long it has been since the one before.
func (s *StoreSuite) TestAnEpisodeAMessageIsTouchingIsNotClosed() {
	person := s.mapped("+15550100")
	episode := s.threadAt(person, "agent:thread-one", s.base)
	s.threadAt(person, "agent:thread-one", s.base.Add(time.Minute))
	touching, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer func() { _ = touching.Rollback() }()
	now := s.base.Add(2 * time.Hour)
	var touched Episode
	s.Require().NoError(touching.NewRaw(touchThreadEpisodeQuery, now, "acme-app", "agent:thread-one").Scan(s.ctx, &touched))
	s.Require().Equal(episode.ID, touched.ID, "the message found the episode open")
	// A sweep that waited for the episode would wait until the message commits.
	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()

	closed, err := s.store.Episodes.CloseIdle(ctx, now.Add(-time.Hour), now, 50, 5*time.Minute, "router-a")

	s.Require().NoError(err, "the sweep waited for the message")
	s.Empty(closed, "the sweep closed an episode a message was touching")
	s.Require().NoError(touching.Commit())
	s.Empty(s.closeIdle(s.store, now), "the message keeps the episode open")
	s.Equal(episodeInProgress, s.episodeStatus(episode.ID))
}

// A message can commit after a sweep read the episode as idle and before it locked it: the
// sweep reads the last message again once it holds the row, and leaves the episode open.
func (s *StoreSuite) TestAMessageThatLandsAsASweepLocksTheEpisodeKeepsItOpen() {
	person := s.mapped("+15550100")
	episode := s.threadAt(person, "agent:thread-one", s.base)
	s.threadAt(person, "agent:thread-one", s.base.Add(time.Minute))
	now := s.base.Add(2 * time.Hour)
	order := "ORDER BY ep.started_at"
	s.Require().Contains(idleEpisodesQuery, order)
	// A pause once the sweep's statement read the rows as they were, before it locks them.
	paused := strings.Replace(idleEpisodesQuery, order, "AND (SELECT 1 FROM pg_sleep(0.5)) = 1\n"+order, 1)
	type sweep struct {
		due []string
		err error
	}
	swept := make(chan sweep, 1)
	go func() {
		var due []string
		err := s.store.GlobalDB().NewRaw(paused, now.Add(-time.Hour), 50).Scan(s.ctx, &due)
		swept <- sweep{due, err}
	}()
	s.Require().Eventually(func() bool {
		var sleeping int
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND datname = current_database()").Scan(&sleeping))
		return sleeping == 1
	}, 5*time.Second, 10*time.Millisecond, "the sweep is in its pause")
	// The message's touch, committed while the sweep holds the episode as idle.
	_, err := s.store.GlobalDB().ExecContext(s.ctx,
		"UPDATE episodes SET last_message_at = ? WHERE id = ?", now, episode.ID)
	s.Require().NoError(err)

	got := <-swept

	s.Require().NoError(got.err)
	s.Empty(got.due, "the sweep took an episode with a message in the idle period")
	s.Empty(s.closeIdle(s.store, now), "the message keeps the episode open")
	s.Equal(episodeInProgress, s.episodeStatus(episode.ID))
}

// A sweep that locked an episode closes it once: the close checks the status again, so an
// episode that already ended is not closed, leased and summarized a second time.
func (s *StoreSuite) TestAnEpisodeThatAlreadyEndedIsNotClosedAgain() {
	person := s.mapped("+15550100")
	episode := s.threadAt(person, "agent:thread-one", s.base)
	now := s.base.Add(2 * time.Hour)
	s.Require().Len(s.closeIdle(s.store, now), 1)

	var closed []ClosedEpisode
	err := s.store.GlobalDB().NewRaw(closeIdleEpisodesQuery, now.Add(time.Minute), (10*time.Minute).Seconds(), "router-a", bun.In([]string{episode.ID}), now.Add(-time.Hour)).Scan(s.ctx, &closed)

	s.Require().NoError(err)
	s.Empty(closed, "an ended episode was closed again")
}

// A sweep's batch is idle episodes only: a thread that started earlier but is still talking
// takes no place in it.
func (s *StoreSuite) TestASweepsBatchHoldsOnlyIdleEpisodes() {
	person := s.mapped("+15550100")
	s.threadAt(person, "agent:thread-talking", s.base)
	s.threadAt(person, "agent:thread-talking", s.base.Add(100*time.Minute))
	idle := s.threadAt(person, "agent:thread-idle", s.base.Add(time.Minute))
	now := s.base.Add(2 * time.Hour)

	closed, err := s.store.Episodes.CloseIdle(s.ctx, now.Add(-time.Hour), now, 1, 5*time.Minute, "router-a")

	s.Require().NoError(err)
	s.Require().Len(closed, 1)
	s.Equal(idle.ID, closed[0].ID)
}

// A router that is taking an episode's summary holds its lease until it commits. Another
// sweep leaves it to that router rather than waiting for it.
func (s *StoreSuite) TestALeaseAnotherRouterIsTakingIsLeftToIt() {
	episode := s.threadAt(s.mapped("+15550100"), "agent:thread-one", s.base)
	s.closeIdle(s.store, s.base.Add(time.Hour))
	s.leasesPass("episodes", 6*time.Minute)
	taking, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer func() { _ = taking.Rollback() }()
	_, err = taking.ExecContext(s.ctx, "UPDATE episodes SET lease_until = now() + interval '5 minutes' WHERE id = ?", episode.ID)
	s.Require().NoError(err)

	// A claim that waited for the lease would wait until the other router commits.
	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()

	taken, err := s.store.Episodes.ClaimSummaries(ctx, 10, 5*time.Minute, "router-a")

	s.Require().NoError(err, "the claim waited for the other router")
	s.Empty(taken)
}

// The call hook ends a call's episodes for every call that ends in the app, video calls
// included, so it finds them by an index rather than by reading every episode.
func (s *StoreSuite) TestACallsEpisodesAreFoundByTheirCall() {
	var plan []string
	err := s.store.global.RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			return err
		}
		return tx.NewRaw("EXPLAIN "+endCallEpisodesQuery, s.base, (5*time.Minute).Seconds(), "router-a", "call-one", AppScope{App: 4242}.clause()).Scan(ctx, &plan)
	})

	s.Require().NoError(err)
	s.Contains(strings.Join(plan, "\n"), "episodes_open_call")
}

// The sweep runs every minute, and episodes keeps a row for every thread episode there was,
// so the sweep reads only the open thread episodes, by their index, rather than the whole table.
func (s *StoreSuite) TestASweepReadsOnlyTheOpenThreadEpisodes() {
	var plan []string
	err := s.store.global.RunInTx(s.ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			return err
		}
		return tx.NewRaw("EXPLAIN "+idleEpisodesQuery, s.base, 10).Scan(ctx, &plan)
	})

	s.Require().NoError(err)
	read := strings.Join(plan, "\n")
	s.Contains(read, "episodes_open_thread")
}

// episodeRows is how many episodes there are.
func (s *StoreSuite) episodeRows() int {
	var count int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx, "SELECT count(*) FROM episodes").Scan(&count))
	return count
}

// episodeIn opens an episode of a contact in a thread channel, started minutes after the
// suite's base time.
func (s *StoreSuite) episodeIn(contactID, threadChannel string, minutes int) *Episode {
	episode := s.episodeOf(contactID, threadChannel)
	episode.StartedAt = s.base.Add(time.Duration(minutes) * time.Minute)
	_, err := s.store.Episodes.Open(s.ctx, episode)
	s.Require().NoError(err)
	return episode
}

// called opens the episode of a call session, as the session manager does.
func (s *StoreSuite) called(contactID, callChannel, sessionID string, minutes int) *Episode {
	episode := s.episodeOf(contactID, callChannel)
	episode.Source, episode.SessionID, episode.CallID = EpisodeCall, sessionID, "call-"+sessionID
	episode.StartedAt = s.base.Add(time.Duration(minutes) * time.Minute)
	_, err := s.store.Episodes.Open(s.ctx, episode)
	s.Require().NoError(err)
	return episode
}

// mappedAs is a contact row for address, in an omni-channel of the test's choosing, of a
// customer and an agent of the test's choosing.
func (s *StoreSuite) mappedAs(customerID, agentConfigID, kind, address, omni string) string {
	entry := contact(address, omni)
	entry.CustomerID, entry.ConfigID, entry.Kind = customerID, agentConfigID, kind
	_, err := s.store.ContactMapEntries.Map(s.ctx, entry)
	s.Require().NoError(err)
	return entry.ID
}

// cardsOf reads the cards of the acme-app agent's omni-channel.
func (s *StoreSuite) cardsOf(omni string, query CardsQuery) []EpisodeCard {
	query.CustomerID, query.AgentConfigID, query.ConversationID = "acme-app", "agent-one", omni
	if query.Limit == 0 {
		query.Limit = 10
	}
	cards, err := s.store.Episodes.Cards(s.ctx, query)
	s.Require().NoError(err)
	return cards
}

func ids(cards []EpisodeCard) []string {
	var out []string
	for _, card := range cards {
		out = append(out, card.ID)
	}
	return out
}

// A person's cards are theirs alone: another person of the same agent, the same omni-channel
// id under another customer, and the same one under another agent of the customer are none
// of theirs.
func (s *StoreSuite) TestAPersonsCardsAreOnlyTheirOwnNewestFirst() {
	ann := s.mapped("+15550100")
	first := s.episodeIn(ann, "agent:thread-one", 0)
	second := s.episodeIn(ann, "agent:thread-two", 10)
	s.episodeIn(s.mapped("+15550199"), "agent:thread-bob", 20)
	stranger := s.episodeOf(s.mappedAs("globex", "agent-one", ContactPhone, "+15550100", "agent:omni-+15550100"), "agent:thread-globex")
	stranger.CustomerID = "globex"
	_, err := s.store.Episodes.Open(s.ctx, stranger)
	s.Require().NoError(err)
	s.episodeIn(s.mappedAs("acme-app", "agent-two", ContactPhone, "+15550100", "agent:omni-+15550100"), "agent:thread-other-agent", 40)

	cards := s.cardsOf("agent:omni-+15550100", CardsQuery{})

	s.Equal([]string{second.ID, first.ID}, ids(cards))
}

// Account linking points a Slack row at a phone row's omni-channel; both rows are then the
// one person, and their cards are read together.
func (s *StoreSuite) TestEveryRowPointingAtTheOmniChannelIsThePerson() {
	phone := s.mapped("+15550100")
	slack := s.mappedAs("acme-app", "agent-one", ContactSlack, "T1:U1", "agent:omni-+15550100")
	sms := s.episodeIn(phone, "agent:thread-sms", 0)
	thread := s.episodeIn(slack, "agent:thread-slack", 10)

	cards := s.cardsOf("agent:omni-+15550100", CardsQuery{})
	s.Equal([]string{thread.ID, sms.ID}, ids(cards))
	s.Equal([2]string{ContactSlack, "T1:U1"}, [2]string{cards[0].ContactKind, cards[0].ContactAddress}, "each card says whose row it is")
	s.Equal([2]string{ContactPhone, "+15550100"}, [2]string{cards[1].ContactKind, cards[1].ContactAddress})
	s.Equal("agent-one", cards[0].ConfigID, "the agent config an app chat's channel is checked against")
}

// A session reads its own thread word for word, and its own call is going on, so neither is
// a card.
func (s *StoreSuite) TestTheSessionsOwnThreadAndCallAreLeftOut() {
	ann := s.mapped("+15550100")
	sms := s.episodeIn(ann, "agent:thread-sms", 0)
	slack := s.episodeIn(ann, "agent:thread-slack", 10)
	call := s.called(ann, "agent:phone-1", "session-now", 20)

	s.Equal([]string{call.ID, sms.ID}, ids(s.cardsOf("agent:omni-+15550100", CardsQuery{ExceptThread: "agent:thread-slack"})))
	s.Equal([]string{slack.ID, sms.ID}, ids(s.cardsOf("agent:omni-+15550100", CardsQuery{ExceptSession: "session-now"})))
}

func (s *StoreSuite) TestNoMoreCardsThanTheLimitAreRead() {
	ann := s.mapped("+15550100")
	var newest []string
	for minutes := range 7 {
		newest = append([]string{s.episodeIn(ann, "agent:thread-"+string(rune('a'+minutes)), minutes).ID}, newest...)
	}

	s.Equal(newest[:5], ids(s.cardsOf("agent:omni-+15550100", CardsQuery{Limit: 5})))
}

// A call channel can hold several callers' calls, so a call's lines end where the next
// episode in the channel starts, when its session closed, or when it ended, whichever is
// first.
func (s *StoreSuite) TestACardsLinesEndAtTheFirstThingThatEndsIt() {
	ann, bob := s.mapped("+15550100"), s.mapped("+15550199")
	open := s.called(ann, "agent:phone-1", "session-ann", 0)
	s.called(bob, "agent:phone-1", "session-bob", 30)
	closed := s.called(ann, "agent:phone-2", "session-closed", 40)
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &AgentSession{ID: "session-closed", CustomerID: "acme-app"}))
	s.Require().NoError(s.store.Sessions.Close(s.ctx, "acme-app", "session-closed", s.base.Add(40*time.Minute), s.base.Add(45*time.Minute), ""))
	ended := s.episodeIn(ann, "agent:thread-ended", 50)
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "UPDATE episodes SET status = 'ended', ended_at = ? WHERE id = ?", s.base.Add(55*time.Minute), ended.ID)
	s.Require().NoError(err)
	going := s.episodeIn(ann, "agent:thread-going", 60)

	until := map[string]*time.Time{}
	for _, card := range s.cardsOf("agent:omni-+15550100", CardsQuery{}) {
		until[card.ID] = card.Until
	}

	at := func(minutes int) *time.Time {
		t := s.base.Add(time.Duration(minutes) * time.Minute)
		return &t
	}
	s.Require().Len(until, 4)
	s.Equal(at(30).UTC(), until[open.ID].UTC(), "Bob's call in the same channel")
	s.Equal(at(45).UTC(), until[closed.ID].UTC(), "the session closed")
	s.Equal(at(55).UTC(), until[ended.ID].UTC(), "the episode ended")
	s.Nil(until[going.ID], "nothing ends it yet")
}

// The person of a thread is the one its episode is with, for the customer and agent asked
// about only.
func (s *StoreSuite) TestAThreadsPersonIsTheOneItsEpisodeIsWith() {
	ann := s.mapped("+15550100")
	s.episodeIn(ann, "agent:thread-sms", 0)

	found, err := s.store.ContactMapEntries.GetByThread(s.ctx, "acme-app", "agent-one", "agent:thread-sms")
	s.Require().NoError(err)
	s.Equal(ann, found.ID)

	for _, asked := range [][2]string{{"globex", "agent-one"}, {"acme-app", "agent-two"}} {
		_, err = s.store.ContactMapEntries.GetByThread(s.ctx, asked[0], asked[1], "agent:thread-sms")
		s.True(errors.Is(err, ErrNoContact), asked)
	}
	_, err = s.store.ContactMapEntries.GetByThread(s.ctx, "acme-app", "agent-one", "agent:thread-nobody")
	s.True(errors.Is(err, ErrNoContact))
}

// Reading a person's cards writes nothing: an address the contact map does not hold is none,
// and stays none.
func (s *StoreSuite) TestLookingUpAContactMakesNone() {
	_, err := s.store.ContactMapEntries.GetByAddress(s.ctx, "acme-app", "agent-one", ContactPhone, "+15550100")

	s.True(errors.Is(err, ErrNoContact))
	s.Zero(s.contactRows())

	id := s.mapped("+15550100")
	found, err := s.store.ContactMapEntries.GetByAddress(s.ctx, "acme-app", "agent-one", ContactPhone, "+15550100")
	s.Require().NoError(err)
	s.Equal(id, found.ID)
}

func (s *StoreSuite) TestEpisodeQueryBudgets() {
	person := s.mapped("+15550100")
	var thread *Episode
	s.Equal(1, s.countQueries(func() { thread = s.threadAt(person, "agent:thread-one", s.base) }), "Open of a new thread episode")
	s.Equal(2, s.countQueries(func() { s.threadAt(person, "agent:thread-one", s.base.Add(time.Minute)) }),
		"Open of a thread's next message: the insert finds it open, the touch")
	call := s.episodeOf(person, "agent:call-one")
	call.Source, call.CallID, call.SessionID = EpisodeCall, "call-one", "session-one"
	_, err := s.store.Episodes.Open(s.ctx, call)
	s.Require().NoError(err)
	s.Equal(1, s.countQueries(func() { s.True(s.anyToSweep()) }), "AnyToSweep")
	now := s.base.Add(2 * time.Hour)
	s.Equal(4, s.countQueries(func() { s.Len(s.closeIdle(s.store, now), 1) }), "CloseIdle: begin, lock the idle, close them, commit")
	s.Equal(1, s.countQueries(func() {
		ended, err := s.store.Episodes.EndCall(s.ctx, AppScope{App: 4242}, "call-one", now, 5*time.Minute, "router-a")
		s.Require().NoError(err)
		s.Len(ended, 1)
	}), "EndCall")
	s.leasesPass("episodes", time.Hour)
	s.Equal(1, s.countQueries(func() {
		taken, err := s.store.Episodes.ClaimSummaries(s.ctx, 10, time.Hour, "router-b")
		s.Require().NoError(err)
		s.Len(taken, 2)
	}), "ClaimSummaries")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.Episodes.FinishSummary(s.ctx, "acme-app", thread.ID, EpisodeSummarized))
	}), "FinishSummary")
	s.Equal(1, s.countQueries(func() {
		cards := s.cardsOf("agent:omni-+15550100", CardsQuery{ExceptSession: "session-one"})
		s.Len(cards, 1)
	}), "Cards with no call, whose session is read in the shards")
}
