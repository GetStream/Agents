//go:build integration

package session

import (
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// RecordFlushSuite is the session writer's Flush, against Postgres.
type RecordFlushSuite struct {
	suite.Suite
	ctx      context.Context
	dsn      string
	store    *store.Store
	customer string
	records  *sessionRecorder
}

func TestRecordFlushSuite(t *testing.T) {
	suite.Run(t, new(RecordFlushSuite))
}

func (s *RecordFlushSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN is not set")
	}
	s.ctx = context.Background()
	s.dsn = testenv.Database(dsn, "record_flush")
	db, err := store.Open(s.dsn)
	s.Require().NoError(err)
	s.Require().NoError(db.Migrate(s.ctx))
	s.store = db
	s.T().Cleanup(func() { s.Require().NoError(db.Close()) })
}

func (s *RecordFlushSuite) SetupTest() {
	s.customer = "flush-" + rand.Text()
	s.records = newSessionRecorder(s.store, slog.New(slog.DiscardHandler))
	s.T().Cleanup(s.records.Close)
}

func (s *RecordFlushSuite) TestFlushesWaitingOnASlowWriterLeaveRoomForRows() {
	release := s.holdWriter()
	waiters := recordQueueSize + 64
	errs := make(chan error, waiters)
	var started, finished sync.WaitGroup
	for range waiters {
		started.Add(1)
		finished.Go(func() {
			started.Done()
			errs <- s.records.Flush(s.bounded())
		})
	}
	started.Wait()
	never(s.T(), func() bool { return len(s.records.queue) > 0 }, 200*time.Millisecond, 5*time.Millisecond,
		"the waiting flushes took room in the queue")

	later := s.row()
	s.records.Opened(later, nil, nil)
	s.Zero(s.records.Dropped(), "a row queued behind the waiting flushes was dropped")

	release()
	finished.Wait()
	close(errs)
	for err := range errs {
		s.Require().NoError(err)
	}
	s.Require().NoError(s.records.Flush(s.bounded()))
	s.True(s.stored(later.ID), "a row queued behind the waiting flushes is not stored")
}

func (s *RecordFlushSuite) TestFlushesWaitingTogetherDoNotWaitForRowsQueuedAfterThem() {
	releaseFirst := s.holdWriter()
	second, releaseSecond := s.heldRow()
	s.records.Opened(second, nil, nil)
	const callers = 64
	errs := make(chan error, callers)
	var started sync.WaitGroup
	for range callers {
		started.Add(1)
		go func() {
			started.Done()
			errs <- s.records.Flush(s.bounded())
		}()
	}
	started.Wait()
	releaseFirst()
	// The writer takes in every waiting Flush, then stalls on the second held row.
	s.Require().Eventually(func() bool { return len(s.records.queue) == 0 }, settleFor, 5*time.Millisecond,
		"the writer never took the second held row")
	third, _ := s.heldRow()
	s.records.Opened(third, nil, nil)

	releaseSecond()
	deadline := time.After(recordWriteTimeout / 2)
	for range callers {
		select {
		case err := <-errs:
			s.Require().NoError(err)
		case <-deadline:
			s.FailNow("a Flush waited for a row queued after the writer took it in")
		}
	}
}

func (s *RecordFlushSuite) TestAFlushIsNotHeldByALaterFlushWaitingOnARowQueuedAfterIt() {
	releaseFirst := s.holdWriter()
	second, releaseSecond := s.heldRow()
	s.records.Opened(second, nil, nil)
	first := make(chan error, 1)
	var started sync.WaitGroup
	started.Add(1)
	go func() {
		started.Done()
		first <- s.records.Flush(s.bounded())
	}()
	started.Wait()
	releaseFirst()
	// The writer takes the first Flush in, then stalls on the second held row.
	s.Require().Eventually(func() bool { return len(s.records.queue) == 0 }, settleFor, 5*time.Millisecond,
		"the writer never took the second held row")
	third, _ := s.heldRow()
	s.records.Opened(third, nil, nil)
	go func() { _ = s.records.Flush(s.bounded()) }()
	// Nothing shows that the later Flush has reached the writer; give it time to.
	time.Sleep(50 * time.Millisecond)

	releaseSecond()
	select {
	case err := <-first:
		s.Require().NoError(err)
	case <-time.After(recordWriteTimeout / 2):
		s.FailNow("a Flush waited for a row queued after it, behind a later Flush")
	}
}

func (s *RecordFlushSuite) TestFlushesWaitingForTheSameWritesShareOneEntry() {
	var waiting flushWaiters
	first := waiting.wait(5)
	for range recordQueueSize {
		s.Equal(first, waiting.wait(5))
	}
	second := waiting.wait(6)
	third := waiting.wait(7)
	s.Len(waiting, 3, "each Flush waiting for the same writes took an entry of its own")

	waiting.release(6)
	s.True(isClosed(first), "a Flush whose writes are all taken was not told")
	s.True(isClosed(second), "a Flush whose writes are all taken was not told")
	s.False(isClosed(third), "a Flush was told before its writes were all taken")
	waiting.release(7)
	s.True(isClosed(third), "a Flush whose writes are all taken was not told")
	s.Empty(waiting)
}

func (s *RecordFlushSuite) TestEachFlushWaitsForTheRowItsCallerQueuedBeforeIt() {
	release := s.holdWriter()
	const callers = 32
	missing := make(chan string, callers)
	var started, finished sync.WaitGroup
	for range callers {
		row := s.row()
		started.Add(1)
		finished.Go(func() {
			s.records.Opened(row, nil, nil)
			started.Done()
			if err := s.records.Flush(s.bounded()); err != nil {
				missing <- err.Error()
				return
			}
			if !s.stored(row.ID) {
				missing <- row.ID
			}
		})
	}
	// Every row is queued behind the held one before the writer moves, so each Flush
	// reaches it with rows still to write.
	started.Wait()
	release()
	finished.Wait()
	close(missing)
	var unwritten []string
	for id := range missing {
		unwritten = append(unwritten, id)
	}
	s.Empty(unwritten, "a Flush returned before the row queued ahead of it was written")
}

func (s *RecordFlushSuite) TestAFlushReturnsOnceTheItemsQueuedBeforeItAreWritten() {
	row := s.row()
	s.records.Opened(row, nil, nil)
	response := store.AgentResponse{ID: rand.Text(), SessionID: row.ID, CustomerID: s.customer, Said: "Where is my order?"}
	s.records.Responding(response)
	for ordinal, text := range []string{"Where is my order?", "It ships today.", "Thanks."} {
		s.records.Item(store.AgentResponseItem{ResponseID: response.ID, Ordinal: ordinal, SessionID: row.ID,
			Kind: store.ItemSaid, Text: text, At: time.Now().UTC()})
	}

	s.Require().NoError(s.records.Flush(s.bounded()))

	// Base 4f22418b8: all three, at once.
	items, err := s.store.ResponseItems.Select(s.ctx, s.customer, row.ID, store.ItemFilter{ResponseID: response.ID, Limit: 10})
	s.Require().NoError(err)
	s.Len(items, 3, "the items still batching in the writer are not written")
}

func (s *RecordFlushSuite) TestAFlushWaitingAsTheWriterClosesReturnsOnceItsRowIsWritten() {
	release := s.holdWriter()
	row := s.row()
	s.records.Opened(row, nil, nil)
	flushed := make(chan error, 1)
	go func() { flushed <- s.records.Flush(s.bounded()) }()
	closed := make(chan struct{})
	go func() {
		s.records.Close()
		close(closed)
	}()

	release()
	<-closed
	// Base 4f22418b8 answered nil with the row stored when the Flush reached the queue
	// before Close did, and panicked when it came after.
	s.Require().NoError(<-flushed)
	s.True(s.stored(row.ID), "the row queued before the Flush is not stored")
}

func (s *RecordFlushSuite) TestAFlushAfterTheWriterClosedReturnsAtOnce() {
	s.records.Close()

	// Base 4f22418b8 panicked here: the marker was sent on the closed queue.
	s.NoError(s.records.Flush(s.bounded()))
}

func (s *RecordFlushSuite) TestAFlushGivesUpWhenItsCallerDoes() {
	s.holdWriter()
	ctx, cancel := context.WithTimeout(s.ctx, 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	s.ErrorIs(s.records.Flush(ctx), context.DeadlineExceeded)
	// The writer itself gives up on the held row only after recordWriteTimeout.
	s.Less(time.Since(start), recordWriteTimeout/2, "the Flush waited for the writer rather than its caller")
}

func (s *RecordFlushSuite) TestAFlushTheWriterTookInGivesUpWhenItsCallerDoes() {
	releaseFirst := s.holdWriter()
	second, _ := s.heldRow()
	s.records.Opened(second, nil, nil)
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	flushed := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		flushed <- s.records.Flush(ctx)
	}()
	<-started
	releaseFirst()
	// The writer takes the Flush in before the second held row, and stalls on that row.
	s.Require().Eventually(func() bool { return len(s.records.queue) == 0 }, settleFor, 5*time.Millisecond,
		"the writer never took the second held row")

	cancel()
	select {
	case err := <-flushed:
		s.ErrorIs(err, context.Canceled)
	case <-time.After(recordWriteTimeout / 2):
		s.Fail("the Flush waited for the writer rather than its caller")
	}
}

func (s *RecordFlushSuite) TestATitleWrittenAfterTheSessionWasDeletedLogsNoError() {
	logged := &syncBuffer{}
	records := s.recorderLogging(s.store, logged)
	row := s.row()
	records.Opened(row, nil, nil)
	s.Require().NoError(records.Flush(s.bounded()))

	// The title is queued behind a write the writer is stuck on, as the one a model is
	// still composing is when the customer deletes the conversation.
	held, release := s.heldRow()
	records.Opened(held, nil, nil)
	records.Described(s.customer, row.ID, "Where an order went", "A question about an order.", nil)
	s.Require().NoError(s.store.Sessions.Delete(s.ctx, s.customer, row.ID))
	release()
	s.Require().NoError(records.Flush(s.bounded()))

	_, err := s.store.Sessions.Get(s.ctx, s.customer, row.ID)
	s.ErrorIs(err, store.ErrNoSession, "the title brought the deleted session back")
	s.NotContains(logged.String(), "level=ERROR", "a title for a deleted session is an expected drop")
	s.Contains(logged.String(), "level=DEBUG", "the drop is logged, at debug")
	s.Contains(logged.String(), "dropped the session's title", "the drop is logged, at debug")
	s.True(s.stored(held.ID), "the write behind the title was lost with it")
}

func (s *RecordFlushSuite) TestATitleForASessionThatIsThereIsStored() {
	logged := &syncBuffer{}
	records := s.recorderLogging(s.store, logged)
	row := s.row()
	records.Opened(row, nil, nil)
	records.Described(s.customer, row.ID, "Where an order went", "A question about an order.", nil)
	s.Require().NoError(records.Flush(s.bounded()))

	got, err := s.store.Sessions.Get(s.ctx, s.customer, row.ID)
	s.Require().NoError(err)
	s.Equal("Where an order went", got.Title)
	s.NotContains(logged.String(), "level=ERROR")
}

func (s *RecordFlushSuite) TestAnotherCustomersCloseOfTheSameSessionIdLeavesItOpen() {
	row := s.row()
	s.records.Opened(row, nil, nil)

	s.records.Closed("other-"+s.customer, row.ID, row.OpenedAt, time.Now().UTC(), TimeoutHangup)
	s.Require().NoError(s.records.Flush(s.bounded()))

	got, err := s.store.Sessions.Get(s.ctx, s.customer, row.ID)
	s.Require().NoError(err)
	s.Nil(got.ClosedAt, "the session is still open")
	s.Equal(store.SessionRunning, got.State)
	s.Empty(got.EndReason)
}

func (s *RecordFlushSuite) TestACloseQueuedByTheRouterThatHeldASessionBeforeItWasReopenedLeavesItOpen() {
	row := s.row()
	s.records.Opened(row, nil, nil)
	s.Require().NoError(s.records.Flush(s.bounded()))
	// Another router carries the session on while the close of this one is still queued.
	reopening := newSessionRecorder(s.store, slog.New(slog.DiscardHandler))
	s.T().Cleanup(reopening.Close)
	reopened := row
	reopened.OpenedAt = row.OpenedAt.Add(time.Minute)
	reopening.CarriedOn(reopened, nil, nil)
	s.Require().NoError(reopening.Flush(s.bounded()))

	s.records.Closed(s.customer, row.ID, row.OpenedAt, time.Now().UTC(), TimeoutHangup)
	s.Require().NoError(s.records.Flush(s.bounded()))
	got, err := s.store.Sessions.Get(s.ctx, s.customer, row.ID)
	s.Require().NoError(err)
	s.Nil(got.ClosedAt, "the run that carried it on has not ended")

	reopening.Closed(s.customer, row.ID, reopened.OpenedAt, time.Now().UTC(), TimeoutHangup)
	s.Require().NoError(reopening.Flush(s.bounded()))
	got, err = s.store.Sessions.Get(s.ctx, s.customer, row.ID)
	s.Require().NoError(err)
	s.NotNil(got.ClosedAt, "the router running it closes it")
}

func (s *RecordFlushSuite) TestTheItemsOfATurnWhoseIdIsAnotherCustomersAreNotWritten() {
	other := "other-" + s.customer
	theirs := store.AgentSession{ID: rand.Text(), CustomerID: other, State: store.SessionRunning, CreatedAt: time.Now().UTC()}
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &theirs))
	turn := rand.Text()
	s.Require().NoError(s.store.Responses.Insert(s.ctx, &store.AgentResponse{ID: turn, SessionID: theirs.ID, CustomerID: other}))
	row := s.row()
	s.records.Opened(row, nil, nil)

	s.records.Responding(store.AgentResponse{ID: turn, SessionID: row.ID, CustomerID: s.customer})
	s.records.Item(store.AgentResponseItem{ResponseID: turn, SessionID: row.ID, Kind: "message", Text: "app's answer"})
	s.records.Responded(s.customer, turn, store.ResponseCompleted, "", time.Now().UTC())
	s.Require().NoError(s.records.Flush(s.bounded()))

	written, err := s.store.ResponseItems.Select(s.ctx, other, row.ID, store.ItemFilter{ResponseID: turn})
	s.Require().NoError(err)
	s.Empty(written, "no item was written under the other customer's turn")
}

func (s *RecordFlushSuite) TestASessionOpenedAgainOnceItsIDIsFreeKeepsItsTurns() {
	other := "other-" + s.customer
	row := s.row()
	theirs := store.AgentSession{ID: row.ID, CustomerID: other, State: store.SessionRunning, CreatedAt: time.Now().UTC()}
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &theirs))
	refused := make(chan struct{})
	s.records.Opened(row, nil, func() { close(refused) })
	s.records.Responding(store.AgentResponse{ID: rand.Text(), SessionID: row.ID, CustomerID: s.customer})
	s.records.Closed(s.customer, row.ID, row.OpenedAt, time.Now().UTC(), EndClosed)
	s.Require().NoError(s.records.Flush(s.bounded()))
	s.Require().Eventually(func() bool {
		select {
		case <-refused:
			return true
		default:
			return false
		}
	}, settleFor, 5*time.Millisecond, "the row taken by another customer was not refused")
	s.Require().NoError(s.store.Sessions.Delete(s.ctx, other, row.ID))

	s.records.Opened(row, nil, nil)
	turn := rand.Text()
	s.records.Responding(store.AgentResponse{ID: turn, SessionID: row.ID, CustomerID: s.customer})
	s.Require().NoError(s.records.Flush(s.bounded()))

	turns, err := s.store.Responses.Select(s.ctx, s.customer, row.ID, store.ResponseFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(turns, 1, "only the turn of the session opened once the id was free is kept")
	s.Equal(turn, turns[0].ID)
}

// The close of the refused session never reached the writer, as when a full queue drops it.
func (s *RecordFlushSuite) TestASessionOpenedAgainOnceItsIDIsFreeKeepsItsTurnsWithNoCloseBetween() {
	other := "other-" + s.customer
	row := s.row()
	theirs := store.AgentSession{ID: row.ID, CustomerID: other, State: store.SessionRunning, CreatedAt: time.Now().UTC()}
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &theirs))
	s.records.Opened(row, nil, nil)
	s.Require().NoError(s.records.Flush(s.bounded()))
	s.Require().NoError(s.store.Sessions.Delete(s.ctx, other, row.ID))

	s.records.Opened(row, nil, nil)
	turn := rand.Text()
	s.records.Responding(store.AgentResponse{ID: turn, SessionID: row.ID, CustomerID: s.customer})
	s.Require().NoError(s.records.Flush(s.bounded()))

	turns, err := s.store.Responses.Select(s.ctx, s.customer, row.ID, store.ResponseFilter{Limit: 10})
	s.Require().NoError(err)
	s.Require().Len(turns, 1, "the session opened once the id was free keeps its turn")
	s.Equal(turn, turns[0].ID)
}

func (s *RecordFlushSuite) TestATitleTheStoreCouldNotWriteStillLogsAnError() {
	logged := &syncBuffer{}
	broken, err := store.Open(s.dsn)
	s.Require().NoError(err)
	s.Require().NoError(broken.Close())
	records := s.recorderLogging(broken, logged)
	records.Described(s.customer, rand.Text(), "Where an order went", "A question about an order.", nil)
	s.Require().NoError(records.Flush(s.bounded()))

	s.Contains(logged.String(), "level=ERROR")
	s.Contains(logged.String(), "could not record the session's title")
}

func (s *RecordFlushSuite) TestASessionWriteCutOffByADroppedConnectionIsStoredWithWhatFollowsIt() {
	held, release := s.heldRow()
	response := store.AgentResponse{ID: rand.Text(), SessionID: held.ID, CustomerID: s.customer, Said: "Where is my order?"}
	s.records.Opened(held, nil, nil)
	s.records.Responding(response)
	s.records.Item(store.AgentResponseItem{ResponseID: response.ID, Ordinal: 0, SessionID: held.ID,
		Kind: store.ItemSaid, Text: "Where is my order?", At: time.Now().UTC()})

	// The writer's session insert waits on the held row's lock; its connection is cut there,
	// mid-statement, as a failover cuts it.
	var writer int
	s.Require().Eventually(func() bool {
		err := s.store.GlobalDB().QueryRowContext(s.ctx, `SELECT pid FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%agent_sessions%'`).Scan(&writer)
		return err == nil
	}, settleFor, 5*time.Millisecond, "the writer never waited on the held row")
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "SELECT pg_terminate_backend(?)", writer)
	s.Require().NoError(err)
	release()
	s.Require().NoError(s.records.Flush(s.bounded()))

	s.True(s.stored(held.ID), "the session is not stored")
	got, err := s.store.Responses.Get(s.ctx, s.customer, held.ID, response.ID)
	s.Require().NoError(err, "the turn is not stored")
	s.Equal(held.ID, got.SessionID)
	items, err := s.store.ResponseItems.Select(s.ctx, s.customer, held.ID, store.ItemFilter{ResponseID: response.ID, Limit: 10})
	s.Require().NoError(err)
	s.Len(items, 1, "the turn's item is not stored")
}

func (s *RecordFlushSuite) TestAnItemWhoseResponseIsMissingDoesNotStopAnotherSessionsItems() {
	kept, orphaned := s.row(), s.row()
	s.records.Opened(kept, nil, nil)
	s.records.Opened(orphaned, nil, nil)
	response := store.AgentResponse{ID: rand.Text(), SessionID: kept.ID, CustomerID: s.customer, Said: "Where is my order?"}
	s.records.Responding(response)
	lost := testutil.ToFloat64(recordsLost.WithLabelValues(lostItems))

	// Back to back, so all go in one batch: the missing turn has two items.
	missing := rand.Text()
	s.records.Item(store.AgentResponseItem{ResponseID: response.ID, Ordinal: 0, SessionID: kept.ID,
		Kind: store.ItemSaid, Text: "It ships today.", At: time.Now().UTC()})
	s.records.Item(store.AgentResponseItem{ResponseID: missing, Ordinal: 0, SessionID: orphaned.ID,
		Kind: store.ItemSaid, Text: "Thanks.", At: time.Now().UTC()})
	s.records.Item(store.AgentResponseItem{ResponseID: missing, Ordinal: 1, SessionID: orphaned.ID,
		Kind: store.ItemSaid, Text: "You're welcome.", At: time.Now().UTC()})
	s.Require().NoError(s.records.Flush(s.bounded()))

	items, err := s.store.ResponseItems.Select(s.ctx, s.customer, kept.ID, store.ItemFilter{ResponseID: response.ID, Limit: 10})
	s.Require().NoError(err)
	s.Len(items, 1, "the item of a session whose turn is stored was lost with the other session's")
	s.InDelta(lost+2, testutil.ToFloat64(recordsLost.WithLabelValues(lostItems)), 0,
		"each dropped item is not counted, one by one")
}

func (s *RecordFlushSuite) TestAnItemWhoseResponseIsMissingDoesNotStopItsSessionsStoredTurn() {
	session := s.row()
	s.records.Opened(session, nil, nil)
	stored := store.AgentResponse{ID: rand.Text(), SessionID: session.ID, CustomerID: s.customer, Said: "Can I change the address?"}
	s.records.Responding(stored)

	// Back to back, so both go in one batch: one item of the stored turn, one of a turn whose
	// row was never written, in the same session.
	s.records.Item(store.AgentResponseItem{ResponseID: stored.ID, Ordinal: 0, SessionID: session.ID,
		Kind: store.ItemSaid, Text: "Yes, until it ships.", At: time.Now().UTC()})
	s.records.Item(store.AgentResponseItem{ResponseID: rand.Text(), Ordinal: 0, SessionID: session.ID,
		Kind: store.ItemSaid, Text: "Great, thanks.", At: time.Now().UTC()})
	s.Require().NoError(s.records.Flush(s.bounded()))

	items, err := s.store.ResponseItems.Select(s.ctx, s.customer, session.ID, store.ItemFilter{ResponseID: stored.ID, Limit: 10})
	s.Require().NoError(err)
	s.Len(items, 1, "the stored turn's item was dropped with its session's missing turn")
}

func (s *RecordFlushSuite) TestEndsThatCannotBeStoredAreCountedOnceTheRetriesRunOut() {
	// Nothing listens on port 1, so every try is refused, the way a database that stays
	// down refuses it.
	down, err := store.Open("postgres://postgres:postgres@127.0.0.1:1/router?sslmode=disable")
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(down.Close()) })
	records := s.recorderLogging(down, &syncBuffer{})
	sessionsLost := testutil.ToFloat64(recordsLost.WithLabelValues(lostSessionEnd))
	callsLost := testutil.ToFloat64(recordsLost.WithLabelValues(lostCallEnd))

	start := time.Now()
	records.Closed(s.customer, rand.Text(), time.Now().UTC(), time.Now().UTC(), TimeoutHangup)
	// Refused before it reaches the database, which says nothing about whether it is back.
	records.Closed("", rand.Text(), time.Now().UTC(), time.Now().UTC(), TimeoutHangup)
	(&callRecorder{records: records}).Ended(s.customer, rand.Text(), time.Now().UTC())
	// Bounded well past one retry budget, so a slow runner is not read as a hang.
	ctx, cancel := context.WithTimeout(s.ctx, 3*recordRetryFor)
	defer cancel()
	s.Require().NoError(records.Flush(ctx))

	// The first end is retried until its budget is all but spent, the last wait being at
	// most recordRetryMost; tried once, it would be refused within milliseconds. Once it ran
	// out, the next is tried once: one budget in all.
	took := time.Since(start)
	s.GreaterOrEqual(took, recordRetryFor-recordRetryMost, "the session end was not tried again")
	s.Less(took, recordRetryFor*3/2, "the writer retried each write for a budget of its own")
	s.InDelta(sessionsLost+2, testutil.ToFloat64(recordsLost.WithLabelValues(lostSessionEnd)), 0,
		"the session ends that were never stored are not counted")
	s.InDelta(callsLost+1, testutil.ToFloat64(recordsLost.WithLabelValues(lostCallEnd)), 0,
		"the call end that was never stored is not counted")
}

// cutRows are the rows a write of every kind changes, stored before it is queued.
type cutRows struct {
	session  store.AgentSession
	response store.AgentResponse
	call     store.Call
}

func (s *RecordFlushSuite) TestEveryWriteCutOffByADroppedConnectionIsStored() {
	tool := []store.ToolDefinition{{Name: "track_order", Description: "Where an order is."}}
	for _, write := range []struct {
		name string
		// held are the tables whose rows are locked while the write is queued: all four
		// unless it names some.
		held []string
		// prepare, when set, readies the rows before they are held.
		prepare func(cutRows)
		queue   func(*sessionRecorder, cutRows)
		check   func(cutRows)
	}{{
		name: "a reopened session's tools",
		held: []string{"agent_session_tools"},
		queue: func(r *sessionRecorder, rows cutRows) {
			rows.session.Title = "Reopened"
			r.Opened(rows.session, tool, nil)
		},
		check: func(rows cutRows) { s.Equal(tool, s.toolsOf(rows.session.ID)) },
	}, {
		name:  "the tools a session is offered",
		queue: func(r *sessionRecorder, rows cutRows) { r.Offered(s.customer, rows.session.ID, tool) },
		check: func(rows cutRows) { s.Equal(tool, s.toolsOf(rows.session.ID)) },
	}, {
		name: "a session ending",
		queue: func(r *sessionRecorder, rows cutRows) {
			r.Closed(s.customer, rows.session.ID, rows.session.OpenedAt, time.Now().UTC(), TimeoutHangup)
		},
		check: func(rows cutRows) { s.NotNil(s.sessionOf(rows.session.ID).ClosedAt) },
	}, {
		name: "a turn starting",
		queue: func(r *sessionRecorder, rows cutRows) {
			r.Responding(store.AgentResponse{ID: "next-" + rows.response.ID, SessionID: rows.session.ID, CustomerID: s.customer})
		},
		check: func(rows cutRows) {
			_, err := s.store.Responses.Get(s.ctx, s.customer, rows.session.ID, "next-"+rows.response.ID)
			s.NoError(err)
		},
	}, {
		name: "a turn ending",
		queue: func(r *sessionRecorder, rows cutRows) {
			r.Responded(s.customer, rows.response.ID, store.ResponseCompleted, "", time.Now().UTC())
		},
		check: func(rows cutRows) {
			got, err := s.store.Responses.Get(s.ctx, s.customer, rows.session.ID, rows.response.ID)
			s.Require().NoError(err)
			s.NotNil(got.FinishedAt)
		},
	}, {
		name: "what a turn did",
		queue: func(r *sessionRecorder, rows cutRows) {
			r.Item(store.AgentResponseItem{ResponseID: rows.response.ID, SessionID: rows.session.ID,
				Kind: store.ItemSaid, Text: "It ships today.", At: time.Now().UTC()})
		},
		check: func(rows cutRows) {
			items, err := s.store.ResponseItems.Select(s.ctx, s.customer, rows.session.ID,
				store.ItemFilter{ResponseID: rows.response.ID, Limit: 10})
			s.Require().NoError(err)
			s.Len(items, 1)
		},
	}, {
		name: "a title",
		queue: func(r *sessionRecorder, rows cutRows) {
			r.Described(s.customer, rows.session.ID, "Where an order went", "", nil)
		},
		check: func(rows cutRows) { s.Equal("Where an order went", s.sessionOf(rows.session.ID).Title) },
	}, {
		name:  "a connection a login chose",
		queue: func(r *sessionRecorder, rows cutRows) { r.Chose(s.customer, rows.session.ID, "github", "connection-1") },
		check: func(rows cutRows) {
			s.Equal([]store.SessionConnectorSelection{{Name: "github", ConnectionID: "connection-1"}},
				s.sessionOf(rows.session.ID).ConnectorSelections)
		},
	}, {
		name: "a tool timeout",
		queue: func(r *sessionRecorder, rows cutRows) {
			r.ToolTimeout(s.customer, rows.session.ID, rows.session.CallerKind, 30_000)
		},
		check: func(rows cutRows) { s.Equal(30_000, s.sessionOf(rows.session.ID).ToolTimeoutMs) },
	}, {
		name:  "video seen",
		queue: func(r *sessionRecorder, rows cutRows) { r.SawVideo(s.customer, rows.session.ID) },
		check: func(rows cutRows) { s.Equal(store.ModalityVideo, s.sessionOf(rows.session.ID).Modality) },
	}, {
		name:  "voice started",
		queue: func(r *sessionRecorder, rows cutRows) { r.Voiced(s.customer, rows.session.ID) },
		check: func(rows cutRows) { s.Equal(store.ModalityVoice, s.sessionOf(rows.session.ID).Modality) },
	}, {
		name: "a call starting again",
		prepare: func(rows cutRows) {
			s.Require().NoError(s.store.Calls.Finish(s.ctx, s.customer, rows.call.ID, time.Now().UTC()))
		},
		queue: func(r *sessionRecorder, rows cutRows) { (&callRecorder{records: r}).Started(rows.call) },
		check: func(rows cutRows) { s.Nil(s.callOf(rows.call.ID).EndedAt) },
	}, {
		name: "a call changing models",
		queue: func(r *sessionRecorder, rows cutRows) {
			rows.call.STT = "deepgram/nova-3"
			(&callRecorder{records: r}).Changed(rows.call)
		},
		check: func(rows cutRows) { s.Equal("deepgram/nova-3", s.callOf(rows.call.ID).STT) },
	}, {
		name: "a call ending",
		queue: func(r *sessionRecorder, rows cutRows) {
			(&callRecorder{records: r}).Ended(s.customer, rows.call.ID, time.Now().UTC())
		},
		check: func(rows cutRows) { s.NotNil(s.callOf(rows.call.ID).EndedAt) },
	}} {
		s.Run(write.name, func() {
			rows := s.cutRows()
			if write.prepare != nil {
				write.prepare(rows)
			}
			held := write.held
			if held == nil {
				held = []string{"agent_sessions", "agent_session_tools", "agent_responses", "calls"}
			}
			release := s.holdRows(rows, held)
			records := newSessionRecorder(s.store, slog.New(slog.DiscardHandler))
			defer records.Close()
			write.queue(records, rows)

			s.cutWaitingWriter()
			release()
			s.Require().NoError(records.Flush(s.bounded()))
			write.check(rows)
		})
	}
}

func (s *RecordFlushSuite) TestAnEndQueuedWhileTheDatabaseIsBrieflyDownAfterAnOutageIsStored() {
	proxy := s.proxy()
	db, err := store.Open(proxy.dsn)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(db.Close()) })
	records := s.recorderLogging(db, &syncBuffer{})
	row := s.row()
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &row))
	long := func() context.Context {
		ctx, cancel := context.WithTimeout(s.ctx, 3*recordRetryFor)
		s.T().Cleanup(cancel)
		return ctx
	}

	// An outage longer than a budget: the writer gives up and tries the next write once.
	proxy.setDown(true)
	records.Voiced(s.customer, row.ID)
	s.Require().NoError(records.Flush(long()))
	// The database is back, and a write gets through.
	proxy.setDown(false)
	records.SawVideo(s.customer, row.ID)
	s.Require().NoError(records.Flush(s.bounded()))
	s.Equal(store.ModalityVideo, s.sessionOf(row.ID).Modality, "a write after the outage is not stored")

	// A blip: down when the end is first tried, up again for its retry.
	proxy.setDown(true)
	refused := proxy.refused.Load()
	records.Closed(s.customer, row.ID, row.OpenedAt, time.Now().UTC(), TimeoutHangup)
	s.Require().Eventually(func() bool { return proxy.refused.Load() > refused }, settleFor, time.Millisecond,
		"the end was never tried while the database was down")
	proxy.setDown(false)
	s.Require().NoError(records.Flush(s.bounded()))
	s.NotNil(s.sessionOf(row.ID).ClosedAt, "the writer still tried each write once after one got through")
}

func (s *RecordFlushSuite) TestEndsAFullQueueDropsAreCounted() {
	release := s.holdWriter()
	defer release()
	for range recordQueueSize {
		s.records.Voiced(s.customer, rand.Text())
	}
	sessionsLost := testutil.ToFloat64(recordsLost.WithLabelValues(lostSessionEnd))
	callsLost := testutil.ToFloat64(recordsLost.WithLabelValues(lostCallEnd))

	s.records.Closed(s.customer, rand.Text(), time.Now().UTC(), time.Now().UTC(), TimeoutHangup)
	(&callRecorder{records: s.records}).Ended(s.customer, rand.Text(), time.Now().UTC())

	s.Equal(int64(2), s.records.Dropped(), "the queue was not full")
	s.InDelta(sessionsLost+1, testutil.ToFloat64(recordsLost.WithLabelValues(lostSessionEnd)), 0,
		"a session end the full queue dropped is not counted")
	s.InDelta(callsLost+1, testutil.ToFloat64(recordsLost.WithLabelValues(lostCallEnd)), 0,
		"a call end the full queue dropped is not counted")
}

func (s *RecordFlushSuite) TestEveryWriteAFullQueueDropsIsCounted() {
	release := s.holdWriter()
	defer release()
	for range recordQueueSize {
		s.records.Voiced(s.customer, rand.Text())
	}
	dropped := s.droppedTotal()

	opened := s.row()
	s.records.Opened(opened, nil, nil)
	s.records.Responding(store.AgentResponse{ID: rand.Text(), SessionID: opened.ID, CustomerID: s.customer})
	s.records.Item(store.AgentResponseItem{ResponseID: rand.Text(), SessionID: opened.ID,
		Kind: store.ItemSaid, Text: "Where is my order?", At: time.Now().UTC()})
	s.records.Closed(s.customer, opened.ID, opened.OpenedAt, time.Now().UTC(), TimeoutHangup)

	s.Equal(int64(4), s.records.Dropped(), "the queue was not full")
	s.InDelta(dropped+4, s.droppedTotal(), 0, "a write the full queue dropped is not counted")
}

// droppedTotal reads the counter of writes a full queue dropped, as a scrape does, 0 before
// it exists.
func (s *RecordFlushSuite) droppedTotal() float64 {
	families, err := prometheus.DefaultGatherer.Gather()
	s.Require().NoError(err)
	for _, family := range families {
		if family.GetName() == "router_session_records_dropped_total" {
			return family.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}

func (s *RecordFlushSuite) TestTheItemsOfATurnWhoseRowIsMissingDoNotFailEveryLaterBatch() {
	logged := &syncBuffer{}
	records := s.recorderLogging(s.store, logged)
	session := s.row()
	records.Opened(session, nil, nil)
	stored := store.AgentResponse{ID: rand.Text(), SessionID: session.ID, CustomerID: s.customer, Said: "Where is my order?"}
	records.Responding(stored)
	missing := rand.Text()
	lost := testutil.ToFloat64(recordsLost.WithLabelValues(lostItems))

	// A batch per Flush, each holding an item of the stored turn and one of the turn whose row
	// was never written.
	batch := func(ordinal int, said string) {
		records.Item(store.AgentResponseItem{ResponseID: stored.ID, Ordinal: ordinal, SessionID: session.ID,
			Kind: store.ItemSaid, Text: said, At: time.Now().UTC()})
		records.Item(store.AgentResponseItem{ResponseID: missing, Ordinal: ordinal, SessionID: session.ID,
			Kind: store.ItemSaid, Text: said, At: time.Now().UTC()})
		s.Require().NoError(records.Flush(s.bounded()))
	}
	batch(0, "It ships today.")
	batch(1, "It arrives on Friday.")

	items, err := s.store.ResponseItems.Select(s.ctx, s.customer, session.ID, store.ItemFilter{ResponseID: stored.ID, Limit: 10})
	s.Require().NoError(err)
	s.Len(items, 2, "an item of the stored turn was dropped")
	s.Equal(1, strings.Count(logged.String(), "its response was never recorded"),
		"a later batch failed again on the turn already found missing")
	s.InDelta(lost+2, testutil.ToFloat64(recordsLost.WithLabelValues(lostItems)), 0,
		"each item of the missing turn is not counted, one by one")

	// Once the turn ended its id is tried again, as any other turn's is.
	records.Responded(s.customer, missing, store.ResponseCompleted, "", time.Now().UTC())
	batch(2, "Anything else?")
	s.Equal(2, strings.Count(logged.String(), "its response was never recorded"),
		"the turn found missing is still kept after it ended")
}

// cutRows stores a running text session offered no tools yet, a turn of it and a call of it.
func (s *RecordFlushSuite) cutRows() cutRows {
	rows := cutRows{session: s.row()}
	rows.session.CallerKind = "server"
	rows.session.Modality = store.ModalityText
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &rows.session))
	s.Require().NoError(s.store.SessionTools.Upsert(s.ctx, s.customer, rows.session.ID, nil))
	rows.response = store.AgentResponse{ID: rand.Text(), SessionID: rows.session.ID, CustomerID: s.customer}
	s.Require().NoError(s.store.Responses.Insert(s.ctx, &rows.response))
	rows.call = store.Call{ID: rand.Text(), CustomerID: s.customer, SessionID: rows.session.ID}
	s.Require().NoError(s.store.Calls.Start(s.ctx, &rows.call))
	return rows
}

// holdRows locks the rows of tables in another transaction, until the returned release
// rolls it back.
func (s *RecordFlushSuite) holdRows(rows cutRows, tables []string) (release func()) {
	tx, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	release = sync.OnceFunc(func() { s.NoError(tx.Rollback()) })
	s.T().Cleanup(release)
	keys := map[string]string{"agent_sessions": "id", "agent_session_tools": "session_id", "agent_responses": "id", "calls": "id"}
	ids := map[string]string{"agent_sessions": rows.session.ID, "agent_session_tools": rows.session.ID,
		"agent_responses": rows.response.ID, "calls": rows.call.ID}
	for _, table := range tables {
		var one int
		s.Require().NoError(tx.QueryRowContext(s.ctx, "SELECT 1 FROM "+table+" WHERE "+keys[table]+" = ? FOR UPDATE",
			ids[table]).Scan(&one), table)
	}
	return release
}

// cutWaitingWriter terminates the backend waiting on a lock, as a failover cuts the
// writer's connection mid-statement.
func (s *RecordFlushSuite) cutWaitingWriter() {
	var writer int
	s.Require().Eventually(func() bool {
		err := s.store.GlobalDB().QueryRowContext(s.ctx, `SELECT pid FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND pid <> pg_backend_pid()`).Scan(&writer)
		return err == nil
	}, settleFor, 5*time.Millisecond, "the writer never waited on the held rows")
	_, err := s.store.GlobalDB().ExecContext(s.ctx, "SELECT pg_terminate_backend(?)", writer)
	s.Require().NoError(err)
}

func (s *RecordFlushSuite) sessionOf(id string) store.AgentSession {
	got, err := s.store.Sessions.Get(s.ctx, s.customer, id)
	s.Require().NoError(err)
	return got
}

func (s *RecordFlushSuite) toolsOf(id string) []store.ToolDefinition {
	got, err := s.store.SessionTools.Get(s.ctx, s.customer, id)
	s.Require().NoError(err)
	return got
}

func (s *RecordFlushSuite) callOf(id string) store.Call {
	got, err := s.store.Calls.Get(s.ctx, s.customer, id)
	s.Require().NoError(err)
	return got
}

// flakyProxy passes connections through to the suite's Postgres. While down it cuts the
// connections it holds and closes each new one at once, counting them in refused.
type flakyProxy struct {
	dsn     string
	target  string
	refused atomic.Int64

	mu    sync.Mutex
	down  bool
	pipes []net.Conn
}

func (s *RecordFlushSuite) proxy() *flakyProxy {
	address, err := url.Parse(s.dsn)
	s.Require().NoError(err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	proxy := &flakyProxy{target: address.Host}
	address.Host = listener.Addr().String()
	proxy.dsn = address.String()
	var serving sync.WaitGroup
	s.T().Cleanup(func() {
		s.NoError(listener.Close())
		proxy.setDown(true)
		serving.Wait()
	})
	serving.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			proxy.mu.Lock()
			if proxy.down {
				proxy.mu.Unlock()
				proxy.refused.Add(1)
				_ = conn.Close()
				continue
			}
			upstream, err := net.Dial("tcp", proxy.target)
			if err != nil {
				proxy.mu.Unlock()
				_ = conn.Close()
				continue
			}
			proxy.pipes = append(proxy.pipes, conn, upstream)
			proxy.mu.Unlock()
			serving.Go(func() { _, _ = io.Copy(upstream, conn); _ = upstream.Close() })
			serving.Go(func() { _, _ = io.Copy(conn, upstream); _ = conn.Close() })
		}
	})
	return proxy
}

func (p *flakyProxy) setDown(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		for _, conn := range p.pipes {
			_ = conn.Close()
		}
		p.pipes = nil
	}
}

// recorderLogging is a writer over db that logs everything, even debug lines, into logged.
func (s *RecordFlushSuite) recorderLogging(db *store.Store, logged *syncBuffer) *sessionRecorder {
	records := newSessionRecorder(db, slog.New(slog.NewTextHandler(logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	s.T().Cleanup(records.Close)
	return records
}

// syncBuffer is what the writer's goroutine logs into and the test reads back.
type syncBuffer struct {
	mu sync.Mutex
	sb strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sb.String()
}

// holdWriter stalls the writer on a session row another transaction is inserting, until the
// returned release rolls that transaction back. The writer gives up on its own after
// recordWriteTimeout, so a test releases well before that.
func (s *RecordFlushSuite) holdWriter() (release func()) {
	held, release := s.heldRow()
	s.records.Opened(held, nil, nil)
	s.Require().Eventually(func() bool { return len(s.records.queue) == 0 }, settleFor, 5*time.Millisecond,
		"the writer never took the held row")
	return release
}

// heldRow is a session row another transaction is inserting, so the writer stalls on it
// until the returned release rolls that transaction back, or the test ends.
func (s *RecordFlushSuite) heldRow() (store.AgentSession, func()) {
	held := s.row()
	tx, err := s.store.GlobalDB().BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	rollback := sync.OnceFunc(func() { s.NoError(tx.Rollback()) })
	s.T().Cleanup(rollback)
	_, err = tx.ExecContext(s.ctx, "INSERT INTO agent_sessions (id, customer_id, state) VALUES (?, ?, ?)",
		held.ID, held.CustomerID, store.SessionRunning)
	s.Require().NoError(err)
	return held, rollback
}

// bounded is a context a broken Flush cannot hang the suite past.
func (s *RecordFlushSuite) bounded() context.Context {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	s.T().Cleanup(cancel)
	return ctx
}

func (s *RecordFlushSuite) row() store.AgentSession {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return store.AgentSession{ID: rand.Text(), CustomerID: s.customer, State: store.SessionRunning, CreatedAt: now, OpenedAt: now}
}

// stored says whether the session's row is in Postgres. It is called from the callers'
// goroutines, so a failure to read is not stored rather than a failed check.
func (s *RecordFlushSuite) stored(id string) bool {
	rows, err := s.store.Sessions.Select(s.ctx, s.customer, store.SessionFilter{Limit: 100})
	if err != nil {
		return false
	}
	for _, row := range rows {
		if row.ID == id {
			return true
		}
	}
	return false
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
