//go:build integration

package session

import (
	"context"
	"crypto/rand"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

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
	s.records.Opened(later, nil)
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
	s.records.Opened(second, nil)
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
	s.records.Opened(third, nil)

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
	s.records.Opened(second, nil)
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
	s.records.Opened(third, nil)
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
			s.records.Opened(row, nil)
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
	s.records.Opened(row, nil)
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
	s.records.Opened(row, nil)
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
	s.records.Opened(second, nil)
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
	records.Opened(row, nil)
	s.Require().NoError(records.Flush(s.bounded()))

	// The title is queued behind a write the writer is stuck on, as the one a model is
	// still composing is when the customer deletes the conversation.
	held, release := s.heldRow()
	records.Opened(held, nil)
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
	records.Opened(row, nil)
	records.Described(s.customer, row.ID, "Where an order went", "A question about an order.", nil)
	s.Require().NoError(records.Flush(s.bounded()))

	got, err := s.store.Sessions.Get(s.ctx, s.customer, row.ID)
	s.Require().NoError(err)
	s.Equal("Where an order went", got.Title)
	s.NotContains(logged.String(), "level=ERROR")
}

func (s *RecordFlushSuite) TestAnotherCustomersCloseOfTheSameSessionIdLeavesItOpen() {
	row := s.row()
	s.records.Opened(row, nil)

	s.records.Closed("other-"+s.customer, row.ID, time.Now().UTC(), TimeoutHangup)
	s.Require().NoError(s.records.Flush(s.bounded()))

	got, err := s.store.Sessions.Get(s.ctx, s.customer, row.ID)
	s.Require().NoError(err)
	s.Nil(got.ClosedAt, "the session is still open")
	s.Equal(store.SessionRunning, got.State)
	s.Empty(got.EndReason)
}

func (s *RecordFlushSuite) TestTheItemsOfATurnWhoseIdIsAnotherCustomersAreNotWritten() {
	other := "other-" + s.customer
	theirs := store.AgentSession{ID: rand.Text(), CustomerID: other, State: store.SessionRunning, CreatedAt: time.Now().UTC()}
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &theirs))
	turn := rand.Text()
	s.Require().NoError(s.store.Responses.Insert(s.ctx, &store.AgentResponse{ID: turn, SessionID: theirs.ID, CustomerID: other}))
	row := s.row()
	s.records.Opened(row, nil)

	s.records.Responding(store.AgentResponse{ID: turn, SessionID: row.ID, CustomerID: s.customer})
	s.records.Item(store.AgentResponseItem{ResponseID: turn, SessionID: row.ID, Kind: "message", Text: "app's answer"})
	s.records.Responded(s.customer, turn, store.ResponseCompleted, "", time.Now().UTC())
	s.Require().NoError(s.records.Flush(s.bounded()))

	written, err := s.store.ResponseItems.Select(s.ctx, other, row.ID, store.ItemFilter{ResponseID: turn})
	s.Require().NoError(err)
	s.Empty(written, "no item was written under the other customer's turn")
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
	s.records.Opened(held, nil)
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
	s.records.Opened(kept, nil)
	s.records.Opened(orphaned, nil)
	response := store.AgentResponse{ID: rand.Text(), SessionID: kept.ID, CustomerID: s.customer, Said: "Where is my order?"}
	s.records.Responding(response)
	lost := testutil.ToFloat64(recordsLost.WithLabelValues(lostItems))

	// Back to back, so both go in one batch.
	s.records.Item(store.AgentResponseItem{ResponseID: response.ID, Ordinal: 0, SessionID: kept.ID,
		Kind: store.ItemSaid, Text: "It ships today.", At: time.Now().UTC()})
	s.records.Item(store.AgentResponseItem{ResponseID: rand.Text(), Ordinal: 0, SessionID: orphaned.ID,
		Kind: store.ItemSaid, Text: "Thanks.", At: time.Now().UTC()})
	s.Require().NoError(s.records.Flush(s.bounded()))

	items, err := s.store.ResponseItems.Select(s.ctx, s.customer, kept.ID, store.ItemFilter{ResponseID: response.ID, Limit: 10})
	s.Require().NoError(err)
	s.Len(items, 1, "the item of a session whose turn is stored was lost with the other session's")
	s.InDelta(lost+1, testutil.ToFloat64(recordsLost.WithLabelValues(lostItems)), 0, "the dropped item is not counted")
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
	records.Closed(s.customer, rand.Text(), time.Now().UTC(), TimeoutHangup)
	(&callRecorder{records: records}).Ended(s.customer, rand.Text(), time.Now().UTC())
	s.Require().NoError(records.Flush(s.bounded()))

	// One budget in all: once a write ran out of retries, the next is tried once.
	s.Less(time.Since(start), recordRetryFor*3/2, "the writer retried each write for a budget of its own")
	s.InDelta(sessionsLost+1, testutil.ToFloat64(recordsLost.WithLabelValues(lostSessionEnd)), 0,
		"the session end that was never stored is not counted")
	s.InDelta(callsLost+1, testutil.ToFloat64(recordsLost.WithLabelValues(lostCallEnd)), 0,
		"the call end that was never stored is not counted")
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
	s.records.Opened(held, nil)
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
	return store.AgentSession{ID: rand.Text(), CustomerID: s.customer, State: store.SessionRunning, CreatedAt: time.Now().UTC()}
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
