package session

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/usage"
)

// recordQueueSize bounds how far the session writer may fall behind before rows are
// dropped.
const recordQueueSize = 1024

// recordWriteTimeout bounds a single write so a stuck database cannot wedge the writer.
const recordWriteTimeout = 5 * time.Second

// recordRetryFor bounds how long one write is tried again after a failure that may pass, so
// a database that stays down holds the single writer, and the rows queued behind it, no
// longer than a failover takes. recordRetryFirst is the wait before the first retry, doubled
// each time up to recordRetryMost.
const (
	recordRetryFor   = 10 * time.Second
	recordRetryFirst = 50 * time.Millisecond
	recordRetryMost  = time.Second
)

// The writes recordsLost counts, a fixed set.
const (
	lostSessionEnd = "session_ended"
	lostCallEnd    = "call_ended"
	lostItems      = "items"
)

var recordsLost = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "router",
	Name:      "session_records_lost_total",
	Help: "Session records the writer could not store after retrying, by write: session_ended and call_ended " +
		"(the row stays open), and items (dropped because their session or response row was never stored)",
}, []string{"write"})

var recordsDropped = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "router",
	Name:      "session_records_dropped_total",
	Help:      "Session records of every kind dropped because the writer's queue was full",
})

// The series exist from the start, so a rate over the first loss reads it.
func init() {
	for _, write := range []string{lostSessionEnd, lostCallEnd, lostItems} {
		recordsLost.WithLabelValues(write)
	}
}

// itemFlushEvery is how long a partial batch of items waits for company.
//
// Items arrive in bursts -- a question, then nothing while a tool runs, then several at once
// -- so batching only on a full buffer would leave the last few items of a quiet turn
// unwritten until the next turn happened. Short enough that a reader refreshing the page
// sees the turn they just watched.
const itemFlushEvery = 250 * time.Millisecond

// itemBatchSize is how many items are written in one statement.
const itemBatchSize = 64

// recordWrite is one thing that happened to a session that Postgres should hear about.
//
// A single queue rather than one per kind, because the order matters and separate queues
// would race: a response row written before the session row it points at violates the
// foreign key, and items written before their response do the same.
type recordWrite struct {
	// customerID is whose session the write is to: an id alone may be another customer's.
	customerID string
	// session is a session that opened or was renamed, nil otherwise, and tools what its
	// model is offered. carriedOn says it was carried on from a row it had. refused is called
	// if the store refuses its row because another customer's session has its id.
	session   *store.AgentSession
	tools     []store.ToolDefinition
	carriedOn bool
	refused   func()
	// offered is the session whose model is now offered tools.
	offered string
	// closed is the session that ended, opened when the router closing it opened it, closedAt
	// when it ended and endReason why.
	closed    string
	opened    time.Time
	closedAt  time.Time
	endReason string
	// response is a turn that began, nil otherwise.
	response *store.AgentResponse
	// finished is the response that ended, with how it ended.
	finished   string
	status     string
	failure    string
	finishedAt time.Time
	// items are things that happened during a turn.
	items []store.AgentResponseItem
	// described is a session given a new title and description, nil otherwise.
	described *described
	// sawVideo is the session whose user's video the agent saw.
	sawVideo string
	// voiced is the session voice was started on.
	voiced string
	// callStarted is a call that began and callChanged one that moved onto other models,
	// nil otherwise. callEnded is the call that ended, callEndedAt when.
	callStarted *store.Call
	callChanged *store.Call
	callEnded   string
	callEndedAt time.Time
	// chose is a connection a login in the conversation chose for a binding, nil otherwise.
	chose *chose
	// timed is a running session given a new tool_timeout_ms, nil otherwise.
	timed *timed
}

// recorder is what a session needs of the writer behind it, which is less than the writer
// offers: starting and stopping it belongs to the manager that owns it. Narrowing it here
// also says plainly what a session may record, which is the list a reader of the event
// mapping wants.
type recorder interface {
	// Responding says a turn began.
	Responding(row store.AgentResponse)
	// Responded says how it ended.
	Responded(customerID, id, status, failure string, at time.Time)
	// Item says one thing that happened during it.
	Item(item store.AgentResponseItem)
	// Described says the session was renamed. A nil custom leaves the labels as they were.
	Described(customerID, id, title, description string, custom map[string]any)
	// SawVideo says the agent saw the user's video.
	SawVideo(customerID, id string)
	// Voiced says voice was started on the session.
	Voiced(customerID, id string)
	// Chose says a login in the conversation chose a connection for the binding called name.
	Chose(customerID, id, name, connectionID string)
	// ToolTimeout says the session was given a new tool_timeout_ms while it ran, by a caller
	// of kind.
	ToolTimeout(customerID, id, kind string, milliseconds int)
	// Offered says the session's model is now offered tools, after its caller's changed.
	Offered(customerID, id string, tools []store.ToolDefinition)
	// Flush waits until everything said so far has been written, which is what reading the
	// conversation back straight after it happened needs.
	Flush(ctx context.Context) error
}

// sessionRecords is a recorder for every session a manager opens, which also hears each
// session open and close.
type sessionRecords interface {
	recorder
	// Opened says a session started. refused is called, on the writer, if its id turns out to
	// be another customer's session's.
	Opened(row store.AgentSession, tools []store.ToolDefinition, refused func())
	// CarriedOn says a session was carried on from a row it had; refused is as for Opened.
	CarriedOn(row store.AgentSession, tools []store.ToolDefinition, refused func())
	Closed(customerID, id string, opened, at time.Time, reason EndReason)
	Close()
}

// sessionRecorder writes sessions, their calls, turns and items to Postgres off the
// conversation's path.
//
// A caller must never wait on a database to be answered, and a conversation must not fail to happen because the row
// recording it could not be written. So recording is asynchronous and rows are what gets
// dropped when the writer cannot keep up.
//
// An incognito session never reaches this writer at all. That is deliberate: a flag checked
// here would be one place, and one place that forgot to check it would be a conversation
// kept against its caller's wishes. The session simply never hands anything over.
type sessionRecorder struct {
	store  *store.Store
	logger *slog.Logger

	queue chan recordWrite
	// flushes takes each Flush to the writer beside the queue rather than in it: a reader
	// waiting on a slow writer must not take the room the rows need, or enough of them at
	// once would get every later row dropped. The writer answers each with the channel it
	// closes once the Flush may return.
	flushes chan chan (<-chan struct{})
	done    chan struct{}

	closeOnce sync.Once
	dropped   atomic.Int64
	// refused are the turns, by response id, whose row the store refused because the id is
	// another customer's: their items are not written under it. Kept by the writer alone, and
	// each until its turn ends.
	refused map[string]struct{}
	// failing is set, by the writer alone, once a write ran out of retries, and cleared by the
	// next that succeeds. Meanwhile each write is tried once: a database that stays down
	// would otherwise hold every queued write, and Close behind them, for a whole budget each.
	failing bool
	// taken are the sessions, by customer and id, whose row the store refused because the id
	// is another customer's: their turns and calls are not written under it. Kept by the
	// writer alone, and each until the session closes.
	taken map[[2]string]struct{}
	// missing are the turns, by response id, whose items the store refused because the turn's
	// row is not there: their later items are dropped as they arrive, rather than failing every
	// batch they are in. Kept by the writer alone, and each until its turn ends.
	missing map[string]struct{}
}

func newSessionRecorder(pgStore *store.Store, logger *slog.Logger) *sessionRecorder {
	r := &sessionRecorder{
		store:   pgStore,
		logger:  logger,
		queue:   make(chan recordWrite, recordQueueSize),
		flushes: make(chan chan (<-chan struct{})),
		done:    make(chan struct{}),
		refused: map[string]struct{}{},
		taken:   map[[2]string]struct{}{},
		missing: map[string]struct{}{},
	}
	go r.run()
	return r
}

// Opened queues the row for a session that has just started.
func (r *sessionRecorder) Opened(row store.AgentSession, tools []store.ToolDefinition, refused func()) {
	r.queueWrite(recordWrite{session: &row, tools: tools, refused: refused})
}

// CarriedOn queues the row of a session carried on from a row it had, maybe in another shard,
// which it supersedes once written.
func (r *sessionRecorder) CarriedOn(row store.AgentSession, tools []store.ToolDefinition, refused func()) {
	r.queueWrite(recordWrite{session: &row, tools: tools, carriedOn: true, refused: refused})
}

// Offered queues what a session's model is now offered.
func (r *sessionRecorder) Offered(customerID, id string, tools []store.ToolDefinition) {
	r.queueWrite(recordWrite{customerID: customerID, offered: id, tools: tools})
}

// Closed queues the time a session this router opened at opened ended.
func (r *sessionRecorder) Closed(customerID, id string, opened, at time.Time, reason EndReason) {
	r.queueWrite(recordWrite{customerID: customerID, closed: id, opened: opened, closedAt: at, endReason: string(reason)})
}

// described is what a session was renamed to.
type described struct {
	customerID, id, title, description string
	custom                             map[string]any
}

// Described queues a session's new name. It goes through the queue rather than straight to
// the store so it cannot be written before the row it renames.
func (r *sessionRecorder) Described(customerID, id, title, description string, custom map[string]any) {
	r.queueWrite(recordWrite{described: &described{customerID, id, title, description, custom}})
}

// chose is the connection a login chose for one of a session's bindings.
type chose struct {
	customerID, id, name, connectionID string
}

// Chose queues a connection a login chose, behind the row it changes, as Described does.
func (r *sessionRecorder) Chose(customerID, id, name, connectionID string) {
	r.queueWrite(recordWrite{chose: &chose{customerID, id, name, connectionID}})
}

// timed is the tool_timeout_ms a running session was given.
type timed struct {
	id, kind     string
	milliseconds int
}

// ToolTimeout queues a running session's new tool_timeout_ms behind the row it changes, so a
// reopen's own row, written first, cannot write the old one back over it.
func (r *sessionRecorder) ToolTimeout(customerID, id, kind string, milliseconds int) {
	r.queueWrite(recordWrite{customerID: customerID, timed: &timed{id, kind, milliseconds}})
}

// SawVideo queues that a session became a video one, behind the row it changes.
func (r *sessionRecorder) SawVideo(customerID, id string) {
	r.queueWrite(recordWrite{customerID: customerID, sawVideo: id})
}

// Voiced queues that voice was started on a session, behind the row it changes.
func (r *sessionRecorder) Voiced(customerID, id string) {
	r.queueWrite(recordWrite{customerID: customerID, voiced: id})
}

// Responding queues a turn that has just begun.
func (r *sessionRecorder) Responding(row store.AgentResponse) {
	r.queueWrite(recordWrite{response: &row})
}

// Responded queues how a turn ended.
func (r *sessionRecorder) Responded(customerID, id, status, failure string, at time.Time) {
	r.queueWrite(recordWrite{customerID: customerID, finished: id, status: status, failure: failure, finishedAt: at})
}

// Item queues one thing that happened during a turn.
func (r *sessionRecorder) Item(item store.AgentResponseItem) {
	r.queueWrite(recordWrite{items: []store.AgentResponseItem{item}})
}

// Flush waits for the writer to catch up with everything queued before it. Unlike the other
// writes it is never dropped, because the caller is about to read what it wrote.
func (r *sessionRecorder) Flush(ctx context.Context) error {
	// The writer answers at once, so the reply never holds it up.
	reply := make(chan (<-chan struct{}), 1)
	select {
	case r.flushes <- reply:
	case <-r.done:
		// The writer drained the queue before it stopped.
		return nil
	case <-ctx.Done():
		return stack.Wrap(ctx.Err())
	}
	select {
	case <-<-reply:
		return nil
	case <-ctx.Done():
		return stack.Wrap(ctx.Err())
	}
}

// Dropped reports how many writes were thrown away, for a test or a health check that wants
// to know the writer is keeping up.
func (r *sessionRecorder) Dropped() int64 { return r.dropped.Load() }

// Close drains the queue and stops the writer.
func (r *sessionRecorder) Close() {
	r.closeOnce.Do(func() {
		close(r.queue)
		<-r.done
		if dropped := r.dropped.Load(); dropped > 0 {
			r.logger.Warn("dropped session records because the writer fell behind", "count", dropped)
		}
	})
}

func (r *sessionRecorder) queueWrite(write recordWrite) {
	select {
	case r.queue <- write:
	default:
		r.dropped.Add(1)
		recordsDropped.Inc()
		// An end dropped here leaves its row open, as one the store refused does.
		switch {
		case write.closed != "":
			recordsLost.WithLabelValues(lostSessionEnd).Inc()
		case write.callEnded != "":
			recordsLost.WithLabelValues(lostCallEnd).Inc()
		}
	}
}

func (r *sessionRecorder) run() {
	defer close(r.done)

	// Items are the only writes that batch, so they collect here while everything else goes
	// straight through. Anything that is not an item flushes them first: a response marked
	// finished before its items were written would be read back as a turn that did nothing.
	pending := make([]store.AgentResponseItem, 0, itemBatchSize)
	flush := time.NewTicker(itemFlushEvery)
	defer flush.Stop()

	// Each waiting Flush is released once the writer has taken everything that was queued
	// when it arrived. The queue is first in, first out, so that is the writes taken so far
	// plus those still in it.
	var taken int
	var waiting flushWaiters
	for {
		// Every Flush already waiting is taken in before the next write, so none waits on
		// writes queued while the ones before it were taken in.
		for registering := true; registering; {
			select {
			case reply := <-r.flushes:
				reply <- waiting.wait(taken + len(r.queue))
			default:
				registering = false
			}
		}
		if waiting.due(taken) {
			// The items pending ahead of them are written before they are told.
			r.writeItems(pending)
			pending = pending[:0]
			waiting.release(taken)
		}
		select {
		case write, open := <-r.queue:
			if !open {
				r.writeItems(pending)
				return
			}
			taken++
			if len(write.items) > 0 {
				for _, item := range write.items {
					if _, missing := r.missing[item.ResponseID]; missing {
						recordsLost.WithLabelValues(lostItems).Inc()
					} else if _, refused := r.refused[item.ResponseID]; !refused {
						pending = append(pending, item)
					}
				}
				if len(pending) >= itemBatchSize {
					r.writeItems(pending)
					pending = pending[:0]
				}
				continue
			}
			if len(pending) > 0 {
				r.writeItems(pending)
				pending = pending[:0]
			}
			r.write(write)
		case reply := <-r.flushes:
			reply <- waiting.wait(taken + len(r.queue))
		case <-flush.C:
			if len(pending) > 0 {
				r.writeItems(pending)
				pending = pending[:0]
			}
		}
	}
}

// flushWaiters are the Flushes waiting on the writer, in order of the writes they wait for.
// Those waiting for the same writes share one entry, so there is at most one entry per write
// still queued, however many callers wait.
type flushWaiters []flushWaiter

// flushWaiter is the Flushes waiting until the writer has taken its first after writes.
type flushWaiter struct {
	after   int
	flushed chan struct{}
}

// wait returns the channel closed once the writer has taken its first after writes. after
// never falls below the last one asked for, since the writer only takes and the queue only
// grows in between.
func (w *flushWaiters) wait(after int) <-chan struct{} {
	if n := len(*w); n == 0 || (*w)[n-1].after != after {
		*w = append(*w, flushWaiter{after: after, flushed: make(chan struct{})})
	}
	return (*w)[len(*w)-1].flushed
}

// due says whether any of them is released by the writer having taken taken writes.
func (w flushWaiters) due(taken int) bool {
	return len(w) > 0 && w[0].after <= taken
}

// release tells every one whose writes are all taken.
func (w *flushWaiters) release(taken int) {
	due := slices.IndexFunc(*w, func(waiter flushWaiter) bool { return waiter.after > taken })
	if due < 0 {
		due = len(*w)
	}
	for _, waiter := range (*w)[:due] {
		close(waiter.flushed)
	}
	*w = (*w)[due:]
}

func (r *sessionRecorder) write(write recordWrite) {
	switch {
	case write.session != nil:
		key := [2]string{write.session.CustomerID, write.session.ID}
		err := r.attempt(func(ctx context.Context) error { return r.store.Sessions.Upsert(ctx, write.session) })
		if errors.Is(err, store.ErrIDTaken) {
			r.taken[key] = struct{}{}
			if write.refused != nil {
				write.refused()
			}
			// Expected when two routers open one id at once, and handled where refused ends
			// the session, which says so: no ERROR with a stack here.
			return
		}
		if err != nil {
			r.logger.Error("could not record the session starting", "error", err)
			return
		}
		// The id is the customer's now, whether or not the close of a refused session before
		// it reached the writer: a full queue drops a close.
		delete(r.taken, key)
		if write.carriedOn {
			if err := r.attempt(func(ctx context.Context) error {
				return r.store.Sessions.Supersede(ctx, write.session.CustomerID, write.session.ID, write.session.OpenedAt)
			}); err != nil {
				r.logger.Error("could not supersede the rows of a session carried on", "session", write.session.ID, "error", err)
			}
		}
		if err := r.attempt(func(ctx context.Context) error {
			return r.store.SessionTools.Upsert(ctx, write.session.CustomerID, write.session.ID, write.tools)
		}); err != nil {
			r.logger.Error("could not record the session's tools", "error", err)
		}
	case write.offered != "":
		if err := r.attempt(func(ctx context.Context) error {
			return r.store.SessionTools.Upsert(ctx, write.customerID, write.offered, write.tools)
		}); err != nil {
			r.logger.Error("could not record the session's tools", "error", err)
		}
	case write.closed != "":
		delete(r.taken, [2]string{write.customerID, write.closed})
		if err := r.attempt(func(ctx context.Context) error {
			return r.store.Sessions.Close(ctx, write.customerID, write.closed, write.opened, write.closedAt, write.endReason)
		}); err != nil {
			// The row stays open, read as live and counted in every concurrency peak.
			recordsLost.WithLabelValues(lostSessionEnd).Inc()
			r.logger.Error("could not record the session ending", "session", write.closed, "error", err)
		}
	case write.response != nil:
		if _, taken := r.taken[[2]string{write.response.CustomerID, write.response.SessionID}]; taken {
			r.refused[write.response.ID] = struct{}{}
			return
		}
		err := r.attempt(func(ctx context.Context) error { return r.store.Responses.Insert(ctx, write.response) })
		if errors.Is(err, store.ErrIDTaken) {
			r.refused[write.response.ID] = struct{}{}
		}
		if err != nil {
			r.logger.Error("could not record the turn starting", "error", err)
		}
	case write.finished != "":
		delete(r.refused, write.finished)
		delete(r.missing, write.finished)
		if err := r.attempt(func(ctx context.Context) error {
			return r.store.Responses.Finish(ctx, write.customerID, write.finished, write.status, write.failure, write.finishedAt)
		}); err != nil {
			r.logger.Error("could not record the turn ending", "error", err)
		}
	case write.described != nil:
		d := write.described
		err := r.attempt(func(ctx context.Context) error {
			return r.store.Sessions.Describe(ctx, d.customerID, d.id, d.title, d.description, d.custom)
		})
		if errors.Is(err, store.ErrNoSession) {
			// The conversation is named after it ran, so the customer may have deleted it by
			// then. Nothing is lost: the title had nothing left to name.
			r.logger.Debug("dropped the session's title: the session is gone", "session", d.id)
		} else if err != nil {
			r.logger.Error("could not record the session's title", "session", d.id, "error", err)
		}
	case write.chose != nil:
		c := write.chose
		if err := r.attempt(func(ctx context.Context) error {
			return r.store.Sessions.ChooseConnection(ctx, c.customerID, c.id, c.name, c.connectionID)
		}); err != nil {
			r.logger.Error("could not record the connection a login chose", "session", c.id, "error", err)
		}
	case write.timed != nil:
		t := write.timed
		if err := r.attempt(func(ctx context.Context) error {
			return r.store.Sessions.SetToolTimeout(ctx, write.customerID, t.id, t.kind, t.milliseconds)
		}); err != nil {
			r.logger.Error("could not record the session's tool timeout", "session", t.id, "error", err)
		}
	case write.sawVideo != "":
		if err := r.attempt(func(ctx context.Context) error {
			return r.store.Sessions.SawVideo(ctx, write.customerID, write.sawVideo)
		}); err != nil {
			r.logger.Error("could not record the session's video", "session", write.sawVideo, "error", err)
		}
	case write.voiced != "":
		if err := r.attempt(func(ctx context.Context) error {
			return r.store.Sessions.Voiced(ctx, write.customerID, write.voiced)
		}); err != nil {
			r.logger.Error("could not record the session's voice", "session", write.voiced, "error", err)
		}
	case write.callStarted != nil:
		if _, taken := r.taken[[2]string{write.callStarted.CustomerID, write.callStarted.SessionID}]; taken {
			return
		}
		if err := r.attempt(func(ctx context.Context) error { return r.store.Calls.Start(ctx, write.callStarted) }); err != nil {
			r.logger.Error("could not record the call starting", "error", err)
		}
	case write.callChanged != nil:
		if err := r.attempt(func(ctx context.Context) error { return r.store.Calls.ChangeModels(ctx, write.callChanged) }); err != nil {
			r.logger.Error("could not record the call changing models", "error", err)
		}
	case write.callEnded != "":
		if err := r.attempt(func(ctx context.Context) error {
			return r.store.Calls.Finish(ctx, write.customerID, write.callEnded, write.callEndedAt)
		}); err != nil {
			// The call stays open, its minutes growing, as a session left open does.
			recordsLost.WithLabelValues(lostCallEnd).Inc()
			r.logger.Error("could not record the call ending", "call", write.callEnded, "error", err)
		}
	}
}

func (r *sessionRecorder) writeItems(items []store.AgentResponseItem) {
	if len(items) == 0 {
		return
	}
	// The slice is copied because the caller reuses its buffer, and the write outlives the
	// call that handed it over only by as long as this statement takes.
	batch := append([]store.AgentResponseItem(nil), items...)
	err := r.attempt(func(ctx context.Context) error { return r.store.ResponseItems.InsertMany(ctx, batch) })
	if !store.IsForeignKeyViolation(err) {
		if err != nil {
			r.logger.Error("could not record what a turn did", "error", err, "items", len(batch))
		}
		return
	}
	// The batch holds the items of every turn, and an item's key is its response, so one
	// whose response row was never written would fail all of them: each turn's are written
	// on their own instead. A batch of one turn already had its answer.
	var responses []string
	byResponse := map[string][]store.AgentResponseItem{}
	for _, item := range batch {
		if _, seen := byResponse[item.ResponseID]; !seen {
			responses = append(responses, item.ResponseID)
		}
		byResponse[item.ResponseID] = append(byResponse[item.ResponseID], item)
	}
	for _, response := range responses {
		own := byResponse[response]
		if len(responses) > 1 {
			err = r.attempt(func(ctx context.Context) error { return r.store.ResponseItems.InsertMany(ctx, own) })
		}
		switch {
		case store.IsForeignKeyViolation(err):
			r.missing[response] = struct{}{}
			recordsLost.WithLabelValues(lostItems).Add(float64(len(own)))
			r.logger.Warn("dropped what a turn did: its response was never recorded",
				"session", own[0].SessionID, "response", response, "items", len(own))
		case err != nil:
			r.logger.Error("could not record what a turn did", "session", own[0].SessionID, "response", response,
				"error", err, "items", len(own))
		}
	}
}

// attempt runs do until it succeeds, fails for a reason another try would not change, or
// recordRetryFor has passed. It retries in place, holding the writes queued behind it: a
// session or response row lost to a dropped connection or a failover would fail the foreign
// key of every later row of its conversation, and the queue's order is what keeps those
// keys whole. While failing, a write is tried once.
func (r *sessionRecorder) attempt(do func(context.Context) error) error {
	deadline := time.Now().Add(recordRetryFor)
	wait := recordRetryFirst
	for {
		ctx, cancel := context.WithTimeout(context.Background(), min(time.Until(deadline), recordWriteTimeout))
		err := do(ctx)
		cancel()
		if err == nil || !store.Transient(err) {
			// Only a write that succeeded says the database is back.
			if err == nil {
				r.failing = false
			}
			return err
		}
		if r.failing || time.Until(deadline) < wait {
			r.failing = true
			return err
		}
		time.Sleep(wait)
		wait = min(2*wait, recordRetryMost)
	}
}

// sessionRow is what a session looks like to somebody reading it back later.
//
// Deliberately not the call row. That one is a call: it carries numbers, a direction and the
// review a finished call gets, none of which a text conversation has. This one carries what
// the caller said about the conversation, which is what they will look for it by.
func sessionRow(created *Session) store.AgentSession {
	spec := created.Spec()
	row := store.AgentSession{
		ID:              created.id,
		CustomerID:      spec.CustomerID,
		StreamAppPK:     spec.StreamApp,
		ConfigID:        spec.ConfigID,
		AgentName:       spec.AgentName,
		AgentID:         spec.AgentID,
		ConversationID:  spec.ConversationID,
		Title:           spec.Title,
		Description:     spec.Description,
		Project:         spec.Project,
		Custom:          spec.Custom,
		ModelOverwrites: spec.ModelOverwrites,
		ResumeHistory:   spec.ResumeHistory,
		FloorControl:    spec.FloorControl,
		ToolTimeoutMs:   spec.ToolTimeoutMs,
		CallerTools:     StoredTools(spec.Tools),
		ForkedFrom:      spec.ForkedFrom,
		ReturnURL:       spec.ReturnURL,
		NoUI:            spec.NoUI,
		State:           store.SessionRunning,
		Modality:        created.Modality(),
		CreatedAt:       created.created.UTC(),
		OpenedAt:        created.began.UTC().Truncate(time.Microsecond),
	}
	// Whose the session is comes from the credential rather than from the spec's UserID,
	// which is who the agent joined the call as. Recording the agent's own id as the owner
	// would make every session look like it belonged to the agent.
	row.UserID = spec.Caller.UserID
	row.CallerKind = string(spec.CallerKind)
	row.APIKey, row.UsageCategories = spec.Attribution.APIKey, spec.Attribution.UsageCategories
	row.Team = usage.Team(spec.Attribution.UsageCategories)
	// What opens the session's memory (Manager.Create), so a delete knows whether there is
	// anything in the memory store to forget.
	row.Remembers = new(spec.Memory.UserID != "")
	// Only the alias and the connection's id, so a fork can choose them again: a selection
	// is a reference, and the credential stays sealed on the connection.
	for _, selection := range spec.ConnectorSelections {
		row.ConnectorSelections = append(row.ConnectorSelections,
			store.SessionConnectorSelection{Name: selection.Name, ConnectionID: selection.ConnectionID})
	}
	return row
}

// toolRows is what a session's model is offered, as it is kept.
func toolRows(created *Session) []store.ToolDefinition {
	offered := created.ToolDefinitions()
	rows := make([]store.ToolDefinition, 0, len(offered))
	for _, tool := range offered {
		rows = append(rows, store.ToolDefinition{
			Name: tool.Name, Description: tool.Description, Parameters: tool.Parameters,
			Source: string(created.ToolSource(tool.Name)),
		})
	}
	return rows
}

// StoredTools are a caller's declared tools as a session's row keeps them, nil for none.
func StoredTools(tools []harness.Tool) []store.CallerTool {
	var stored []store.CallerTool
	for _, tool := range tools {
		kept := store.CallerTool{
			Name: tool.Name, Description: tool.Description, Parameters: tool.Parameters,
			Client: tool.Client, DisplayTitle: tool.DisplayTitle, Interruptible: tool.Interruptible,
			TimeoutMs: tool.Timeout.Milliseconds(),
		}
		if approval := tool.Approval; approval != nil {
			kept.Approval = &store.CallerToolApproval{
				Title: approval.Title, Message: approval.Message, ReasonArgument: approval.ReasonArgument,
				AllowTitle: approval.AllowTitle, DeclineTitle: approval.DeclineTitle,
			}
		}
		stored = append(stored, kept)
	}
	return stored
}

// DeclaredTools are the tools a session's row keeps as its caller declared them, nil for none.
func DeclaredTools(stored []store.CallerTool) []harness.Tool {
	var tools []harness.Tool
	for _, kept := range stored {
		tool := harness.Tool{
			Name: kept.Name, Description: kept.Description, Parameters: kept.Parameters,
			Client: kept.Client, DisplayTitle: kept.DisplayTitle, Interruptible: kept.Interruptible,
			Timeout: time.Duration(kept.TimeoutMs) * time.Millisecond,
		}
		if approval := kept.Approval; approval != nil {
			tool.Approval = &harness.ToolApproval{
				Title: approval.Title, Message: approval.Message, ReasonArgument: approval.ReasonArgument,
				AllowTitle: approval.AllowTitle, DeclineTitle: approval.DeclineTitle,
			}
		}
		tools = append(tools, tool)
	}
	return tools
}
