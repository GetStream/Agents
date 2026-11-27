package agent

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/goroutine"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/memory"
)

// memoryQueueSize bounds how many exchanges may wait to be remembered before they are
// dropped. Forgetting a turn costs the next conversation a little context; blocking the
// current one costs the participant a silence.
const memoryQueueSize = 64

// memoryTimeout bounds one call to the memory store.
const memoryTimeout = 10 * time.Second

// memoryDrainTimeout bounds how long Close waits for the queue to be written, the way
// turnDrainTimeout does: two writes, the one in flight and what is left for the rest. A
// mem0 that never answers would otherwise hold a call's teardown for memoryQueueSize
// writes of memoryTimeout each. Exchanges not written by then are dropped and counted.
const memoryDrainTimeout = 2 * memoryTimeout

// lateExtraction is how long after a turn is forgotten it is forgotten again. mem0 extracts
// an exchange on its own side, after the add returned, and has been seen to start one ten
// minutes after queueing it: a memory it writes for the turn then lands after the first
// deletion.
const lateExtraction = 12 * time.Minute

// memoryJob is an exchange to remember under the turn it was said in.
type memoryJob struct {
	turnID   string
	messages []llm.Message
}

// memoryWriter hands finished exchanges to the memory store off the conversation's path. The
// store is the agent's owner's, so every call is recorded under the same customer and labels
// as the models the conversation used.
type memoryWriter struct {
	store memory.Store
	scope memory.Scope
	// incognito recalls but never writes, so nothing said in the session is kept.
	incognito bool
	logger    *slog.Logger

	queue chan memoryJob
	done  chan struct{}
	// forgetting is every Forget under way, each its own goroutine. Once closed is set no
	// other starts, and stop ends the wait of those waiting to forget again.
	forgetting sync.WaitGroup
	forgetMu   sync.Mutex
	closed     bool
	// forgotten are the turns Forget was asked for, whose exchanges still queued are dropped.
	forgotten map[string]struct{}
	// rewritten are forgotten turns whose exchange was being written as Forget ran, which
	// Close forgets once more after the queue is drained.
	rewritten []string
	stop      chan struct{}
	// forgetCtx is what deletions run under, cancelled when Close gives up on them.
	forgetCtx    context.Context
	forgetCancel context.CancelFunc
	// lateAfter is lateExtraction, shortened by tests.
	lateAfter time.Duration

	// ctx is the writer's, cancelled when Close gives up on what is still queued.
	ctx          context.Context
	cancel       context.CancelFunc
	drainTimeout time.Duration

	closeOnce sync.Once
	dropped   atomic.Int64
}

func newMemoryWriter(store memory.Store, scope memory.Scope, incognito bool, logger *slog.Logger) *memoryWriter {
	w := &memoryWriter{
		store:     store,
		scope:     scope,
		incognito: incognito,
		logger:    logger,
		queue:     make(chan memoryJob, memoryQueueSize),
		done:      make(chan struct{}),
		stop:      make(chan struct{}),
		lateAfter: lateExtraction,

		drainTimeout: memoryDrainTimeout,
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.forgetCtx, w.forgetCancel = context.WithCancel(w.ctx)
	goroutine.Go(w.ctx, w.run)
	return w
}

// Remember queues an exchange said in turn turnID, dropping it if the writer is too far
// behind.
func (w *memoryWriter) Remember(turnID string, messages []llm.Message) {
	if w.incognito || len(messages) == 0 {
		return
	}

	select {
	case w.queue <- memoryJob{turnID: turnID, messages: messages}:
	default:
		w.dropped.Add(1)
	}
}

// Forget deletes what turns turnIDs of this session taught the store, off the caller's
// path, and deletes it again lateExtraction later: an exchange of one of those turns may
// still be waiting in the queue, or be extracted by the store after the first deletion.
func (w *memoryWriter) Forget(turnIDs []string) {
	if w.incognito || len(turnIDs) == 0 {
		return
	}
	// Why: an edit can arrive while the session is closing the agent, and a goroutine
	// started once Close is waiting would not be waited for.
	w.forgetMu.Lock()
	defer w.forgetMu.Unlock()
	// An exchange of these turns still queued is never written: a deletion that ran before it
	// would not find it, and the second pass does not run once the writer closes. Marked even
	// once closed, since Close goes on draining the queue.
	if w.forgotten == nil {
		w.forgotten = map[string]struct{}{}
	}
	for _, turnID := range turnIDs {
		w.forgotten[turnID] = struct{}{}
	}
	if w.closed {
		w.logger.Warn("closed before forgetting an abandoned branch's turns", "turns", len(turnIDs))
		return
	}
	turnIDs = slices.Clone(turnIDs)
	w.forgetting.Add(1)
	goroutine.Go(w.ctx, func() {
		defer w.forgetting.Done()
		w.forget(w.forgetCtx, turnIDs)
		wait := time.NewTimer(w.lateAfter)
		defer wait.Stop()
		select {
		case <-wait.C:
			w.forget(w.forgetCtx, turnIDs)
		case <-w.stop:
			w.logger.Warn("closed before forgetting an abandoned branch's turns again; what the store extracts of them late is kept", "turns", len(turnIDs))
		}
	})
}

// Recall asks what is already known about the participant. A failure is not fatal: an
// agent that has forgotten is worse than one that never knew, but neither is a broken
// call, so this reports the problem and carries on with nothing.
func (w *memoryWriter) Recall(ctx context.Context, limit int) []memory.Memory {
	recalled, err := w.store.Recall(ctx, memory.Query{Scope: w.scope, Limit: limit})
	if err != nil {
		w.logger.Error("could not recall what is known about this participant", "error", err)
		return nil
	}
	return recalled
}

// Close drains the queue and stops the writer, giving the writes drainTimeout between
// them. Exchanges still unwritten then are dropped and counted with the rest.
func (w *memoryWriter) Close() {
	w.closeOnce.Do(func() {
		w.forgetMu.Lock()
		w.closed = true
		w.forgetMu.Unlock()
		close(w.stop)
		// A deletion under way is given drainTimeout too, so a store that never answers
		// holds the teardown for that long and no longer.
		forgotten := make(chan struct{})
		goroutine.Go(w.ctx, func() {
			w.forgetting.Wait()
			close(forgotten)
		})
		drainWithin(forgotten, w.drainTimeout, w.forgetCancel)
		close(w.queue)
		drainWithin(w.done, w.drainTimeout, w.cancel)
		// An exchange written while its turn was being forgotten may have landed after the
		// deletion, and the second pass that would have found it does not run now.
		if rewritten := w.rewrittenTurns(); len(rewritten) > 0 {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), w.drainTimeout)
			w.forget(ctx, rewritten)
			cancel()
		}
		if dropped := w.dropped.Load(); dropped > 0 {
			w.logger.Warn("dropped exchanges the memory writer could not keep up with", "count", dropped)
		}
	})
}

func (w *memoryWriter) run() {
	defer close(w.done)

	for job := range w.queue {
		if w.wasForgotten(job.turnID) {
			continue
		}
		if w.ctx.Err() != nil {
			w.dropped.Add(1)
			continue
		}
		scope := w.scope
		scope.TurnID = job.turnID
		ctx, cancel := context.WithTimeout(w.ctx, memoryTimeout)
		if err := w.store.Remember(ctx, scope, job.messages); err != nil {
			w.logger.Error("could not remember an exchange", "error", err)
			if ctx.Err() != nil {
				w.dropped.Add(1)
			}
		}
		cancel()
		if w.wasForgotten(job.turnID) {
			w.forgetMu.Lock()
			w.rewritten = append(w.rewritten, job.turnID)
			w.forgetMu.Unlock()
		}
	}
}

// rewrittenTurns are the forgotten turns whose exchange was written as Forget ran.
func (w *memoryWriter) rewrittenTurns() []string {
	w.forgetMu.Lock()
	defer w.forgetMu.Unlock()
	return slices.Clone(w.rewritten)
}

// forget deletes what turnIDs taught the store, within ctx. The store asks once or more per
// turn, so each turn has memoryTimeout.
func (w *memoryWriter) forget(ctx context.Context, turnIDs []string) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(len(turnIDs))*memoryTimeout)
	defer cancel()
	if err := w.store.ForgetTurns(ctx, w.scope.AppID, w.scope.RunID, turnIDs); err != nil {
		w.logger.Error("could not forget the turns of an abandoned branch", "turns", len(turnIDs), "error", err)
	}
}

// wasForgotten is whether Forget was asked for turnID.
func (w *memoryWriter) wasForgotten(turnID string) bool {
	w.forgetMu.Lock()
	defer w.forgetMu.Unlock()
	_, forgotten := w.forgotten[turnID]
	return forgotten
}
