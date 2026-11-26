package session

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// invocationQueueSize bounds how far the invocation writer may fall behind before rows are
// dropped. The session writer's recordQueueSize: a connector call is rarer than a turn's
// items, so a queue that holds those holds these. Unverified, not measured.
const invocationQueueSize = 1024

// invocationBatch is the most rows one insert writes: whatever queued while the last one was
// written, so a busy router writes them in a few round trips rather than one each.
const invocationBatch = 200

// invocationRecorder writes one store.ConnectorInvocation for each connector tool call off
// the call's path (T29, AI-856).
//
// The same trade as the session writer (sessionRecorder): the model must never wait on a
// database to read a tool's answer, and a call must not fail because its row could not be
// written. So Record never blocks and never fails; a row the writer cannot keep up with is
// dropped and counted, and one the store refuses is logged.
//
// An incognito session's rows reach it too, without the session's id: the row says a
// credential was used, which the connection's owner is owed, and nothing in it ties the use
// to the conversation (Spec.Incognito). No session's row holds the call's arguments or
// result.
type invocationRecorder struct {
	store  *store.Store
	logger *slog.Logger

	queue chan store.ConnectorInvocation
	done  chan struct{}

	// mu keeps Record from sending on queue once Close has closed it.
	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	dropped   atomic.Int64
}

func newInvocationRecorder(pgStore *store.Store, logger *slog.Logger) *invocationRecorder {
	r := &invocationRecorder{
		store:  pgStore,
		logger: logger,
		queue:  make(chan store.ConnectorInvocation, invocationQueueSize),
		done:   make(chan struct{}),
	}
	go r.run()
	return r
}

// Record queues one call's row. A nil recorder, on a deployment with connectors off, records
// nothing.
func (r *invocationRecorder) Record(row store.ConnectorInvocation) {
	if r == nil {
		return
	}
	r.mu.RLock()
	if !r.closed {
		select {
		case r.queue <- row:
			r.mu.RUnlock()
			return
		default:
		}
	}
	r.mu.RUnlock()
	r.dropped.Add(1)
}

// Close drains the queue and stops the writer.
func (r *invocationRecorder) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		close(r.queue)
		r.mu.Unlock()
		<-r.done
		if dropped := r.dropped.Load(); dropped > 0 {
			r.logger.Warn("dropped connector invocations because the writer fell behind", "count", dropped)
		}
	})
}

func (r *invocationRecorder) run() {
	defer close(r.done)
	for first := range r.queue {
		r.write(r.batch(first))
	}
}

// batch takes first and whatever else is queued, up to invocationBatch, without waiting.
func (r *invocationRecorder) batch(first store.ConnectorInvocation) []store.ConnectorInvocation {
	batch := []store.ConnectorInvocation{first}
	for len(batch) < invocationBatch {
		select {
		case row, ok := <-r.queue:
			if !ok {
				return batch
			}
			batch = append(batch, row)
		default:
			return batch
		}
	}
	return batch
}

// write stores a batch in one statement, and a row at a time when the database refuses the
// batch for a row in it, so a row it refuses loses only itself.
func (r *invocationRecorder) write(batch []store.ConnectorInvocation) {
	err := r.insert(batch)
	if err != nil && len(batch) > 1 && store.Unwritable(err) {
		for i := range batch {
			if err := r.insert(batch[i : i+1]); err != nil {
				r.logger.Error("could not record a connector invocation", "connection", batch[i].ConnectionID, "error", err)
			}
		}
		return
	}
	if err != nil {
		r.logger.Error("could not record connector invocations", "rows", len(batch), "connection", batch[0].ConnectionID, "error", err)
	}
}

func (r *invocationRecorder) insert(rows []store.ConnectorInvocation) error {
	ctx, cancel := context.WithTimeout(context.Background(), recordWriteTimeout)
	defer cancel()
	return r.store.ConnectorInvocations.InsertMany(ctx, rows)
}
