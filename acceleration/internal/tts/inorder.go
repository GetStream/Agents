package tts

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/GetStream/Vision-Agents/acceleration/internal/goroutine"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ErrIDSpoken refuses an utterance under the id of one still being spoken.
var ErrIDSpoken = errors.New("give the next utterance an id of its own")

// InOrder makes a provider that takes each sentence as its own request speak them in the
// order they were asked for.
//
// Such a provider runs its requests side by side, which is what has the next sentence
// ready the moment the last one ends, and sends each one's audio back as it arrives.
// Played as it arrives, two sentences are heard spliced into one another. This holds a
// sentence's audio until every sentence asked for before it has finished, so the requests
// still overlap and only what the listener hears is put in order.
//
// A barge-in drops everything held. Every sentence still settles, because a completion is
// what a caller counts speech in flight by and what it is billed by: a sentence nobody
// heard is completed all the same, marked interrupted.
func InOrder(ctx context.Context, provider TTS) TTS {
	ordered := &inOrder{
		TTS:     provider,
		emitter: NewEmitter(64),
		notify:  make(chan struct{}, 1),
	}
	goroutine.Go(ctx, ordered.relay)
	return ordered
}

type entryState int

const (
	entryQueuedStreaming entryState = iota
	entryQueuedClosed
	entryQueuedCompleted
	entryHeadStreaming
	entryHeadClosed
	entryDropped
)

// synthesisEntry is one sentence in the queue. Its generation grows with every new
// sentence, so a sentence dropped by Interrupt and a new one that reuses its id are
// never mistaken for each other.
type synthesisEntry struct {
	id         string
	generation uint64
	state      entryState
	held       []Event
	completion SynthesisComplete
}

type inOrder struct {
	TTS
	emitter *Emitter
	// notify wakes relay when Interrupt or a failed Synthesize queues events in pending.
	notify chan struct{}

	mu      sync.Mutex
	nextGen uint64
	// entries holds every active and dropped synthesis in request order. The first active
	// entry is the head being heard; every following active entry is held behind it.
	entries []*synthesisEntry
	// pending holds events produced outside relay (settled interrupted completions or
	// held sentences released when a first chunk fails) so relay emits them before closing.
	pending []Event
}

// Synthesize queues the sentence behind the ones already asked for and sends it to the
// provider straight away. The pieces of one sentence share its place in the queue.
//
// A request for a sentence whose final piece was already sent, before it has finished, is
// refused: the order is kept by id, and a second sentence under the same id would leave the
// queue waiting on a sentence that has already finished.
func (o *inOrder) Synthesize(request Request) error {
	// The order is kept by id, so the sentence needs one before the provider is asked.
	if request.ID == "" {
		request.ID = NewSynthesisID()
	}

	o.mu.Lock()
	var (
		existed bool
		gen     uint64
	)
	if idx := o.indexActiveLocked(request.ID); idx >= 0 {
		entry := o.entries[idx]
		if entry.state != entryHeadStreaming && entry.state != entryQueuedStreaming {
			o.mu.Unlock()
			return stack.Wrap(fmt.Errorf("tts: %q is still being spoken; %w", request.ID, ErrIDSpoken))
		}
		existed = true
		gen = entry.generation
		if request.Final {
			if entry.state == entryHeadStreaming {
				entry.state = entryHeadClosed
			} else {
				entry.state = entryQueuedClosed
			}
		}
	} else {
		o.nextGen++
		gen = o.nextGen
		state := entryQueuedStreaming
		if o.firstActiveIndexLocked() < 0 {
			state = entryHeadStreaming
		}
		if request.Final {
			if state == entryHeadStreaming {
				state = entryHeadClosed
			} else {
				state = entryQueuedClosed
			}
		}
		o.entries = append(o.entries, &synthesisEntry{
			id:         request.ID,
			generation: gen,
			state:      state,
		})
	}
	o.mu.Unlock()

	if err := o.TTS.Synthesize(request); err != nil {
		o.mu.Lock()
		o.rollbackLocked(gen, existed, request.Final)
		o.mu.Unlock()
		return err
	}
	return nil
}

// Interrupt drops every sentence held or being heard and stops the provider.
func (o *inOrder) Interrupt() error {
	o.mu.Lock()
	var settled []Event
	kept := o.entries[:0]
	for _, entry := range o.entries {
		switch entry.state {
		case entryDropped:
			kept = append(kept, entry)
		case entryQueuedCompleted:
			// Finished while it waited, so its completion is already here and was never
			// passed on. Nobody heard it.
			complete := entry.completion
			complete.Interrupted = true
			settled = append(settled, complete)
		case entryHeadClosed, entryQueuedClosed:
			entry.state = entryDropped
			entry.held = nil
			kept = append(kept, entry)
		case entryHeadStreaming, entryQueuedStreaming:
			// A sentence whose final piece was never sent will get no completion: the
			// provider throws its unsent text away.
		}
	}
	clear(o.entries[len(kept):])
	o.entries = kept
	o.enqueuePendingLocked(settled)
	o.mu.Unlock()

	return o.TTS.Interrupt()
}

// Events carries the provider's events with each sentence's audio in the order asked for.
func (o *inOrder) Events() <-chan Event { return o.emitter.Events() }

// Voice is the provider's voice, empty when it cannot say.
func (o *inOrder) Voice() string {
	if voiced, ok := o.TTS.(Voiced); ok {
		return voiced.Voice()
	}
	return ""
}

// relay puts the provider's events in order until it closes.
func (o *inOrder) relay() {
	defer o.emitter.Close()

	providerEvents := o.TTS.Events()
	for providerEvents != nil {
		select {
		case <-o.notify:
			o.flushPending()
		case event, ok := <-providerEvents:
			if !ok {
				providerEvents = nil
				break
			}
			for _, ordered := range o.order(event) {
				o.emitter.Send(ordered)
			}
		}
	}

	// The provider has closed. Send everything still waiting, so no finished sentence and
	// no completion that Interrupt settled is lost.
	o.mu.Lock()
	rest := o.drainLocked()
	o.mu.Unlock()
	for _, event := range rest {
		o.emitter.Send(event)
	}
}

// order returns what can be passed on now that event has arrived.
func (o *inOrder) order(event Event) []Event {
	var id string
	switch typed := event.(type) {
	case AudioChunk:
		id = typed.SynthesisID
	case SynthesisComplete:
		id = typed.SynthesisID
	default:
		// Starts, errors and the connection's own events are never held: nothing is heard
		// from them, and a caller settling a sentence needs its error before its end.
		o.mu.Lock()
		pending := o.takePendingLocked()
		o.mu.Unlock()
		return append(pending, event)
	}
	complete, completes := event.(SynthesisComplete)

	o.mu.Lock()
	defer o.mu.Unlock()

	out := o.takePendingLocked()

	// A completion settles the oldest sentence under its id, the way the session counts them.
	// A request cancelled by a barge-in finishes before a new one under the same id has even
	// started, so its completion belongs to the dropped sentence, not the new one.
	if completes {
		if idx := o.indexDroppedLocked(id); idx >= 0 {
			o.entries = slices.Delete(o.entries, idx, idx+1)
			complete.Interrupted = true
			return append(out, complete)
		}
	}

	if idx := o.indexActiveLocked(id); idx >= 0 {
		entry := o.entries[idx]
		if entry.state == entryHeadStreaming || entry.state == entryHeadClosed {
			if !completes {
				return append(out, event)
			}
			o.entries = slices.Delete(o.entries, idx, idx+1)
			out = append(out, event)
			return append(out, o.promoteLocked()...)
		}
		entry.held = append(entry.held, event)
		if completes {
			entry.state = entryQueuedCompleted
			entry.completion = complete
		}
		return out
	}

	// Nobody hears audio from a sentence a barge-in dropped.
	if o.indexDroppedLocked(id) >= 0 {
		return out
	}

	// Not a sentence this asked for, so there is nothing to keep it in order with.
	return append(out, event)
}

// rollbackLocked undoes a Synthesize whose provider call failed. A failed first chunk
// removes the entry and promotes the next active sentence if it was head. A failed later
// chunk keeps the live entry in place and reverts a Final transition back to streaming.
func (o *inOrder) rollbackLocked(gen uint64, existed bool, final bool) {
	idx := o.indexByGenLocked(gen)
	if idx < 0 {
		return
	}
	entry := o.entries[idx]
	if !existed {
		// Refused outright, it will send nothing, and waiting for it would silence
		// everything asked for after it.
		wasHead := entry.state == entryHeadStreaming || entry.state == entryHeadClosed
		if entry.state == entryQueuedCompleted {
			// A completion already came in under this id, and the session bills a sentence
			// by its completion, so it is passed on even though nobody heard the sentence.
			complete := entry.completion
			complete.Interrupted = true
			o.enqueuePendingLocked([]Event{complete})
		}
		o.entries = slices.Delete(o.entries, idx, idx+1)
		if wasHead {
			o.enqueuePendingLocked(o.promoteLocked())
		}
		return
	}
	if !final {
		return
	}
	switch entry.state {
	case entryHeadClosed:
		entry.state = entryHeadStreaming
	case entryQueuedClosed:
		entry.state = entryQueuedStreaming
	case entryDropped:
		// Interrupt ran while the final chunk was being sent, so it waits for a completion.
		// The final chunk failed, so that completion never comes: remove the entry.
		o.entries = slices.Delete(o.entries, idx, idx+1)
	}
}

// promoteLocked advances the next active entry to head, returning its held events and
// carrying on past every queued entry that had already finished.
func (o *inOrder) promoteLocked() []Event {
	var released []Event
	for {
		idx := o.firstActiveIndexLocked()
		if idx < 0 {
			return released
		}
		entry := o.entries[idx]
		released = append(released, entry.held...)
		entry.held = nil
		switch entry.state {
		case entryQueuedCompleted:
			o.entries = slices.Delete(o.entries, idx, idx+1)
		case entryQueuedStreaming:
			entry.state = entryHeadStreaming
			return released
		case entryQueuedClosed:
			entry.state = entryHeadClosed
			return released
		default:
			return released
		}
	}
}

func (o *inOrder) indexActiveLocked(id string) int {
	for i, entry := range o.entries {
		if entry.state != entryDropped && entry.id == id {
			return i
		}
	}
	return -1
}

func (o *inOrder) indexDroppedLocked(id string) int {
	for i, entry := range o.entries {
		if entry.state == entryDropped && entry.id == id {
			return i
		}
	}
	return -1
}

func (o *inOrder) indexByGenLocked(gen uint64) int {
	for i, entry := range o.entries {
		if entry.generation == gen {
			return i
		}
	}
	return -1
}

func (o *inOrder) firstActiveIndexLocked() int {
	for i, entry := range o.entries {
		if entry.state != entryDropped {
			return i
		}
	}
	return -1
}

func (o *inOrder) enqueuePendingLocked(events []Event) {
	if len(events) == 0 {
		return
	}
	o.pending = append(o.pending, events...)
	select {
	case o.notify <- struct{}{}:
	default:
	}
}

func (o *inOrder) takePendingLocked() []Event {
	if len(o.pending) == 0 {
		return nil
	}
	events := o.pending
	o.pending = nil
	return events
}

func (o *inOrder) flushPending() {
	o.mu.Lock()
	events := o.takePendingLocked()
	o.mu.Unlock()
	for _, event := range events {
		o.emitter.Send(event)
	}
}

func (o *inOrder) drainLocked() []Event {
	rest := o.takePendingLocked()
	for _, entry := range o.entries {
		if entry.state != entryDropped {
			rest = append(rest, entry.held...)
		}
	}
	o.entries = nil
	return rest
}
