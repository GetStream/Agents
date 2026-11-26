package quota

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

const (
	// refreshEvery is how often a router rereads the counters it holds a copy of. What a user
	// spends through other routers reaches this one's Allow at most this late, so it is the
	// most a user can overshoot their day by: what their sessions elsewhere spend in that time.
	refreshEvery = 2 * time.Second
	// staleAfter is how old a copy may grow while its refreshes fail before Allow reads Redis
	// for itself again, and so fails open on a Redis that stays unreachable, as it always has.
	staleAfter = 5 * refreshEvery
	// idleAfter is how long a counter nobody asked about or debited stays in the copy.
	idleAfter = 10 * time.Minute
	// maxCounters bounds the copy, a few hundred bytes a counter. Past it a counter is read
	// from Redis on each Allow. A refresh of all of them takes maxCounters/refreshChunk round
	// trips, refreshChunks at once, which fits in refreshEvery from another region.
	maxCounters = 20_000
	// refreshChunk is the most counters one round trip of a refresh reads, and refreshChunks
	// how many such round trips one refresh sends at once.
	refreshChunk   = 256
	refreshChunks  = 4
	refreshTimeout = refreshEvery
)

// counter is this router's copy of one bucket.
type counter struct {
	// remote is the bucket as Redis last answered for it: to a read, or to a write of this
	// router's with the total it came to.
	remote spend
	// unwritten is what this router debited in the pending writes Redis has not answered yet.
	unwritten     spend
	pending       int
	pendingShrink int
	overlap       bool
	// written is closed once no write is pending, and made again when one starts.
	written chan struct{}
	// writes counts this router's answered writes, so a read that started before one of them
	// was answered does not undo it.
	writes uint64
	// read is when Redis last answered for the bucket, zero for never, and used when Allow or
	// Debit last touched it.
	read time.Time
	used time.Time
}

func (c *counter) spent() spend {
	return spend{
		messages: max(c.remote.messages+c.unwritten.messages, 0),
		tokens:   max(c.remote.tokens+c.unwritten.tokens, 0),
	}
}

// seen is a counter as it was before Redis was read for it.
type seen struct {
	counter *counter
	writes  uint64
}

// Start refreshes the copy of the counters every refreshEvery until Close.
func (l *Limiter) Start(ctx context.Context) {
	if l == nil {
		return
	}
	ctx, l.cancel = context.WithCancel(ctx)
	l.done = make(chan struct{})
	go l.run(ctx)
}

// Close stops the refresh and waits for the debits still being written, each bounded by the
// Redis client's own write timeout.
func (l *Limiter) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	if l.cancel != nil {
		l.cancel()
		<-l.done
	}
	l.writes.Wait()
}

func (l *Limiter) run(ctx context.Context) {
	defer close(l.done)
	ticker := time.NewTicker(refreshEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.refresh(ctx)
		}
	}
}

// refresh drops the counters nobody touched for idleAfter and rereads the rest from Redis.
func (l *Limiter) refresh(ctx context.Context) {
	now := l.now()
	l.mu.Lock()
	keys := make([]string, 0, len(l.counters))
	for key, held := range l.counters {
		if now.Sub(held.used) > idleAfter && held.pending == 0 {
			delete(l.counters, key)
			continue
		}
		keys = append(keys, key)
	}
	l.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	slots := make(chan struct{}, refreshChunks)
	var wg sync.WaitGroup
	for chunk := range slices.Chunk(keys, refreshChunk) {
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			before := l.seen(chunk)
			fetched, err := l.spent(ctx, chunk...)
			if err != nil {
				if l.refreshLogs.Due("refresh", time.Minute) {
					l.logger.Warn("could not refresh the daily limits' counters, answering from the copy", "error", err)
				}
				return
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			for i, key := range chunk {
				if _, held := l.counters[key]; held {
					l.keep(key, before[i], fetched[i], now)
				}
			}
		})
	}
	wg.Wait()
}

// current is what each bucket has been debited today: from the copy where it holds a recent
// one, and for the rest from Redis in one round trip, which the copy keeps from then on.
func (l *Limiter) current(ctx context.Context, now time.Time, keys []string) ([]spend, error) {
	if err := l.awaitWrites(ctx, now, keys); err != nil {
		return nil, err
	}
	spent := make([]spend, len(keys))
	var missing []string
	var at []int
	l.mu.Lock()
	for i, key := range keys {
		held, ok := l.counters[key]
		if !ok || now.Sub(held.read) > staleAfter {
			missing = append(missing, key)
			at = append(at, i)
			continue
		}
		held.used = now
		spent[i] = held.spent()
	}
	l.mu.Unlock()
	if len(missing) == 0 {
		return spent, nil
	}

	before := l.seen(missing)
	fetched, err := l.spent(ctx, missing...)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for j, key := range missing {
		held := l.keep(key, before[j], fetched[j], now)
		held.used = now
		spent[at[j]] = held.spent()
	}
	return spent, nil
}

// awaitWrites waits for the pending writes of each counter Redis would have to be read for.
// A read could not tell which of them it saw, and their answers bring the totals it would.
func (l *Limiter) awaitWrites(ctx context.Context, now time.Time, keys []string) error {
	var waits []chan struct{}
	l.mu.Lock()
	for _, key := range keys {
		if held, ok := l.counters[key]; ok && held.pending > 0 && now.Sub(held.read) > staleAfter {
			waits = append(waits, held.written)
		}
	}
	l.mu.Unlock()
	for _, written := range waits {
		select {
		case <-written:
		case <-ctx.Done():
			return stack.Wrap(ctx.Err())
		}
	}
	return nil
}

// awaitPending waits for any in-flight writes on keys so a subsequent Redis script sees them.
func (l *Limiter) awaitPending(ctx context.Context, keys []string) error {
	var waits []chan struct{}
	l.mu.Lock()
	for _, key := range keys {
		if held, ok := l.counters[key]; ok && held.pending > 0 {
			waits = append(waits, held.written)
		}
	}
	l.mu.Unlock()
	for _, written := range waits {
		select {
		case <-written:
		case <-ctx.Done():
			return stack.Wrap(ctx.Err())
		}
	}
	return nil
}

// observe records an authoritative Redis total returned by a synchronous script call.
func (l *Limiter) observe(key string, total spend, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	held, ok := l.counters[key]
	if !ok {
		if len(l.counters) >= maxCounters {
			return
		}
		l.counters[key] = &counter{remote: total, read: at, used: at, writes: 1}
		return
	}
	held.writes++
	held.used = at
	if held.pending == 0 {
		held.remote = atLeast(held.remote, total)
		held.read = at
	}
}

// seen notes each key's counter before Redis is read for it.
func (l *Limiter) seen(keys []string) []seen {
	l.mu.Lock()
	defer l.mu.Unlock()
	before := make([]seen, len(keys))
	for i, key := range keys {
		if held, ok := l.counters[key]; ok {
			before[i] = seen{counter: held, writes: held.writes}
		}
	}
	return before
}

// keep lays what Redis answered for key over the copy, and returns the counter. Called with
// l.mu held. A counter past maxCounters is returned without being kept.
func (l *Limiter) keep(key string, before seen, fetched spend, at time.Time) *counter {
	held, ok := l.counters[key]
	switch {
	case !ok:
		held = &counter{remote: fetched}
		if len(l.counters) < maxCounters {
			l.counters[key] = held
		}
	case held.pending > 0:
		// The read may have seen a pending write or not; its answer will bring the total.
		return held
	case held == before.counter && held.writes == before.writes:
		held.remote = fetched
	default:
		// A write was answered while Redis was being read, with a total the read may predate.
		return held
	}
	held.read = at
	return held
}

// bump counts a debit in the copy of each key, and returns those counters, nil past
// maxCounters, and whether the write is to be waited for by Close. A key the copy did not hold
// gets a counter never read, which an Allow waits for the write's answer to fill.
func (l *Limiter) bump(keys []string, tokens int64, now time.Time) ([]*counter, bool) {
	tokenDeltas := make([]int64, len(keys))
	for i := range tokenDeltas {
		tokenDeltas[i] = tokens
	}
	return l.bumpDelta(keys, 1, tokenDeltas, now)
}

func (l *Limiter) bumpDelta(keys []string, deltaMessages int64, deltaTokens []int64, now time.Time) ([]*counter, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	bumped := make([]*counter, len(keys))
	for i, key := range keys {
		held, ok := l.counters[key]
		if !ok {
			if len(l.counters) >= maxCounters {
				continue
			}
			held = &counter{}
			l.counters[key] = held
		}
		if held.pending == 0 {
			held.written = make(chan struct{})
			held.overlap = false
		} else if deltaMessages < 0 || deltaTokens[i] < 0 || held.pendingShrink > 0 {
			held.overlap = true
		}
		if deltaMessages < 0 || deltaTokens[i] < 0 {
			held.pendingShrink++
		}
		held.pending++
		held.unwritten.messages += deltaMessages
		held.unwritten.tokens += deltaTokens[i]
		held.used = now
		bumped[i] = held
	}
	if l.closed {
		return bumped, false
	}
	l.writes.Add(1)
	return bumped, true
}

// settle moves a debit from unwritten to remote in the counters it bumped, with the totals
// Redis answered at at, or drops it from them when the write failed, as Redis did.
func (l *Limiter) settle(bumped []*counter, tokens int64, totals []spend, at time.Time) {
	tokenDeltas := make([]int64, len(bumped))
	for i := range tokenDeltas {
		tokenDeltas[i] = tokens
	}
	l.settleDelta(bumped, 1, tokenDeltas, totals, at)
}

func (l *Limiter) settleDelta(bumped []*counter, deltaMessages int64, deltaTokens []int64, totals []spend, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, held := range bumped {
		if held == nil {
			continue
		}
		shrink := deltaMessages < 0 || deltaTokens[i] < 0
		if shrink {
			held.pendingShrink--
		}
		held.unwritten.messages -= deltaMessages
		held.unwritten.tokens -= deltaTokens[i]
		held.writes++
		if totals != nil {
			switch {
			case held.overlap:
				// Overlapping writes with a shrink can settle out of order: drop the
				// cached read time so the next Allow rereads Redis once pending hits 0.
				held.read = time.Time{}
			case !shrink:
				held.remote = atLeast(held.remote, totals[i])
				held.read = at
			default:
				held.remote = totals[i]
				held.read = at
			}
		}
		held.pending--
		if held.pending == 0 {
			held.overlap = false
			close(held.written)
		}
	}
}

func atLeast(a, b spend) spend {
	return spend{messages: max(a.messages, b.messages), tokens: max(a.tokens, b.tokens)}
}
