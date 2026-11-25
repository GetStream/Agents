package routing

import (
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
)

// How a stream's lease is kept: LeaseTTL long, renewed every LeaseRenew, so a process that
// dies holds its streams' money at most LeaseTTL longer.
const (
	LeaseTTL   = 90 * time.Second
	LeaseRenew = 30 * time.Second
)

// Lease sets aside what a minute of a stream may cost while it runs, so the other routers
// count a stream in progress before its units are recorded: one hold under the stream's own
// usage id, renewed every LeaseRenew and let go when the stream ends. Each unit the stream
// records settles a hold of its own. A renewal that is refused keeps the hold there was until it
// expires on its own, and stops nothing by itself: Err says the last renewal was refused, and
// Refusals how many in a row were, for a stream that ends on it. A nil Lease holds nothing.
type Lease struct {
	recorder *Recorder
	// holding is held while a minute is reserved or let go of, so a frame that reads why the
	// lease was refused never waits for it. It guards config.
	holding sync.Mutex
	config  ProviderConfig
	owner   Owner
	id      uuid.UUID
	minute  pricing.Quantities
	// attributes are what the stream's rows carry, so the minute is priced as they are.
	attributes map[string]string
	every      time.Duration
	ttl        time.Duration

	mu      sync.Mutex
	refused error
	// refusal, refusals and refusedSince are the last refusal of the run of them since a renewal
	// was last granted, how many there are and when the first was. One that could not be checked
	// is neither.
	refusal      error
	refusals     int
	refusedSince time.Time

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// StartLease reserves a minute of the stream named kind and id, as minute says it uses, on the
// provider for its owner, and keeps renewing it until Stop. A recorder that holds nothing, or a
// provider that bills nothing, leases nothing.
func StartLease(recorder *Recorder, config ProviderConfig, owner Owner, kind, id string, minute pricing.Quantities) *Lease {
	return startLease(recorder, config, owner, kind, id, minute, nil, LeaseRenew, LeaseTTL)
}

// StartLeaseOf is StartLease for a stream whose rows carry attributes its price depends on,
// such as a sandbox's class.
func StartLeaseOf(recorder *Recorder, config ProviderConfig, owner Owner, kind, id string, minute pricing.Quantities, attributes map[string]string) *Lease {
	return startLease(recorder, config, owner, kind, id, minute, attributes, LeaseRenew, LeaseTTL)
}

// StartLeaseEvery is StartLease renewing every every, for ttl each time.
func StartLeaseEvery(recorder *Recorder, config ProviderConfig, owner Owner, kind, id string, minute pricing.Quantities, every, ttl time.Duration) *Lease {
	return startLease(recorder, config, owner, kind, id, minute, nil, every, ttl)
}

func startLease(recorder *Recorder, config ProviderConfig, owner Owner, kind, id string, minute pricing.Quantities,
	attributes map[string]string, every, ttl time.Duration,
) *Lease {
	if recorder == nil || !recorder.writer.records() || !recorder.billableFor(config, attributes) {
		return nil
	}
	if _, holds := recorder.moneyGate().(Reserver); !holds {
		return nil
	}
	l := &Lease{
		recorder: recorder, config: config, owner: owner, id: UsageID("lease", kind, id), minute: minute,
		attributes: maps.Clone(attributes), every: every, ttl: ttl, stop: make(chan struct{}), done: make(chan struct{}),
	}
	l.renew()
	go l.keep()
	return l
}

func (l *Lease) keep() {
	defer close(l.done)
	ticker := time.NewTicker(l.every)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.renew()
		case <-l.stop:
			return
		}
	}
}

// stat is a minute of the stream, as a row of it is priced.
func (l *Lease) stat() Stat {
	var minute Stat
	minute.Use(l.minute)
	for name, value := range l.attributes {
		minute.Attribute(name, value)
	}
	return minute
}

func (l *Lease) renew() {
	l.holding.Lock()
	defer l.holding.Unlock()
	err := l.recorder.ReserveStat(l.config, l.owner, l.id, l.stat(), l.ttl)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refused = err
	switch {
	case err == nil:
		l.refusal, l.refusals, l.refusedSince = nil, 0, time.Time{}
	case unchecked(err):
	default:
		if l.refusals == 0 {
			l.refusedSince = time.Now()
		}
		l.refusal = err
		l.refusals++
	}
}

// unchecked reports whether a refusal is of money that could not be checked, rather than of
// money that ran out.
func unchecked(err error) bool {
	var refusal interface{ Unchecked() bool }
	return errors.As(err, &refusal) && refusal.Unchecked()
}

// Move has the lease hold a minute of the model that took the stream over, from the next
// renewal, and nothing when it bills nothing. The refusals counted so far are kept.
func (l *Lease) Move(config ProviderConfig) {
	if l == nil {
		return
	}
	l.holding.Lock()
	defer l.holding.Unlock()
	l.config = config
	if !l.recorder.billableFor(config, l.attributes) || l.recorder.QuoteStat(config, l.stat()).CostMicros <= 0 {
		l.recorder.Release(l.id)
	}
}

// ID is the usage id the lease is held under.
func (l *Lease) ID() uuid.UUID {
	if l == nil {
		return uuid.Nil
	}
	return l.id
}

// Err is why the last renewal was refused, nil while the lease is held.
func (l *Lease) Err() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.refused
}

// Refusals is why the last renewal refused was, and how many have been since one was last
// granted, with when the first was. A renewal whose money could not be checked neither counts
// nor ends the run.
func (l *Lease) Refusals() (last error, count int, since time.Time) {
	if l == nil {
		return nil, 0, time.Time{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.refusal, l.refusals, l.refusedSince
}

// Stop ends the lease and lets go of its hold. Calling it twice lets go once.
func (l *Lease) Stop() {
	if l == nil {
		return
	}
	l.stopOnce.Do(func() {
		close(l.stop)
		<-l.done
		l.recorder.Release(l.id)
	})
}
