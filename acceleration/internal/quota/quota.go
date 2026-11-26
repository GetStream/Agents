// Package quota caps what one end user may spend in a day.
//
// Two kinds of limit are counted apart, because they answer to different people. The
// deployment's own default is a guard against a browser holding a token its customer's
// backend minted: it counts device callers only, since a customer's backend is trusted with
// its spend. A quota the customer set (on its organization, on an app, or on one named user)
// is the customer's own rule about its users, so it counts everything recorded with the user,
// a backend acting for them included.
//
// The deployment's default is counted in two buckets rather than one: a user id, which is the
// honest answer to "who is this", and an address, which is the answer that still holds when a
// backend mints a fresh user id per request. An IPv6 address is counted by its /64
// (routing.Caller.CountedAddress). A customer's quota is counted per user, in a
// bucket of its own, and never against the address, which many of its users can share.
//
// A day is a UTC calendar day, so a new day is a new key and nothing has to be reset.
package quota

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"

	"github.com/GetStream/Vision-Agents/acceleration/internal/redisscript"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tracing"
)

var tracer = tracing.Tracer("quota")

//go:embed reserve.lua
var reserveSource string

//go:embed adjust.lua
var adjustSource string

var (
	reserveScript = redisscript.New(reserveSource)
	adjustScript  = redisscript.New(adjustSource)
)

// retention outlives a day by enough that a counter is still there for the whole of the day
// it belongs to, whatever the clock does at the boundary, and is gone long before it could
// be confused with the same date a year later.
const retention = 48 * time.Hour

// writeTimeout bounds how long a debit holds its caller reading the limits, because it is
// taken on the goroutine draining a response and a slow policy store should cost that
// goroutine a moment rather than the rest of the answer. The write is not waited for: see Debit.
const writeTimeout = 2 * time.Second

const (
	messagesField = "messages"
	tokensField   = "tokens"
)

// ErrExhausted is what a caller with nothing left to spend is refused with. It is one error
// for every bucket and limit, with the limit that was reached, and whose it is, in the message.
var ErrExhausted = errors.New("quota: the daily limit is spent")

// Limits is what one caller may spend in a day. A limit of zero is not enforced, which is
// how a deployment turns one half off without turning the other off with it.
type Limits struct {
	// MessagesPerDay is how many responses a caller may ask for.
	MessagesPerDay int64
	// TokensPerDay is the backstop under the message count, for the caller who asks for
	// few responses and makes each one enormous.
	TokensPerDay int64
}

// Enforced reports whether these limits constrain anything.
func (l Limits) Enforced() bool { return l.MessagesPerDay > 0 || l.TokensPerDay > 0 }

// Level is who set a daily limit.
type Level string

const (
	// LevelDeployment is the router's own default, which counts device callers only.
	LevelDeployment Level = ""
	// LevelOrganization is an organization's policy: the default for every user of its apps.
	LevelOrganization Level = "organization"
	// LevelApp is an app's policy: the default for its users.
	LevelApp Level = "app"
	// LevelUser is one named user's own policy.
	LevelUser Level = "user"
)

// Setter is who set one daily limit: the level, and the organization, app or user id there.
type Setter struct {
	Level Level
	ID    string
}

// Customer reports whether the customer set the limit, rather than the deployment.
func (s Setter) Customer() bool { return s.Level != LevelDeployment }

// String names the limit's owner as a refusal does.
func (s Setter) String() string {
	switch s.Level {
	case LevelUser:
		return "user " + s.ID
	case LevelApp:
		return "every user of app " + s.ID
	case LevelOrganization:
		return "every user of organization " + s.ID
	}
	return "the router's default"
}

// Limit is one daily number and who set it. Zero is no cap.
type Limit struct {
	PerDay int64
	By     Setter
}

// Quota is what one end user is held to, each number decided on its own: the first level that
// sets it of the user's own, the app's, the organization's and the deployment's.
type Quota struct {
	Messages Limit
	Tokens   Limit
	// Unread is set when the user's own quota could not be read, so a limit it sets is not in
	// Messages and Tokens. A debit counts what the user spent all the same, which that limit
	// reads once it can be read again.
	Unread bool
}

// Default is the deployment's limits, which a customer that decided nothing is held to.
func Default(limits Limits) Quota {
	return Quota{Messages: Limit{PerDay: limits.MessagesPerDay}, Tokens: Limit{PerDay: limits.TokensPerDay}}
}

// Enforced reports whether the quota constrains anything.
func (q Quota) Enforced() bool { return q.Messages.PerDay > 0 || q.Tokens.PerDay > 0 }

// enforced are the limits that constrain something, the customer's or the deployment's.
func (q Quota) enforced(customer bool) []counted {
	var limits []counted
	if q.Messages.PerDay > 0 && q.Messages.By.Customer() == customer {
		limits = append(limits, counted{Limit: q.Messages, unit: "messages"})
	}
	if q.Tokens.PerDay > 0 && q.Tokens.By.Customer() == customer {
		limits = append(limits, counted{Limit: q.Tokens, unit: "tokens"})
	}
	return limits
}

// counted is one enforced limit and what it counts.
type counted struct {
	Limit
	unit string
}

func (c counted) spent(in spend) int64 {
	if c.unit == "tokens" {
		return in.tokens
	}
	return in.messages
}

// Source answers which quota one of a customer's users is held to. It is given the
// deployment's own limits, which are what a customer that decided nothing is held to.
type Source interface {
	Quota(ctx context.Context, customerID, userID string, defaults Limits) Quota
}

// Status is what a caller has left of the limit nearest to refusing them. A zero Status is
// a caller nothing was counted for.
type Status struct {
	// Limit is the day's allowance, in messages or in tokens.
	Limit int64
	// Remaining is what is left of it, never below zero.
	Remaining int64
	// Reset is when the day ends and the count starts again: the next UTC midnight.
	Reset time.Time
}

// Limiter counts what a caller has spent today and refuses them once it is gone.
//
// Every path through it fails open: an unreachable Redis, an unreadable counter or a nil
// Limiter all allow the request. Redis is optional in this deployment, and a limiter that
// failed closed would turn a cache blip into an outage of the thing it is protecting.
//
// The counters live in a Redis every router shares, often in another region, so a router
// keeps a copy of the ones its callers use: read on first use, counted in at once on each
// debit here, and reread every refreshEvery while Start runs. Allow answers from the copy.
type Limiter struct {
	redis    rueidis.Client
	defaults Limits
	source   Source
	logger   *slog.Logger
	now      func() time.Time
	// unreadLogs throttles the warning a debit logs for a user whose own quota could not be
	// read, and refreshLogs the one a failed refresh logs.
	unreadLogs  Throttle
	refreshLogs Throttle

	// mu guards counters, keyed by bucket, and closed.
	mu       sync.Mutex
	counters map[string]*counter
	closed   bool
	// writes are the debits still being written, which Close waits for.
	writes sync.WaitGroup
	cancel context.CancelFunc
	done   chan struct{}
}

// New returns a Limiter counting against the quota source gives each user, or against
// defaults for every user when source is nil. Defaults of zero are allowed, since a
// customer may still set quotas of its own; a user held to none is not counted.
func New(client rueidis.Client, defaults Limits, source Source, logger *slog.Logger) (*Limiter, error) {
	if client == nil {
		return nil, errors.New("quota: a redis client is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Limiter{redis: client, defaults: defaults, source: source, logger: logger, now: time.Now, counters: map[string]*counter{}}, nil
}

// bucket is one counter and the limits read against it.
type bucket struct {
	key    string
	limits []counted
}

// bucketsFor returns the buckets and enforced limits that apply to caller for held on now's day.
func bucketsFor(customerID string, caller routing.Caller, held Quota, now time.Time) []bucket {
	every := append(held.enforced(true), held.enforced(false)...)
	var buckets []bucket
	if key := customerKey(customerID, named(caller), now); key != "" {
		if limits := held.enforced(true); len(limits) > 0 {
			buckets = append(buckets, bucket{key: key, limits: limits})
		}
	}
	if !caller.ServerSide {
		// A name nobody vouched for has no customer bucket, so its device bucket answers for
		// every number, as it did before customers counted backends.
		limits := every
		if caller.Verified {
			limits = held.enforced(false)
		}
		if key := userKey(customerID, caller.UserID, now); key != "" && len(limits) > 0 {
			buckets = append(buckets, bucket{key: key, limits: limits})
		}
		// An address answers for the deployment's limits only, the guard against a backend
		// minting a fresh user id per request. Many people share one address (an office, a
		// carrier's NAT), so a customer's quota on one user is counted for that user alone.
		if key := addressKey(customerID, caller, now); key != "" {
			if limits := held.enforced(false); len(limits) > 0 {
				buckets = append(buckets, bucket{key: key, limits: limits})
			}
		}
	}
	return buckets
}

// Allow reports whether the caller may ask for another response, and what they have left
// of the limit nearest to refusing them. The Status is returned with a refusal too.
//
// A caller with nothing to count against is allowed: one with neither a user nor an address.
// A customer's own process (Caller.ServerSide) is held only to the quotas the customer set,
// and only for the user it names.
func (l *Limiter) Allow(ctx context.Context, customerID string, caller routing.Caller) (Status, error) {
	if l == nil || !countable(customerID, caller) {
		return Status{}, nil
	}
	ctx, span := tracer.Start(ctx, "quota.allow")
	defer span.End()

	// One clock reading names the day counted and the midnight it resets at.
	now := l.now()
	held := l.quota(ctx, customerID, named(caller))
	if !held.Enforced() {
		return Status{}, nil
	}
	buckets := bucketsFor(customerID, caller, held, now)
	if len(buckets) == 0 {
		return Status{}, nil
	}
	keys := make([]string, len(buckets))
	for i, counter := range buckets {
		keys[i] = counter.key
	}
	spent, err := l.current(ctx, now, keys)
	if err != nil {
		l.logger.Error("could not read a quota, allowing the request", "error", err)
		return Status{}, nil
	}

	reset := nextMidnight(now)
	var nearest Status
	for i, counter := range buckets {
		for _, limit := range counter.limits {
			nearest = closer(nearest, limit.PerDay, limit.spent(spent[i]), reset)
		}
		for _, limit := range counter.limits {
			if limit.spent(spent[i]) >= limit.PerDay {
				return nearest, stack.Wrap(fmt.Errorf("%w: %d %s a day, for %s", ErrExhausted, limit.PerDay, limit.unit, limit.By))
			}
		}
	}
	return nearest, nil
}

type keyHold struct {
	key    string
	tokens int64
}

// Reservation is a daily-quota slot held for one in-flight operation. Call Commit once the
// operation settles with the tokens it spent, or Release if it did not run. Both are safe on
// a nil Reservation and idempotent.
type Reservation struct {
	limiter    *Limiter
	reserved   bool
	customerID string
	caller     routing.Caller
	holds      []keyHold
	done       atomic.Bool
}

// Commit reconciles the reserved message and token hold with the actual tokens spent.
func (r *Reservation) Commit(ctx context.Context, tokens int64) {
	if r == nil || !r.done.CompareAndSwap(false, true) {
		return
	}
	if !r.reserved {
		r.limiter.Debit(ctx, r.customerID, r.caller, tokens)
		return
	}
	var keys []string
	var tokenDeltas []int64
	for _, h := range r.holds {
		if delta := tokens - h.tokens; delta != 0 {
			keys = append(keys, h.key)
			tokenDeltas = append(tokenDeltas, delta)
		}
	}
	if len(keys) == 0 {
		return
	}
	r.limiter.adjust(ctx, keys, 0, tokenDeltas, r.limiter.now())
}

// AddTokens records additional tokens spent by an auxiliary attempt (such as a losing hedge)
// without consuming the reservation or incrementing the message count.
func (r *Reservation) AddTokens(ctx context.Context, tokens int64) {
	if r == nil || tokens <= 0 || !r.reserved || len(r.holds) == 0 {
		return
	}
	keys := make([]string, len(r.holds))
	tokenDeltas := make([]int64, len(r.holds))
	for i, h := range r.holds {
		keys[i] = h.key
		tokenDeltas[i] = tokens
	}
	r.limiter.adjust(ctx, keys, 0, tokenDeltas, r.limiter.now())
}

// Release returns the reserved message and token hold when the operation did not run.
func (r *Reservation) Release(ctx context.Context) {
	if r == nil || !r.done.CompareAndSwap(false, true) {
		return
	}
	if !r.reserved || len(r.holds) == 0 {
		return
	}
	keys := make([]string, len(r.holds))
	tokenDeltas := make([]int64, len(r.holds))
	for i, h := range r.holds {
		keys[i] = h.key
		tokenDeltas[i] = -h.tokens
	}
	r.limiter.adjust(ctx, keys, -1, tokenDeltas, r.limiter.now())
}

// Reserve atomically checks the caller's daily limits and reserves one message, plus up to
// estimatedTokens against any enforced token limit, in Redis. The caller must call Commit once
// the work settles or Release if the work did not run.
//
// Like Allow, Reserve fails open when l is nil, the caller is uncountable, or Redis cannot be
// reached within writeTimeout.
func (l *Limiter) Reserve(ctx context.Context, customerID string, caller routing.Caller, estimatedTokens int64) (*Reservation, Status, error) {
	if l == nil || !countable(customerID, caller) {
		return nil, Status{}, nil
	}
	ctx, span := tracer.Start(ctx, "quota.reserve")
	defer span.End()

	now := l.now()
	held := l.quota(ctx, customerID, named(caller))
	if !held.Enforced() && !held.Unread {
		return nil, Status{}, nil
	}
	buckets := bucketsFor(customerID, caller, held, now)
	seen := make(map[string]struct{}, len(buckets)+2)
	for _, b := range buckets {
		seen[b.key] = struct{}{}
	}
	if held.Unread {
		customer := len(held.enforced(true)) > 0
		if key := customerKey(customerID, named(caller), now); key != "" {
			if _, ok := seen[key]; !ok {
				buckets = append(buckets, bucket{key: key})
				seen[key] = struct{}{}
			}
		}
		if !customer && l.unreadLogs.Due(customerID, time.Minute) {
			l.logger.Warn("could not read a user's own quota, counting what they spent anyway",
				"app_id", customerID)
		}
	}
	// A device caller is always counted in its user and address buckets (matching Debit), even
	// when its active limit is customer-set, so address-only checks see the spend too.
	if !caller.ServerSide {
		for _, key := range deviceKeys(customerID, caller, now) {
			if _, ok := seen[key]; !ok {
				buckets = append(buckets, bucket{key: key})
				seen[key] = struct{}{}
			}
		}
	}
	if len(buckets) == 0 {
		return nil, Status{}, nil
	}

	keys := make([]string, len(buckets))
	for i, b := range buckets {
		keys[i] = b.key
	}
	reserveCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if err := l.awaitPending(reserveCtx, keys); err != nil {
		l.logger.Error("could not wait for pending quota writes, allowing the request", "error", err)
		return &Reservation{limiter: l, customerID: customerID, caller: caller}, Status{}, nil
	}

	seconds := strconv.FormatInt(int64(retention.Seconds()), 10)
	calls := make([]rueidis.LuaExec, len(buckets))
	heldTokens := make([]int64, len(buckets))
	for i, b := range buckets {
		var msgLimit, tokLimit int64
		for _, lim := range b.limits {
			if lim.unit == "tokens" {
				tokLimit = lim.PerDay
			} else {
				msgLimit = lim.PerDay
			}
		}
		var tokHold int64
		if tokLimit > 0 && estimatedTokens > 0 {
			tokHold = min(estimatedTokens, tokLimit)
		}
		heldTokens[i] = tokHold
		calls[i] = rueidis.LuaExec{
			Keys: []string{b.key},
			Args: []string{
				strconv.FormatInt(msgLimit, 10),
				strconv.FormatInt(tokLimit, 10),
				strconv.FormatInt(tokHold, 10),
				seconds,
			},
		}
	}

	results := reserveScript.ExecMulti(reserveCtx, l.redis, calls...)
	admitted := make([]bool, len(buckets))
	spent := make([]spend, len(buckets))
	var firstErr error
	anyRefused := false
	for i, res := range results {
		vals, err := res.AsIntSlice()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if len(vals) != 3 {
			if firstErr == nil {
				firstErr = fmt.Errorf("quota: unexpected reserve reply length %d", len(vals))
			}
			continue
		}
		admitted[i] = vals[0] == 1
		spent[i] = spend{messages: vals[1], tokens: vals[2]}
		if !admitted[i] {
			anyRefused = true
		}
	}

	if firstErr != nil {
		l.logger.Error("could not reserve a quota, allowing the request", "error", firstErr)
		l.rollbackAdmitted(context.WithoutCancel(ctx), buckets, admitted, heldTokens, now)
		return &Reservation{limiter: l, customerID: customerID, caller: caller}, Status{}, nil
	}

	reset := nextMidnight(now)
	if anyRefused {
		l.rollbackAdmitted(context.WithoutCancel(ctx), buckets, admitted, heldTokens, now)
		var nearest Status
		var exhausted error
		for i, b := range buckets {
			actual := spent[i]
			if admitted[i] {
				actual.messages = max(actual.messages-1, 0)
				actual.tokens = max(actual.tokens-heldTokens[i], 0)
			} else {
				l.observe(b.key, actual, now)
			}
			for _, limit := range b.limits {
				nearest = closer(nearest, limit.PerDay, limit.spent(actual), reset)
			}
			if exhausted == nil {
				for _, limit := range b.limits {
					if limit.spent(actual) >= limit.PerDay {
						exhausted = stack.Wrap(fmt.Errorf("%w: %d %s a day, for %s", ErrExhausted, limit.PerDay, limit.unit, limit.By))
						break
					}
				}
			}
		}
		return nil, nearest, exhausted
	}

	holds := make([]keyHold, len(buckets))
	var nearest Status
	for i, b := range buckets {
		l.observe(b.key, spent[i], now)
		holds[i] = keyHold{key: b.key, tokens: heldTokens[i]}
		for _, limit := range b.limits {
			nearest = closer(nearest, limit.PerDay, limit.spent(spent[i]), reset)
		}
	}
	return &Reservation{limiter: l, reserved: true, customerID: customerID, caller: caller, holds: holds}, nearest, nil
}

// rollbackAdmitted releases the buckets that succeeded in a Reserve batch when another bucket
// refused or errored.
func (l *Limiter) rollbackAdmitted(ctx context.Context, buckets []bucket, admitted []bool, heldTokens []int64, now time.Time) {
	var keys []string
	var tokenDeltas []int64
	for i, b := range buckets {
		if admitted[i] {
			keys = append(keys, b.key)
			tokenDeltas = append(tokenDeltas, -heldTokens[i])
		}
	}
	if len(keys) == 0 {
		return
	}
	l.adjust(ctx, keys, -1, tokenDeltas, now)
}

// adjust applies signed message and token deltas to keys asynchronously, updating the local
// copy immediately and tracking the write for Close.
func (l *Limiter) adjust(ctx context.Context, keys []string, deltaMessages int64, deltaTokens []int64, now time.Time) {
	if len(keys) == 0 {
		return
	}
	seconds := strconv.FormatInt(int64(retention.Seconds()), 10)
	msgDelta := strconv.FormatInt(deltaMessages, 10)
	calls := make([]rueidis.LuaExec, len(keys))
	for i, key := range keys {
		calls[i] = rueidis.LuaExec{
			Keys: []string{key},
			Args: []string{msgDelta, strconv.FormatInt(deltaTokens[i], 10), seconds},
		}
	}
	bumped, tracked := l.bumpDelta(keys, deltaMessages, deltaTokens, now)
	writing := context.WithoutCancel(ctx)
	go func() {
		if tracked {
			defer l.writes.Done()
		}
		results := adjustScript.ExecMulti(writing, l.redis, calls...)
		totals := make([]spend, len(keys))
		for i, res := range results {
			vals, err := res.AsIntSlice()
			if err != nil || len(vals) != 2 {
				l.logger.Error("could not adjust a caller's quota", "error", err)
				l.settleDelta(bumped, deltaMessages, deltaTokens, nil, time.Time{})
				return
			}
			totals[i] = spend{messages: vals[0], tokens: vals[1]}
		}
		l.settleDelta(bumped, deltaMessages, deltaTokens, totals, l.now())
	}()
}

// Debit records one response and what it cost against every bucket the caller falls in.
//
// It reports nothing, because it is called once a response has already been generated and
// there is no longer a decision left for the answer to inform. A debit that does not land
// is logged and the caller keeps the tokens. A user whose own quota could not be read is
// counted as one held to a limit of the customer's, since that is where the limit would count.
//
// It waits at most writeTimeout for the limits, and not for the write: the debit is in this
// router's copy at once, and the write to Redis finishes on its own, which Close waits for. A
// write held to a deadline would never be sent once the limits took the whole of it to read,
// or once this process stalled past it, and the response would go uncounted. The write is
// bounded by the Redis client's own ConnWriteTimeout instead (rueidis gives up on a
// connection that answers nothing for that long, 10 s unless set), and ends with the client.
func (l *Limiter) Debit(ctx context.Context, customerID string, caller routing.Caller, tokens int64) {
	if l == nil || !countable(customerID, caller) {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	now := l.now()
	held := l.quota(ctx, customerID, named(caller))
	if !held.Enforced() && !held.Unread {
		return
	}
	var keys []string
	customer := len(held.enforced(true)) > 0
	if key := customerKey(customerID, named(caller), now); key != "" && (customer || held.Unread) {
		keys = append(keys, key)
	}
	if held.Unread && !customer {
		if l.unreadLogs.Due(customerID, time.Minute) {
			l.logger.Warn("could not read a user's own quota, counting what they spent anyway",
				"app_id", customerID)
		}
	}
	if !caller.ServerSide {
		keys = append(keys, deviceKeys(customerID, caller, now)...)
	}
	if len(keys) == 0 {
		return
	}

	seconds := int64(retention.Seconds())
	commands := make([]rueidis.Completed, 0, len(keys)*3)
	for _, key := range keys {
		commands = append(commands,
			l.redis.B().Hincrby().Key(key).Field(messagesField).Increment(1).Build(),
			l.redis.B().Hincrby().Key(key).Field(tokensField).Increment(tokens).Build(),
			l.redis.B().Expire().Key(key).Seconds(seconds).Build(),
		)
	}

	bumped, tracked := l.bump(keys, tokens, now)
	writing := context.WithoutCancel(ctx)
	go func() {
		if tracked {
			defer l.writes.Done()
		}
		responses := l.redis.DoMulti(writing, commands...)
		totals := make([]spend, len(keys))
		for i, response := range responses {
			total, err := response.AsInt64()
			if err != nil {
				l.logger.Error("could not record what a caller spent", "error", err)
				l.settle(bumped, tokens, nil, time.Time{})
				return
			}
			switch i % 3 {
			case 0:
				totals[i/3].messages = total
			case 1:
				totals[i/3].tokens = total
			}
		}
		l.settle(bumped, tokens, totals, l.now())
	}()
}

// Effective is the quota one of a customer's users is held to now. A nil Limiter enforces
// nothing, so it answers the zero Quota.
func (l *Limiter) Effective(ctx context.Context, customerID, userID string) Quota {
	if l == nil {
		return Quota{}
	}
	return l.quota(ctx, customerID, userID)
}

// Today is what one of a customer's users has spent today toward the quotas the customer set,
// a backend's work for them included, and when the count starts again.
func (l *Limiter) Today(ctx context.Context, customerID, userID string) (messages, tokens int64, reset time.Time, err error) {
	now := time.Now()
	reset = nextMidnight(now)
	key := customerKey(customerID, userID, now)
	if l == nil || key == "" {
		return 0, 0, reset, nil
	}
	spent, err := l.spent(ctx, key)
	if err != nil {
		return 0, 0, reset, err
	}
	return spent[0].messages, spent[0].tokens, reset, nil
}

// spend is what one bucket has been debited today.
type spend struct {
	messages int64
	tokens   int64
}

// spent reads buckets in one round trip. A bucket nobody has spent from reads as zero rather
// than as an error, since the first request of a day is the common case.
func (l *Limiter) spent(ctx context.Context, keys ...string) ([]spend, error) {
	commands := make([]rueidis.Completed, len(keys))
	for i, key := range keys {
		commands[i] = l.redis.B().Hgetall().Key(key).Build()
	}
	spent := make([]spend, len(keys))
	for i, response := range l.redis.DoMulti(ctx, commands...) {
		entries, err := response.AsStrMap()
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("quota: read %s: %w", keys[i], err))
		}
		spent[i] = spend{messages: parseInt(entries[messagesField]), tokens: parseInt(entries[tokensField])}
	}
	return spent, nil
}

// quota is what the user is held to: what the source says, or the deployment's own when
// there is no source.
func (l *Limiter) quota(ctx context.Context, customerID, userID string) Quota {
	if l.source == nil {
		return Default(l.defaults)
	}
	return l.source.Quota(ctx, customerID, userID, l.defaults)
}

// countable reports whether a caller falls in any bucket: a user a customer named, or a
// device caller's address.
func countable(customerID string, caller routing.Caller) bool {
	return customerID != "" && caller.UserID != "" || !caller.ServerSide && caller.IP != ""
}

// named is the user whose own quota and customer bucket a caller answers for: the one a
// backend or a verified token names, and nobody for an anonymous name, which anyone could
// claim.
func named(caller routing.Caller) string {
	if caller.ServerSide || caller.Verified {
		return caller.UserID
	}
	return ""
}

// customerKey is the bucket the customer's quotas count a user in, whoever asked for the work.
// A user id is the customer's own name for somebody, so it is keyed by the customer too.
func customerKey(customerID, userID string, now time.Time) string {
	if customerID == "" || userID == "" {
		return ""
	}
	return "quota:" + day(now) + ":customer:" + customerID + ":" + userID
}

// deviceKeys are the buckets the deployment's default counts a device caller in.
func deviceKeys(customerID string, caller routing.Caller, now time.Time) []string {
	var keys []string
	if key := userKey(customerID, caller.UserID, now); key != "" {
		keys = append(keys, key)
	}
	if key := addressKey(customerID, caller, now); key != "" {
		keys = append(keys, key)
	}
	return keys
}

// userKey is a device caller's bucket by user id.
func userKey(customerID, userID string, now time.Time) string {
	if customerID == "" || userID == "" {
		return ""
	}
	return "quota:" + day(now) + ":user:" + customerID + ":" + userID
}

// UserKeys are the buckets one of a customer's users is counted in, for every day whose
// buckets may not have expired by now, by name.
func UserKeys(customerID, userID string, now time.Time) []string {
	var keys []string
	for back := retention; back >= 0; back -= 24 * time.Hour {
		at := now.Add(-back)
		if key := customerKey(customerID, userID, at); key != "" {
			keys = append(keys, key, userKey(customerID, userID, at))
		}
	}
	return keys
}

// addressKey is a device caller's bucket by address. It is keyed by the customer as well, since
// each customer sets its own limit for an address.
func addressKey(customerID string, caller routing.Caller, now time.Time) string {
	if caller.IP == "" {
		return ""
	}
	return "quota:" + day(now) + ":ip:" + customerID + ":" + caller.CountedAddress()
}

func day(now time.Time) string { return now.UTC().Format("20060102") }

// closer returns whichever of nearest and one limit's standing has the smaller share left,
// so a caller is told about the limit that will refuse them first. A limit of zero is not
// enforced and is never the nearest.
func closer(nearest Status, limit, spent int64, reset time.Time) Status {
	if limit <= 0 {
		return nearest
	}
	candidate := Status{Limit: limit, Remaining: max(limit-spent, 0), Reset: reset}
	if nearest.Limit == 0 || share(candidate) < share(nearest) {
		return candidate
	}
	return nearest
}

// share is the part of a limit that is left, from 0 to 1.
func share(status Status) float64 { return float64(status.Remaining) / float64(status.Limit) }

// nextMidnight is when the UTC day containing now ends.
func nextMidnight(now time.Time) time.Time {
	return now.UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
}

func parseInt(value string) int64 {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

// Throttle says whether a line keyed by key is due, at most once every every.
type Throttle struct {
	mu   sync.Mutex
	last map[string]time.Time
}

// Due reports whether the line keyed by key may be logged now, and counts it as logged if so.
func (t *Throttle) Due(key string, every time.Duration) bool {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if last, ok := t.last[key]; ok && now.Sub(last) < every {
		return false
	}
	if t.last == nil {
		t.last = map[string]time.Time{}
	}
	t.last[key] = now
	return true
}
