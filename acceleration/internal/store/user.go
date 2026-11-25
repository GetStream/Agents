package store

import (
	"container/list"
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/redis/rueidis"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// userTTL is how long a user's key lives in Redis. It is a backstop rather than a policy:
// the key says the row exists, the row is never deleted on its own, and claiming drops the
// key itself.
const userTTL = time.Hour

// emailTTL is how long an email this process wrote is not written again. It is short, and
// not the TTL, because the row is what the first consent for an owner_email connector is
// checked against (api ownerEmailRefusal): an address another replica wrote under this one
// is the one that consent admits until this replica writes its own again, so a held address
// leaves the row stale for at most this long.
const emailTTL = time.Minute

// emailStripes is how many locks the email writes are spread over, so that one slow write
// holds up only the users that hash to its lock. Any count is correct; 64 is a judgement,
// not a measurement.
const emailStripes = 64

// usersPrefix names the users' keys in Redis, so they can be told from the rest.
const usersPrefix = "users:"

// seenCapacity is how many users one process keeps in memory, across every app it serves.
// It is a count rather than a size because the entries are two short strings each, so ten
// thousand of them is a few hundred kilobytes.
const seenCapacity = 10_000

// Users records the end users an app is seen acting for.
//
// Every authenticated request names one, and nearly every one of them names somebody
// already written down, so the work is almost always a write that changes nothing. Three
// tiers keep it off Postgres: an LRU of the last ten thousand users this process saw, a
// key in Redis that the other replicas share, and the row itself.
//
// Redis is told to track the keys this process read, so a user whose row changes under it
// -- a guest who was claimed -- is dropped from every replica's LRU at once rather than
// each on its own timer.
//
// A deployment with no Redis keeps the LRU and writes to Postgres on a miss, and so does
// one whose Redis is unreachable. Nothing here fails a request because a cache is missing,
// and recording who called fails no request at all: it is not what the caller asked for.
// A guest's row, secret and claim are here too, and those do answer with their errors.
//
// The email a backend says its user signs in with is said on every request too, and is
// held in an LRU of its own, by user and address, for emailTTL: a changed address is written
// at once.
//
// Whether the router minted a user as a guest is read on each of their requests (Minted), and
// held apart from all of that: a user is seen whether or not they were minted.
type Users struct {
	store  *Store
	seen   *seen
	emails *seen
	// minted is what Minted read, in a router with no Redis to hold it.
	minted *seen

	// emailTTL is emailTTL; a test shortens it.
	emailTTL time.Duration
	// emailLocks keep each user's write and hold together, so that two requests saying two
	// addresses at once leave the held one the one the row holds.
	emailLocks [emailStripes]sync.Mutex
}

func newUsers(s *Store) *Users {
	return &Users{store: s, seen: newSeen(seenCapacity), emails: newSeen(seenCapacity), minted: newSeen(seenCapacity), emailTTL: emailTTL}
}

// Seen records that an app acted for a user, as the kind their own token proved them to
// be, and does nothing for one already written down from a sighting that settles it.
//
// The kind is what the credential proved them to be, so only a verified caller belongs
// here: an anonymous one goes by a name nobody checked, and recording it would fill the
// table with whatever names were asked for.
//
// A user's own full token is the one sighting that changes a row already there
// (RecordAccount), so it is settled only by having been written: one held from a guest
// token, or from a backend naming them, is written again.
func (r *Users) Seen(ctx context.Context, customerID, userID, kind string) error {
	if kind == "" {
		return nil
	}
	return r.record(ctx, customerID, userID, kind)
}

// Named records a user a backend said it acts for, as an authenticated user when there is
// no row yet. A backend may name any user, a guest of its own included, so this proves
// nothing about which kind they are and changes no row already there.
func (r *Users) Named(ctx context.Context, customerID, userID string) error {
	return r.record(ctx, customerID, userID, namedUser)
}

// namedUser is what a user a backend named is held with: there is a row, of a kind this
// does not know.
const namedUser = "named"

// record writes a sighting down unless one held for the user settles it, and holds the
// user with the sighting.
func (r *Users) record(ctx context.Context, customerID, userID, sighting string) error {
	if customerID == "" || userID == "" {
		return nil
	}
	if held, found := r.seen.Value(customerID, userID); found && settles(held, sighting) {
		return nil
	}

	name := userKey(customerID, userID)
	redis := r.store.sharedRedis()
	if redis != nil {
		// Read through the client-side cache, which is what puts the key under Redis's
		// tracking: a later claim deleting it drops this entry on every replica holding
		// one. A Redis that cannot be reached is a slow deployment and not a broken one,
		// so the answer there is the same as a miss, within cacheWait.
		read, cancel := context.WithTimeout(ctx, cacheWait)
		found, err := redis.DoCache(read, redis.B().Get().Key(name).Cache(), userTTL).ToString()
		cancel()
		switch {
		case err == nil && found != "" && settles(found, sighting):
			r.hold(ctx, customerID, userID, found)
			return nil
		case err != nil && !rueidis.IsRedisNil(err):
			slog.Default().Error("could not read a user from redis, writing postgres",
				"customer", customerID, "user", userID, "error", err)
		}
	}

	var err error
	switch sighting {
	case UserKindAuthenticated:
		err = r.RecordAccount(ctx, customerID, userID)
	case namedUser:
		err = r.Insert(ctx, &User{CustomerID: customerID, ID: userID, Kind: UserKindAuthenticated})
	default:
		err = r.Insert(ctx, &User{CustomerID: customerID, ID: userID, Kind: sighting})
	}
	if err != nil {
		return err
	}

	if redis != nil {
		// Writing the key ends this process's tracking of it, which the read in hold starts again.
		// Redis sends this process the invalidation of its own copy after the SET's reply. Once
		// the EXISTS after it is answered that has been applied (as in Store.forget), so hold does
		// not read the old copy and drop the user.
		set := redis.B().Set().Key(name).Value(sighting).Ex(userTTL).Build()
		write, cancel := context.WithTimeout(ctx, cacheWait)
		err := redis.DoMulti(write, set, redis.B().Exists().Key(name).Build())[0].Error()
		cancel()
		if err == nil {
			r.hold(ctx, customerID, userID, sighting)
			return nil
		}
		slog.Default().Error("could not cache a user in redis",
			"customer", customerID, "user", userID, "error", err)
	}
	r.seen.Keep(customerID, userID, sighting, time.Time{})
	return nil
}

// hold holds a user with what Redis holds for them, unless it no longer holds that.
//
// The user is held before the read that tracks the key: held after it, an invalidation sent in
// between would find nothing to drop. A read answered by this process's own copy needs no round
// trip, and is safe: the copy is dropped before the user is (invalidate).
func (r *Users) hold(ctx context.Context, customerID, userID, value string) {
	r.seen.Keep(customerID, userID, value, time.Time{})
	redis := r.store.sharedRedis()
	read, cancel := context.WithTimeout(ctx, cacheWait)
	defer cancel()
	found, err := redis.DoCache(read, redis.B().Get().Key(userKey(customerID, userID)).Cache(), userTTL).ToString()
	if err == nil && found == value {
		return
	}
	r.seen.Remove(customerID, userID)
	if err != nil && !rueidis.IsRedisNil(err) {
		slog.Default().Error("could not read a user back from redis",
			"customer", customerID, "user", userID, "error", err)
	}
}

// settles reports whether a user held with held needs no write for sighting. Only a user's
// own full token changes a row already there, so only having written that settles it; any
// other sighting is settled by the row being there at all.
func settles(held, sighting string) bool {
	return sighting != UserKindAuthenticated || held == UserKindAuthenticated
}

// Email records the address a backend said a user signs in with (SetEmail), and does
// nothing for the address this process last wrote for them within emailTTL. A backend may
// say it on every request. A changed address is written at once; one another replica wrote
// under this one is written again here once emailTTL ends, so a row lags for at most that
// long. A failed write is not held, so the next request tries again. Like Seen, a request
// naming no app or no user records nothing.
func (r *Users) Email(ctx context.Context, customerID, userID, email string) error {
	if customerID == "" || userID == "" || email == "" {
		return nil
	}
	if r.emails.Holds(customerID, userID, email, time.Now()) {
		return nil
	}

	lock := r.emailLock(customerID, userID)
	lock.Lock()
	defer lock.Unlock()
	now := time.Now()
	if err := r.SetEmail(ctx, customerID, userID, email); err != nil {
		return err
	}
	r.emails.Keep(customerID, userID, email, now.Add(r.emailTTL))
	return nil
}

// emailLock is the lock one user's email writes take.
func (r *Users) emailLock(customerID, userID string) *sync.Mutex {
	hash := fnv.New32a()
	hash.Write([]byte(userKey(customerID, userID)))
	return &r.emailLocks[hash.Sum32()%emailStripes]
}

// Minted reports whether a user is a guest this router handed out (InsertGuest), or one an import
// brought as another deployment's, through the cache. That is the importing app's word, which only
// keeps its own user from more. A claimed guest still is one, and an account or no row is none.
//
// The answer is held in Redis and in the copy each router tracks of it, or in an LRU of this
// router's with no Redis, where nothing tells it of another router's write. One that could
// become yes (InsertGuest, a moved row) is kept for missTTL, and a failed read is not kept.
func (r *Users) Minted(ctx context.Context, customerID, userID string) (bool, error) {
	return cachedFact(ctx, r.store, r.minted, customerID, userID, mintedKey(customerID, userID), cacheTTL, missTTL,
		func(ctx context.Context) (bool, error) { return r.readMinted(ctx, customerID, userID) })
}

func (r *Users) readMinted(ctx context.Context, customerID, userID string) (bool, error) {
	minted, err := r.store.global.NewSelect().Model((*User)(nil)).
		Where("customer_id = ?", customerID).
		Where("id = ?", userID).
		Where("kind = ?", UserKindGuest).
		Where("minted OR minted_elsewhere").
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: minted guest: %w", err))
	}
	return minted, nil
}

// mintedKey names the cached answer of Minted.
func mintedKey(customerID, userID string) string {
	return cacheKey("minted-guest", customerID, userID)
}

// forgetMinted drops what Minted holds for a user, here and in every router on this Redis.
func (r *Users) forgetMinted(ctx context.Context, customerID, userID string) {
	r.minted.Remove(customerID, userID)
	r.store.forget(ctx, mintedKey(customerID, userID))
}

// Forget drops a user from the caches, for one whose row has changed under them. Deleting
// the key is what tells the other replicas and, once Redis sends it, this process's own
// copy too; this process's entry goes directly as well, so it does not wait for that.
func (r *Users) Forget(ctx context.Context, customerID, userID string) {
	r.seen.Remove(customerID, userID)
	redis := r.store.sharedRedis()
	if redis == nil {
		return
	}
	if err := redis.Do(ctx, redis.B().Del().Key(userKey(customerID, userID)).Build()).Error(); err != nil {
		slog.Default().Error("could not drop a cached user",
			"customer", customerID, "user", userID, "error", err)
	}
}

// forgetKeys drops from this process the users named among keys a write has just deleted
// (Store.forget), at once, as Forget does; with no Redis, nothing else would.
func (r *Users) forgetKeys(names []string) {
	for _, name := range names {
		if customerID, userID, named := splitUserKey(name); named {
			r.seen.Remove(customerID, userID)
			r.minted.Remove(customerID, userID)
		}
	}
}

// invalidate drops what Redis says has gone stale. A nil message is Redis saying it
// cannot name the keys -- a flush, or a restart -- so everything goes.
func (r *Users) invalidate(messages []rueidis.RedisMessage) {
	if messages == nil {
		r.seen.Clear()
		return
	}
	for _, message := range messages {
		name, err := message.ToString()
		if err != nil {
			continue
		}
		customerID, userID, named := splitUserKey(name)
		if named {
			r.seen.Remove(customerID, userID)
		}
	}
}

// userKey names one user's key. The customer comes first because it carries no colon,
// which is what lets splitUserKey read the pair back out of an invalidation naming only
// the key.
func userKey(customerID, userID string) string {
	return usersPrefix + customerID + ":" + userID
}

func splitUserKey(name string) (customerID, userID string, ok bool) {
	rest, found := strings.CutPrefix(name, usersPrefix)
	if !found {
		return "", "", false
	}
	customerID, userID, found = strings.Cut(rest, ":")
	return customerID, userID, found
}

// seen is which users this process has already recorded, newest use first.
//
// Partitioned per app so one app's users can be dropped without walking the rest, and
// because a user id belongs to the customer: the same id under two apps is two people.
// Eviction is across the partitions rather than within them, so an app serving nobody
// holds nothing and a busy one gets the room, which is what a cache of a fixed total size
// means.
//
// No row is kept beside the key, only the one value a write was made with (an email, or the
// sighting a user was recorded from), since the only question asked of this is whether that
// write was already made, and keeping the row as well would be keeping a copy of Postgres
// that nothing reads.
type seen struct {
	capacity int

	mu    sync.Mutex
	order *list.List
	apps  map[string]map[string]*list.Element
	// removals counts the users dropped, so a read begun before one is not kept after it.
	removals uint64
}

// seenEntry is what one element of the recency list holds, which is enough to find its
// partition when it is evicted from the far end, and the value the user is held with until
// expires. A zero expires never ends.
type seenEntry struct {
	app     string
	id      string
	value   string
	expires time.Time
}

func newSeen(capacity int) *seen {
	if capacity <= 0 {
		capacity = seenCapacity
	}
	return &seen{
		capacity: capacity,
		order:    list.New(),
		apps:     map[string]map[string]*list.Element{},
	}
}

// Value returns what a user is held with, and marks them as the most recently used.
func (s *seen) Value(app, id string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, found := s.apps[app][id]
	if !found {
		return "", false
	}
	s.order.MoveToFront(element)
	return element.Value.(seenEntry).value, true
}

// Holds reports whether a user is held with value and their hold has not expired by now,
// and marks them as the most recently used.
func (s *seen) Holds(app, id, value string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, found := s.apps[app][id]
	if !found {
		return false
	}
	held := element.Value.(seenEntry)
	if held.value != value || (!held.expires.IsZero() && !now.Before(held.expires)) {
		return false
	}
	s.order.MoveToFront(element)
	return true
}

// Keep holds a user with value until expires, in place of what they were held with,
// evicting the least recently used one across every app when the cache is full.
func (s *seen) Keep(app, id, value string, expires time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keep(app, id, value, expires)
}

// keep is Keep. The caller holds the lock.
func (s *seen) keep(app, id, value string, expires time.Time) {
	held := seenEntry{app: app, id: id, value: value, expires: expires}
	if element, found := s.apps[app][id]; found {
		element.Value = held
		s.order.MoveToFront(element)
		return
	}

	partition, found := s.apps[app]
	if !found {
		partition = map[string]*list.Element{}
		s.apps[app] = partition
	}
	partition[id] = s.order.PushFront(held)

	for s.order.Len() > s.capacity {
		s.evict(s.order.Back())
	}
}

// Removals is how many users were dropped so far.
func (s *seen) Removals() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.removals
}

// KeepSince is Keep, unless a user was dropped since there were removals of them: the value was
// read before a write that may have changed it.
func (s *seen) KeepSince(removals uint64, app, id, value string, expires time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.removals == removals {
		s.keep(app, id, value, expires)
	}
}

// Remove drops a user, for one whose row has changed under them.
func (s *seen) Remove(app, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.removals++
	if element, found := s.apps[app][id]; found {
		s.evict(element)
	}
}

// Clear drops everything, which is what Redis asks for when it cannot say which keys went
// stale.
func (s *seen) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.order.Init()
	s.apps = map[string]map[string]*list.Element{}
}

// evict removes one element and the partition it empties. The caller holds the lock.
func (s *seen) evict(element *list.Element) {
	if element == nil {
		return
	}
	evicted := s.order.Remove(element).(seenEntry)
	partition := s.apps[evicted.app]
	delete(partition, evicted.id)
	if len(partition) == 0 {
		delete(s.apps, evicted.app)
	}
}
