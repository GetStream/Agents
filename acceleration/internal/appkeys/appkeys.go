// Package appkeys seals and opens secrets under an app's own data key. A data key is 32 random
// bytes for AES-256-GCM. A Cloud KMS key wraps it, and the router stores only the wrapped form
// (store.AppDataKeys). An unwrapped key is kept in this process's memory for a while, so most
// seals and opens make no KMS call.
package appkeys

import (
	"container/list"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

const (
	// defaultMaxKeys bounds the keys held, the least recently used dropped first. A held key
	// takes a few hundred bytes, so this many is a few MB.
	defaultMaxKeys = 10_000
	// retryEvery is the shortest wait between two KMS calls for one key while KMS is failing.
	// Without it, every open during an outage would wait out the KMS deadline before using the
	// key it already holds, and every seal of an app with no key held would wait it out before
	// it failed.
	retryEvery = 30 * time.Second
	// sealVersion is the version of the data key new secrets are sealed under. No key is
	// rotated yet, so an app has only the first one.
	sealVersion = 1
	// keySize is a data key's length: 32 bytes is AES-256.
	keySize = 32
)

// The counters have no app label, because a label per app would add one series per app in
// every region.
var (
	unwraps = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "router",
		Name:      "app_key_unwraps_total",
		Help:      "Calls to KMS to unwrap an app's data key.",
	})
	kmsErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "router",
		Name:      "app_key_kms_errors_total",
		Help:      "Calls to KMS to wrap or unwrap an app's data key that failed or ran out of time.",
	})
)

// ErrUnavailable says KMS did not unwrap the app's data key. Nothing is wrong with the secret,
// so a caller keeps the record as it is and fails only this use.
var ErrUnavailable = errors.New("appkeys: the key service is unavailable")

// ErrDoesNotOpen says the secret does not open under the app's data key: the blob belongs to
// another record or app, or the app has no data key of that version.
var ErrDoesNotOpen = errors.New("appkeys: the secret does not open under the app's data key")

// KMS wraps and unwraps data keys with one Cloud KMS key.
type KMS interface {
	// Wrap encrypts a plain key with aad as its additional authenticated data.
	Wrap(ctx context.Context, plaintext, aad []byte) ([]byte, error)
	// Unwrap decrypts ciphertext, a key that was wrapped with aad as its additional
	// authenticated data, and returns the plain key.
	Unwrap(ctx context.Context, ciphertext, aad []byte) ([]byte, error)
}

// Options configures whether Keys seals and how long it keeps an unwrapped key. TTL,
// StaleLimit and KMSTimeout are required; config.Defaults holds the router's values.
type Options struct {
	// Seal says new secrets are sealed under the app's data key. When it is off, Keys only
	// opens them.
	Seal bool
	// TTL is how long a key is used after KMS unwrapped it, before KMS is asked again.
	TTL time.Duration
	// StaleLimit is how long after its unwrap a key is still used while KMS fails.
	StaleLimit time.Duration
	// KMSTimeout bounds each call to KMS together with the database read or insert that goes
	// with it.
	KMSTimeout time.Duration
	// MaxKeys bounds how many keys are held. Zero is ten thousand.
	MaxKeys int
	// Now is the clock, for tests. Empty is time.Now.
	Now func() time.Time
	// Logger receives the log line for each unwrap and each failed wrap. Empty is slog.Default().
	Logger *slog.Logger
}

// Keys seals and opens secrets under the apps' data keys. One Keys serves every sealed record
// of the router, so all of them share its keys and its KMS client.
type Keys struct {
	store   *store.Store
	kms     KMS
	options Options

	mu    sync.Mutex
	held  map[keyID]*list.Element
	order *list.List
}

// keyID is one version of one app's data key.
type keyID struct {
	customerID string
	version    int
}

// heldKey is what Keys holds for one keyID. Its fields are read and written under Keys.mu.
type heldKey struct {
	id keyID
	// plain is the unwrapped key, nil until an unwrap succeeds. It is kept only in this
	// process's memory.
	plain     []byte
	unwrapped time.Time
	// failed is when KMS last failed to wrap or unwrap this key.
	failed time.Time
	// unwrapping is the unwrap in flight, nil when there is none.
	unwrapping *unwrap
}

// unwrap is one KMS call for a key. Every open that needs the key meanwhile waits on this call
// instead of making its own.
type unwrap struct {
	done  chan struct{}
	plain []byte
	err   error
}

// New returns a Keys that keeps wrapped data keys in db and wraps and unwraps them with kms.
func New(db *store.Store, kms KMS, options Options) (*Keys, error) {
	if db == nil || kms == nil {
		return nil, errors.New("appkeys: a store and a KMS are required")
	}
	if options.MaxKeys <= 0 {
		options.MaxKeys = defaultMaxKeys
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Keys{store: db, kms: kms, options: options, held: map[keyID]*list.Element{}, order: list.New()}, nil
}

// Open opens sealed, which was sealed with aad under version of customerID's data key. The
// blob is a nonce followed by the GCM ciphertext, the layout auth.Sealer uses.
func (k *Keys) Open(ctx context.Context, customerID string, version int, sealed, aad []byte) ([]byte, error) {
	plain, err := k.key(ctx, keyID{customerID: customerID, version: version})
	if err != nil {
		return nil, err
	}
	aead, err := newAEAD(customerID, plain)
	if err != nil {
		return nil, err
	}
	if len(sealed) < aead.NonceSize() {
		return nil, stack.Wrap(ErrDoesNotOpen)
	}
	opened, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], aad)
	if err != nil {
		return nil, stack.Wrap(ErrDoesNotOpen)
	}
	return opened, nil
}

// Seals says whether new secrets are sealed under the app's data key (Options.Seal).
func (k *Keys) Seals() bool {
	return k.options.Seal
}

// Seal seals plaintext with aad under customerID's data key, in the layout Open opens, and
// returns the blob and the key's version. An app with no data key gets one here.
func (k *Keys) Seal(ctx context.Context, customerID string, plaintext, aad []byte) ([]byte, int, error) {
	id := keyID{customerID: customerID, version: sealVersion}
	// A caller can seal under the router's key instead, so a seal does not wait out the KMS
	// deadline again for a key KMS failed on a moment ago. An open has no such choice, so it
	// still asks KMS.
	if k.failedLately(id) {
		return nil, 0, stack.Wrap(fmt.Errorf("%w: KMS failed on the data key of %s less than %s ago", ErrUnavailable, customerID, retryEvery))
	}
	plain, err := k.key(ctx, id)
	if errors.Is(err, ErrDoesNotOpen) {
		plain, err = k.create(ctx, id)
	}
	if err != nil {
		return nil, 0, err
	}
	aead, err := newAEAD(customerID, plain)
	if err != nil {
		return nil, 0, err
	}
	nonce := make([]byte, aead.NonceSize())
	_, _ = rand.Read(nonce)
	return aead.Seal(nonce, nonce, plaintext, aad), id.version, nil
}

// create makes id's key: KMS wraps a random key, and the wrapped form is stored. Two writes
// for one app can race here, on two replicas or in one. The store keeps the first key, and
// the write that lost uses that key and drops its own, so every secret of the app opens.
func (k *Keys) create(ctx context.Context, id keyID) ([]byte, error) {
	plain := make([]byte, keySize)
	_, _ = rand.Read(plain)
	inserted, err := k.put(ctx, id, plain)
	if err != nil {
		return nil, err
	}
	if !inserted {
		return k.key(ctx, id)
	}
	k.mu.Lock()
	held := k.hold(id)
	// The store now holds this key, so it is held as if KMS had just unwrapped it, and the first
	// open of a secret under it makes no KMS call.
	held.plain, held.unwrapped, held.failed = plain, k.options.Now(), time.Time{}
	k.mu.Unlock()
	k.options.Logger.InfoContext(ctx, "made an app's data key", "app_id", id.customerID, "version", id.version)
	return plain, nil
}

// put wraps plain and stores it as id's key, within the KMS deadline. It is false when the
// app already had a key of that version.
func (k *Keys) put(ctx context.Context, id keyID, plain []byte) (bool, error) {
	// The caller going away must not fail the wrap, since that is reported as KMS being down.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), k.options.KMSTimeout)
	defer cancel()
	wrapped, err := k.kms.Wrap(ctx, plain, wrapAAD(id.customerID, id.version))
	if err != nil {
		kmsErrors.Inc()
		// Callers answer with text of their own and drop the cause (core.KeyFault), so this
		// line is the one place the cause is logged.
		k.options.Logger.ErrorContext(ctx, "KMS could not wrap a new data key for an app",
			"app_id", id.customerID, "version", id.version, "error", err)
		k.mu.Lock()
		k.hold(id).failed = k.options.Now()
		k.mu.Unlock()
		return false, stack.Wrap(fmt.Errorf("%w: wrap a data key for %s: %w", ErrUnavailable, id.customerID, err))
	}
	return k.store.AppDataKeys.InsertIfAbsent(ctx, &store.AppDataKey{CustomerID: id.customerID, Version: id.version, WrappedKey: wrapped})
}

// key returns the plain key of id. It uses a held key younger than the TTL as it is. An older
// one is unwrapped again, and while that fails the older key is used up to the stale limit.
func (k *Keys) key(ctx context.Context, id keyID) ([]byte, error) {
	k.mu.Lock()
	held := k.hold(id)
	now := k.options.Now()
	age := now.Sub(held.unwrapped)
	usable := held.plain != nil && age < k.options.StaleLimit
	// The key is read under the lock, since an unwrap that finishes replaces it.
	plain := held.plain
	if plain != nil && age < k.options.TTL {
		k.mu.Unlock()
		return plain, nil
	}
	// While another open unwraps the key, or KMS failed a moment ago, the key unwrapped before
	// is used as it is, so these opens do not wait on KMS.
	if usable && (held.unwrapping != nil || now.Sub(held.failed) < retryEvery) {
		k.mu.Unlock()
		return plain, nil
	}
	if shared := held.unwrapping; shared != nil {
		k.mu.Unlock()
		select {
		case <-shared.done:
			return shared.plain, shared.err
		case <-ctx.Done():
			return nil, stack.Wrap(ctx.Err())
		}
	}
	shared := &unwrap{done: make(chan struct{})}
	held.unwrapping = shared
	k.mu.Unlock()
	return k.lead(ctx, held, shared)
}

// lead makes the unwrap that shared stands for. finish runs deferred, so a panic in the store
// or the KMS client still ends shared, and the opens that wait on it fail instead of waiting
// for as long as the process runs.
func (k *Keys) lead(ctx context.Context, held *heldKey, shared *unwrap) (plain []byte, err error) {
	// err keeps this value only when the unwrap panics.
	err = stack.Wrap(fmt.Errorf("%w: the unwrap of the data key of %s panicked", ErrUnavailable, held.id.customerID))
	defer func() { plain, err = k.finish(ctx, held, shared, plain, err) }()
	// The unwrap is shared, so one caller giving up must not fail the others that wait on it.
	return k.unwrap(context.WithoutCancel(ctx), held.id)
}

// finish ends shared, the unwrap of held that returned plain and err, and returns what the
// open that made it gets.
func (k *Keys) finish(ctx context.Context, held *heldKey, shared *unwrap, plain []byte, err error) ([]byte, error) {
	id := held.id
	k.mu.Lock()
	held.unwrapping = nil
	// The unwrap took a while, so the stale limit is checked again.
	now := k.options.Now()
	usable := held.plain != nil && now.Sub(held.unwrapped) < k.options.StaleLimit
	failure, unwrapped := err, held.unwrapped
	switch {
	case err == nil:
		held.plain, held.unwrapped = plain, now
	// A store that answers the version does not exist is not an outage, so the held key is
	// not used then.
	case usable && !errors.Is(err, ErrDoesNotOpen):
		held.failed = now
		plain, err = held.plain, nil
	// Nothing usable is held, so the next open asks KMS again. The time of the failure is kept
	// for failedLately.
	case errors.Is(err, ErrUnavailable):
		held.plain, held.failed = nil, now
	default:
		// The store has no such version or failed itself, so the next open asks again rather
		// than finding a failure here.
		k.drop(held)
	}
	shared.plain, shared.err = plain, err
	k.mu.Unlock()
	close(shared.done)

	// Every open of every app takes k.mu, so the lines are written after it is let go.
	switch {
	case failure == nil:
		k.options.Logger.InfoContext(ctx, "unwrapped an app's data key", "app_id", id.customerID, "version", id.version)
	case err == nil:
		k.options.Logger.WarnContext(ctx, "could not unwrap an app's data key again, so the key unwrapped before is used",
			"app_id", id.customerID, "version", id.version, "unwrapped_at", unwrapped, "error", failure)
	// Callers answer with text of their own and drop the cause (core.KeyFault), so this line
	// is the one place the cause is logged. A store that has no such key, or fails itself, is
	// not a KMS failure and gets no line here.
	case errors.Is(failure, ErrUnavailable):
		k.options.Logger.ErrorContext(ctx, "KMS could not unwrap an app's data key",
			"app_id", id.customerID, "version", id.version, "error", failure)
	}
	return plain, err
}

// unwrap reads id's wrapped key and asks KMS to unwrap it, within the KMS deadline.
func (k *Keys) unwrap(ctx context.Context, id keyID) ([]byte, error) {
	// The open runs under the connection's lock, so one deadline covers both the read and KMS.
	// A database with no free connection waits with no deadline of its own, and Cloud KMS's
	// own Decrypt deadline is 60 s.
	ctx, cancel := context.WithTimeout(ctx, k.options.KMSTimeout)
	defer cancel()
	key, err := k.store.AppDataKeys.Get(ctx, id.customerID, id.version)
	if errors.Is(err, store.ErrNoAppDataKey) {
		return nil, fmt.Errorf("%w: %w", ErrDoesNotOpen, err)
	}
	if err != nil {
		return nil, err
	}
	unwraps.Inc()
	plain, err := k.kms.Unwrap(ctx, key.WrappedKey, wrapAAD(id.customerID, id.version))
	if err != nil {
		kmsErrors.Inc()
		return nil, stack.Wrap(fmt.Errorf("%w: unwrap the data key of %s: %w", ErrUnavailable, id.customerID, err))
	}
	return plain, nil
}

// failedLately says KMS failed on id less than retryEvery ago, and no key that can still be used
// is held for it. It holds nothing new.
func (k *Keys) failedLately(id keyID) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	element, ok := k.held[id]
	if !ok {
		return false
	}
	held := element.Value.(*heldKey)
	now := k.options.Now()
	usable := held.plain != nil && now.Sub(held.unwrapped) < k.options.StaleLimit
	return !usable && now.Sub(held.failed) < retryEvery
}

// hold returns what is held for id, held from now on if it was not, and marks it the most
// recently used. Past MaxKeys, the least recently used key is dropped.
func (k *Keys) hold(id keyID) *heldKey {
	if element, ok := k.held[id]; ok {
		k.order.MoveToFront(element)
		return element.Value.(*heldKey)
	}
	held := &heldKey{id: id}
	k.held[id] = k.order.PushFront(held)
	if k.order.Len() > k.options.MaxKeys {
		k.drop(k.order.Back().Value.(*heldKey))
	}
	return held
}

// drop forgets held. An unwrap in flight for it still finishes for the opens that wait on it.
func (k *Keys) drop(held *heldKey) {
	if element, ok := k.held[held.id]; ok && element.Value == held {
		k.order.Remove(element)
		delete(k.held, held.id)
	}
}

// newAEAD is AES-256-GCM under plain, customerID's data key.
func newAEAD(customerID string, plain []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(plain)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("appkeys: the data key of %s: %w", customerID, err))
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("appkeys: the data key of %s: %w", customerID, err))
	}
	return aead, nil
}

// wrapAAD is the additional authenticated data an app's data key is wrapped with. It names
// the purpose, the app and the version, so a wrapped key copied to another app or version
// does not unwrap. The app is length-prefixed, so no two apps give the same bytes. Do not
// change this layout or the v1 prefix: keys already wrapped with it must keep unwrapping.
func wrapAAD(customerID string, version int) []byte {
	return fmt.Appendf(nil, "accelerate:app-data-key:v1:%d:%s:%d", len(customerID), customerID, version)
}
