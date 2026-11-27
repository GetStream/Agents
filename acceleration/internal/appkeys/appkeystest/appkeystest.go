// Package appkeystest is an in-process KMS and the data keys it wraps, for tests of code that
// opens secrets with appkeys.
package appkeystest

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// Options are cache settings for a test that does not test the cache: the router's defaults.
func Options() appkeys.Options {
	return appkeys.Options{TTL: 10 * time.Minute, StaleLimit: time.Hour, KMSTimeout: 3 * time.Second}
}

// KMS stands in for Cloud KMS. It wraps under a key of its own with AES-GCM and, like Cloud
// KMS, unwraps only with the AAD a key was wrapped with. It is safe for concurrent use.
type KMS struct {
	aead cipher.AEAD

	mu      sync.Mutex
	failure error
	unwraps int
	wraps   int
	// asked is each plaintext Wrap was called with, for a test that checks no log line holds it.
	asked [][]byte
	// held is a channel that Unwrap waits on while the KMS is held, and nil when Unwrap answers
	// at once.
	held chan struct{}
}

// NewKMS returns a KMS with a random key.
func NewKMS() *KMS {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return &KMS{aead: aead}
}

// Wrap returns plaintext wrapped under the KMS's key, with aad as its additional
// authenticated data, and counts the call. It fails and waits as Unwrap does.
func (k *KMS) Wrap(ctx context.Context, plaintext, aad []byte) ([]byte, error) {
	k.mu.Lock()
	k.wraps++
	k.asked = append(k.asked, bytes.Clone(plaintext))
	failure, held := k.failure, k.held
	k.mu.Unlock()
	if err := wait(ctx, held, failure); err != nil {
		return nil, err
	}
	return k.wrap(plaintext, aad), nil
}

// wrap is Wrap with no failure, wait or count, for a test that stores a key itself.
func (k *KMS) wrap(plaintext, aad []byte) []byte {
	nonce := make([]byte, k.aead.NonceSize())
	_, _ = rand.Read(nonce)
	return k.aead.Seal(nonce, nonce, plaintext, aad)
}

// Unwrap opens what Wrap wrapped with the same aad, and counts the call. While the KMS is
// held, it waits for Release or for ctx to end, as a Cloud KMS that does not answer does.
func (k *KMS) Unwrap(ctx context.Context, ciphertext, aad []byte) ([]byte, error) {
	k.mu.Lock()
	k.unwraps++
	failure, held := k.failure, k.held
	k.mu.Unlock()
	if err := wait(ctx, held, failure); err != nil {
		return nil, err
	}
	if len(ciphertext) < k.aead.NonceSize() {
		return nil, errors.New("appkeystest: decryption failed")
	}
	plaintext, err := k.aead.Open(nil, ciphertext[:k.aead.NonceSize()], ciphertext[k.aead.NonceSize():], aad)
	if err != nil {
		return nil, errors.New("appkeystest: decryption failed")
	}
	return plaintext, nil
}

// wait acts like a KMS that does not answer: when held is set, it blocks until held is closed or ctx
// ends. It then returns failure.
func wait(ctx context.Context, held chan struct{}, failure error) error {
	if held != nil {
		select {
		case <-held:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return failure
}

// Fail makes every Wrap and Unwrap fail with err, until Fail(nil).
func (k *KMS) Fail(err error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.failure = err
}

// Hold makes every Wrap and Unwrap wait until Release.
func (k *KMS) Hold() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.held = make(chan struct{})
}

// Release lets the calls waiting since Hold answer, and the ones after answer at once.
func (k *KMS) Release() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.held != nil {
		close(k.held)
		k.held = nil
	}
}

// Unwraps is how many times Unwrap was called.
func (k *KMS) Unwraps() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.unwraps
}

// Wraps is how many times Wrap was called.
func (k *KMS) Wraps() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.wraps
}

// Asked is every plaintext Wrap was called with, including the calls that failed.
func (k *KMS) Asked() [][]byte {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.asked)
}

// WrapAAD is the AAD a data key is wrapped with. It is written out here rather than taken
// from appkeys, so a test fails when appkeys changes the AAD that keys already stored need.
func WrapAAD(customerID string, version int) []byte {
	return fmt.Appendf(nil, "accelerate:app-data-key:v1:%d:%s:%d", len(customerID), customerID, version)
}

// AddKey stores a new random data key for the app at version, wrapped by kms, and returns
// the plain key.
func AddKey(t testing.TB, db *store.Store, kms *KMS, customerID string, version int) []byte {
	t.Helper()
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	inserted, err := db.AppDataKeys.InsertIfAbsent(t.Context(), &store.AppDataKey{
		CustomerID: customerID, Version: version, WrappedKey: kms.wrap(key, WrapAAD(customerID, version)),
	})
	require.NoError(t, err)
	require.True(t, inserted, "%s already has a data key of version %d", customerID, version)
	return key
}

// Seal seals plaintext with aad under key, as appkeys lays out a sealed secret: the nonce,
// then the GCM ciphertext.
func Seal(t testing.TB, key, plaintext, aad []byte) []byte {
	t.Helper()
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	aead, err := cipher.NewGCM(block)
	require.NoError(t, err)
	nonce := make([]byte, aead.NonceSize())
	_, _ = rand.Read(nonce)
	return aead.Seal(nonce, nonce, plaintext, aad)
}

// SealFallbacks is router_app_key_seal_fallbacks_total for one kind of record, as the router
// serves it. The name is written out here, so a test fails when appkeys renames the series.
func SealFallbacks(t testing.TB, kind string) float64 {
	return counted(t, "router_app_key_seal_fallbacks_total", kind)
}

// KEKOpens is router_sealed_kek_opens_total for one kind of record, as the router serves it.
func KEKOpens(t testing.TB, kind string) float64 {
	return counted(t, "router_sealed_kek_opens_total", kind)
}

// counted reads the counter name with the record label kind from the default registry. It is 0
// while nothing has counted that kind, since a labelled series appears on its first count.
func counted(t testing.TB, name, kind string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "record" && label.GetValue() == kind {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
