// Package appkeystest is an in-process KMS and the data keys it wraps, for tests of code that
// opens secrets with appkeys.
package appkeystest

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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

// Wrap returns plaintext wrapped under the KMS's key, with aad as its additional authenticated data.
func (k *KMS) Wrap(plaintext, aad []byte) []byte {
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
	if held != nil {
		select {
		case <-held:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if failure != nil {
		return nil, failure
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

// Fail makes every Unwrap fail with err, until Fail(nil).
func (k *KMS) Fail(err error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.failure = err
}

// Hold makes every Unwrap wait until Release.
func (k *KMS) Hold() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.held = make(chan struct{})
}

// Release lets the Unwraps waiting since Hold answer, and the ones after answer at once.
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
	require.NoError(t, db.AppDataKeys.Insert(t.Context(), &store.AppDataKey{
		CustomerID: customerID, Version: version, WrappedKey: kms.Wrap(key, WrapAAD(customerID, version)),
	}))
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
