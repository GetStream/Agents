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

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// KMS stands in for Cloud KMS. It wraps under a key of its own with AES-GCM and, like Cloud
// KMS, unwraps only with the AAD a key was wrapped with. It is safe for concurrent use.
type KMS struct {
	aead cipher.AEAD

	mu      sync.Mutex
	failure error
	unwraps int
}

// NewKMS is a KMS with a random key.
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

// Wrap is plaintext wrapped under the KMS's key with aad.
func (k *KMS) Wrap(plaintext, aad []byte) []byte {
	nonce := make([]byte, k.aead.NonceSize())
	_, _ = rand.Read(nonce)
	return k.aead.Seal(nonce, nonce, plaintext, aad)
}

// Unwrap opens what Wrap wrapped with the same aad, and counts the call.
func (k *KMS) Unwrap(_ context.Context, ciphertext, aad []byte) ([]byte, error) {
	k.mu.Lock()
	k.unwraps++
	failure := k.failure
	k.mu.Unlock()
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
