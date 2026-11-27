// Package appkeys opens secrets sealed under an app's own data key. A data key is 32 random
// bytes for AES-256-GCM. A Cloud KMS key wraps it, and the router stores only the wrapped form
// (store.AppDataKeys), so every open asks KMS to unwrap the app's key.
package appkeys

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ErrUnavailable says KMS did not unwrap the app's data key. Nothing is wrong with the secret,
// so a caller keeps the record as it is and fails only this use.
var ErrUnavailable = errors.New("appkeys: the key service is unavailable")

// ErrDoesNotOpen says the secret does not open under the app's data key: the blob belongs to
// another record or app, or the app has no data key of that version.
var ErrDoesNotOpen = errors.New("appkeys: the secret does not open under the app's data key")

// Unwrapper decrypts a wrapped data key with the Cloud KMS key that wrapped it.
type Unwrapper interface {
	// Unwrap decrypts ciphertext, a key that was wrapped with aad as its additional
	// authenticated data, and returns the plain key.
	Unwrap(ctx context.Context, ciphertext, aad []byte) ([]byte, error)
}

// Keys opens secrets sealed under the apps' data keys. One Keys serves every sealed record
// of the router.
type Keys struct {
	store *store.Store
	kms   Unwrapper
}

// New returns a Keys that reads wrapped data keys from db and unwraps them with kms.
func New(db *store.Store, kms Unwrapper) (*Keys, error) {
	if db == nil || kms == nil {
		return nil, errors.New("appkeys: a store and a KMS are required")
	}
	return &Keys{store: db, kms: kms}, nil
}

// Open opens sealed, which was sealed with aad under version of customerID's data key. The
// blob is a nonce followed by the GCM ciphertext, the layout auth.Sealer uses.
func (k *Keys) Open(ctx context.Context, customerID string, version int, sealed, aad []byte) ([]byte, error) {
	key, err := k.store.AppDataKeys.Get(ctx, customerID, version)
	if errors.Is(err, store.ErrNoAppDataKey) {
		return nil, fmt.Errorf("%w: %w", ErrDoesNotOpen, err)
	}
	if err != nil {
		return nil, err
	}
	plain, err := k.kms.Unwrap(ctx, key.WrappedKey, wrapAAD(customerID, version))
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("%w: unwrap the data key of %s: %w", ErrUnavailable, customerID, err))
	}
	block, err := aes.NewCipher(plain)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("appkeys: the data key of %s: %w", customerID, err))
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("appkeys: the data key of %s: %w", customerID, err))
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

// wrapAAD is the additional authenticated data an app's data key is wrapped with. It names
// the purpose, the app and the version, so a wrapped key copied to another app or version
// does not unwrap. The app is length-prefixed, so no two apps give the same bytes. Do not
// change this layout or the v1 prefix: keys already wrapped with it must keep unwrapping.
func wrapAAD(customerID string, version int) []byte {
	return fmt.Appendf(nil, "accelerate:app-data-key:v1:%d:%s:%d", len(customerID), customerID, version)
}
