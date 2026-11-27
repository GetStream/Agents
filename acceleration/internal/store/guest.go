package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ErrGuestSecretRefused is a refresh secret that is not the one the guest was last handed:
// wrong, already used, expired, or for a guest that has since been claimed.
var ErrGuestSecretRefused = errors.New("store: that refresh secret does not belong to an unclaimed guest")

// ErrGuestSecretReplayed is a refresh secret the guest already spent, presented again after
// GuestSecretReplayGrace: somebody else holds a copy of it. Both of the guest's secrets are
// cleared, so they come back only through the app's backend or their own token.
var ErrGuestSecretReplayed = errors.New("store: that refresh secret was already spent, so it leaked")

// GuestSecretTTL is how long a refresh secret works after it was issued or last rotated, so a
// secret taken from a device whose guest never comes back stops working.
const GuestSecretTTL = 30 * 24 * time.Hour

// GuestSecretReplayGrace is how long after a rotation the secret it spent is only refused, not
// taken as leaked. Two requests from one device carrying the same secret (two tabs, a retry)
// both pass the token mint before either rotates; the second then finds its secret spent a
// moment ago by the first, which is no theft.
const GuestSecretReplayGrace = 30 * time.Second

// NewGuestSecret returns a refresh secret to hand a guest and the hash to keep of it.
//
// 256 random bits, so the hash needs no salt or stretching: there is nothing to guess.
func NewGuestSecret() (secret string, hash []byte, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, stack.Wrap(err)
	}
	secret = base64.RawURLEncoding.EncodeToString(raw)
	return secret, GuestSecretHash(secret), nil
}

// GuestSecretHash is what is kept of a refresh secret.
func GuestSecretHash(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// RotateGuestSecret spends a guest's refresh secret and records the next one in its place,
// returning the guest. The next one expires GuestSecretTTL from now.
//
// One statement, so a secret is spent once: two requests replaying the same secret cannot both
// see it match. A guest that has been claimed is refused, since it has an account to sign in
// with now, and so is an expired secret.
//
// A secret that matches the one the last rotation spent, presented more than
// GuestSecretReplayGrace after it, is ErrGuestSecretReplayed and clears both secrets. Every
// other refusal is ErrGuestSecretRefused.
func (r *Users) RotateGuestSecret(ctx context.Context, customerID, id string, spent, next []byte) (User, error) {
	if customerID == "" || id == "" || len(spent) == 0 || len(next) == 0 {
		return User{}, stack.Wrap(errors.New("store: a customer, a guest id and both secret hashes are required"))
	}

	// SET reads the row as it was, so the previous hash is the one this spends.
	var guest User
	err := r.store.global.NewUpdate().Model(&guest).
		Set("previous_refresh_secret_hash = refresh_secret_hash").
		Set("refresh_secret_hash = ?", next).
		Set("refresh_secret_expires_at = now() + make_interval(secs => ?)", GuestSecretTTL.Seconds()).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("kind = ?", UserKindGuest).
		Where("claimed_by IS NULL").
		Where("refresh_secret_hash = ?", spent).
		Where("refresh_secret_expires_at > now()").
		Returning("*").
		Scan(ctx)
	if err == nil {
		return guest, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return User{}, stack.Wrap(fmt.Errorf("store: rotate guest secret: %w", err))
	}

	// The rotation that spent it set the expiry, so it was GuestSecretTTL before that. Read in
	// the same statement as the clear, so a rotation that lands between the two is seen.
	result, err := r.store.global.NewUpdate().Model((*User)(nil)).
		Set("refresh_secret_hash = NULL").
		Set("previous_refresh_secret_hash = NULL").
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("kind = ?", UserKindGuest).
		Where("claimed_by IS NULL").
		Where("previous_refresh_secret_hash = ?", spent).
		Where("refresh_secret_expires_at < now() + make_interval(secs => ?)", (GuestSecretTTL - GuestSecretReplayGrace).Seconds()).
		Exec(ctx)
	if err != nil {
		return User{}, stack.Wrap(fmt.Errorf("store: clear a replayed guest secret: %w", err))
	}
	if n, _ := result.RowsAffected(); n > 0 {
		return User{}, stack.Wrap(ErrGuestSecretReplayed)
	}
	return User{}, stack.Wrap(ErrGuestSecretRefused)
}

// SetGuestSecret replaces a guest's refresh secret without the old one, for a caller that
// already proved it may: the guest's own still-valid token, or the app's backend.
//
// No previous secret is kept: the one replaced may still be on another device of the guest's,
// and coming back with it is no sign that it leaked.
func (r *Users) SetGuestSecret(ctx context.Context, customerID, id string, hash []byte) error {
	if customerID == "" || id == "" || len(hash) == 0 {
		return stack.Wrap(errors.New("store: a customer, a guest id and a secret hash are required"))
	}

	result, err := r.store.global.NewUpdate().Model((*User)(nil)).
		Set("refresh_secret_hash = ?", hash).
		Set("previous_refresh_secret_hash = NULL").
		Set("refresh_secret_expires_at = now() + make_interval(secs => ?)", GuestSecretTTL.Seconds()).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("kind = ?", UserKindGuest).
		Where("claimed_by IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set guest secret: %w", err))
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return notAGuest(id, customerID)
	}
	return nil
}

// GiveGuestSecret records a refresh secret for a guest that holds none, and reports whether it
// did. For a guest refreshing with their own token: one that already holds a secret keeps it,
// since replacing it would strand the device or tab holding it, and would let whoever holds a
// leaked token take the guest's only way back once that token expires.
func (r *Users) GiveGuestSecret(ctx context.Context, customerID, id string, hash []byte) (bool, error) {
	if customerID == "" || id == "" || len(hash) == 0 {
		return false, stack.Wrap(errors.New("store: a customer, a guest id and a secret hash are required"))
	}

	result, err := r.store.global.NewUpdate().Model((*User)(nil)).
		Set("refresh_secret_hash = ?", hash).
		Set("previous_refresh_secret_hash = NULL").
		Set("refresh_secret_expires_at = now() + make_interval(secs => ?)", GuestSecretTTL.Seconds()).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("kind = ?", UserKindGuest).
		Where("claimed_by IS NULL").
		Where("refresh_secret_hash IS NULL").
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: give guest a secret: %w", err))
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

// SetGuestProfile records what the app's backend says a guest is called, so the name a
// transcript shows and the one a returning guest is handed agree with what Stream holds.
func (r *Users) SetGuestProfile(ctx context.Context, customerID, id, name string, custom map[string]any) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a guest id are required"))
	}
	custom = orEmptyMap(custom)

	result, err := r.store.global.NewUpdate().Model((*User)(nil)).
		Set("name = ?", name).
		Set("custom = ?", custom).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("kind = ?", UserKindGuest).
		Where("claimed_by IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set guest profile: %w", err))
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return notAGuest(id, customerID)
	}
	return nil
}
