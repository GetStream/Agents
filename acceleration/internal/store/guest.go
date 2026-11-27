package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ErrGuestSecretRefused is a refresh secret that is not the one the guest was last handed:
// wrong, expired, spent a moment ago, or for a guest that has since been claimed.
var ErrGuestSecretRefused = errors.New("store: that refresh secret does not belong to an unclaimed guest")

// ErrGuestSecretReplayed is a refresh secret of the guest's that was already spent, presented
// again: somebody else holds a copy of it. The guest's secret is cleared and their own token
// can give them no new one, so they come back only through the app's backend.
var ErrGuestSecretReplayed = errors.New("store: that refresh secret was already spent, so it leaked")

// GuestSecretTTL is how long a refresh secret works after it was issued or last rotated, so a
// secret taken from a device whose guest never comes back stops working.
const GuestSecretTTL = 30 * 24 * time.Hour

// GuestSecretReplayGrace is how long after a rotation the secret it spent is only refused, not
// taken as leaked. Two requests from one device carrying the same secret (two tabs, a retry)
// both pass the token mint before either rotates; the second then finds its secret spent a
// moment ago by the first, which is no theft.
const GuestSecretReplayGrace = 30 * time.Second

// guestSecretSeparator parts a secret's family from the rest. Base64url never holds it, so a
// secret without one is from before families: it has none.
const guestSecretSeparator = "."

// NewGuestSecret returns a refresh secret of a new family to hand a guest, and the hash to keep
// of it.
//
// A secret is <family>.<random>, 128 and 256 random bits, so the hashes need no salt or
// stretching: there is nothing to guess. Every rotation keeps the family, so a secret of the
// guest's family that is not the one they hold was spent, however many rotations ago.
func NewGuestSecret() (secret string, hash []byte, err error) {
	family, err := randomPart(16)
	if err != nil {
		return "", nil, err
	}
	return newGuestSecretIn(family)
}

// NextGuestSecret returns the secret that replaces spent, of the same family. A secret from
// before families starts one.
func NextGuestSecret(spent string) (secret string, hash []byte, err error) {
	family, _, found := strings.Cut(spent, guestSecretSeparator)
	if !found || family == "" {
		return NewGuestSecret()
	}
	return newGuestSecretIn(family)
}

func newGuestSecretIn(family string) (string, []byte, error) {
	random, err := randomPart(32)
	if err != nil {
		return "", nil, err
	}
	secret := family + guestSecretSeparator + random
	return secret, GuestSecretHash(secret), nil
}

func randomPart(size int) (string, error) {
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		return "", stack.Wrap(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// GuestSecretHash is what is kept of a refresh secret.
func GuestSecretHash(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// GuestSecretFamilyHash is what is kept of a refresh secret's family, nil for a secret from
// before families.
func GuestSecretFamilyHash(secret string) []byte {
	family, _, found := strings.Cut(secret, guestSecretSeparator)
	if !found || family == "" {
		return nil
	}
	sum := sha256.Sum256([]byte(family))
	return sum[:]
}

// RotateGuestSecret spends a guest's refresh secret and records next, which NextGuestSecret
// made from it, in its place, returning the guest. The next one expires GuestSecretTTL from now.
//
// One statement, so a secret is spent once: two requests replaying the same secret cannot both
// see it match. A guest that has been claimed is refused, since it has an account to sign in
// with now, and so is an expired secret.
//
// A secret of the guest's family that is not the one they hold, or the one the last rotation
// spent, is ErrGuestSecretReplayed (clearReplayedGuestSecret). Every other refusal is
// ErrGuestSecretRefused.
func (r *Users) RotateGuestSecret(ctx context.Context, customerID, id, spent, next string) (User, error) {
	if customerID == "" || id == "" || spent == "" || next == "" {
		return User{}, stack.Wrap(errors.New("store: a customer, a guest id and both secrets are required"))
	}

	// SET reads the row as it was, so the previous hash is the one this spends.
	var guest User
	err := r.store.global.NewUpdate().Model(&guest).
		Set("previous_refresh_secret_hash = refresh_secret_hash").
		Set("refresh_secret_hash = ?", GuestSecretHash(next)).
		Set("refresh_family_secret_hash = ?", GuestSecretFamilyHash(next)).
		Set("refresh_secret_expires_at = now() + make_interval(secs => ?)", GuestSecretTTL.Seconds()).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("kind = ?", UserKindGuest).
		Where("claimed_by IS NULL").
		Where("refresh_secret_hash = ?", GuestSecretHash(spent)).
		Where("refresh_secret_expires_at > now()").
		Returning("*").
		Scan(ctx)
	if err == nil {
		return guest, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return User{}, stack.Wrap(fmt.Errorf("store: rotate guest secret: %w", err))
	}

	replayed, err := r.clearReplayedGuestSecret(ctx, customerID, id, spent)
	if err != nil {
		return User{}, err
	}
	if replayed {
		return User{}, stack.Wrap(ErrGuestSecretReplayed)
	}
	return User{}, stack.Wrap(ErrGuestSecretRefused)
}

// clearReplayedGuestSecret clears the secret of the guest whose spent one this is, and reports
// whether it was: one of the family they hold that is not their secret, or the one the last
// rotation spent (a secret from before families has only that). The one the last rotation
// spent is no sign of a leak for GuestSecretReplayGrace after it.
//
// The secret held goes and the previous one stays, as the mark that keeps GiveGuestSecret from
// handing whoever replayed it, or whoever redeemed it first, a new one.
//
// The rotation time is read in the same statement as the clear, so a rotation that lands
// after RotateGuestSecret's own missed is seen: the rotation set the expiry, so it was
// GuestSecretTTL before that.
func (r *Users) clearReplayedGuestSecret(ctx context.Context, customerID, id, spent string) (bool, error) {
	hash, family := GuestSecretHash(spent), GuestSecretFamilyHash(spent)
	result, err := r.store.global.NewUpdate().Model((*User)(nil)).
		Set("previous_refresh_secret_hash = COALESCE(previous_refresh_secret_hash, refresh_secret_hash)").
		Set("refresh_secret_hash = NULL").
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("kind = ?", UserKindGuest).
		Where("claimed_by IS NULL").
		Where("refresh_secret_hash IS DISTINCT FROM ?", hash).
		Where("(refresh_family_secret_hash = ? OR previous_refresh_secret_hash = ?)", family, hash).
		Where("NOT (COALESCE(previous_refresh_secret_hash = ?, false) AND refresh_secret_expires_at >= now() + make_interval(secs => ?))",
			hash, (GuestSecretTTL - GuestSecretReplayGrace).Seconds()).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: clear a replayed guest secret: %w", err))
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

// SetGuestSecret replaces a guest's refresh secret without the old one, for a caller that
// already proved it may: the guest's own still-valid token, or the app's backend. The secret
// starts a family of its own.
//
// No previous secret is kept: the one replaced may still be on another device of the guest's,
// and coming back with it is no sign that it leaked. That also lifts the mark a replay left,
// so the backend is the way back for a guest whose secret leaked.
func (r *Users) SetGuestSecret(ctx context.Context, customerID, id, secret string) error {
	if customerID == "" || id == "" || secret == "" {
		return stack.Wrap(errors.New("store: a customer, a guest id and a secret are required"))
	}

	result, err := r.store.global.NewUpdate().Model((*User)(nil)).
		Set("refresh_secret_hash = ?", GuestSecretHash(secret)).
		Set("refresh_family_secret_hash = ?", GuestSecretFamilyHash(secret)).
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

// GiveGuestSecret records a refresh secret for a guest that holds none, or holds one that
// expired, and reports whether it did. For a guest refreshing with their own token: one that
// holds a working secret keeps it, since replacing it would strand the device or tab holding
// it, and would let whoever holds a leaked token take the guest's only way back once that token
// expires.
//
// A guest whose secret a replay cleared is given none: the token asking may be the one the
// leaked secret was redeemed for (clearReplayedGuestSecret).
func (r *Users) GiveGuestSecret(ctx context.Context, customerID, id, secret string) (bool, error) {
	if customerID == "" || id == "" || secret == "" {
		return false, stack.Wrap(errors.New("store: a customer, a guest id and a secret are required"))
	}

	result, err := r.store.global.NewUpdate().Model((*User)(nil)).
		Set("refresh_secret_hash = ?", GuestSecretHash(secret)).
		Set("refresh_family_secret_hash = ?", GuestSecretFamilyHash(secret)).
		Set("previous_refresh_secret_hash = NULL").
		Set("refresh_secret_expires_at = now() + make_interval(secs => ?)", GuestSecretTTL.Seconds()).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("kind = ?", UserKindGuest).
		Where("claimed_by IS NULL").
		Where("((refresh_secret_hash IS NULL AND previous_refresh_secret_hash IS NULL) OR " +
			"(refresh_secret_hash IS NOT NULL AND refresh_secret_expires_at <= now()))").
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
