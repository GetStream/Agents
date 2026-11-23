package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// CallBridge is what a vendor is told to do when the person it called picks up.
//
// Three vendors take a URL rather than a call plan when a call is placed, and fetch it on
// answer. This is the answer, parked between the two moments. The token is the whole of the
// fetch's authentication, so a bridge is read once and then gone.
type CallBridge struct {
	bun.BaseModel `bun:"table:call_bridges,alias:cb"`

	// Token is what the vendor puts in the url it fetches, and the only thing proving the
	// fetch is one this service asked for. Only its hash is kept (callTokenHash), so the
	// table never holds the value a fetch presents, and a bridge read back has no Token.
	Token      string `bun:"-"`
	TokenHash  string `bun:"token_hash,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	Vendor     string `bun:"vendor,notnull"`
	// TrunkURI is the SIP address the answered leg is transferred to.
	TrunkURI string `bun:"trunk_uri,notnull"`
	// TrunkUsername and the sealed TrunkPasswordSealed are the trunk's digest credentials,
	// set only for a vendor that can send them; TrunkKEKVersion is the key that sealed it.
	TrunkUsername       string `bun:"trunk_username,nullzero"`
	TrunkPasswordSealed []byte `bun:"trunk_password_sealed,nullzero"`
	TrunkKEKVersion     int    `bun:"trunk_password_kek_version,nullzero"`
	// InitialDigits are pressed at the person before the transfer.
	InitialDigits string `bun:"initial_digits,nullzero"`
	// CallID is the Stream call the leg is routed into, kept for the audit trail.
	CallID    string    `bun:"call_id,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	ExpiresAt time.Time `bun:"expires_at,notnull"`
}

// Validate refuses a bridge no vendor could be told to follow.
func (b *CallBridge) Validate() error {
	return validate("call bridge").
		required("token", b.Token).
		required("customer_id", b.CustomerID).
		required("vendor", b.Vendor).
		required("trunk_uri", b.TrunkURI).
		check(!b.ExpiresAt.IsZero(), "expires_at", "is required").
		done()
}

// CallBridges are bridges parked until a vendor fetches them, on each shard.
type CallBridges struct {
	store *Store
}

// Insert saves what a vendor should be told when the person it is calling answers.
func (r *CallBridges) Insert(ctx context.Context, bridge *CallBridge) error {
	if err := bridge.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if bridge.CreatedAt.IsZero() {
		bridge.CreatedAt = time.Now().UTC()
	}
	bridge.TokenHash = callTokenHash(bridge.Token)

	if _, err := r.store.shard.NewInsert().Model(bridge).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: park bridge: %w", err))
	}
	return nil
}

// Claim returns a parked bridge and removes it, so a token works exactly once.
//
// The read and the delete are one statement rather than two, because two vendors retrying
// the same fetch at once would otherwise both be told to bridge, and the second transfer
// would arrive at a trunk the first had already taken.
func (r *CallBridges) Claim(ctx context.Context, token string) (CallBridge, error) {
	if token == "" {
		return CallBridge{}, stack.Wrap(errors.New("store: a token is required"))
	}

	// The vendor's fetch lands on whichever shard is nearest it, not the one that parked it.
	// The bridge is on one shard, and its delete there is done whatever the others answer, so
	// a shard that did not answer fails the claim only when no other shard held it.
	lists, failed := answeringShards(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]CallBridge, error) {
		var claimed []CallBridge
		err := db.NewDelete().Model((*CallBridge)(nil)).
			Where("token_hash = ?", callTokenHash(token)).
			Where("expires_at > ?", time.Now().UTC()).
			Returning("*").
			Scan(ctx, &claimed)
		return claimed, err
	})
	claimed := slices.Concat(lists...)
	if len(claimed) > 0 {
		r.store.settledWithout(ctx, "claim bridge", failed)
		return claimed[0], nil
	}
	if err := errors.Join(failed...); err != nil {
		return CallBridge{}, stack.Wrap(fmt.Errorf("store: claim bridge: %w", err))
	}
	// One message for a token that never existed, one that has been used and one that has
	// expired: telling them apart would say whether a guessed token was ever real.
	return CallBridge{}, stack.Wrap(errors.New("store: that bridge is not waiting to be claimed"))
}

// Sweep removes bridges nobody claimed, which is what a call that rang out leaves behind.
// Each shard sweeps its own.
func (r *CallBridges) Sweep(ctx context.Context) (int64, error) {
	result, err := r.store.shard.NewDelete().Model((*CallBridge)(nil)).
		Where("expires_at <= ?", time.Now().UTC()).
		Exec(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: sweep bridges: %w", err))
	}
	swept, err := result.RowsAffected()
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: sweep bridges: %w", err))
	}
	return swept, nil
}

// callTokenHash is how a call's bearer token is stored and looked up: hashed, so the table
// never holds the value a vendor presents.
func callTokenHash(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}
