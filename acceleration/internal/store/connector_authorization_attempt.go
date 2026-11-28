package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Attempt kinds, from the architecture doc («Add» item 5): consent and reconnect for T17,
// step_up and admin_consent for T27.
const (
	AttemptConsent      = "consent"
	AttemptReconnect    = "reconnect"
	AttemptStepUp       = "step_up"
	AttemptAdminConsent = "admin_consent"
)

// ErrNoAuthorizationAttempt says the attempt is absent, expired, already consumed, or for a
// connection that was deleted. One error for all four, so a callback learns nothing about
// which.
var ErrNoAuthorizationAttempt = errors.New("store: authorization attempt is absent, expired or already used")

// ConnectorAuthorizationAttempt is one interactive acquisition in flight. Its state is sealed
// by the caller; only the hash of the OAuth state is searchable.
type ConnectorAuthorizationAttempt struct {
	bun.BaseModel `bun:"table:connector_authorization_attempts,alias:caa"`

	// ID is chosen by the caller, so it can be sealed into the attempt before the row exists.
	ID           string `bun:"id,pk"`
	CustomerID   string `bun:"customer_id,notnull"`
	ConnectionID string `bun:"connection_id,notnull"`
	Kind         string `bun:"kind,notnull"`
	StateHash    string `bun:"state_hash,notnull"`
	// AttemptSealed is sealed under KEKVersion, or under the app's data key when
	// AttemptDataKeyVersion is set.
	AttemptSealed []byte `bun:"attempt_sealed,notnull"`
	KEKVersion    int    `bun:"kek_version,notnull"`
	// AttemptDataKeyVersion is the version of the app's data key that sealed AttemptSealed. Nil
	// means the router's key sealed it. KEKVersion still holds the router's current version,
	// because the table's CHECK requires one beside every sealed attempt.
	AttemptDataKeyVersion *int       `bun:"attempt_data_key_version"`
	ExpiresAt             time.Time  `bun:"expires_at,notnull"`
	ConsumedAt            *time.Time `bun:"consumed_at"`
	CreatedAt             time.Time  `bun:"created_at,notnull"`
}

// Validate refuses an attempt no callback could finish: one without its keys, of an unknown kind,
// or not sealed under a key version.
func (a *ConnectorAuthorizationAttempt) Validate() error {
	return validate("connector authorization attempt").
		required("id", a.ID).
		required("customer_id", a.CustomerID).
		required("connection_id", a.ConnectionID).
		oneOf("kind", a.Kind, AttemptConsent, AttemptReconnect, AttemptStepUp, AttemptAdminConsent).
		required("state_hash", a.StateHash).
		check(len(a.AttemptSealed) > 0, "attempt_sealed", "is empty: an attempt is sealed").
		check(a.KEKVersion >= 1, "kek_version", "is below 1").
		check(!a.ExpiresAt.IsZero(), "expires_at", "is required").
		done()
}

// ConnectorAuthorizationAttempts are the interactive acquisitions in flight.
type ConnectorAuthorizationAttempts struct {
	store *Store
}

// Insert stores a sealed attempt for a live connection of the
// customer's, after deleting the customer's expired ones. Every read already ignores an
// expired attempt, so expiry is the earliest a row is dead and the cleanup needs no age of
// its own; a consumed one is deleted once it expires too.
func (r *ConnectorAuthorizationAttempts) Insert(ctx context.Context, attempt *ConnectorAuthorizationAttempt) error {
	if err := attempt.Validate(); err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if !attempt.ExpiresAt.After(now) {
		return errors.New("store: an attempt expires in the future")
	}
	if _, err := r.store.ConnectorConnections.Get(ctx, attempt.CustomerID, attempt.ConnectionID); err != nil {
		return err
	}

	if _, err := r.store.global.NewDelete().Model((*ConnectorAuthorizationAttempt)(nil)).
		Where("customer_id = ?", attempt.CustomerID).
		Where("expires_at <= now()").
		Exec(ctx); err != nil {
		return fmt.Errorf("store: clean expired authorization attempts: %w", err)
	}
	attempt.ConsumedAt = nil
	attempt.CreatedAt = now
	if _, err := r.store.global.NewInsert().Model(attempt).Exec(ctx); err != nil {
		return fmt.Errorf("store: create authorization attempt: %w", err)
	}
	return nil
}

// GetByState reads an open attempt by its OAuth state without
// consuming it, so the callback can check the browser that began it first. A callback
// carries no app credential, so the state, not a customer, is what finds the row.
func (r *ConnectorAuthorizationAttempts) GetByState(ctx context.Context, state string) (ConnectorAuthorizationAttempt, error) {
	if state == "" {
		return ConnectorAuthorizationAttempt{}, errors.New("store: an oauth state is required")
	}
	return r.open(ctx, "caa.state_hash = ?", AuthorizationStateHash(state))
}

// Get reads an open attempt by id, for the launch page and the
// browser handoff, which carry no app credential either.
func (r *ConnectorAuthorizationAttempts) Get(ctx context.Context, id string) (ConnectorAuthorizationAttempt, error) {
	if id == "" {
		return ConnectorAuthorizationAttempt{}, errors.New("store: an attempt id is required")
	}
	return r.open(ctx, "caa.id = ?", id)
}

// Consume claims an open attempt by its OAuth state, once. It
// is one statement: Postgres locks the row for the first UPDATE, and every other caller's
// UPDATE then re-reads it, finds consumed_at set and matches nothing. So of any number of
// callbacks racing with the same state exactly one gets the attempt.
func (r *ConnectorAuthorizationAttempts) Consume(ctx context.Context, state string) (ConnectorAuthorizationAttempt, error) {
	if state == "" {
		return ConnectorAuthorizationAttempt{}, errors.New("store: an oauth state is required")
	}
	var attempt ConnectorAuthorizationAttempt
	err := r.store.global.NewUpdate().Model(&attempt).
		Set("consumed_at = now()").
		Where("caa.state_hash = ?", AuthorizationStateHash(state)).
		ApplyQueryBuilder(attemptOpen).
		Returning("caa.*").
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ConnectorAuthorizationAttempt{}, ErrNoAuthorizationAttempt
	}
	if err != nil {
		return ConnectorAuthorizationAttempt{}, fmt.Errorf("store: consume authorization attempt: %w", err)
	}
	return attempt, nil
}

// HandOff replaces an open attempt's sealed blob with sealed, but only while the row still
// holds previous, the blob the caller read. Every seal has a fresh random nonce, under the
// router's key (auth.Sealer.SealWithAAD) and under the app's data key alike, so no two blobs
// are equal. When several handoffs race from the same read, exactly one wins.
// dataKeyVersion is the version of the app's data key that sealed it, or nil for the
// router's key.
func (r *ConnectorAuthorizationAttempts) HandOff(ctx context.Context, id string, previous, sealed []byte, kekVersion int,
	dataKeyVersion *int) error {
	if id == "" || len(previous) == 0 || len(sealed) == 0 || kekVersion < 1 {
		return errors.New("store: a handoff needs an attempt id, the blob it read and a new one under a key version of 1 or more")
	}
	result, err := r.store.global.NewUpdate().Model((*ConnectorAuthorizationAttempt)(nil)).
		Set("attempt_sealed = ?", sealed).
		Set("kek_version = ?", kekVersion).
		Set("attempt_data_key_version = ?", dataKeyVersion).
		Where("caa.id = ?", id).
		Where("caa.attempt_sealed = ?", previous).
		ApplyQueryBuilder(attemptOpen).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("store: hand off authorization attempt: %w", err)
	}
	replaced, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: hand off authorization attempt: %w", err)
	}
	if replaced == 0 {
		return ErrNoAuthorizationAttempt
	}
	return nil
}

// AuthorizationStateHash is how an OAuth state is stored and looked up: hashed, so the
// table never holds the bearer value a callback presents.
func AuthorizationStateHash(state string) string {
	hash := sha256.Sum256([]byte(state))
	return hex.EncodeToString(hash[:])
}

// attemptOpen keeps the attempts that are unconsumed, unexpired and for a live connection.
func attemptOpen(q bun.QueryBuilder) bun.QueryBuilder {
	return q.
		Where("caa.consumed_at IS NULL").
		Where("caa.expires_at > now()").
		Where("EXISTS (SELECT 1 FROM connector_connections AS cc WHERE cc.id = caa.connection_id AND cc.customer_id = caa.customer_id AND cc.deleted_at IS NULL)")
}

// open is the one attempt matching where that is unconsumed, unexpired and for a
// live connection.
func (r *ConnectorAuthorizationAttempts) open(ctx context.Context, where string, arg any) (ConnectorAuthorizationAttempt, error) {
	var attempt ConnectorAuthorizationAttempt
	err := r.store.global.NewSelect().Model(&attempt).
		Where(where, arg).
		ApplyQueryBuilder(attemptOpen).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ConnectorAuthorizationAttempt{}, ErrNoAuthorizationAttempt
	}
	if err != nil {
		return ConnectorAuthorizationAttempt{}, fmt.Errorf("store: authorization attempt: %w", err)
	}
	return attempt, nil
}
