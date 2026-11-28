package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// MaxEventDestinations is how many event destinations a customer may have for one connector:
// the three Vercel Connect allows a connector («A connector can have up to three trigger
// destinations», https://vercel.com/docs/connect/concepts/triggers, opened October 6, 2026).
// One provider event is one send to each, so the cap bounds the fan-out of one event.
const MaxEventDestinations = 3

// eventDestinationLockSeed is the second argument to hashtextextended for the lock creates of
// one customer's destinations of one connector take turns under, so two creates at once cannot
// both find room for a third. 875 is this table's issue, AI-875, as definitionLockSeed's 833 is
// its own.
const eventDestinationLockSeed = 875

const (
	defaultEventDestinationLimit = 25
	maxEventDestinationLimit     = 200
)

// What an event destination is sent (EventDestination.Forward).
const (
	// ForwardUnhandled is a delivery the router acted on in no way: no signal, and no message
	// an agent of the customer answers.
	ForwardUnhandled = "unhandled"
	// ForwardAll is every verified delivery but a URL handshake.
	ForwardAll = "all"
)

// ErrNoEventDestination says the customer has no such destination for the connector.
var ErrNoEventDestination = errors.New("store: no such event destination")

// ErrEventDestinationsFull says the customer already has MaxEventDestinations for the connector.
var ErrEventDestinationsFull = errors.New("store: the connector has as many event destinations as it may")

// EventDestination is one customer URL a connector's raw provider events are forwarded to,
// with its signing secret sealed by the caller.
//
// A secret under the app's data key keeps the router's current key version in its KEK version
// column, since the table's CHECK asks for one beside every sealed secret. Its data key version
// column says which key sealed it.
type EventDestination struct {
	bun.BaseModel `bun:"table:connector_event_destinations,alias:ced"`

	ID          string `bun:"id,pk"`
	CustomerID  string `bun:"customer_id,notnull"`
	ConnectorID string `bun:"connector_id,notnull"`
	URL         string `bun:"url,notnull"`
	Forward     string `bun:"forward,notnull"`
	// SecretSealed is the signing secret, sealed under KEKVersion, or under the app's data key
	// when SecretDataKeyVersion is set.
	SecretSealed []byte `bun:"secret_sealed,notnull"`
	KEKVersion   int    `bun:"kek_version,notnull"`
	// SecretDataKeyVersion is the version of the app's data key that sealed SecretSealed. Nil
	// means the router's key sealed it.
	SecretDataKeyVersion *int `bun:"secret_data_key_version"`
	// PreviousSecretSealed is the secret the last rotation replaced, which still signs until
	// PreviousUntil. Nil when none does.
	PreviousSecretSealed []byte `bun:"previous_secret_sealed"`
	PreviousKEKVersion   int    `bun:"previous_kek_version,notnull"`
	// PreviousSecretDataKeyVersion is SecretDataKeyVersion for PreviousSecretSealed.
	PreviousSecretDataKeyVersion *int       `bun:"previous_secret_data_key_version"`
	PreviousUntil                *time.Time `bun:"previous_until"`
	CreatedAt                    time.Time  `bun:"created_at,notnull"`
	UpdatedAt                    time.Time  `bun:"updated_at,notnull"`
}

// Validate refuses a destination the table refuses, or one nothing could be sent to or signed for.
func (d *EventDestination) Validate() error {
	return validate("event destination").
		required("id", d.ID).
		required("customer_id", d.CustomerID).
		required("connector_id", d.ConnectorID).
		required("url", d.URL).
		oneOf("forward", d.Forward, ForwardUnhandled, ForwardAll).
		check(len(d.SecretSealed) > 0, "secret_sealed", "is empty").
		check(d.KEKVersion >= 1, "kek_version", "is below 1").
		done()
}

// EventDestinationFilter picks a page of a customer's destinations of one connector.
type EventDestinationFilter struct {
	ConnectorID string
	Limit       int
	// After is the last destination of the previous page.
	After *EventDestinationPosition
}

// EventDestinationPosition is where a page of event destinations ended, newest first.
type EventDestinationPosition struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"id"`
}

// EventDestinationLimit is the page size a destination list uses for the limit asked for.
// Select returns one row more than this, so a caller can tell the page is not the
// last without counting.
func EventDestinationLimit(asked int) int {
	return ClampLimit(asked, defaultEventDestinationLimit, maxEventDestinationLimit)
}

// EventDestinations are the customer URLs connectors' raw events are forwarded to.
type EventDestinations struct {
	store *Store
}

// Insert stores a new destination, unless the customer already has
// MaxEventDestinations for the connector, which is ErrEventDestinationsFull. The count and the
// insert run under a transaction-scoped advisory lock on the customer and connector, so two
// creates at once take turns.
func (r *EventDestinations) Insert(ctx context.Context, destination *EventDestination) error {
	if err := destination.Validate(); err != nil {
		return stack.Wrap(err)
	}
	// Truncated to what Postgres keeps, so the cursor a page hands out finds this row again.
	now := time.Now().UTC().Truncate(time.Microsecond)
	destination.CreatedAt, destination.UpdatedAt = now, now
	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockUntilCommit(ctx, tx, destination.CustomerID+"/"+destination.ConnectorID, eventDestinationLockSeed); err != nil {
			return err
		}
		held, err := tx.NewSelect().Model((*EventDestination)(nil)).
			Where("customer_id = ?", destination.CustomerID).
			Where("connector_id = ?", destination.ConnectorID).
			Count(ctx)
		if err != nil {
			return err
		}
		if held >= MaxEventDestinations {
			return fmt.Errorf("%w: %d for %s", ErrEventDestinationsFull, MaxEventDestinations, destination.ConnectorID)
		}
		_, err = tx.NewInsert().Model(destination).Exec(ctx)
		return err
	}))
}

// Select returns a page of the customer's destinations of the connector, newest
// first, after the position after when it is set, with one row more than the page holds.
func (r *EventDestinations) Select(ctx context.Context, customerID string, filter EventDestinationFilter) ([]EventDestination, error) {
	if customerID == "" || filter.ConnectorID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a connector id are required"))
	}
	destinations := []EventDestination{}
	query := r.store.global.NewSelect().Model(&destinations).
		Where("customer_id = ?", customerID).
		Where("connector_id = ?", filter.ConnectorID)
	if after := filter.After; after != nil {
		query = query.Where("(created_at, id) < (?, ?)", after.CreatedAt, after.ID)
	}
	err := query.Order("created_at DESC", "id DESC").Limit(EventDestinationLimit(filter.Limit) + 1).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: list event destinations: %w", err))
	}
	return destinations, nil
}

// Delete removes one of the customer's destinations of the connector, and with
// it every forward to it not yet sent.
func (r *EventDestinations) Delete(ctx context.Context, customerID, connectorID, id string) error {
	result, err := r.store.global.NewDelete().Model((*EventDestination)(nil)).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("connector_id = ?", connectorID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete event destination: %w", err))
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return stack.Wrap(fmt.Errorf("%w: %s", ErrNoEventDestination, id))
	}
	return nil
}

// RotateSecret stores sealed as the destination's signing secret. The secret it replaces keeps
// signing beside it until previousUntil. A rotation during another one drops the oldest secret,
// so at most two sign. dataKeyVersion is the version of the app's data key that sealed it, or
// nil when the router's key did. It returns the destination as it is now.
func (r *EventDestinations) RotateSecret(ctx context.Context, customerID, connectorID, id string, sealed []byte, version int,
	dataKeyVersion *int, previousUntil time.Time) (EventDestination, error) {
	if len(sealed) == 0 || version < 1 {
		return EventDestination{}, stack.Wrap(errors.New("store: a rotation needs a sealed secret under a key version of 1 or more"))
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	var destination EventDestination
	err := r.store.global.NewUpdate().Model(&destination).
		Set("previous_secret_sealed = secret_sealed").
		Set("previous_kek_version = kek_version").
		Set("previous_secret_data_key_version = secret_data_key_version").
		Set("previous_until = ?", previousUntil.UTC()).
		Set("secret_sealed = ?", sealed).
		Set("kek_version = ?", version).
		Set("secret_data_key_version = ?", dataKeyVersion).
		Set("updated_at = ?", now).
		Where("id = ? AND customer_id = ? AND connector_id = ?", id, customerID, connectorID).
		Returning("*").
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return EventDestination{}, stack.Wrap(fmt.Errorf("%w: %s", ErrNoEventDestination, id))
	}
	if err != nil {
		return EventDestination{}, stack.Wrap(fmt.Errorf("store: rotate event destination secret: %w", err))
	}
	return destination, nil
}

// ResealSecret replaces the destination's signing secret, was, with the same secret sealed again
// under version dataKeyVersion of the app's data key. It changes nothing and returns false when
// the secret was replaced meanwhile, for example by a rotation.
func (r *EventDestinations) ResealSecret(ctx context.Context, id string, was, sealed []byte, version,
	dataKeyVersion int) (bool, error) {
	return r.reseal(ctx, "secret_sealed", "kek_version", "secret_data_key_version", id, was, sealed, version, dataKeyVersion)
}

// ResealPreviousSecret is ResealSecret for the secret the last rotation replaced.
func (r *EventDestinations) ResealPreviousSecret(ctx context.Context, id string, was, sealed []byte, version,
	dataKeyVersion int) (bool, error) {
	return r.reseal(ctx, "previous_secret_sealed", "previous_kek_version", "previous_secret_data_key_version", id, was, sealed,
		version, dataKeyVersion)
}

// reseal writes a secret sealed again into column only while column still holds was. The id is
// the table's primary key, so it names one destination of one app.
func (r *EventDestinations) reseal(ctx context.Context, column, versionColumn, dataKeyColumn, id string, was, sealed []byte,
	version, dataKeyVersion int) (bool, error) {
	if len(was) == 0 || len(sealed) == 0 || version < 1 || dataKeyVersion < 1 {
		return false, stack.Wrap(errors.New("store: reseal needs the old and the new sealed secret, and key versions of 1 or more"))
	}
	result, err := r.store.global.NewUpdate().Model((*EventDestination)(nil)).
		Set("? = ?", bun.Ident(column), sealed).
		Set("? = ?", bun.Ident(versionColumn), version).
		Set("? = ?", bun.Ident(dataKeyColumn), dataKeyVersion).
		Where("id = ?", id).
		Where("? = ?", bun.Ident(column), was).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: reseal event destination secret: %w", err))
	}
	affected, _ := result.RowsAffected()
	return affected > 0, nil
}
