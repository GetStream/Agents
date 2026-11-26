package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// StreamTrunk is a Stream SIP trunk a customer's numbers of one vendor share in one Stream
// app, with the credentials the vendor presents to it. Its numbers are the numbers held on it
// (PhoneNumber.StreamTrunkID).
type StreamTrunk struct {
	bun.BaseModel `bun:"table:stream_trunks,alias:st"`

	// ID is Stream's id for the trunk.
	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	// StreamAppPK is the app the trunk is in. Zero, stored as NULL, is the deployment's own.
	StreamAppPK int64  `bun:"stream_app_pk,nullzero"`
	Vendor      string `bun:"vendor,notnull"`
	// Numbers are the numbers held on the trunk, oldest first, read by TrunkGroupTx.Trunks.
	Numbers        []string `bun:"numbers,array,scanonly"`
	BridgeURI      string   `bun:"bridge_uri,notnull"`
	BridgeUsername string   `bun:"bridge_username,notnull"`
	// BridgePasswordSealed is sealed once for the trunk, since every number on it shares it.
	// Empty for an own trunk made without a key to seal it under.
	BridgePasswordSealed     []byte `bun:"bridge_password_sealed,notnull"`
	BridgePasswordKEKVersion int    `bun:"bridge_password_kek_version,notnull"`
	// Own is a trunk made for its one number, before trunks were shared or without a key to
	// seal a shared one's password. Its password is sealed to the number, no other number
	// joins it, and a line adopts it into the group as a shared one.
	Own       bool      `bun:"own,notnull"`
	CreatedAt time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
}

// Validate refuses a trunk the table would refuse.
func (t *StreamTrunk) Validate() error {
	return validate("stream trunk").
		required("id", t.ID).
		required("customer_id", t.CustomerID).
		required("vendor", t.Vendor).
		done()
}

// StreamTrunks are the Stream SIP trunks customers' numbers share, in the shared database.
type StreamTrunks struct {
	store *Store
}

// ErrNoStreamTrunk says the customer has no Stream trunk with that id.
var ErrNoStreamTrunk = errors.New("store: no such stream trunk")

// TrunkGroup is the trunks numbers may share: one customer's, of one vendor, in one app.
type TrunkGroup struct {
	CustomerID string
	// StreamAppPK is the app, zero for the deployment's.
	StreamAppPK int64
	Vendor      string
}

// TrunkGroupTx is one trunk group's rows, read and written in a transaction that holds the
// group's lock.
type TrunkGroupTx struct {
	// ctx lives only as long as the InGroup call that made the TrunkGroupTx.
	ctx   context.Context
	tx    bun.Tx
	group TrunkGroup
}

// trunkGroupLockSeed keeps the trunk group lock apart from other advisory locks hashed from
// the same text.
const trunkGroupLockSeed = 0

// trunkGroupIdle is how long a trunk group's transaction may sit idle: longer than one call
// to Stream's API, which its client gives up on after 30 s.
const trunkGroupIdle = time.Minute

// Get returns one of a customer's Stream trunks by Stream's id.
func (r *StreamTrunks) Get(ctx context.Context, customerID, id string) (StreamTrunk, error) {
	var trunk StreamTrunk
	err := r.store.global.NewSelect().Model(&trunk).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return StreamTrunk{}, stack.Wrap(ErrNoStreamTrunk)
	}
	if err != nil {
		return StreamTrunk{}, stack.Wrap(fmt.Errorf("store: stream trunk: %w", err))
	}
	return trunk, nil
}

// OfSIPTrunks are the Stream trunks made in the app the pin names for numbers on customers' own
// SIP trunks, without their numbers. The deployment's own app is its id or no pin.
func (r *StreamTrunks) OfSIPTrunks(ctx context.Context, app int64) ([]StreamTrunk, error) {
	var trunks []StreamTrunk
	err := r.store.global.NewSelect().Model(&trunks).
		Where("vendor = ?", sipTrunkVendor).
		ApplyQueryBuilder(r.store.pinnedTo(app).where).
		OrderExpr("id").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: stream trunks of sip trunks: %w", err))
	}
	return trunks, nil
}

// InGroup runs fn in a transaction that holds a lock on the group, so two numbers
// joining at once do not both see room on a trunk and both open a new one.
func (r *StreamTrunks) InGroup(ctx context.Context, group TrunkGroup, fn func(ctx context.Context, tx *TrunkGroupTx) error) error {
	if group.CustomerID == "" || group.Vendor == "" {
		return stack.Wrap(errors.New("store: a trunk group needs a customer and a vendor"))
	}
	key := "stream_trunks:" + group.CustomerID + ":" + strconv.FormatInt(group.StreamAppPK, 10) + ":" + group.Vendor
	return r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// fn calls Stream between its statements, each call allowed as long as the serving
		// idle limit, so the transaction gets an idle limit that outlasts one. Another router
		// joining the group waits on the lock while those calls run: its wait is ended by the
		// statement limit rather than the serving lock limit, which is shorter than one call.
		if err := boundTransaction(ctx, tx, statementTimeout, trunkGroupIdle, trunkGroupIdle); err != nil {
			return stack.Wrap(fmt.Errorf("store: trunk group: %w", err))
		}
		if err := lockUntilCommit(ctx, tx, key, trunkGroupLockSeed); err != nil {
			return stack.Wrap(fmt.Errorf("store: lock trunk group: %w", err))
		}
		return fn(ctx, &TrunkGroupTx{ctx: ctx, tx: tx, group: group})
	})
}

// Trunks are the group's trunks, oldest first, each with its numbers.
func (t *TrunkGroupTx) Trunks() ([]StreamTrunk, error) {
	var trunks []StreamTrunk
	err := t.tx.NewSelect().Model(&trunks).
		ColumnExpr("st.*").
		ColumnExpr("ARRAY(SELECT pn.e164 FROM phone_numbers pn WHERE pn.customer_id = st.customer_id " +
			"AND pn.stream_trunk_id = st.id AND pn.released_at IS NULL ORDER BY pn.id) AS numbers").
		ApplyQueryBuilder(t.where).
		OrderExpr("created_at, id").
		Scan(t.ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: trunks: %w", err))
	}
	return trunks, nil
}

// SaveTrunk stores a trunk in the group, or replaces what is stored for it. A trunk of
// another group is refused rather than taken over.
func (t *TrunkGroupTx) SaveTrunk(trunk StreamTrunk) error {
	trunk.CustomerID, trunk.StreamAppPK, trunk.Vendor = t.group.CustomerID, t.group.StreamAppPK, t.group.Vendor
	trunk.BridgePasswordSealed = orEmpty(trunk.BridgePasswordSealed)
	if err := trunk.Validate(); err != nil {
		return stack.Wrap(err)
	}
	result, err := t.tx.NewInsert().Model(&trunk).
		On("CONFLICT (id) DO UPDATE").
		Set("bridge_uri = EXCLUDED.bridge_uri").
		Set("bridge_username = EXCLUDED.bridge_username").
		Set("bridge_password_sealed = EXCLUDED.bridge_password_sealed").
		Set("bridge_password_kek_version = EXCLUDED.bridge_password_kek_version").
		Set("own = EXCLUDED.own").
		Where("st.customer_id = EXCLUDED.customer_id").
		Where("st.vendor = EXCLUDED.vendor").
		Where("st.stream_app_pk IS NOT DISTINCT FROM EXCLUDED.stream_app_pk").
		Exec(t.ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save trunk: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save trunk: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(fmt.Errorf("store: trunk %s is another group's", trunk.ID))
	}
	return nil
}

// DeleteTrunk forgets one of the group's trunks.
func (t *TrunkGroupTx) DeleteTrunk(id string) error {
	_, err := t.tx.NewDelete().Model((*StreamTrunk)(nil)).
		Where("id = ?", id).
		ApplyQueryBuilder(t.where).
		Exec(t.ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete trunk: %w", err))
	}
	return nil
}

// Number is one of the customer's numbers held, locked until the transaction ends. A line
// being made for it in another app's group, which holds a different lock, then waits to
// record itself until this transaction is done, and finds the number released if it was.
func (t *TrunkGroupTx) Number(e164 string) (PhoneNumber, error) {
	return heldNumber(t.ctx, t.tx.NewSelect().For("UPDATE"), t.group.CustomerID, e164)
}

// ReleaseNumber marks one of the customer's numbers as given back.
func (t *TrunkGroupTx) ReleaseNumber(e164 string, at time.Time) error {
	return releaseNumber(t.ctx, t.tx, t.group.CustomerID, e164, at)
}

// SetLine records the trunk and rule a number held in the group is reached through, which
// puts it among the trunk's numbers.
func (t *TrunkGroupTx) SetLine(e164, trunkID, routeID string) error {
	result, err := t.tx.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("stream_trunk_id = ?", trunkID).
		Set("stream_route_id = ?", routeID).
		Set("stream_app_pk = ?", nullablePin(t.group.StreamAppPK)).
		Where("customer_id = ?", t.group.CustomerID).
		Where("vendor = ?", t.group.Vendor).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Exec(t.ctx)
	return t.heldOne(result, err, e164, "set line")
}

// ClearLine takes a number held in the group off its trunk and rule.
func (t *TrunkGroupTx) ClearLine(e164 string) error {
	result, err := t.tx.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("stream_trunk_id = NULL").
		Set("stream_route_id = NULL").
		Set("attached_at = NULL").
		Set("config_id = NULL").
		Where("customer_id = ?", t.group.CustomerID).
		Where("vendor = ?", t.group.Vendor).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Exec(t.ctx)
	return t.heldOne(result, err, e164, "clear line")
}

// Attach records that a number held in the group is attached on its own trunk, which it saves
// in the same transaction (PhoneNumbers.Attach).
func (t *TrunkGroupTx) Attach(e164 string, attached NumberAttachment) error {
	return attachNumber(t.ctx, t.tx, t.group.CustomerID, e164, attached)
}

// Detach stops a number held in the group from ringing its agent, and keeps its trunk and rule.
func (t *TrunkGroupTx) Detach(e164 string) error {
	result, err := t.tx.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("attached_at = NULL").
		Set("config_id = NULL").
		Where("customer_id = ?", t.group.CustomerID).
		Where("vendor = ?", t.group.Vendor).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Exec(t.ctx)
	return t.heldOne(result, err, e164, "detach")
}

// heldOne is an update's error, or one saying the number is not held in the group.
func (t *TrunkGroupTx) heldOne(result sql.Result, err error, e164, what string) error {
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: %s: %w", what, err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: %s: %w", what, err))
	}
	if affected == 0 {
		return stack.Wrap(fmt.Errorf("store: %s is not a %s number %s holds", e164, t.group.Vendor, t.group.CustomerID))
	}
	return nil
}

func (t *TrunkGroupTx) where(q bun.QueryBuilder) bun.QueryBuilder {
	q = q.Where("customer_id = ?", t.group.CustomerID).Where("vendor = ?", t.group.Vendor)
	if t.group.StreamAppPK == 0 {
		return q.Where("stream_app_pk IS NULL")
	}
	return q.Where("stream_app_pk = ?", t.group.StreamAppPK)
}
