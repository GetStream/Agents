package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// PhoneNumber is a number this service holds on a customer's behalf.
//
// A released number keeps its row: what it cost while it was held is still part of that
// month's bill after it is gone.
type PhoneNumber struct {
	bun.BaseModel `bun:"table:phone_numbers,alias:pn"`

	ID   int64  `bun:"id,pk,autoincrement"`
	E164 string `bun:"e164,notnull"`
	// Vendor is who it was bought from.
	Vendor  string `bun:"vendor,notnull"`
	Country string `bun:"country,notnull"`
	// Capabilities is what the number can carry: voice, sms, mms, fax.
	Capabilities []string `bun:"capabilities,array"`
	// MonthlyCostMicros is millionths of a dollar per month, charged whether or not the
	// number is used.
	MonthlyCostMicros int64  `bun:"monthly_cost_micros,notnull"`
	CustomerID        string `bun:"customer_id,notnull"`
	// StreamAppPK is the Stream app this was made in, and the one it is finished in. Zero,
	// stored as NULL, is the deployment's own app.
	StreamAppPK int64             `bun:"stream_app_pk,nullzero"`
	Tags        map[string]string `bun:"tags,type:jsonb,nullzero"`
	// VendorID is the vendor's own identifier, needed to release or reconfigure it.
	VendorID string `bun:"vendor_id,nullzero"`
	// StreamTrunkID is the SIP trunk calls to this number arrive on, empty until it has
	// been attached to one.
	StreamTrunkID string `bun:"stream_trunk_id,nullzero"`
	// StreamRouteID is the routing rule made beside the trunk, in the same app.
	StreamRouteID string `bun:"stream_route_id,nullzero"`
	// StreamCallID and StreamCallType are the Stream call the routing rule puts callers
	// in. They are what an arriving call is recognised by, since a webhook names the call
	// rather than the number.
	StreamCallID   string `bun:"stream_call_id,nullzero"`
	StreamCallType string `bun:"stream_call_type,nullzero"`
	// SIPTrunkID is the customer's own trunk a number with vendor sip_trunk is dialled
	// through. Empty for a bought number, and cleared when the number is released.
	SIPTrunkID string `bun:"sip_trunk_id,nullzero"`
	// BridgeURI is the address of the number's Stream trunk, read only by NumberByCallInApp
	// and NumberRungInApp.
	BridgeURI string `bun:"bridge_uri,scanonly"`
	// UseCaseID is the 10DLC use case the number sends as. Empty sends as the app's default.
	UseCaseID   string    `bun:"dlc_use_case_id,nullzero"`
	PurchasedAt time.Time `bun:"purchased_at,notnull"`
	// AttachedAt is when the number was attached, so its callers reach an agent. A number
	// can be on a trunk without it, to place calls only.
	AttachedAt *time.Time `bun:"attached_at"`
	ReleasedAt *time.Time `bun:"released_at"`

	// ConfigID is the agent config that answers the number's calls. Empty sends them to
	// dispatch. Only an attached number has one: detaching or releasing it clears it.
	ConfigID string `bun:"config_id,nullzero"`
}

// Validate refuses a number no vendor or trunk could be held for.
func (n *PhoneNumber) Validate() error {
	return validate("phone number").
		required("customer_id", n.CustomerID).
		required("e164", n.E164).
		phoneNumber("e164", n.E164).
		required("vendor", n.Vendor).
		nonNegative("monthly_cost_micros", float64(n.MonthlyCostMicros)).
		done()
}

// sipTrunkVendor is the vendor of a number on one of the customer's own trunks.
const sipTrunkVendor = "sip_trunk"

// ErrNumberHeld says the customer already holds the number, bought or on a trunk.
var ErrNumberHeld = errors.New("store: the customer already holds this number")

// ErrNotE164 says a number is not written in E.164 form, the only form a number is held in.
var ErrNotE164 = errors.New("store: a number is held in E.164 form, such as +15551234567")

// e164Pattern is E.164 as numbers are held here: a plus, a country code that does not start
// with 0, and 7 to 15 digits (ITU-T E.164 caps a number at 15). One number has exactly one
// form, because vendors drop what they do not need from it: Plivo, DIDWW and Vonage the
// plus, Bandwidth the plus and a leading 1. Held in two forms it would be two numbers here
// and one at the vendor.
var e164Pattern = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)

// IsE164 reports whether a number is written in the one form a number is held in.
func IsE164(number string) bool { return e164Pattern.MatchString(number) }

// ErrNoNumber is what GetByE164's error matches when the customer holds no such number, as
// opposed to the lookup failing. So do Attach's, Release's and SetAgent's.
var ErrNoNumber = errors.New("store: no such number")

// numberNotHeld keeps GetByE164's message as it was while matching ErrNoNumber.
type numberNotHeld struct{ message string }

func (e numberNotHeld) Error() string        { return e.message }
func (e numberNotHeld) Is(target error) bool { return target == ErrNoNumber }

// ErrNoStoredBridge is AttachedBridge's error for a number that is attached but whose trunk
// credentials were not kept: it was attached before they were, or arrived by a data move,
// which leaves the password behind. Attaching it again keeps them.
var ErrNoStoredBridge = errors.New("store: the number's trunk credentials were not kept")

// NumberAttachment is what attaching a number made in Stream, and where its calls go.
type NumberAttachment struct {
	TrunkID string
	RouteID string
	// StreamAppPK is the app the trunk and route were made in, zero for the deployment's.
	StreamAppPK int64
	CallType    string
	CallID      string
	// ConfigID is the agent the number keeps answering with. Empty leaves its agent as it is.
	ConfigID string
}

// AttachedBridge is the Stream trunk an attached number's calls go to, with its password
// still sealed.
type AttachedBridge struct {
	CustomerID string
	TrunkID    string
	// Own is a trunk made for the number alone, whose password is sealed to the number.
	Own            bool
	URI            string
	Username       string
	PasswordSealed []byte
	KEKVersion     int
}

// PhoneNumberFilter picks a customer's numbers. Released numbers are left out unless asked
// for, since what is normally wanted is what can be called.
type PhoneNumberFilter struct {
	IncludeReleased bool
}

// PhoneNumbers are the numbers customers hold, in the shared database.
type PhoneNumbers struct {
	store *Store
}

// Insert stores a number a customer now holds.
func (r *PhoneNumbers) Insert(ctx context.Context, number *PhoneNumber) error {
	if err := number.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if number.PurchasedAt.IsZero() {
		number.PurchasedAt = time.Now().UTC()
	}
	// A vendor that says nothing about what a number carries is not the same as a null
	// column, and the array is not nullable.
	if number.Capabilities == nil {
		number.Capabilities = []string{}
	}

	if _, err := r.store.global.NewInsert().Model(number).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: record number: %w", err))
	}
	return nil
}

// InsertOnSIPTrunk records a number the customer says is on one of their own trunks.
//
// A customer looking a number up gets one row, so holding it twice, once bought and once
// on a trunk, would make which one a call goes through an accident. That is refused here;
// the unique index only covers numbers on trunks.
func (r *PhoneNumbers) InsertOnSIPTrunk(ctx context.Context, number *PhoneNumber) error {
	number.Vendor = sipTrunkVendor
	if err := number.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if _, err := r.store.SIPTrunks.Get(ctx, number.CustomerID, number.SIPTrunkID); err != nil {
		return err
	}
	_, err := r.GetByE164(ctx, number.CustomerID, number.E164)
	if err == nil {
		return stack.Wrap(ErrNumberHeld)
	}
	if !errors.Is(err, ErrNoNumber) {
		return err
	}
	err = r.Insert(ctx, number)
	if isViolation(err, uniqueViolation) {
		return stack.Wrap(ErrNumberHeld)
	}
	// The trunk was deleted after it was looked up above.
	if isViolation(err, foreignKeyViolation) {
		return stack.Wrap(ErrNoSIPTrunk)
	}
	return err
}

// Release marks a number as given back. The row stays, because what it cost while it was
// held is still part of that month's bill.
func (r *PhoneNumbers) Release(ctx context.Context, customerID, e164 string, at time.Time) error {
	if customerID == "" || e164 == "" {
		return stack.Wrap(errors.New("store: a customer and a number are required"))
	}
	return releaseNumber(ctx, r.store.global, customerID, e164, at)
}

// Attach records which SIP trunk calls to a number arrive on and which Stream call they
// land in.
//
// The call is recorded as well as the trunk because an inbound call arrives over a webhook
// that names the call, so without it there is nothing to attribute the call to.
func (r *PhoneNumbers) Attach(ctx context.Context, customerID, e164 string, attached NumberAttachment) error {
	if customerID == "" || e164 == "" {
		return stack.Wrap(errors.New("store: a customer and a number are required"))
	}
	return attachNumber(ctx, r.store.global, customerID, e164, attached)
}

// attachNumber records in db that a number is attached.
func attachNumber(ctx context.Context, db bun.IDB, customerID, e164 string, attached NumberAttachment) error {
	if attached.TrunkID == "" {
		return stack.Wrap(errors.New("store: a trunk id is required"))
	}
	update := db.NewUpdate().Model((*PhoneNumber)(nil))
	if attached.ConfigID != "" {
		update.Set("config_id = ?", attached.ConfigID)
	}
	result, err := update.
		Set("stream_trunk_id = ?", attached.TrunkID).
		Set("stream_route_id = ?", nullable(attached.RouteID)).
		Set("stream_app_pk = ?", nullablePin(attached.StreamAppPK)).
		Set("stream_call_id = ?", attached.CallID).
		Set("stream_call_type = ?", attached.CallType).
		Set("attached_at = now()").
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: attach number: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: attach number: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not a number %s holds", e164, customerID)})
	}
	return nil
}

// AttachedBridge returns the trunk calls to a number of the vendor go to. It matches
// ErrNoNumber when nobody holds the number at that vendor or it is not attached, and
// ErrNoStoredBridge when its trunk's credentials were not kept.
func (r *PhoneNumbers) AttachedBridge(ctx context.Context, vendor, e164 string) (AttachedBridge, error) {
	if vendor == "" || e164 == "" {
		return AttachedBridge{}, stack.Wrap(errors.New("store: a vendor and a number are required"))
	}

	var found []struct {
		CustomerID     string `bun:"customer_id"`
		TrunkID        string `bun:"trunk_id"`
		Own            bool   `bun:"own"`
		URI            string `bun:"bridge_uri"`
		Username       string `bun:"bridge_username"`
		PasswordSealed []byte `bun:"bridge_password_sealed"`
		KEKVersion     int    `bun:"bridge_password_kek_version"`
	}
	err := r.store.global.NewSelect().TableExpr("phone_numbers AS pn").
		ColumnExpr("pn.customer_id, COALESCE(st.id, '') AS trunk_id, COALESCE(st.own, false) AS own").
		ColumnExpr("COALESCE(st.bridge_uri, '') AS bridge_uri, COALESCE(st.bridge_username, '') AS bridge_username").
		ColumnExpr("st.bridge_password_sealed, COALESCE(st.bridge_password_kek_version, 0) AS bridge_password_kek_version").
		Join("LEFT JOIN stream_trunks AS st ON st.id = pn.stream_trunk_id AND st.customer_id = pn.customer_id").
		Where("pn.vendor = ?", vendor).
		Where("pn.e164 = ?", e164).
		Where("pn.released_at IS NULL").
		Where("pn.attached_at IS NOT NULL").
		Limit(1).
		Scan(ctx, &found)
	if err != nil {
		return AttachedBridge{}, stack.Wrap(fmt.Errorf("store: attached bridge: %w", err))
	}
	if len(found) == 0 {
		return AttachedBridge{}, stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not an attached %s number", e164, vendor)})
	}
	bridge := found[0]
	if bridge.URI == "" || len(bridge.PasswordSealed) == 0 {
		return AttachedBridge{}, stack.Wrap(fmt.Errorf("%w: %s", ErrNoStoredBridge, e164))
	}
	return AttachedBridge(bridge), nil
}

// Select returns the numbers a customer holds, newest first.
func (r *PhoneNumbers) Select(ctx context.Context, customerID string, filter PhoneNumberFilter) ([]PhoneNumber, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}

	query := r.store.global.NewSelect().Model((*PhoneNumber)(nil)).
		Where("customer_id = ?", customerID).
		Order("purchased_at DESC")
	if !filter.IncludeReleased {
		query = query.Where("released_at IS NULL")
	}

	var numbers []PhoneNumber
	if err := query.Scan(ctx, &numbers); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer numbers: %w", err))
	}
	return numbers, nil
}

// GetByE164 returns one number a customer holds.
func (r *PhoneNumbers) GetByE164(ctx context.Context, customerID, e164 string) (PhoneNumber, error) {
	if customerID == "" || e164 == "" {
		return PhoneNumber{}, stack.Wrap(errors.New("store: a customer and a number are required"))
	}
	return heldNumber(ctx, r.store.global.NewSelect(), customerID, e164)
}

// SetAgent makes a config answer an attached number's calls, replacing any it had.
func (r *PhoneNumbers) SetAgent(ctx context.Context, customerID, e164, configID string) error {
	if customerID == "" || e164 == "" || configID == "" {
		return stack.Wrap(errors.New("store: a customer, a number and a config are required"))
	}
	// Only an attached number has an agent, and a detach can land between the caller's
	// read and this write.
	result, err := r.store.global.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("config_id = ?", configID).
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Where("attached_at IS NOT NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set number agent: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set number agent: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not a number %s holds attached", e164, customerID)})
	}
	return nil
}

// ClearAgent sends a number's calls back to dispatch. A number with no agent is left as it
// is.
func (r *PhoneNumbers) ClearAgent(ctx context.Context, customerID, e164 string) error {
	if customerID == "" || e164 == "" {
		return stack.Wrap(errors.New("store: a customer and a number are required"))
	}
	result, err := r.store.global.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("config_id = NULL").
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: clear number agent: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: clear number agent: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not a number %s holds", e164, customerID)})
	}
	return nil
}

// AnsweredBy returns the numbers a config answers, sorted.
func (r *PhoneNumbers) AnsweredBy(ctx context.Context, customerID, configID string) ([]string, error) {
	if customerID == "" || configID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a config are required"))
	}
	var numbers []string
	err := r.store.global.NewSelect().Model((*PhoneNumber)(nil)).
		Column("e164").
		Where("customer_id = ?", customerID).
		Where("config_id = ?", configID).
		Where("released_at IS NULL").
		// A router from before agents on numbers detaches without clearing the agent.
		Where("attached_at IS NOT NULL").
		Order("e164").
		Scan(ctx, &numbers)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: numbers answered by: %w", err))
	}
	return numbers, nil
}

// HeldSince is every number held at some time after since: still held, or released after
// it. It is what a month of rental is billed from.
func (r *PhoneNumbers) HeldSince(ctx context.Context, since time.Time) ([]PhoneNumber, error) {
	var held []PhoneNumber
	err := r.store.global.NewSelect().Model(&held).
		WhereGroup(" AND ", func(q *bun.SelectQuery) *bun.SelectQuery {
			return q.Where("released_at IS NULL").WhereOr("released_at > ?", since.UTC())
		}).
		Order("id").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: numbers held since: %w", err))
	}
	return held, nil
}

// releaseNumber marks a number as given back, in db.
func releaseNumber(ctx context.Context, db bun.IDB, customerID, e164 string, at time.Time) error {
	if at.IsZero() {
		at = time.Now().UTC()
	}

	result, err := db.NewUpdate().Model((*PhoneNumber)(nil)).
		Set("released_at = ?", at).
		Set("attached_at = NULL").
		Set("config_id = NULL").
		Set("sip_trunk_id = NULL").
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: release number: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: release number: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not a number %s holds", e164, customerID)})
	}
	return nil
}

// heldNumber is one number a customer holds, read by query.
func heldNumber(ctx context.Context, query *bun.SelectQuery, customerID, e164 string) (PhoneNumber, error) {
	// Nobody holds a number written another way, and the vendor may read it as one somebody
	// else holds.
	if !IsE164(e164) {
		return PhoneNumber{}, stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not a number %s holds", e164, customerID)})
	}
	var number PhoneNumber
	err := query.Model(&number).
		Where("customer_id = ?", customerID).
		Where("e164 = ?", e164).
		Where("released_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return PhoneNumber{}, stack.Wrap(numberNotHeld{fmt.Sprintf("store: %s is not a number %s holds", e164, customerID)})
	}
	if err != nil {
		return PhoneNumber{}, stack.Wrap(fmt.Errorf("store: number: %w", err))
	}
	return number, nil
}

// nullablePin stores the deployment's own app as NULL, as every pin written before apps
// had identities reads.
func nullablePin(app int64) any {
	if app == 0 {
		return nil
	}
	return app
}
