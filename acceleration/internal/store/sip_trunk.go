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

// ErrNoSIPTrunk says the customer has no trunk with that id: none was made, it is another
// customer's, or it was deleted. One error for all three, so a caller learns nothing about
// anybody else's trunks.
var ErrNoSIPTrunk = errors.New("store: no such sip trunk")

// ErrSIPTrunkInUse says a trunk still has numbers on it, which have to be released first.
var ErrSIPTrunkInUse = errors.New("store: the sip trunk still has numbers on it")

// SIPTrunk is a customer's own SIP trunk. The password is sealed by the caller before it
// reaches the store, with the customer and the trunk id as associated data, which is why
// the caller chooses the id.
type SIPTrunk struct {
	bun.BaseModel `bun:"table:sip_trunks,alias:st"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	Name       string `bun:"name,notnull"`
	// Host is a bare hostname: no sip: and no port.
	Host      string `bun:"host,notnull"`
	Port      int    `bun:"port,notnull"`
	Transport string `bun:"transport,notnull"`
	Username  string `bun:"username,notnull"`
	// PasswordSealed is empty, at version 0, for a trunk whose password has to be set
	// again, which is how one arrives from a data move.
	PasswordSealed     []byte `bun:"password_sealed,notnull"`
	PasswordKEKVersion int    `bun:"password_kek_version,notnull"`
	// PasswordDataKeyVersion is the version of the app's data key that sealed the password, at
	// PasswordKEKVersion 0. It is nil when the router's key sealed it.
	PasswordDataKeyVersion *int `bun:"password_data_key_version"`
	// LateOffer says the trunk accepts an INVITE without SDP.
	LateOffer bool      `bun:"late_offer,notnull"`
	Codecs    []string  `bun:"codecs,array"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

// Validate refuses a trunk the table would refuse.
func (t *SIPTrunk) Validate() error {
	return validate("sip trunk").
		required("id", t.ID).
		required("customer_id", t.CustomerID).
		oneOf("transport", t.Transport, "udp", "tcp", "tls").
		done()
}

// HasPassword says whether the trunk holds a password that a key can open. After a data move
// the row can keep sealed bytes with KEK version 0 and no data key version. No key opens
// those, so the trunk counts as having no password.
func (t SIPTrunk) HasPassword() bool {
	return len(t.PasswordSealed) > 0 && (t.PasswordKEKVersion > 0 || t.DataKeyVersion() != nil)
}

// DataKeyVersion is the version of the app's data key that sealed the password, or nil when the
// router's key sealed it. A nonzero PasswordKEKVersion always means the router's key sealed it:
// a build from before data keys can save the trunk again under the router's key and leave the
// old data key version in place.
func (t SIPTrunk) DataKeyVersion() *int {
	if t.PasswordKEKVersion != 0 {
		return nil
	}
	return t.PasswordDataKeyVersion
}

// SIPTrunks are customers' own SIP trunks, in the shared database.
type SIPTrunks struct {
	store *Store
}

// Insert stores a new trunk.
func (r *SIPTrunks) Insert(ctx context.Context, trunk *SIPTrunk) error {
	if err := trunk.Validate(); err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC()
	trunk.CreatedAt, trunk.UpdatedAt = now, now
	trunk.Codecs = orEmpty(trunk.Codecs)
	if _, err := r.store.global.NewInsert().Model(trunk).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: create sip trunk: %w", err))
	}
	return nil
}

// Select returns a customer's trunks, newest first.
func (r *SIPTrunks) Select(ctx context.Context, customerID string) ([]SIPTrunk, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}
	var trunks []SIPTrunk
	err := r.store.global.NewSelect().Model(&trunks).
		Where("customer_id = ?", customerID).
		Order("created_at DESC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: sip trunks: %w", err))
	}
	return trunks, nil
}

// Get returns one of a customer's trunks.
func (r *SIPTrunks) Get(ctx context.Context, customerID, id string) (SIPTrunk, error) {
	var trunk SIPTrunk
	err := r.store.global.NewSelect().Model(&trunk).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return SIPTrunk{}, stack.Wrap(ErrNoSIPTrunk)
	}
	if err != nil {
		return SIPTrunk{}, stack.Wrap(fmt.Errorf("store: sip trunk: %w", err))
	}
	return trunk, nil
}

// Update writes every field of a trunk the customer holds.
func (r *SIPTrunks) Update(ctx context.Context, trunk *SIPTrunk) error {
	if err := trunk.Validate(); err != nil {
		return stack.Wrap(err)
	}
	trunk.UpdatedAt = time.Now().UTC()
	trunk.Codecs = orEmpty(trunk.Codecs)
	result, err := r.store.global.NewUpdate().Model(trunk).
		Column("name", "host", "port", "transport", "username", "password_sealed",
			"password_kek_version", "password_data_key_version", "late_offer", "codecs", "updated_at").
		Where("customer_id = ?", trunk.CustomerID).
		Where("id = ?", trunk.ID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: update sip trunk: %w", err))
	}
	return oneRow(result, ErrNoSIPTrunk)
}

// ResealPassword writes a password sealed again under version dataKeyVersion of the app's data
// key over the trunk it opened from. It answers false and writes nothing when the row no longer
// holds the opened blob, since every seal makes a new blob and a password set in the meantime is
// newer than the one resealed.
func (r *SIPTrunks) ResealPassword(ctx context.Context, opened SIPTrunk, sealed []byte,
	dataKeyVersion int) (bool, error) {
	result, err := r.store.global.NewUpdate().Model((*SIPTrunk)(nil)).
		Set("password_sealed = ?", sealed).
		Set("password_kek_version = 0").
		Set("password_data_key_version = ?", dataKeyVersion).
		Where("customer_id = ?", opened.CustomerID).
		Where("id = ?", opened.ID).
		Where("password_sealed = ?", opened.PasswordSealed).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: reseal sip trunk password: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: reseal sip trunk password: %w", err))
	}
	return affected == 1, nil
}

// Delete removes a trunk with no numbers on it. The foreign key from phone_numbers is what
// refuses one that has some, so a number added at the same moment cannot be left pointing
// at nothing.
func (r *SIPTrunks) Delete(ctx context.Context, customerID, id string) error {
	result, err := r.store.global.NewDelete().Model((*SIPTrunk)(nil)).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Exec(ctx)
	if isViolation(err, foreignKeyViolation) {
		return stack.Wrap(ErrSIPTrunkInUse)
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete sip trunk: %w", err))
	}
	return oneRow(result, ErrNoSIPTrunk)
}
