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

// ChannelAccount is one app's line on a channel: the number people write to and the
// credentials of the provider carrying it.
//
// It belongs to the app rather than to an agent, the way a removed plugin login did: the number is
// connected once and any number of configs name it.
type ChannelAccount struct {
	bun.BaseModel `bun:"table:channel_accounts,alias:ca"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	// Kind is the channel it carries: whatsapp, sms or imessage.
	Kind string `bun:"kind,notnull"`
	E164 string `bun:"e164,notnull"`
	// AccountID is the provider's own id for the line, such as WhatsApp's phone number id.
	AccountID string `bun:"account_id,notnull"`
	// Token is the webhook's path segment, which is what tells the provider delivering to
	// it apart from anybody who guessed the route.
	Token string `bun:"token,notnull"`
	// SecretsSealed is the provider's credentials as JSON, sealed under SecretsKEKVersion and
	// bound to the customer, or under SecretsDataKeyVersion of the app's data key and bound to
	// the line.
	SecretsSealed     []byte `bun:"secrets_sealed"`
	SecretsKEKVersion int    `bun:"kek_version,notnull"`
	// SecretsDataKeyVersion is set when SecretsSealed is sealed under that version of the app's
	// data key, and SecretsKEKVersion is 0 then. Treat a row with a SecretsKEKVersion as under
	// the router's key, whatever this says, since a build from before data keys writes over the
	// blob and leaves this column as it was.
	SecretsDataKeyVersion *int       `bun:"secrets_data_key_version"`
	CreatedAt             time.Time  `bun:"created_at,notnull"`
	UpdatedAt             time.Time  `bun:"updated_at,notnull"`
	DeletedAt             *time.Time `bun:"deleted_at"`
}

// Validate refuses a line no delivery could be addressed to.
func (a *ChannelAccount) Validate() error {
	return validate("channel account").
		required("customer_id", a.CustomerID).
		oneOf("kind", a.Kind, "whatsapp", "sms", "imessage").
		required("e164", a.E164).
		required("token", a.Token).
		check(a.SecretsKEKVersion >= 0, "kek_version", "is negative").
		done()
}

// ErrUnknownChannelAccount is a webhook token nobody connected, or one whose account has
// since been disconnected.
var ErrUnknownChannelAccount = errors.New("store: there is no such channel account")

// ChannelAccounts are the lines apps connected, in the shared database.
type ChannelAccounts struct {
	store *Store
}

// Select returns the lines one app has connected, oldest first.
func (r *ChannelAccounts) Select(ctx context.Context, customerID string) ([]ChannelAccount, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: a customer is required"))
	}

	var accounts []ChannelAccount
	err := r.store.global.NewSelect().Model(&accounts).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Order("created_at").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: channel accounts: %w", err))
	}
	return accounts, nil
}

// GetByNumber is the line one app connected on a channel for a number.
func (r *ChannelAccounts) GetByNumber(ctx context.Context, customerID, kind, e164 string) (ChannelAccount, error) {
	if customerID == "" || kind == "" || e164 == "" {
		return ChannelAccount{}, stack.Wrap(ErrUnknownChannelAccount)
	}

	var account ChannelAccount
	err := r.store.global.NewSelect().Model(&account).
		Where("customer_id = ?", customerID).
		Where("kind = ?", kind).
		Where("e164 = ?", e164).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ChannelAccount{}, stack.Wrap(ErrUnknownChannelAccount)
	}
	if err != nil {
		return ChannelAccount{}, stack.Wrap(fmt.Errorf("store: channel account: %w", err))
	}
	return account, nil
}

// GetByToken finds the line a delivery is addressed to.
func (r *ChannelAccounts) GetByToken(ctx context.Context, token string) (ChannelAccount, error) {
	if token == "" {
		return ChannelAccount{}, stack.Wrap(ErrUnknownChannelAccount)
	}

	var account ChannelAccount
	err := r.store.global.NewSelect().Model(&account).
		Where("token = ?", token).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return ChannelAccount{}, stack.Wrap(ErrUnknownChannelAccount)
	}
	if err != nil {
		return ChannelAccount{}, stack.Wrap(fmt.Errorf("store: channel account by token: %w", err))
	}
	return account, nil
}

// Upsert connects a line, or replaces the credentials of one already connected. The token
// survives a reconnection, so the URL the provider was given goes on working.
func (r *ChannelAccounts) Upsert(ctx context.Context, account *ChannelAccount) error {
	if err := account.Validate(); err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC()
	account.ID, account.CreatedAt, account.UpdatedAt = newID(), now, now
	err := r.store.global.NewInsert().Model(account).
		On("CONFLICT (customer_id, kind, e164) WHERE deleted_at IS NULL DO UPDATE").
		Set("account_id = EXCLUDED.account_id").
		Set("secrets_sealed = EXCLUDED.secrets_sealed").
		Set("kek_version = EXCLUDED.kek_version").
		Set("secrets_data_key_version = EXCLUDED.secrets_data_key_version").
		Set("updated_at = EXCLUDED.updated_at").
		Returning("id, token, created_at").
		Scan(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save channel account: %w", err))
	}
	return nil
}

// Reseal writes credentials sealed again under version dataKeyVersion of the app's data key over
// the line they opened from. It answers false and writes nothing when the line's credentials
// changed since they were read, since a reconnect in the meantime is newer than what was resealed.
func (r *ChannelAccounts) Reseal(ctx context.Context, opened ChannelAccount, sealed []byte, dataKeyVersion int) (bool, error) {
	result, err := r.store.global.NewUpdate().Model((*ChannelAccount)(nil)).
		Set("secrets_sealed = ?", sealed).
		Set("kek_version = 0").
		Set("secrets_data_key_version = ?", dataKeyVersion).
		Where("id = ?", opened.ID).
		Where("customer_id = ?", opened.CustomerID).
		// Every seal makes a new blob, so an equal blob means no reconnect or reseal came between.
		Where("secrets_sealed = ?", opened.SecretsSealed).
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: reseal channel account: %w", err))
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: reseal channel account: %w", err))
	}
	return rows == 1, nil
}

// Delete disconnects a line, so deliveries to it are refused. The credentials go with it: a
// line nobody answers on has no use for them.
func (r *ChannelAccounts) Delete(ctx context.Context, customerID, kind, e164 string) error {
	result, err := r.store.global.NewUpdate().Model((*ChannelAccount)(nil)).
		Set("deleted_at = ?", time.Now().UTC()).
		Set("secrets_sealed = NULL").
		Where("customer_id = ?", customerID).
		Where("kind = ?", kind).
		Where("e164 = ?", e164).
		Where("deleted_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: disconnect channel account: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: disconnect channel account: %w", err))
	}
	if affected == 0 {
		return stack.Wrap(ErrUnknownChannelAccount)
	}
	return nil
}
