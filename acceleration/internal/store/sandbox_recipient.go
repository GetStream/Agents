package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// SandboxRecipient is a number an app may reach before any use case of its is approved.
type SandboxRecipient struct {
	bun.BaseModel `bun:"table:sandbox_recipients,alias:sr"`

	CustomerID string    `bun:"customer_id,pk"`
	Recipient  string    `bun:"recipient,pk"`
	CreatedAt  time.Time `bun:"created_at,notnull"`
}

// Validate refuses a recipient no app owns.
func (r *SandboxRecipient) Validate() error {
	return validate("sandbox recipient").
		required("customer_id", r.CustomerID).
		required("recipient", r.Recipient).
		done()
}

// SandboxRecipients are the numbers apps may reach before approval, in the shared database.
type SandboxRecipients struct {
	store *Store
}

// Select returns the numbers an app may reach while sandboxed, in order.
func (r *SandboxRecipients) Select(ctx context.Context, customerID string) ([]string, error) {
	recipients := []string{}
	err := r.store.global.NewSelect().Model((*SandboxRecipient)(nil)).
		Column("recipient").
		Where("customer_id = ?", customerID).
		Order("recipient").
		Scan(ctx, &recipients)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: sandbox recipients: %w", err))
	}
	return recipients, nil
}

// Replace replaces the numbers an app may reach while sandboxed.
func (r *SandboxRecipients) Replace(ctx context.Context, customerID string, recipients []string) error {
	now := time.Now().UTC()
	rows := make([]SandboxRecipient, len(recipients))
	for i, recipient := range recipients {
		rows[i] = SandboxRecipient{CustomerID: customerID, Recipient: recipient, CreatedAt: now}
		if err := rows[i].Validate(); err != nil {
			return stack.Wrap(err)
		}
	}
	return stack.Wrap(r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewDelete().Model((*SandboxRecipient)(nil)).Where("customer_id = ?", customerID).Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: set sandbox recipients: %w", err)
		}
		if len(recipients) == 0 {
			return nil
		}
		if _, err := tx.NewInsert().Model(&rows).On("CONFLICT DO NOTHING").Exec(ctx); err != nil {
			return fmt.Errorf("store: set sandbox recipients: %w", err)
		}
		return nil
	}))
}
