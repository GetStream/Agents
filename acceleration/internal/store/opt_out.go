package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nyaruka/phonenumbers"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// OptOut is somebody who asked not to be reached on a channel, or on any.
type OptOut struct {
	bun.BaseModel `bun:"table:opt_outs,alias:oo"`

	ID         string     `bun:"id,pk"`
	CustomerID string     `bun:"customer_id,notnull"`
	Recipient  string     `bun:"recipient,notnull"`
	Channel    string     `bun:"channel,notnull"`
	Source     string     `bun:"source,notnull"`
	CreatedAt  time.Time  `bun:"created_at,notnull"`
	RevokedAt  *time.Time `bun:"revoked_at"`
}

// OptOutAll is an opt-out from every channel at once.
const OptOutAll = "all"

// Validate refuses an opt-out that names nobody.
func (o *OptOut) Validate() error {
	return validate("opt-out").
		required("customer_id", o.CustomerID).
		required("recipient", o.Recipient).
		required("channel", o.Channel).
		done()
}

// OptOuts are the people who asked not to be reached, in the shared database.
type OptOuts struct {
	store *Store
}

// ErrUnknownOptOut is an opt-out id the app does not hold, or one already revoked.
var ErrUnknownOptOut = errors.New("store: there is no such opt-out")

// Insert records that somebody asked not to be reached. Asking twice keeps the first.
func (r *OptOuts) Insert(ctx context.Context, optOut *OptOut) error {
	return insertOptOut(ctx, r.store.global, optOut)
}

// insertOptOut is OptOuts.Insert on db, which may be the transaction a delivery is claimed
// in (ChannelDeliveries.ClaimStop).
func insertOptOut(ctx context.Context, db bun.IDB, optOut *OptOut) error {
	optOut.Recipient, _ = optOutRecipient(optOut.Recipient, "")
	if err := optOut.Validate(); err != nil {
		return stack.Wrap(err)
	}
	optOut.ID = newID()
	optOut.CreatedAt = time.Now().UTC().Truncate(time.Microsecond)
	_, err := db.NewInsert().Model(optOut).
		On("CONFLICT (customer_id, recipient, channel) WHERE revoked_at IS NULL DO NOTHING").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: opt out: %w", err))
	}
	err = db.NewSelect().Model(optOut).
		Where("customer_id = ?", optOut.CustomerID).
		Where("recipient = ?", optOut.Recipient).
		Where("channel = ?", optOut.Channel).
		Where("revoked_at IS NULL").
		Scan(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: opt out: %w", err))
	}
	return nil
}

// Revoke lifts one opt-out, keeping the row.
func (r *OptOuts) Revoke(ctx context.Context, customerID, id string) error {
	result, err := r.store.global.NewUpdate().Model((*OptOut)(nil)).
		Set("revoked_at = ?", time.Now().UTC()).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("revoked_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: revoke opt-out: %w", err))
	}
	if affected, _ := result.RowsAffected(); affected == 0 {
		return stack.Wrap(ErrUnknownOptOut)
	}
	return nil
}

// RevokeAll lifts every opt-out a recipient has on a channel and on all of them, as
// texting START does.
func (r *OptOuts) RevokeAll(ctx context.Context, customerID, recipient, channel string) error {
	return revokeAllOptOuts(ctx, r.store.global, customerID, channel, recipient)
}

// revokeAllOptOuts is OptOuts.RevokeAll on db, which may be the transaction a delivery is
// claimed in (ChannelDeliveries.ClaimStart), of the opt-outs kept under any of recipients.
func revokeAllOptOuts(ctx context.Context, db bun.IDB, customerID, channel string, recipients ...string) error {
	_, err := db.NewUpdate().Model((*OptOut)(nil)).
		Set("revoked_at = ?", time.Now().UTC()).
		Where("customer_id = ?", customerID).
		Where("recipient IN (?)", bun.In(keptUnder(recipients...))).
		Where("channel IN (?)", bun.In([]string{channel, OptOutAll})).
		Where("revoked_at IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("store: revoke opt-outs: %w", err)
	}
	return nil
}

// OptedOut reports whether a recipient asked not to be reached on a channel, or on any.
func (r *OptOuts) OptedOut(ctx context.Context, customerID, recipient, channel string) (bool, error) {
	found, err := r.store.global.NewSelect().Model((*OptOut)(nil)).
		Where("customer_id = ?", customerID).
		Where("recipient IN (?)", bun.In(keptUnder(recipient))).
		Where("channel IN (?)", bun.In([]string{channel, OptOutAll})).
		Where("revoked_at IS NULL").
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: opted out: %w", err))
	}
	return found, nil
}

// Select pages through an app's live opt-outs, newest first.
func (r *OptOuts) Select(ctx context.Context, customerID string, limit int, after *CreatedPosition) ([]OptOut, error) {
	optOuts := []OptOut{}
	query := r.store.global.NewSelect().Model(&optOuts).
		Where("customer_id = ?", customerID).
		Where("revoked_at IS NULL")
	if after != nil {
		query = query.Where("(created_at, id) < (?, ?)", after.At, after.ID)
	}
	err := query.Order("created_at DESC", "id DESC").Limit(DLCLimit(limit) + 1).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: opt-outs: %w", err))
	}
	return optOuts, nil
}

// NormalizedOptOuts is what OptOuts.Normalize did.
type NormalizedOptOuts struct {
	// Rewritten is how many opt-outs now name their recipient in E.164.
	Rewritten int
	// Revoked is how many of those were revoked: the recipient already had a live opt-out from
	// the same channel in E.164, or lifted one after this one was recorded.
	Revoked int
	// Handles is how many opt-outs name somebody by what is not a phone number, such as the
	// email address an iMessage handle can be, and were left as they are.
	Handles int
}

// normalizeRetries is how many times Normalize reads a batch again after a STOP recorded at
// the same moment took the recipient it was rewriting a row to.
const normalizeRetries = 3

// Normalize rewrites every opt-out whose recipient is a phone number not written in E.164,
// such as one stored before the recipient was checked, in batches of batch rows, so it
// matches the number dialled or texted. Of two live opt-outs of one recipient and channel,
// the one rewritten is revoked rather than deleted, and so is one the recipient lifted under
// E.164 after it was recorded. A recipient that looks like a number but has no one valid
// reading is left as it is and handed to unread, one batch at a time. Running it again
// changes nothing.
func (r *OptOuts) Normalize(ctx context.Context, batch int, unread func([]OptOut)) (NormalizedOptOuts, error) {
	var done NormalizedOptOuts
	after := ""
	for {
		var page []OptOut
		var did NormalizedOptOuts
		var left []OptOut
		var err error
		for attempt := 0; ; attempt++ {
			page = nil
			err = r.store.global.NewSelect().Model(&page).
				Where("id > ?", after).
				Order("id").Limit(batch).Scan(ctx)
			if err != nil {
				return done, stack.Wrap(fmt.Errorf("store: normalize opt-outs: %w", err))
			}
			did, left, err = r.normalizePage(ctx, page)
			if err == nil || !isViolation(err, uniqueViolation) || attempt == normalizeRetries {
				break
			}
		}
		if err != nil {
			return done, stack.Wrap(fmt.Errorf("store: normalize opt-outs: %w", err))
		}
		if len(page) == 0 {
			return done, nil
		}
		after = page[len(page)-1].ID
		done.Rewritten += did.Rewritten
		done.Revoked += did.Revoked
		done.Handles += did.Handles
		if len(left) > 0 {
			unread(left)
		}
	}
}

// normalizePage is Normalize of one batch, in one transaction. It reports the opt-outs it
// could not read.
func (r *OptOuts) normalizePage(ctx context.Context, page []OptOut) (NormalizedOptOuts, []OptOut, error) {
	type key struct{ customer, recipient, channel string }
	var did NormalizedOptOuts
	var unread, rewrite []OptOut
	var targets [][]string
	var redundant []string
	live := map[key]bool{}
	for _, optOut := range page {
		recipient, kind := optOutRecipient(optOut.Recipient, "")
		switch {
		case kind == recipientHandle:
			did.Handles++
			continue
		case kind == recipientUnread:
			unread = append(unread, optOut)
			continue
		case recipient == optOut.Recipient:
			continue
		case recipient != strings.TrimSpace(optOut.Recipient):
			did.Rewritten++
		}
		optOut.Recipient = recipient
		rewrite = append(rewrite, optOut)
		if optOut.RevokedAt != nil {
			continue
		}
		at := key{optOut.CustomerID, recipient, optOut.Channel}
		if live[at] {
			redundant = append(redundant, optOut.ID)
			continue
		}
		live[at] = true
		targets = append(targets, []string{at.customer, at.recipient, at.channel})
	}
	if len(rewrite) == 0 {
		return did, unread, nil
	}
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if len(targets) > 0 {
			var held []OptOut
			err := tx.NewSelect().Model(&held).
				Column("customer_id", "recipient", "channel", "revoked_at").
				Where("(customer_id, recipient, channel) IN (?)", bun.In(targets)).
				Scan(ctx)
			if err != nil {
				return err
			}
			for _, kept := range held {
				for _, row := range rewrite {
					if row.RevokedAt != nil || row.CustomerID != kept.CustomerID ||
						row.Recipient != kept.Recipient || row.Channel != kept.Channel {
						continue
					}
					// Live here, or lifted after this row was recorded: either way this row
					// must not be what keeps the recipient opted out.
					if kept.RevokedAt == nil || kept.RevokedAt.After(row.CreatedAt) {
						redundant = append(redundant, row.ID)
					}
				}
			}
		}
		if len(redundant) > 0 {
			result, err := tx.NewUpdate().Model((*OptOut)(nil)).
				Set("revoked_at = ?", time.Now().UTC()).
				Where("id IN (?)", bun.In(redundant)).
				Exec(ctx)
			if err != nil {
				return err
			}
			affected, _ := result.RowsAffected()
			did.Revoked = int(affected)
		}
		_, err := tx.NewUpdate().Model(&rewrite).Column("recipient").Bulk().Exec(ctx)
		return err
	})
	if err != nil {
		return NormalizedOptOuts{}, nil, err
	}
	return did, unread, nil
}

// recipientKind is what optOutRecipient made of a recipient.
type recipientKind int

const (
	// recipientNumber is a phone number, written in E.164.
	recipientNumber recipientKind = iota
	// recipientUnread looks like a phone number but has no one valid reading.
	recipientUnread
	// recipientHandle is not a phone number, such as an email address.
	recipientHandle
)

// numberLike is a recipient written as a phone number: digits, with the punctuation and
// spaces numbers are written with, and no letters.
var numberLike = regexp.MustCompile(`^[0-9+()\-. ]*[0-9][0-9+()\-. ]*$`)

// OptOutRecipient is the recipient an opt-out from sender, who wrote to line, is kept under:
// their number in E.164, read in the line's country when it has no country code, or what they
// sent when it has no one valid reading.
func OptOutRecipient(sender, line string) string {
	region := ""
	if number, err := phonenumbers.Parse(line, ""); err == nil {
		region = phonenumbers.GetRegionCodeForNumber(number)
	}
	recipient, _ := optOutRecipient(sender, region)
	return recipient
}

// optOutRecipient is the recipient an opt-out is kept and matched under, and what it is. A
// phone number is written in E.164 when exactly one reading of it is a valid number: as written
// when it has a +; otherwise as an international number without its + (as WhatsApp sends
// senders, or 1 and a North American number), or as a number of region when one is known. A
// number read two ways, or none, stays as written: guessing a country would match somebody
// else. What is not a number, such as the email address an iMessage handle can be, stays as it
// is too, so whether a recipient must be in E.164 is decided by what it is, the same for an
// opt-out from one channel and from all of them.
func optOutRecipient(recipient, region string) (string, recipientKind) {
	recipient = strings.TrimSpace(recipient)
	number := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(recipient, "tel:"), "whatsapp:"))
	if !numberLike.MatchString(number) {
		return recipient, recipientHandle
	}
	readings := []string{number}
	if !strings.HasPrefix(number, "+") {
		readings = []string{"+" + number}
		if region != "" {
			readings = append(readings, number)
		}
	}
	var valid []string
	for _, reading := range readings {
		parsed, err := phonenumbers.Parse(reading, region)
		if err != nil || !phonenumbers.IsValidNumber(parsed) {
			continue
		}
		if e164 := phonenumbers.Format(parsed, phonenumbers.E164); !slices.Contains(valid, e164) {
			valid = append(valid, e164)
		}
	}
	if len(valid) != 1 {
		return recipient, recipientUnread
	}
	return valid[0], recipientNumber
}

// keptUnder is every recipient an opt-out of somebody written as recipients may be kept
// under: each in E.164, and as written, for a row stored before recipients were normalized.
func keptUnder(recipients ...string) []string {
	var kept []string
	for _, written := range recipients {
		normalized, _ := optOutRecipient(written, "")
		for _, recipient := range []string{normalized, strings.TrimSpace(written)} {
			if !slices.Contains(kept, recipient) {
				kept = append(kept, recipient)
			}
		}
	}
	return kept
}
