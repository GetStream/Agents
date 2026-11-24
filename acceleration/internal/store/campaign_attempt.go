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

// CampaignAttempt is one dial of one contact.
type CampaignAttempt struct {
	bun.BaseModel `bun:"table:campaign_attempts,alias:ca"`

	ID         string `bun:"id,pk"`
	CampaignID string `bun:"campaign_id,notnull"`
	ContactID  string `bun:"contact_id,notnull"`
	StepID     string `bun:"step_id,notnull"`
	Attempt    int    `bun:"attempt,notnull"`
	// Dial is 1, or 2 for the second call of a double dial.
	Dial         int    `bun:"dial,notnull"`
	CallID       string `bun:"call_id,notnull"`
	VendorCallID string `bun:"vendor_call_id,notnull"`
	// Result is why the dial placed no call: invalid_number, refused, deferred, paused or
	// not_placed. It is voicemail for a call an answering machine took, and empty for any other.
	Result string `bun:"result,notnull"`
	// CallOutcome is how the placed call ended, a phone outcome. Empty while the call is
	// happening, and for a dial that placed none.
	CallOutcome string `bun:"call_outcome,nullzero"`
	Outcome     string `bun:"outcome,notnull"`
	// ClassifyState is empty, pending, done or abandoned: whether an answered call's outcome
	// is still to be read from its transcript. Abandoned is one whose contact settled before
	// it was read.
	ClassifyState string     `bun:"classify_state,notnull"`
	ClassifyAfter *time.Time `bun:"classify_after"`
	ClassifyTries int        `bun:"classify_tries,notnull"`
	StartedAt     time.Time  `bun:"started_at,notnull"`
	EndedAt       *time.Time `bun:"ended_at"`
}

// Validate refuses a dial no contact could own.
func (a *CampaignAttempt) Validate() error {
	return validate("campaign attempt").
		required("campaign_id", a.CampaignID).
		required("contact_id", a.ContactID).
		check(!a.StartedAt.IsZero(), "started_at", "is required").
		check(a.Dial >= 1, "dial", "is below 1").
		oneOf("classify_state", a.ClassifyState, "", classifyPending, classifyDone, classifyAbandoned).
		done()
}

// CampaignAttempts are campaigns' dials, in the shared database.
type CampaignAttempts struct {
	store *Store
}

// Whether an answered dial's outcome is still to be read from its transcript.
const (
	classifyPending   = "pending"
	classifyDone      = "done"
	classifyAbandoned = "abandoned"
)

// Insert records a dial about to be placed, filling in its id.
func (r *CampaignAttempts) Insert(ctx context.Context, attempt *CampaignAttempt) error {
	if attempt.Dial == 0 {
		attempt.Dial = 1
	}
	if err := attempt.Validate(); err != nil {
		return stack.Wrap(err)
	}
	attempt.ID = newID()
	if _, err := r.store.global.NewInsert().Model(attempt).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: start dial: %w", err))
	}
	return nil
}

// End records how a dial ended: its result, and callOutcome, the phone outcome of the call
// it placed, empty for none. A classifyAfter marks an answered call whose outcome is to be read
// from its transcript once it is written. A dial deleted with its campaign is nothing to end,
// and one already ended is left as it is: a router whose call outlived its lease ends its dial
// after the router that took the contact over ended and settled it.
func (r *CampaignAttempts) End(ctx context.Context, customerID, id string, result, callOutcome, callID, vendorCallID string, endedAt time.Time, classifyAfter *time.Time) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a dial id are required"))
	}
	classify := ""
	if classifyAfter != nil {
		classify = classifyPending
	}
	_, err := r.store.global.NewUpdate().Model((*CampaignAttempt)(nil)).
		Set("result = ?", result).
		Set("call_outcome = NULLIF(?, '')", callOutcome).
		Set("call_id = COALESCE(NULLIF(?, ''), call_id)", callID).
		Set("vendor_call_id = COALESCE(NULLIF(?, ''), vendor_call_id)", vendorCallID).
		Set("ended_at = ?", endedAt).
		Set("classify_state = ?", classify).
		Set("classify_after = ?", classifyAfter).
		Where("id = ?", id).
		Where("ended_at IS NULL").
		// A dial has no customer of its own: it is its campaign's.
		Where("campaign_id IN (SELECT id FROM campaigns WHERE customer_id = ?)", customerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: end dial: %w", err))
	}
	return nil
}

// CountResult counts a contact's dials of one attempt that ended with result.
func (r *CampaignAttempts) CountResult(ctx context.Context, contactID string, attempt int, result string) (int, error) {
	count, err := r.store.global.NewSelect().Model((*CampaignAttempt)(nil)).
		Where("contact_id = ?", contactID).
		Where("attempt = ?", attempt).
		Where("result = ?", result).
		Count(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: count dials: %w", err))
	}
	return count, nil
}

// Latest returns a contact's most recent dial, and false when it was never dialed.
func (r *CampaignAttempts) Latest(ctx context.Context, contactID string) (CampaignAttempt, bool, error) {
	var attempt CampaignAttempt
	err := r.store.global.NewSelect().Model(&attempt).
		Where("contact_id = ?", contactID).
		Order("started_at DESC", "dial DESC").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return CampaignAttempt{}, false, nil
	}
	if err != nil {
		return CampaignAttempt{}, false, stack.Wrap(fmt.Errorf("store: latest dial: %w", err))
	}
	return attempt, true, nil
}

// Classifying is an answered dial whose outcome a runner is to read, with the contact it now
// holds the lease of and the campaign.
type Classifying struct {
	Attempt  CampaignAttempt
	Contact  Contact
	Campaign Campaign
}

// ClaimClassifications hands owner up to limit answered dials whose transcript should be written
// by now. Each is put off by lease, so another runner retries it if this one dies, and its
// contact is leased to owner so owner can settle it. Only a contact's latest answered dial is
// read, and not while another runner holds the contact, nor while a data move's source still
// reads it (move_paused). Postgres's clock sets the lease of the contact, and the claim compares
// the lease with the same clock. Thus a runner with a clock that is ahead does not read a dial
// that another runner reads at the same time.
func (r *CampaignAttempts) ClaimClassifications(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]Classifying, error) {
	if owner == "" {
		return nil, stack.Wrap(errors.New("store: a claim needs an owner"))
	}
	if limit <= 0 {
		return nil, nil
	}

	var claimed []Classifying
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var attempts []CampaignAttempt
		err := tx.NewRaw(`WITH due AS (
	SELECT ca.id FROM campaign_attempts ca
	JOIN campaign_contacts cc ON cc.id = ca.contact_id AND cc.campaign_id = ca.campaign_id
	WHERE ca.classify_state = ? AND ca.classify_after <= ? AND cc.stage = ?
		AND (cc.leased_by = '' OR cc.lease_until IS NULL OR cc.lease_until < now())
		AND NOT EXISTS (SELECT 1 FROM campaigns cp WHERE cp.id = cc.campaign_id AND cp.move_paused)
		AND NOT EXISTS (SELECT 1 FROM campaign_attempts newer
			WHERE newer.contact_id = ca.contact_id AND newer.campaign_id = ca.campaign_id AND newer.classify_state = ca.classify_state
			AND (newer.started_at, newer.dial) > (ca.started_at, ca.dial))
	ORDER BY ca.classify_after
	LIMIT ?
	FOR UPDATE OF ca, cc SKIP LOCKED
)
UPDATE campaign_attempts ca SET classify_after = ?, classify_tries = ca.classify_tries + 1
FROM due WHERE ca.id = due.id
RETURNING ca.*`, classifyPending, now, AwaitingOutcome, limit, now.Add(lease)).Scan(ctx, &attempts)
		if err != nil {
			return fmt.Errorf("store: claim classifications: %w", err)
		}
		if len(attempts) == 0 {
			return nil
		}

		contactIDs := make([]string, 0, len(attempts))
		for _, attempt := range attempts {
			contactIDs = append(contactIDs, attempt.ContactID)
		}
		var contacts []Contact
		err = tx.NewUpdate().Model(&contacts).
			Set("leased_by = ?", owner).
			Set("lease_until = now() + make_interval(secs => ?)", lease.Seconds()).
			Where("id IN (?)", bun.In(contactIDs)).
			Returning("*").
			Scan(ctx)
		if err != nil {
			return fmt.Errorf("store: lease contacts to classify: %w", err)
		}
		campaigns, err := campaignsOf(ctx, tx, contacts)
		if err != nil {
			return err
		}
		byID := make(map[string]Contact, len(contacts))
		for _, contact := range contacts {
			byID[contact.ID] = contact
		}
		for _, attempt := range attempts {
			contact := byID[attempt.ContactID]
			claimed = append(claimed, Classifying{Attempt: attempt, Contact: contact, Campaign: campaigns[contact.CampaignID]})
		}
		return nil
	})
	if err != nil {
		return nil, stack.Wrap(err)
	}
	return claimed, nil
}

// PutOff gives back an answered dial whose transcript is not written yet, to be
// read again at at, and releases owner's lease on its contact so whichever runner is free then
// reads it.
func (r *CampaignAttempts) PutOff(ctx context.Context, owner, attemptID string, at time.Time) error {
	if owner == "" || attemptID == "" {
		return stack.Wrap(errors.New("store: an owner and a dial id are required"))
	}
	var released int64
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		result, err := tx.NewUpdate().Model((*Contact)(nil)).
			TableExpr("campaign_attempts AS ca").
			Set("leased_by = ''").
			Set("lease_until = NULL").
			Where("ca.id = ? AND ca.classify_state = ?", attemptID, classifyPending).
			Where("cc.id = ca.contact_id AND cc.stage = ? AND cc.leased_by = ?", AwaitingOutcome, owner).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: release a contact to classify later: %w", err)
		}
		if released, err = result.RowsAffected(); err != nil || released == 0 {
			return err
		}
		if _, err := tx.NewUpdate().Model((*CampaignAttempt)(nil)).
			Set("classify_after = ?", at).
			Where("id = ?", attemptID).
			Exec(ctx); err != nil {
			return fmt.Errorf("store: put off a classification: %w", err)
		}
		return nil
	})
	if err != nil {
		return stack.Wrap(err)
	}
	if released == 0 {
		return stack.Wrap(ErrNotLeaseHolder)
	}
	return nil
}

// Made counts, per contact, the attempts with at least one dial that placed a call. A
// contact with none is left out.
func (r *CampaignAttempts) Made(ctx context.Context, contactIDs []string) (map[string]int, error) {
	made := map[string]int{}
	if len(contactIDs) == 0 {
		return made, nil
	}
	var rows []struct {
		ContactID string
		Made      int
	}
	err := r.store.global.NewRaw(`SELECT contact_id, count(DISTINCT (step_id, attempt)) AS made
FROM campaign_attempts WHERE contact_id IN (?) AND `+placedDial+`
GROUP BY contact_id`, bun.In(contactIDs)).Scan(ctx, &rows)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: count attempts made: %w", err))
	}
	for _, row := range rows {
		made[row.ContactID] = row.Made
	}
	return made, nil
}
