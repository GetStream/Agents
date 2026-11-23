package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Where one contact is in the campaign's sequence.
const (
	// Enrolled is somebody waiting for their next attempt.
	Enrolled = "enrolled"
	// InProgress is a call happening now.
	InProgress = "in_progress"
	// AwaitingOutcome is a call that was answered whose outcome is not yet classified. It
	// does not count toward concurrency, and the API shows it as in_progress.
	AwaitingOutcome = "awaiting_outcome"
	// ContactCompleted is somebody the sequence is finished with.
	ContactCompleted = "completed"
	// Rejected is a number that was refused before any call.
	Rejected = "rejected"
	// ContactStopped is somebody left unrung because the campaign was stopped.
	ContactStopped = "stopped"
	// Failed is a call that could not be made.
	Failed = "failed"
)

// Contact is one person to ring, and what became of ringing them.
type Contact struct {
	bun.BaseModel `bun:"table:campaign_contacts,alias:cc"`

	ID string `bun:"id,pk"`
	// Seq is the order they are rung in, which is the order they were added.
	Seq        int64  `bun:"seq,autoincrement"`
	CampaignID string `bun:"campaign_id,notnull"`
	ToNumber   string `bun:"to_number,notnull"`
	// Instructions are what to say to this person, added to whatever the config says.
	Instructions string            `bun:"instructions,notnull"`
	Variables    map[string]string `bun:"variables,type:jsonb"`
	TimeZone     string            `bun:"time_zone,notnull"`
	Stage        string            `bun:"stage,notnull"`
	StepID       string            `bun:"step_id,notnull"`
	// Attempt is how many attempts of the current step have been used.
	Attempt       int        `bun:"attempt,notnull"`
	NextAttemptAt *time.Time `bun:"next_attempt_at"`
	// LeasedBy and LeaseUntil say which runner holds the contact while it is rung.
	LeasedBy   string     `bun:"leased_by,notnull"`
	LeaseUntil *time.Time `bun:"lease_until"`
	Outcome    string     `bun:"outcome,notnull"`
	Converted  bool       `bun:"converted,notnull"`
	// CallID is the calls row of the last dial.
	CallID    string    `bun:"call_id,nullzero"`
	Reason    string    `bun:"reason,nullzero"`
	CreatedAt time.Time `bun:"created_at,notnull"`
}

// Validate refuses a contact nobody could be rung at.
func (c *Contact) Validate() error {
	return validate("campaign contact").
		required("campaign_id", c.CampaignID).
		required("to_number", c.ToNumber).
		oneOf("stage", c.Stage, Enrolled, InProgress, AwaitingOutcome, ContactCompleted, Rejected, ContactStopped, Failed).
		done()
}

// CampaignContacts are the people campaigns ring, in the shared database.
type CampaignContacts struct {
	store *Store
}

// ErrTooManyContacts is a campaign that would hold more than MaxCampaignContacts.
var ErrTooManyContacts = errors.New("store: a campaign holds at most 100000 contacts")

// ErrNoCampaignContact is a contact the campaign does not hold.
var ErrNoCampaignContact = errors.New("store: the campaign holds no such contact")

// ErrDuplicateContact is a number another contact of the campaign already holds.
var ErrDuplicateContact = errors.New("store: another contact of the campaign holds this number")

// MaxCampaignContacts is how many contacts one campaign may hold.
const MaxCampaignContacts = 100_000

// insertChunk keeps one INSERT of contacts a size the server parses quickly.
const insertChunk = 5000

// InsertMany stores people to ring in a draft campaign, or in an auto-enroll campaign that is
// scheduled, running or paused, in the order given. In a launched one they start the sequence
// now, or at its start time when that has not come. A number the campaign already holds, or an earlier contact of the same call repeats, is not stored: its
// index in contacts is returned instead, so the caller can say which were skipped.
func (r *CampaignContacts) InsertMany(ctx context.Context, campaignID string, contacts []Contact) (added []Contact, duplicates []int, err error) {
	if campaignID == "" {
		return nil, nil, stack.Wrap(errors.New("store: a campaign id is required"))
	}
	if len(contacts) == 0 {
		return nil, nil, stack.Wrap(errors.New("store: there is nobody to add"))
	}

	now := time.Now().UTC()
	for index := range contacts {
		contacts[index].ID = newID()
		contacts[index].CampaignID = campaignID
		contacts[index].Stage = Enrolled
		contacts[index].CreatedAt = now
		if contacts[index].Variables == nil {
			contacts[index].Variables = map[string]string{}
		}
		if err := contacts[index].Validate(); err != nil {
			return nil, nil, stack.Wrap(err)
		}
	}

	stored := make(map[string]int64, len(contacts))
	err = r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		campaign, err := lockCampaign(ctx, tx, campaignID)
		if err != nil {
			return err
		}
		if !campaign.TakesContacts() {
			return ErrCampaignLocked
		}
		for start := 0; start < len(contacts); start += insertChunk {
			chunk := contacts[start:min(start+insertChunk, len(contacts))]
			var inserted []struct {
				ID  string
				Seq int64
			}
			// A repeat is skipped rather than failing the batch; rows the migration marked
			// duplicate are outside the index, so they do not count as holding the number.
			err := tx.NewInsert().Model(&chunk).
				On("CONFLICT (campaign_id, to_number) WHERE reason IS DISTINCT FROM 'duplicate' DO NOTHING").
				Returning("id, seq").
				Scan(ctx, &inserted)
			if err != nil {
				return fmt.Errorf("store: add contacts: %w", err)
			}
			for _, row := range inserted {
				stored[row.ID] = row.Seq
			}
		}
		// Counted after the insert, so a batch of numbers already held does not count against
		// the cap; the lock on the campaign row keeps the count from moving, and returning
		// the error rolls the insert back.
		held, err := tx.NewSelect().Model((*Contact)(nil)).Where("campaign_id = ?", campaignID).Count(ctx)
		if err != nil {
			return fmt.Errorf("store: count contacts: %w", err)
		}
		if held > MaxCampaignContacts {
			return ErrTooManyContacts
		}
		if campaign.State == Draft || len(stored) == 0 {
			return nil
		}
		at := now
		if campaign.StartAt != nil && campaign.StartAt.After(now) {
			at = *campaign.StartAt
		}
		return enroll(ctx, tx, campaign.CustomerID, campaignID, at, slices.Collect(maps.Keys(stored)))
	})
	if err != nil {
		return nil, nil, stack.Wrap(err)
	}
	for index, contact := range contacts {
		seq, ok := stored[contact.ID]
		if !ok {
			duplicates = append(duplicates, index)
			continue
		}
		contact.Seq = seq
		added = append(added, contact)
	}
	return added, duplicates, nil
}

// lockCampaign holds a campaign's row, which keeps a launch or a stop from slipping in between
// a check of its state and the write that relies on it.
func lockCampaign(ctx context.Context, tx bun.Tx, campaignID string) (Campaign, error) {
	var campaign Campaign
	err := tx.NewSelect().Model(&campaign).Column("customer_id", "state", "type", "start_at").
		Where("id = ?", campaignID).For("UPDATE").Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Campaign{}, unknownCampaign(campaignID)
	}
	if err != nil {
		return Campaign{}, fmt.Errorf("store: campaign state: %w", err)
	}
	return campaign, nil
}

// lockDraft holds a campaign's row as lockCampaign does, and refuses one that is not a draft.
func lockDraft(ctx context.Context, tx bun.Tx, campaignID string) error {
	campaign, err := lockCampaign(ctx, tx, campaignID)
	if err != nil {
		return err
	}
	if campaign.State != Draft {
		return ErrCampaignLocked
	}
	return nil
}

// enroll starts a campaign's waiting contacts on the sequence's first step at at, rejecting those
// who opted out of voice without a call. Only the contacts in ids are enrolled, or every waiting
// one when ids is nil.
func enroll(ctx context.Context, tx bun.Tx, customerID, campaignID string, at time.Time, ids []string) error {
	only := func(query *bun.UpdateQuery) *bun.UpdateQuery {
		query = query.Where("campaign_id = ?", campaignID).Where("stage = ?", Enrolled)
		if ids != nil {
			query = query.Where("id IN (?)", bun.List(ids))
		}
		return query
	}
	if _, err := only(tx.NewUpdate().Model((*Contact)(nil)).
		Set("stage = ?", Rejected).
		Set("reason = 'opted_out'").
		Set("next_attempt_at = NULL")).
		Where(`EXISTS (SELECT 1 FROM opt_outs oo WHERE oo.customer_id = ? AND oo.recipient = cc.to_number
			AND oo.channel IN ('voice', ?) AND oo.revoked_at IS NULL)`, customerID, OptOutAll).
		Exec(ctx); err != nil {
		return fmt.Errorf("store: reject opted-out contacts: %w", err)
	}
	if _, err := only(tx.NewUpdate().Model((*Contact)(nil)).
		Set("step_id = 'step_1'").
		Set("attempt = 0").
		Set("next_attempt_at = ?", at)).
		Exec(ctx); err != nil {
		return fmt.Errorf("store: enroll contacts: %w", err)
	}
	return nil
}

// DeleteAll removes every contact of a draft campaign.
func (r *CampaignContacts) DeleteAll(ctx context.Context, campaignID string) error {
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockDraft(ctx, tx, campaignID); err != nil {
			return err
		}
		if _, err := tx.NewDelete().Model((*Contact)(nil)).Where("campaign_id = ?", campaignID).Exec(ctx); err != nil {
			return fmt.Errorf("store: delete contacts: %w", err)
		}
		return nil
	})
	return stack.Wrap(err)
}

// Update changes a draft's contact to what change makes of it: its number, variables, time zone
// and instructions. change is given the contact as it is under the draft's lock, so overlapping
// changes each start from the other's result. It keeps its place in the queue.
func (r *CampaignContacts) Update(ctx context.Context, campaignID, contactID string, change func(Contact) (Contact, error)) (Contact, error) {
	var changed Contact
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockDraft(ctx, tx, campaignID); err != nil {
			return err
		}
		var found Contact
		err := tx.NewSelect().Model(&found).Where("id = ?", contactID).Where("campaign_id = ?", campaignID).Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return unknownContact(contactID)
		}
		if err != nil {
			return fmt.Errorf("store: read contact: %w", err)
		}
		changes, err := change(found)
		if err != nil {
			return err
		}
		if changes.ToNumber == "" {
			return errors.New("store: a contact needs a number to ring")
		}
		if changes.Variables == nil {
			changes.Variables = map[string]string{}
		}
		err = tx.NewUpdate().Model(&changed).
			Set("to_number = ?", changes.ToNumber).
			Set("variables = ?", changes.Variables).
			Set("time_zone = ?", changes.TimeZone).
			Set("instructions = ?", changes.Instructions).
			Where("id = ?", contactID).
			Where("campaign_id = ?", campaignID).
			Returning("*").
			Scan(ctx)
		if isViolation(err, uniqueViolation) {
			return ErrDuplicateContact
		}
		if err != nil {
			return fmt.Errorf("store: update contact: %w", err)
		}
		return nil
	})
	if err != nil {
		return Contact{}, stack.Wrap(err)
	}
	return changed, nil
}

// Delete removes one contact of a draft.
func (r *CampaignContacts) Delete(ctx context.Context, campaignID, contactID string) error {
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockDraft(ctx, tx, campaignID); err != nil {
			return err
		}
		result, err := tx.NewDelete().Model((*Contact)(nil)).
			Where("id = ?", contactID).
			Where("campaign_id = ?", campaignID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: delete contact: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("store: delete contact: %w", err)
		} else if affected == 0 {
			return unknownContact(contactID)
		}
		return nil
	})
	return stack.Wrap(err)
}

// ContactFilter narrows and pages CampaignContacts.Select. After is the seq of the last contact seen.
// Stage in_progress also finds calls awaiting an outcome, which the API shows the same way.
type ContactFilter struct {
	Stage  string
	Number string
	Limit  int
	After  *int64
}

// Select returns a campaign's contacts in the order they were added. Like
// Campaigns.Select it returns at most Limit rows.
func (r *CampaignContacts) Select(ctx context.Context, campaignID string, filter ContactFilter) ([]Contact, error) {
	if campaignID == "" {
		return nil, stack.Wrap(errors.New("store: a campaign id is required"))
	}

	var contacts []Contact
	query := r.store.global.NewSelect().Model(&contacts).Where("campaign_id = ?", campaignID)
	switch filter.Stage {
	case "":
	case InProgress:
		query = query.Where("stage IN (?)", bun.In([]string{InProgress, AwaitingOutcome}))
	default:
		query = query.Where("stage = ?", filter.Stage)
	}
	if filter.Number != "" {
		query = query.Where(`to_number LIKE ? ESCAPE '\'`, "%"+likeEscaper.Replace(filter.Number)+"%")
	}
	if filter.After != nil {
		query = query.Where("seq > ?", *filter.After)
	}
	if err := query.Order("seq ASC").Limit(clampLimit(filter.Limit, defaultCampaignLimit, maxCampaignLimit)).Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: query contacts: %w", err))
	}
	return contacts, nil
}

// Get returns one contact of a campaign with its attempts, oldest first.
func (r *CampaignContacts) Get(ctx context.Context, campaignID, contactID string) (Contact, []CampaignAttempt, error) {
	var contact Contact
	err := r.store.global.NewSelect().Model(&contact).Where("id = ?", contactID).Where("campaign_id = ?", campaignID).Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Contact{}, nil, unknownContact(contactID)
	}
	if err != nil {
		return Contact{}, nil, stack.Wrap(fmt.Errorf("store: campaign contact: %w", err))
	}
	var attempts []CampaignAttempt
	err = r.store.global.NewSelect().Model(&attempts).Where("contact_id = ?", contactID).Order("started_at ASC", "id ASC").Scan(ctx)
	if err != nil {
		return Contact{}, nil, stack.Wrap(fmt.Errorf("store: contact attempts: %w", err))
	}
	return contact, attempts, nil
}

// Stop leaves a contact unrung. One waiting for its next attempt is stopped now; one
// in a call is marked, and is stopped when the call settles, not cut off. A contact the
// sequence is done with is left as it is.
func (r *CampaignContacts) Stop(ctx context.Context, campaignID, contactID string) error {
	result, err := r.store.global.NewUpdate().Model((*Contact)(nil)).
		Set("stage = CASE WHEN stage = ? THEN ? ELSE stage END", Enrolled, ContactStopped).
		Set("next_attempt_at = CASE WHEN stage = ? THEN NULL ELSE next_attempt_at END", Enrolled).
		Set("reason = CASE WHEN stage = ? THEN reason ELSE 'stopped' END", Enrolled).
		Where("id = ?", contactID).
		Where("campaign_id = ?", campaignID).
		Where("stage IN (?)", bun.In([]string{Enrolled, InProgress, AwaitingOutcome})).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: stop contact: %w", err))
	}
	if affected, err := result.RowsAffected(); err != nil {
		return stack.Wrap(fmt.Errorf("store: stop contact: %w", err))
	} else if affected == 0 {
		if _, _, err := r.Get(ctx, campaignID, contactID); err != nil {
			return err
		}
	}
	return nil
}

func unknownContact(id string) error {
	return stack.Wrap(fmt.Errorf("%w: %s", ErrNoCampaignContact, id))
}

// ErrNotLeaseHolder is a runner settling a contact it no longer holds: another took it over
// after its lease ran out, or it was settled already.
var ErrNotLeaseHolder = errors.New("store: another runner holds this contact")

// Claimed is a contact a runner now holds the lease of, with its campaign.
type Claimed struct {
	Campaign Campaign
	Contact  Contact
}

// Claim leases up to limit contacts that are due to be rung, to owner until now+lease.
// A campaign never has more contacts in progress than its concurrency, however many runners
// claim at once.
func (r *CampaignContacts) Claim(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]Claimed, error) {
	if owner == "" {
		return nil, stack.Wrap(errors.New("store: a claim needs an owner"))
	}
	if limit <= 0 {
		return nil, nil
	}

	var claimed []Claimed
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// Holding the campaign's row is what keeps two runners from both counting the same
		// free slots: whoever holds it counts and claims, the other skips the campaign this
		// tick. At most limit campaigns are held, so the rest stay free for other runners.
		// The least busy go first, so one customer's big campaign does not starve another's.
		var campaigns []Campaign
		err := tx.NewSelect().Model(&campaigns).
			Where("cp.state = ?", Running).
			Where(`EXISTS (SELECT 1 FROM campaign_contacts cc WHERE cc.campaign_id = cp.id
				AND cc.stage = ? AND cc.next_attempt_at <= ?)`, Enrolled, now).
			Where(`cp.concurrency > (SELECT count(*) FROM campaign_contacts cc WHERE cc.campaign_id = cp.id
				AND cc.stage = ?)`, InProgress).
			OrderExpr(`(SELECT count(*) FROM campaign_contacts cc WHERE cc.campaign_id = cp.id
				AND cc.stage = ?)::float / cp.concurrency ASC`, InProgress).
			Order("cp.started_at ASC", "cp.id ASC").
			Limit(limit).
			For("UPDATE SKIP LOCKED").
			Scan(ctx)
		if err != nil {
			return fmt.Errorf("store: lock due campaigns: %w", err)
		}

		for index, campaign := range campaigns {
			remaining := limit - len(claimed)
			if remaining == 0 {
				break
			}
			// Each campaign left gets an even share of what is left, so what one cannot use
			// passes to the next.
			left := len(campaigns) - index
			share := (remaining + left - 1) / left

			var contacts []Contact
			err := tx.NewRaw(`WITH due AS (
	SELECT id FROM campaign_contacts
	WHERE campaign_id = ? AND stage = ? AND next_attempt_at <= ?
	ORDER BY next_attempt_at, seq
	LIMIT GREATEST(LEAST(?::int, ?::int - (SELECT count(*) FROM campaign_contacts WHERE campaign_id = ? AND stage = ?)), 0)
	FOR UPDATE SKIP LOCKED
), claimed AS (
	UPDATE campaign_contacts cc SET stage = ?, leased_by = ?, lease_until = ?
	FROM due WHERE cc.id = due.id
	RETURNING cc.*
)
SELECT * FROM claimed ORDER BY next_attempt_at, seq`,
				campaign.ID, Enrolled, now, share, campaign.Concurrency, campaign.ID, InProgress,
				InProgress, owner, now.Add(lease)).Scan(ctx, &contacts)
			if err != nil {
				return fmt.Errorf("store: claim due contacts: %w", err)
			}
			for _, contact := range contacts {
				claimed = append(claimed, Claimed{Campaign: campaign, Contact: contact})
			}
		}
		return nil
	})
	if err != nil {
		return nil, stack.Wrap(err)
	}
	return claimed, nil
}

// TakeOverExpired leases to owner up to limit contacts whose call's runner stopped renewing its
// lease, most likely because it died. The new owner settles the call; it never dials again.
// A contact of a campaign a data move paused is the source's until the cut-over, and its dial
// may not have been imported yet.
func (r *CampaignContacts) TakeOverExpired(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]Claimed, error) {
	if owner == "" {
		return nil, stack.Wrap(errors.New("store: a take-over needs an owner"))
	}
	if limit <= 0 {
		return nil, nil
	}

	expired := r.store.global.NewSelect().Model((*Contact)(nil)).
		Column("cc.id").
		Where("cc.stage = ? AND cc.lease_until < ?", InProgress, now).
		Where("NOT EXISTS (SELECT 1 FROM campaigns cp WHERE cp.id = cc.campaign_id AND cp.move_paused)").
		Order("cc.lease_until").
		Limit(limit).
		For("UPDATE SKIP LOCKED")
	var contacts []Contact
	err := r.store.global.NewUpdate().With("expired", expired).Model((*Contact)(nil)).
		TableExpr("expired").
		Set("leased_by = ?", owner).
		Set("lease_until = ?", now.Add(lease)).
		Where("cc.id = expired.id").
		Returning("cc.*").
		Scan(ctx, &contacts)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: take over expired leases: %w", err))
	}
	if len(contacts) == 0 {
		return nil, nil
	}

	campaigns, err := campaignsOf(ctx, r.store.global, contacts)
	if err != nil {
		return nil, stack.Wrap(err)
	}
	claimed := make([]Claimed, 0, len(contacts))
	for _, contact := range contacts {
		claimed = append(claimed, Claimed{Campaign: campaigns[contact.CampaignID], Contact: contact})
	}
	return claimed, nil
}

// Settle is where a contact goes once its lease holder is done with a call.
type Settle struct {
	// Stage is Enrolled for another attempt, AwaitingOutcome for an answered call not yet
	// classified, or where the sequence ended it.
	Stage string
	// StepID moves the contact to another step of the sequence; empty keeps the one it is on.
	StepID        string
	Attempt       int
	NextAttemptAt *time.Time
	Outcome       string
	Converted     bool
	Reason        string
	// CallID is the last dial's; empty keeps the one already stored.
	CallID string
	// LeaseUntil is the lease the owner's claim returned, which every claim and take-over sets
	// anew. Set, the contact settles only under that same lease: an owner is one process, and a
	// call that outlives its lease may find the contact taken over, settled and claimed again
	// by that same process.
	LeaseUntil *time.Time
}

// Settle records where a call left a contact and releases owner's lease on it. A contact
// stopped while in the call, or whose campaign was stopped, is stopped rather than rung again;
// one the sequence finished, or whose answered call still awaits its outcome, settles as given. A
// contact that no longer exists, because its campaign was deleted mid-call, is nothing to settle.
func (r *CampaignContacts) Settle(ctx context.Context, owner, contactID string, settle Settle) error {
	return r.settleContact(ctx, owner, contactID, settle, nil)
}

// errUnsettled rolls back a settle that touched no contact, with whatever was written before it.
var errUnsettled = errors.New("store: no contact settled")

// settleContact is Settle, with first written in the same transaction, before the
// contact, and rolled back with it when the contact does not settle.
func (r *CampaignContacts) settleContact(ctx context.Context, owner, contactID string, settle Settle, first func(context.Context, bun.Tx) error) error {
	if owner == "" || contactID == "" {
		return stack.Wrap(errors.New("store: an owner and a contact id are required"))
	}
	if settle.Stage == "" || settle.Stage == InProgress {
		return stack.Wrap(fmt.Errorf("store: a contact cannot settle in stage %q", settle.Stage))
	}

	// Only a stage that would ring the contact again gives way to a stop. An answered call
	// still gets its outcome read, and keeps the stop mark until then: a classified outcome
	// ends it completed, and one read as nobody answering is stopped rather than rung again.
	// A contact the sequence ended says why in its own reason.
	stoppable := settle.Stage == Enrolled
	keepsMark := stoppable || settle.Stage == AwaitingOutcome
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if first != nil {
			if err := first(ctx, tx); err != nil {
				return err
			}
		}
		result, err := tx.NewUpdate().Model((*Contact)(nil)).
			TableExpr("campaigns AS cp").
			Set("stage = CASE WHEN ? AND (cc.reason = 'stopped' OR cp.state = ?) THEN ? ELSE ? END",
				stoppable, Stopped, ContactStopped, settle.Stage).
			Set("next_attempt_at = CASE WHEN ? AND (cc.reason = 'stopped' OR cp.state = ?) THEN NULL ELSE ?::timestamptz END",
				stoppable, Stopped, settle.NextAttemptAt).
			Set("reason = CASE WHEN (? AND cc.reason = 'stopped') OR (? AND cp.state = ?) THEN cc.reason ELSE ? END",
				keepsMark, stoppable, Stopped, nullable(settle.Reason)).
			Set("step_id = COALESCE(?, cc.step_id)", nullable(settle.StepID)).
			Set("attempt = ?", settle.Attempt).
			Set("outcome = ?", settle.Outcome).
			Set("converted = ?", settle.Converted).
			Set("call_id = COALESCE(?, cc.call_id)", nullable(settle.CallID)).
			Set("leased_by = ''").
			Set("lease_until = NULL").
			Where("cp.id = cc.campaign_id").
			Where("cc.id = ? AND cc.leased_by = ?", contactID, owner).
			Where("cc.stage IN (?, ?)", InProgress, AwaitingOutcome).
			Where("(?::timestamptz IS NULL OR cc.lease_until = ?)", settle.LeaseUntil, settle.LeaseUntil).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: settle contact: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: settle contact: %w", err)
		}
		if affected == 0 {
			return errUnsettled
		}
		if settle.Stage == AwaitingOutcome {
			return nil
		}
		// The contact no longer waits for an outcome, so no dial of it is left to wake a
		// runner later.
		if _, err := tx.NewUpdate().Model((*CampaignAttempt)(nil)).
			Set("classify_state = ?", classifyAbandoned).
			Set("classify_after = NULL").
			Where("contact_id = ?", contactID).
			Where("classify_state = ?", classifyPending).
			Exec(ctx); err != nil {
			return fmt.Errorf("store: abandon pending classifications: %w", err)
		}
		return nil
	})
	if !errors.Is(err, errUnsettled) {
		return stack.Wrap(err)
	}
	exists, err := r.store.global.NewSelect().Model((*Contact)(nil)).Where("id = ?", contactID).Exists(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: settle contact: %w", err))
	}
	if exists {
		return stack.Wrap(ErrNotLeaseHolder)
	}
	return nil
}

// Classification is what reading an answered dial's transcript found: an outcome id, or
// no_response when nobody but the agent spoke.
type Classification struct {
	AttemptID string
	Outcome   string
	Converted bool
}

// ClassifyAndSettle records what an answered dial's transcript said and settles its contact, both
// or neither: a dial recorded while its contact stayed awaiting its outcome would never be read
// again, and the contact never move on. A dial no longer pending is not changed.
func (r *CampaignContacts) ClassifyAndSettle(ctx context.Context, owner string, classification Classification, contactID string, settle Settle) error {
	if classification.AttemptID == "" {
		return stack.Wrap(errors.New("store: a dial id is required"))
	}
	return r.settleContact(ctx, owner, contactID, settle, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewUpdate().Model((*CampaignAttempt)(nil)).
			Set("classify_after = NULL").
			Set("classify_state = ?", classifyDone).
			Set("outcome = ?", classification.Outcome).
			Set("converted = ?", classification.Converted).
			Where("id = ?", classification.AttemptID).
			Where("classify_state = ?", classifyPending).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: classify a dial: %w", err)
		}
		return nil
	})
}

// Stages counts each campaign's contacts by stage. A call awaiting its outcome counts
// as in progress, which is how the API shows it.
func (r *CampaignContacts) Stages(ctx context.Context, campaignIDs []string) (map[string]map[string]int, error) {
	stages := map[string]map[string]int{}
	if len(campaignIDs) == 0 {
		return stages, nil
	}
	var rows []struct {
		CampaignID string
		Stage      string
		Count      int
	}
	err := r.store.global.NewSelect().Model((*Contact)(nil)).
		Column("cc.campaign_id").
		ColumnExpr("CASE WHEN cc.stage = ? THEN ? ELSE cc.stage END AS stage", AwaitingOutcome, InProgress).
		ColumnExpr("count(*) AS count").
		Where("cc.campaign_id IN (?)", bun.In(campaignIDs)).
		GroupExpr("1, 2").
		Scan(ctx, &rows)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: count contact stages: %w", err))
	}
	for _, row := range rows {
		if stages[row.CampaignID] == nil {
			stages[row.CampaignID] = map[string]int{}
		}
		stages[row.CampaignID][row.Stage] = row.Count
	}
	return stages, nil
}
