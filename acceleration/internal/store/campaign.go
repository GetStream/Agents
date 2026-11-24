package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// What a campaign is doing.
const (
	// Draft is a campaign that has never been started.
	Draft = "draft"
	// Scheduled is a launched campaign waiting for its start time.
	Scheduled = "scheduled"
	// Running means the runner is working through its contacts.
	Running = "running"
	// Paused means it was stopped partway and can be started again.
	Paused = "paused"
	// Stopped means somebody ended it for good before every contact was done.
	Stopped = "stopped"
	// Completed means there is nobody left to ring.
	Completed = "completed"
)

// What kind of campaign it is.
const (
	// Standard is a campaign that rings the contacts it was given before launch, and completes
	// once it is done with them.
	Standard = "standard"
	// AutoEnroll also takes contacts while it is scheduled, running or paused, and never
	// completes on its own.
	AutoEnroll = "auto_enroll"
)

// Campaign is a list of people to ring and one agent to ring them with.
type Campaign struct {
	bun.BaseModel `bun:"table:campaigns,alias:cp"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	Name       string `bun:"name,notnull"`
	// ConfigID is the agent that makes the calls.
	ConfigID string `bun:"config_id,notnull"`
	// FromNumber is one of the customer's own, which is what the person sees.
	FromNumber string `bun:"from_number,notnull"`
	// Concurrency is how many of these calls may be happening at once.
	Concurrency int `bun:"concurrency,notnull"`
	// Sequence is the steps each contact goes through, and CallingHours when they may be
	// rung. Both are null on a draft until the API fills them.
	Sequence     json.RawMessage `bun:"sequence,type:jsonb"`
	CallingHours json.RawMessage `bun:"calling_hours,type:jsonb"`
	// DefaultCountry is the region a contact's number without a country code is read in.
	DefaultCountry string            `bun:"default_country,notnull"`
	State          string            `bun:"state,notnull"`
	Type           string            `bun:"type,notnull"`
	Tags           map[string]string `bun:"tags,type:jsonb,nullzero"`
	CreatedAt      time.Time         `bun:"created_at,notnull"`
	StartAt        *time.Time        `bun:"start_at"`
	LaunchedAt     *time.Time        `bun:"launched_at"`
	StartedAt      *time.Time        `bun:"started_at"`
	StoppedAt      *time.Time        `bun:"stopped_at"`
	ConsentAt      *time.Time        `bun:"consent_at"`
	// FinishedAt is when the campaign completed.
	FinishedAt *time.Time `bun:"finished_at"`
	// PausedReason is the code of what refused a call when the router paused the campaign
	// itself, as telephony_suspended or insufficient_credits; empty otherwise.
	PausedReason string `bun:"paused_reason,nullzero"`

	// APIKey, UserID and UsageCategories are who scheduled the campaign, as the request that
	// created it named them: the calls it places later are owned by them.
	APIKey          string   `bun:"api_key,notnull"`
	UserID          string   `bun:"user_id,notnull"`
	UsageCategories []string `bun:"usage_categories,array,nullzero"`
}

// Validate refuses a campaign the runner could not ring with.
func (c *Campaign) Validate() error {
	return validate("campaign").
		required("customer_id", c.CustomerID).
		required("name", c.Name).
		check(c.Concurrency >= 1 && c.Concurrency <= maxConcurrency, "concurrency", fmt.Sprintf("is not between 1 and %d", maxConcurrency)).
		oneOf("state", c.State, Draft, Scheduled, Running, Paused, Stopped, Completed).
		oneOf("type", c.Type, Standard, AutoEnroll).
		done()
}

// Campaigns are customers' campaigns, in the shared database.
type Campaigns struct {
	store *Store
}

// ErrCampaignLocked is a change a campaign's state does not allow.
var ErrCampaignLocked = errors.New("store: the campaign is in a state that does not allow this")

// ErrNoCampaign is a campaign the customer does not hold.
var ErrNoCampaign = errors.New("store: there is no campaign")

// defaultConcurrency is how many calls a campaign that did not say may have at once. One
// is the safe answer: a campaign that rings a thousand people the moment it starts is a
// mistake nobody meant to make.
const defaultConcurrency = 1

const (
	defaultCampaignLimit = 50
	maxCampaignLimit     = 1001
)

// maxConcurrency bounds what a customer may ask for, since every call in flight is a
// conversation this process is running.
const maxConcurrency = 50

// Insert stores a campaign and fills in its id and timestamps.
func (r *Campaigns) Insert(ctx context.Context, campaign *Campaign) error {
	if campaign.Concurrency <= 0 {
		campaign.Concurrency = defaultConcurrency
	}
	campaign.State = Draft
	if campaign.Type == "" {
		campaign.Type = Standard
	}
	if err := campaign.Validate(); err != nil {
		return stack.Wrap(err)
	}

	campaign.ID = newID()
	campaign.CreatedAt = time.Now().UTC()
	if campaign.Tags == nil {
		campaign.Tags = map[string]string{}
	}

	if _, err := r.store.global.NewInsert().Model(campaign).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: create campaign: %w", err))
	}
	return nil
}

// Get returns one campaign a customer holds.
func (r *Campaigns) Get(ctx context.Context, customerID, id string) (Campaign, error) {
	return campaignIn(ctx, r.store.global, customerID, id)
}

func campaignIn(ctx context.Context, db bun.IDB, customerID, id string) (Campaign, error) {
	if customerID == "" || id == "" {
		return Campaign{}, stack.Wrap(errors.New("store: a customer and a campaign id are required"))
	}

	var campaign Campaign
	err := db.NewSelect().Model(&campaign).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Campaign{}, unknownCampaign(id)
	}
	if err != nil {
		return Campaign{}, stack.Wrap(fmt.Errorf("store: campaign: %w", err))
	}
	return campaign, nil
}

// CampaignFilter narrows and pages Campaigns.Select. It returns at most Limit rows: a caller
// that wants to know whether another page follows asks for one more than it shows.
type CampaignFilter struct {
	State string
	Limit int
	After *CreatedPosition
}

// Select returns a customer's campaigns, newest first.
func (r *Campaigns) Select(ctx context.Context, customerID string, filter CampaignFilter) ([]Campaign, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}

	var campaigns []Campaign
	query := r.store.global.NewSelect().Model(&campaigns).Where("customer_id = ?", customerID)
	if filter.State != "" {
		query = query.Where("state = ?", filter.State)
	}
	if filter.After != nil {
		query = query.Where("(created_at, id) < (?, ?)", filter.After.At, filter.After.ID)
	}
	err := query.Order("created_at DESC", "id DESC").Limit(clampLimit(filter.Limit, defaultCampaignLimit, maxCampaignLimit)).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: query campaigns: %w", err))
	}
	return campaigns, nil
}

// CampaignChanges are the fields an update sets; a nil one is left as it is.
type CampaignChanges struct {
	Name, ConfigID, FromNumber, DefaultCountry *string
	Concurrency                                *int
	Sequence, CallingHours                     json.RawMessage
	Tags                                       map[string]string
}

// Update changes a campaign. Once it has launched only its name, number and
// concurrency may change: the rest is what the customer consented to ring people with.
func (r *Campaigns) Update(ctx context.Context, customerID, id string, changes CampaignChanges) (Campaign, error) {
	if customerID == "" || id == "" {
		return Campaign{}, stack.Wrap(errors.New("store: a customer and a campaign id are required"))
	}
	if changes.Concurrency != nil && (*changes.Concurrency < 1 || *changes.Concurrency > maxConcurrency) {
		return Campaign{}, stack.Wrap(fmt.Errorf("store: a campaign may run between 1 and %d calls at once", maxConcurrency))
	}

	query := r.store.global.NewUpdate().Model((*Campaign)(nil)).Where("id = ?", id).Where("customer_id = ?", customerID)
	changed := 0
	var same []string
	var sameArgs []any
	set := func(column, cast string, value any, frozen bool) {
		query = query.Set(column+" = ?"+cast, value)
		changed++
		if frozen {
			same = append(same, column+" IS NOT DISTINCT FROM ?"+cast)
			sameArgs = append(sameArgs, value)
		}
	}
	if changes.Name != nil {
		set("name", "", *changes.Name, false)
	}
	if changes.FromNumber != nil {
		set("from_number", "", *changes.FromNumber, false)
	}
	if changes.Concurrency != nil {
		set("concurrency", "", *changes.Concurrency, false)
	}
	if changes.ConfigID != nil {
		set("config_id", "", *changes.ConfigID, true)
	}
	if changes.DefaultCountry != nil {
		set("default_country", "", *changes.DefaultCountry, true)
	}
	if changes.Sequence != nil {
		set("sequence", "::jsonb", string(changes.Sequence), true)
	}
	if changes.CallingHours != nil {
		set("calling_hours", "::jsonb", string(changes.CallingHours), true)
	}
	if changes.Tags != nil {
		tags, err := json.Marshal(changes.Tags)
		if err != nil {
			return Campaign{}, stack.Wrap(fmt.Errorf("store: update campaign tags: %w", err))
		}
		set("tags", "::jsonb", string(tags), true)
	}
	if changed == 0 {
		return r.Get(ctx, customerID, id)
	}
	// A launched campaign accepts a locked field only when it says what is already there, so
	// a form that sends the whole object does not have to leave the rest out.
	if len(same) > 0 {
		query = query.Where("(state = ? OR ("+strings.Join(same, " AND ")+"))", append([]any{Draft}, sameArgs...)...)
	}

	var updated []Campaign
	if err := query.Returning("*").Scan(ctx, &updated); err != nil {
		return Campaign{}, stack.Wrap(fmt.Errorf("store: update campaign: %w", err))
	}
	if len(updated) == 0 {
		return Campaign{}, r.whyNotChanged(ctx, r.store.global, customerID, id)
	}
	return updated[0], nil
}

// whyNotChanged names why a guarded write touched no row: there is no such campaign, or its
// state refused it.
func (r *Campaigns) whyNotChanged(ctx context.Context, db bun.IDB, customerID, id string) error {
	if _, err := campaignIn(ctx, db, customerID, id); err != nil {
		return err
	}
	return stack.Wrap(ErrCampaignLocked)
}

// Delete removes a campaign that is not ringing anybody, with its contacts. Its
// calls stay: they are what was billed and recorded.
func (r *Campaigns) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a campaign id are required"))
	}
	result, err := r.store.global.NewDelete().Model((*Campaign)(nil)).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("state IN (?)", bun.In([]string{Draft, Stopped, Completed})).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete campaign: %w", err))
	}
	return r.guarded(ctx, result, customerID, id)
}

// guarded turns a write that touched no row into the error saying why.
func (r *Campaigns) guarded(ctx context.Context, result sql.Result, customerID, id string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: campaign write: %w", err))
	}
	if affected == 0 {
		return r.whyNotChanged(ctx, r.store.global, customerID, id)
	}
	return nil
}

// Duplicate contacts, in Campaigns.Duplicate.
const (
	DuplicateNone         = "none"
	DuplicateAll          = "all"
	DuplicateNotContacted = "not_contacted"
)

// Duplicate makes a draft with a campaign's settings and, as contacts says, none of
// its contacts, all of them or those never dialed. Contacts start over, enrolled with no
// attempts. Never dialed means the campaign has finished dialing: from a stopped or completed one.
func (r *Campaigns) Duplicate(ctx context.Context, customerID, id, contacts string) (Campaign, error) {
	if contacts != DuplicateNone && contacts != DuplicateAll && contacts != DuplicateNotContacted {
		return Campaign{}, stack.Wrap(fmt.Errorf("store: duplicate contacts must be none, all or not_contacted, not %q", contacts))
	}

	var copied Campaign
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		source, err := campaignIn(ctx, tx, customerID, id)
		if err != nil {
			return err
		}
		if contacts == DuplicateNotContacted && source.State != Stopped && source.State != Completed {
			return stack.Wrap(ErrCampaignLocked)
		}

		copied = Campaign{
			ID: newID(), CustomerID: source.CustomerID, Name: "Copy of " + source.Name, ConfigID: source.ConfigID,
			FromNumber: source.FromNumber, Concurrency: source.Concurrency, Sequence: source.Sequence,
			CallingHours: source.CallingHours, DefaultCountry: source.DefaultCountry, State: Draft, Type: source.Type,
			Tags: source.Tags, CreatedAt: time.Now().UTC(),
		}
		if _, err := tx.NewInsert().Model(&copied).Exec(ctx); err != nil {
			return fmt.Errorf("store: duplicate campaign: %w", err)
		}
		if contacts == DuplicateNone {
			return nil
		}

		// A row the migration marked duplicate repeats a number another row holds, so it is
		// left behind: copying it would break the unique number index.
		query := `INSERT INTO campaign_contacts (id, campaign_id, to_number, instructions, variables, time_zone, stage, created_at)
SELECT replace(gen_random_uuid()::text, '-', ''), ?, to_number, instructions, variables, time_zone, 'enrolled', now()
FROM campaign_contacts cc
WHERE campaign_id = ? AND reason IS DISTINCT FROM 'duplicate'`
		if contacts == DuplicateNotContacted {
			// An attempt that was deferred or refused rang nobody.
			query += ` AND NOT EXISTS (SELECT 1 FROM campaign_attempts ca WHERE ca.contact_id = cc.id AND ` + placedDial + `)`
		}
		if _, err := tx.ExecContext(ctx, query+" ORDER BY seq", copied.ID, source.ID); err != nil {
			return fmt.Errorf("store: duplicate contacts: %w", err)
		}
		return nil
	})
	if err != nil {
		return Campaign{}, stack.Wrap(err)
	}
	return copied, nil
}

// Launch is what starting a campaign records. Now is passed in so the router decides once what
// time it is for the whole launch.
type Launch struct {
	StartAt, Now time.Time
	// APIKey, UserID and UsageCategories are who launched it: the calls it places later are
	// owned by them.
	APIKey, UserID  string
	UsageCategories []string
}

// Launch takes a draft out of draft. The customer's consent is recorded, contacts
// that opted out of voice are rejected without a call, and the rest are enrolled to be
// rung from StartAt.
func (r *Campaigns) Launch(ctx context.Context, customerID, id string, launch Launch) (Campaign, error) {
	state := Running
	if launch.StartAt.After(launch.Now) {
		state = Scheduled
	}
	// The column holds an empty list, never NULL, for a request that named no categories.
	categories := launch.UsageCategories
	if categories == nil {
		categories = []string{}
	}

	var launched Campaign
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		query := tx.NewUpdate().Model((*Campaign)(nil)).
			Set("state = ?", state).
			Set("start_at = ?", launch.StartAt).
			Set("launched_at = ?", launch.Now).
			Set("consent_at = ?", launch.Now).
			Set("api_key = ?", launch.APIKey).
			Set("user_id = ?", launch.UserID).
			Set("usage_categories = ?", pgdialect.Array(categories)).
			Where("id = ?", id).
			Where("customer_id = ?", customerID).
			Where("state = ?", Draft)
		if state == Running {
			query = query.Set("started_at = ?", launch.Now)
		}
		result, err := query.Exec(ctx)
		if err != nil {
			return fmt.Errorf("store: launch campaign: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("store: launch campaign: %w", err)
		} else if affected == 0 {
			return r.whyNotChanged(ctx, tx, customerID, id)
		}

		if err := enroll(ctx, tx, customerID, id, launch.StartAt, nil); err != nil {
			return err
		}
		launched, err = campaignIn(ctx, tx, customerID, id)
		return err
	})
	if err != nil {
		return Campaign{}, stack.Wrap(err)
	}
	return launched, nil
}

// TakesContacts is a campaign people may be added to: a draft, or an auto-enroll campaign that
// has not ended.
func (c Campaign) TakesContacts() bool {
	if c.State == Draft {
		return true
	}
	return c.Type == AutoEnroll && slices.Contains([]string{Scheduled, Running, Paused}, c.State)
}

// Pause pauses a running or scheduled campaign. A reason names what refused its next call when the
// router paused it itself; empty means somebody paused it.
func (r *Campaigns) Pause(ctx context.Context, customerID, id, reason string) error {
	return r.transition(ctx, customerID, id, []string{Running, Scheduled}, Paused, func(query *bun.UpdateQuery) *bun.UpdateQuery {
		return query.Set("paused_reason = NULLIF(?, '')", reason)
	})
}

// Resume starts a paused campaign again, or returns it to scheduled when its start
// time has not come.
func (r *Campaigns) Resume(ctx context.Context, customerID, id string) error {
	campaign, err := r.Get(ctx, customerID, id)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	to := Running
	if campaign.StartAt != nil && campaign.StartAt.After(now) {
		to = Scheduled
	}
	return r.transition(ctx, customerID, id, []string{Paused}, to, func(query *bun.UpdateQuery) *bun.UpdateQuery {
		query = query.Set("paused_reason = NULL")
		if to == Running {
			query = query.Set("started_at = COALESCE(started_at, ?)", now)
		}
		return query
	})
}

// Stop ends a campaign for good. Contacts still waiting stay enrolled, and are never
// rung; calls in progress are left to finish.
func (r *Campaigns) Stop(ctx context.Context, customerID, id string, now time.Time) error {
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		err := r.transitionIn(ctx, tx, customerID, id, []string{Scheduled, Running, Paused}, Stopped, func(query *bun.UpdateQuery) *bun.UpdateQuery {
			return query.Set("paused_reason = NULL").Set("stopped_at = ?", now)
		})
		if err != nil {
			return err
		}
		if _, err := tx.NewUpdate().Model((*Contact)(nil)).
			Set("next_attempt_at = NULL").
			Where("campaign_id = ?", id).
			Where("stage = ?", Enrolled).
			Exec(ctx); err != nil {
			return fmt.Errorf("store: stop campaign contacts: %w", err)
		}
		return nil
	})
	return stack.Wrap(err)
}

func (r *Campaigns) transition(ctx context.Context, customerID, id string, from []string, to string, set func(*bun.UpdateQuery) *bun.UpdateQuery) error {
	return stack.Wrap(r.transitionIn(ctx, r.store.global, customerID, id, from, to, set))
}

// transitionIn moves a campaign from one of the states to another, and says why when it was not
// in one of them. A move clears the data move's mark: the customer has now decided what the
// campaign does here.
func (r *Campaigns) transitionIn(ctx context.Context, db bun.IDB, customerID, id string, from []string, to string, set func(*bun.UpdateQuery) *bun.UpdateQuery) error {
	if customerID == "" || id == "" {
		return errors.New("store: a customer and a campaign id are required")
	}
	query := db.NewUpdate().Model((*Campaign)(nil)).
		Set("state = ?", to).
		Set("move_paused = false").
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("state IN (?)", bun.In(from))
	result, err := set(query).Exec(ctx)
	if err != nil {
		return fmt.Errorf("store: move campaign to %s: %w", to, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: move campaign to %s: %w", to, err)
	}
	if affected == 0 {
		return r.whyNotChanged(ctx, db, customerID, id)
	}
	return nil
}

func unknownCampaign(id string) error {
	return stack.Wrap(fmt.Errorf("%w %s", ErrNoCampaign, id))
}

// StartDue starts the scheduled campaigns whose start time has come.
func (r *Campaigns) StartDue(ctx context.Context, now time.Time) error {
	_, err := r.store.global.NewUpdate().Model((*Campaign)(nil)).
		Set("state = ?", Running).
		Set("started_at = COALESCE(started_at, ?)", now).
		Where("state = ?", Scheduled).
		Where("start_at <= ?", now).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: start due campaigns: %w", err))
	}
	return nil
}

// Complete completes the running campaigns that have nobody left to ring and no call
// whose outcome is still unknown. An auto-enroll campaign waits for more people instead.
func (r *Campaigns) Complete(ctx context.Context, now time.Time) error {
	_, err := r.store.global.NewUpdate().Model((*Campaign)(nil)).
		Set("state = ?", Completed).
		Set("finished_at = ?", now).
		Where("cp.state = ?", Running).
		Where("cp.type = ?", Standard).
		Where(`NOT EXISTS (SELECT 1 FROM campaign_contacts cc WHERE cc.campaign_id = cp.id
			AND ((cc.stage = ? AND cc.next_attempt_at IS NOT NULL) OR cc.stage IN (?, ?)))`,
			Enrolled, InProgress, AwaitingOutcome).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: complete campaigns: %w", err))
	}
	return nil
}

// campaignsOf returns the campaigns of contacts, by id.
func campaignsOf(ctx context.Context, db bun.IDB, contacts []Contact) (map[string]Campaign, error) {
	var ids []string
	for _, contact := range contacts {
		if !slices.Contains(ids, contact.CampaignID) {
			ids = append(ids, contact.CampaignID)
		}
	}
	var campaigns []Campaign
	if err := db.NewSelect().Model(&campaigns).Where("id IN (?)", bun.In(ids)).Scan(ctx); err != nil {
		return nil, fmt.Errorf("store: campaigns of contacts: %w", err)
	}
	byID := make(map[string]Campaign, len(campaigns))
	for _, campaign := range campaigns {
		byID[campaign.ID] = campaign
	}
	return byID, nil
}

// CampaignAnalytics is how a campaign's calls went. Dialed, Connected and Converted count
// contacts, not dials: one contact rung three times is one.
type CampaignAnalytics struct {
	Dialed    int
	Connected int
	Converted int
	// Outcomes counts the contacts that are done, by outcome.
	Outcomes map[string]int
	// CallOutcomes counts the calls that ended, by how the phone says they ended.
	CallOutcomes map[string]int
	Days         []DialDay
}

// DialDay is the dials placed on one UTC day, and how many were answered.
type DialDay struct {
	Day       time.Time
	Dials     int
	Answered  int
	Voicemail int
}

// CampaignTotal is what the campaign list shows of each campaign.
type CampaignTotal struct {
	Dialed    int
	Converted int
}

// A dial is an attempt that placed a call: one with no result, still happening or ended as its
// call_outcome says, or one an answering machine took. Deferred, paused, refused, invalid_number
// and not_placed attempts placed none. An answered dial is a person on a call: its phone joined,
// no machine took it, and somebody spoke.
const (
	placedDial   = `result IN ('', 'voicemail')`
	answeredDial = `result = '' AND call_outcome = 'completed' AND outcome <> 'no_response'`
)

// Analytics counts a campaign's contacts and dials. Days run oldest first and hold
// only the days with a dial.
func (r *Campaigns) Analytics(ctx context.Context, campaignID string) (CampaignAnalytics, error) {
	totals, err := r.Totals(ctx, []string{campaignID})
	if err != nil {
		return CampaignAnalytics{}, err
	}
	analytics := CampaignAnalytics{
		Dialed:       totals[campaignID].Dialed,
		Converted:    totals[campaignID].Converted,
		Outcomes:     map[string]int{},
		CallOutcomes: map[string]int{},
		Days:         []DialDay{},
	}

	err = r.store.global.NewRaw(`SELECT count(*) FROM campaign_contacts cc
WHERE cc.campaign_id = ? AND EXISTS (SELECT 1 FROM campaign_attempts ca
	WHERE ca.contact_id = cc.id AND `+answeredDial+`)`, campaignID).Scan(ctx, &analytics.Connected)
	if err != nil {
		return CampaignAnalytics{}, stack.Wrap(fmt.Errorf("store: count connected contacts: %w", err))
	}

	var outcomes []struct {
		Outcome string
		Count   int
	}
	err = r.store.global.NewSelect().Model((*Contact)(nil)).
		Column("cc.outcome").
		ColumnExpr("count(*) AS count").
		Where("cc.campaign_id = ? AND cc.stage = ?", campaignID, ContactCompleted).
		Group("cc.outcome").
		Scan(ctx, &outcomes)
	if err != nil {
		return CampaignAnalytics{}, stack.Wrap(fmt.Errorf("store: count outcomes: %w", err))
	}
	for _, row := range outcomes {
		analytics.Outcomes[row.Outcome] = row.Count
	}

	var calls []struct {
		CallOutcome string
		Count       int
	}
	err = r.store.global.NewSelect().Model((*CampaignAttempt)(nil)).
		Column("ca.call_outcome").
		ColumnExpr("count(*) AS count").
		Where("ca.campaign_id = ? AND ca.call_outcome IS NOT NULL", campaignID).
		Group("ca.call_outcome").
		Scan(ctx, &calls)
	if err != nil {
		return CampaignAnalytics{}, stack.Wrap(fmt.Errorf("store: count call outcomes: %w", err))
	}
	for _, row := range calls {
		analytics.CallOutcomes[row.CallOutcome] = row.Count
	}

	err = r.store.global.NewRaw(`SELECT date_trunc('day', started_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS day,
	count(*) AS dials, count(*) FILTER (WHERE `+answeredDial+`) AS answered,
	count(*) FILTER (WHERE result = 'voicemail') AS voicemail
FROM campaign_attempts WHERE campaign_id = ? AND `+placedDial+` GROUP BY 1 ORDER BY 1`, campaignID).Scan(ctx, &analytics.Days)
	if err != nil {
		return CampaignAnalytics{}, stack.Wrap(fmt.Errorf("store: count dials per day: %w", err))
	}
	for i := range analytics.Days {
		analytics.Days[i].Day = analytics.Days[i].Day.UTC()
	}
	return analytics, nil
}

// Totals counts, per campaign, the contacts dialed and converted, as Analytics does.
func (r *Campaigns) Totals(ctx context.Context, campaignIDs []string) (map[string]CampaignTotal, error) {
	totals := map[string]CampaignTotal{}
	if len(campaignIDs) == 0 {
		return totals, nil
	}
	var rows []struct {
		CampaignID string
		Dialed     int
		Converted  int
	}
	err := r.store.global.NewRaw(`SELECT cc.campaign_id,
	count(*) FILTER (WHERE EXISTS (SELECT 1 FROM campaign_attempts ca WHERE ca.contact_id = cc.id AND `+placedDial+`)) AS dialed,
	count(*) FILTER (WHERE cc.converted) AS converted
FROM campaign_contacts cc WHERE cc.campaign_id IN (?) GROUP BY cc.campaign_id`, bun.In(campaignIDs)).Scan(ctx, &rows)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: count campaign totals: %w", err))
	}
	for _, row := range rows {
		totals[row.CampaignID] = CampaignTotal{Dialed: row.Dialed, Converted: row.Converted}
	}
	return totals, nil
}
