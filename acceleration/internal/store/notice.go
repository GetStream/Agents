package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// The kinds of notice (Notice.Kind). The money limits an organization is first refused at:
// its credit, Stream's spend hard limit for the day or the month, and one of its own spend
// limits; the warnings it is sent as its credit runs out; and a day its spend jumped past its
// own baseline.
const (
	LimitNoCredit          = "no_credit"
	LimitSpendHardLimit    = "spend_hard_limit"
	LimitSpendLimit        = "spend_limit"
	NoticeBalanceLow       = "balance_low"
	NoticeBalanceExhausted = "balance_exhausted"
	NoticeCostAnomaly      = "cost_anomaly"
)

// StreamSource is the built-in source of the events the router itself sends a customer, such
// as a warning that its organization's credit is running out. Its destinations are connector
// event destinations whose connector id is StreamSource, which need no provider app.
const StreamSource = "stream"

// Notice is something an organization is told once per kind and DedupeKey: the first router to
// write the row is the one that sends it, and one whose send failed is sent by whichever router
// claims it next. Each kind's own fields are in Payload, in the shape of LimitNotice,
// BalanceNotice or CostAnomaly.
type Notice struct {
	bun.BaseModel `bun:"table:notices,alias:nt"`

	OrganizationID string `bun:"organization_id,pk"`
	Kind           string `bun:"kind,pk"`
	// DedupeKey is what makes a kind's notice due again: nothing for a money limit, noticed
	// once ever, the credit granted for a balance warning, the UTC day for a cost anomaly.
	DedupeKey string          `bun:"dedupe_key,pk"`
	Payload   json.RawMessage `bun:"payload,type:jsonb,notnull"`
	CreatedAt time.Time       `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	ClaimedAt time.Time       `bun:"claimed_at,nullzero,notnull,default:current_timestamp"`
	PostedAt  *time.Time      `bun:"posted_at"`
}

// Validate refuses a notice of a kind there is none of, outside an organization, with a key
// its kind does not make, or whose payload is not an object.
func (n *Notice) Validate() error {
	var payload map[string]any
	keyed := true
	switch n.Kind {
	case LimitNoCredit, LimitSpendHardLimit, LimitSpendLimit:
		keyed = n.DedupeKey == ""
	case NoticeBalanceLow, NoticeBalanceExhausted:
		_, err := strconv.ParseInt(n.DedupeKey, 10, 64)
		keyed = err == nil
	case NoticeCostAnomaly:
		_, err := time.Parse(time.DateOnly, n.DedupeKey)
		keyed = err == nil
	}
	return validate("notice").
		required("organization_id", n.OrganizationID).
		oneOf("kind", n.Kind, LimitNoCredit, LimitSpendHardLimit, LimitSpendLimit, NoticeBalanceLow, NoticeBalanceExhausted, NoticeCostAnomaly).
		check(keyed, "dedupe_key", fmt.Sprintf("is %q, which a %s notice is not keyed by", n.DedupeKey, n.Kind)).
		check(json.Unmarshal(n.Payload, &payload) == nil && payload != nil, "payload", "is not a JSON object").
		done()
}

// LimitNotice is the first time an organization was refused at a kind of money limit, or would
// have been in shadow (Enforced false): a Notice of kind LimitNoCredit, LimitSpendHardLimit or
// LimitSpendLimit.
type LimitNotice struct {
	OrganizationID string `json:"-"`
	Kind           string `json:"-"`
	AppID          string `json:"app_id"`
	Tier           string `json:"tier"`
	Enforced       bool   `json:"enforced"`
	Message        string `json:"message"`
}

// Notice is the row the limit notice is kept as.
func (l LimitNotice) Notice() (Notice, error) {
	return noticeOf(l.OrganizationID, l.Kind, "", l)
}

// LimitNotice reads the notice as a money limit's.
func (n Notice) LimitNotice() (LimitNotice, error) {
	notice := LimitNotice{OrganizationID: n.OrganizationID, Kind: n.Kind}
	return notice, n.decode(&notice)
}

// BalanceNotice is one warning sent that an organization's credit is running out: a Notice of
// kind NoticeBalanceLow or NoticeBalanceExhausted keyed by the credit granted, so a top-up or a
// grant re-arms it.
type BalanceNotice struct {
	OrganizationID  string `json:"-"`
	Kind            string `json:"-"`
	GrantedMicros   int64  `json:"-"`
	BalanceMicros   int64  `json:"balance_micros"`
	ThresholdMicros int64  `json:"threshold_micros"`
}

// Notice is the row the balance warning is kept as.
func (b BalanceNotice) Notice() (Notice, error) {
	return noticeOf(b.OrganizationID, b.Kind, strconv.FormatInt(b.GrantedMicros, 10), b)
}

// BalanceNotice reads the notice as a balance warning.
func (n Notice) BalanceNotice() (BalanceNotice, error) {
	granted, err := strconv.ParseInt(n.DedupeKey, 10, 64)
	if err != nil {
		return BalanceNotice{}, stack.Wrap(fmt.Errorf("store: balance notice grant: %w", err))
	}
	notice := BalanceNotice{OrganizationID: n.OrganizationID, Kind: n.Kind, GrantedMicros: granted}
	return notice, n.decode(&notice)
}

// CostAnomaly is a UTC day on which an organization's spend jumped past its own baseline: what
// it had spent that day when the check saw it, and its median day before. It is a Notice of
// kind NoticeCostAnomaly keyed by the day.
type CostAnomaly struct {
	OrganizationID string    `json:"-"`
	Day            time.Time `json:"-"`
	SpentMicros    int64     `json:"spent_micros"`
	BaselineMicros int64     `json:"baseline_micros"`
}

// Notice is the row the anomaly is kept as.
func (c CostAnomaly) Notice() (Notice, error) {
	return noticeOf(c.OrganizationID, NoticeCostAnomaly, c.Day.UTC().Format(time.DateOnly), c)
}

// CostAnomaly reads the notice as a cost anomaly.
func (n Notice) CostAnomaly() (CostAnomaly, error) {
	day, err := time.Parse(time.DateOnly, n.DedupeKey)
	if err != nil {
		return CostAnomaly{}, stack.Wrap(fmt.Errorf("store: cost anomaly day: %w", err))
	}
	anomaly := CostAnomaly{OrganizationID: n.OrganizationID, Day: day}
	return anomaly, n.decode(&anomaly)
}

func noticeOf(organizationID, kind, key string, payload any) (Notice, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Notice{}, stack.Wrap(fmt.Errorf("store: encode a %s notice: %w", kind, err))
	}
	return Notice{OrganizationID: organizationID, Kind: kind, DedupeKey: key, Payload: raw}, nil
}

func (n Notice) decode(into any) error {
	if err := json.Unmarshal(n.Payload, into); err != nil {
		return stack.Wrap(fmt.Errorf("store: decode a %s notice: %w", n.Kind, err))
	}
	return nil
}

// Notices are the notices organizations were sent, in the shared database.
type Notices struct {
	store *Store
}

// Insert writes the notice unless one of its organization, kind and key already is. written
// says this call wrote it, and so holds the claim to send it.
func (r *Notices) Insert(ctx context.Context, notice *Notice) (written bool, err error) {
	if err := notice.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	notice.CreatedAt, notice.ClaimedAt, notice.PostedAt = time.Time{}, time.Time{}, nil
	result, err := r.store.global.NewInsert().Model(notice).On("CONFLICT DO NOTHING").Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: write a notice: %w", err))
	}
	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

// InsertQueued writes the notice and, when this call is the one that wrote it, queues one
// delivery of it under the delivery's id and headers for every StreamSource destination of
// every app the organization was seen with, each with the body body makes for its app: all in
// one transaction, so a notice written is a notice queued, and so posted as it is written. sent
// says this call wrote it, and queued is how many deliveries it queued.
func (r *Notices) InsertQueued(ctx context.Context, notice *Notice, delivery EventDelivery, body func(appID string) ([]byte, error)) (sent bool, queued int, err error) {
	if err := notice.Validate(); err != nil {
		return false, 0, stack.Wrap(err)
	}
	if delivery.ID == "" {
		return false, 0, stack.Wrap(&ValidationError{Model: "event delivery", Field: "id", Reason: "is required"})
	}
	err = r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		notice.CreatedAt, notice.ClaimedAt = time.Time{}, time.Time{}
		posted := time.Now().UTC()
		notice.PostedAt = &posted
		written, err := tx.NewInsert().Model(notice).On("CONFLICT DO NOTHING").Exec(ctx)
		if err != nil {
			return err
		}
		if affected, _ := written.RowsAffected(); affected == 0 {
			return nil
		}
		sent = true
		var destinations []struct {
			ID         string `bun:"id"`
			CustomerID string `bun:"customer_id"`
		}
		err = tx.NewSelect().TableExpr("connector_event_destinations AS ced").
			ColumnExpr("ced.id, ced.customer_id").
			Join("JOIN app_organizations AS ao ON ao.app_id = ced.customer_id").
			Where("ao.organization_id = ?", notice.OrganizationID).
			Where("ced.connector_id = ?", StreamSource).
			Scan(ctx, &destinations)
		if err != nil || len(destinations) == 0 {
			return err
		}
		rows := make([]EventDelivery, 0, len(destinations))
		for _, destination := range destinations {
			payload, err := body(destination.CustomerID)
			if err != nil {
				return err
			}
			rows = append(rows, EventDelivery{
				DestinationID: destination.ID, ID: delivery.ID, Headers: delivery.Headers, Body: payload,
				NextAttemptAt: delivery.NextAttemptAt.UTC(),
			})
		}
		inserted, err := tx.NewInsert().Model(&rows).
			Column("destination_id", "id", "headers", "body", "attempts", "next_attempt_at").
			On("CONFLICT (destination_id, id) DO NOTHING").
			Exec(ctx)
		if err != nil {
			return err
		}
		n, _ := inserted.RowsAffected()
		queued = int(n)
		return nil
	})
	if err != nil {
		return false, 0, stack.Wrap(fmt.Errorf("store: queue a notice: %w", err))
	}
	return sent, queued, nil
}

// Claim claims, and returns, up to limit notices never posted whose last claim is older than
// stale, oldest claim first, so a router that wrote one and stopped, or failed to send it,
// leaves it for another to send.
func (r *Notices) Claim(ctx context.Context, stale time.Duration, limit int) ([]Notice, error) {
	var notices []Notice
	due := r.store.global.NewSelect().Model((*Notice)(nil)).Column("organization_id", "kind", "dedupe_key").
		Where("posted_at IS NULL").
		Where("claimed_at < now() - ? * interval '1 millisecond'", stale.Milliseconds()).
		OrderExpr("claimed_at").Limit(limit).For("UPDATE SKIP LOCKED")
	err := r.store.global.NewUpdate().Model((*Notice)(nil)).
		Set("claimed_at = now()").
		Where("(nt.organization_id, nt.kind, nt.dedupe_key) IN (?)", due).
		Returning("*").
		Scan(ctx, &notices)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: claim notices: %w", err))
	}
	return notices, nil
}

// MarkPosted records that the notice was sent, so no claim takes it again.
func (r *Notices) MarkPosted(ctx context.Context, notice Notice) error {
	if notice.OrganizationID == "" || notice.Kind == "" {
		return stack.Wrap(errors.New("store: a notice is known by its organization, kind and key"))
	}
	_, err := r.store.global.NewUpdate().Model((*Notice)(nil)).
		Set("posted_at = now()").
		Where("organization_id = ?", notice.OrganizationID).
		Where("kind = ?", notice.Kind).
		Where("dedupe_key = ?", notice.DedupeKey).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: mark a notice posted: %w", err))
	}
	return nil
}
