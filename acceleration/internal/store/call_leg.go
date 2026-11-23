package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// What ended a call leg.
const (
	// LegLeft is the leg's phone leaving the call.
	LegLeft = "left"
	// LegEnded is the call ending with the leg still on it.
	LegEnded = "ended"
	// LegSwept is the sweep ending a leg nothing said had ended.
	LegSwept = "swept"
	// LegUnanswered is a leg that ended without its phone ever joining.
	LegUnanswered = "unanswered"
)

// CallLeg is one leg of a phone call, from when its phone joined the call until it left or the
// call ended, which is what its minutes are billed by. Who it is billed to is kept from when
// it opened, so the minutes of a placed call stay with the key that placed it.
type CallLeg struct {
	bun.BaseModel `bun:"table:call_legs,alias:cl"`

	ID               int64    `bun:"id,pk,autoincrement"`
	CustomerID       string   `bun:"customer_id,notnull"`
	OrganizationID   string   `bun:"organization_id,notnull"`
	OrganizationTier string   `bun:"organization_tier,notnull"`
	APIKey           string   `bun:"api_key,notnull"`
	UserID           string   `bun:"user_id,notnull"`
	UsageCategories  []string `bun:"usage_categories,array,nullzero"`
	// StreamAppPK is the Stream app the call is in. Zero, stored as NULL, is the
	// deployment's own app.
	StreamAppPK int64  `bun:"stream_app_pk,nullzero"`
	CallType    string `bun:"call_type,notnull"`
	CallID      string `bun:"call_id,notnull"`
	// StreamSessionID is Stream's session of the call, and ParticipantSession the user session
	// of the leg's phone in it. Empty until known.
	StreamSessionID    string `bun:"stream_session_id,notnull"`
	ParticipantSession string `bun:"participant_session,notnull"`
	Vendor             string `bun:"vendor,notnull"`
	// VendorCallID is the vendor's id for an outbound leg. Empty for an inbound one.
	VendorCallID string `bun:"vendor_call_id,nullzero"`
	// Direction is Inbound or Outbound.
	Direction    string `bun:"direction,notnull"`
	LocalNumber  string `bun:"local_number,notnull"`
	RemoteNumber string `bun:"remote_number,notnull"`
	// Country is what the leg's minutes are priced by: where an outbound leg rang, and the
	// country of the number an inbound one rang. Empty when unknown.
	Country string            `bun:"country,notnull"`
	AgentID string            `bun:"agent_id,notnull"`
	Tags    map[string]string `bun:"tags,type:jsonb,nullzero"`
	// DialedAt is when the vendor took the dial, or the call arrived: zero, stored as NULL, for
	// a placed leg not dialled yet, which no phone, end or sweep finds. StartedAt is when the
	// leg's phone joined, nil while it rings; EndedAt nil while the leg is open; EndReason
	// what ended it (LegLeft and the rest).
	DialedAt  time.Time  `bun:"dialed_at,nullzero"`
	StartedAt *time.Time `bun:"started_at"`
	EndedAt   *time.Time `bun:"ended_at"`
	EndReason string     `bun:"end_reason,notnull"`

	// The rest is kept for a leg the router placed (Place), whose outcome package outcome
	// decides; the store only keeps it. Token is in the URL the vendor posts the call's status
	// to, and the only thing proving the post is about this call. Only its hash is kept
	// (callTokenHash), empty once swept; a leg read back has no Token.
	Token     string `bun:"-"`
	TokenHash string `bun:"token_hash,nullzero"`
	// Reports is whether the vendor posts a final status for the call.
	Reports    bool       `bun:"reports,notnull"`
	PlacedAt   *time.Time `bun:"placed_at"`
	AnsweredAt *time.Time `bun:"answered_at"`
	// OutcomeEndedAt is when the outcome says the call ended, which EndedAt, the leg's billed
	// end, need not be.
	OutcomeEndedAt *time.Time `bun:"outcome_ended_at"`
	// Outcome is empty until decided.
	Outcome      string     `bun:"outcome,nullzero"`
	VendorStatus string     `bun:"vendor_status,nullzero"`
	DecidedAt    *time.Time `bun:"decided_at"`
}

// Validate refuses a leg no call or bill could be found for.
func (l *CallLeg) Validate() error {
	return validate("call leg").
		required("customer_id", l.CustomerID).
		required("call_type", l.CallType).
		required("call_id", l.CallID).
		oneOf("direction", l.Direction, Inbound, Outbound).
		oneOf("end_reason", l.EndReason, "", LegLeft, LegEnded, LegSwept, LegUnanswered).
		oneOf("outcome", l.Outcome, "", "completed", "busy", "no_answer", "failed", "canceled", "unknown").
		check(l.PlacedAt != nil || (l.Token == "" && l.Outcome == ""), "placed_at", "is required for a token or an outcome").
		check(!l.DialedAt.IsZero() || l.PlacedAt != nil, "dialed_at", "is required").
		done()
}

// Connected reports whether the leg's phone ever joined the call, which is what makes it billed.
func (l CallLeg) Connected() bool { return l.StartedAt != nil }

// Used is how long an ended leg was connected, in seconds, a started second counting whole:
// what a vendor bills its minutes from. A leg still open, or never connected, has used nothing.
func (l CallLeg) Used() pricing.Quantities {
	if l.StartedAt == nil || l.EndedAt == nil || !l.EndedAt.After(*l.StartedAt) {
		return pricing.Quantities{pricing.CallSeconds: 0}
	}
	lasted := l.EndedAt.Sub(*l.StartedAt)
	seconds := int64(lasted / time.Second)
	if lasted%time.Second != 0 {
		seconds++
	}
	return pricing.Quantities{pricing.CallSeconds: seconds}
}

// CallLegs are the legs of phone calls, each on the shard that opened it.
type CallLegs struct {
	store *Store
	// stripes are the turns PlaceWithin waits for, an organization's always the same one.
	stripes     [placingStripes]chan struct{}
	stripesOnce sync.Once
}

// Open records a leg of a call in this shard: an outbound one ringing, an inbound one
// started. It reports false, and records nothing, for an inbound leg its Stream session already
// has, ended or not: every delivery of the event announcing a call that arrives opens it, and
// only the first does. A leg with a Token is the one Place kept before the dial, and is dialled
// now; it reports false with ErrNoPlacedCall when that leg is gone or dialled already. A leg
// that opens takes the join of its phone delivered before it (takeParticipant), and leg is
// what was recorded: started, or ended too, by that join.
func (r *CallLegs) Open(ctx context.Context, leg *CallLeg) (bool, error) {
	if leg.DialedAt.IsZero() {
		leg.DialedAt = time.Now()
	}
	// A token only finds the placed leg: Open writes nothing Place kept.
	written := *leg
	written.Token = ""
	if err := written.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	leg.DialedAt = leg.DialedAt.UTC().Truncate(time.Microsecond)
	if leg.StartedAt != nil {
		started := leg.StartedAt.UTC().Truncate(time.Microsecond)
		leg.StartedAt = &started
	}
	if leg.Token != "" {
		var dialed []CallLeg
		err := r.store.shard.NewUpdate().Model(leg).
			ExcludeColumn("id", "token_hash", "reports", "placed_at", "answered_at", "outcome_ended_at", "outcome", "vendor_status", "decided_at").
			Where("token_hash = ?", callTokenHash(leg.Token)).Where("dialed_at IS NULL").
			Returning("*").Scan(ctx, &dialed)
		if err != nil {
			return false, stack.Wrap(fmt.Errorf("store: open placed call leg: %w", err))
		}
		if len(dialed) == 0 {
			return false, stack.Wrap(ErrNoPlacedCall)
		}
		token := leg.Token
		*leg = dialed[0]
		leg.Token = token
	} else {
		result, err := r.store.shard.NewInsert().Model(leg).On("CONFLICT DO NOTHING").Returning("id").Exec(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, stack.Wrap(fmt.Errorf("store: open call leg: %w", err))
		}
		opened, err := result.RowsAffected()
		if err != nil {
			return false, stack.Wrap(fmt.Errorf("store: open call leg: %w", err))
		}
		if opened == 0 {
			return false, nil
		}
	}
	if err := r.takeParticipant(ctx, leg); err != nil {
		return true, stack.Wrap(fmt.Errorf("store: give a call leg the phone that joined before it: %w", err))
	}
	return true, nil
}

// takeParticipant gives a leg that just opened the join of its phone kept from before it
// (CallParticipants.Record): by the leg's remote number, then, for an outbound leg, the number it
// was placed from, then its participant session, then, for a leg that knows none of them, the
// call's first phone to join. The leg starts when that phone joined, or when the leg opened if
// that is later, and ends when the phone left if its leave was kept too.
func (r *CallLegs) takeParticipant(ctx context.Context, leg *CallLeg) error {
	var matches []func(*bun.SelectQuery) *bun.SelectQuery
	byNumber := func(number string) func(*bun.SelectQuery) *bun.SelectQuery {
		return func(q *bun.SelectQuery) *bun.SelectQuery { return q.Where("ltrim(cp.number, '+') = ?", number) }
	}
	remote, local := strings.TrimPrefix(leg.RemoteNumber, "+"), strings.TrimPrefix(leg.LocalNumber, "+")
	if remote != "" {
		matches = append(matches, byNumber(remote))
	}
	if leg.Direction == Outbound && local != "" {
		matches = append(matches, byNumber(local))
	}
	if leg.ParticipantSession != "" {
		matches = append(matches, func(q *bun.SelectQuery) *bun.SelectQuery {
			return q.Where("cp.participant_session = ?", leg.ParticipantSession)
		})
	}
	if len(matches) == 0 {
		matches = append(matches, func(q *bun.SelectQuery) *bun.SelectQuery { return q })
	}
	for _, match := range matches {
		for _, db := range r.store.shards() {
			taken, err := r.takeFrom(ctx, db, leg, match)
			if err != nil || taken {
				return err
			}
		}
	}
	return nil
}

// errLegGone rolls back a take whose leg was given another phone, or ended, meanwhile; and
// errPhoneHasLeg one whose phone was given another leg.
var (
	errLegGone     = errors.New("store: the leg is no longer waiting for its phone")
	errPhoneHasLeg = errors.New("store: the phone has another leg")
)

// takeFrom takes, from one shard, the first kept join match finds for the leg, and gives the
// leg that phone, in one transaction with the delete when the shard is the leg's own. A join
// in another shard is deleted only once the leg has it. It reports true when there is nothing
// more to look for: the leg has a phone, or no longer waits for one.
func (r *CallLegs) takeFrom(ctx context.Context, db bun.IDB, leg *CallLeg, match func(*bun.SelectQuery) *bun.SelectQuery) (bool, error) {
	var joined CallLeg
	take := func(ctx context.Context, from, legs bun.IDB) error {
		kept := match(from.NewSelect().Model((*CallParticipant)(nil)).Column("cp.id").
			Where("cp.call_type = ?", leg.CallType).Where("cp.call_id = ?", leg.CallID).
			Where("cp.joined_at IS NOT NULL").Where("cp.participant_session <> ''").
			// Said again of l, which call_legs_participant_idx is built on: the planner does not
			// carry it over from cp, and without it reads every leg.
			Where("NOT EXISTS (SELECT 1 FROM call_legs AS l WHERE l.call_type = cp.call_type AND l.call_id = cp.call_id " +
				"AND l.participant_session = cp.participant_session AND l.participant_session <> '')"))
		if leg.StreamAppPK != 0 {
			kept = kept.Where("cp.stream_app_pk = ?", leg.StreamAppPK)
		} else {
			kept = kept.Where("cp.unpinned")
		}
		var taken []CallParticipant
		err := from.NewDelete().Model((*CallParticipant)(nil)).
			Where("id IN (?)", kept.OrderExpr("cp.joined_at").Limit(1).For("UPDATE SKIP LOCKED")).
			Returning("*").
			Scan(ctx, &taken)
		if err != nil || len(taken) == 0 {
			return err
		}
		joined, err = joinTaken(ctx, legs, *leg, taken[0])
		return err
	}
	var err error
	if db == bun.IDB(r.store.shard) {
		err = r.store.shard.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error { return take(ctx, tx, tx) })
	} else {
		err = db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error { return take(ctx, tx, r.store.shard) })
	}
	switch {
	case errors.Is(err, errLegGone):
		return true, nil
	case errors.Is(err, errPhoneHasLeg):
		return false, nil
	case err != nil:
		return false, err
	case joined.ID != 0:
		*leg = joined
		return true, nil
	}
	return false, nil
}

// joinTaken starts a leg, which legs holds, from the kept join of its phone, no earlier than the
// leg opened, and ends it if the phone's leave was kept too. A leg given another phone
// meanwhile keeps it, and its start (errLegGone), and so does a phone given another leg
// (errPhoneHasLeg).
func joinTaken(ctx context.Context, legs bun.IDB, leg CallLeg, taken CallParticipant) (CallLeg, error) {
	update := legs.NewUpdate().Model((*CallLeg)(nil)).
		Set("participant_session = ?", taken.ParticipantSession).
		Set("started_at = GREATEST(?::timestamptz, dialed_at)", *taken.JoinedAt)
	if taken.LeftAt != nil {
		update = update.Set("ended_at = GREATEST(?::timestamptz, ?::timestamptz, dialed_at)", *taken.LeftAt, *taken.JoinedAt).
			Set("end_reason = ?", LegLeft)
	}
	var joined []CallLeg
	err := update.Where("id = ?", leg.ID).Where("ended_at IS NULL").
		Where("participant_session IN ('', ?)", taken.ParticipantSession).
		Returning("*").Scan(ctx, &joined)
	if isViolation(err, uniqueViolation) {
		return CallLeg{}, errPhoneHasLeg
	}
	if err != nil {
		return CallLeg{}, err
	}
	if len(joined) == 0 {
		return CallLeg{}, errLegGone
	}
	return joined[0], nil
}

// Connection is what a phone joining a call did to the call's legs.
type Connection int

const (
	// NothingWaiting is no leg of the call waiting for a phone: the join is kept instead
	// (CallParticipants.Record).
	NothingWaiting Connection = iota
	// LegConnected is the phone given its leg, which starts.
	LegConnected
	// LegKnown is the phone already having its leg, from an earlier or concurrent delivery.
	LegKnown
)

// Connect gives a phone that joined a call, at at, the leg it is, no earlier than the leg
// was dialled: the waiting leg whose remote number is the phone's, or when none is, an outbound
// one placed from it, or when neither is or the number is unknown, the call's inbound leg that
// does not know its phone yet, or else the oldest outbound leg still ringing. Stream does not
// order its call events, so a call whose inbound leg opened without its caller has a transfer's
// phone joining it, and only the number tells them apart. A phone whose leave came first, at
// left, has its leg ended then too.
//
// A join delivered again after the call ended, its first delivery refused, finds its leg ended
// unanswered: an outbound leg of its number that ended after the join is revived, connected
// from the join and ended as it was, so it is billed once, then.
func (r *CallLegs) Connect(ctx context.Context, scope AppScope, callType, callID, sessionID, participant, number string,
	at time.Time, left *time.Time) (CallLeg, Connection, error) {
	at = at.UTC().Truncate(time.Microsecond)
	known, err := r.HasParticipant(ctx, scope, callType, callID, participant)
	if err != nil {
		return CallLeg{}, NothingWaiting, stack.Wrap(fmt.Errorf("store: connect call leg: %w", err))
	}
	if known {
		return CallLeg{}, LegKnown, nil
	}
	var byNumber []legMatch
	if number = strings.TrimPrefix(number, "+"); number != "" {
		byNumber = []legMatch{{column: "remote_number", number: number}, {column: "local_number", number: number}}
	}
	passes := append(slices.Clone(byNumber), legMatch{})
	for _, match := range byNumber {
		match.revive = true
		passes = append(passes, match)
	}
	for _, match := range passes {
		leg, connection, err := r.connectWaitingLeg(ctx, scope, callType, callID, sessionID, participant, match, at, left)
		if err != nil || connection != NothingWaiting {
			return leg, connection, err
		}
	}
	return CallLeg{}, NothingWaiting, nil
}

// legMatch is which legs of a call a joining phone may be given: those whose column (remote or,
// outbound only, local number) is number, or with no column any waiting leg; and revive looks
// among the outbound legs that ended unanswered instead.
type legMatch struct {
	column, number string
	revive         bool
}

// connectWaitingLeg gives the phone the first leg of the call, across the shards, that match
// keeps.
func (r *CallLegs) connectWaitingLeg(ctx context.Context, scope AppScope, callType, callID, sessionID, participant string,
	match legMatch, at time.Time, left *time.Time) (CallLeg, Connection, error) {
	var leftAt any
	if left != nil {
		leftAt = left.UTC().Truncate(time.Microsecond)
	}
	for _, db := range r.store.shards() {
		waiting := db.NewSelect().Model((*CallLeg)(nil)).Column("id").
			Where("call_type = ?", callType).Where("call_id = ?", callID).
			Where("participant_session = ''").
			ApplyQueryBuilder(scope.where)
		if match.revive {
			waiting = waiting.Where("direction = ?", Outbound).Where("end_reason = ?", LegUnanswered).
				Where("ended_at > GREATEST(?::timestamptz, dialed_at)", at)
		} else {
			waiting = waiting.Where("ended_at IS NULL").Where("dialed_at IS NOT NULL").
				WhereGroup(" AND ", func(q *bun.SelectQuery) *bun.SelectQuery {
					return q.Where("direction = ?", Inbound).WhereOr("started_at IS NULL")
				})
		}
		switch match.column {
		case "remote_number":
			waiting = waiting.Where("ltrim(remote_number, '+') = ?", match.number)
		case "local_number":
			waiting = waiting.Where("direction = ?", Outbound).Where("ltrim(local_number, '+') = ?", match.number)
		}
		waiting = waiting.OrderExpr("direction = ? DESC, dialed_at", Inbound).Limit(1).For("UPDATE SKIP LOCKED")
		update := db.NewUpdate().Model((*CallLeg)(nil)).
			Set("participant_session = ?", participant).
			Set("stream_session_id = CASE WHEN stream_session_id = '' THEN ? ELSE stream_session_id END", sessionID)
		switch {
		case match.revive:
			update = update.Set("started_at = GREATEST(?::timestamptz, dialed_at)", at).
				Set("ended_at = CASE WHEN ?::timestamptz IS NULL THEN ended_at "+
					"ELSE LEAST(ended_at, GREATEST(?::timestamptz, ?::timestamptz, dialed_at)) END", leftAt, leftAt, at).
				Set("end_reason = CASE WHEN ?::timestamptz IS NULL OR ?::timestamptz >= ended_at THEN ? ELSE ? END",
					leftAt, leftAt, LegEnded, LegLeft)
		case left != nil:
			update = update.Set("started_at = COALESCE(started_at, GREATEST(?::timestamptz, dialed_at))", at).
				Set("ended_at = GREATEST(?::timestamptz, COALESCE(started_at, GREATEST(?::timestamptz, dialed_at)))", leftAt, at).
				Set("end_reason = ?", LegLeft)
		default:
			update = update.Set("started_at = COALESCE(started_at, GREATEST(?::timestamptz, dialed_at))", at)
		}
		var connected []CallLeg
		err := update.Where("id IN (?)", waiting).Returning("*").Scan(ctx, &connected)
		if isViolation(err, uniqueViolation) {
			return CallLeg{}, LegKnown, nil
		}
		if err != nil {
			return CallLeg{}, NothingWaiting, stack.Wrap(fmt.Errorf("store: connect call leg: %w", err))
		}
		if len(connected) > 0 {
			return connected[0], LegConnected, nil
		}
	}
	return CallLeg{}, NothingWaiting, nil
}

// Ringing reports whether the call has an outbound leg, in any shard, still ringing: the
// calls CallParticipants.RecordHeard is worth recording for.
func (r *CallLegs) Ringing(ctx context.Context, scope AppScope, callType, callID string) (bool, error) {
	ringing, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (bool, error) {
		return db.NewSelect().Model((*CallLeg)(nil)).
			Where("call_type = ?", callType).Where("call_id = ?", callID).
			Where("ended_at IS NULL").Where("started_at IS NULL").Where("dialed_at IS NOT NULL").Where("direction = ?", Outbound).
			ApplyQueryBuilder(scope.where).Exists(ctx)
	})
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: call ringing: %w", err))
	}
	return slices.Contains(ringing, true), nil
}

// HasParticipant reports whether a leg of the call, open or ended, is the phone's.
func (r *CallLegs) HasParticipant(ctx context.Context, scope AppScope, callType, callID, participant string) (bool, error) {
	known, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (bool, error) {
		return db.NewSelect().Model((*CallLeg)(nil)).
			Where("call_type = ?", callType).Where("call_id = ?", callID).
			Where("participant_session = ?", participant).ApplyQueryBuilder(scope.where).Exists(ctx)
	})
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: participant has leg: %w", err))
	}
	return slices.Contains(known, true), nil
}

// EndParticipant ends the open leg of a phone that left a call, at at, and returns it.
func (r *CallLegs) EndParticipant(ctx context.Context, scope AppScope, callType, callID, participant string,
	at time.Time) ([]CallLeg, error) {
	return r.endLegs(ctx, func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return q.Where("call_type = ?", callType).Where("call_id = ?", callID).
			Where("participant_session = ?", participant).ApplyQueryBuilder(scope.where)
	}, LegLeft, at)
}

// EndCall ends the legs of one call still open in one Stream app, at at, and returns them:
// one whose phone joined is ended, and one still ringing is unanswered. A call's id is only
// unique within its app, and its legs may have been opened from any shard. A leg already
// ended is not ended again, so a repeated event ends nothing.
func (r *CallLegs) EndCall(ctx context.Context, scope AppScope, callType, callID string, at time.Time) ([]CallLeg, error) {
	return r.endLegs(ctx, func(q *bun.UpdateQuery) *bun.UpdateQuery {
		return q.Where("call_type = ?", callType).Where("call_id = ?", callID).ApplyQueryBuilder(scope.where)
	}, LegEnded, at)
}

// endLegs ends the open legs where narrows to, in every shard, at at or when each began if
// that is later, for reason, or as unanswered when the leg's phone never joined.
func (r *CallLegs) endLegs(ctx context.Context, where func(*bun.UpdateQuery) *bun.UpdateQuery, reason string,
	at time.Time) ([]CallLeg, error) {
	at = at.UTC().Truncate(time.Microsecond)
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]CallLeg, error) {
		var ended []CallLeg
		err := where(db.NewUpdate().Model((*CallLeg)(nil)).
			Set("ended_at = GREATEST(?, COALESCE(started_at, dialed_at))", at).
			Set("end_reason = CASE WHEN started_at IS NULL THEN ? ELSE ? END", LegUnanswered, reason).
			Where("ended_at IS NULL").Where("dialed_at IS NOT NULL")).
			Returning("*").
			Scan(ctx, &ended)
		return ended, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: end call legs: %w", err))
	}
	return slices.Concat(lists...), nil
}

// CountOpen is how many legs of an organization's calls are up or ringing now, in every
// shard: open, with their phone joined, or dialed or placed since ringingSince, so a leg whose
// answer was never heard drops out once it could no longer be ringing. inCall is how many of
// those are legs of the call callType:callID, none when it is empty. A leg with no organization
// is never counted.
func (r *CallLegs) CountOpen(ctx context.Context, organizationID string, ringingSince time.Time, callType, callID string) (total, inCall int, err error) {
	if organizationID == "" {
		return 0, 0, nil
	}
	counts, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (openCount, error) {
		return countOpen(ctx, db, organizationID, ringingSince, callType, callID)
	})
	if err != nil {
		return 0, 0, stack.Wrap(fmt.Errorf("store: open call legs: %w", err))
	}
	for _, count := range counts {
		total += count.Total
		if callID != "" {
			inCall += count.InCall
		}
	}
	return total, inCall, nil
}

// openCount is what countOpen counts in one shard.
type openCount struct {
	Total  int `bun:"total"`
	InCall int `bun:"in_call"`
}

// countOpen is CountOpen in the shard db.
func countOpen(ctx context.Context, db bun.IDB, organizationID string, ringingSince time.Time, callType, callID string) (openCount, error) {
	var found openCount
	err := db.NewSelect().Model((*CallLeg)(nil)).
		ColumnExpr("count(*) AS total").
		ColumnExpr("count(*) FILTER (WHERE call_type = ? AND call_id = ?) AS in_call", callType, callID).
		Where("organization_id = ?", organizationID).Where("ended_at IS NULL").
		WhereGroup(" AND ", func(q *bun.SelectQuery) *bun.SelectQuery {
			return q.Where("started_at IS NOT NULL").WhereOr("dialed_at > ?", ringingSince.UTC()).
				WhereOr("placed_at > ?", ringingSince.UTC())
		}).
		Scan(ctx, &found)
	return found, err
}

// Sweep ends the legs of this shard open longer than longest at now: one whose phone
// joined before now less longest is ended at its start plus longest, as swept, and one that has
// rung since before then as unanswered. At most limit of each are ended at once.
func (r *CallLegs) Sweep(ctx context.Context, now time.Time, longest time.Duration, limit int) ([]CallLeg, error) {
	since := now.UTC().Add(-longest)
	var swept, unanswered []CallLeg
	err := r.store.shard.NewUpdate().Model((*CallLeg)(nil)).
		Set("ended_at = started_at + make_interval(secs => ?)", longest.Seconds()).
		Set("end_reason = ?", LegSwept).
		Where("id IN (?)", r.store.shard.NewSelect().Model((*CallLeg)(nil)).Column("id").
			Where("ended_at IS NULL").Where("started_at < ?", since).OrderExpr("started_at").Limit(limit)).
		Where("ended_at IS NULL").
		Returning("*").
		Scan(ctx, &swept)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: sweep call legs: %w", err))
	}
	err = r.store.shard.NewUpdate().Model((*CallLeg)(nil)).
		Set("ended_at = ?", now.UTC().Truncate(time.Microsecond)).
		Set("end_reason = ?", LegUnanswered).
		Where("id IN (?)", r.store.shard.NewSelect().Model((*CallLeg)(nil)).Column("id").
			Where("ended_at IS NULL").Where("started_at IS NULL").Where("dialed_at < ?", since).
			OrderExpr("dialed_at").Limit(limit)).
		Where("ended_at IS NULL").
		Returning("*").
		Scan(ctx, &unanswered)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: sweep call legs: %w", err))
	}
	return append(swept, unanswered...), nil
}

// SelectEnded is the legs of this shard whose phone joined and that ended in [from, to),
// oldest first, at most limit: what the sweep checks were billed.
func (r *CallLegs) SelectEnded(ctx context.Context, from, to time.Time, limit int) ([]CallLeg, error) {
	var ended []CallLeg
	err := r.store.shard.NewSelect().Model(&ended).
		Where("started_at IS NOT NULL").Where("ended_at >= ?", from.UTC()).Where("ended_at < ?", to.UTC()).
		OrderExpr("ended_at").Limit(limit).Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: ended call legs: %w", err))
	}
	return ended, nil
}

// Placed reports whether customerID placed the outbound leg vendor knows as
// vendorCallID, a call or a transfer, on whichever shard holds it.
func (r *CallLegs) Placed(ctx context.Context, customerID, vendor, vendorCallID string) (bool, error) {
	_, err := whicheverShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (int64, error) {
		var id int64
		err := db.NewSelect().Model((*CallLeg)(nil)).Column("cl.id").
			Where("cl.vendor_call_id = ?", vendorCallID).
			Where("cl.customer_id = ?", customerID).
			Where("cl.vendor = ?", vendor).
			Limit(1).
			Scan(ctx, &id)
		return id, err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: placed call leg: %w", err))
	}
	return true, nil
}

// MarkRefused marks the Stream session session of the customer's arriving call in app as one
// the router refused, for ttl, so its leg is counted failed when it ends, on whichever node ends
// it. The mark is in the shared Redis because that node may be another shard's. A store with
// no Redis marks nothing.
func (r *CallLegs) MarkRefused(ctx context.Context, customer string, app int64, callType, callID, session string, ttl time.Duration) error {
	if r.store.cache == nil {
		return nil
	}
	client := r.store.cache.Client()
	command := client.B().Set().Key(refusedKey(customer, app, callType, callID, session)).Value("1").Ex(ttl).Build()
	if err := client.Do(ctx, command).Error(); err != nil {
		return stack.Wrap(fmt.Errorf("store: mark a refused call: %w", err))
	}
	return nil
}

// Refused reports whether the call of an inbound leg was marked MarkRefused. A store with no
// Redis has marked none.
func (r *CallLegs) Refused(ctx context.Context, leg CallLeg) (bool, error) {
	if r.store.cache == nil {
		return false, nil
	}
	client := r.store.cache.Client()
	command := client.B().Exists().Key(refusedKey(leg.CustomerID, leg.StreamAppPK, leg.CallType, leg.CallID, leg.StreamSessionID)).Build()
	found, err := client.Do(ctx, command).AsInt64()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: read a refused call: %w", err))
	}
	return found > 0, nil
}

// refusedKey names the mark of a refused call. A call's id is unique only within its app, and
// a number attached before per-caller calls puts every caller in the number's own call, each
// caller a Stream session of it with a leg of its own: only the refused caller's leg is marked.
func refusedKey(customer string, app int64, callType, callID, session string) string {
	return refusedPrefix + customer + ":" + strconv.FormatInt(app, 10) + ":" + callType + ":" + callID + ":" + session
}

// refusedPrefix starts every refused call's mark.
const refusedPrefix = "phone:refused:"

// ErrNoPlacedCall is what a placed call no shard holds reads as.
var ErrNoPlacedCall = errors.New("store: no such placed call")

// PlacedWhere finds one placed leg: by Token when it is set, else by the rest. The newest
// match is the one.
type PlacedWhere struct {
	Token        string
	CustomerID   string
	StreamAppPK  int64
	CallType     string
	CallID       string
	LocalNumber  string
	RemoteNumber string
}

func (w PlacedWhere) on(q *bun.SelectQuery) *bun.SelectQuery {
	if w.Token != "" {
		return q.Where("cl.token_hash = ?", callTokenHash(w.Token))
	}
	return q.Where("cl.customer_id = ?", w.CustomerID).
		Where("cl.call_id = ?", w.CallID).
		Where("cl.placed_at IS NOT NULL").
		Where("COALESCE(cl.stream_app_pk, 0) = ?", w.StreamAppPK).
		Where("cl.call_type = ?", w.CallType).
		Where("cl.local_number = ?", w.LocalNumber).
		Where("cl.remote_number = ?", w.RemoteNumber)
}

// Place keeps the outbound leg of a call this shard is about to dial, with its Token and
// PlacedAt, so the vendor's posts about it find it from the moment it is asked. Open dials it.
func (r *CallLegs) Place(ctx context.Context, leg *CallLeg) error {
	if err := placeable(leg); err != nil {
		return err
	}
	if _, err := r.store.shard.NewInsert().Model(leg).Returning("id").Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: place call leg: %w", err))
	}
	return nil
}

// placeable refuses a leg Place cannot keep, and gives it the hash of its Token.
func placeable(leg *CallLeg) error {
	if err := leg.Validate(); err != nil {
		return stack.Wrap(err)
	}
	invalid := validate("call leg").
		required("token", leg.Token).
		check(leg.PlacedAt != nil, "placed_at", "is required").
		check(leg.Direction == Outbound, "direction", "must be outbound").
		check(leg.DialedAt.IsZero(), "dialed_at", "must be empty").
		done()
	if invalid != nil {
		return stack.Wrap(invalid)
	}
	leg.TokenHash = callTokenHash(leg.Token)
	return nil
}

// ErrCallsAtOnce is a leg PlaceWithin did not keep: its organization already has as many calls
// up or ringing as it may have at once.
var ErrCallsAtOnce = errors.New("store: the organization has as many calls up as it may have at once")

// CallsAtOnce is how many of an organization's calls may be up or ringing at once: Limit legs
// as CountOpen counts them, a leg rung or placed less than Ringing before the one being placed
// counting as ringing.
type CallsAtOnce struct {
	Limit   int
	Ringing time.Duration
}

// callsAtOnceLockSeed seeds the advisory lock an organization's calls are counted and placed
// under, one at a time.
const callsAtOnceLockSeed = 1907

// placingStripes is how many organizations a router counts and places calls for at once.
const placingStripes = 64

// PlaceWithin is Place for a leg of leg.OrganizationID that only its calls at once leave room
// for: it counts the organization's open legs in every shard and keeps leg, or refuses it with
// ErrCallsAtOnce, as one step. The step holds an advisory lock on the organization in the shared
// database until the leg is kept, so two calls placed at once, from any router, never both see
// the last free one.
func (r *CallLegs) PlaceWithin(ctx context.Context, leg *CallLeg, atOnce CallsAtOnce) error {
	if err := placeable(leg); err != nil {
		return err
	}
	if leg.OrganizationID == "" {
		return stack.Wrap(validate("call leg").required("organization_id", leg.OrganizationID).done())
	}
	// A campaign tick places many calls of one organization at once, and each waiting on the
	// advisory lock holds a connection of the shared database's pool; waiting here first keeps a
	// router to one per organization, so a burst cannot take the pool every request needs.
	release, err := r.placing(ctx, leg.OrganizationID)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: place call leg: %w", err))
	}
	defer release()
	err = r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockUntilCommit(ctx, tx, leg.OrganizationID, callsAtOnceLockSeed); err != nil {
			return err
		}
		// This shard's count and insert run in the lock's own transaction when the shard is the
		// shared database, so the step never waits on a second connection of the pool it holds one of.
		var own bun.IDB = r.store.shard
		if r.store.shard == r.store.global {
			own = tx
		}
		dbs := append([]bun.IDB{own}, r.store.shards()[1:]...)
		counts := make([]openCount, len(dbs))
		failed := make([]error, len(dbs))
		since := leg.PlacedAt.Add(-atOnce.Ringing)
		var wg sync.WaitGroup
		for i, db := range dbs {
			wg.Go(func() { counts[i], failed[i] = countOpen(ctx, db, leg.OrganizationID, since, "", "") })
		}
		wg.Wait()
		if err := errors.Join(failed...); err != nil {
			return err
		}
		open := 0
		for _, count := range counts {
			open += count.Total
		}
		if open >= atOnce.Limit {
			return ErrCallsAtOnce
		}
		_, err := own.NewInsert().Model(leg).Returning("id").Exec(ctx)
		return err
	})
	if errors.Is(err, ErrCallsAtOnce) {
		return stack.Wrap(ErrCallsAtOnce)
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: place call leg: %w", err))
	}
	return nil
}

// placing waits for this router's turn to count and place a call of organization, until ctx
// ends, and returns what gives the turn up.
func (r *CallLegs) placing(ctx context.Context, organization string) (func(), error) {
	r.stripesOnce.Do(func() {
		for i := range r.stripes {
			r.stripes[i] = make(chan struct{}, 1)
		}
	})
	h := fnv.New32a()
	_, _ = h.Write([]byte(organization)) // a hash.Hash never fails to write
	stripe := r.stripes[h.Sum32()%placingStripes]
	select {
	case stripe <- struct{}{}:
		return func() { <-stripe }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Unplace deletes the placed leg with token, from whichever shard holds it, while it is not
// dialled: one that never rang has no outcome and no minutes.
func (r *CallLegs) Unplace(ctx context.Context, token string) error {
	_, err := onEveryShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (sql.Result, error) {
		return db.NewDelete().Model((*CallLeg)(nil)).Where("token_hash = ?", callTokenHash(token)).Where("dialed_at IS NULL").Exec(ctx)
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: unplace call leg: %w", err))
	}
	return nil
}

// ChangeOutcome locks the placed leg where finds, in whichever shard holds it, hands it to
// change, and writes back the outcome change left: VendorCallID, AnsweredAt, OutcomeEndedAt,
// Outcome, VendorStatus and DecidedAt. change returning false writes nothing. It returns the
// leg as it now stands, or ErrNoPlacedCall.
//
// The lock is what makes concurrent changes safe: a vendor's post and the call's own events
// can arrive at once, and each must see what the other wrote.
func (r *CallLegs) ChangeOutcome(ctx context.Context, where PlacedWhere, change func(*CallLeg) bool) (CallLeg, error) {
	// A vendor's post lands on whichever shard is nearest it, not the one that placed the call.
	for _, db := range r.store.shards() {
		var leg CallLeg
		err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			err := where.on(tx.NewSelect().Model(&leg)).OrderExpr("cl.id DESC").Limit(1).For("UPDATE").Scan(ctx)
			if err != nil {
				return err
			}
			changed := leg
			if !change(&changed) {
				return nil
			}
			if err := changed.Validate(); err != nil {
				return err
			}
			_, err = tx.NewUpdate().Model(&changed).
				Column("vendor_call_id", "answered_at", "outcome_ended_at", "outcome", "vendor_status", "decided_at").
				WherePK().Exec(ctx)
			if err != nil {
				return err
			}
			leg.VendorCallID, leg.AnsweredAt, leg.OutcomeEndedAt = changed.VendorCallID, changed.AnsweredAt, changed.OutcomeEndedAt
			leg.Outcome, leg.VendorStatus, leg.DecidedAt = changed.Outcome, changed.VendorStatus, changed.DecidedAt
			return nil
		})
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		var invalid *ValidationError
		if errors.As(err, &invalid) {
			return CallLeg{}, stack.Wrap(err)
		}
		if err != nil {
			return CallLeg{}, stack.Wrap(fmt.Errorf("store: change placed call leg: %w", err))
		}
		return leg, nil
	}
	return CallLeg{}, ErrNoPlacedCall
}

// GetByToken reads the placed leg token was made for, from whichever shard holds it, without
// locking it. It returns ErrNoPlacedCall when none does.
func (r *CallLegs) GetByToken(ctx context.Context, token string) (CallLeg, error) {
	leg, err := whicheverShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (CallLeg, error) {
		var leg CallLeg
		err := PlacedWhere{Token: token}.on(db.NewSelect().Model(&leg)).Limit(1).Scan(ctx)
		return leg, err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return CallLeg{}, ErrNoPlacedCall
	}
	if err != nil {
		return CallLeg{}, stack.Wrap(fmt.Errorf("store: placed call leg by token: %w", err))
	}
	return leg, nil
}

// SelectPlaced returns the customer's newest placed leg for each of callIDs, from every shard.
// A call id with none is left out.
func (r *CallLegs) SelectPlaced(ctx context.Context, customerID string, callIDs []string) ([]CallLeg, error) {
	if len(callIDs) == 0 {
		return nil, nil
	}
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]CallLeg, error) {
		var legs []CallLeg
		err := db.NewSelect().Model(&legs).
			DistinctOn("cl.call_id").
			Where("cl.customer_id = ?", customerID).
			Where("cl.call_id IN (?)", bun.In(callIDs)).
			Where("cl.placed_at IS NOT NULL").
			OrderExpr("cl.call_id, cl.id DESC").
			Scan(ctx)
		return legs, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: placed call legs: %w", err))
	}
	newest := map[string]CallLeg{}
	for _, leg := range slices.Concat(lists...) {
		if kept, ok := newest[leg.CallID]; !ok || leg.PlacedAt.After(*kept.PlacedAt) {
			newest[leg.CallID] = leg
		}
	}
	return slices.Collect(maps.Values(newest)), nil
}

// ClearTokens clears the token of every leg this shard placed before placedBefore, so a
// vendor's post for it is refused, and keeps the rows. Each shard clears its own.
func (r *CallLegs) ClearTokens(ctx context.Context, placedBefore time.Time) (int64, error) {
	result, err := r.store.shard.NewUpdate().Model((*CallLeg)(nil)).
		Set("token_hash = NULL").
		Where("token_hash IS NOT NULL").
		Where("placed_at < ?", placedBefore).
		Exec(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: clear placed call tokens: %w", err))
	}
	cleared, err := result.RowsAffected()
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: clear placed call tokens: %w", err))
	}
	return cleared, nil
}
