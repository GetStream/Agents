package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// CallParticipant is a phone's join or leave of a call that no leg was waiting for when it
// was delivered, kept for the leg that opens later (CallLegs.Open) and for the phone's join
// that comes after its leave (CallLegs.Connect). One with no participant session is the call
// hook having delivered a participant event of the call (CallParticipants.RecordHeard).
type CallParticipant struct {
	bun.BaseModel `bun:"table:call_participants,alias:cp"`

	ID int64 `bun:"id,pk,autoincrement"`
	// StreamAppPK and Unpinned are the scope of the hook that delivered it.
	StreamAppPK        int64  `bun:"stream_app_pk,nullzero"`
	Unpinned           bool   `bun:"unpinned,notnull"`
	CallType           string `bun:"call_type,notnull"`
	CallID             string `bun:"call_id,notnull"`
	ParticipantSession string `bun:"participant_session,notnull"`
	// Number is off the phone's participant id, empty when it named none.
	Number     string     `bun:"number,notnull"`
	JoinedAt   *time.Time `bun:"joined_at"`
	LeftAt     *time.Time `bun:"left_at"`
	RecordedAt time.Time  `bun:"recorded_at,notnull"`
}

// Validate refuses a participant no leg could be matched to.
func (p *CallParticipant) Validate() error {
	return validate("call participant").
		required("call_type", p.CallType).
		required("call_id", p.CallID).
		done()
}

// CallParticipants are the phones' joins and leaves kept for legs, on each shard.
type CallParticipants struct {
	store *Store
}

// Record keeps, in this shard, a phone's join (JoinedAt) or leave (LeftAt) of a
// call that no leg was waiting for, in the scope of the hook that delivered it, at now. A
// second event of the same phone fills in what the first left empty: its first join and its
// first leave are what is kept.
func (r *CallParticipants) Record(ctx context.Context, scope AppScope, kept CallParticipant, now time.Time) error {
	if err := kept.Validate(); err != nil {
		return stack.Wrap(err)
	}
	kept.StreamAppPK, kept.Unpinned = scope.App, scope.Unpinned
	kept.Number = strings.TrimPrefix(kept.Number, "+")
	kept.RecordedAt = now.UTC().Truncate(time.Microsecond)
	for _, at := range []**time.Time{&kept.JoinedAt, &kept.LeftAt} {
		if *at != nil {
			truncated := (*at).UTC().Truncate(time.Microsecond)
			*at = &truncated
		}
	}
	_, err := r.store.shard.NewInsert().Model(&kept).
		On("CONFLICT (call_type, call_id, COALESCE(stream_app_pk, 0), participant_session) DO UPDATE").
		Set("joined_at = COALESCE(cp.joined_at, EXCLUDED.joined_at)").
		Set("left_at = COALESCE(cp.left_at, EXCLUDED.left_at)").
		Set("number = CASE WHEN cp.number = '' THEN EXCLUDED.number ELSE cp.number END").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: record call participant: %w", err))
	}
	return nil
}

// RecordHeard records, in this shard, that the call hook delivered a participant event of the
// call, anybody's: a row with no participant session. A hook subscribed to them delivers the
// agent's own join on every placed call, so a placed call ending with none heard is a hook that
// does not deliver them. Only a call with an outbound leg ringing (CallLegs.Ringing) needs it.
func (r *CallParticipants) RecordHeard(ctx context.Context, scope AppScope, callType, callID string, now time.Time) error {
	_, err := r.store.shard.NewInsert().Model(&CallParticipant{
		StreamAppPK: scope.App, Unpinned: scope.Unpinned, CallType: callType, CallID: callID,
		RecordedAt: now.UTC().Truncate(time.Microsecond),
	}).On("CONFLICT DO NOTHING").Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: heard call: %w", err))
	}
	return nil
}

// Heard reports whether the call hook delivered any participant event of the call
// (CallParticipants.RecordHeard), in any shard.
func (r *CallParticipants) Heard(ctx context.Context, scope AppScope, callType, callID string) (bool, error) {
	heard, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (bool, error) {
		return db.NewSelect().Model((*CallParticipant)(nil)).
			Where("call_type = ?", callType).Where("call_id = ?", callID).
			ApplyQueryBuilder(scope.where).Exists(ctx)
	})
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: call heard: %w", err))
	}
	return slices.Contains(heard, true), nil
}

// Left is when a phone left a call, from the leave kept when no leg had it, or nil.
func (r *CallParticipants) Left(ctx context.Context, scope AppScope, callType, callID, participant string) (*time.Time, error) {
	lefts, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]time.Time, error) {
		var left []time.Time
		err := db.NewSelect().Model((*CallParticipant)(nil)).Column("left_at").
			Where("call_type = ?", callType).Where("call_id = ?", callID).
			Where("participant_session = ?", participant).Where("left_at IS NOT NULL").
			ApplyQueryBuilder(scope.where).Scan(ctx, &left)
		return left, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: participant left: %w", err))
	}
	all := slices.Concat(lefts...)
	if len(all) == 0 {
		return nil, nil
	}
	first := slices.MinFunc(all, time.Time.Compare)
	return &first, nil
}

// Forget deletes what was kept of a phone that has its leg now: its join, and its
// leave only when its leg was ended at it (applied) or later. A leave kept after that is left
// for the Left that kept it, which ends the leg itself.
func (r *CallParticipants) Forget(ctx context.Context, scope AppScope, callType, callID, participant string,
	applied *time.Time) error {
	_, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (struct{}, error) {
		_, err := db.NewDelete().Model((*CallParticipant)(nil)).
			Where("call_type = ?", callType).Where("call_id = ?", callID).
			Where("participant_session = ?", participant).ApplyQueryBuilder(scope.where).
			WhereGroup(" AND ", func(q *bun.DeleteQuery) *bun.DeleteQuery {
				q = q.Where("left_at IS NULL")
				if applied != nil {
					q = q.WhereOr("left_at <= ?", applied.UTC())
				}
				return q
			}).
			Exec(ctx)
		return struct{}{}, err
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: forget call participant: %w", err))
	}
	return nil
}

// DeleteBefore deletes, from this shard, at most limit of the participants kept
// before before, and reports how many.
func (r *CallParticipants) DeleteBefore(ctx context.Context, before time.Time, limit int) (int, error) {
	result, err := r.store.shard.NewDelete().Model((*CallParticipant)(nil)).
		Where("id IN (?)", r.store.shard.NewSelect().Model((*CallParticipant)(nil)).Column("id").
			Where("recorded_at < ?", before.UTC()).OrderExpr("recorded_at").Limit(limit)).
		Exec(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: delete call participants: %w", err))
	}
	deleted, err := result.RowsAffected()
	return int(deleted), err
}
