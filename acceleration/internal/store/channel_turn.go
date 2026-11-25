package store

import (
	"context"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ChannelTurn is the lease on a channel's one running turn: an app chat's channel of a type an
// agent config answers in, or a thread channel the channel bridge writes into. Two routers
// answering one channel at once would talk over each other, and each one's session would miss
// what the other said.
type ChannelTurn struct {
	bun.BaseModel `bun:"table:channel_turns,alias:cht"`

	// StreamAppPK and CID are the channel: a cid is unique only within its Stream app. 0 is the
	// deployment's own.
	StreamAppPK int64     `bun:"stream_app_pk,pk"`
	CID         string    `bun:"cid,pk"`
	LeasedBy    string    `bun:"leased_by,notnull"`
	LeaseUntil  time.Time `bun:"lease_until,notnull"`
	// Lease is how long Take leases the turn for, counted on Postgres's clock: every router
	// judges the lease by now() there, so a router whose clock is off neither ends nor stretches it.
	Lease time.Duration `bun:"-"`
}

// Validate refuses a lease no one holds.
func (t *ChannelTurn) Validate() error {
	return validate("channel turn").
		required("cid", t.CID).
		required("leased_by", t.LeasedBy).
		check(t.Lease > 0, "lease", "must be positive").
		done()
}

// ChannelTurns are the leases on channels' running turns, in the shared database.
type ChannelTurns struct {
	store *Store
}

// Take leases the channel's turn to turn.LeasedBy for turn.Lease, when no turn holds it
// or the one that did ran out. False is a turn another holder has. One statement, so two
// routers asking at once get one lease between them.
func (r *ChannelTurns) Take(ctx context.Context, turn ChannelTurn) (bool, error) {
	if err := turn.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	result, err := r.store.global.NewInsert().Model(&turn).
		Value("lease_until", "now() + make_interval(secs => ?)", turn.Lease.Seconds()).
		On("CONFLICT (stream_app_pk, cid) DO UPDATE").
		Set("leased_by = EXCLUDED.leased_by").
		Set("lease_until = EXCLUDED.lease_until").
		Where("cht.lease_until < now() OR cht.leased_by = EXCLUDED.leased_by").
		Exec(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: take channel turn: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: take channel turn: %w", err))
	}
	return affected == 1, nil
}

// Release ends holder's lease on a channel's turn, so the next turn need not wait for it to
// run out. A lease another holder took since is left alone.
func (r *ChannelTurns) Release(ctx context.Context, app int64, cid, holder string) error {
	_, err := r.store.global.NewDelete().Model((*ChannelTurn)(nil)).
		Where("stream_app_pk = ? AND cid = ? AND leased_by = ?", app, cid, holder).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: release channel turn: %w", err))
	}
	return nil
}
