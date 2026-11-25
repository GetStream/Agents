package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// The kinds of branch switch.
const (
	// SwitchRegenerate answers a question again.
	SwitchRegenerate = "regenerate"
	// SwitchEdit asks a question again in other words.
	SwitchEdit = "edit"
	// SwitchActivate brings back a branch a switch hid.
	SwitchActivate = "activate"
)

// How far a branch switch got, in the order it gets there.
const (
	// SwitchSnapshotted has copied the messages the rewind will hide.
	SwitchSnapshotted = "snapshotted"
	// SwitchRewound has rewound the channel.
	SwitchRewound = "rewound"
	// SwitchResending is sending the chosen branch's messages again.
	SwitchResending = "resending"
	// SwitchDone has nothing left to do.
	SwitchDone = "done"
)

// switchStates are a switch's states in the order it passes them.
var switchStates = []string{SwitchSnapshotted, SwitchRewound, SwitchResending, SwitchDone}

// AgentBranchSwitch is one move of a conversation to another branch, kept until it is done
// so a router that stopped partway finishes it.
type AgentBranchSwitch struct {
	bun.BaseModel `bun:"table:agent_branch_switches,alias:abs"`

	ID               string `bun:"id,pk"`
	SessionID        string `bun:"session_id,notnull"`
	CustomerID       string `bun:"customer_id,notnull"`
	Kind             string `bun:"kind,notnull"`
	TargetResponseID string `bun:"target_response_id,notnull"`
	// ForkMessageID is the Stream Chat message the channel is rewound to, and Inclusive says
	// whether it is hidden too.
	ForkMessageID string `bun:"fork_message_id,notnull"`
	Inclusive     bool   `bun:"inclusive,notnull"`
	// HiddenFrom is when ForkMessageID was created: with CreatedAt, the range of messages the
	// rewind hid.
	HiddenFrom time.Time `bun:"hidden_from,notnull"`
	State      string    `bun:"state,notnull"`
	// Resent is how many messages of the chosen branch were sent again.
	Resent    int       `bun:"resent,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

// Validate mirrors the table's checks. An empty state is the snapshotted one Insert fills in.
func (b *AgentBranchSwitch) Validate() error {
	return validate("branch switch").
		required("id", b.ID).
		required("session_id", b.SessionID).
		required("customer_id", b.CustomerID).
		oneOf("kind", b.Kind, SwitchRegenerate, SwitchEdit, SwitchActivate).
		oneOf("state", b.State, "", SwitchSnapshotted, SwitchRewound, SwitchResending, SwitchDone).
		nonNegative("resent", float64(b.Resent)).
		done()
}

// BranchSwitches are the moves of conversations between branches, in the shard of the
// session.
type BranchSwitches struct {
	store *Store
}

// Insert records a switch about to begin. Starting the same switch again is not an error: a
// request retried after the first attempt wrote its row finds it there.
func (r *BranchSwitches) Insert(ctx context.Context, branchSwitch *AgentBranchSwitch) error {
	if err := branchSwitch.Validate(); err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC()
	if branchSwitch.CreatedAt.IsZero() {
		branchSwitch.CreatedAt = now
	}
	branchSwitch.UpdatedAt = now
	if branchSwitch.State == "" {
		branchSwitch.State = SwitchSnapshotted
	}
	// As Responses.Insert: a retry matches its own row, another customer's id none.
	result, err := r.store.shard.NewInsert().Model(branchSwitch).
		On("CONFLICT (id) DO UPDATE").
		Set("id = EXCLUDED.id").
		Where("abs.customer_id = EXCLUDED.customer_id").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: start branch switch: %w", err))
	}
	written, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: start branch switch: %w", err))
	}
	if written == 0 {
		return stack.Wrap(fmt.Errorf("%w: branch switch %s", ErrIDTaken, branchSwitch.ID))
	}
	return nil
}

// Advance moves a switch on to state. A switch already there or past it stays where it is,
// so a router finishing a switch another router already moved on never moves it back.
func (r *BranchSwitches) Advance(ctx context.Context, customerID, id, state string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a switch id are required"))
	}
	if !slices.Contains(switchStates, state) {
		return stack.Wrap(fmt.Errorf("store: advance branch switch: unknown state %q", state))
	}
	_, err := r.store.shard.NewUpdate().Model((*AgentBranchSwitch)(nil)).
		Set("state = ?", state).
		Set("updated_at = ?", time.Now().UTC()).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("array_position(?::text[], state) < array_position(?::text[], ?)",
			pgdialect.Array(switchStates), pgdialect.Array(switchStates), state).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: advance branch switch: %w", err))
	}
	return nil
}

// Abandon forgets a switch whose rewind Chat refused: its row and the copies it made, which
// are of messages still visible. A message an earlier switch hid is in no later switch's
// copies, so none of those go with it.
func (r *BranchSwitches) Abandon(ctx context.Context, customerID, sessionID, id string) error {
	if customerID == "" || sessionID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer, a session and a switch id are required"))
	}
	_, err := r.store.shard.NewDelete().Model((*AgentBranchMessage)(nil)).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("switch_id = ?", id).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: abandon branch switch: copies: %w", err))
	}
	_, err = r.store.shard.NewDelete().Model((*AgentBranchSwitch)(nil)).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: abandon branch switch: %w", err))
	}
	return nil
}

// CountResent counts one more message of a switch's branch sent again, and answers how many
// have been.
func (r *BranchSwitches) CountResent(ctx context.Context, customerID, id string) (int, error) {
	if customerID == "" || id == "" {
		return 0, stack.Wrap(errors.New("store: a customer and a switch id are required"))
	}
	var resent int
	err := r.store.shard.NewUpdate().Model((*AgentBranchSwitch)(nil)).
		Set("resent = resent + 1").
		Set("updated_at = ?", time.Now().UTC()).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Returning("resent").
		Scan(ctx, &resent)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, stack.Wrap(fmt.Errorf("store: count branch resent: no switch %s", id))
	}
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: count branch resent: %w", err))
	}
	return resent, nil
}

// Unfinished returns a session's switches that are not done, oldest first.
func (r *BranchSwitches) Unfinished(ctx context.Context, customerID, sessionID string) ([]AgentBranchSwitch, error) {
	if customerID == "" || sessionID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}
	var switches []AgentBranchSwitch
	err := r.store.shard.NewSelect().Model((*AgentBranchSwitch)(nil)).
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("state <> ?", SwitchDone).
		Order("created_at ASC", "id ASC").
		Scan(ctx, &switches)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: unfinished branch switches: %w", err))
	}
	return switches, nil
}

// MergedRangeCount is how many hidden ranges Stream Chat keeps for a session's channel after
// the switches that rewound it, merged the way Chat merges them, which is what Chat caps. A
// switch counts once the channel was rewound, from the rewound state on. An activation that
// copied nothing hid nothing, as one at the end of the conversation does, and is no range.
func (r *BranchSwitches) MergedRangeCount(ctx context.Context, customerID, sessionID string) (int, error) {
	if customerID == "" || sessionID == "" {
		return 0, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}
	var switches []AgentBranchSwitch
	err := r.store.shard.NewSelect().Model((*AgentBranchSwitch)(nil)).
		Column("id", "kind", "hidden_from", "inclusive", "created_at").
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("state <> ?", SwitchSnapshotted).
		Order("created_at ASC", "id ASC").
		Scan(ctx, &switches)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: merged range count: %w", err))
	}
	copied, err := r.copyingActivations(ctx, customerID, sessionID, switches)
	if err != nil {
		return 0, err
	}
	ranges := make([]HiddenRange, 0, len(switches))
	for _, b := range switches {
		if b.Kind == SwitchActivate && !copied[b.ID] {
			continue
		}
		ranges = append(ranges, HiddenRange{From: b.HiddenFrom, To: b.CreatedAt, FromInclusive: b.Inclusive})
	}
	return len(MergeHiddenRanges(ranges)), nil
}

// copyingActivations are the activations among switches that copied a message.
func (r *BranchSwitches) copyingActivations(ctx context.Context, customerID, sessionID string, switches []AgentBranchSwitch) (map[string]bool, error) {
	var activations []string
	for _, b := range switches {
		if b.Kind == SwitchActivate {
			activations = append(activations, b.ID)
		}
	}
	if len(activations) == 0 {
		return nil, nil
	}
	var ids []string
	err := r.store.shard.NewSelect().Model((*AgentBranchMessage)(nil)).
		Distinct().
		Column("switch_id").
		Where("customer_id = ?", customerID).
		Where("session_id = ?", sessionID).
		Where("switch_id IN (?)", bun.In(activations)).
		Scan(ctx, &ids)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: merged range count: copies: %w", err))
	}
	copied := map[string]bool{}
	for _, id := range ids {
		copied[id] = true
	}
	return copied, nil
}

// HiddenRange is the created_at range of the messages one rewind hid. To is inclusive; From
// is inclusive only when the message rewound to was hidden too.
type HiddenRange struct {
	From          time.Time
	To            time.Time
	FromInclusive bool
}

// MergeHiddenRanges sorts ranges by From and merges the ones that overlap or touch, as
// Stream Chat's rewindstate.Merge does: To is inclusive, so a range starting where another
// ends leaves no gap whatever its own lower bound.
func MergeHiddenRanges(ranges []HiddenRange) []HiddenRange {
	sorted := slices.Clone(ranges)
	slices.SortStableFunc(sorted, func(a, b HiddenRange) int {
		if c := a.From.Compare(b.From); c != 0 {
			return c
		}
		// Inclusive first, so a merged range keeps the inclusive lower bound.
		switch {
		case a.FromInclusive == b.FromInclusive:
			return 0
		case a.FromInclusive:
			return -1
		default:
			return 1
		}
	})
	out := make([]HiddenRange, 0, len(sorted))
	for _, r := range sorted {
		if n := len(out); n > 0 && !r.From.After(out[n-1].To) {
			last := &out[n-1]
			if r.From.Equal(last.From) {
				last.FromInclusive = last.FromInclusive || r.FromInclusive
			}
			if r.To.After(last.To) {
				last.To = r.To
			}
			continue
		}
		out = append(out, r)
	}
	return out
}
