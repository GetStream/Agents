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

// Insert stores an end user this app was seen acting for.
func (r *Users) Insert(ctx context.Context, user *User) error {
	if user.ID == "" {
		return stack.Wrap(errors.New("store: a user id is required"))
	}
	if user.CustomerID == "" {
		return stack.Wrap(errors.New("store: customer id is required"))
	}
	if user.Kind == "" {
		return stack.Wrap(errors.New("store: a user kind is required"))
	}
	if user.CreatedAt.IsZero() {
		user.CreatedAt = time.Now().UTC()
	}
	if user.Custom == nil {
		user.Custom = map[string]any{}
	}

	// Recording a user is get-or-create, so the same id arriving again is the same person
	// coming back rather than a conflict. A row already here is left alone: a guest who
	// signed up is claimed rather than overwritten, and overwriting would lose the kind
	// the claim path reads.
	_, err := r.store.global.NewInsert().Model(user).
		On("CONFLICT (customer_id, id) DO NOTHING").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: record user: %w", err))
	}
	return nil
}

// RecordAccount stores a user whose own token said they are not a guest. A guest row the
// app's own guest token wrote for the id becomes an account's: kept a guest's, the account
// would stay one a guest token is minted for (createGuestUser). A guest the router minted
// stays a guest, because the token minted for them names no role and so reads as a full
// user's too; so does a claimed one, whose claim stands.
func (r *Users) RecordAccount(ctx context.Context, customerID, userID string) error {
	user := &User{
		CustomerID: customerID, ID: userID, Kind: UserKindAuthenticated,
		Custom: map[string]any{}, CreatedAt: time.Now().UTC(),
	}
	_, err := r.store.global.NewInsert().Model(user).
		On("CONFLICT (customer_id, id) DO UPDATE").
		Set("kind = EXCLUDED.kind").
		Where("u.kind = ?", UserKindGuest).
		Where("u.claimed_by IS NULL").
		Where("NOT u.minted").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: record account: %w", err))
	}
	return nil
}

// SetEmail records the address a server-side caller said a user signs in with. The row
// is created when there is none; on an existing row only the email moves, never the kind
// the claim path reads, and an email it already holds is not written again: a backend may
// say it on every request.
func (r *Users) SetEmail(ctx context.Context, customerID, userID, email string) error {
	if customerID == "" || userID == "" || email == "" {
		return nil
	}
	user := &User{CustomerID: customerID, ID: userID, Kind: UserKindAuthenticated, Email: email, CreatedAt: time.Now().UTC()}
	_, err := r.store.global.NewInsert().Model(user).
		On("CONFLICT (customer_id, id) DO UPDATE").
		Set("email = EXCLUDED.email").
		Where("u.email IS DISTINCT FROM EXCLUDED.email").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set user email: %w", err))
	}
	return nil
}

// GetEmail returns the address last recorded for a user, or empty.
func (r *Users) GetEmail(ctx context.Context, customerID, userID string) (string, error) {
	var user User
	err := r.store.global.NewSelect().Model(&user).
		Column("email").
		Where("customer_id = ?", customerID).
		Where("id = ?", userID).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("store: user email: %w", err))
	}
	return user.Email, nil
}

// RecordGuest stores a guest this app handed out, so it can later be claimed.
func (s *Store) RecordGuest(ctx context.Context, guest *User) error {
	guest.Kind = UserKindGuest
	guest.Minted = true
	return s.Users.Insert(ctx, guest)
}

// Guest returns one guest of a customer's. A user of some other kind is not one: an
// account that signed up is nobody's to claim.
func (s *Store) Guest(ctx context.Context, customerID, id string) (User, error) {
	if customerID == "" || id == "" {
		return User{}, stack.Wrap(errors.New("store: a customer and a guest id are required"))
	}

	var guest User
	err := s.global.NewSelect().Model(&guest).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("kind = ?", UserKindGuest).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, notAGuest(id, customerID)
	}
	if err != nil {
		return User{}, stack.Wrap(fmt.Errorf("store: guest: %w", err))
	}
	return guest, nil
}

// ClaimGuest moves a guest's sessions and scheduled tasks onto a real user and returns how
// many sessions moved.
//
// One transaction, because the halves are one fact. A guest marked claimed whose
// sessions still say the guest owns them is a person who signed up and lost their history;
// sessions moved without the guest being marked is a guest that can be claimed again, by
// somebody else. A task left on the guest follows up for nobody who can still see it.
//
// With shards, the guest and the tasks are in the shared database and the sessions in every
// shard, and no transaction spans them. The guest is claimed with its tasks first. A claim by
// the same user again moves whatever sessions a failed one left behind, with shards or without,
// so a caller with steps after this one can finish them on a retry.
func (s *Store) ClaimGuest(ctx context.Context, customerID, guestID, userID string) (int64, error) {
	if customerID == "" || guestID == "" || userID == "" {
		return 0, stack.Wrap(errors.New("store: a customer, a guest and a user id are required"))
	}
	if guestID == userID {
		return 0, stack.Wrap(errors.New("store: a guest cannot be claimed by itself"))
	}

	// Only rows a verified caller of the guest's id wrote move: an anonymous caller that went
	// by the same name is somebody else. A guest's row becomes the account's, so the account
	// reaches it by its own kind; a backend's stays a backend's, which the account reaches too.
	const claimedKind = "caller_kind = CASE WHEN caller_kind = ? THEN ? ELSE caller_kind END"
	now := time.Now().UTC()
	moveSessions := func(ctx context.Context, db bun.IDB) (sql.Result, error) {
		return db.NewUpdate().Model((*AgentSession)(nil)).
			Set("user_id = ?", userID).
			Set(claimedKind, UserKindGuest, UserKindAuthenticated).
			Set("updated_at = ?", now).
			Where("customer_id = ?", customerID).
			Where("user_id = ?", guestID).
			Where("caller_kind <> 'anonymous'").
			Exec(ctx)
	}
	moveTasks := func(ctx context.Context, db bun.IDB) error {
		_, err := db.NewUpdate().Model((*ScheduledTask)(nil)).
			Set("user_id = ?", userID).
			Set(claimedKind, UserKindGuest, UserKindAuthenticated).
			Set("updated_at = ?", now).
			Where("customer_id = ?", customerID).
			Where("user_id = ?", guestID).
			Where("caller_kind <> 'anonymous'").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("claim guest scheduled tasks: %w", err)
		}
		return nil
	}

	if !s.Sharded() && len(s.peers) == 0 {
		var moved int64
		err := s.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			if err := claimGuest(ctx, tx, customerID, guestID, userID); err != nil {
				return err
			}
			if err := moveTasks(ctx, tx); err != nil {
				return err
			}
			result, err := moveSessions(ctx, tx)
			if err != nil {
				return fmt.Errorf("claim guest sessions: %w", err)
			}
			moved, err = result.RowsAffected()
			if err != nil {
				return fmt.Errorf("claim guest sessions: %w", err)
			}
			return nil
		})
		if err != nil {
			return 0, stack.Wrap(fmt.Errorf("store: %w", err))
		}
		return moved, nil
	}

	err := s.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := claimGuest(ctx, tx, customerID, guestID, userID); err != nil {
			return err
		}
		return moveTasks(ctx, tx)
	})
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: %w", err))
	}
	moved, err := onEveryShard(ctx, s, moveSessions)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: claim guest sessions: %w", err))
	}
	return moved, nil
}

// claimGuest marks a guest claimed by userID. A claim userID already made stands rather than
// fails, for a caller that has more to finish after it.
func claimGuest(ctx context.Context, db bun.IDB, customerID, guestID, userID string) error {
	claimed, err := db.NewUpdate().Model((*User)(nil)).
		Set("claimed_by = ?", userID).
		Where("id = ?", guestID).
		Where("customer_id = ?", customerID).
		Where("kind = ?", UserKindGuest).
		Where("claimed_by IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("claim guest: %w", err)
	}
	affected, err := claimed.RowsAffected()
	if err != nil {
		return fmt.Errorf("claim guest: %w", err)
	}
	if affected > 0 {
		return nil
	}
	// Either there is no such guest or somebody already claimed them, and the caller is told
	// which: a guest already claimed is a retry, and a guest that never existed is a
	// different mistake.
	var existing User
	err = db.NewSelect().Model(&existing).
		Where("id = ?", guestID).
		Where("customer_id = ?", customerID).
		Where("kind = ?", UserKindGuest).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return notAGuest(guestID, customerID)
	}
	if err != nil {
		return fmt.Errorf("claim guest: %w", err)
	}
	if existing.ClaimedBy == userID {
		return nil
	}
	return stack.Wrap(ErrGuestClaimed)
}

// FinishGuestClaim records that every step of userID's claim of a guest ran, so the same claim
// sent again has nothing left to do. A guest userID has not claimed is left as it is.
func (s *Store) FinishGuestClaim(ctx context.Context, customerID, guestID, userID string) error {
	if customerID == "" || guestID == "" || userID == "" {
		return stack.Wrap(errors.New("store: a customer, a guest and a user id are required"))
	}
	_, err := s.global.NewUpdate().Model((*User)(nil)).
		Set("claim_finished_at = ?", time.Now().UTC()).
		Where("id = ?", guestID).
		Where("customer_id = ?", customerID).
		Where("kind = ?", UserKindGuest).
		Where("claimed_by = ?", userID).
		Where("claim_finished_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: finish guest claim: %w", err))
	}
	return nil
}

// ErrGuestClaimed is a guest another user has already claimed.
var ErrGuestClaimed = errors.New("store: guest claimed")

// ErrNotAGuest is a user id that is not a guest of the customer.
var ErrNotAGuest = errors.New("store: not a guest")

func notAGuest(id, customerID string) error {
	return stack.Wrap(fmt.Errorf("%w: %s is not a guest of %s", ErrNotAGuest, id, customerID))
}
