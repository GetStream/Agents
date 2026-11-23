package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// CallResource is the Stream SIP trunk and routing rule one outbound leg was given, kept so
// the call.session_ended hook knows what to delete once the call ends.
type CallResource struct {
	bun.BaseModel `bun:"table:call_resources,alias:cr"`

	ID int64 `bun:"id,pk,autoincrement"`
	// TrunkID is the Stream SIP trunk this call leg was given: a call may own several, its
	// own plus one per transfer into it.
	TrunkID    string `bun:"trunk_id,notnull"`
	RouteID    string `bun:"route_id,notnull"`
	CallType   string `bun:"call_type,notnull"`
	CallID     string `bun:"call_id,notnull"`
	CustomerID string `bun:"customer_id,notnull"`
	// StreamAppPK is the Stream app this was made in, and the one it is finished in. Zero,
	// stored as NULL, is the deployment's own app.
	StreamAppPK int64 `bun:"stream_app_pk,nullzero"`
	// Shared is a trunk and rule other calls use too: the call's end deletes only this row.
	Shared    bool      `bun:"shared,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
}

// Validate refuses a resource the call's end could not find.
func (c *CallResource) Validate() error {
	return validate("call resource").
		required("trunk_id", c.TrunkID).
		required("route_id", c.RouteID).
		required("call_id", c.CallID).
		done()
}

// CallResources are the trunks and routes outbound legs were given, on each shard.
type CallResources struct {
	store *Store
}

// Insert saves the trunk and route one call leg was given, so the call's end can find and
// delete them.
func (r *CallResources) Insert(ctx context.Context, resource *CallResource) error {
	if err := resource.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if resource.CreatedAt.IsZero() {
		resource.CreatedAt = time.Now().UTC()
	}

	if _, err := r.store.shard.NewInsert().Model(resource).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: record call resource: %w", err))
	}
	return nil
}

// Delete forgets one leg's row, for a leg that was never placed. It is named by its id
// because a call's legs on one shared trunk have the same call and trunk.
func (r *CallResources) Delete(ctx context.Context, id int64) error {
	_, err := r.store.shard.NewDelete().Model((*CallResource)(nil)).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete call resource: %w", err))
	}
	return nil
}

// Release deletes the trunks and routes one call's legs were given in one Stream app, and
// returns them. A call's id is only unique within an app, so an event about it releases
// only what that app holds; unpinned also takes the rows written before apps had
// identities, which were all made in the deployment's own app.
func (r *CallResources) Release(ctx context.Context, scope AppScope, callType, callID string) ([]CallResource, error) {
	// A call's legs may have been dialled from more than one shard.
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]CallResource, error) {
		var released []CallResource
		err := db.NewDelete().Model((*CallResource)(nil)).
			Where("call_type = ?", callType).
			Where("call_id = ?", callID).
			ApplyQueryBuilder(scope.where).
			Returning("*").
			Scan(ctx, &released)
		return released, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: release call resources: %w", err))
	}
	return slices.Concat(lists...), nil
}

// SelectStale returns the rows older than before, for calls whose end hook never arrived.
// They are left in place until Forget.
func (r *CallResources) SelectStale(ctx context.Context, before time.Time) ([]CallResource, error) {
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]CallResource, error) {
		var stale []CallResource
		err := db.NewSelect().Model(&stale).
			Where("created_at < ?", before).
			Scan(ctx)
		return stale, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: stale call resources: %w", err))
	}
	return slices.Concat(lists...), nil
}

// Forget deletes a row SelectStale returned. The row may be on any shard, whose ids are
// their own, so it is matched by its call and trunk too.
func (r *CallResources) Forget(ctx context.Context, resource CallResource) error {
	_, err := onEveryShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (sql.Result, error) {
		return db.NewDelete().Model((*CallResource)(nil)).
			Where("id = ?", resource.ID).
			Where("call_id = ?", resource.CallID).
			Where("trunk_id = ?", resource.TrunkID).
			Exec(ctx)
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: forget call resource: %w", err))
	}
	return nil
}

// Select is what Release would delete for one call, read without deleting it.
//
// A call's lines are on the shard that placed it, so rows from one shard settle it while
// another does not answer; with no rows, a shard that did not answer may hold them.
func (r *CallResources) Select(ctx context.Context, scope AppScope, callType, callID string) ([]CallResource, error) {
	lists, failed := answeringShards(ctx, r.store, "call resources", func(resources []CallResource) bool { return len(resources) > 0 }, func(ctx context.Context, db bun.IDB) ([]CallResource, error) {
		var resources []CallResource
		err := db.NewSelect().Model(&resources).
			Where("call_type = ?", callType).
			Where("call_id = ?", callID).
			ApplyQueryBuilder(scope.where).
			Scan(ctx)
		return resources, err
	})
	resources := slices.Concat(lists...)
	if len(resources) > 0 {
		return resources, nil
	}
	if err := errors.Join(failed...); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: call resources: %w", err))
	}
	return nil, nil
}

// Pin is the Stream app a customer's call has its lines in: the app the trunk carrying it
// was made in, whether an outbound leg's or an attached number's. A session joining the
// call has to be in that app. Found is false for a call with no lines of its own.
func (r *CallResources) Pin(ctx context.Context, customerID, callType, callID string) (int64, bool, error) {
	type pinned struct {
		Pin       sql.NullInt64 `bun:"stream_app_pk"`
		CreatedAt time.Time     `bun:"created_at"`
	}
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]pinned, error) {
		var newest []pinned
		err := db.NewSelect().Model((*CallResource)(nil)).Column("stream_app_pk", "created_at").
			Where("customer_id = ?", customerID).
			Where("call_type = ?", callType).
			Where("call_id = ?", callID).
			OrderExpr("created_at DESC").
			Limit(1).
			Scan(ctx, &newest)
		return newest, err
	})
	if err != nil {
		return 0, false, stack.Wrap(fmt.Errorf("store: call pin: %w", err))
	}
	if newest := merged(lists, func(a, b pinned) int { return b.CreatedAt.Compare(a.CreatedAt) }, 1); len(newest) > 0 {
		return newest[0].Pin.Int64, true, nil
	}

	var pin sql.NullInt64
	err = r.store.global.NewSelect().Model((*PhoneNumber)(nil)).Column("stream_app_pk").
		Where("customer_id = ?", customerID).
		Where("stream_call_type = ?", callType).
		Where("stream_call_id = ?", callID).
		Where("released_at IS NULL").
		Limit(1).
		Scan(ctx, &pin)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, stack.Wrap(fmt.Errorf("store: call pin: %w", err))
	}
	return pin.Int64, true, nil
}
