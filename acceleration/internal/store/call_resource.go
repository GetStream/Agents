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

// CallResources are the trunks and routes outbound legs were given, each in the shard that placed it.
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
	return r.store.CallHomes.record(ctx, resource.CustomerID, resource.StreamAppPK, resource.CallType, resource.CallID)
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
	var released []CallResource
	err := r.store.shard.NewDelete().Model((*CallResource)(nil)).
		Where("call_type = ?", callType).
		Where("call_id = ?", callID).
		ApplyQueryBuilder(scope.where).
		Returning("*").
		Scan(ctx, &released)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: release call resources: %w", err))
	}
	return released, nil
}

// SelectStale returns this shard's rows older than before, for calls whose end hook never
// arrived. They are left in place until Forget.
func (r *CallResources) SelectStale(ctx context.Context, before time.Time) ([]CallResource, error) {
	var stale []CallResource
	if err := r.store.shard.NewSelect().Model(&stale).Where("created_at < ?", before).Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: stale call resources: %w", err))
	}
	return stale, nil
}

// Forget deletes a row SelectStale returned.
func (r *CallResources) Forget(ctx context.Context, resource CallResource) error {
	_, err := r.store.shard.NewDelete().Model((*CallResource)(nil)).Where("id = ?", resource.ID).Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: forget call resource: %w", err))
	}
	return nil
}

// Select is a call's lines in this shard, read without deleting them. It answers whether the
// router placed a call and whose it is, not everything Release would delete. A call placed in
// another shard is handed there first (CallHomes).
func (r *CallResources) Select(ctx context.Context, scope AppScope, callType, callID string) ([]CallResource, error) {
	var resources []CallResource
	err := r.store.shard.NewSelect().Model(&resources).
		Where("call_type = ?", callType).
		Where("call_id = ?", callID).
		ApplyQueryBuilder(scope.where).
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: call resources: %w", err))
	}
	return resources, nil
}

// Pin is the Stream app a customer's call has its lines in: the app the trunk carrying it
// was made in, whether an outbound leg's or an attached number's. A session joining the
// call has to be in that app. Found is false for a call with no lines of its own.
func (r *CallResources) Pin(ctx context.Context, customerID, callType, callID string) (int64, bool, error) {
	var pin sql.NullInt64
	err := r.store.shard.NewSelect().Model((*CallResource)(nil)).Column("stream_app_pk").
		Where("customer_id = ?", customerID).
		Where("call_type = ?", callType).
		Where("call_id = ?", callID).
		OrderExpr("created_at DESC").
		Limit(1).
		Scan(ctx, &pin)
	if err == nil {
		return pin.Int64, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, stack.Wrap(fmt.Errorf("store: call pin: %w", err))
	}

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
