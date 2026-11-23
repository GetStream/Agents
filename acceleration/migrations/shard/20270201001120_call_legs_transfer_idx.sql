-- +goose NO TRANSACTION
-- +goose Up

-- A leg written with no token is a transfer: known by its call and the vendor's id for the
-- call it placed, so writing it again after a write whose answer was lost (OpenLeg's retry,
-- the leg sweep) writes nothing. Each dial has an id of its own, but legs that share one, as a
-- test vendor's fixed id left, would keep the index from building and the router from
-- starting: every one but the newest forgets the id, which only pressing digits reads. The
-- update locks only those rows. Built without stopping the writes of legs; a build that fails
-- is built again in Go (store.usageIndexes).
UPDATE call_legs AS l SET vendor_call_id = NULL
FROM call_legs AS newer
WHERE l.placed_at IS NULL AND l.vendor_call_id IS NOT NULL
    AND newer.placed_at IS NULL AND newer.vendor_call_id = l.vendor_call_id
    AND newer.call_type = l.call_type AND newer.call_id = l.call_id
    AND COALESCE(newer.stream_app_pk, 0) = COALESCE(l.stream_app_pk, 0)
    AND newer.id > l.id;
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS call_legs_transfer_idx
    ON call_legs (call_type, call_id, COALESCE(stream_app_pk, 0), vendor_call_id)
    WHERE placed_at IS NULL AND vendor_call_id IS NOT NULL;

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS call_legs_transfer_idx;
