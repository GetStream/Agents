-- +goose NO TRANSACTION
-- +goose Up

-- The outbound legs that ended unanswered, which a join delivered again after its call ended
-- revives (CallLegs.Connect). Built without stopping the writes of legs; a build that fails is
-- built again in Go (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS call_legs_unanswered_idx
    ON call_legs (call_type, call_id) WHERE direction = 'outbound' AND end_reason = 'unanswered';

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS call_legs_unanswered_idx;
