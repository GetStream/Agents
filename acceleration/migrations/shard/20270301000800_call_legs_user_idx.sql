-- +goose NO TRANSACTION
-- +goose Up

-- An app's end user's call legs, which forgetting them rewrites (CallLegs.RenameUser): no other
-- index starts with the customer, so each batch read the shard's every leg. Built without
-- stopping the writes of legs; a build that fails is built again in Go (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS call_legs_user_idx ON call_legs (customer_id, user_id) WHERE user_id <> '';

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS call_legs_user_idx;
