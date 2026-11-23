-- +goose NO TRANSACTION
-- +goose Up

-- The placed leg a status post is about, found by its token's hash (CallLegs.GetByToken), and
-- the legs whose status url is still open, which phone.SweepStatusTokens retires. Built without
-- stopping the writes of legs; a build that fails is built again in Go (store.usageIndexes).
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS call_legs_token_hash_key ON call_legs (token_hash);
CREATE INDEX CONCURRENTLY IF NOT EXISTS call_legs_token_hash_placed_idx
    ON call_legs (placed_at) WHERE token_hash IS NOT NULL;

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS call_legs_token_hash_placed_idx;
DROP INDEX CONCURRENTLY IF EXISTS call_legs_token_hash_key;
