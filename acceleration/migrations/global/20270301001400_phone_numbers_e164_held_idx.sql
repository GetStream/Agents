-- +goose NO TRANSACTION
-- +goose Up

-- Who holds a number, by its E.164 alone: a number added to a trunk, imported, or called from
-- a trunk, and every attach or call from a bought number, which looks for other apps' lines
-- on it. No index led by e164 covered a number on a trunk, so each read every held row.
-- Built without stopping writes; a build that fails is built again in Go (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS phone_numbers_e164_held_idx
    ON phone_numbers (e164) WHERE released_at IS NULL;

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS phone_numbers_e164_held_idx;
