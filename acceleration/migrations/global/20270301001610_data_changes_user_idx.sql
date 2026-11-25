-- +goose NO TRANSACTION
-- +goose Up

-- The recorded changes of requests naming an end user, by request, which renaming them
-- rewrites a batch of requests at a time. Built without stopping the writes of changes; a
-- build that fails is built again in Go (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS data_changes_user_idx
    ON data_changes (customer_id, (payload->>'user_id'), (payload->>'id'))
    WHERE table_name = 'requests' AND payload->>'user_id' IS NOT NULL;

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS data_changes_user_idx;
