-- +goose NO TRANSACTION
-- +goose Up

-- The recorded changes of a scheduled task and of its runs, which deleting them rewrites. A
-- delete leaves it, so it holds only the changes that keep a row. Built without stopping the
-- writes of changes; a build that fails is built again in Go (store.usageIndexes).
CREATE INDEX CONCURRENTLY IF NOT EXISTS data_changes_tasks_idx
    ON data_changes (customer_id, (COALESCE(payload->>'task_id', payload->>'id')), (payload->>'id'))
    WHERE table_name IN ('scheduled_tasks', 'scheduled_task_runs') AND op <> 'delete';

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS data_changes_tasks_idx;
