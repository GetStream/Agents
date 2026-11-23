-- +goose Up
SET LOCAL lock_timeout = '5s';
-- The sweeper reads the rows of calls whose end hook never arrived by age (CallResources.SelectStale).
CREATE INDEX call_resources_created_idx ON call_resources (created_at);

-- +goose Down
DROP INDEX call_resources_created_idx;
