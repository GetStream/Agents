-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Whether organization_id is an organization's id or that of an app in none, whose limits, resets
-- and grants are its own. Null on a row a release from before this wrote: it goes on holding the
-- work of both until Store.AttributeLimits knows whose it is. That release reads and writes by
-- organization_id alone, as it did.
ALTER TABLE limits ADD COLUMN billed boolean;
ALTER TABLE usage_resets ADD COLUMN billed boolean;
ALTER TABLE limit_grants ADD COLUMN billed boolean;

-- +goose Down
SET LOCAL lock_timeout = '5s';

ALTER TABLE limit_grants DROP COLUMN billed;
ALTER TABLE usage_resets DROP COLUMN billed;
ALTER TABLE limits DROP COLUMN billed;
