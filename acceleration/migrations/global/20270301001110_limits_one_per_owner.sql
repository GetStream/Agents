-- +goose NO TRANSACTION
-- +goose Up

-- An organization and an app in none of the same id each have a limit of their own at one place.
-- A row not known to be either's is in both indexes, so a release from before billed is refused a
-- second one exactly as limits_one_per_level refused it. Built before that one goes, without
-- stopping writes; a build that fails is built again in Go (store.usageIndexes), and dropped first
-- here, so the old index never goes before both are valid.
DROP INDEX CONCURRENTLY IF EXISTS limits_one_per_level_organization;
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS limits_one_per_level_organization
    ON limits (organization_id, subject, level, "interval", app_id, api_key, user_id, category) WHERE billed IS NOT FALSE;
DROP INDEX CONCURRENTLY IF EXISTS limits_one_per_level_app;
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS limits_one_per_level_app
    ON limits (organization_id, subject, level, "interval", app_id, api_key, user_id, category) WHERE billed IS NOT TRUE;
-- The limits of an organization id, whoever they belong to, which limits_one_per_level was read for.
CREATE INDEX CONCURRENTLY IF NOT EXISTS limits_organization_idx ON limits (organization_id, id);
-- What Store.AttributeLimits finds who wrote a row by.
CREATE INDEX CONCURRENTLY IF NOT EXISTS audit_log_limit_idx
    ON audit_log (resource_id) WHERE resource_type = 'limit';
DROP INDEX CONCURRENTLY IF EXISTS limits_one_per_level;

-- +goose Down
-- Fails, and changes nothing, while an organization and an app of one id both hold a place.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS limits_one_per_level
    ON limits (organization_id, subject, level, "interval", app_id, api_key, user_id, category);
DROP INDEX CONCURRENTLY IF EXISTS audit_log_limit_idx;
DROP INDEX CONCURRENTLY IF EXISTS limits_organization_idx;
DROP INDEX CONCURRENTLY IF EXISTS limits_one_per_level_app;
DROP INDEX CONCURRENTLY IF EXISTS limits_one_per_level_organization;
