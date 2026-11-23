-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The usage id a request had where it was first recorded, kept by an import, which gives the row
-- a usage id of this deployment's: a row taken back where it was recorded is then known there.
ALTER TABLE requests ADD COLUMN IF NOT EXISTS source_usage_id uuid;

-- +goose Down
ALTER TABLE requests DROP COLUMN IF EXISTS source_usage_id;
