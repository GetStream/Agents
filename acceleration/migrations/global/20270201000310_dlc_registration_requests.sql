-- +goose Up
SET LOCAL lock_timeout = '5s';

-- When the vendor was asked to register an app's brand, and a use case's campaign, written
-- before it is asked. One with no brand or campaign id recorded is a registration whose answer
-- was lost: the vendor may hold it, and have billed it, so it is not asked again.
ALTER TABLE business_profiles ADD COLUMN IF NOT EXISTS brand_requested_at TIMESTAMPTZ;
ALTER TABLE dlc_use_cases ADD COLUMN IF NOT EXISTS campaign_requested_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE dlc_use_cases DROP COLUMN campaign_requested_at;
ALTER TABLE business_profiles DROP COLUMN brand_requested_at;
