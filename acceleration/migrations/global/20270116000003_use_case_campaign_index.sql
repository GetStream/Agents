-- +goose Up
SET LOCAL lock_timeout = '5s';
-- A vendor's campaign webhook finds its use case by the vendor's id for it (UseCases.GetByCampaign).
CREATE INDEX dlc_use_cases_vendor_campaign_idx ON dlc_use_cases (vendor, vendor_campaign_id) WHERE vendor_campaign_id <> '';

-- +goose Down
DROP INDEX dlc_use_cases_vendor_campaign_idx;
