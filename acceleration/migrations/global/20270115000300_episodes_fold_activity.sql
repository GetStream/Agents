-- +goose Up
SET LOCAL lock_timeout = '5s';

-- episode_activity was split from episodes only so a rollback could still read it; the store
-- reads with unknown columns discarded, so its columns are the episode's again. The summary
-- lease is lease_until, held by leased_by, the router that closed or took the episode.
ALTER TABLE episodes
    ADD COLUMN last_message_at TIMESTAMPTZ,
    ADD COLUMN lease_until TIMESTAMPTZ,
    ADD COLUMN leased_by TEXT;

UPDATE episodes AS ep
SET last_message_at = act.last_message_at, lease_until = act.summary_lease_until
FROM episode_activity AS act
WHERE act.episode_id = ep.id;

-- The leases a claim takes once they run out (Episodes.ClaimSummaries).
CREATE INDEX episodes_lease ON episodes (lease_until) WHERE lease_until IS NOT NULL;

DROP TABLE episode_activity;

-- +goose Down
CREATE TABLE episode_activity (
    episode_id TEXT PRIMARY KEY REFERENCES episodes (id) ON DELETE CASCADE,
    last_message_at TIMESTAMPTZ,
    summary_lease_until TIMESTAMPTZ
);
CREATE INDEX episode_activity_summary_lease ON episode_activity (summary_lease_until) WHERE summary_lease_until IS NOT NULL;
INSERT INTO episode_activity (episode_id, last_message_at, summary_lease_until)
SELECT id, last_message_at, lease_until FROM episodes WHERE last_message_at IS NOT NULL OR lease_until IS NOT NULL;
DROP INDEX episodes_lease;
ALTER TABLE episodes DROP COLUMN last_message_at, DROP COLUMN lease_until, DROP COLUMN leased_by;
