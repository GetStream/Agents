-- +goose Up

-- hook_deliveries and connection_event_deliveries only made a second insert of one delivery
-- fail. One table does that for both, a scope apart, and one pruner forgets both after a day.

CREATE TABLE deliveries_seen (
    scope text NOT NULL,
    key text NOT NULL,
    seen_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, key),
    CONSTRAINT deliveries_seen_key_check CHECK (key <> '')
);

CREATE INDEX deliveries_seen_seen_at ON deliveries_seen (seen_at);

-- A subscription id is hex, so the first slash of a connection event's key ends it
-- (store.ConnectionEventKey).
INSERT INTO deliveries_seen (scope, key, seen_at)
SELECT 'hook', key, seen_at FROM hook_deliveries
UNION ALL
SELECT 'connection_event', subscription_id || '/' || event_id, received_at FROM connection_event_deliveries;

DROP TABLE hook_deliveries;
DROP TABLE connection_event_deliveries;
