-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The shard each session was first opened in, which keeps it for good: its rows are written
-- there and only there, and a request about it that lands in another shard is handed to a
-- router of this one. Written when the session first opens (Sessions.Upsert), and for the
-- sessions from before by each shard's shipper.
CREATE TABLE IF NOT EXISTS session_homes (
    customer_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    shard TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Session id first: an id is one session's whoever owns it (Sessions.Exists).
    PRIMARY KEY (session_id, customer_id)
);

-- How far each shard has shipped each kind of row to the shared database (Store.ShipIndex).
CREATE TABLE IF NOT EXISTS shard_index_shipped (
    shard TEXT NOT NULL,
    kind TEXT NOT NULL,
    shipped_through TIMESTAMPTZ NOT NULL,
    shipped_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (shard, kind)
);

-- +goose Down
DROP TABLE IF EXISTS shard_index_shipped;
DROP TABLE IF EXISTS session_homes;
