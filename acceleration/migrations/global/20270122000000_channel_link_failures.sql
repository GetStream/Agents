-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The messages each number texted a line that claimed no link code, per window, so a code
-- cannot be guessed by texting one after another.
CREATE TABLE IF NOT EXISTS channel_link_failures (
    account_id   text        NOT NULL,
    from_address text        NOT NULL,
    window_start timestamptz NOT NULL,
    failures     integer     NOT NULL,
    CONSTRAINT channel_link_failures_pkey PRIMARY KEY (account_id, from_address, window_start)
);
CREATE INDEX IF NOT EXISTS channel_link_failures_window_idx ON channel_link_failures (window_start);

-- +goose Down
DROP TABLE IF EXISTS channel_link_failures;
