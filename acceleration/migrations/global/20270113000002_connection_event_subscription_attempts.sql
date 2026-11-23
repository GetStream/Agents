-- +goose Up
SET LOCAL lock_timeout = '5s';

-- A claim counts its attempt here, and a worker's write names the count it claimed, so a
-- worker whose lease ran out does not overwrite the one that took the subscription after it.
-- failures stays the streak of the server's refusals, which the retry wait grows with.
ALTER TABLE connection_event_subscriptions
    ADD COLUMN attempts integer NOT NULL DEFAULT 0,
    ADD CONSTRAINT connection_event_subscriptions_attempts_check CHECK (attempts >= 0);

-- +goose Down

ALTER TABLE connection_event_subscriptions DROP COLUMN attempts;
