-- +goose Up
SET LOCAL lock_timeout = '5s';

-- A purchase or a release of a number, written before the vendor is asked and deleted once the
-- number's row says what the vendor did, or the vendor refused. A row left behind is one whose
-- outcome was not recorded: the phone sweep claims it once next_attempt_at is due, asks the
-- vendor whether it holds the number, and writes the row or the release it says. A claim moves
-- next_attempt_at on, so one router asks at a time and an order no vendor can settle goes to
-- the back. created_at is when the number was bought or released.
CREATE TABLE IF NOT EXISTS phone_number_orders (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    vendor TEXT NOT NULL,
    e164 TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('buy', 'release')),
    country TEXT NOT NULL DEFAULT '',
    monthly_cost_micros BIGINT NOT NULL DEFAULT 0,
    tags JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS phone_number_orders_due_idx ON phone_number_orders (next_attempt_at, id);

-- +goose Down
DROP TABLE phone_number_orders;
