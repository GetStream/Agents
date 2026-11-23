-- +goose Up
SET LOCAL lock_timeout = '5s';
-- A placed call is its outbound leg: kept from before the dial, with its status token, until its
-- outcome is decided. dialed_at is NULL until the vendor takes the dial.
ALTER TABLE call_legs
    ALTER COLUMN dialed_at DROP NOT NULL,
    ADD COLUMN token text,
    ADD COLUMN reports boolean NOT NULL DEFAULT false,
    ADD COLUMN placed_at timestamp with time zone,
    ADD COLUMN answered_at timestamp with time zone,
    ADD COLUMN outcome_ended_at timestamp with time zone,
    ADD COLUMN outcome text,
    ADD COLUMN vendor_status text,
    ADD COLUMN decided_at timestamp with time zone,
    ADD CONSTRAINT call_legs_token_key UNIQUE (token),
    ADD CONSTRAINT call_legs_outcome_check
        CHECK (outcome = ANY (ARRAY['completed', 'busy', 'no_answer', 'failed', 'canceled', 'unknown'])),
    ADD CONSTRAINT call_legs_placed_check CHECK (placed_at IS NOT NULL OR (token IS NULL AND outcome IS NULL)),
    ADD CONSTRAINT call_legs_dialed_check CHECK (dialed_at IS NOT NULL OR placed_at IS NOT NULL);

CREATE INDEX call_legs_placed_call_idx ON call_legs (customer_id, call_id, id DESC) WHERE placed_at IS NOT NULL;
CREATE INDEX call_legs_token_placed_idx ON call_legs (placed_at) WHERE token IS NOT NULL;

-- Each placed call goes onto the outbound leg it opened, the one with its vendor id first, then
-- the one dialled nearest it; a call no leg is left for becomes an ended leg of its own, which
-- was never answered and is billed nothing. A leg two placed calls pick goes to the one by the
-- same rule: its vendor id first, then the nearest in time.
WITH candidates AS (
    SELECT DISTINCT ON (pc.id) pc.id AS pc_id, cl.id AS cl_id,
        (cl.vendor_call_id IS NOT DISTINCT FROM pc.vendor_call_id) AS same_vendor_call,
        abs(extract(epoch FROM cl.dialed_at - pc.placed_at)) AS apart
    FROM phone_calls pc
    JOIN call_legs cl ON cl.direction = 'outbound'
        AND cl.customer_id = pc.customer_id AND cl.call_type = pc.call_type AND cl.call_id = pc.call_id
        AND COALESCE(cl.stream_app_pk, 0) = COALESCE(pc.stream_app_pk, 0)
        AND cl.local_number = pc.local_number AND cl.remote_number = pc.remote_number
        AND (pc.vendor_call_id IS NULL OR cl.vendor_call_id IS NULL OR cl.vendor_call_id = pc.vendor_call_id)
    ORDER BY pc.id, same_vendor_call DESC, apart, cl.id
), matched AS (
    SELECT DISTINCT ON (cl_id) pc_id, cl_id FROM candidates ORDER BY cl_id, same_vendor_call DESC, apart, pc_id DESC
), merged AS (
    UPDATE call_legs cl SET
        token = pc.token, reports = pc.reports, placed_at = pc.placed_at, answered_at = pc.answered_at,
        outcome_ended_at = pc.ended_at, outcome = pc.outcome, vendor_status = pc.vendor_status,
        decided_at = pc.decided_at, vendor_call_id = COALESCE(cl.vendor_call_id, pc.vendor_call_id)
    FROM matched m JOIN phone_calls pc ON pc.id = m.pc_id
    WHERE cl.id = m.cl_id
)
INSERT INTO call_legs (customer_id, stream_app_pk, call_type, call_id, vendor, vendor_call_id, direction,
    local_number, remote_number, dialed_at, ended_at, token, reports, placed_at, answered_at,
    outcome_ended_at, outcome, vendor_status, decided_at)
SELECT pc.customer_id, pc.stream_app_pk, pc.call_type, pc.call_id, pc.vendor, pc.vendor_call_id, 'outbound',
    pc.local_number, pc.remote_number, pc.placed_at, COALESCE(pc.ended_at, pc.placed_at), pc.token, pc.reports,
    pc.placed_at, pc.answered_at, pc.ended_at, pc.outcome, pc.vendor_status, pc.decided_at
FROM phone_calls pc
WHERE pc.id NOT IN (SELECT pc_id FROM matched);

DROP TABLE phone_calls;

-- +goose Down
CREATE TABLE phone_calls (
    id bigserial PRIMARY KEY,
    token text UNIQUE,
    customer_id text NOT NULL,
    stream_app_pk bigint,
    call_type text NOT NULL,
    call_id text NOT NULL,
    vendor text NOT NULL,
    local_number text NOT NULL,
    remote_number text NOT NULL,
    reports boolean NOT NULL,
    vendor_call_id text,
    placed_at timestamp with time zone NOT NULL,
    answered_at timestamp with time zone,
    ended_at timestamp with time zone,
    outcome text CHECK (outcome = ANY (ARRAY['completed', 'busy', 'no_answer', 'failed', 'canceled', 'unknown'])),
    vendor_status text,
    decided_at timestamp with time zone
);
CREATE INDEX phone_calls_customer_call_idx ON phone_calls (customer_id, call_id, id DESC);
CREATE INDEX phone_calls_placed_idx ON phone_calls (placed_at) WHERE token IS NOT NULL;
INSERT INTO phone_calls (token, customer_id, stream_app_pk, call_type, call_id, vendor, local_number, remote_number,
    reports, vendor_call_id, placed_at, answered_at, ended_at, outcome, vendor_status, decided_at)
SELECT token, customer_id, stream_app_pk, call_type, call_id, vendor, local_number, remote_number,
    reports, vendor_call_id, placed_at, answered_at, outcome_ended_at, outcome, vendor_status, decided_at
FROM call_legs WHERE placed_at IS NOT NULL ORDER BY id;
DELETE FROM call_legs WHERE dialed_at IS NULL;
ALTER TABLE call_legs
    DROP CONSTRAINT call_legs_dialed_check,
    DROP CONSTRAINT call_legs_placed_check,
    DROP CONSTRAINT call_legs_outcome_check,
    DROP CONSTRAINT call_legs_token_key,
    DROP COLUMN token, DROP COLUMN reports, DROP COLUMN placed_at, DROP COLUMN answered_at,
    DROP COLUMN outcome_ended_at, DROP COLUMN outcome, DROP COLUMN vendor_status, DROP COLUMN decided_at,
    ALTER COLUMN dialed_at SET NOT NULL;
