-- +goose Up
SET LOCAL lock_timeout = '5s';

-- What customers' agents did, kept in the shard where it happened. With one database this is
-- the shared set's table (20270110000000_initial.sql), which every statement here finds.
CREATE TABLE IF NOT EXISTS agent_logs (
    id bigserial PRIMARY KEY,
    customer_id text NOT NULL,
    config_id text DEFAULT ''::text NOT NULL,
    agent_id text DEFAULT ''::text NOT NULL,
    session_id text DEFAULT ''::text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    source text NOT NULL,
    severity text NOT NULL,
    event_type text NOT NULL,
    message text NOT NULL,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL,
    ingested_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_id bigint
);
CREATE INDEX IF NOT EXISTS agent_logs_config_order ON agent_logs (customer_id, config_id, id DESC);
CREATE INDEX IF NOT EXISTS agent_logs_customer_order ON agent_logs (customer_id, id DESC);
CREATE INDEX IF NOT EXISTS agent_logs_error_order ON agent_logs (customer_id, severity, id DESC);
CREATE INDEX IF NOT EXISTS agent_logs_session_order ON agent_logs (customer_id, session_id, id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS agent_logs_source_idx ON agent_logs (customer_id, source_id) WHERE source_id IS NOT NULL;

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    -- With one database the table is the shared set's.
    IF to_regclass('goose_global_version') IS NULL THEN
        DROP TABLE IF EXISTS agent_logs;
    END IF;
END $$;
-- +goose StatementEnd
