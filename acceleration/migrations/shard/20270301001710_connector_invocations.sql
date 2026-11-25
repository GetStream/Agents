-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The connector tool calls sessions ran, kept in the shard that ran them. With one database
-- this is the shared set's table (20270110000000_initial.sql), which every statement here
-- finds. Its connections are in the shared database, so no foreign key names them: deleting a
-- user's connections deletes their calls here too (ConnectorConnections.DeleteUser).
CREATE TABLE IF NOT EXISTS connector_invocations (
    id text PRIMARY KEY,
    customer_id text NOT NULL,
    connection_id text NOT NULL,
    connector_id text NOT NULL,
    config_id text NOT NULL,
    binding text NOT NULL,
    tool text NOT NULL,
    session_id text DEFAULT ''::text NOT NULL,
    started_at timestamp with time zone NOT NULL,
    latency_ms integer NOT NULL,
    error_type text DEFAULT ''::text NOT NULL,
    provider_status integer,
    error_message text DEFAULT ''::text NOT NULL,
    arguments jsonb,
    CONSTRAINT connector_invocations_error_type_check CHECK ((error_type = ANY (ARRAY[''::text, 'customer_auth'::text, 'external_server'::text, 'client_timeout'::text, 'outcome_unknown'::text, 'denied'::text]))),
    CONSTRAINT connector_invocations_latency_ms_check CHECK ((latency_ms >= 0))
);
CREATE INDEX IF NOT EXISTS connector_invocations_connection_idx ON connector_invocations (customer_id, connection_id, started_at DESC, id DESC);

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    -- With one database the table is the shared set's.
    IF to_regclass('goose_global_version') IS NULL THEN
        DROP TABLE IF EXISTS connector_invocations;
    END IF;
END $$;
-- +goose StatementEnd
