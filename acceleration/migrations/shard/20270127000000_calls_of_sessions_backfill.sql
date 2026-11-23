-- +goose NO TRANSACTION
-- +goose Up

-- Every session held its one call under its own id, text sessions included, so each keeps that
-- call: the existing row gains the session and its Stream call, and a session whose row was
-- never written gets one. A thousand sessions a transaction, so no lock is held for the whole
-- table. A database whose sessions no longer name their call ran this in one transaction
-- already. Run again after it stopped part way, it skips the calls it gave a session.
-- +goose StatementBegin
DO $$
DECLARE
    after text := '';
    upto text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = current_schema() AND table_name = 'agent_sessions' AND column_name = 'call_id') THEN
        RETURN;
    END IF;
    LOOP
        PERFORM set_config('lock_timeout', '5s', true);
        SELECT max(id) INTO upto FROM (SELECT id FROM agent_sessions WHERE id > after ORDER BY id LIMIT 1000) AS batch;
        EXIT WHEN upto IS NULL;
        INSERT INTO calls (id, session_id, customer_id, stream_app_pk, call_id, call_type, conversation_id, agent_id, direction, started_at, ended_at)
        SELECT s.id, s.id, s.customer_id, s.stream_app_pk, COALESCE(s.call_id, ''), COALESCE(s.call_type, ''), s.conversation_id,
            s.agent_id, 'inbound', s.created_at, s.closed_at
        FROM agent_sessions AS s
        WHERE s.id > after AND s.id <= upto
        ON CONFLICT (id) DO UPDATE SET
            session_id = EXCLUDED.session_id,
            call_type = EXCLUDED.call_type,
            conversation_id = COALESCE(calls.conversation_id, EXCLUDED.conversation_id),
            call_id = CASE WHEN calls.call_id = '' THEN EXCLUDED.call_id ELSE calls.call_id END
        WHERE calls.session_id IS NULL;
        after := upto;
        COMMIT;
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down
