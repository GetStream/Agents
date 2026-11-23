-- +goose Up
SET LOCAL lock_timeout = '5s';

-- A session's call is its row of calls now, and a call's config is its session's. A router of
-- the release before reads these columns and fails until it is replaced. The lock waits at most
-- 5 s behind a running query, past which the migration fails and the router's next start tries
-- again.
--
-- A router of the release before may have written a session after the backfill passed it. The
-- lock stops those writes until the columns are gone, the sessions with no call yet get theirs
-- as the backfill gives it, and a call whose session was voiced since gains its Stream call.
LOCK TABLE agent_sessions IN SHARE MODE;

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = current_schema() AND table_name = 'agent_sessions' AND column_name = 'call_id') THEN
        RETURN;
    END IF;
    INSERT INTO calls (id, session_id, customer_id, stream_app_pk, call_id, call_type, conversation_id, agent_id, direction, started_at, ended_at)
    SELECT s.id, s.id, s.customer_id, s.stream_app_pk, COALESCE(s.call_id, ''), COALESCE(s.call_type, ''), s.conversation_id,
        s.agent_id, 'inbound', s.created_at, s.closed_at
    FROM agent_sessions AS s
    WHERE NOT EXISTS (SELECT 1 FROM calls AS c WHERE c.session_id = s.id)
    ON CONFLICT (id) DO UPDATE SET
        session_id = EXCLUDED.session_id,
        call_type = EXCLUDED.call_type,
        conversation_id = COALESCE(calls.conversation_id, EXCLUDED.conversation_id),
        call_id = CASE WHEN calls.call_id = '' THEN EXCLUDED.call_id ELSE calls.call_id END
    WHERE calls.session_id IS NULL;
    -- A session given its call before it was voiced holds its Stream call on the session alone.
    UPDATE calls AS c SET call_id = s.call_id, call_type = COALESCE(s.call_type, '')
    FROM agent_sessions AS s
    WHERE c.id = s.id AND c.session_id = s.id AND c.call_id = '' AND COALESCE(s.call_id, '') <> '';
END $$;
-- +goose StatementEnd

ALTER TABLE calls
    DROP COLUMN IF EXISTS user_id,
    DROP COLUMN IF EXISTS config_id;

ALTER TABLE agent_sessions
    DROP COLUMN IF EXISTS call_id,
    DROP COLUMN IF EXISTS call_type;

-- +goose Down
SET LOCAL lock_timeout = '5s';

ALTER TABLE agent_sessions
    ADD COLUMN call_id text,
    ADD COLUMN call_type text;

ALTER TABLE calls
    ADD COLUMN user_id text DEFAULT ''::text NOT NULL,
    ADD COLUMN config_id text;

UPDATE agent_sessions AS s SET call_id = NULLIF(c.call_id, ''), call_type = NULLIF(c.call_type, '')
FROM (SELECT DISTINCT ON (session_id) session_id, call_id, call_type FROM calls
      WHERE session_id IS NOT NULL ORDER BY session_id, started_at DESC) AS c
WHERE c.session_id = s.id;
