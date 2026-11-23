-- +goose Up
ALTER TABLE calls
    ADD COLUMN session_id text REFERENCES agent_sessions (id) ON DELETE SET NULL,
    ADD COLUMN call_type text DEFAULT ''::text NOT NULL;

-- Every session held its one call under its own id, text sessions included, so each keeps
-- that call: the existing row gains the session and its Stream call, and a session whose row
-- was never written gets one.
INSERT INTO calls (id, session_id, customer_id, stream_app_pk, call_id, call_type, conversation_id, agent_id, direction, started_at, ended_at)
SELECT s.id, s.id, s.customer_id, s.stream_app_pk, COALESCE(s.call_id, ''), COALESCE(s.call_type, ''), s.conversation_id,
    s.agent_id, 'inbound', s.created_at, s.closed_at
FROM agent_sessions AS s
ON CONFLICT (id) DO UPDATE SET
    session_id = EXCLUDED.session_id,
    call_type = EXCLUDED.call_type,
    conversation_id = COALESCE(calls.conversation_id, EXCLUDED.conversation_id),
    call_id = CASE WHEN calls.call_id = '' THEN EXCLUDED.call_id ELSE calls.call_id END;

CREATE INDEX calls_session_idx ON calls USING btree (customer_id, session_id, started_at DESC) WHERE (session_id IS NOT NULL);

CREATE INDEX calls_customer_agent_idx ON calls USING btree (customer_id, agent_id, started_at DESC);

DROP INDEX calls_campaign_idx;

CREATE INDEX calls_campaign_idx ON calls USING btree (customer_id, campaign_id, started_at DESC) WHERE (campaign_id IS NOT NULL);

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION log_agent_request() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE cfg TEXT; sid TEXT;
BEGIN
 IF NEW.agent_id IS NULL OR NEW.agent_id = '' THEN RETURN NEW; END IF;
 IF current_setting('router.request_logged', true) = 'on' THEN RETURN NEW; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.customer_id, 731));
 SELECT s.config_id,c.session_id INTO cfg,sid FROM calls AS c LEFT JOIN agent_sessions AS s ON s.id = c.session_id WHERE c.customer_id=NEW.customer_id AND c.agent_id=NEW.agent_id AND (NEW.call_id IS NULL OR c.call_id=NEW.call_id) AND c.started_at <= NEW.started_at ORDER BY c.started_at DESC LIMIT 1;
 INSERT INTO agent_logs(customer_id,config_id,agent_id,session_id,source,severity,event_type,message,occurred_at,details)
 VALUES(NEW.customer_id,coalesce(cfg,''),NEW.agent_id,coalesce(sid,''),'system',CASE WHEN NEW.success OR NEW.error_code = 'cancelled' THEN 'info' ELSE 'error' END,
 'provider_request',NEW.modality || ' request ' || CASE WHEN NEW.error_code = 'cancelled' THEN 'cancelled' WHEN NEW.success THEN 'completed' ELSE 'failed' END,NEW.started_at,
 jsonb_build_object('provider',NEW.provider,'model',NEW.model,'request_id',NEW.id::text,'duration_ms',NEW.latency_ms,'input_tokens',NEW.input_tokens,'output_tokens',NEW.output_tokens,'cost_micros',NEW.cost_micros,'error_code',NEW.error_code,'error_message',NEW.error_message));
 RETURN NEW;
END $$;
-- +goose StatementEnd

ALTER TABLE calls
    DROP COLUMN user_id,
    DROP COLUMN config_id;

ALTER TABLE agent_sessions
    DROP COLUMN call_id,
    DROP COLUMN call_type;

-- +goose Down
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

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION log_agent_request() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE cfg TEXT; sid TEXT;
BEGIN
 IF NEW.agent_id IS NULL OR NEW.agent_id = '' THEN RETURN NEW; END IF;
 IF current_setting('router.request_logged', true) = 'on' THEN RETURN NEW; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.customer_id, 731));
 SELECT config_id,id INTO cfg,sid FROM calls WHERE customer_id=NEW.customer_id AND agent_id=NEW.agent_id AND (NEW.call_id IS NULL OR call_id=NEW.call_id) AND started_at <= NEW.started_at ORDER BY started_at DESC LIMIT 1;
 INSERT INTO agent_logs(customer_id,config_id,agent_id,session_id,source,severity,event_type,message,occurred_at,details)
 VALUES(NEW.customer_id,coalesce(cfg,''),NEW.agent_id,coalesce(sid,''),'system',CASE WHEN NEW.success OR NEW.error_code = 'cancelled' THEN 'info' ELSE 'error' END,
 'provider_request',NEW.modality || ' request ' || CASE WHEN NEW.error_code = 'cancelled' THEN 'cancelled' WHEN NEW.success THEN 'completed' ELSE 'failed' END,NEW.started_at,
 jsonb_build_object('provider',NEW.provider,'model',NEW.model,'request_id',NEW.id::text,'duration_ms',NEW.latency_ms,'input_tokens',NEW.input_tokens,'output_tokens',NEW.output_tokens,'cost_micros',NEW.cost_micros,'error_code',NEW.error_code,'error_message',NEW.error_message));
 RETURN NEW;
END $$;
-- +goose StatementEnd

DROP INDEX calls_campaign_idx;

CREATE INDEX calls_campaign_idx ON calls USING btree (campaign_id, started_at DESC) WHERE (campaign_id IS NOT NULL);

DROP INDEX calls_customer_agent_idx;

DROP INDEX calls_session_idx;

ALTER TABLE calls
    DROP COLUMN call_type,
    DROP COLUMN session_id;
