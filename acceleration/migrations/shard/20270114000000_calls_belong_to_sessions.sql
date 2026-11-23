-- +goose Up

-- A call belongs to a session. This adds the columns and the foreign key alone, so it holds the
-- two tables' locks for a moment: the key is checked on new rows at once and on the rows already
-- there later (20270127000200), the backfill runs in batches (20270127000000), the indexes are
-- built without stopping writes (20270127000100) and the columns this replaces are dropped last
-- (20270127000500). The lock waits at most 5 s behind a running query, past which the migration
-- fails and the router's next start tries again.
SET LOCAL lock_timeout = '5s';

ALTER TABLE calls
    ADD COLUMN session_id text,
    ADD COLUMN call_type text DEFAULT ''::text NOT NULL,
    ADD CONSTRAINT calls_session_id_fkey FOREIGN KEY (session_id) REFERENCES agent_sessions (id) ON DELETE SET NULL NOT VALID;

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

-- +goose Down
SET LOCAL lock_timeout = '5s';

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

ALTER TABLE calls
    DROP COLUMN call_type,
    DROP COLUMN session_id;
