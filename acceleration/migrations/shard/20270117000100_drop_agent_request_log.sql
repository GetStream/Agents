-- +goose Up
SET LOCAL lock_timeout = '5s';

-- A request's provider_request log row is written by the router (Requests.InsertMany), with
-- one database or many, rather than by a trigger that only ran with one.
DROP TRIGGER IF EXISTS agent_request_log ON requests;
DROP FUNCTION IF EXISTS log_agent_request();

-- +goose Down

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION log_agent_request() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE cfg TEXT; sid TEXT;
BEGIN
 IF NEW.agent_id IS NULL OR NEW.agent_id = '' THEN RETURN NEW; END IF;
 IF current_setting('router.request_logged', true) = 'on' THEN RETURN NEW; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.customer_id, 731));
 SELECT s.config_id,c.session_id INTO cfg,sid FROM calls AS c LEFT JOIN agent_sessions AS s ON s.id=c.session_id WHERE c.customer_id=NEW.customer_id AND c.agent_id=NEW.agent_id AND (NEW.call_id IS NULL OR c.call_id=NEW.call_id) AND c.started_at <= NEW.started_at ORDER BY c.started_at DESC LIMIT 1;
 INSERT INTO agent_logs(customer_id,config_id,agent_id,session_id,source,severity,event_type,message,occurred_at,details)
 VALUES(NEW.customer_id,coalesce(cfg,''),NEW.agent_id,coalesce(sid,''),'system',CASE WHEN NEW.success OR NEW.error_code = 'cancelled' THEN 'info' ELSE 'error' END,
 'provider_request',NEW.modality || ' request ' || CASE WHEN NEW.error_code = 'cancelled' THEN 'cancelled' WHEN NEW.success THEN 'completed' ELSE 'failed' END,NEW.started_at,
 jsonb_build_object('provider',NEW.provider,'model',NEW.model,'request_id',NEW.id::text,'duration_ms',NEW.latency_ms,'input_tokens',NEW.input_tokens,'output_tokens',NEW.output_tokens,'cost_micros',NEW.cost_micros,'error_code',NEW.error_code,'error_message',NEW.error_message));
 RETURN NEW;
END $$;
-- +goose StatementEnd

CREATE TRIGGER agent_request_log AFTER INSERT ON requests FOR EACH ROW EXECUTE FUNCTION log_agent_request();
