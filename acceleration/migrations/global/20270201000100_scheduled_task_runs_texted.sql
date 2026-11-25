-- +goose Up
SET LOCAL lock_timeout = '5s';

-- The text channels (sms, whatsapp, imessage) a run's words were sent on, each taken before the
-- send (ScheduledTaskRuns.TakeText): an attempt after the one that sent them, such as a worker
-- taking over a run whose finish was lost, does not text the person again.
ALTER TABLE scheduled_task_runs ADD COLUMN IF NOT EXISTS texted TEXT[] NOT NULL DEFAULT '{}';

-- +goose Down
ALTER TABLE scheduled_task_runs DROP COLUMN IF EXISTS texted;
