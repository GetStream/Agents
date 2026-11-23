-- +goose Up

-- agent_configs names its columns as AgentConfig groups them: each group's columns share its
-- name as their prefix (sandbox_, video_, dispatch_, episode_cards_, channels_), and the model
-- columns keep their own names. A router of the release before reads the old names and fails
-- until it is replaced. An import from an older release carries the old names over
-- (store.renameImported). Run again on a database that has them already, as an adopted one
-- does, it changes nothing. The lock waits at most 5 s behind a running query, past which the
-- migration fails and the router's next start tries again.
SET LOCAL lock_timeout = '5s';

-- +goose StatementBegin
DO $$
DECLARE
    renamed TEXT[][] := ARRAY[
        ARRAY['sandbox', 'sandbox_provider'],
        ARRAY['episode_cards', 'episode_cards_on'],
        ARRAY['channels', 'channels_lines'],
        ARRAY['chat_channels', 'channels_chat']];
    pair TEXT[];
BEGIN
    FOREACH pair SLICE 1 IN ARRAY renamed LOOP
        IF EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = current_schema() AND table_name = 'agent_configs' AND column_name = pair[1]) THEN
            EXECUTE format('ALTER TABLE agent_configs RENAME COLUMN %I TO %I', pair[1], pair[2]);
        END IF;
    END LOOP;
END $$;
-- +goose StatementEnd

-- Tool groups and connector bindings replaced these, and this release reads neither.
ALTER TABLE agent_configs
    DROP COLUMN IF EXISTS progressive_tools,
    DROP COLUMN IF EXISTS tool_output_retention;
