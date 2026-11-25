-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Every shard's sessions and calls as the lists read them, each row a copy its shard ships a
-- few seconds behind (Store.ShipIndex), so a list reads one database rather than every shard.
-- The columns are the shard tables', with the shard the row lives in.
CREATE TABLE IF NOT EXISTS session_index (
    shard text NOT NULL,
    id text NOT NULL,
    customer_id text NOT NULL,
    stream_app_pk bigint,
    config_id text,
    agent_name text NOT NULL DEFAULT '',
    agent_id text NOT NULL DEFAULT '',
    conversation_id text NOT NULL DEFAULT '',
    user_id text NOT NULL DEFAULT '',
    caller_kind text NOT NULL DEFAULT '',
    api_key text NOT NULL DEFAULT '',
    usage_categories text[] NOT NULL DEFAULT '{}',
    team text NOT NULL DEFAULT '',
    title text NOT NULL DEFAULT '',
    description text NOT NULL DEFAULT '',
    project text NOT NULL DEFAULT '',
    project_tag text,
    custom jsonb NOT NULL DEFAULT '{}',
    model_overwrites jsonb NOT NULL DEFAULT '{}',
    forked_from text,
    connector_selections jsonb NOT NULL DEFAULT '[]',
    return_url text,
    no_ui boolean,
    remembers boolean,
    resume_history jsonb NOT NULL DEFAULT '{}',
    floor_control jsonb,
    tool_timeout_ms integer,
    caller_tools jsonb,
    state text NOT NULL,
    modality text NOT NULL DEFAULT 'voice',
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    opened_at timestamptz,
    closed_at timestamptz,
    end_reason text NOT NULL DEFAULT '',
    last_response_at timestamptz,
    pinned_at timestamptz,
    review_score integer,
    changed_at timestamptz NOT NULL,
    searchable tsvector GENERATED ALWAYS AS (
        setweight(to_tsvector('english', COALESCE(title, '')), 'A') ||
        setweight(to_tsvector('english', COALESCE(description, '')), 'B') ||
        setweight(to_tsvector('english', COALESCE(project, '')), 'C') ||
        setweight(to_tsvector('english', COALESCE(agent_name, '')), 'D')) STORED,
    PRIMARY KEY (customer_id, id)
);
CREATE INDEX IF NOT EXISTS session_index_customer_idx ON session_index (customer_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS session_index_updated_idx ON session_index (customer_id, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS session_index_last_message_idx ON session_index (customer_id, COALESCE(last_response_at, created_at) DESC, id DESC);
CREATE INDEX IF NOT EXISTS session_index_pinned_idx ON session_index (customer_id, (pinned_at IS NOT NULL) DESC, COALESCE(last_response_at, created_at) DESC, id DESC);
CREATE INDEX IF NOT EXISTS session_index_user_idx ON session_index (customer_id, user_id, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS session_index_user_last_message_idx ON session_index (customer_id, user_id, COALESCE(last_response_at, created_at) DESC, id DESC);
CREATE INDEX IF NOT EXISTS session_index_agent_id_idx ON session_index (customer_id, agent_id, updated_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS session_index_project_last_message_idx ON session_index (customer_id, project, COALESCE(last_response_at, created_at) DESC, id DESC);
CREATE INDEX IF NOT EXISTS session_index_review_score_idx ON session_index (customer_id, review_score, COALESCE(last_response_at, created_at) DESC, id DESC) WHERE review_score IS NOT NULL;
CREATE INDEX IF NOT EXISTS session_index_custom_idx ON session_index USING gin (custom jsonb_path_ops);
CREATE INDEX IF NOT EXISTS session_index_search_idx ON session_index USING gin (searchable);

CREATE TABLE IF NOT EXISTS call_index (
    shard text NOT NULL,
    id text NOT NULL,
    session_id text,
    customer_id text NOT NULL,
    stream_app_pk bigint,
    call_type text NOT NULL DEFAULT '',
    call_id text NOT NULL,
    agent_id text NOT NULL,
    conversation_id text,
    agent_user_id text,
    campaign_id text,
    contact_id text,
    from_number text,
    to_number text,
    direction text NOT NULL DEFAULT 'inbound',
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    stt text NOT NULL DEFAULT '',
    tts text NOT NULL DEFAULT '',
    llm text NOT NULL DEFAULT '',
    subagent text NOT NULL DEFAULT '',
    sts text NOT NULL DEFAULT '',
    voice text NOT NULL DEFAULT '',
    instructions text NOT NULL DEFAULT '',
    skills jsonb,
    summary text,
    review_score integer,
    review_notes text,
    tags jsonb NOT NULL DEFAULT '{}',
    changed_at timestamptz NOT NULL,
    PRIMARY KEY (customer_id, id)
);
CREATE INDEX IF NOT EXISTS call_index_customer_idx ON call_index (customer_id, started_at DESC);
CREATE INDEX IF NOT EXISTS call_index_agent_idx ON call_index (customer_id, agent_id, started_at DESC);
CREATE INDEX IF NOT EXISTS call_index_session_idx ON call_index (customer_id, session_id, started_at DESC) WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS call_index_campaign_idx ON call_index (customer_id, campaign_id, started_at DESC) WHERE campaign_id IS NOT NULL;

-- +goose Down
DROP TABLE IF EXISTS call_index;
DROP TABLE IF EXISTS session_index;
