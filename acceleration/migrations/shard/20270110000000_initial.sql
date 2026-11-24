-- +goose Up

-- A shard's own tables: sessions, responses, calls, call legs, requests and their rollups. This is
-- the whole history up to 20270126000000, squashed; a database that ran that history records this
-- version as applied and runs nothing here.

-- The change log, kept by both sets: a deployment of one database runs both initial migrations on
-- it, so each of these is a no-op the second time.

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION record_data_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
DECLARE
    subject JSONB;
    owner TEXT;
    identity JSONB := '{}'::JSONB;
    column_name TEXT;
    handed_out BIGINT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        subject := to_jsonb(OLD);
    ELSE
        subject := to_jsonb(NEW);
    END IF;

    -- TG_ARGV[0] is the column holding the customer id, or 'parent' when the row belongs
    -- to a customer through another table: TG_ARGV[1] is then the column referencing it
    -- and TG_ARGV[2] the table it references.
    IF TG_ARGV[0] = 'parent' THEN
        EXECUTE format('SELECT customer_id FROM %I WHERE id = $1', TG_ARGV[2])
            INTO owner USING subject ->> TG_ARGV[1];
    ELSE
        owner := subject ->> TG_ARGV[0];
    END IF;

    IF owner IS NULL OR owner = '' THEN
        RETURN NULL;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM data_change_capture
        WHERE customer_id = owner AND expires_at > now()
    ) THEN
        -- At these two levels every query reads the snapshot of the transaction's first, so a
        -- capture started since is not seen here, and the transaction may hold no id yet for
        -- Store.WaitForOpenWrites to wait on. Every start updates the one row of
        -- data_change_starts. Locking it fails the transaction with a serialization failure if
        -- a start committed after its snapshot, and makes a start that comes later wait until it
        -- ends (https://www.postgresql.org/docs/current/transaction-iso.html#XACT-REPEATABLE-READ).
        IF current_setting('transaction_isolation') IN ('repeatable read', 'serializable') THEN
            PERFORM 1 FROM data_change_starts FOR SHARE;
        END IF;
        RETURN NULL;
    END IF;

    -- TG_ARGV[3] is the table's secret columns as an array literal (store.isSecret, attached by
    -- Store.watchDataChanges), and TG_ARGV[4:] the columns naming the row. A trigger attached
    -- before the list was passed would record the secrets, so it records nothing and fails.
    IF coalesce(left(TG_ARGV[3], 1), '') <> '{' THEN
        RAISE EXCEPTION 'record_data_change: % was attached without its secret columns', TG_TABLE_NAME;
    END IF;

    FOREACH column_name IN ARRAY TG_ARGV[4:] LOOP
        identity := identity || jsonb_build_object(column_name, subject -> column_name);
    END LOOP;

    -- Credentials are left behind. A secret this deployment sealed is worth nothing to
    -- the one reading it, and an access token a customer's user granted to this
    -- deployment is not something to hand out because somebody asked for their data.
    subject := subject - TG_ARGV[3]::text[];

    -- A change is numbered when it is written and seen when its transaction commits, so no
    -- committed row tells a reader which lower number a transaction still running holds.
    -- Before its first change, a transaction takes a shared advisory lock keyed by the
    -- highest number handed out so far, below every number it will be given, and holds it
    -- until it ends. pg_locks shows a lock as soon as it is taken, and Postgres releases it
    -- only once the commit is visible (CommitTransaction in src/backend/access/transam/xact.c:
    -- "this must be done _before_ releasing locks we hold"). Store.changeWatermark reads
    -- these locks. The key
    -- is the number's two halves in the two-integer key space; the other two-integer locks
    -- here start at 839 << 32, a number no feed reaches. The setting, local to the
    -- transaction, takes it once rather than once per row.
    IF coalesce(current_setting('data_changes.bounded', true), '') = '' THEN
        SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END
            INTO handed_out FROM data_changes_seq_seq;
        PERFORM pg_advisory_xact_lock_shared((handed_out >> 32)::int, ((handed_out << 32) >> 32)::int);
        PERFORM set_config('data_changes.bounded', 'on', true);
    END IF;

    INSERT INTO data_changes (customer_id, table_name, op, key, payload)
    VALUES (
        owner,
        TG_TABLE_NAME,
        lower(TG_OP),
        identity,
        CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE subject END
    );
    RETURN NULL;
END;
$_$;
-- +goose StatementEnd

CREATE TABLE IF NOT EXISTS data_change_capture (
    customer_id text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    recorded_since bigint DEFAULT 0 NOT NULL,
    pruned_through bigint DEFAULT 0 NOT NULL
);

CREATE TABLE IF NOT EXISTS data_change_starts (
    one boolean DEFAULT true NOT NULL,
    started bigint DEFAULT 0 NOT NULL,
    CONSTRAINT data_change_starts_one_check CHECK (one)
);

CREATE TABLE IF NOT EXISTS data_changes (
    seq bigint NOT NULL,
    customer_id text NOT NULL,
    table_name text NOT NULL,
    op text NOT NULL,
    key jsonb NOT NULL,
    payload jsonb,
    tx xid8 DEFAULT pg_current_xact_id() NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS data_changes_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE data_changes_seq_seq OWNED BY data_changes.seq;

ALTER TABLE ONLY data_changes ALTER COLUMN seq SET DEFAULT nextval('data_changes_seq_seq'::regclass);

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'data_change_capture_pkey' AND conrelid = 'data_change_capture'::regclass) THEN
        ALTER TABLE ONLY data_change_capture
            ADD CONSTRAINT data_change_capture_pkey PRIMARY KEY (customer_id);
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'data_change_starts_pkey' AND conrelid = 'data_change_starts'::regclass) THEN
        ALTER TABLE ONLY data_change_starts
            ADD CONSTRAINT data_change_starts_pkey PRIMARY KEY (one);
    END IF;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'data_changes_pkey' AND conrelid = 'data_changes'::regclass) THEN
        ALTER TABLE ONLY data_changes
            ADD CONSTRAINT data_changes_pkey PRIMARY KEY (seq);
    END IF;
END $$;
-- +goose StatementEnd

CREATE INDEX IF NOT EXISTS data_changes_at_idx ON data_changes USING btree (at);

CREATE INDEX IF NOT EXISTS data_changes_customer_idx ON data_changes USING btree (customer_id, seq);

INSERT INTO data_change_starts DEFAULT VALUES ON CONFLICT DO NOTHING;

-- The set's own objects.

CREATE TABLE agent_branch_messages (
    session_id text NOT NULL,
    original_message_id text NOT NULL,
    customer_id text NOT NULL,
    response_id text DEFAULT ''::text NOT NULL,
    switch_id text NOT NULL,
    chat_created_at timestamp with time zone NOT NULL,
    author_id text NOT NULL,
    message jsonb DEFAULT '{}'::jsonb NOT NULL
);

CREATE TABLE agent_branch_switches (
    id text NOT NULL,
    session_id text NOT NULL,
    customer_id text NOT NULL,
    kind text NOT NULL,
    target_response_id text NOT NULL,
    fork_message_id text NOT NULL,
    inclusive boolean NOT NULL,
    hidden_from timestamp with time zone NOT NULL,
    state text DEFAULT 'snapshotted'::text NOT NULL,
    resent integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_branch_switches_kind_check CHECK ((kind = ANY (ARRAY['regenerate'::text, 'edit'::text, 'activate'::text]))),
    CONSTRAINT agent_branch_switches_state_check CHECK ((state = ANY (ARRAY['snapshotted'::text, 'rewound'::text, 'resending'::text, 'done'::text])))
);

CREATE TABLE agent_response_items (
    response_id text NOT NULL,
    ordinal integer NOT NULL,
    session_id text NOT NULL,
    kind text NOT NULL,
    text text DEFAULT ''::text NOT NULL,
    tool_name text DEFAULT ''::text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    at timestamp with time zone DEFAULT now() NOT NULL,
    redact_at timestamp with time zone
);

CREATE TABLE agent_responses (
    id text NOT NULL,
    session_id text NOT NULL,
    customer_id text NOT NULL,
    said text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'running'::text NOT NULL,
    error text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    rewound_at timestamp with time zone,
    parent_response_id text,
    question_response_id text
);

CREATE TABLE agent_session_tools (
    session_id text NOT NULL,
    tools jsonb NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE agent_sessions (
    id text NOT NULL,
    customer_id text NOT NULL,
    config_id text,
    agent_name text DEFAULT ''::text NOT NULL,
    agent_id text DEFAULT ''::text NOT NULL,
    conversation_id text DEFAULT ''::text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    caller_kind text DEFAULT ''::text NOT NULL,
    title text DEFAULT ''::text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    project text DEFAULT ''::text NOT NULL,
    custom jsonb DEFAULT '{}'::jsonb NOT NULL,
    model_overwrites jsonb DEFAULT '{}'::jsonb NOT NULL,
    forked_from text,
    state text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    closed_at timestamp with time zone,
    last_response_at timestamp with time zone,
    searchable tsvector GENERATED ALWAYS AS ((((setweight(to_tsvector('english'::regconfig, COALESCE(title, ''::text)), 'A'::"char") || setweight(to_tsvector('english'::regconfig, COALESCE(description, ''::text)), 'B'::"char")) || setweight(to_tsvector('english'::regconfig, COALESCE(project, ''::text)), 'C'::"char")) || setweight(to_tsvector('english'::regconfig, COALESCE(agent_name, ''::text)), 'D'::"char"))) STORED,
    modality text DEFAULT 'voice'::text NOT NULL,
    stream_app_pk bigint,
    connector_selections jsonb DEFAULT '[]'::jsonb NOT NULL,
    return_url text,
    remembers boolean,
    api_key text DEFAULT ''::text NOT NULL,
    usage_categories text[] DEFAULT '{}'::text[] NOT NULL,
    resume_history jsonb DEFAULT '{}'::jsonb NOT NULL,
    end_reason text DEFAULT ''::text NOT NULL,
    floor_control jsonb,
    team text DEFAULT ''::text NOT NULL,
    pinned_at timestamp with time zone,
    review_score integer,
    tool_timeout_ms integer,
    caller_tools jsonb,
    no_ui boolean
);

CREATE TABLE call_bridges (
    token text,
    customer_id text NOT NULL,
    vendor text NOT NULL,
    trunk_uri text NOT NULL,
    trunk_username text,
    trunk_password text,
    initial_digits text,
    call_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    token_hash text,
    trunk_password_sealed bytea,
    trunk_password_kek_version integer
);

CREATE TABLE call_legs (
    id bigint NOT NULL,
    customer_id text NOT NULL,
    organization_id text DEFAULT ''::text NOT NULL,
    organization_tier text DEFAULT ''::text NOT NULL,
    api_key text DEFAULT ''::text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    usage_categories text[] DEFAULT '{}'::text[] NOT NULL,
    stream_app_pk bigint,
    call_type text NOT NULL,
    call_id text NOT NULL,
    stream_session_id text DEFAULT ''::text NOT NULL,
    participant_session text DEFAULT ''::text NOT NULL,
    vendor text NOT NULL,
    direction text NOT NULL,
    local_number text NOT NULL,
    remote_number text NOT NULL,
    country text DEFAULT ''::text NOT NULL,
    agent_id text DEFAULT ''::text NOT NULL,
    tags jsonb DEFAULT '{}'::jsonb NOT NULL,
    dialed_at timestamp with time zone,
    started_at timestamp with time zone,
    ended_at timestamp with time zone,
    end_reason text DEFAULT ''::text NOT NULL,
    vendor_call_id text,
    token text,
    reports boolean DEFAULT false NOT NULL,
    placed_at timestamp with time zone,
    answered_at timestamp with time zone,
    outcome_ended_at timestamp with time zone,
    outcome text,
    vendor_status text,
    decided_at timestamp with time zone,
    token_hash text,
    CONSTRAINT call_legs_dialed_check CHECK (((dialed_at IS NOT NULL) OR (placed_at IS NOT NULL))),
    CONSTRAINT call_legs_direction_check CHECK ((direction = ANY (ARRAY['inbound'::text, 'outbound'::text]))),
    CONSTRAINT call_legs_outcome_check CHECK ((outcome = ANY (ARRAY['completed'::text, 'busy'::text, 'no_answer'::text, 'failed'::text, 'canceled'::text, 'unknown'::text]))),
    CONSTRAINT call_legs_placed_check CHECK (((placed_at IS NOT NULL) OR ((token IS NULL) AND (outcome IS NULL))))
);

CREATE SEQUENCE call_legs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE call_legs_id_seq OWNED BY call_legs.id;

CREATE TABLE call_participants (
    id bigint NOT NULL,
    stream_app_pk bigint,
    unpinned boolean DEFAULT false NOT NULL,
    call_type text NOT NULL,
    call_id text NOT NULL,
    participant_session text DEFAULT ''::text NOT NULL,
    number text DEFAULT ''::text NOT NULL,
    joined_at timestamp with time zone,
    left_at timestamp with time zone,
    recorded_at timestamp with time zone NOT NULL
);

CREATE SEQUENCE call_participants_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE call_participants_id_seq OWNED BY call_participants.id;

CREATE TABLE call_resources (
    trunk_id text NOT NULL,
    route_id text NOT NULL,
    call_type text NOT NULL,
    call_id text NOT NULL,
    customer_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    stream_app_pk bigint,
    id bigint NOT NULL,
    shared boolean DEFAULT false NOT NULL
);

CREATE SEQUENCE call_resources_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE call_resources_id_seq OWNED BY call_resources.id;

CREATE TABLE calls (
    id text NOT NULL,
    customer_id text NOT NULL,
    call_id text NOT NULL,
    agent_id text NOT NULL,
    campaign_id text,
    contact_id text,
    from_number text,
    to_number text,
    direction text DEFAULT 'inbound'::text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    ended_at timestamp with time zone,
    summary text,
    review_score integer,
    review_notes text,
    tags jsonb DEFAULT '{}'::jsonb NOT NULL,
    stt text DEFAULT ''::text NOT NULL,
    tts text DEFAULT ''::text NOT NULL,
    llm text DEFAULT ''::text NOT NULL,
    subagent text DEFAULT ''::text NOT NULL,
    instructions text DEFAULT ''::text NOT NULL,
    skills jsonb,
    sts text DEFAULT ''::text NOT NULL,
    voice text DEFAULT ''::text NOT NULL,
    stream_app_pk bigint,
    conversation_id text,
    agent_user_id text,
    session_id text,
    call_type text DEFAULT ''::text NOT NULL
);

CREATE TABLE conversation_documents (
    id text NOT NULL,
    customer_id text NOT NULL,
    owner_user_id text NOT NULL,
    session_id text NOT NULL,
    conversation_id text DEFAULT ''::text NOT NULL,
    response_id text DEFAULT ''::text NOT NULL,
    name text NOT NULL,
    pages integer DEFAULT 0 NOT NULL,
    sha256 text NOT NULL,
    markdown text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE recordings (
    id text NOT NULL,
    customer_id text NOT NULL,
    modality text NOT NULL,
    status text NOT NULL,
    provider text,
    model text,
    source text,
    text text,
    stt jsonb DEFAULT '{}'::jsonb NOT NULL,
    tts jsonb DEFAULT '{}'::jsonb NOT NULL,
    callback text,
    tags jsonb DEFAULT '{}'::jsonb NOT NULL,
    result jsonb,
    error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone
);

CREATE TABLE requests (
    id bigint CONSTRAINT stt_requests_id_not_null NOT NULL,
    customer_id text CONSTRAINT stt_requests_customer_id_not_null NOT NULL,
    provider text CONSTRAINT stt_requests_provider_not_null NOT NULL,
    model text CONSTRAINT stt_requests_model_not_null NOT NULL,
    started_at timestamp with time zone CONSTRAINT stt_requests_started_at_not_null NOT NULL,
    audio_ms bigint DEFAULT 0 CONSTRAINT stt_requests_audio_ms_not_null NOT NULL,
    latency_ms double precision,
    success boolean CONSTRAINT stt_requests_success_not_null NOT NULL,
    error_code text,
    modality text NOT NULL,
    characters bigint DEFAULT 0 NOT NULL,
    cost_micros bigint DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT 0 NOT NULL,
    cached_input_tokens bigint DEFAULT 0 NOT NULL,
    output_tokens bigint DEFAULT 0 NOT NULL,
    tags jsonb DEFAULT '{}'::jsonb NOT NULL,
    agent_id text,
    call_id text,
    error_message text,
    images bigint DEFAULT 0 NOT NULL,
    operation_id text,
    purpose text,
    turn_id text,
    duration_ms double precision,
    input_instruction_tokens bigint DEFAULT 0 NOT NULL,
    input_message_tokens bigint DEFAULT 0 NOT NULL,
    input_tool_definition_tokens bigint DEFAULT 0 NOT NULL,
    input_tool_use_tokens bigint DEFAULT 0 NOT NULL,
    input_image_tokens bigint DEFAULT 0 NOT NULL,
    input_video_tokens bigint DEFAULT 0 NOT NULL,
    output_cost_micros bigint DEFAULT 0 NOT NULL,
    meters jsonb DEFAULT '{}'::jsonb NOT NULL,
    attributes jsonb DEFAULT '{}'::jsonb NOT NULL,
    billable boolean DEFAULT true NOT NULL,
    price_version text DEFAULT ''::text NOT NULL,
    base_cost_micros bigint DEFAULT 0 NOT NULL,
    usage_id uuid DEFAULT gen_random_uuid(),
    organization_id text DEFAULT ''::text NOT NULL,
    organization_tier text DEFAULT ''::text NOT NULL,
    api_key text DEFAULT ''::text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    usage_categories text[] DEFAULT '{}'::text[] NOT NULL,
    session_id text DEFAULT ''::text NOT NULL,
    to_ship boolean DEFAULT false NOT NULL,
    shipped_at timestamp with time zone,
    ship_error text DEFAULT ''::text NOT NULL,
    limit_grants bigint[] DEFAULT '{}'::bigint[] NOT NULL,
    sku text DEFAULT ''::text NOT NULL,
    region text DEFAULT ''::text NOT NULL,
    byok boolean DEFAULT false NOT NULL,
    source_usage_id uuid
);

CREATE TABLE stats_daily (
    modality text NOT NULL,
    customer_id text NOT NULL,
    provider text NOT NULL,
    model text NOT NULL,
    bucket timestamp with time zone NOT NULL,
    audio_ms_total bigint NOT NULL,
    characters_total bigint NOT NULL,
    cost_micros_total bigint NOT NULL,
    request_count bigint NOT NULL,
    error_count bigint NOT NULL,
    latency_p50_ms double precision,
    latency_p95_ms double precision,
    uptime double precision GENERATED ALWAYS AS ((((request_count - error_count))::double precision / (NULLIF(request_count, 0))::double precision)) STORED,
    input_tokens_total bigint DEFAULT 0 NOT NULL,
    cached_input_tokens_total bigint DEFAULT 0 NOT NULL,
    output_tokens_total bigint DEFAULT 0 NOT NULL,
    images_total bigint DEFAULT 0 NOT NULL
);

CREATE TABLE stats_hourly (
    modality text NOT NULL,
    customer_id text NOT NULL,
    provider text NOT NULL,
    model text NOT NULL,
    bucket timestamp with time zone NOT NULL,
    audio_ms_total bigint NOT NULL,
    characters_total bigint NOT NULL,
    cost_micros_total bigint NOT NULL,
    request_count bigint NOT NULL,
    error_count bigint NOT NULL,
    latency_p50_ms double precision,
    latency_p95_ms double precision,
    uptime double precision GENERATED ALWAYS AS ((((request_count - error_count))::double precision / (NULLIF(request_count, 0))::double precision)) STORED,
    input_tokens_total bigint DEFAULT 0 NOT NULL,
    cached_input_tokens_total bigint DEFAULT 0 NOT NULL,
    output_tokens_total bigint DEFAULT 0 NOT NULL,
    images_total bigint DEFAULT 0 NOT NULL
);

CREATE TABLE stats_tags_daily (
    modality text NOT NULL,
    customer_id text NOT NULL,
    tag_key text NOT NULL,
    tag_value text NOT NULL,
    bucket timestamp with time zone NOT NULL,
    audio_ms_total bigint NOT NULL,
    characters_total bigint NOT NULL,
    input_tokens_total bigint NOT NULL,
    cached_input_tokens_total bigint NOT NULL,
    output_tokens_total bigint NOT NULL,
    cost_micros_total bigint NOT NULL,
    request_count bigint NOT NULL,
    error_count bigint NOT NULL,
    latency_p50_ms double precision,
    latency_p95_ms double precision,
    uptime double precision GENERATED ALWAYS AS ((((request_count - error_count))::double precision / (NULLIF(request_count, 0))::double precision)) STORED,
    images_total bigint DEFAULT 0 NOT NULL
);

CREATE TABLE stats_tags_hourly (
    modality text NOT NULL,
    customer_id text NOT NULL,
    tag_key text NOT NULL,
    tag_value text NOT NULL,
    bucket timestamp with time zone NOT NULL,
    audio_ms_total bigint NOT NULL,
    characters_total bigint NOT NULL,
    input_tokens_total bigint NOT NULL,
    cached_input_tokens_total bigint NOT NULL,
    output_tokens_total bigint NOT NULL,
    cost_micros_total bigint NOT NULL,
    request_count bigint NOT NULL,
    error_count bigint NOT NULL,
    latency_p50_ms double precision,
    latency_p95_ms double precision,
    uptime double precision GENERATED ALWAYS AS ((((request_count - error_count))::double precision / (NULLIF(request_count, 0))::double precision)) STORED,
    images_total bigint DEFAULT 0 NOT NULL
);

CREATE SEQUENCE stt_requests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE stt_requests_id_seq OWNED BY requests.id;

CREATE TABLE turn_latency_hourly (
    customer_id text NOT NULL,
    agent_id text NOT NULL,
    bucket timestamp with time zone NOT NULL,
    leg text NOT NULL,
    bin smallint NOT NULL,
    count bigint NOT NULL
);

CREATE TABLE turn_stats_hourly (
    customer_id text NOT NULL,
    agent_id text NOT NULL,
    bucket timestamp with time zone NOT NULL,
    turn_count bigint NOT NULL,
    interrupted_count bigint NOT NULL,
    audio_out_ms_total double precision NOT NULL
);

ALTER TABLE ONLY call_legs ALTER COLUMN id SET DEFAULT nextval('call_legs_id_seq'::regclass);

ALTER TABLE ONLY call_participants ALTER COLUMN id SET DEFAULT nextval('call_participants_id_seq'::regclass);

ALTER TABLE ONLY call_resources ALTER COLUMN id SET DEFAULT nextval('call_resources_id_seq'::regclass);

ALTER TABLE ONLY requests ALTER COLUMN id SET DEFAULT nextval('stt_requests_id_seq'::regclass);

ALTER TABLE requests
    ADD CONSTRAINT requests_usage_id_present CHECK ((usage_id IS NOT NULL)) NOT VALID;

ALTER TABLE ONLY agent_branch_messages
    ADD CONSTRAINT agent_branch_messages_pkey PRIMARY KEY (session_id, original_message_id);

ALTER TABLE ONLY agent_branch_switches
    ADD CONSTRAINT agent_branch_switches_pkey PRIMARY KEY (id);

ALTER TABLE ONLY agent_response_items
    ADD CONSTRAINT agent_response_items_pkey PRIMARY KEY (response_id, ordinal);

ALTER TABLE ONLY agent_responses
    ADD CONSTRAINT agent_responses_pkey PRIMARY KEY (id);

ALTER TABLE ONLY agent_session_tools
    ADD CONSTRAINT agent_session_tools_pkey PRIMARY KEY (session_id);

ALTER TABLE ONLY agent_sessions
    ADD CONSTRAINT agent_sessions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY call_legs
    ADD CONSTRAINT call_legs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY call_participants
    ADD CONSTRAINT call_participants_pkey PRIMARY KEY (id);

ALTER TABLE ONLY call_resources
    ADD CONSTRAINT call_resources_pkey PRIMARY KEY (id);

ALTER TABLE ONLY calls
    ADD CONSTRAINT calls_pkey PRIMARY KEY (id);

ALTER TABLE ONLY conversation_documents
    ADD CONSTRAINT conversation_documents_pkey PRIMARY KEY (id);

ALTER TABLE ONLY recordings
    ADD CONSTRAINT recordings_pkey PRIMARY KEY (id);

ALTER TABLE ONLY stats_daily
    ADD CONSTRAINT stats_daily_pkey PRIMARY KEY (modality, customer_id, provider, model, bucket);

ALTER TABLE ONLY stats_hourly
    ADD CONSTRAINT stats_hourly_pkey PRIMARY KEY (modality, customer_id, provider, model, bucket);

ALTER TABLE ONLY stats_tags_daily
    ADD CONSTRAINT stats_tags_daily_pkey PRIMARY KEY (modality, customer_id, tag_key, tag_value, bucket);

ALTER TABLE ONLY stats_tags_hourly
    ADD CONSTRAINT stats_tags_hourly_pkey PRIMARY KEY (modality, customer_id, tag_key, tag_value, bucket);

ALTER TABLE ONLY requests
    ADD CONSTRAINT stt_requests_pkey PRIMARY KEY (id);

ALTER TABLE ONLY turn_latency_hourly
    ADD CONSTRAINT turn_latency_hourly_pkey PRIMARY KEY (customer_id, agent_id, bucket, leg, bin);

ALTER TABLE ONLY turn_stats_hourly
    ADD CONSTRAINT turn_stats_hourly_pkey PRIMARY KEY (customer_id, agent_id, bucket);

CREATE INDEX agent_branch_messages_response_idx ON agent_branch_messages USING btree (session_id, response_id);

CREATE INDEX agent_branch_switches_session_idx ON agent_branch_switches USING btree (session_id, hidden_from);

CREATE INDEX agent_branch_switches_unfinished_idx ON agent_branch_switches USING btree (session_id, created_at) WHERE (state <> 'done'::text);

CREATE INDEX agent_response_items_redact_idx ON agent_response_items USING btree (redact_at) WHERE (redact_at IS NOT NULL);

CREATE INDEX agent_response_items_session_idx ON agent_response_items USING btree (session_id, at, response_id, ordinal);

CREATE INDEX agent_responses_parent_idx ON agent_responses USING btree (session_id, parent_response_id);

CREATE INDEX agent_responses_session_idx ON agent_responses USING btree (session_id, created_at, id);

CREATE INDEX agent_sessions_agent_id_idx ON agent_sessions USING btree (customer_id, agent_id, updated_at DESC, id DESC);

CREATE INDEX agent_sessions_agent_idx ON agent_sessions USING btree (customer_id, agent_name, updated_at DESC, id DESC);

CREATE INDEX agent_sessions_conversation_idx ON agent_sessions USING btree (customer_id, conversation_id, created_at DESC);

CREATE INDEX agent_sessions_custom_idx ON agent_sessions USING gin (custom jsonb_path_ops);

CREATE INDEX agent_sessions_customer_idx ON agent_sessions USING btree (customer_id, created_at DESC, id DESC);

CREATE INDEX agent_sessions_last_message_idx ON agent_sessions USING btree (customer_id, COALESCE(last_response_at, created_at) DESC, id DESC);

CREATE INDEX agent_sessions_modality_idx ON agent_sessions USING btree (customer_id, modality, updated_at DESC, id DESC);

CREATE INDEX agent_sessions_pinned_idx ON agent_sessions USING btree (customer_id, ((pinned_at IS NOT NULL)) DESC, COALESCE(last_response_at, created_at) DESC, id DESC);

CREATE INDEX agent_sessions_project_idx ON agent_sessions USING btree (customer_id, project, updated_at DESC, id DESC);

CREATE INDEX agent_sessions_project_last_message_idx ON agent_sessions USING btree (customer_id, project, COALESCE(last_response_at, created_at) DESC, id DESC);

CREATE INDEX agent_sessions_review_score_idx ON agent_sessions USING btree (customer_id, review_score, COALESCE(last_response_at, created_at) DESC, id DESC) WHERE (review_score IS NOT NULL);

CREATE INDEX agent_sessions_search_idx ON agent_sessions USING gin (searchable);

CREATE INDEX agent_sessions_state_idx ON agent_sessions USING btree (customer_id, state, updated_at DESC, id DESC);

CREATE INDEX agent_sessions_team_idx ON agent_sessions USING btree (customer_id, team, updated_at DESC, id DESC);

CREATE INDEX agent_sessions_updated_idx ON agent_sessions USING btree (customer_id, updated_at DESC, id DESC);

CREATE INDEX agent_sessions_user_idx ON agent_sessions USING btree (customer_id, user_id, updated_at DESC, id DESC);

CREATE INDEX agent_sessions_user_last_message_idx ON agent_sessions USING btree (customer_id, user_id, COALESCE(last_response_at, created_at) DESC, id DESC);

CREATE INDEX call_bridges_expires_idx ON call_bridges USING btree (expires_at);

CREATE INDEX call_legs_call_idx ON call_legs USING btree (call_type, call_id) WHERE (ended_at IS NULL);

CREATE INDEX call_legs_ended_idx ON call_legs USING btree (ended_at) WHERE (started_at IS NOT NULL);

CREATE UNIQUE INDEX call_legs_inbound_idx ON call_legs USING btree (call_type, call_id, COALESCE(stream_app_pk, (0)::bigint), stream_session_id) WHERE (direction = 'inbound'::text);

CREATE INDEX call_legs_open_idx ON call_legs USING btree (organization_id) WHERE ((ended_at IS NULL) AND (started_at IS NOT NULL));

CREATE INDEX call_legs_open_started_idx ON call_legs USING btree (started_at) WHERE (ended_at IS NULL);

CREATE UNIQUE INDEX call_legs_participant_idx ON call_legs USING btree (call_type, call_id, COALESCE(stream_app_pk, (0)::bigint), participant_session) WHERE (participant_session <> ''::text);

CREATE INDEX call_legs_ringing_idx ON call_legs USING btree (dialed_at) WHERE ((ended_at IS NULL) AND (started_at IS NULL));

CREATE INDEX call_legs_vendor_call_idx ON call_legs USING btree (vendor_call_id) WHERE (vendor_call_id IS NOT NULL);

CREATE UNIQUE INDEX call_participants_idx ON call_participants USING btree (call_type, call_id, COALESCE(stream_app_pk, (0)::bigint), participant_session);

CREATE INDEX call_participants_recorded_idx ON call_participants USING btree (recorded_at);

CREATE INDEX call_resources_call_idx ON call_resources USING btree (call_type, call_id);

CREATE INDEX calls_agent_idx ON calls USING btree (agent_id, started_at DESC);

CREATE INDEX calls_customer_idx ON calls USING btree (customer_id, started_at DESC);

CREATE INDEX calls_tags_idx ON calls USING gin (tags);

CREATE INDEX conversation_documents_conversation_idx ON conversation_documents USING btree (customer_id, conversation_id, created_at) WHERE (conversation_id <> ''::text);

CREATE INDEX conversation_documents_session_idx ON conversation_documents USING btree (customer_id, session_id, created_at);

CREATE INDEX recordings_customer_idx ON recordings USING btree (customer_id, created_at DESC);

CREATE INDEX requests_agent_started_idx ON requests USING btree (agent_id, started_at DESC) WHERE (agent_id IS NOT NULL);

CREATE INDEX requests_category_spend_idx ON requests USING btree (customer_id, started_at) INCLUDE (cost_micros, usage_categories) WHERE (billable AND (usage_categories <> '{}'::text[]));

CREATE INDEX requests_customer_spend_idx ON requests USING btree (customer_id, started_at DESC) INCLUDE (cost_micros);

CREATE INDEX requests_customer_started_idx ON requests USING btree (modality, customer_id, started_at DESC);

CREATE INDEX requests_key_spend_idx ON requests USING btree (customer_id, api_key, started_at) INCLUDE (cost_micros) WHERE billable;

CREATE INDEX requests_limit_grants_started_idx ON requests USING btree (started_at, id) WHERE (limit_grants <> '{}'::bigint[]);

CREATE INDEX requests_started_idx ON requests USING btree (started_at);

CREATE INDEX requests_tags_idx ON requests USING gin (tags);

CREATE INDEX requests_to_ship_idx ON requests USING btree (started_at) WHERE to_ship;

CREATE UNIQUE INDEX requests_usage_id_idx ON requests USING btree (usage_id);

CREATE INDEX requests_user_spend_idx ON requests USING btree (customer_id, user_id, started_at) INCLUDE (cost_micros) WHERE (billable AND (user_id <> ''::text));

CREATE INDEX calls_customer_agent_idx ON calls USING btree (customer_id, agent_id, started_at DESC);

CREATE INDEX calls_campaign_idx ON calls USING btree (customer_id, campaign_id, started_at DESC) WHERE (campaign_id IS NOT NULL);

ALTER TABLE ONLY call_legs
    ADD CONSTRAINT call_legs_token_key UNIQUE (token);

CREATE INDEX call_legs_placed_call_idx ON call_legs USING btree (customer_id, call_id, id DESC) WHERE (placed_at IS NOT NULL);

CREATE INDEX call_legs_token_placed_idx ON call_legs USING btree (placed_at) WHERE (token IS NOT NULL);

CREATE INDEX call_resources_created_idx ON call_resources USING btree (created_at);

CREATE INDEX call_legs_unanswered_idx ON call_legs USING btree (call_type, call_id) WHERE ((direction = 'outbound'::text) AND (end_reason = 'unanswered'::text));

CREATE INDEX calls_session_started_idx ON calls USING btree (session_id, started_at DESC) WHERE (session_id IS NOT NULL);

ALTER TABLE ONLY call_bridges
    ADD CONSTRAINT call_bridges_token_key UNIQUE (token);

ALTER TABLE ONLY call_bridges
    ADD CONSTRAINT call_bridges_token_hash_key UNIQUE (token_hash);

CREATE UNIQUE INDEX call_legs_token_hash_key ON call_legs USING btree (token_hash);

CREATE INDEX call_legs_token_hash_placed_idx ON call_legs USING btree (placed_at) WHERE (token_hash IS NOT NULL);

ALTER TABLE ONLY agent_branch_messages
    ADD CONSTRAINT agent_branch_messages_session_id_fkey FOREIGN KEY (session_id) REFERENCES agent_sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY agent_branch_switches
    ADD CONSTRAINT agent_branch_switches_session_id_fkey FOREIGN KEY (session_id) REFERENCES agent_sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY agent_response_items
    ADD CONSTRAINT agent_response_items_response_id_fkey FOREIGN KEY (response_id) REFERENCES agent_responses(id) ON DELETE CASCADE;

ALTER TABLE ONLY agent_responses
    ADD CONSTRAINT agent_responses_session_id_fkey FOREIGN KEY (session_id) REFERENCES agent_sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY agent_session_tools
    ADD CONSTRAINT agent_session_tools_session_id_fkey FOREIGN KEY (session_id) REFERENCES agent_sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY calls
    ADD CONSTRAINT calls_session_id_fkey FOREIGN KEY (session_id) REFERENCES agent_sessions(id) ON DELETE SET NULL;

-- +goose Down
-- The initial migration is not undone.
SELECT 1;
