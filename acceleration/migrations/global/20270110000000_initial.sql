-- +goose Up

-- The shared tables: tenants, configs, connectors, numbers, limits. Every shard reads them from the
-- one shared database. This is the whole history up to 20270124093000, squashed; a database that ran
-- that history records this version as applied and runs nothing here.

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

-- +goose StatementBegin
CREATE FUNCTION price_changes_guard() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'price_changes rows are never deleted';
    END IF;
    IF OLD.cancelled_at IS NOT NULL OR OLD.effective_at <= now() OR NEW.cancelled_at IS NULL
        OR (NEW.name, NEW.kind, NEW.reason, NEW.source, NEW.effective_at,
            NEW.created_by, NEW.idempotency_key, NEW.created_at)
           IS DISTINCT FROM
           (OLD.name, OLD.kind, OLD.reason, OLD.source, OLD.effective_at,
            OLD.created_by, OLD.idempotency_key, OLD.created_at) THEN
        RAISE EXCEPTION 'a price change can only be cancelled, once, before it takes effect';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION price_entries_guard() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'price_entries rows are never changed or deleted: write a newer one';
    END IF;
    NEW.effective_at := (SELECT effective_at FROM price_changes WHERE name = NEW.change);
    RETURN NEW;
END $$;
-- +goose StatementEnd

CREATE TABLE activity_digests (
    day date NOT NULL,
    claimed_at timestamp with time zone DEFAULT now() NOT NULL,
    posted_at timestamp with time zone
);

CREATE TABLE agent_configs (
    id text NOT NULL,
    customer_id text NOT NULL,
    name text NOT NULL,
    stt text DEFAULT ''::text NOT NULL,
    tts text DEFAULT ''::text NOT NULL,
    voice text DEFAULT ''::text NOT NULL,
    llm text DEFAULT ''::text NOT NULL,
    subagent text DEFAULT ''::text NOT NULL,
    instructions text DEFAULT ''::text NOT NULL,
    greeting text DEFAULT ''::text NOT NULL,
    skills jsonb DEFAULT '[]'::jsonb NOT NULL,
    knowledge_namespace text DEFAULT ''::text NOT NULL,
    tags jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    keyterms jsonb DEFAULT '[]'::jsonb NOT NULL,
    sync_hash text DEFAULT ''::text NOT NULL,
    search text DEFAULT ''::text NOT NULL,
    mode text DEFAULT 'voice'::text NOT NULL,
    sandbox_provider text DEFAULT ''::text CONSTRAINT agent_configs_sandbox_not_null NOT NULL,
    video_source text DEFAULT ''::text NOT NULL,
    video_max_frames integer DEFAULT 1 NOT NULL,
    sts text DEFAULT ''::text NOT NULL,
    guardrail text DEFAULT ''::text NOT NULL,
    visible_tools jsonb DEFAULT '[]'::jsonb NOT NULL,
    harness text DEFAULT 'default'::text NOT NULL,
    dispatch_incoming_call boolean DEFAULT false NOT NULL,
    dispatch_text boolean DEFAULT false NOT NULL,
    connectors jsonb DEFAULT '[]'::jsonb NOT NULL,
    sandbox_options jsonb DEFAULT '{}'::jsonb NOT NULL,
    channels_lines jsonb DEFAULT '{}'::jsonb CONSTRAINT agent_configs_channels_not_null NOT NULL,
    episode_cards_on boolean DEFAULT false CONSTRAINT agent_configs_episode_cards_not_null NOT NULL,
    greeting_mode text DEFAULT 'exact'::text NOT NULL,
    left_out_connectors jsonb DEFAULT '[]'::jsonb NOT NULL,
    role text DEFAULT ''::text NOT NULL,
    scheduling jsonb DEFAULT '{}'::jsonb NOT NULL,
    review_instructions text DEFAULT ''::text NOT NULL,
    resume_history jsonb DEFAULT '{}'::jsonb NOT NULL,
    region text DEFAULT 'us'::text NOT NULL,
    calls jsonb DEFAULT '{}'::jsonb NOT NULL,
    episode_cards_record boolean DEFAULT false NOT NULL,
    floor_control jsonb,
    llm_fallbacks jsonb,
    channels_chat jsonb DEFAULT '[]'::jsonb CONSTRAINT agent_configs_chat_channels_not_null NOT NULL,
    agents jsonb,
    image text DEFAULT ''::text NOT NULL,
    ui_instructions text DEFAULT ''::text NOT NULL,
    tool_groups jsonb,
    CONSTRAINT agent_configs_region_check CHECK ((region ~ '^[a-z][a-z0-9-]{0,31}$'::text))
);

CREATE TABLE agent_logs (
    id bigint NOT NULL,
    customer_id text NOT NULL,
    config_id text DEFAULT ''::text NOT NULL,
    agent_id text DEFAULT ''::text NOT NULL,
    session_id text DEFAULT ''::text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    source text NOT NULL,
    severity text NOT NULL,
    event_type text NOT NULL,
    message text NOT NULL,
    occurred_at timestamp with time zone DEFAULT now() NOT NULL,
    ingested_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    details jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_id bigint
);

CREATE SEQUENCE agent_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE agent_logs_id_seq OWNED BY agent_logs.id;

CREATE TABLE api_keys (
    id text NOT NULL,
    app_id text NOT NULL,
    name text NOT NULL,
    environment text DEFAULT 'live'::text NOT NULL,
    secret_sealed bytea NOT NULL,
    kek_version integer DEFAULT 1 NOT NULL,
    last4 text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by text DEFAULT ''::text NOT NULL,
    expires_at timestamp with time zone,
    last_used_at timestamp with time zone,
    revoked_at timestamp with time zone,
    revoked_by text DEFAULT ''::text NOT NULL
);

CREATE TABLE app_organizations (
    app_id text NOT NULL,
    organization_id text NOT NULL,
    seen_at timestamp with time zone NOT NULL
);

CREATE TABLE apps (
    id text NOT NULL,
    organization_id text NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    settings jsonb DEFAULT '{}'::jsonb NOT NULL
);

CREATE TABLE audit_log (
    id text NOT NULL,
    customer_id text NOT NULL,
    resource_type text NOT NULL,
    resource_id text NOT NULL,
    resource_name text DEFAULT ''::text NOT NULL,
    agent_id text DEFAULT ''::text NOT NULL,
    action text NOT NULL,
    source text NOT NULL,
    actor_id text DEFAULT ''::text NOT NULL,
    actor_name text DEFAULT ''::text NOT NULL,
    request_id text DEFAULT ''::text NOT NULL,
    changes jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL
);

CREATE TABLE business_profiles (
    customer_id text NOT NULL,
    legal_business_name text DEFAULT ''::text NOT NULL,
    brand_name text DEFAULT ''::text NOT NULL,
    legal_entity_type text DEFAULT ''::text NOT NULL,
    organization_type text DEFAULT ''::text NOT NULL,
    business_registration_country text DEFAULT ''::text NOT NULL,
    tax_id text DEFAULT ''::text NOT NULL,
    tax_id_issuing_country text DEFAULT ''::text NOT NULL,
    registered_address jsonb DEFAULT '{}'::jsonb NOT NULL,
    website_url text DEFAULT ''::text NOT NULL,
    industry text DEFAULT ''::text NOT NULL,
    authorized_contact_first_name text DEFAULT ''::text NOT NULL,
    authorized_contact_last_name text DEFAULT ''::text NOT NULL,
    authorized_contact_title text DEFAULT ''::text NOT NULL,
    authorized_contact_email text DEFAULT ''::text NOT NULL,
    authorized_contact_phone text DEFAULT ''::text NOT NULL,
    privacy_policy_url text DEFAULT ''::text NOT NULL,
    terms_and_conditions_url text DEFAULT ''::text NOT NULL,
    stock_symbol text DEFAULT ''::text NOT NULL,
    stock_exchange text DEFAULT ''::text NOT NULL,
    business_verification_documents text[] DEFAULT '{}'::text[] NOT NULL,
    vendor_brand_id text DEFAULT ''::text NOT NULL,
    brand_status text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE campaign_attempts (
    id text NOT NULL,
    campaign_id text NOT NULL,
    contact_id text NOT NULL,
    step_id text NOT NULL,
    attempt integer NOT NULL,
    dial integer DEFAULT 1 NOT NULL,
    call_id text DEFAULT ''::text NOT NULL,
    vendor_call_id text DEFAULT ''::text NOT NULL,
    result text DEFAULT ''::text NOT NULL,
    call_outcome text,
    outcome text DEFAULT ''::text NOT NULL,
    converted boolean DEFAULT false NOT NULL,
    classify_state text DEFAULT ''::text NOT NULL,
    classify_after timestamp with time zone,
    classify_tries integer DEFAULT 0 NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    ended_at timestamp with time zone
);

CREATE TABLE campaign_contacts (
    id text NOT NULL,
    seq bigint NOT NULL,
    campaign_id text NOT NULL,
    to_number text NOT NULL,
    instructions text DEFAULT ''::text NOT NULL,
    stage text DEFAULT 'enrolled'::text CONSTRAINT campaign_contacts_state_not_null NOT NULL,
    attempt integer DEFAULT 0 CONSTRAINT campaign_contacts_attempts_not_null NOT NULL,
    call_id text,
    vendor_call_id text,
    reason text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    variables jsonb DEFAULT '{}'::jsonb NOT NULL,
    time_zone text DEFAULT ''::text NOT NULL,
    step_id text DEFAULT ''::text NOT NULL,
    next_attempt_at timestamp with time zone,
    leased_by text DEFAULT ''::text NOT NULL,
    lease_until timestamp with time zone,
    outcome text DEFAULT ''::text NOT NULL,
    converted boolean DEFAULT false NOT NULL
);

CREATE SEQUENCE campaign_contacts_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE campaign_contacts_seq_seq OWNED BY campaign_contacts.seq;

CREATE TABLE campaigns (
    id text NOT NULL,
    customer_id text NOT NULL,
    name text NOT NULL,
    config_id text DEFAULT ''::text NOT NULL,
    from_number text DEFAULT ''::text NOT NULL,
    concurrency integer DEFAULT 1 NOT NULL,
    state text DEFAULT 'draft'::text NOT NULL,
    tags jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    api_key text DEFAULT ''::text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    usage_categories text[] DEFAULT '{}'::text[] NOT NULL,
    paused_reason text,
    move_paused boolean DEFAULT false NOT NULL,
    sequence jsonb,
    calling_hours jsonb,
    default_country text DEFAULT ''::text NOT NULL,
    start_at timestamp with time zone,
    launched_at timestamp with time zone,
    stopped_at timestamp with time zone,
    consent_at timestamp with time zone,
    type text DEFAULT 'standard'::text NOT NULL
);

CREATE TABLE channel_accounts (
    id text NOT NULL,
    customer_id text NOT NULL,
    kind text NOT NULL,
    e164 text NOT NULL,
    account_id text DEFAULT ''::text NOT NULL,
    token text NOT NULL,
    secrets_sealed bytea,
    kek_version integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone
);

CREATE TABLE channel_deliveries (
    account_id text NOT NULL,
    message_id text NOT NULL,
    received_at timestamp with time zone NOT NULL,
    from_address text DEFAULT ''::text NOT NULL
);

CREATE TABLE channel_link_failures (
    account_id text NOT NULL,
    from_address text NOT NULL,
    window_start timestamp with time zone NOT NULL,
    failures integer NOT NULL
);

CREATE TABLE channel_links (
    code text NOT NULL,
    customer_id text NOT NULL,
    config_id text NOT NULL,
    user_id text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    used_at timestamp with time zone
);

CREATE TABLE channel_thread_inbound (
    seq bigint NOT NULL,
    channel_id text NOT NULL,
    provider_message_id text NOT NULL,
    customer_id text NOT NULL,
    connector_id text NOT NULL,
    provider_unit_id text NOT NULL,
    thread_key text NOT NULL,
    config_id text NOT NULL,
    config_name text NOT NULL,
    author_id text NOT NULL,
    text text NOT NULL,
    media jsonb DEFAULT '[]'::jsonb NOT NULL,
    raw bytea NOT NULL,
    forward jsonb,
    state text DEFAULT 'queued'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    keyword_answered boolean DEFAULT false NOT NULL,
    CONSTRAINT channel_thread_inbound_state_check CHECK ((state = ANY (ARRAY['queued'::text, 'done'::text, 'failed'::text])))
);

ALTER TABLE channel_thread_inbound ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME channel_thread_inbound_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

CREATE TABLE channel_thread_members (
    channel_id text NOT NULL,
    member text NOT NULL
);

CREATE TABLE channel_thread_messages (
    channel_id text NOT NULL,
    kind text NOT NULL,
    message_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_thread_messages_kind_check CHECK ((kind = ANY (ARRAY['inbound'::text, 'turn'::text, 'reply'::text])))
);

CREATE TABLE channel_thread_waiting (
    customer_id text NOT NULL,
    connector_id text NOT NULL,
    provider_unit_id text NOT NULL,
    thread_key text NOT NULL,
    provider_message_id text NOT NULL,
    author_id text NOT NULL,
    text text NOT NULL,
    raw bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE channel_threads (
    channel_id text NOT NULL,
    customer_id text NOT NULL,
    connector_id text NOT NULL,
    provider_unit_id text NOT NULL,
    thread_key text NOT NULL,
    connection_id text NOT NULL,
    thread_parts jsonb DEFAULT '{}'::jsonb NOT NULL,
    stream_app_pk bigint,
    last_inbound_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    chat_person text DEFAULT ''::text NOT NULL,
    starter text DEFAULT ''::text NOT NULL
);

CREATE TABLE channel_turns (
    stream_app_pk bigint CONSTRAINT chat_channel_turns_stream_app_pk_not_null NOT NULL,
    cid text CONSTRAINT chat_channel_turns_cid_not_null NOT NULL,
    leased_by text CONSTRAINT chat_channel_turns_turn_holder_not_null NOT NULL,
    lease_until timestamp with time zone CONSTRAINT chat_channel_turns_turn_until_not_null NOT NULL
);

CREATE TABLE chat_inbound (
    seq bigint NOT NULL,
    stream_app_pk bigint CONSTRAINT chat_inbound_stream_app_not_null NOT NULL,
    message_id text NOT NULL,
    customer_id text DEFAULT ''::text NOT NULL,
    deployment boolean NOT NULL,
    api_key text DEFAULT ''::text NOT NULL,
    body bytea NOT NULL,
    state text DEFAULT 'queued'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    heard text,
    CONSTRAINT chat_inbound_state_check CHECK ((state = ANY (ARRAY['queued'::text, 'done'::text, 'failed'::text])))
);

ALTER TABLE chat_inbound ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME chat_inbound_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

CREATE TABLE connection_event_subscriptions (
    id text NOT NULL,
    customer_id text NOT NULL,
    connection_id text NOT NULL,
    config_id text NOT NULL,
    binding text NOT NULL,
    event text NOT NULL,
    arguments jsonb DEFAULT '{}'::jsonb NOT NULL,
    key text NOT NULL,
    token text NOT NULL,
    secret_sealed bytea NOT NULL,
    kek_version integer NOT NULL,
    remote_id text DEFAULT ''::text NOT NULL,
    refresh_before timestamp with time zone,
    status text NOT NULL,
    error text DEFAULT ''::text NOT NULL,
    next_attempt_at timestamp with time zone,
    failures integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    CONSTRAINT connection_event_subscriptions_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT connection_event_subscriptions_kek_version_check CHECK ((kek_version >= 1)),
    CONSTRAINT connection_event_subscriptions_secret_sealed_check CHECK ((secret_sealed <> '\x'::bytea)),
    CONSTRAINT connection_event_subscriptions_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'active'::text, 'failed'::text])))
);

CREATE TABLE connector_audit (
    id text NOT NULL,
    customer_id text NOT NULL,
    connection_id text NOT NULL,
    connector_id text NOT NULL,
    owner_type text NOT NULL,
    action text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    revision integer DEFAULT 0 NOT NULL,
    request_id text DEFAULT ''::text NOT NULL,
    session_id text DEFAULT ''::text NOT NULL,
    attempt_id text DEFAULT ''::text NOT NULL,
    status_code integer,
    latency_ms integer,
    target text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    tools jsonb,
    credential jsonb
);

CREATE TABLE connector_authorization_attempts (
    id text NOT NULL,
    customer_id text NOT NULL,
    connection_id text NOT NULL,
    kind text NOT NULL,
    state_hash text NOT NULL,
    attempt_sealed bytea NOT NULL,
    kek_version integer NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    consumed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT connector_authorization_attempts_kek_version_check CHECK ((kek_version >= 1))
);

CREATE TABLE connector_broken_revisions (
    connector_id text NOT NULL,
    revision integer NOT NULL,
    reason text NOT NULL,
    marked_by integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT connector_broken_revisions_check CHECK ((marked_by > revision)),
    CONSTRAINT connector_broken_revisions_connector_id_check CHECK ((NOT starts_with(connector_id, 'custom_'::text))),
    CONSTRAINT connector_broken_revisions_reason_check CHECK ((reason <> ''::text)),
    CONSTRAINT connector_broken_revisions_revision_check CHECK ((revision >= 1))
);

CREATE TABLE connector_config_tokens (
    customer_id text NOT NULL,
    connector_id text NOT NULL,
    tokens_sealed bytea NOT NULL,
    kek_version integer NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT connector_config_tokens_kek_version_check CHECK ((kek_version >= 1)),
    CONSTRAINT connector_config_tokens_tokens_sealed_check CHECK ((tokens_sealed <> '\x'::bytea))
);

CREATE TABLE connector_connections (
    id text NOT NULL,
    customer_id text NOT NULL,
    connector_id text NOT NULL,
    definition_revision integer NOT NULL,
    owner_type text NOT NULL,
    owner_id text DEFAULT ''::text NOT NULL,
    auth_scheme text NOT NULL,
    tls_scheme text,
    inputs jsonb DEFAULT '{}'::jsonb NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    label text DEFAULT ''::text NOT NULL,
    account_id text DEFAULT ''::text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    granted_scopes jsonb DEFAULT '[]'::jsonb NOT NULL,
    revision integer DEFAULT 1 NOT NULL,
    credentials_sealed bytea DEFAULT '\x'::bytea CONSTRAINT connector_connections_material_sealed_not_null NOT NULL,
    credentials_kek_version integer DEFAULT 0 CONSTRAINT connector_connections_material_kek_version_not_null NOT NULL,
    expires_at timestamp with time zone,
    cached_tools jsonb DEFAULT '[]'::jsonb NOT NULL,
    tools_digest text DEFAULT ''::text NOT NULL,
    tools_checked_at timestamp with time zone,
    last_error text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    provider_unit_id text,
    connected_at timestamp with time zone,
    client_registration text DEFAULT ''::text NOT NULL,
    client_id text DEFAULT ''::text NOT NULL,
    client_updated_at timestamp with time zone,
    validation_revision integer DEFAULT 0 NOT NULL,
    validation_status text DEFAULT ''::text NOT NULL,
    validation_code text DEFAULT ''::text NOT NULL,
    validation_error text DEFAULT ''::text NOT NULL,
    validation_checked_at timestamp with time zone,
    CONSTRAINT connector_connections_definition_revision_check CHECK ((definition_revision >= 1)),
    CONSTRAINT connector_connections_owner CHECK ((((owner_type = 'app'::text) AND (owner_id = ''::text)) OR ((owner_type = 'user'::text) AND (owner_id <> ''::text)))),
    CONSTRAINT connector_connections_revision_check CHECK ((revision >= 1))
);

CREATE TABLE connector_definitions (
    customer_id text NOT NULL,
    id text NOT NULL,
    revision integer NOT NULL,
    name text NOT NULL,
    category text DEFAULT ''::text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    manifest jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT connector_definitions_custom_prefix CHECK (((customer_id = ''::text) <> starts_with(id, 'custom_'::text))),
    CONSTRAINT connector_definitions_revision_check CHECK ((revision >= 1))
);

CREATE TABLE connector_event_deliveries (
    destination_id text NOT NULL,
    id text NOT NULL,
    headers jsonb DEFAULT '{}'::jsonb NOT NULL,
    body bytea NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    provider_headers_until timestamp with time zone
);

CREATE TABLE connector_event_destinations (
    id text NOT NULL,
    customer_id text NOT NULL,
    connector_id text NOT NULL,
    url text NOT NULL,
    forward text NOT NULL,
    secret_sealed bytea NOT NULL,
    kek_version integer NOT NULL,
    previous_secret_sealed bytea,
    previous_kek_version integer DEFAULT 0 NOT NULL,
    previous_until timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT connector_event_destinations_forward_check CHECK ((forward = ANY (ARRAY['unhandled'::text, 'all'::text]))),
    CONSTRAINT connector_event_destinations_kek_version_check CHECK ((kek_version >= 1)),
    CONSTRAINT connector_event_destinations_secret_sealed_check CHECK ((secret_sealed <> '\x'::bytea))
);

CREATE TABLE connector_invocations (
    id text NOT NULL,
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

CREATE TABLE connector_oauth_clients (
    customer_id text NOT NULL,
    connector_id text NOT NULL,
    registration text NOT NULL,
    client_id text NOT NULL,
    auth_method text DEFAULT ''::text NOT NULL,
    secret_sealed bytea DEFAULT '\x'::bytea NOT NULL,
    kek_version integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    provider_app_id text DEFAULT ''::text NOT NULL,
    stream_app_pk bigint,
    signing_secret_sealed bytea DEFAULT '\x'::bytea NOT NULL,
    signing_kek_version integer DEFAULT 0 NOT NULL,
    CONSTRAINT connector_oauth_clients_client_or_signing_secret CHECK (((client_id <> ''::text) OR (signing_secret_sealed <> '\x'::bytea))),
    CONSTRAINT connector_oauth_clients_secret CHECK ((((secret_sealed = '\x'::bytea) AND (kek_version = 0)) OR ((secret_sealed <> '\x'::bytea) AND (kek_version >= 1)))),
    CONSTRAINT connector_oauth_clients_signing_secret CHECK ((((signing_secret_sealed = '\x'::bytea) AND (signing_kek_version = 0)) OR ((signing_secret_sealed <> '\x'::bytea) AND (signing_kek_version >= 1)))),
    CONSTRAINT connector_oauth_clients_signing_secret_app CHECK (((signing_secret_sealed = '\x'::bytea) OR (provider_app_id <> ''::text)))
);

CREATE TABLE connector_tool_pins (
    connection_id text NOT NULL,
    tool_name text NOT NULL,
    schema_digest text NOT NULL,
    connected_at timestamp with time zone,
    pinned_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE contact_map (
    id text NOT NULL,
    customer_id text NOT NULL,
    config_id text CONSTRAINT contact_map_agent_config_id_not_null NOT NULL,
    kind text NOT NULL,
    address text NOT NULL,
    conversation_id text NOT NULL,
    user_id text,
    stream_app_pk bigint,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    thread text DEFAULT ''::text NOT NULL,
    CONSTRAINT contact_map_address_check CHECK ((address <> ''::text))
);

CREATE TABLE custom_models (
    id text NOT NULL,
    customer_id text NOT NULL,
    name text NOT NULL,
    base_url text NOT NULL,
    model text NOT NULL,
    api_key_sealed bytea,
    kek_version integer DEFAULT 0 NOT NULL,
    context_window bigint DEFAULT 0 NOT NULL,
    input_modalities text[] DEFAULT '{}'::text[] NOT NULL,
    per_million_input_tokens double precision DEFAULT 0 NOT NULL,
    per_million_output_tokens double precision DEFAULT 0 NOT NULL,
    trains_on_data text DEFAULT ''::text NOT NULL,
    retention text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE customer_activity (
    organization_id text DEFAULT ''::text NOT NULL,
    app_id text NOT NULL,
    area text NOT NULL,
    client text NOT NULL,
    day date NOT NULL,
    requests bigint DEFAULT 0 NOT NULL,
    refused bigint DEFAULT 0 NOT NULL,
    last_refusal_code text,
    last_refusal_at timestamp with time zone,
    first_at timestamp with time zone NOT NULL,
    last_at timestamp with time zone NOT NULL
);

CREATE TABLE data_move_cursors (
    customer_id text NOT NULL,
    source text NOT NULL,
    cursor bigint,
    read_at timestamp with time zone NOT NULL
);

CREATE TABLE deliveries_seen (
    scope text NOT NULL,
    key text NOT NULL,
    seen_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT deliveries_seen_key_check CHECK ((key <> ''::text))
);

CREATE TABLE dlc_review_logs (
    id text NOT NULL,
    use_case_id text NOT NULL,
    customer_id text NOT NULL,
    actor text NOT NULL,
    actor_name text DEFAULT ''::text NOT NULL,
    from_status text NOT NULL,
    to_status text NOT NULL,
    notes text DEFAULT ''::text NOT NULL,
    vendor_payload jsonb,
    created_at timestamp with time zone NOT NULL,
    submitted_at timestamp with time zone,
    approved_at timestamp with time zone
);

CREATE TABLE dlc_use_cases (
    id text NOT NULL,
    customer_id text NOT NULL,
    name text NOT NULL,
    is_default boolean DEFAULT false NOT NULL,
    status text NOT NULL,
    use_case_type text DEFAULT ''::text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    message_flow text DEFAULT ''::text NOT NULL,
    message_samples text[] DEFAULT '{}'::text[] NOT NULL,
    help_message text DEFAULT ''::text NOT NULL,
    opt_out_message text DEFAULT ''::text NOT NULL,
    opt_in_message text DEFAULT ''::text NOT NULL,
    embedded_links boolean DEFAULT false NOT NULL,
    embedded_phone boolean DEFAULT false NOT NULL,
    age_gated boolean DEFAULT false NOT NULL,
    direct_lending boolean DEFAULT false NOT NULL,
    channels jsonb DEFAULT '{}'::jsonb NOT NULL,
    vendor text DEFAULT ''::text NOT NULL,
    vendor_campaign_id text DEFAULT ''::text NOT NULL,
    vendor_status text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    submitted_at timestamp with time zone,
    approved_at timestamp with time zone
);

CREATE TABLE episodes (
    id text NOT NULL,
    customer_id text NOT NULL,
    contact_id text NOT NULL,
    source text NOT NULL,
    thread_channel text NOT NULL,
    call_id text,
    session_id text,
    card_message_id text NOT NULL,
    status text NOT NULL,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone,
    stream_app_pk bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_message_at timestamp with time zone,
    lease_until timestamp with time zone,
    leased_by text,
    CONSTRAINT episodes_check CHECK (((source = 'call'::text) = (session_id IS NOT NULL))),
    CONSTRAINT episodes_status_check CHECK ((status = ANY (ARRAY['in_progress'::text, 'ended'::text, 'summarized'::text, 'summary_failed'::text])))
);

CREATE TABLE image_jobs (
    id text NOT NULL,
    customer_id text NOT NULL,
    stream_app_pk bigint DEFAULT 0 CONSTRAINT image_jobs_stream_app_not_null NOT NULL,
    cid text NOT NULL,
    agent_user text NOT NULL,
    session_id text NOT NULL,
    turn_id text DEFAULT ''::text NOT NULL,
    tool_call_id text NOT NULL,
    request_key text NOT NULL,
    agent_id text DEFAULT ''::text NOT NULL,
    call_id text DEFAULT ''::text NOT NULL,
    region text DEFAULT ''::text NOT NULL,
    target text NOT NULL,
    tags jsonb,
    attribution jsonb NOT NULL,
    prompt text NOT NULL,
    aspect_ratio text DEFAULT ''::text NOT NULL,
    state text DEFAULT 'queued'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    draws integer DEFAULT 0 NOT NULL,
    released integer DEFAULT 0 NOT NULL,
    posts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone NOT NULL,
    drawn bytea,
    drawn_type text DEFAULT ''::text NOT NULL,
    picture jsonb,
    message text DEFAULT ''::text NOT NULL,
    failed boolean DEFAULT false NOT NULL,
    outcome text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT image_jobs_state_check CHECK ((state = ANY (ARRAY['queued'::text, 'done'::text, 'failed'::text])))
);

CREATE TABLE knowledge_documents (
    id text NOT NULL,
    customer_id text NOT NULL,
    namespace text NOT NULL,
    source text NOT NULL,
    passages integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    text text DEFAULT ''::text NOT NULL,
    state text DEFAULT 'indexed'::text NOT NULL,
    error text DEFAULT ''::text NOT NULL,
    pages integer DEFAULT 0 NOT NULL,
    content_type text DEFAULT ''::text NOT NULL,
    bytes bigint DEFAULT 0 NOT NULL,
    object_key text DEFAULT ''::text NOT NULL
);

CREATE TABLE knowledge_namespaces (
    customer_id text NOT NULL,
    namespace text NOT NULL,
    region text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    account text DEFAULT 'stream'::text NOT NULL,
    CONSTRAINT knowledge_namespaces_account_check CHECK ((account = ANY (ARRAY['stream'::text, 'customer'::text]))),
    CONSTRAINT knowledge_namespaces_region_check CHECK ((region ~ '^[a-z][a-z0-9-]{0,31}$'::text))
);

CREATE TABLE knowledge_urls (
    id text NOT NULL,
    customer_id text NOT NULL,
    namespace text NOT NULL,
    url text NOT NULL,
    title text DEFAULT ''::text NOT NULL,
    state text NOT NULL,
    error text DEFAULT ''::text NOT NULL,
    passages integer DEFAULT 0 NOT NULL,
    last_indexed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    declared_title text DEFAULT ''::text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    refresh_hours integer DEFAULT 0 NOT NULL
);

CREATE TABLE limit_grant_reconciles (
    shard text NOT NULL,
    started_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE limit_grant_uses (
    grant_id bigint NOT NULL,
    usage_id uuid NOT NULL,
    micros bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT limit_grant_uses_micros_check CHECK ((micros > 0))
);

CREATE TABLE limit_grants (
    id bigint NOT NULL,
    organization_id text NOT NULL,
    subject text NOT NULL,
    app_id text DEFAULT ''::text NOT NULL,
    api_key text DEFAULT ''::text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    category text DEFAULT ''::text NOT NULL,
    amount_micros bigint NOT NULL,
    expires_at timestamp with time zone,
    revoked_at timestamp with time zone,
    revoked_by text DEFAULT ''::text NOT NULL,
    reason text NOT NULL,
    actor text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    used_micros bigint DEFAULT 0 NOT NULL,
    CONSTRAINT limit_grants_amount_micros_check CHECK ((amount_micros > 0)),
    CONSTRAINT limit_grants_expiry_check CHECK (((expires_at IS NULL) OR (expires_at > created_at))),
    CONSTRAINT limit_grants_ids_check CHECK ((((app_id <> ''::text) = (subject = ANY (ARRAY['app'::text, 'api_key'::text, 'user'::text, 'app_category'::text]))) AND ((api_key <> ''::text) = (subject = 'api_key'::text)) AND ((user_id <> ''::text) = (subject = 'user'::text)) AND ((category <> ''::text) = (subject = ANY (ARRAY['category'::text, 'app_category'::text]))))),
    CONSTRAINT limit_grants_organization_id_check CHECK ((organization_id <> ''::text)),
    CONSTRAINT limit_grants_reason_check CHECK ((reason <> ''::text)),
    CONSTRAINT limit_grants_subject_check CHECK ((subject = ANY (ARRAY['organization'::text, 'app'::text, 'api_key'::text, 'user'::text, 'category'::text, 'app_category'::text]))),
    CONSTRAINT limit_grants_used_micros_check CHECK ((used_micros >= 0))
);

CREATE SEQUENCE limit_grants_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE limit_grants_id_seq OWNED BY limit_grants.id;

CREATE TABLE limits (
    id bigint NOT NULL,
    subject text NOT NULL,
    level text NOT NULL,
    organization_id text NOT NULL,
    app_id text DEFAULT ''::text NOT NULL,
    api_key text DEFAULT ''::text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    category text DEFAULT ''::text NOT NULL,
    "interval" text NOT NULL,
    limit_micros bigint NOT NULL,
    note text DEFAULT ''::text NOT NULL,
    updated_by text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT limits_ids_check CHECK ((((app_id <> ''::text) = (level = ANY (ARRAY['app'::text, 'api_key'::text, 'user'::text]))) AND ((api_key <> ''::text) = (level = 'api_key'::text)) AND ((user_id <> ''::text) = (level = 'user'::text)) AND ((category <> ''::text) = (subject = ANY (ARRAY['category'::text, 'app_category'::text]))))),
    CONSTRAINT limits_interval_check CHECK (("interval" = ANY (ARRAY['daily'::text, 'weekly'::text, 'biweekly'::text, 'monthly'::text]))),
    CONSTRAINT limits_level_check CHECK ((((subject = 'organization'::text) AND (level = 'organization'::text)) OR ((subject = 'app'::text) AND (level = 'app'::text)) OR ((subject = 'api_key'::text) AND (level = 'organization'::text)) OR ((subject = 'api_key'::text) AND (level = 'app'::text)) OR ((subject = 'api_key'::text) AND (level = 'api_key'::text)) OR ((subject = 'user'::text) AND (level = 'organization'::text)) OR ((subject = 'user'::text) AND (level = 'app'::text)) OR ((subject = 'user'::text) AND (level = 'user'::text)) OR ((subject = 'category'::text) AND (level = 'organization'::text)) OR ((subject = 'app_category'::text) AND (level = 'app'::text)))),
    CONSTRAINT limits_limit_micros_check CHECK ((limit_micros >= 0)),
    CONSTRAINT limits_organization_id_check CHECK ((organization_id <> ''::text))
);

CREATE SEQUENCE limits_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE limits_id_seq OWNED BY limits.id;

CREATE TABLE notices (
    organization_id text NOT NULL,
    kind text NOT NULL,
    dedupe_key text DEFAULT ''::text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    claimed_at timestamp with time zone DEFAULT now() NOT NULL,
    posted_at timestamp with time zone,
    CONSTRAINT notices_kind_check CHECK ((kind = ANY (ARRAY['no_credit'::text, 'spend_hard_limit'::text, 'spend_limit'::text, 'balance_low'::text, 'balance_exhausted'::text, 'cost_anomaly'::text]))),
    CONSTRAINT notices_organization_id_check CHECK ((organization_id <> ''::text)),
    CONSTRAINT notices_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text))
);

CREATE TABLE opt_outs (
    id text NOT NULL,
    customer_id text NOT NULL,
    recipient text NOT NULL,
    channel text NOT NULL,
    source text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    revoked_at timestamp with time zone
);

CREATE TABLE organization_tiers (
    organization_id text NOT NULL,
    tier text NOT NULL,
    seen_at timestamp with time zone NOT NULL,
    CONSTRAINT organization_tiers_tier_check CHECK ((tier <> ''::text))
);

CREATE TABLE organizations (
    id text NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE phone_numbers (
    id bigint NOT NULL,
    e164 text NOT NULL,
    vendor text NOT NULL,
    country text NOT NULL,
    capabilities text[] DEFAULT '{}'::text[] NOT NULL,
    monthly_cost_micros bigint DEFAULT 0 NOT NULL,
    customer_id text NOT NULL,
    tags jsonb DEFAULT '{}'::jsonb NOT NULL,
    vendor_id text,
    stream_trunk_id text,
    purchased_at timestamp with time zone DEFAULT now() NOT NULL,
    released_at timestamp with time zone,
    stream_call_id text,
    stream_call_type text,
    dlc_use_case_id text,
    stream_app_pk bigint,
    stream_route_id text,
    sip_trunk_id text,
    attached_at timestamp with time zone,
    config_id text
);

CREATE SEQUENCE phone_numbers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE phone_numbers_id_seq OWNED BY phone_numbers.id;

CREATE TABLE policies (
    scope text NOT NULL,
    scope_id text NOT NULL,
    document jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE price_changes (
    name text NOT NULL,
    kind text NOT NULL,
    reason text NOT NULL,
    source text NOT NULL,
    effective_at timestamp with time zone NOT NULL,
    created_by text NOT NULL,
    idempotency_key text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    cancelled_at timestamp with time zone,
    cancelled_by text,
    cancel_reason text,
    CONSTRAINT price_changes_kind_check CHECK ((kind = ANY (ARRAY['seed'::text, 'release'::text, 'api'::text])))
);

CREATE TABLE price_list_entries (
    id bigint CONSTRAINT price_entries_id_not_null NOT NULL,
    change text CONSTRAINT price_entries_change_not_null NOT NULL,
    modality text CONSTRAINT price_entries_modality_not_null NOT NULL,
    provider text CONSTRAINT price_entries_provider_not_null NOT NULL,
    model text CONSTRAINT price_entries_model_not_null NOT NULL,
    attributes jsonb DEFAULT '{}'::jsonb CONSTRAINT price_entries_attributes_not_null NOT NULL,
    free boolean DEFAULT false CONSTRAINT price_entries_free_not_null NOT NULL,
    rates jsonb DEFAULT '{}'::jsonb CONSTRAINT price_entries_rates_not_null NOT NULL,
    tiers jsonb DEFAULT '[]'::jsonb CONSTRAINT price_entries_tiers_not_null NOT NULL,
    withdrawn boolean DEFAULT false CONSTRAINT price_entries_withdrawn_not_null NOT NULL,
    effective_at timestamp with time zone CONSTRAINT price_entries_effective_at_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT price_entries_created_at_not_null NOT NULL,
    sku text DEFAULT ''::text CONSTRAINT price_entries_sku_not_null NOT NULL,
    prices jsonb DEFAULT '{}'::jsonb CONSTRAINT price_entries_prices_not_null NOT NULL,
    price_tiers jsonb DEFAULT '[]'::jsonb CONSTRAINT price_entries_price_tiers_not_null NOT NULL,
    region text NOT NULL,
    byok boolean DEFAULT false NOT NULL,
    CONSTRAINT price_entries_priced CHECK ((free OR withdrawn OR (rates = '{}'::jsonb) OR (prices <> '{}'::jsonb))),
    CONSTRAINT price_entries_sku CHECK ((sku <> ''::text)),
    CONSTRAINT price_list_entries_region CHECK ((region <> ''::text))
);

CREATE SEQUENCE price_entries_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE price_entries_id_seq OWNED BY price_list_entries.id;

CREATE TABLE provider_keys (
    customer_id text NOT NULL,
    provider text NOT NULL,
    region text NOT NULL,
    api_key_sealed bytea NOT NULL,
    kek_version integer NOT NULL,
    key_hint text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT provider_keys_provider_check CHECK ((provider ~ '^[a-z0-9][a-z0-9._-]{0,63}$'::text)),
    CONSTRAINT provider_keys_region_check CHECK ((region ~ '^[a-z][a-z0-9-]{0,31}$'::text))
);

CREATE TABLE router_configs (
    id text NOT NULL,
    customer_id text NOT NULL,
    name text NOT NULL,
    stt jsonb DEFAULT '{}'::jsonb NOT NULL,
    tts jsonb DEFAULT '{}'::jsonb NOT NULL,
    llm jsonb DEFAULT '{}'::jsonb NOT NULL,
    search jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    sts jsonb DEFAULT '{}'::jsonb NOT NULL
);

CREATE TABLE sandbox_recipients (
    customer_id text NOT NULL,
    recipient text NOT NULL,
    created_at timestamp with time zone NOT NULL
);

CREATE TABLE scheduled_task_runs (
    id text NOT NULL,
    task_id text NOT NULL,
    customer_id text NOT NULL,
    scheduled_for timestamp with time zone NOT NULL,
    trigger text DEFAULT 'schedule'::text NOT NULL,
    prompt text DEFAULT ''::text NOT NULL,
    state text DEFAULT 'running'::text NOT NULL,
    delivered_to text DEFAULT ''::text NOT NULL,
    session_id text DEFAULT ''::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    error text,
    postponed_until timestamp with time zone,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE scheduled_tasks (
    id text NOT NULL,
    customer_id text NOT NULL,
    config_id text DEFAULT ''::text NOT NULL,
    user_id text NOT NULL,
    caller_kind text DEFAULT ''::text NOT NULL,
    origin_session_id text DEFAULT ''::text NOT NULL,
    session_id text DEFAULT ''::text NOT NULL,
    conversation_id text DEFAULT ''::text NOT NULL,
    title text NOT NULL,
    instructions text NOT NULL,
    start_local timestamp without time zone NOT NULL,
    timezone text NOT NULL,
    rrule text,
    until_date date,
    next_run_at timestamp with time zone,
    state text DEFAULT 'active'::text NOT NULL,
    channels jsonb DEFAULT '["conversation"]'::jsonb NOT NULL,
    custom jsonb DEFAULT '{}'::jsonb NOT NULL,
    quiet_hours jsonb,
    lease_until timestamp with time zone,
    leased_by text DEFAULT ''::text CONSTRAINT scheduled_tasks_claim_id_not_null NOT NULL,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    created_by text DEFAULT 'api'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    channel_thread text DEFAULT ''::text NOT NULL,
    api_key text DEFAULT ''::text NOT NULL,
    usage_categories text[] DEFAULT '{}'::text[] NOT NULL,
    move_paused boolean DEFAULT false NOT NULL
);

CREATE TABLE simulation_cases (
    id text NOT NULL,
    run_id text NOT NULL,
    variation integer DEFAULT 0 NOT NULL,
    scenario text DEFAULT ''::text NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    call_id text,
    transcript jsonb DEFAULT '[]'::jsonb NOT NULL,
    turns integer DEFAULT 0 NOT NULL,
    passed boolean,
    verdict text,
    score integer,
    ended text DEFAULT ''::text NOT NULL,
    error text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone
);

CREATE TABLE simulation_runs (
    id text NOT NULL,
    customer_id text NOT NULL,
    simulation_id text NOT NULL,
    state text DEFAULT 'running'::text NOT NULL,
    cases integer DEFAULT 0 NOT NULL,
    passed integer DEFAULT 0 NOT NULL,
    failed integer DEFAULT 0 NOT NULL,
    mode text DEFAULT ''::text NOT NULL,
    config_id text DEFAULT ''::text NOT NULL,
    scenario text DEFAULT ''::text NOT NULL,
    assertion text DEFAULT ''::text NOT NULL,
    judge_target text DEFAULT ''::text NOT NULL,
    error text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    lease_until timestamp with time zone,
    stop_requested boolean DEFAULT false NOT NULL
);

CREATE TABLE simulations (
    id text NOT NULL,
    customer_id text NOT NULL,
    name text NOT NULL,
    mode text DEFAULT 'text'::text NOT NULL,
    config_id text DEFAULT ''::text NOT NULL,
    scenario text DEFAULT ''::text NOT NULL,
    assertion text DEFAULT ''::text NOT NULL,
    variations integer DEFAULT 1 NOT NULL,
    judge_target text DEFAULT ''::text NOT NULL,
    caller_target text DEFAULT ''::text NOT NULL,
    caller_tts text DEFAULT ''::text NOT NULL,
    caller_stt text DEFAULT ''::text NOT NULL,
    caller_voice text DEFAULT ''::text NOT NULL,
    max_turns integer DEFAULT 12 NOT NULL,
    tags jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone
);

CREATE TABLE sip_trunks (
    id text NOT NULL,
    customer_id text NOT NULL,
    name text NOT NULL,
    host text NOT NULL,
    port integer NOT NULL,
    transport text NOT NULL,
    username text NOT NULL,
    password_sealed bytea DEFAULT '\x'::bytea NOT NULL,
    password_kek_version integer DEFAULT 0 NOT NULL,
    late_offer boolean DEFAULT false NOT NULL,
    codecs text[] DEFAULT '{}'::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT sip_trunks_transport_check CHECK ((transport = ANY (ARRAY['udp'::text, 'tcp'::text, 'tls'::text])))
);

CREATE TABLE skills (
    id text NOT NULL,
    customer_id text NOT NULL,
    name text NOT NULL,
    description text NOT NULL,
    instructions text NOT NULL,
    deadline_ms bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    config_id text DEFAULT ''::text NOT NULL,
    capture_video boolean DEFAULT false NOT NULL,
    files jsonb DEFAULT '{}'::jsonb NOT NULL
);

CREATE TABLE stream_app_tokens (
    stream_app_pk bigint NOT NULL,
    customer_id text NOT NULL,
    api_key text NOT NULL,
    token_sealed bytea NOT NULL,
    kek_version integer NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE stream_fallback_uses (
    customer_id text NOT NULL,
    first_at timestamp with time zone NOT NULL,
    last_at timestamp with time zone NOT NULL,
    uses bigint NOT NULL
);

CREATE TABLE stream_trunks (
    id text NOT NULL,
    customer_id text NOT NULL,
    stream_app_pk bigint,
    vendor text NOT NULL,
    bridge_uri text NOT NULL,
    bridge_username text NOT NULL,
    bridge_password_sealed bytea NOT NULL,
    bridge_password_kek_version integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    own boolean DEFAULT false NOT NULL
);

CREATE TABLE telephony_suspensions (
    customer_id text NOT NULL,
    reason text NOT NULL,
    suspended_by text NOT NULL,
    suspended_at timestamp with time zone NOT NULL
);

CREATE TABLE usage_resets (
    id bigint NOT NULL,
    organization_id text NOT NULL,
    subject text NOT NULL,
    app_id text DEFAULT ''::text NOT NULL,
    api_key text DEFAULT ''::text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    category text DEFAULT ''::text NOT NULL,
    "interval" text NOT NULL,
    window_start timestamp with time zone NOT NULL,
    baseline_micros bigint NOT NULL,
    reason text NOT NULL,
    actor text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT usage_resets_baseline_micros_check CHECK ((baseline_micros >= 0)),
    CONSTRAINT usage_resets_organization_id_check CHECK ((organization_id <> ''::text))
);

CREATE SEQUENCE usage_resets_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE usage_resets_id_seq OWNED BY usage_resets.id;

CREATE TABLE user_phone_history (
    customer_id text NOT NULL,
    stream_app_pk bigint NOT NULL,
    user_id text NOT NULL,
    linked_at timestamp with time zone NOT NULL,
    since timestamp with time zone
);

CREATE TABLE user_phones (
    customer_id text NOT NULL,
    stream_app_pk bigint NOT NULL,
    user_id text NOT NULL,
    phone text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT user_phones_phone_check CHECK ((phone <> ''::text)),
    CONSTRAINT user_phones_user_id_check CHECK ((user_id <> ''::text))
);

CREATE TABLE users (
    customer_id text NOT NULL,
    id text NOT NULL,
    kind text NOT NULL,
    name text DEFAULT ''::text NOT NULL,
    custom jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    claimed_by text,
    claimed_at timestamp with time zone,
    timezone text,
    email text,
    minted boolean DEFAULT false NOT NULL,
    refresh_secret_hash bytea
);

CREATE TABLE voice_bindings (
    id text NOT NULL,
    voice_id text NOT NULL,
    provider text NOT NULL,
    external_id text DEFAULT ''::text NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    error text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    synced_at timestamp with time zone
);

CREATE TABLE voice_samples (
    id text NOT NULL,
    voice_id text NOT NULL,
    object_key text NOT NULL,
    content_type text DEFAULT ''::text NOT NULL,
    bytes bigint DEFAULT 0 NOT NULL,
    transcript text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE voices (
    id text NOT NULL,
    customer_id text NOT NULL,
    name text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone
);

ALTER TABLE ONLY agent_logs ALTER COLUMN id SET DEFAULT nextval('agent_logs_id_seq'::regclass);

ALTER TABLE ONLY campaign_contacts ALTER COLUMN seq SET DEFAULT nextval('campaign_contacts_seq_seq'::regclass);

ALTER TABLE ONLY limit_grants ALTER COLUMN id SET DEFAULT nextval('limit_grants_id_seq'::regclass);

ALTER TABLE ONLY limits ALTER COLUMN id SET DEFAULT nextval('limits_id_seq'::regclass);

ALTER TABLE ONLY phone_numbers ALTER COLUMN id SET DEFAULT nextval('phone_numbers_id_seq'::regclass);

ALTER TABLE ONLY price_list_entries ALTER COLUMN id SET DEFAULT nextval('price_entries_id_seq'::regclass);

ALTER TABLE ONLY usage_resets ALTER COLUMN id SET DEFAULT nextval('usage_resets_id_seq'::regclass);

ALTER TABLE audit_log
    ADD CONSTRAINT audit_log_action_check CHECK ((action = ANY (ARRAY['created'::text, 'updated'::text, 'deleted'::text, 'synced'::text, 'cancelled'::text]))) NOT VALID;

ALTER TABLE audit_log
    ADD CONSTRAINT audit_log_resource_type_check CHECK ((resource_type = ANY (ARRAY['agent_config'::text, 'skill'::text, 'knowledge'::text, 'knowledge_url'::text, 'router_config'::text, 'plugin'::text, 'policy'::text, 'price_list'::text, 'end_user'::text, 'limit'::text, 'provider_key'::text]))) NOT VALID;

ALTER TABLE audit_log
    ADD CONSTRAINT audit_log_source_check CHECK ((source = ANY (ARRAY['dashboard'::text, 'cli'::text, 'sdk'::text, 'api'::text, 'manage'::text]))) NOT VALID;

ALTER TABLE connector_audit
    ADD CONSTRAINT connector_audit_action_check CHECK ((action = ANY (ARRAY['grant_created'::text, 'grant_refreshed'::text, 'grant_revoked'::text, 'proxy_call'::text, 'token_export'::text, 'tool_reapproved'::text]))) NOT VALID;

ALTER TABLE contact_map
    ADD CONSTRAINT contact_map_kind_check CHECK ((kind = ANY (ARRAY['phone'::text, 'slack'::text, 'stream_user'::text, 'whatsapp'::text, 'sms'::text, 'imessage'::text]))) NOT VALID;

ALTER TABLE episodes
    ADD CONSTRAINT episodes_source_check CHECK ((source = ANY (ARRAY['call'::text, 'sms'::text, 'whatsapp'::text, 'slack'::text, 'imessage'::text, 'chat'::text]))) NOT VALID;

ALTER TABLE knowledge_documents
    ADD CONSTRAINT knowledge_documents_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'indexed'::text, 'failed'::text]))) NOT VALID;

ALTER TABLE policies
    ADD CONSTRAINT policies_scope_check CHECK ((scope = ANY (ARRAY['organization'::text, 'app'::text, 'user'::text]))) NOT VALID;

ALTER TABLE ONLY agent_configs
    ADD CONSTRAINT agent_configs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY agent_logs
    ADD CONSTRAINT agent_logs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);

ALTER TABLE ONLY app_organizations
    ADD CONSTRAINT app_organizations_pkey PRIMARY KEY (app_id);

ALTER TABLE ONLY apps
    ADD CONSTRAINT apps_pkey PRIMARY KEY (id);

ALTER TABLE ONLY audit_log
    ADD CONSTRAINT audit_log_pkey PRIMARY KEY (id);

ALTER TABLE ONLY business_profiles
    ADD CONSTRAINT business_profiles_pkey PRIMARY KEY (customer_id);

ALTER TABLE ONLY campaign_attempts
    ADD CONSTRAINT campaign_attempts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY campaign_contacts
    ADD CONSTRAINT campaign_contacts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY campaigns
    ADD CONSTRAINT campaigns_pkey PRIMARY KEY (id);

ALTER TABLE ONLY channel_accounts
    ADD CONSTRAINT channel_accounts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY channel_deliveries
    ADD CONSTRAINT channel_deliveries_pkey PRIMARY KEY (account_id, message_id);

ALTER TABLE ONLY channel_links
    ADD CONSTRAINT channel_links_pkey PRIMARY KEY (code);

ALTER TABLE ONLY channel_thread_inbound
    ADD CONSTRAINT channel_thread_inbound_channel_id_provider_message_id_key UNIQUE (channel_id, provider_message_id);

ALTER TABLE ONLY channel_thread_inbound
    ADD CONSTRAINT channel_thread_inbound_pkey PRIMARY KEY (seq);

ALTER TABLE ONLY channel_thread_members
    ADD CONSTRAINT channel_thread_members_pkey PRIMARY KEY (channel_id, member);

ALTER TABLE ONLY channel_thread_messages
    ADD CONSTRAINT channel_thread_messages_pkey PRIMARY KEY (channel_id, kind, message_id);

ALTER TABLE ONLY channel_thread_waiting
    ADD CONSTRAINT channel_thread_waiting_pkey PRIMARY KEY (customer_id, connector_id, provider_unit_id, thread_key, provider_message_id);

ALTER TABLE ONLY channel_threads
    ADD CONSTRAINT channel_threads_pkey PRIMARY KEY (channel_id);

ALTER TABLE ONLY channel_turns
    ADD CONSTRAINT channel_turns_pkey PRIMARY KEY (stream_app_pk, cid);

ALTER TABLE ONLY chat_inbound
    ADD CONSTRAINT chat_inbound_pkey PRIMARY KEY (seq);

ALTER TABLE ONLY chat_inbound
    ADD CONSTRAINT chat_inbound_stream_app_pk_message_id_key UNIQUE (stream_app_pk, message_id);

ALTER TABLE ONLY connection_event_subscriptions
    ADD CONSTRAINT connection_event_subscriptions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY connector_audit
    ADD CONSTRAINT connector_audit_pkey PRIMARY KEY (id);

ALTER TABLE ONLY connector_authorization_attempts
    ADD CONSTRAINT connector_authorization_attempts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY connector_authorization_attempts
    ADD CONSTRAINT connector_authorization_attempts_state_hash_key UNIQUE (state_hash);

ALTER TABLE ONLY connector_broken_revisions
    ADD CONSTRAINT connector_broken_revisions_pkey PRIMARY KEY (connector_id, revision);

ALTER TABLE ONLY connector_config_tokens
    ADD CONSTRAINT connector_config_tokens_pkey PRIMARY KEY (customer_id, connector_id);

ALTER TABLE ONLY connector_connections
    ADD CONSTRAINT connector_connections_pkey PRIMARY KEY (id);

ALTER TABLE ONLY connector_definitions
    ADD CONSTRAINT connector_definitions_pkey PRIMARY KEY (customer_id, id, revision);

ALTER TABLE ONLY connector_event_deliveries
    ADD CONSTRAINT connector_event_deliveries_pkey PRIMARY KEY (destination_id, id);

ALTER TABLE ONLY connector_event_destinations
    ADD CONSTRAINT connector_event_destinations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY connector_invocations
    ADD CONSTRAINT connector_invocations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY connector_oauth_clients
    ADD CONSTRAINT connector_oauth_clients_pkey PRIMARY KEY (customer_id, connector_id);

ALTER TABLE ONLY connector_tool_pins
    ADD CONSTRAINT connector_tool_pins_pkey PRIMARY KEY (connection_id, tool_name);

ALTER TABLE ONLY contact_map
    ADD CONSTRAINT contact_map_pkey PRIMARY KEY (id);

ALTER TABLE ONLY custom_models
    ADD CONSTRAINT custom_models_pkey PRIMARY KEY (id);

ALTER TABLE ONLY customer_activity
    ADD CONSTRAINT customer_activity_pkey PRIMARY KEY (app_id, area, client, day);

ALTER TABLE ONLY data_move_cursors
    ADD CONSTRAINT data_move_cursors_pkey PRIMARY KEY (customer_id, source);

ALTER TABLE ONLY dlc_review_logs
    ADD CONSTRAINT dlc_review_logs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY dlc_use_cases
    ADD CONSTRAINT dlc_use_cases_pkey PRIMARY KEY (id);

ALTER TABLE ONLY episodes
    ADD CONSTRAINT episodes_pkey PRIMARY KEY (id);

ALTER TABLE ONLY image_jobs
    ADD CONSTRAINT image_jobs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY image_jobs
    ADD CONSTRAINT image_jobs_request_key_key UNIQUE (request_key);

ALTER TABLE ONLY knowledge_documents
    ADD CONSTRAINT knowledge_documents_pkey PRIMARY KEY (id);

ALTER TABLE ONLY knowledge_namespaces
    ADD CONSTRAINT knowledge_namespaces_pkey PRIMARY KEY (customer_id, namespace);

ALTER TABLE ONLY knowledge_urls
    ADD CONSTRAINT knowledge_urls_pkey PRIMARY KEY (id);

ALTER TABLE ONLY limit_grant_reconciles
    ADD CONSTRAINT limit_grant_reconciles_pkey PRIMARY KEY (shard);

ALTER TABLE ONLY limit_grant_uses
    ADD CONSTRAINT limit_grant_uses_pkey PRIMARY KEY (grant_id, usage_id);

ALTER TABLE ONLY limit_grants
    ADD CONSTRAINT limit_grants_pkey PRIMARY KEY (id);

ALTER TABLE ONLY limits
    ADD CONSTRAINT limits_pkey PRIMARY KEY (id);

ALTER TABLE ONLY opt_outs
    ADD CONSTRAINT opt_outs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY organization_tiers
    ADD CONSTRAINT organization_tiers_pkey PRIMARY KEY (organization_id);

ALTER TABLE ONLY organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY phone_numbers
    ADD CONSTRAINT phone_numbers_pkey PRIMARY KEY (id);

ALTER TABLE ONLY policies
    ADD CONSTRAINT policies_pkey PRIMARY KEY (scope, scope_id);

ALTER TABLE ONLY price_changes
    ADD CONSTRAINT price_changes_idempotency_key_key UNIQUE (idempotency_key);

ALTER TABLE ONLY price_changes
    ADD CONSTRAINT price_changes_pkey PRIMARY KEY (name);

ALTER TABLE ONLY price_list_entries
    ADD CONSTRAINT price_entries_pkey PRIMARY KEY (id);

ALTER TABLE ONLY provider_keys
    ADD CONSTRAINT provider_keys_pkey PRIMARY KEY (customer_id, provider, region);

ALTER TABLE ONLY router_configs
    ADD CONSTRAINT router_configs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY sandbox_recipients
    ADD CONSTRAINT sandbox_recipients_pkey PRIMARY KEY (customer_id, recipient);

ALTER TABLE ONLY scheduled_task_runs
    ADD CONSTRAINT scheduled_task_runs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY scheduled_tasks
    ADD CONSTRAINT scheduled_tasks_pkey PRIMARY KEY (customer_id, id);

ALTER TABLE ONLY simulation_cases
    ADD CONSTRAINT simulation_cases_pkey PRIMARY KEY (id);

ALTER TABLE ONLY simulation_runs
    ADD CONSTRAINT simulation_runs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY simulations
    ADD CONSTRAINT simulations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY sip_trunks
    ADD CONSTRAINT sip_trunks_customer_id_id_key UNIQUE (customer_id, id);

ALTER TABLE ONLY sip_trunks
    ADD CONSTRAINT sip_trunks_pkey PRIMARY KEY (id);

ALTER TABLE ONLY skills
    ADD CONSTRAINT skills_pkey PRIMARY KEY (id);

ALTER TABLE ONLY stream_app_tokens
    ADD CONSTRAINT stream_app_tokens_pkey PRIMARY KEY (stream_app_pk);

ALTER TABLE ONLY stream_fallback_uses
    ADD CONSTRAINT stream_fallback_uses_pkey PRIMARY KEY (customer_id);

ALTER TABLE ONLY stream_trunks
    ADD CONSTRAINT stream_trunks_pkey PRIMARY KEY (id);

ALTER TABLE ONLY telephony_suspensions
    ADD CONSTRAINT telephony_suspensions_pkey PRIMARY KEY (customer_id);

ALTER TABLE ONLY usage_resets
    ADD CONSTRAINT usage_resets_pkey PRIMARY KEY (id);

ALTER TABLE ONLY user_phone_history
    ADD CONSTRAINT user_phone_history_pkey PRIMARY KEY (customer_id, stream_app_pk, user_id);

ALTER TABLE ONLY user_phones
    ADD CONSTRAINT user_phones_pkey PRIMARY KEY (customer_id, stream_app_pk, user_id);

ALTER TABLE ONLY users
    ADD CONSTRAINT users_pkey PRIMARY KEY (customer_id, id);

ALTER TABLE ONLY voice_bindings
    ADD CONSTRAINT voice_bindings_pkey PRIMARY KEY (id);

ALTER TABLE ONLY voice_samples
    ADD CONSTRAINT voice_samples_pkey PRIMARY KEY (id);

ALTER TABLE ONLY voices
    ADD CONSTRAINT voices_pkey PRIMARY KEY (id);

CREATE INDEX agent_configs_customer_idx ON agent_configs USING btree (customer_id, created_at DESC);

CREATE UNIQUE INDEX agent_configs_name_idx ON agent_configs USING btree (customer_id, name) WHERE (deleted_at IS NULL);

CREATE UNIQUE INDEX agent_configs_role_idx ON agent_configs USING btree (customer_id, role) WHERE ((role <> ''::text) AND (deleted_at IS NULL) AND (COALESCE((tags ->> 'draft_of'::text), ''::text) = ''::text));

CREATE INDEX agent_logs_config_order ON agent_logs USING btree (customer_id, config_id, id DESC);

CREATE INDEX agent_logs_customer_order ON agent_logs USING btree (customer_id, id DESC);

CREATE INDEX agent_logs_error_order ON agent_logs USING btree (customer_id, severity, id DESC);

CREATE UNIQUE INDEX agent_logs_source_idx ON agent_logs USING btree (customer_id, source_id) WHERE (source_id IS NOT NULL);

CREATE INDEX api_keys_app_idx ON api_keys USING btree (app_id, created_at DESC);

CREATE INDEX app_organizations_organization_idx ON app_organizations USING btree (organization_id);

CREATE INDEX apps_organization_idx ON apps USING btree (organization_id, created_at DESC);

CREATE INDEX audit_log_agent_idx ON audit_log USING btree (customer_id, agent_id, created_at DESC, id DESC);

CREATE INDEX audit_log_customer_idx ON audit_log USING btree (customer_id, created_at DESC, id DESC);

CREATE INDEX audit_log_resource_idx ON audit_log USING btree (customer_id, resource_type, created_at DESC, id DESC);

CREATE INDEX campaign_attempts_classify_idx ON campaign_attempts USING btree (classify_after) WHERE (classify_state = 'pending'::text);

CREATE INDEX campaign_attempts_contact_idx ON campaign_attempts USING btree (contact_id, started_at);

CREATE INDEX campaign_contacts_campaign_idx ON campaign_contacts USING btree (campaign_id, seq);

CREATE INDEX campaign_contacts_due_idx ON campaign_contacts USING btree (campaign_id, stage, next_attempt_at);

CREATE INDEX campaign_contacts_lease_idx ON campaign_contacts USING btree (lease_until) WHERE (stage = 'in_progress'::text);

CREATE UNIQUE INDEX campaign_contacts_number_idx ON campaign_contacts USING btree (campaign_id, to_number) WHERE (reason IS DISTINCT FROM 'duplicate'::text);

CREATE INDEX campaigns_customer_idx ON campaigns USING btree (customer_id, created_at DESC);

CREATE INDEX campaigns_tags_idx ON campaigns USING gin (tags);

CREATE UNIQUE INDEX channel_accounts_line_idx ON channel_accounts USING btree (customer_id, kind, e164) WHERE (deleted_at IS NULL);

CREATE UNIQUE INDEX channel_accounts_token_idx ON channel_accounts USING btree (token);

CREATE INDEX channel_deliveries_from_idx ON channel_deliveries USING btree (account_id, from_address, received_at DESC);

CREATE INDEX channel_thread_inbound_finished ON channel_thread_inbound USING btree (finished_at) WHERE (state <> 'queued'::text);

CREATE INDEX channel_thread_inbound_queued ON channel_thread_inbound USING btree (channel_id, seq) WHERE (state = 'queued'::text);

CREATE INDEX channel_thread_waiting_created ON channel_thread_waiting USING btree (created_at);

CREATE UNIQUE INDEX channel_threads_thread ON channel_threads USING btree (customer_id, connector_id, provider_unit_id, thread_key);

CREATE INDEX chat_inbound_finished ON chat_inbound USING btree (finished_at) WHERE (state <> 'queued'::text);

CREATE INDEX chat_inbound_queued ON chat_inbound USING btree (next_attempt_at, seq) WHERE (state = 'queued'::text);

CREATE INDEX connection_event_subscriptions_due ON connection_event_subscriptions USING btree (next_attempt_at) WHERE (next_attempt_at IS NOT NULL);

CREATE UNIQUE INDEX connection_event_subscriptions_one ON connection_event_subscriptions USING btree (connection_id, config_id, binding, key);

CREATE UNIQUE INDEX connection_event_subscriptions_token ON connection_event_subscriptions USING btree (token);

CREATE INDEX connector_audit_connection_idx ON connector_audit USING btree (customer_id, connection_id, created_at DESC, id DESC);

CREATE INDEX connector_audit_customer_idx ON connector_audit USING btree (customer_id, created_at DESC, id DESC);

CREATE INDEX connector_authorization_attempts_expiry_idx ON connector_authorization_attempts USING btree (customer_id, expires_at);

CREATE INDEX connector_connections_owner_idx ON connector_connections USING btree (customer_id, owner_type, owner_id, created_at DESC, id DESC) WHERE (deleted_at IS NULL);

CREATE UNIQUE INDEX connector_connections_provider_unit_idx ON connector_connections USING btree (connector_id, provider_unit_id) WHERE ((deleted_at IS NULL) AND (provider_unit_id IS NOT NULL));

CREATE INDEX connector_event_deliveries_due ON connector_event_deliveries USING btree (next_attempt_at);

CREATE INDEX connector_event_destinations_list ON connector_event_destinations USING btree (customer_id, connector_id, created_at DESC, id DESC);

CREATE INDEX connector_invocations_connection_idx ON connector_invocations USING btree (customer_id, connection_id, started_at DESC, id DESC);

CREATE UNIQUE INDEX connector_oauth_clients_provider_app ON connector_oauth_clients USING btree (connector_id, provider_app_id) WHERE (provider_app_id <> ''::text);

CREATE UNIQUE INDEX contact_map_address ON contact_map USING btree (customer_id, config_id, kind, address);

CREATE INDEX contact_map_conversation ON contact_map USING btree (customer_id, config_id, conversation_id);

CREATE INDEX contact_map_retired ON contact_map USING btree (customer_id, regexp_replace(address, '^retired:[^:]*:'::text, ''::text)) WHERE (address ~~ 'retired:%'::text);

CREATE INDEX custom_models_customer_idx ON custom_models USING btree (customer_id, created_at DESC, id DESC);

CREATE UNIQUE INDEX custom_models_name_idx ON custom_models USING btree (customer_id, name);

CREATE INDEX customer_activity_organization_day ON customer_activity USING btree (organization_id, day);

CREATE INDEX dlc_review_logs_use_case_idx ON dlc_review_logs USING btree (use_case_id, created_at, id);

CREATE INDEX dlc_use_cases_customer_idx ON dlc_use_cases USING btree (customer_id, created_at DESC, id DESC);

CREATE UNIQUE INDEX dlc_use_cases_default_idx ON dlc_use_cases USING btree (customer_id) WHERE is_default;

CREATE INDEX dlc_use_cases_status_idx ON dlc_use_cases USING btree (status, updated_at, id);

CREATE UNIQUE INDEX episodes_call_session ON episodes USING btree (session_id);

CREATE INDEX episodes_contact ON episodes USING btree (contact_id, started_at DESC);

CREATE INDEX episodes_open_call ON episodes USING btree (call_id) WHERE ((status = 'in_progress'::text) AND (session_id IS NOT NULL));

CREATE UNIQUE INDEX episodes_open_thread ON episodes USING btree (customer_id, thread_channel) WHERE ((status = 'in_progress'::text) AND (session_id IS NULL));

CREATE INDEX episodes_thread ON episodes USING btree (customer_id, thread_channel, started_at);

CREATE UNIQUE INDEX knowledge_documents_source_idx ON knowledge_documents USING btree (customer_id, namespace, source);

CREATE INDEX knowledge_urls_namespace_idx ON knowledge_urls USING btree (customer_id, namespace) WHERE (deleted_at IS NULL);

CREATE UNIQUE INDEX knowledge_urls_url_idx ON knowledge_urls USING btree (customer_id, namespace, url) WHERE (deleted_at IS NULL);

CREATE INDEX limit_grant_uses_usage_idx ON limit_grant_uses USING btree (usage_id);

CREATE INDEX limit_grants_live_idx ON limit_grants USING btree (organization_id) WHERE (revoked_at IS NULL);

CREATE UNIQUE INDEX limits_one_per_level ON limits USING btree (organization_id, subject, level, "interval", app_id, api_key, user_id, category);

CREATE INDEX opt_outs_customer_idx ON opt_outs USING btree (customer_id, created_at DESC, id DESC);

CREATE UNIQUE INDEX opt_outs_recipient_idx ON opt_outs USING btree (customer_id, recipient, channel) WHERE (revoked_at IS NULL);

CREATE INDEX phone_numbers_call_idx ON phone_numbers USING btree (stream_call_type, stream_call_id) WHERE (released_at IS NULL);

CREATE INDEX phone_numbers_customer_idx ON phone_numbers USING btree (customer_id, purchased_at DESC);

CREATE UNIQUE INDEX phone_numbers_held_idx ON phone_numbers USING btree (vendor, e164) WHERE ((released_at IS NULL) AND (vendor <> 'sip_trunk'::text));

CREATE INDEX phone_numbers_tags_idx ON phone_numbers USING gin (tags);

CREATE UNIQUE INDEX phone_numbers_trunk_held_idx ON phone_numbers USING btree (customer_id, e164) WHERE ((released_at IS NULL) AND (vendor = 'sip_trunk'::text));

CREATE INDEX policies_user_scope_idx ON policies USING btree (scope_id COLLATE "C") WHERE (scope = 'user'::text);

CREATE INDEX price_entries_change_idx ON price_list_entries USING btree (change);

CREATE INDEX price_entries_sku_idx ON price_list_entries USING btree (sku);

CREATE INDEX price_list_entries_item_idx ON price_list_entries USING btree (modality, provider, model, region, byok, effective_at DESC);

CREATE INDEX router_configs_customer_idx ON router_configs USING btree (customer_id, created_at DESC);

CREATE UNIQUE INDEX router_configs_name_idx ON router_configs USING btree (customer_id, name) WHERE (deleted_at IS NULL);

CREATE INDEX scheduled_task_runs_finished_idx ON scheduled_task_runs USING btree (finished_at);

CREATE UNIQUE INDEX scheduled_task_runs_occurrence_idx ON scheduled_task_runs USING btree (customer_id, task_id, scheduled_for) WHERE (trigger = 'schedule'::text);

CREATE INDEX scheduled_task_runs_task_idx ON scheduled_task_runs USING btree (customer_id, task_id, created_at DESC, id DESC);

CREATE INDEX scheduled_tasks_due_idx ON scheduled_tasks USING btree (next_run_at) WHERE (state = 'active'::text);

CREATE INDEX scheduled_tasks_origin_session_idx ON scheduled_tasks USING btree (customer_id, origin_session_id) WHERE (origin_session_id <> ''::text);

CREATE INDEX scheduled_tasks_pruned_idx ON scheduled_tasks USING btree (updated_at) WHERE (state = ANY (ARRAY['completed'::text, 'failed'::text]));

CREATE INDEX scheduled_tasks_session_idx ON scheduled_tasks USING btree (customer_id, session_id) WHERE (session_id <> ''::text);

CREATE INDEX scheduled_tasks_user_idx ON scheduled_tasks USING btree (customer_id, user_id, created_at DESC, id DESC);

CREATE INDEX scheduled_tasks_wake_idx ON scheduled_tasks USING btree (GREATEST(next_run_at, lease_until)) WHERE ((state = 'active'::text) AND (next_run_at IS NOT NULL));

CREATE INDEX simulation_cases_run_idx ON simulation_cases USING btree (run_id, variation);

CREATE INDEX simulation_runs_customer_idx ON simulation_runs USING btree (customer_id, started_at DESC);

CREATE INDEX simulation_runs_lease_idx ON simulation_runs USING btree (lease_until) WHERE (state = 'running'::text);

CREATE INDEX simulation_runs_simulation_idx ON simulation_runs USING btree (simulation_id, started_at DESC);

CREATE INDEX simulations_customer_idx ON simulations USING btree (customer_id, created_at DESC) WHERE (deleted_at IS NULL);

CREATE INDEX sip_trunks_customer_idx ON sip_trunks USING btree (customer_id, created_at DESC);

CREATE UNIQUE INDEX skills_name_idx ON skills USING btree (customer_id, config_id, name) WHERE (deleted_at IS NULL);

CREATE INDEX stream_trunks_group_idx ON stream_trunks USING btree (customer_id, stream_app_pk, vendor);

CREATE INDEX usage_resets_organization_idx ON usage_resets USING btree (organization_id, window_start);

CREATE UNIQUE INDEX user_phones_phone ON user_phones USING btree (customer_id, stream_app_pk, phone);

CREATE INDEX users_claimed_idx ON users USING btree (customer_id, claimed_by) WHERE (claimed_by IS NOT NULL);

CREATE INDEX users_customer_idx ON users USING btree (customer_id, created_at DESC);

CREATE UNIQUE INDEX voice_bindings_provider_idx ON voice_bindings USING btree (voice_id, provider);

CREATE INDEX voice_samples_voice_idx ON voice_samples USING btree (voice_id, created_at);

CREATE INDEX voices_customer_idx ON voices USING btree (customer_id, created_at DESC);

CREATE UNIQUE INDEX voices_name_idx ON voices USING btree (customer_id, name) WHERE (deleted_at IS NULL);

ALTER TABLE ONLY notices
    ADD CONSTRAINT notices_pkey PRIMARY KEY (organization_id, kind, dedupe_key);

CREATE INDEX notices_unposted ON notices USING btree (claimed_at) WHERE (posted_at IS NULL);

CREATE INDEX knowledge_urls_due_idx ON knowledge_urls USING btree (updated_at) WHERE ((deleted_at IS NULL) AND (refresh_hours > 0) AND (state <> 'pending'::text));

ALTER TABLE ONLY deliveries_seen
    ADD CONSTRAINT deliveries_seen_pkey PRIMARY KEY (scope, key);

CREATE INDEX deliveries_seen_seen_at ON deliveries_seen USING btree (seen_at);

CREATE INDEX episodes_lease ON episodes USING btree (lease_until) WHERE (lease_until IS NOT NULL);

CREATE INDEX phone_numbers_trunk_idx ON phone_numbers USING btree (customer_id, stream_trunk_id) WHERE (released_at IS NULL);

CREATE INDEX dlc_use_cases_vendor_campaign_idx ON dlc_use_cases USING btree (vendor, vendor_campaign_id) WHERE (vendor_campaign_id <> ''::text);

CREATE INDEX image_jobs_undecided ON image_jobs USING btree (next_attempt_at, id) WHERE ((state = 'queued'::text) AND (message = ''::text));

CREATE INDEX image_jobs_decided ON image_jobs USING btree (next_attempt_at, id) WHERE ((state = 'queued'::text) AND (message <> ''::text));

ALTER TABLE ONLY channel_link_failures
    ADD CONSTRAINT channel_link_failures_pkey PRIMARY KEY (account_id, from_address, window_start);

CREATE INDEX channel_link_failures_window_idx ON channel_link_failures USING btree (window_start);

CREATE INDEX voice_bindings_external_idx ON voice_bindings USING btree (provider, external_id);

ALTER TABLE ONLY activity_digests
    ADD CONSTRAINT activity_digests_pkey PRIMARY KEY (day);

CREATE TRIGGER price_changes_guard BEFORE DELETE OR UPDATE ON price_changes FOR EACH ROW EXECUTE FUNCTION price_changes_guard();

CREATE TRIGGER price_entries_guard BEFORE INSERT OR DELETE OR UPDATE ON price_list_entries FOR EACH ROW EXECUTE FUNCTION price_entries_guard();

ALTER TABLE ONLY api_keys
    ADD CONSTRAINT api_keys_app_id_fkey FOREIGN KEY (app_id) REFERENCES apps(id) ON DELETE CASCADE;

ALTER TABLE ONLY apps
    ADD CONSTRAINT apps_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE CASCADE;

ALTER TABLE ONLY campaign_attempts
    ADD CONSTRAINT campaign_attempts_campaign_id_fkey FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE;

ALTER TABLE ONLY campaign_attempts
    ADD CONSTRAINT campaign_attempts_contact_id_fkey FOREIGN KEY (contact_id) REFERENCES campaign_contacts(id) ON DELETE CASCADE;

ALTER TABLE ONLY campaign_contacts
    ADD CONSTRAINT campaign_contacts_campaign_id_fkey FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE;

ALTER TABLE ONLY channel_thread_inbound
    ADD CONSTRAINT channel_thread_inbound_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES channel_threads(channel_id) ON DELETE CASCADE;

ALTER TABLE ONLY channel_thread_members
    ADD CONSTRAINT channel_thread_members_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES channel_threads(channel_id) ON DELETE CASCADE;

ALTER TABLE ONLY channel_thread_messages
    ADD CONSTRAINT channel_thread_messages_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES channel_threads(channel_id);

ALTER TABLE ONLY connector_authorization_attempts
    ADD CONSTRAINT connector_authorization_attempts_connection_id_fkey FOREIGN KEY (connection_id) REFERENCES connector_connections(id);

ALTER TABLE ONLY connector_event_deliveries
    ADD CONSTRAINT connector_event_deliveries_destination_id_fkey FOREIGN KEY (destination_id) REFERENCES connector_event_destinations(id) ON DELETE CASCADE;

ALTER TABLE ONLY connector_invocations
    ADD CONSTRAINT connector_invocations_connection_id_fkey FOREIGN KEY (connection_id) REFERENCES connector_connections(id) ON DELETE CASCADE;

ALTER TABLE ONLY connector_tool_pins
    ADD CONSTRAINT connector_tool_pins_connection_id_fkey FOREIGN KEY (connection_id) REFERENCES connector_connections(id) ON DELETE CASCADE;

ALTER TABLE ONLY dlc_review_logs
    ADD CONSTRAINT dlc_review_logs_use_case_id_fkey FOREIGN KEY (use_case_id) REFERENCES dlc_use_cases(id) ON DELETE CASCADE;

ALTER TABLE ONLY episodes
    ADD CONSTRAINT episodes_contact_id_fkey FOREIGN KEY (contact_id) REFERENCES contact_map(id);

ALTER TABLE ONLY limit_grant_uses
    ADD CONSTRAINT limit_grant_uses_grant_id_fkey FOREIGN KEY (grant_id) REFERENCES limit_grants(id) ON DELETE CASCADE;

ALTER TABLE ONLY phone_numbers
    ADD CONSTRAINT phone_numbers_customer_id_sip_trunk_id_fkey FOREIGN KEY (customer_id, sip_trunk_id) REFERENCES sip_trunks(customer_id, id);

ALTER TABLE ONLY price_list_entries
    ADD CONSTRAINT price_entries_change_fkey FOREIGN KEY (change) REFERENCES price_changes(name);

ALTER TABLE ONLY scheduled_task_runs
    ADD CONSTRAINT scheduled_task_runs_customer_id_task_id_fkey FOREIGN KEY (customer_id, task_id) REFERENCES scheduled_tasks(customer_id, id) ON DELETE CASCADE;

ALTER TABLE ONLY simulation_cases
    ADD CONSTRAINT simulation_cases_run_id_fkey FOREIGN KEY (run_id) REFERENCES simulation_runs(id) ON DELETE CASCADE;

ALTER TABLE ONLY simulation_runs
    ADD CONSTRAINT simulation_runs_simulation_id_fkey FOREIGN KEY (simulation_id) REFERENCES simulations(id) ON DELETE CASCADE;

ALTER TABLE ONLY user_phone_history
    ADD CONSTRAINT user_phone_history_customer_id_stream_app_pk_user_id_fkey FOREIGN KEY (customer_id, stream_app_pk, user_id) REFERENCES user_phones(customer_id, stream_app_pk, user_id) ON DELETE CASCADE;

ALTER TABLE ONLY voice_bindings
    ADD CONSTRAINT voice_bindings_voice_id_fkey FOREIGN KEY (voice_id) REFERENCES voices(id) ON DELETE CASCADE;

ALTER TABLE ONLY voice_samples
    ADD CONSTRAINT voice_samples_voice_id_fkey FOREIGN KEY (voice_id) REFERENCES voices(id) ON DELETE CASCADE;

-- +goose Down
-- The initial migration is not undone.
SELECT 1;
