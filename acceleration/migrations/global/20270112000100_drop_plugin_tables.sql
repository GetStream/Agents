-- +goose Up

-- Plugins were replaced by connectors. No release reads these tables, or the config columns
-- that named plugins, plugin events and MCP servers: the model never mapped the columns and an
-- insert left them at their default. An import from an older release skips the tables' rows
-- (store.retiredTables). The lock waits at most 5 s behind a running query, past which the
-- migration fails and the router's next start tries again.
SET LOCAL lock_timeout = '5s';

DROP TABLE IF EXISTS agent_plugin_event_deliveries;
DROP TABLE IF EXISTS agent_plugin_event_subscriptions;
DROP TABLE IF EXISTS agent_plugin_connections;
DROP TABLE IF EXISTS agent_plugin_clients;

ALTER TABLE agent_configs
    DROP COLUMN IF EXISTS plugins,
    DROP COLUMN IF EXISTS plugin_events,
    DROP COLUMN IF EXISTS mcp_servers;

-- +goose Down
SET LOCAL lock_timeout = '5s';

ALTER TABLE agent_configs
    ADD COLUMN plugins jsonb DEFAULT '[]'::jsonb NOT NULL,
    ADD COLUMN plugin_events jsonb DEFAULT '[]'::jsonb NOT NULL,
    ADD COLUMN mcp_servers jsonb DEFAULT '[]'::jsonb NOT NULL;

CREATE TABLE agent_plugin_clients (
    customer_id text NOT NULL,
    config_id text NOT NULL,
    plugin_id text NOT NULL,
    client_id text NOT NULL,
    secret_sealed bytea,
    kek_version integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT agent_plugin_clients_pkey PRIMARY KEY (customer_id, config_id, plugin_id)
);

CREATE TABLE agent_plugin_connections (
    id text NOT NULL,
    customer_id text NOT NULL,
    config_id text NOT NULL,
    plugin_id text NOT NULL,
    instance_url text DEFAULT ''::text NOT NULL,
    access_token text DEFAULT ''::text NOT NULL,
    refresh_token text DEFAULT ''::text NOT NULL,
    expires_at timestamp with time zone,
    status text DEFAULT 'pending'::text NOT NULL,
    oauth_state text DEFAULT ''::text NOT NULL,
    code_verifier text DEFAULT ''::text NOT NULL,
    client_id text DEFAULT ''::text NOT NULL,
    token_endpoint text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    user_id text DEFAULT ''::text NOT NULL,
    CONSTRAINT agent_plugin_connections_pkey PRIMARY KEY (id)
);

CREATE TABLE agent_plugin_event_deliveries (
    subscription_id text NOT NULL,
    event_id text NOT NULL,
    received_at timestamp with time zone NOT NULL,
    CONSTRAINT agent_plugin_event_deliveries_pkey PRIMARY KEY (subscription_id, event_id)
);

CREATE TABLE agent_plugin_event_subscriptions (
    id text NOT NULL,
    customer_id text NOT NULL,
    config_id text NOT NULL,
    plugin_id text NOT NULL,
    user_id text DEFAULT ''::text NOT NULL,
    event text NOT NULL,
    arguments jsonb DEFAULT '{}'::jsonb NOT NULL,
    key text NOT NULL,
    token text NOT NULL,
    secret text NOT NULL,
    remote_id text DEFAULT ''::text NOT NULL,
    refresh_before timestamp with time zone,
    status text NOT NULL,
    error text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone,
    CONSTRAINT agent_plugin_event_subscriptions_pkey PRIMARY KEY (id)
);

CREATE INDEX agent_plugin_connections_config_idx ON agent_plugin_connections USING btree (customer_id, config_id) WHERE (deleted_at IS NULL);
CREATE UNIQUE INDEX agent_plugin_connections_one_idx ON agent_plugin_connections USING btree (config_id, plugin_id, user_id) WHERE (deleted_at IS NULL);
CREATE UNIQUE INDEX agent_plugin_connections_state_idx ON agent_plugin_connections USING btree (oauth_state) WHERE ((deleted_at IS NULL) AND (oauth_state <> ''::text));
CREATE UNIQUE INDEX agent_plugin_event_subscriptions_one_idx ON agent_plugin_event_subscriptions USING btree (config_id, plugin_id, user_id, key) WHERE (deleted_at IS NULL);
CREATE UNIQUE INDEX agent_plugin_event_subscriptions_token_idx ON agent_plugin_event_subscriptions USING btree (token);
