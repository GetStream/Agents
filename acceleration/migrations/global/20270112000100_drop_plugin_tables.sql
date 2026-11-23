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
