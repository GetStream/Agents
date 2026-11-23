-- +goose Up
SET LOCAL lock_timeout = '5s';

-- channel_identities and contact_map both said who an address is to one agent of a customer.
-- The channel lines' rows (kind whatsapp, sms or imessage) move into contact_map with thread,
-- where a reply reaches the person; their conversation_id stays the conversation the next
-- message carries on. Their kinds are their own, so the phone rows of episode cards and links
-- never share a row with them.
ALTER TABLE contact_map RENAME COLUMN agent_config_id TO config_id;
ALTER TABLE contact_map ADD COLUMN thread TEXT NOT NULL DEFAULT '';
ALTER TABLE contact_map DROP CONSTRAINT contact_map_kind_check;
ALTER TABLE contact_map ADD CONSTRAINT contact_map_kind_check
    CHECK (kind IN ('phone', 'slack', 'stream_user', 'whatsapp', 'sms', 'imessage')) NOT VALID;

INSERT INTO contact_map (id, customer_id, config_id, kind, address, conversation_id, user_id, thread, created_at, updated_at)
SELECT id, customer_id, config_id, kind, address, conversation_id, user_id, thread, created_at, updated_at
FROM channel_identities
ON CONFLICT DO NOTHING;

DROP TABLE channel_identities;

-- +goose Down
CREATE TABLE channel_identities (
    id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    config_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    address TEXT NOT NULL,
    thread TEXT NOT NULL DEFAULT '',
    user_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX channel_identities_address_idx ON channel_identities (customer_id, config_id, kind, address);

INSERT INTO channel_identities (id, customer_id, config_id, kind, address, thread, user_id, conversation_id, created_at, updated_at)
SELECT id, customer_id, config_id, kind, address, thread, COALESCE(user_id, ''), conversation_id, created_at, updated_at
FROM contact_map
WHERE kind IN ('whatsapp', 'sms', 'imessage');

DELETE FROM contact_map WHERE kind IN ('whatsapp', 'sms', 'imessage');
ALTER TABLE contact_map DROP CONSTRAINT contact_map_kind_check;
ALTER TABLE contact_map ADD CONSTRAINT contact_map_kind_check CHECK (kind IN ('phone', 'slack', 'stream_user')) NOT VALID;
ALTER TABLE contact_map DROP COLUMN thread;
ALTER TABLE contact_map RENAME COLUMN config_id TO agent_config_id;
