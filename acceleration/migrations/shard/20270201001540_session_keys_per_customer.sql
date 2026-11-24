-- +goose Up
SET LOCAL lock_timeout = '5s';

-- Two customers may each have a session of one id: a session is keyed by its customer and its id,
-- and so is every row that names one. Each key is made of the index the migration before built,
-- and each foreign key is added unchecked, so no statement here reads a row: the locks are held
-- for as long as the catalogue takes to change. They are all taken first, sessions first, so the
-- migration never waits for one while it holds some of the others in the middle of its changes.
-- The rows already there are checked by the next migration, under locks that stop no write.
LOCK TABLE agent_sessions, agent_session_tools, agent_branch_messages, agent_branch_switches, agent_responses, calls
    IN ACCESS EXCLUSIVE MODE;

ALTER TABLE agent_session_tools
    DROP CONSTRAINT agent_session_tools_session_id_fkey,
    DROP CONSTRAINT agent_session_tools_pkey,
    ADD CONSTRAINT agent_session_tools_pkey PRIMARY KEY USING INDEX agent_session_tools_customer_id_session_id_key;
ALTER TABLE agent_branch_messages
    DROP CONSTRAINT agent_branch_messages_session_id_fkey,
    DROP CONSTRAINT agent_branch_messages_pkey,
    ADD CONSTRAINT agent_branch_messages_pkey PRIMARY KEY USING INDEX agent_branch_messages_customer_key;
ALTER TABLE agent_branch_switches DROP CONSTRAINT agent_branch_switches_session_id_fkey;
ALTER TABLE agent_responses DROP CONSTRAINT agent_responses_session_id_fkey;
ALTER TABLE calls DROP CONSTRAINT calls_session_id_fkey;
ALTER TABLE agent_sessions
    DROP CONSTRAINT agent_sessions_pkey,
    ADD CONSTRAINT agent_sessions_pkey PRIMARY KEY USING INDEX agent_sessions_customer_id_id_key;

ALTER TABLE agent_session_tools ADD CONSTRAINT agent_session_tools_customer_id_session_id_fkey
    FOREIGN KEY (customer_id, session_id) REFERENCES agent_sessions (customer_id, id) ON DELETE CASCADE NOT VALID;
ALTER TABLE agent_branch_messages ADD CONSTRAINT agent_branch_messages_customer_id_session_id_fkey
    FOREIGN KEY (customer_id, session_id) REFERENCES agent_sessions (customer_id, id) ON DELETE CASCADE NOT VALID;
ALTER TABLE agent_branch_switches ADD CONSTRAINT agent_branch_switches_customer_id_session_id_fkey
    FOREIGN KEY (customer_id, session_id) REFERENCES agent_sessions (customer_id, id) ON DELETE CASCADE NOT VALID;
ALTER TABLE agent_responses ADD CONSTRAINT agent_responses_customer_id_session_id_fkey
    FOREIGN KEY (customer_id, session_id) REFERENCES agent_sessions (customer_id, id) ON DELETE CASCADE NOT VALID;
-- A call outlives its session: only the session it names is forgotten, its customer stays.
ALTER TABLE calls ADD CONSTRAINT calls_customer_id_session_id_fkey
    FOREIGN KEY (customer_id, session_id) REFERENCES agent_sessions (customer_id, id) ON DELETE SET NULL (session_id) NOT VALID;

-- +goose Down
SET LOCAL lock_timeout = '5s';

LOCK TABLE agent_sessions, agent_session_tools, agent_branch_messages, agent_branch_switches, agent_responses, calls
    IN ACCESS EXCLUSIVE MODE;

ALTER TABLE agent_session_tools DROP CONSTRAINT agent_session_tools_customer_id_session_id_fkey;
ALTER TABLE agent_branch_messages DROP CONSTRAINT agent_branch_messages_customer_id_session_id_fkey;
ALTER TABLE agent_branch_switches DROP CONSTRAINT agent_branch_switches_customer_id_session_id_fkey;
ALTER TABLE agent_responses DROP CONSTRAINT agent_responses_customer_id_session_id_fkey;
ALTER TABLE calls DROP CONSTRAINT calls_customer_id_session_id_fkey;

ALTER TABLE agent_sessions DROP CONSTRAINT agent_sessions_pkey, ADD CONSTRAINT agent_sessions_pkey PRIMARY KEY (id);
ALTER TABLE agent_session_tools DROP CONSTRAINT agent_session_tools_pkey, ADD CONSTRAINT agent_session_tools_pkey PRIMARY KEY (session_id);
ALTER TABLE agent_branch_messages DROP CONSTRAINT agent_branch_messages_pkey,
    ADD CONSTRAINT agent_branch_messages_pkey PRIMARY KEY (session_id, original_message_id);

ALTER TABLE agent_session_tools ADD CONSTRAINT agent_session_tools_session_id_fkey
    FOREIGN KEY (session_id) REFERENCES agent_sessions (id) ON DELETE CASCADE;
ALTER TABLE agent_branch_messages ADD CONSTRAINT agent_branch_messages_session_id_fkey
    FOREIGN KEY (session_id) REFERENCES agent_sessions (id) ON DELETE CASCADE;
ALTER TABLE agent_branch_switches ADD CONSTRAINT agent_branch_switches_session_id_fkey
    FOREIGN KEY (session_id) REFERENCES agent_sessions (id) ON DELETE CASCADE;
ALTER TABLE agent_responses ADD CONSTRAINT agent_responses_session_id_fkey
    FOREIGN KEY (session_id) REFERENCES agent_sessions (id) ON DELETE CASCADE;
ALTER TABLE calls ADD CONSTRAINT calls_session_id_fkey
    FOREIGN KEY (session_id) REFERENCES agent_sessions (id) ON DELETE SET NULL;
