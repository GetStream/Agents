-- +goose Up
SET LOCAL lock_timeout = '5s';

-- channel_thread_people and channel_thread_starters were split from channel_threads only so a
-- rollback could still read it; the store reads with unknown columns discarded, so they are
-- columns of the thread again. Empty is none recorded.
ALTER TABLE channel_threads
    ADD COLUMN chat_person TEXT NOT NULL DEFAULT '',
    ADD COLUMN starter TEXT NOT NULL DEFAULT '';

UPDATE channel_threads AS ct
SET chat_person = COALESCE(ctp.chat_person, ''), starter = COALESCE(cts.starter, '')
FROM channel_threads AS t
LEFT JOIN channel_thread_people AS ctp ON ctp.channel_id = t.channel_id
LEFT JOIN channel_thread_starters AS cts ON cts.channel_id = t.channel_id
WHERE t.channel_id = ct.channel_id AND (ctp.channel_id IS NOT NULL OR cts.channel_id IS NOT NULL);

DROP TABLE channel_thread_people;
DROP TABLE channel_thread_starters;

-- +goose Down
CREATE TABLE channel_thread_people (
    channel_id TEXT PRIMARY KEY REFERENCES channel_threads (channel_id) ON DELETE CASCADE,
    chat_person TEXT NOT NULL
);
CREATE TABLE channel_thread_starters (
    channel_id TEXT PRIMARY KEY REFERENCES channel_threads (channel_id) ON DELETE CASCADE,
    starter TEXT NOT NULL
);
INSERT INTO channel_thread_people (channel_id, chat_person)
SELECT channel_id, chat_person FROM channel_threads WHERE chat_person <> '';
INSERT INTO channel_thread_starters (channel_id, starter)
SELECT channel_id, starter FROM channel_threads WHERE starter <> '';
ALTER TABLE channel_threads DROP COLUMN chat_person, DROP COLUMN starter;
