-- +goose Up

-- KnowledgeURLs.SelectDue reads every customer's pages oldest first; without this it reads
-- the whole table.
CREATE INDEX IF NOT EXISTS knowledge_urls_due_idx ON knowledge_urls (updated_at)
    WHERE deleted_at IS NULL AND refresh_hours > 0 AND state <> 'pending';
