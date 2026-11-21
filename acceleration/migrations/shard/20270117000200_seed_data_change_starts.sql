-- +goose Up

-- record_data_change locks this row on a repeatable read write here as in the shared database,
-- so a shard keeps the table too, with its one row.
CREATE TABLE IF NOT EXISTS data_change_starts (
    one boolean DEFAULT true NOT NULL PRIMARY KEY,
    started bigint DEFAULT 0 NOT NULL,
    CONSTRAINT data_change_starts_one_check CHECK (one)
);
INSERT INTO data_change_starts (one) VALUES (true) ON CONFLICT DO NOTHING;

-- +goose Down

SELECT 1;
