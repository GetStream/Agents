-- +goose Up

-- The baseline carried the table but not its one row, so record_data_change locked nothing and a
-- repeatable read write on a snapshot from before a capture started was lost.
INSERT INTO data_change_starts (one) VALUES (true) ON CONFLICT DO NOTHING;

-- +goose Down

SELECT 1;
