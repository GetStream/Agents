-- +goose Up
SET LOCAL lock_timeout = '5s';

-- A claimed guest's memories to move to the account that claimed them, one row per guest of a
-- customer. The claim writes it and returns; routers claim it once next_attempt_at is due and
-- move what mem0 holds under the guest, in passes: memories mem0 extracts from the guest's last
-- turns land after the claim, so a pass is run again after the extraction window, and until one
-- finds nothing under the guest. passes counts the passes that ran, failures the passes that
-- failed since the last that ran, which pace the retries; attempts counts claims, which a
-- write back names its claim by. api_key and organization_id are
-- the claim's attribution, which the copies are billed under. The row is deleted once the move
-- is done or given up.
CREATE TABLE IF NOT EXISTS guest_memory_moves (
    id BIGSERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL,
    guest_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    api_key TEXT NOT NULL DEFAULT '',
    organization_id TEXT NOT NULL DEFAULT '',
    passes INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (customer_id, guest_id)
);
CREATE INDEX IF NOT EXISTS guest_memory_moves_due_idx ON guest_memory_moves (next_attempt_at, id);

-- +goose Down
DROP TABLE guest_memory_moves;
