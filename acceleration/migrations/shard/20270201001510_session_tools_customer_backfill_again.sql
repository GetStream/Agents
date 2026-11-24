-- +goose NO TRANSACTION
-- +goose Up

-- A router of two releases before wrote rows naming no customer until it drained, after
-- 20270201001310 had given the rows before them theirs: each gets its session's, as that one did.
-- A thousand rows a transaction, so no lock is held for the whole table. Run again after it
-- stopped part way, it skips the rows that name one.
-- +goose StatementBegin
DO $$
DECLARE
    after text := '';
    upto text;
BEGIN
    LOOP
        PERFORM set_config('lock_timeout', '5s', true);
        SELECT max(session_id) INTO upto
        FROM (SELECT session_id FROM agent_session_tools WHERE session_id > after ORDER BY session_id LIMIT 1000) AS batch;
        EXIT WHEN upto IS NULL;
        UPDATE agent_session_tools AS t SET customer_id = s.customer_id
        FROM agent_sessions AS s
        WHERE s.id = t.session_id AND t.session_id > after AND t.session_id <= upto AND t.customer_id IS NULL;
        after := upto;
        COMMIT;
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down
