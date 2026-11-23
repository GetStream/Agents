-- +goose Up

-- A link code is claimed on the app's own lines, so it needs to be unique only within the app:
-- keyed on the code alone, every app drew from one space of codes, and a code one app held
-- failed another app's link. The new key refuses nothing the old one let through, so an older
-- release still running writes and claims as before. A code is minted for one person linking one
-- phone, so the table is small and the key is rebuilt under the table's lock, as
-- 20270124000000_hash_call_tokens rebuilt call_bridges'.
ALTER TABLE channel_links
    DROP CONSTRAINT channel_links_pkey,
    ADD CONSTRAINT channel_links_pkey PRIMARY KEY (customer_id, code);

-- +goose Down

-- Two apps may hold one code now: all but one app's are dropped, and those links fail.
DELETE FROM channel_links held USING channel_links kept
    WHERE held.code = kept.code AND held.customer_id > kept.customer_id;
ALTER TABLE channel_links
    DROP CONSTRAINT channel_links_pkey,
    ADD CONSTRAINT channel_links_pkey PRIMARY KEY (code);
