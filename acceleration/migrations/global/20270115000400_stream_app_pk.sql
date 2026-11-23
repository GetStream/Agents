-- +goose Up
SET LOCAL lock_timeout = '5s';

-- stream_app_pk names a Stream app pinned to a row, as every other table names it.
ALTER TABLE chat_inbound RENAME COLUMN stream_app TO stream_app_pk;
ALTER TABLE chat_inbound RENAME CONSTRAINT chat_inbound_stream_app_message_id_key TO chat_inbound_stream_app_pk_message_id_key;
ALTER TABLE image_jobs RENAME COLUMN stream_app TO stream_app_pk;

-- +goose Down
ALTER TABLE image_jobs RENAME COLUMN stream_app_pk TO stream_app;
ALTER TABLE chat_inbound RENAME CONSTRAINT chat_inbound_stream_app_pk_message_id_key TO chat_inbound_stream_app_message_id_key;
ALTER TABLE chat_inbound RENAME COLUMN stream_app_pk TO stream_app;
