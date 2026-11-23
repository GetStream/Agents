-- +goose Up
SET LOCAL lock_timeout = '5s';

-- A voice's samples and bindings are no longer carried by a move (store.dataTables): the audio
-- is in this deployment's bucket and the clone in this deployment's account at the provider.
DROP TRIGGER IF EXISTS data_changes_recorded ON voice_samples;
DROP TRIGGER IF EXISTS data_changes_recorded ON voice_bindings;

-- +goose Down

-- Nothing: the router puts the trigger back on every table a move carries when it starts.
