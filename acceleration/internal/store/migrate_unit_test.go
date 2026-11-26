package store

import (
	"io/fs"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

// movingTables are the tables on their way from the shared database to the shards'. Both
// migration sets create them until the shared set drops its copy.
var movingTables = []string{"agent_logs", "connector_invocations"}

// A table is created by one set alone, so a deployment of shards knows where each one is. One
// moving to the shards is created by both until the shared set drops it, and is read from both.
func TestEveryTableBelongsToOneSet(t *testing.T) {
	global, err := setTables(globalSet)
	require.NoError(t, err)
	shard, err := setTables(shardSet)
	require.NoError(t, err)
	require.Contains(t, global, "agent_configs")
	require.Contains(t, shard, "requests")
	for _, table := range shard {
		if slices.Contains(movingTables, table) {
			require.Contains(t, global, table, "a table moving to the shards is still in the shared set")
			continue
		}
		require.NotContains(t, global, table)
	}
}

// An export reads and writes each table in the database it lives in.
func TestEveryExportedTableIsOnTheSideItsSetSays(t *testing.T) {
	shard, err := setTables(shardSet)
	require.NoError(t, err)
	for _, table := range dataTables {
		require.Equal(t, slices.Contains(shard, table.name), table.shard, table.name)
	}
}

// A table a later migration drops or renames is not one the set leaves under its old name, so
// nothing describes or moves it.
func TestATableALaterMigrationDropsOrRenamesIsNotTheSets(t *testing.T) {
	set := migrationSet{files: fstest.MapFS{
		"1_initial.sql": {Data: []byte("-- +goose Up\nCREATE TABLE kept (id int);\nCREATE TABLE dropped (id int);\nCREATE TABLE renamed (id int);\n" +
			"-- +goose Down\nCREATE TABLE downed (id int);\n")},
		"2_change.sql": {Data: []byte("-- +goose Up\nDROP TABLE dropped;\nALTER TABLE renamed RENAME TO moved;\n")},
	}}

	tables, err := setTables(set)
	require.NoError(t, err)
	require.Equal(t, []string{"kept", "moved"}, tables)
}

// A migration that waits behind a long query for a table's lock holds every write queued behind
// it, so each one run in a transaction after the initial one sets a lock timeout before it takes
// a lock: as its first statement. The initial one runs only on an empty database.
func TestEveryTransactionalMigrationAfterTheInitialOneSetsALockTimeout(t *testing.T) {
	for _, set := range []migrationSet{globalSet, shardSet} {
		files, err := fs.Glob(set.files, "*.sql")
		require.NoError(t, err)
		for _, name := range files {
			if strings.HasSuffix(name, "_initial.sql") {
				continue
			}
			raw, err := fs.ReadFile(set.files, name)
			require.NoError(t, err)
			if strings.Contains(string(raw), "-- +goose NO TRANSACTION") {
				continue
			}
			up, _, _ := strings.Cut(string(raw), "-- +goose Down")
			var first string
			for _, line := range strings.Split(up, "\n") {
				if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "--") {
					first = line
					break
				}
			}
			require.True(t, strings.HasPrefix(first, "SET LOCAL lock_timeout"), "%s/%s starts with %q", set.name, name, first)
		}
	}
}
