package store

import (
	"slices"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

// A table is created by one set alone, so a deployment of shards knows where each one is.
func TestEveryTableBelongsToOneSet(t *testing.T) {
	global, err := setTables(globalSet)
	require.NoError(t, err)
	shard, err := setTables(shardSet)
	require.NoError(t, err)
	require.Contains(t, global, "agent_configs")
	require.Contains(t, shard, "requests")
	for _, table := range shard {
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
