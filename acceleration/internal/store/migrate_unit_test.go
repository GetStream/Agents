package store

import (
	"io/fs"
	"slices"
	"strconv"
	"strings"
	"testing"

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

// A table a later migration drops is not one the set leaves, so nothing describes or moves it.
func TestADroppedTableIsNotInItsSet(t *testing.T) {
	global, err := setTables(globalSet)
	require.NoError(t, err)

	require.Contains(t, global, "connector_connections")
	require.NotContains(t, global, "connector_connection_clients")
}

// A table a later migration drops is no longer the set's, but a database of the other set may
// still hold it from before the split, so it stays a stray to drop there.
func TestADroppedTableLeavesItsSetButStaysAStray(t *testing.T) {
	global, err := setTables(globalSet)
	require.NoError(t, err)
	created, err := createdTables(globalSet)
	require.NoError(t, err)
	for _, table := range retiredTables {
		require.NotContains(t, global, table)
		require.Contains(t, created, table)
	}
	require.Contains(t, global, "agent_configs")
}

// An export reads and writes each table in the database it lives in.
func TestEveryExportedTableIsOnTheSideItsSetSays(t *testing.T) {
	shard, err := setTables(shardSet)
	require.NoError(t, err)
	for _, table := range dataTables {
		require.Equal(t, slices.Contains(shard, table.name), table.shard, table.name)
	}
}

// A table a later migration drops is no longer the set's, though the baseline created it.
func TestADroppedTableIsNoLongerTheSets(t *testing.T) {
	shard, err := setTables(shardSet)
	require.NoError(t, err)
	require.NotContains(t, shard, "phone_calls")
	require.Contains(t, shard, "call_legs")
}

// A migration that waits behind a long query for a table's lock holds every write queued behind
// it, so each one run in a transaction after the baseline sets a lock timeout before it takes a
// lock: as its first statement.
func TestEveryTransactionalMigrationAfterTheBaselineSetsALockTimeout(t *testing.T) {
	for _, set := range []migrationSet{globalSet, shardSet} {
		files, err := fs.Glob(set.files, "*.sql")
		require.NoError(t, err)
		for _, name := range files {
			if strings.HasPrefix(name, strconv.FormatInt(baseline, 10)) {
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
