package store

import (
	"slices"
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
