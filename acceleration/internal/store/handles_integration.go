//go:build integration

package store

import (
	"context"

	"github.com/uptrace/bun"
)

// GlobalDB is the shared database's bun handle, for a test to make a state no repository
// can, such as a row backdated by a day. It is only built into integration tests, so no
// production code reads a table around the store.
func (s *Store) GlobalDB() *bun.DB { return s.global }

// ShardDB is this shard's bun handle, for the same tests. It is GlobalDB when the deployment
// has one database.
func (s *Store) ShardDB() *bun.DB { return s.shard }

// ForgetCached drops the cached copy of an agent config whose row a test changed through
// GlobalDB, so the next read is the row.
func (r *AgentConfigs) ForgetCached(ctx context.Context, customerID, id string) {
	r.store.forget(ctx, agentConfigKey(customerID, id))
}
