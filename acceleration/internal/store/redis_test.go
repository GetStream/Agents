//go:build integration

package store

import (
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// clusterAddressVar names a node of a Redis Cluster with more than one shard, which CI does
// not run. Three `redis-server --cluster-enabled yes` joined by `redis-cli --cluster create`
// make one.
const clusterAddressVar = "ROUTER_TEST_REDIS_CLUSTER_ADDR"

// clusterReplica opens another router on the suite's databases whose Redis is the cluster at
// clusterAddressVar, as a deployment on a Redis Cluster runs. A test of it is skipped when the
// variable is not set.
func (s *StoreSuite) clusterReplica() *Store {
	address := os.Getenv(clusterAddressVar)
	if address == "" {
		s.T().Skipf("%s not set", clusterAddressVar)
	}
	replica, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN, Redis: Redis{Address: address}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(replica.Close()) })
	return replica
}

// A cluster refuses one DEL whose keys sit in different hash slots, and the keys one write
// forgets are in different slots, such as an app's organization and its policies.
func (s *StoreSuite) TestForgetDropsKeysInEverySlotOfACluster() {
	replica := s.clusterReplica()
	client := replica.cache.Client()
	names := s.cachedNames(replica)
	slots := map[int64]bool{}
	for _, name := range names {
		slot, err := client.Do(s.ctx, client.B().ClusterKeyslot().Key(name).Build()).AsInt64()
		s.Require().NoError(err)
		slots[slot] = true
	}
	s.Require().Greater(len(slots), 1, "the keys sit in more than one slot")

	replica.forget(s.ctx, names...)

	s.forgotten(replica, names)
}

func (s *StoreSuite) TestForgetDropsEveryKeyOfOneRedis() {
	if s.store.cache == nil {
		s.T().Skipf("%s not set", redisAddressVar)
	}
	names := s.cachedNames(s.store)

	s.store.forget(s.ctx, names...)

	s.forgotten(s.store, names)
}

// An app's organization is recorded on every start (policy.Enforcer.Join), and moving it
// forgets the app's organization and its policies, two keys in different slots. The replica
// that recorded the move reads the new organization's policies next, not a copy from before,
// whichever node holds them. Many writers keep the nodes busy, since a barrier that waits on
// one key only lets a read through to a stale copy on another node now and then.
func (s *StoreSuite) TestAnAppMovedToAnotherOrganizationIsWhatTheReplicaOnAClusterReadsPoliciesOf() {
	replica := s.clusterReplica()
	var stale atomic.Int64
	var moves sync.WaitGroup
	prefix := rand.Text()
	for writer := range 16 {
		moves.Add(1)
		go func() {
			defer moves.Done()
			for attempt := range 500 {
				app := fmt.Sprintf("app-%s-%d-%d", prefix, writer, attempt)
				if err := replica.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: "org-first"}); err != nil {
					stale.Add(1)
				}
				for range 2 {
					if _, err := replica.Policies.OfApp(s.ctx, app); err != nil {
						stale.Add(1)
					}
				}
				if err := replica.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: "org-second"}); err != nil {
					stale.Add(1)
				}
				if now, err := replica.Policies.OfApp(s.ctx, app); err != nil || now.OrganizationID != "org-second" {
					stale.Add(1)
				}
			}
		}()
	}
	moves.Wait()
	s.Zero(stale.Load(), "reads that failed or answered with the organization from before")
}

// The same move, read through the app's organization: the other key the write forgets.
func (s *StoreSuite) TestAnAppMovedToAnotherOrganizationIsWhatTheReplicaOnAClusterReadsOrganizationOf() {
	replica := s.clusterReplica()
	var stale atomic.Int64
	var moves sync.WaitGroup
	prefix := rand.Text()
	for writer := range 16 {
		moves.Add(1)
		go func() {
			defer moves.Done()
			for attempt := range 500 {
				app := fmt.Sprintf("app-%s-%d-%d", prefix, writer, attempt)
				if err := replica.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: "org-first"}); err != nil {
					stale.Add(1)
				}
				for range 2 {
					if _, err := replica.AppOrganizations.Get(s.ctx, app); err != nil {
						stale.Add(1)
					}
				}
				if err := replica.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: "org-second"}); err != nil {
					stale.Add(1)
				}
				if now, err := replica.AppOrganizations.Get(s.ctx, app); err != nil || now.OrganizationID != "org-second" {
					stale.Add(1)
				}
			}
		}()
	}
	moves.Wait()
	s.Zero(stale.Load(), "reads that failed or answered with the organization from before")
}

// cachedNames stores rows under several cache keys of store's Redis, enough of them that a
// cluster of three shards holds them on more than one.
func (s *StoreSuite) cachedNames(store *Store) []string {
	client := store.cache.Client()
	names := make([]string, 16)
	for i := range names {
		names[i] = cacheKey("forget", rand.Text())
		s.Require().NoError(client.Do(s.ctx, client.B().Set().Key(names[i]).Value("row").Ex(cacheTTL).Build()).Error())
	}
	return names
}

// forgotten asserts that store's Redis holds none of names.
func (s *StoreSuite) forgotten(store *Store, names []string) {
	client := store.cache.Client()
	for _, name := range names {
		held, err := client.Do(s.ctx, client.B().Exists().Key(name).Build()).AsInt64()
		s.Require().NoError(err)
		s.Zero(held, name)
	}
}
