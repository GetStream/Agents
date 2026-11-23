//go:build integration

package store

import (
	"crypto/rand"
	"os"
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

// An app's organization is recorded on every start (policy.Enforcer.Join), and on a cluster
// the replica that recorded it reads the new one next, not the copy it held before.
func (s *StoreSuite) TestAnAppMovedToAnotherOrganizationIsWhatTheReplicaOnAClusterReadsNext() {
	replica := s.clusterReplica()
	app := "app-" + rand.Text()
	s.Require().NoError(replica.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: "org-first"}))
	for range 2 {
		under, err := replica.AppOrganizations.Get(s.ctx, app)
		s.Require().NoError(err)
		s.Equal("org-first", under.OrganizationID)
	}

	s.Require().NoError(replica.AppOrganizations.Upsert(s.ctx, &AppOrganization{AppID: app, OrganizationID: "org-second"}))

	under, err := replica.AppOrganizations.Get(s.ctx, app)
	s.Require().NoError(err)
	s.Equal("org-second", under.OrganizationID)
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
