//go:build integration

package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"
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

// heldBy begins a transaction that hold makes the writes under test wait behind, such as one
// holding a row with FOR UPDATE, and answers it with its backend's pid.
func (s *StoreSuite) heldBy(hold func(bun.Tx)) (bun.Tx, int) {
	holder, err := s.store.global.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = holder.Rollback() })
	var pid int
	s.Require().NoError(holder.NewRaw("SELECT pg_backend_pid()").Scan(s.ctx, &pid))
	hold(holder)
	return holder, pid
}

// lockingRow holds the row of table with this id.
func (s *StoreSuite) lockingRow(table, id string) func(bun.Tx) {
	return func(holder bun.Tx) {
		_, err := holder.NewRaw("SELECT 1 FROM ? WHERE id = ? FOR UPDATE", bun.Ident(table), id).Exec(s.ctx)
		s.Require().NoError(err)
	}
}

// waitingBehind waits until a statement waits for the holder with this pid, and answers that
// statement's backend's pid.
func (s *StoreSuite) waitingBehind(holder int) int {
	var waiting int
	s.Require().Eventually(func() bool {
		var pids []int
		s.Require().NoError(s.store.global.NewSelect().TableExpr("pg_stat_activity").Column("pid").
			Where("? = ANY(pg_blocking_pids(pid))", holder).Scan(s.ctx, &pids))
		if len(pids) == 0 {
			return false
		}
		waiting = pids[0]
		return true
	}, 5*time.Second, 10*time.Millisecond, "the write's statement waits behind the holder")
	return waiting
}

// readWhileWriting runs write under a request deadline that passes while its statement waits
// behind a transaction hold set up, and then read: a read before Postgres can commit the
// write. The write must outlive its request until then. release ends the holder, and what the
// write answered is returned.
func (s *StoreSuite) readWhileWriting(hold func(bun.Tx), write func(context.Context) error, read func(), release func(bun.Tx) error) error {
	holder, pid := s.heldBy(hold)
	ctx, cancel := context.WithTimeout(s.ctx, 300*time.Millisecond)
	defer cancel()
	answered := make(chan error, 1)
	go func() { answered <- write(ctx) }()
	s.waitingBehind(pid)
	<-ctx.Done()
	var err error
	returned := false
	s.Never(func() bool {
		select {
		case err = <-answered:
			returned = true
		default:
		}
		return returned
	}, time.Second, 10*time.Millisecond, "the write gave up with its request")
	read()
	s.Require().NoError(release(holder))
	if !returned {
		err = <-answered
	}
	return err
}

func (s *StoreSuite) TestAWriteThatRunsOutOfTimeIsRolledBackAndTheCacheAgrees() {
	impatient, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN, Redis: s.redis(), writeTimeout: 300 * time.Millisecond})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(impatient.Close()) })
	config := s.routerConfig("acme-app", "fast")
	holder, pid := s.heldBy(s.lockingRow("router_configs", config.ID))

	answered := make(chan error, 1)
	go func() { answered <- impatient.RouterConfigs.Delete(s.ctx, "acme-app", config.ID) }()
	writer := s.waitingBehind(pid)
	select {
	case err = <-answered:
		s.Require().Error(err, "the delete runs out of time")
	case <-time.After(3 * time.Second):
		s.FailNow("the delete outlived its own write timeout")
	}
	_, err = s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(err, "the config is still there while the delete waits")
	s.Require().NoError(holder.Commit())
	s.Require().Eventually(func() bool {
		alive, err := s.store.global.NewSelect().TableExpr("pg_stat_activity").Where("pid = ?", writer).Exists(s.ctx)
		s.Require().NoError(err)
		return !alive
	}, 5*time.Second, 10*time.Millisecond, "the delete's backend finishes")

	deleted, err := s.store.global.NewSelect().Model((*RouterConfig)(nil)).
		Where("id = ? AND deleted_at IS NOT NULL", config.ID).Exists(s.ctx)
	s.Require().NoError(err)
	s.False(deleted, "a delete that ran out of time is rolled back")
	_, err = s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
	s.NoError(err, "and the cache agrees with the row")
}

// redisProxy stands between a store and the suite's Redis and can drop every connection
// through it, as a Redis that restarts or a network that fails does.
type redisProxy struct {
	listener net.Listener
	target   string
	mu       sync.Mutex
	down     bool
	conns    []net.Conn
}

func (s *StoreSuite) redisProxy() *redisProxy {
	if s.store.cache == nil {
		s.T().Skipf("%s not set", redisAddressVar)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	proxy := &redisProxy{listener: listener, target: s.redis().Address}
	s.T().Cleanup(func() {
		_ = listener.Close()
		proxy.setDown(true)
	})
	go proxy.serve()
	return proxy
}

func (p *redisProxy) serve() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.down {
			p.mu.Unlock()
			_ = conn.Close()
			continue
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			p.mu.Unlock()
			_ = conn.Close()
			continue
		}
		p.conns = append(p.conns, conn, upstream)
		p.mu.Unlock()
		go func() { _, _ = io.Copy(upstream, conn); _ = upstream.Close() }()
		go func() { _, _ = io.Copy(conn, upstream); _ = conn.Close() }()
	}
}

// setDown drops every connection and refuses new ones while down, and lets them through again
// once not.
func (p *redisProxy) setDown(down bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down = down
	if down {
		for _, conn := range p.conns {
			_ = conn.Close()
		}
		p.conns = nil
	}
}

func (s *StoreSuite) TestACachedRowIsForgottenOnceRedisTakesTheDeletionAgain() {
	proxy := s.redisProxy()
	behind, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN, Redis: Redis{Address: proxy.listener.Addr().String()}})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(behind.Close()) })
	config := s.boundConfig("acme-app")
	_, err = s.store.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(err)

	proxy.setDown(true)
	s.Require().NoError(behind.AgentConfigs.Delete(s.ctx, "acme-app", config.ID))
	proxy.setDown(false)

	s.Eventually(func() bool {
		_, err := s.store.AgentConfigs.Get(s.ctx, "acme-app", config.ID)
		return errors.Is(err, ErrNoAgentConfig)
	}, 5*time.Second, 10*time.Millisecond, "the other node still holds the deleted config")
}

// pgProxy stands between a store and the suite's Postgres and does one thing to the first
// COMMIT a client sends through it: drops every answer after it, as a network that fails then
// does, or holds it until let, as a COMMIT slow to reach the server does.
type pgProxy struct {
	listener net.Listener
	target   string
	hold     bool
	release  chan struct{}
	mu       sync.Mutex
	seen     bool
	released bool
}

// pgProxy starts a proxy in front of the suite's shared database, and answers it with the DSN
// of a store on it, whose reads give up after a second.
func (s *StoreSuite) pgProxy(hold bool) (*pgProxy, string) {
	target, err := url.Parse(s.dsn)
	s.Require().NoError(err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	proxy := &pgProxy{listener: listener, target: target.Host, hold: hold, release: make(chan struct{})}
	s.T().Cleanup(func() {
		_ = listener.Close()
		proxy.let()
	})
	go proxy.serve()
	target.Host = listener.Addr().String()
	query := target.Query()
	query.Set("read_timeout", "1s")
	target.RawQuery = query.Encode()
	return proxy, target.String()
}

// let sends a held COMMIT on.
func (p *pgProxy) let() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.released {
		p.released = true
		close(p.release)
	}
}

func (p *pgProxy) serve() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = conn.Close()
			continue
		}
		var dropped atomic.Bool
		go func() {
			defer func() { _ = upstream.Close() }()
			p.toServer(conn, upstream, &dropped)
		}()
		go func() {
			defer func() { _ = conn.Close() }()
			buf := make([]byte, 32<<10)
			for {
				n, err := upstream.Read(buf)
				if n > 0 && !dropped.Load() {
					if _, err := conn.Write(buf[:n]); err != nil {
						return
					}
				}
				if err != nil {
					return
				}
			}
		}()
	}
}

// toServer forwards a client's messages, the startup message and then typed ones, and treats
// the first simple-query COMMIT among them as the proxy says.
func (p *pgProxy) toServer(conn, upstream net.Conn, dropped *atomic.Bool) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return
	}
	startup := make([]byte, binary.BigEndian.Uint32(header)-4)
	if _, err := io.ReadFull(conn, startup); err != nil {
		return
	}
	if _, err := upstream.Write(append(header, startup...)); err != nil {
		return
	}
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(header[1:])-4)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		if header[0] == 'Q' && bytes.HasPrefix(body, []byte("COMMIT")) && p.first() {
			if p.hold {
				<-p.release
			} else {
				dropped.Store(true)
			}
		}
		if _, err := upstream.Write(append(header, body...)); err != nil {
			return
		}
	}
}

func (p *pgProxy) first() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	first := !p.seen
	p.seen = true
	return first
}

func (s *StoreSuite) TestADeleteWhoseCommitAnswerIsLostIsForgottenAtOnce() {
	_, dsn := s.pgProxy(false)
	lossy, err := OpenShards(Options{DSN: dsn, ShardDSN: s.shardDSN, Redis: s.redis(), writeTimeout: 300 * time.Millisecond})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(lossy.Close()) })
	config := s.routerConfig("acme-app", "fast")
	_, err = s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(err)

	s.Require().Error(lossy.RouterConfigs.Delete(s.ctx, "acme-app", config.ID), "the COMMIT's answer never comes")

	_, err = s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
	s.ErrorIs(err, ErrNoRouterConfig, "the COMMIT landed, and the copy was dropped with no answer to it")
}

func (s *StoreSuite) TestADeleteWhoseCommitLandsAfterItGaveUpIsForgottenAgain() {
	proxy, dsn := s.pgProxy(true)
	slow, err := OpenShards(Options{DSN: dsn, ShardDSN: s.shardDSN, Redis: s.redis(), writeTimeout: 300 * time.Millisecond})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(slow.Close()) })
	config := s.routerConfig("acme-app", "fast")

	s.Require().Error(slow.RouterConfigs.Delete(s.ctx, "acme-app", config.ID), "the COMMIT is held past the read timeout")
	_, err = s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
	s.Require().NoError(err, "the config is still there while the COMMIT is held")
	proxy.let()
	s.Require().Eventually(func() bool {
		deleted, err := s.store.global.NewSelect().Model((*RouterConfig)(nil)).
			Where("id = ? AND deleted_at IS NOT NULL", config.ID).Exists(s.ctx)
		s.Require().NoError(err)
		return deleted
	}, 5*time.Second, 10*time.Millisecond, "the held COMMIT lands")

	s.Eventually(func() bool {
		_, err := s.store.RouterConfigs.Get(s.ctx, "acme-app", config.ID)
		return errors.Is(err, ErrNoRouterConfig)
	}, 5*time.Second, 10*time.Millisecond, "the copy cached before the COMMIT landed is dropped again")
}

func (s *StoreSuite) TestForgetsBehindADownRedisAreBoundedAndEndWithTheStore() {
	proxy := s.redisProxy()
	before := runtime.NumGoroutine()
	behind, err := OpenShards(Options{DSN: s.dsn, ShardDSN: s.shardDSN, Redis: Redis{Address: proxy.listener.Addr().String()}})
	s.Require().NoError(err)
	opened := runtime.NumGoroutine()
	proxy.setDown(true)

	for i := range 200 {
		behind.forget(s.ctx, cacheKey("probe", strconv.Itoa(i)))
	}
	s.LessOrEqual(runtime.NumGoroutine()-opened, forgetSlots+8, "the retries are bounded")

	closed := make(chan error, 1)
	go func() { closed <- behind.Close() }()
	select {
	case err := <-closed:
		s.NoError(err)
	case <-time.After(2 * time.Second):
		s.FailNow("Close waits on the retries")
	}
	// Polled here rather than with Eventually, whose own goroutine would be counted.
	for deadline := time.Now().Add(2 * time.Second); runtime.NumGoroutine() > before && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
	}
	s.LessOrEqual(runtime.NumGoroutine(), before, "no retry outlives the store")
}

// lockingWhere holds the rows of table that where picks.
func (s *StoreSuite) lockingWhere(table, where string, args ...any) func(bun.Tx) {
	return func(holder bun.Tx) {
		_, err := holder.NewRaw("SELECT 1 FROM ? WHERE "+where+" FOR UPDATE", append([]any{bun.Ident(table)}, args...)...).Exec(s.ctx)
		s.Require().NoError(err)
	}
}

// inserting holds an insert of row, not yet committed, which a write of the same name waits
// behind.
func (s *StoreSuite) inserting(row any) func(bun.Tx) {
	return func(holder bun.Tx) {
		_, err := holder.NewInsert().Model(row).Exec(s.ctx)
		s.Require().NoError(err)
	}
}

// Every write of a cached row outlives the request that asked for it, and a read made while it
// waits leaves no stale copy behind: each site, written on its request's context and forgotten
// only on success, fails its row.
func (s *StoreSuite) TestEveryCachedWriteARequestGaveUpOnLeavesNoStaleCopy() {
	now := time.Now().UTC()
	for name, site := range map[string]func(){
		"AgentConfigs.Insert": func() {
			name := "agent-" + newID()
			other := AgentConfig{ID: newID(), CustomerID: "acme-app", Name: name, CreatedAt: now, UpdatedAt: now}
			normalizeConfig(&other)
			config := AgentConfig{CustomerID: "acme-app", Name: name, Mode: AgentModeText}
			err := s.readWhileWriting(s.inserting(&other), func(ctx context.Context) error {
				return s.store.AgentConfigs.Insert(ctx, &config)
			}, func() {
				_, err := s.store.AgentConfigs.GetByName(s.ctx, "acme-app", name)
				s.Require().ErrorIs(err, ErrNoAgentConfig, "the name is not taken yet")
			}, bun.Tx.Rollback)
			s.Require().NoError(err)
			found, err := s.store.AgentConfigs.GetByName(s.ctx, "acme-app", name)
			s.Require().NoError(err)
			s.Equal(config.ID, found.ID)
		},
		"AgentConfigs.Update": func() {
			config := s.boundConfig("acme-app")
			was := config.Name
			config.Name = "agent-" + newID()
			err := s.readWhileWriting(s.lockingRow("agent_configs", config.ID), func(ctx context.Context) error {
				return s.store.AgentConfigs.Update(ctx, &config)
			}, func() {
				_, err := s.store.AgentConfigs.GetByName(s.ctx, "acme-app", was)
				s.Require().NoError(err, "the config is not renamed yet")
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			_, err = s.store.AgentConfigs.GetByName(s.ctx, "acme-app", was)
			s.ErrorIs(err, ErrNoAgentConfig)
		},
		"Skills.Insert": func() {
			config := s.boundConfig("acme-app")
			other := Skill{ID: newID(), CustomerID: "acme-app", ConfigID: config.ID, Name: "refund", Files: map[string]string{}, CreatedAt: now, UpdatedAt: now}
			skill := Skill{CustomerID: "acme-app", ConfigID: config.ID, Name: "refund", Description: "does refund", Instructions: "do refund"}
			err := s.readWhileWriting(s.inserting(&other), func(ctx context.Context) error {
				return s.store.Skills.Insert(ctx, &skill)
			}, func() {
				named, err := s.store.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"refund"})
				s.Require().NoError(err)
				s.Require().Empty(named, "the skill is not there yet")
			}, bun.Tx.Rollback)
			s.Require().NoError(err)
			named, err := s.store.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"refund"})
			s.Require().NoError(err)
			s.Len(named, 1)
		},
		"Skills.Update": func() {
			config := s.boundConfig("acme-app")
			skill := s.skill(config, "refund")
			skill.Instructions = "ask first"
			err := s.readWhileWriting(s.lockingRow("skills", skill.ID), func(ctx context.Context) error {
				return s.store.Skills.Update(ctx, &skill)
			}, func() {
				named, err := s.store.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"refund"})
				s.Require().NoError(err)
				s.Require().Len(named, 1)
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			named, err := s.store.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"refund"})
			s.Require().NoError(err)
			s.Require().Len(named, 1)
			s.Equal("ask first", named[0].Instructions)
		},
		"Skills.Delete": func() {
			config := s.boundConfig("acme-app")
			skill := s.skill(config, "refund")
			err := s.readWhileWriting(s.lockingRow("skills", skill.ID), func(ctx context.Context) error {
				return s.store.Skills.Delete(ctx, "acme-app", skill.ID)
			}, func() {
				named, err := s.store.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"refund"})
				s.Require().NoError(err)
				s.Require().Len(named, 1, "the skill is not deleted yet")
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			named, err := s.store.Skills.GetByNames(s.ctx, "acme-app", config.ID, []string{"refund"})
			s.Require().NoError(err)
			s.Empty(named)
		},
		"Voices.Insert": func() {
			name := "voice-" + newID()
			other := Voice{ID: newID(), CustomerID: "acme", Name: name, CreatedAt: now, UpdatedAt: now}
			voice := Voice{CustomerID: "acme", Name: name}
			err := s.readWhileWriting(s.inserting(&other), func(ctx context.Context) error {
				return s.store.Voices.Insert(ctx, &voice)
			}, func() {
				_, err := s.store.Voices.GetByName(s.ctx, "acme", name)
				s.Require().ErrorIs(err, ErrNoVoice, "the name is not taken yet")
			}, bun.Tx.Rollback)
			s.Require().NoError(err)
			found, err := s.store.Voices.GetByName(s.ctx, "acme", name)
			s.Require().NoError(err)
			s.Equal(voice.ID, found.ID)
		},
		"Voices.Update": func() {
			voice := Voice{CustomerID: "acme", Name: "voice-" + newID()}
			s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))
			was := voice.Name
			voice.Name = "voice-" + newID()
			err := s.readWhileWriting(s.lockingRow("voices", voice.ID), func(ctx context.Context) error {
				return s.store.Voices.Update(ctx, &voice)
			}, func() {
				_, err := s.store.Voices.GetByName(s.ctx, "acme", was)
				s.Require().NoError(err, "the voice is not renamed yet")
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			_, err = s.store.Voices.GetByName(s.ctx, "acme", was)
			s.ErrorIs(err, ErrNoVoice)
		},
		"Voices.Delete": func() {
			voice := Voice{CustomerID: "acme", Name: "voice-" + newID()}
			s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))
			err := s.readWhileWriting(s.lockingRow("voices", voice.ID), func(ctx context.Context) error {
				return s.store.Voices.Delete(ctx, "acme", voice.ID)
			}, func() {
				_, err := s.store.Voices.Get(s.ctx, "acme", voice.ID)
				s.Require().NoError(err, "the voice is not deleted yet")
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			_, err = s.store.Voices.Get(s.ctx, "acme", voice.ID)
			s.ErrorIs(err, ErrNoVoice)
		},
		"VoiceBindings.Upsert": func() {
			voice := Voice{CustomerID: "acme", Name: "voice-" + newID()}
			s.Require().NoError(s.store.Voices.Insert(s.ctx, &voice))
			binding := VoiceBinding{VoiceID: voice.ID, Provider: "elevenlabs", ExternalID: "old", State: VoiceReady}
			s.Require().NoError(s.store.VoiceBindings.Upsert(s.ctx, &binding))
			binding.ExternalID = "new"
			err := s.readWhileWriting(s.lockingRow("voice_bindings", binding.ID), func(ctx context.Context) error {
				return s.store.VoiceBindings.Upsert(ctx, &binding)
			}, func() {
				external, err := s.store.VoiceBindings.GetReady(s.ctx, "acme", voice.ID, "elevenlabs")
				s.Require().NoError(err)
				s.Require().Equal("old", external)
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			external, err := s.store.VoiceBindings.GetReady(s.ctx, "acme", voice.ID, "elevenlabs")
			s.Require().NoError(err)
			s.Equal("new", external)
		},
		"CustomModels.Insert": func() {
			name := "model-" + newID()
			other := CustomModel{ID: newID(), CustomerID: "app", Name: name, BaseURL: "https://models.example.com/v1", Model: "llama",
				InputModalities: []string{}, CreatedAt: now, UpdatedAt: now}
			model := CustomModel{CustomerID: "app", Name: name, BaseURL: "https://models.example.com/v1", Model: "llama"}
			err := s.readWhileWriting(s.inserting(&other), func(ctx context.Context) error {
				return s.store.CustomModels.Insert(ctx, &model)
			}, func() {
				_, err := s.store.CustomModels.GetByName(s.ctx, "app", name)
				s.Require().ErrorIs(err, ErrNoCustomModel, "the name is not taken yet")
			}, bun.Tx.Rollback)
			s.Require().NoError(err)
			found, err := s.store.CustomModels.GetByName(s.ctx, "app", name)
			s.Require().NoError(err)
			s.Equal(model.ID, found.ID)
		},
		"CustomModels.Update": func() {
			model := s.customModel("app", "model-"+newID())
			model.Model = "llama-2"
			err := s.readWhileWriting(s.lockingRow("custom_models", model.ID), func(ctx context.Context) error {
				return s.store.CustomModels.Update(ctx, &model)
			}, func() {
				found, err := s.store.CustomModels.GetByName(s.ctx, "app", model.Name)
				s.Require().NoError(err)
				s.Require().Equal("llama", found.Model)
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			found, err := s.store.CustomModels.GetByName(s.ctx, "app", model.Name)
			s.Require().NoError(err)
			s.Equal("llama-2", found.Model)
		},
		"CustomModels.Delete": func() {
			model := s.customModel("app", "model-"+newID())
			err := s.readWhileWriting(s.lockingRow("custom_models", model.ID), func(ctx context.Context) error {
				return s.store.CustomModels.Delete(ctx, "app", model.ID)
			}, func() {
				_, err := s.store.CustomModels.GetByName(s.ctx, "app", model.Name)
				s.Require().NoError(err, "the model is not deleted yet")
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			_, err = s.store.CustomModels.GetByName(s.ctx, "app", model.Name)
			s.ErrorIs(err, ErrNoCustomModel)
		},
		"ProviderKeys.Upsert": func() {
			customer := "app-" + newID()
			key := s.providerKey(customer, "openai", "global", "one")
			_, _, err := s.store.ProviderKeys.Upsert(s.ctx, &key)
			s.Require().NoError(err)
			key = s.providerKey(customer, "openai", "global", "two")
			err = s.readWhileWriting(s.lockingWhere("provider_keys", "customer_id = ?", customer), func(ctx context.Context) error {
				_, _, err := s.store.ProviderKeys.Upsert(ctx, &key)
				return err
			}, func() {
				all, err := s.store.ProviderKeys.SelectAll(s.ctx, customer)
				s.Require().NoError(err)
				s.Require().Len(all, 1)
				s.Require().Equal([]byte("one"), all[0].APIKeySealed)
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			all, err := s.store.ProviderKeys.SelectAll(s.ctx, customer)
			s.Require().NoError(err)
			s.Require().Len(all, 1)
			s.Equal([]byte("two"), all[0].APIKeySealed)
		},
		"ProviderKeys.Delete": func() {
			customer := "app-" + newID()
			key := s.providerKey(customer, "openai", "global", "one")
			_, _, err := s.store.ProviderKeys.Upsert(s.ctx, &key)
			s.Require().NoError(err)
			err = s.readWhileWriting(s.lockingWhere("provider_keys", "customer_id = ?", customer), func(ctx context.Context) error {
				_, err := s.store.ProviderKeys.Delete(ctx, customer, "openai", "global")
				return err
			}, func() {
				all, err := s.store.ProviderKeys.SelectAll(s.ctx, customer)
				s.Require().NoError(err)
				s.Require().Len(all, 1, "the key is not deleted yet")
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			all, err := s.store.ProviderKeys.SelectAll(s.ctx, customer)
			s.Require().NoError(err)
			s.Empty(all)
		},
		"Policies.Upsert": func() {
			app := "app-" + newID()
			yes, no := true, false
			s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeApp, ScopeID: app, Document: PolicyDocument{PromptInjection: &yes}}))
			err := s.readWhileWriting(s.lockingWhere("policies", "scope = ? AND scope_id = ?", ScopeApp, app), func(ctx context.Context) error {
				return s.store.Policies.Upsert(ctx, &Policy{Scope: ScopeApp, ScopeID: app, Document: PolicyDocument{PromptInjection: &no}})
			}, func() {
				document, err := s.store.Policies.Get(s.ctx, ScopeApp, app)
				s.Require().NoError(err)
				s.Require().Equal(&yes, document.PromptInjection)
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			document, err := s.store.Policies.Get(s.ctx, ScopeApp, app)
			s.Require().NoError(err)
			s.Equal(&no, document.PromptInjection)
		},
		"Policies.Delete": func() {
			app := "app-" + newID()
			yes := true
			s.Require().NoError(s.store.Policies.Upsert(s.ctx, &Policy{Scope: ScopeApp, ScopeID: app, Document: PolicyDocument{PromptInjection: &yes}}))
			err := s.readWhileWriting(s.lockingWhere("policies", "scope = ? AND scope_id = ?", ScopeApp, app), func(ctx context.Context) error {
				return s.store.Policies.Delete(ctx, ScopeApp, app)
			}, func() {
				document, err := s.store.Policies.Get(s.ctx, ScopeApp, app)
				s.Require().NoError(err)
				s.Require().NotNil(document.PromptInjection, "the policy is not deleted yet")
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			document, err := s.store.Policies.Get(s.ctx, ScopeApp, app)
			s.Require().NoError(err)
			s.Nil(document.PromptInjection)
		},
		"APIKeys.Revoke": func() {
			app := s.app()
			key := s.key(app.ID, "vak_live_dddddddddddddddd00000000", nil)
			err := s.readWhileWriting(s.lockingRow("api_keys", key.ID), func(ctx context.Context) error {
				_, err := s.store.APIKeys.Revoke(ctx, app.ID, key.ID, "thierry")
				return err
			}, func() {
				_, err := s.store.APIKeys.GetLive(s.ctx, key.ID)
				s.Require().NoError(err, "the key is not revoked yet")
			}, bun.Tx.Commit)
			s.Require().NoError(err)
			_, err = s.store.APIKeys.GetLive(s.ctx, key.ID)
			s.ErrorIs(err, ErrNoAPIKey)
		},
	} {
		s.Run(name, site)
	}
}
