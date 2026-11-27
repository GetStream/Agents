//go:build integration

package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// commitHolder stands in front of a Postgres and holds, on the first connection that deletes
// a policy, the COMMIT after that delete until let. gaveUp is closed once the client drops that
// connection, as pgdriver does when its read times out.
type commitHolder struct {
	listener net.Listener
	target   string
	release  chan struct{}
	gaveUp   chan struct{}
	once     sync.Once
	letOnce  sync.Once
}

// holdingCommit puts a commitHolder in front of the shared database and answers the DSN that
// reaches it, whose reads give up after a second.
func (s *ForgetUserSuite) holdingCommit() (*commitHolder, string) {
	target, err := url.Parse(s.settings.Postgres.DSN)
	s.Require().NoError(err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	holder := &commitHolder{listener: listener, target: target.Host, release: make(chan struct{}), gaveUp: make(chan struct{})}
	s.T().Cleanup(func() {
		_ = listener.Close()
		holder.let()
	})
	go holder.serve()
	target.Host = listener.Addr().String()
	query := target.Query()
	query.Set("read_timeout", "1s")
	target.RawQuery = query.Encode()
	return holder, target.String()
}

func (h *commitHolder) let() { h.letOnce.Do(func() { close(h.release) }) }

func (h *commitHolder) serve() {
	for {
		conn, err := h.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", h.target)
		if err != nil {
			_ = conn.Close()
			continue
		}
		go func() {
			defer func() { _ = upstream.Close() }()
			h.toServer(conn, upstream)
		}()
		go func() {
			defer func() { _ = conn.Close() }()
			_, _ = io.Copy(conn, upstream)
		}()
	}
}

// toServer forwards a client's startup message and then its typed messages, holding the
// COMMIT that follows a delete of policies on the first connection to send one.
func (h *commitHolder) toServer(conn, upstream net.Conn) {
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
	var deleting bool
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(header[1:])-4)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		if header[0] == 'Q' && bytes.HasPrefix(body, []byte(`DELETE FROM "policies"`)) {
			deleting = true
		}
		if header[0] == 'Q' && deleting && bytes.HasPrefix(body, []byte("COMMIT")) {
			held := false
			h.once.Do(func() { held = true })
			if held {
				// The client gives up on the answer and drops the connection meanwhile.
				go func() {
					_, _ = io.Copy(io.Discard, conn)
					close(h.gaveUp)
				}()
				<-h.release
			}
		}
		if _, err := upstream.Write(append(header, body...)); err != nil {
			return
		}
	}
}

// A run whose COMMIT lands after it gave up on its answer still drops, before it exits, the
// policy a router cached meanwhile: closing the store lets the deletions held for that COMMIT
// run.
func (s *ForgetUserSuite) TestARunWhoseCommitLandsAfterItGaveUpStillDropsThePolicyARouterCached() {
	router, err := store.OpenShards(store.Options{
		DSN: s.settings.Postgres.DSN, ShardDSN: s.settings.Postgres.ShardDSN, Shard: s.settings.Shard, Shards: s.settings.Shards,
		Redis: store.Redis{Address: s.settings.Redis.Addr},
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(router.Close()) })
	s.settingsOf(s.app, person)
	scope := store.UserScopeID(s.app, person)
	cached, err := router.Policies.Get(s.ctx, store.ScopeUser, scope)
	s.Require().NoError(err)
	s.Require().NotNil(cached.Quota)
	holder, dsn := s.holdingCommit()
	s.settings.Postgres.DSN = dsn

	ran := make(chan error, 1)
	go func() {
		ran <- s.runForget([]string{"forget-user", "--app", s.app, "--user", person, "--actor", "staff@example.com", "--settle", "0s"}, &bytes.Buffer{})
	}()
	select {
	case <-holder.gaveUp:
	case <-time.After(10 * time.Second):
		s.FailNow("the run never gave up on the held COMMIT")
	}
	// The run drops the policy as it gives up; a router reading it meanwhile caches it again,
	// since the delete has not committed.
	for deadline := time.Now().Add(30 * time.Millisecond); time.Now().Before(deadline); {
		read, err := router.Policies.Get(s.ctx, store.ScopeUser, scope)
		s.Require().NoError(err)
		s.Require().NotNil(read.Quota, "the delete is not committed yet")
	}
	holder.let()
	select {
	case err := <-ran:
		s.Error(err, "the run never had the COMMIT's answer")
	case <-time.After(10 * time.Second):
		s.FailNow("the run never ended")
	}

	s.Eventually(func() bool {
		read, err := router.Policies.Get(s.ctx, store.ScopeUser, scope)
		s.Require().NoError(err)
		return read.Quota == nil
	}, 3*time.Second, 10*time.Millisecond, "the router still answers the policy the user had")
}
