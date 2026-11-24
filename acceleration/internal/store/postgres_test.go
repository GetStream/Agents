//go:build integration

package store

import (
	"crypto/rand"
	"strings"
)

func (s *StoreSuite) TestAWriteToADemotedPrimaryIsTransient() {
	conn, err := s.store.GlobalDB().Conn(s.ctx)
	s.Require().NoError(err)
	// Back in the pool read-only, the connection would refuse every later test's writes.
	defer func() {
		_, err := conn.ExecContext(s.ctx, "RESET ALL")
		s.NoError(err)
		s.NoError(conn.Close())
	}()
	table := "transient_" + strings.ToLower(rand.Text())
	_, err = conn.ExecContext(s.ctx, "CREATE TABLE "+table+" (x int)")
	s.Require().NoError(err)
	defer func() {
		_, err := s.store.GlobalDB().ExecContext(s.ctx, "DROP TABLE "+table)
		s.NoError(err)
	}()

	// A primary a failover demoted answers every write as a read-only transaction does.
	_, err = conn.ExecContext(s.ctx, "SET default_transaction_read_only = on")
	s.Require().NoError(err)
	_, err = conn.ExecContext(s.ctx, "INSERT INTO "+table+" VALUES (1)")
	s.Require().Error(err)
	s.True(Transient(err), "a write refused as read-only is not retried: %v", err)
}

func (s *StoreSuite) TestAStatementCutOffByStatementTimeoutIsTransient() {
	conn, err := s.store.GlobalDB().Conn(s.ctx)
	s.Require().NoError(err)
	// The driver drops a connection whose statement was cancelled, so no later test gets its
	// timeout, and Close finds it already closed.
	defer func() { _ = conn.Close() }()

	_, err = conn.ExecContext(s.ctx, "SET statement_timeout = 1")
	s.Require().NoError(err)
	_, err = conn.ExecContext(s.ctx, "SELECT pg_sleep(1)")
	s.Require().Error(err)
	s.True(Transient(err), "a statement the server timed out is not retried: %v", err)
}
