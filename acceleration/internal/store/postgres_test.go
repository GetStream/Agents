//go:build integration

package store

import (
	"crypto/rand"
	"strings"

	"github.com/uptrace/bun"
)

func (s *StoreSuite) TestAWriteToADemotedPrimaryIsTransient() {
	conn, err := s.store.GlobalDB().Conn(s.ctx)
	s.Require().NoError(err)
	// Back in the pool read-only, the connection would refuse every later test's writes.
	defer discard(conn)
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

func (s *StoreSuite) TestAWriteRefusedAsReadOnlyIsTriedAgainOnAFreshConnection() {
	db, table := s.demotedPool()
	_, err := db.ExecContext(s.ctx, "INSERT INTO "+table+" VALUES (1)")
	s.Require().True(isViolation(err, "25006"), "the write was not refused as read-only: %v", err)

	_, err = db.ExecContext(s.ctx, "INSERT INTO "+table+" VALUES (2)")
	s.NoError(err, "the retry reached the connection that refused it as read-only")
}

func (s *StoreSuite) TestAWriteReadBackRefusedAsReadOnlyIsTriedAgainOnAFreshConnection() {
	db, table := s.demotedPool()
	var x int
	err := db.QueryRowContext(s.ctx, "INSERT INTO "+table+" VALUES (1) RETURNING x").Scan(&x)
	s.Require().True(isViolation(err, "25006"), "the write was not refused as read-only: %v", err)

	s.NoError(db.QueryRowContext(s.ctx, "INSERT INTO "+table+" VALUES (2) RETURNING x").Scan(&x),
		"the retry reached the connection that refused it as read-only")
	s.Equal(2, x)
}

func (s *StoreSuite) TestAConnectionIsKeptAfterAWriteRefusedForAnotherReason() {
	db, err := connect(s.dsn, 1)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(db.Close()) })
	_, err = db.ExecContext(s.ctx, "SET application_name = 'kept'")
	s.Require().NoError(err)

	_, err = db.ExecContext(s.ctx, "SELECT 1/0")
	s.Require().Error(err)
	var name string
	s.Require().NoError(db.QueryRowContext(s.ctx, "SHOW application_name").Scan(&name))
	s.Equal("kept", name, "a statement's error dropped the connection it ran on")
}

// demotedPool is a pool of one connection, which answers every write as a primary a failover
// demoted does, and a table to write to.
func (s *StoreSuite) demotedPool() (*bun.DB, string) {
	db, err := connect(s.dsn, 1)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(db.Close()) })
	table := "demoted_" + strings.ToLower(rand.Text())
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "CREATE TABLE "+table+" (x int)")
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		_, err := s.store.GlobalDB().ExecContext(s.ctx, "DROP TABLE "+table)
		s.NoError(err)
	})
	_, err = db.ExecContext(s.ctx, "SET default_transaction_read_only = on")
	s.Require().NoError(err)
	return db, table
}
