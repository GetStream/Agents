//go:build integration

package store

func (s *StoreSuite) TestPostgresProbesTheConnectionHoldingTheShipLock() {
	lock := s.store.ShipLock()
	defer lock.Release()

	held, err := lock.Hold(s.ctx)
	s.Require().NoError(err)
	s.Require().True(held)

	var idle, interval, count string
	s.Require().NoError(lock.conn.QueryRowContext(s.ctx,
		"SELECT current_setting('tcp_keepalives_idle'), current_setting('tcp_keepalives_interval'), current_setting('tcp_keepalives_count')",
	).Scan(&idle, &interval, &count))
	s.Equal([]string{"30", "10", "3"}, []string{idle, interval, count})
}

func (s *StoreSuite) TestARouterWithoutTheShipLockKeepsNoConnectionForIt() {
	holder, other := s.store.ShipLock(), s.store.ShipLock()
	defer holder.Release()
	defer other.Release()
	held, err := holder.Hold(s.ctx)
	s.Require().NoError(err)
	s.Require().True(held)

	held, err = other.Hold(s.ctx)

	s.Require().NoError(err)
	s.False(held)
	s.Nil(other.conn, "given back to the pool until the next try")
}
