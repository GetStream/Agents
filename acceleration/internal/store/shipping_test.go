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

func (s *StoreSuite) TestOneRouterHoldsTheCostAnomalyLock() {
	holder, other, ship := s.store.CostAnomalyLock(), s.store.CostAnomalyLock(), s.store.ShipLock()
	defer holder.Release()
	defer other.Release()
	defer ship.Release()
	held, err := ship.Hold(s.ctx)
	s.Require().NoError(err)
	s.Require().True(held)

	held, err = holder.Hold(s.ctx)
	s.Require().NoError(err)
	s.True(held, "the ship lock is another lock")
	held, err = other.Hold(s.ctx)
	s.Require().NoError(err)
	s.False(held)

	holder.Release()
	held, err = other.Hold(s.ctx)
	s.Require().NoError(err)
	s.True(held, "given back")
}

func (s *ShardedStoreSuite) TestOneRouterOfEveryShardHoldsTheCostAnomalyLock() {
	here, there := s.store.CostAnomalyLock(), s.other.CostAnomalyLock()
	defer here.Release()
	defer there.Release()

	held, err := here.Hold(s.ctx)
	s.Require().NoError(err)
	s.True(held)
	held, err = there.Hold(s.ctx)
	s.Require().NoError(err)
	s.False(held, "a router of another shard checks the same organizations")
}
