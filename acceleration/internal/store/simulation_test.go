//go:build integration

package store

// simulation stores a simulation of a customer's, defaulting the fields a test does not care
// about.
func (s *StoreSuite) simulation(customerID string) Simulation {
	simulation := Simulation{
		CustomerID: customerID, Name: "order status", Mode: SimulationText,
		ConfigID: "config", Scenario: "ask where the order is", Assertion: "it says where",
	}
	s.Require().NoError(s.store.Simulations.Insert(s.ctx, &simulation))
	return simulation
}

func (s *StoreSuite) TestASimulationIsOnlyItsOwnCustomers() {
	mine := s.simulation("app")
	s.simulation("other")

	_, err := s.store.Simulations.Get(s.ctx, "other", mine.ID)
	s.ErrorIs(err, ErrNoSimulation)
	listed, err := s.store.Simulations.Select(s.ctx, "app")
	s.Require().NoError(err)
	s.Require().Len(listed, 1)
	s.Equal(mine.ID, listed[0].ID)
	stolen := mine
	stolen.CustomerID = "other"
	s.ErrorIs(s.store.Simulations.Update(s.ctx, &stolen), ErrNoSimulation)
	s.ErrorIs(s.store.Simulations.Delete(s.ctx, "other", mine.ID), ErrNoSimulation)
}

func (s *StoreSuite) TestASimulationIsStoredWithItsDefaults() {
	simulation := s.simulation("app")

	read, err := s.store.Simulations.Get(s.ctx, "app", simulation.ID)
	s.Require().NoError(err)
	s.Equal(1, read.Variations)
	s.Equal(defaultMaxTurns, read.MaxTurns)
	s.Equal(map[string]string{}, read.Tags)
}

func (s *StoreSuite) TestAnInvalidSimulationIsNotWritten() {
	var invalid *ValidationError
	s.Require().ErrorAs(s.store.Simulations.Insert(s.ctx, &Simulation{CustomerID: "app", Name: "x", Mode: SimulationText}), &invalid)
	s.Equal("config_id", invalid.Field)
	listed, err := s.store.Simulations.Select(s.ctx, "app")
	s.Require().NoError(err)
	s.Empty(listed)

	stored := s.simulation("app")
	stored.Variations = maxVariations + 1
	s.Require().ErrorAs(s.store.Simulations.Update(s.ctx, &stored), &invalid)
	s.Equal("variations", invalid.Field)
	read, err := s.store.Simulations.Get(s.ctx, "app", stored.ID)
	s.Require().NoError(err)
	s.Equal(1, read.Variations)
}

func (s *StoreSuite) TestADeletedSimulationIsNoLongerFound() {
	simulation := s.simulation("app")
	s.Require().NoError(s.store.Simulations.Delete(s.ctx, "app", simulation.ID))

	_, err := s.store.Simulations.Get(s.ctx, "app", simulation.ID)
	s.ErrorIs(err, ErrNoSimulation)
	s.ErrorIs(s.store.Simulations.Delete(s.ctx, "app", simulation.ID), ErrNoSimulation)
}

func (s *StoreSuite) TestSimulationQueryBudgets() {
	for range 20 {
		s.simulation("app")
	}
	simulation := Simulation{CustomerID: "app", Name: "refund", Mode: SimulationAudio, ConfigID: "config", Scenario: "ask", Assertion: "offered"}

	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Simulations.Insert(s.ctx, &simulation)) }))
	s.Equal(1, s.countQueries(func() {
		listed, err := s.store.Simulations.Select(s.ctx, "app")
		s.Require().NoError(err)
		s.Len(listed, 21)
	}))
	s.Equal(1, s.countQueries(func() {
		_, err := s.store.Simulations.Get(s.ctx, "app", simulation.ID)
		s.Require().NoError(err)
	}))
	simulation.Scenario = "ask twice"
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Simulations.Update(s.ctx, &simulation)) }))
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.Simulations.Delete(s.ctx, "app", simulation.ID)) }))
}
