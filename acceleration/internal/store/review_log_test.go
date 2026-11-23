//go:build integration

package store

func (s *StoreSuite) TestReviewLogQueryBudgets() {
	useCase := s.useCase(newID(), "support")
	for _, status := range []string{"submitted", "approved", "suspended"} {
		from := useCase.Status
		useCase.Status = status
		s.Require().NoError(s.store.UseCases.Move(s.ctx, &useCase, from, &ReviewLog{Actor: "vendor"}))
	}

	s.Equal(1, s.countQueries(func() {
		logs, err := s.store.ReviewLogs.Select(s.ctx, useCase.CustomerID, useCase.ID, 10, nil)
		s.Require().NoError(err)
		s.Len(logs, 3)
	}), "Select")
}
