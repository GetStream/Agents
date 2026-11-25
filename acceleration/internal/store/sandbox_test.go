//go:build integration

package store

import "time"

func (s *StoreSuite) TestSandboxQueryBudgets() {
	at := time.Now().UTC().Add(-time.Hour)
	s.withinQueryBudgets([]queryBudget{
		{"Sandboxes.Usage", 1, 0, func() error {
			_, err := s.store.Sandboxes.Usage(s.ctx, "local", []string{"sandbox-1"}, at, time.Now())
			return err
		}},
	})
}
