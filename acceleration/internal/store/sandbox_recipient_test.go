//go:build integration

package store

func (s *StoreSuite) TestSandboxRecipientsAreReplacedWhole() {
	customerID := newID()
	s.Require().NoError(s.store.SandboxRecipients.Replace(s.ctx, customerID, []string{"+15551110000", "+15552220000"}))
	s.Require().NoError(s.store.SandboxRecipients.Replace(s.ctx, customerID, []string{"+15553330000"}))

	recipients, err := s.store.SandboxRecipients.Select(s.ctx, customerID)
	s.Require().NoError(err)
	s.Equal([]string{"+15553330000"}, recipients)
}

func (s *StoreSuite) TestSandboxRecipientQueryBudgets() {
	customerID := newID()
	// The delete and the insert, in one transaction.
	s.Equal(2+2, s.countQueries(func() {
		s.Require().NoError(s.store.SandboxRecipients.Replace(s.ctx, customerID, []string{"+15551110000", "+15552220000", "+15553330000"}))
	}), "Replace")
	s.Equal(1, s.countQueries(func() {
		recipients, err := s.store.SandboxRecipients.Select(s.ctx, customerID)
		s.Require().NoError(err)
		s.Len(recipients, 3)
	}), "Select")
}
