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
