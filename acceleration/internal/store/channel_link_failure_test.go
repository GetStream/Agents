//go:build integration

package store

import "time"

func (s *StoreSuite) TestLinkFailuresAreCountedForTheLineAndForTheNumber() {
	line, other := newID(), newID()
	window := time.Now().UTC().Truncate(15 * time.Minute)
	s.Require().NoError(s.store.ChannelLinkFailures.Record(s.ctx, line, "+15550001", window, window.Add(-time.Hour)))
	s.Require().NoError(s.store.ChannelLinkFailures.Record(s.ctx, line, "+15550001", window, window.Add(-time.Hour)))
	s.Require().NoError(s.store.ChannelLinkFailures.Record(s.ctx, line, "+15550002", window, window.Add(-time.Hour)))
	s.Require().NoError(s.store.ChannelLinkFailures.Record(s.ctx, other, "+15550001", window, window.Add(-time.Hour)))
	s.Require().NoError(s.store.ChannelLinkFailures.Record(s.ctx, line, "+15550001", window.Add(-15*time.Minute), window.Add(-time.Hour)))

	onLine, fromSender, err := s.store.ChannelLinkFailures.Count(s.ctx, line, "+15550001", window)

	s.Require().NoError(err)
	s.Equal(3, onLine, "two numbers on this line in this window")
	s.Equal(2, fromSender)
}

func (s *StoreSuite) TestRecordingALinkFailureForgetsWindowsPastTheirTime() {
	line := newID()
	old := time.Now().UTC().Truncate(15 * time.Minute).Add(-time.Hour)
	s.Require().NoError(s.store.ChannelLinkFailures.Record(s.ctx, line, "+15550001", old, old.Add(-time.Hour)))

	s.Require().NoError(s.store.ChannelLinkFailures.Record(s.ctx, newID(), "+15550002", old.Add(time.Hour), old.Add(time.Minute)))

	kept, err := s.store.global.NewSelect().Model((*channelLinkFailure)(nil)).Where("account_id = ?", line).Count(s.ctx)
	s.Require().NoError(err)
	s.Zero(kept)
}

func (s *StoreSuite) TestChannelLinkFailureQueryBudgets() {
	line, window := newID(), time.Now().UTC().Truncate(15*time.Minute)

	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.ChannelLinkFailures.Record(s.ctx, line, "+15550001", window, window.Add(-time.Hour)))
	}), "Record")
	s.Equal(1, s.countQueries(func() {
		_, _, err := s.store.ChannelLinkFailures.Count(s.ctx, line, "+15550001", window)
		s.Require().NoError(err)
	}), "Count")
}
