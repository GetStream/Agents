//go:build integration

package store

import "time"

func (s *StoreSuite) TestCallParticipantQueryBudgets() {
	scope := AppScope{App: 7}
	joined, left := s.base, s.base.Add(time.Minute)
	for _, session := range []string{"phone-1", "phone-2", "phone-3"} {
		s.Equal(1, s.countQueries(func() {
			s.Require().NoError(s.store.CallParticipants.Record(s.ctx, scope,
				CallParticipant{CallType: "default", CallID: "call-1", ParticipantSession: session, Number: "+15125550100", JoinedAt: &joined}, s.base))
		}), "Record")
	}
	s.Require().NoError(s.store.CallParticipants.Record(s.ctx, scope,
		CallParticipant{CallType: "default", CallID: "call-1", ParticipantSession: "phone-1", LeftAt: &left}, s.base))
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.CallParticipants.RecordHeard(s.ctx, scope, "default", "call-1", s.base))
	}), "RecordHeard")

	s.Equal(1, s.countQueries(func() {
		heard, err := s.store.CallParticipants.Heard(s.ctx, scope, "default", "call-1")
		s.Require().NoError(err)
		s.True(heard)
	}), "Heard")
	s.Equal(1, s.countQueries(func() {
		at, err := s.store.CallParticipants.Left(s.ctx, scope, "default", "call-1", "phone-1")
		s.Require().NoError(err)
		s.Require().NotNil(at)
		s.True(left.Equal(*at))
	}), "Left")
	s.Equal(1, s.countQueries(func() {
		s.Require().NoError(s.store.CallParticipants.Forget(s.ctx, scope, "default", "call-1", "phone-2", nil))
	}), "Forget")
	s.Equal(1, s.countQueries(func() {
		deleted, err := s.store.CallParticipants.DeleteBefore(s.ctx, s.base.Add(time.Second), 10)
		s.Require().NoError(err)
		s.Equal(3, deleted, "phone-1, phone-3 and the heard row; phone-2 was forgotten")
	}), "DeleteBefore")
}
