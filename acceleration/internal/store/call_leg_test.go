//go:build integration

package store

import (
	"time"
)

func (s *StoreSuite) TestOnlyTheRefusedCallersLegIsRefused() {
	refused := CallLeg{CustomerID: "acme", StreamAppPK: 7, CallType: "phone", CallID: "number-call", StreamSessionID: "session-1"}
	s.Equal(1, s.countRedis(func() {
		s.Require().NoError(s.store.CallLegs.MarkRefused(s.ctx, "acme", 7, "phone", "number-call", "session-1", time.Hour))
	}))

	var found bool
	s.Equal(1, s.countRedis(func() {
		var err error
		found, err = s.store.CallLegs.Refused(s.ctx, refused)
		s.Require().NoError(err)
	}))
	s.True(found)
	for _, other := range []CallLeg{
		{CustomerID: "acme", StreamAppPK: 7, CallType: "phone", CallID: "number-call", StreamSessionID: "session-2"},
		{CustomerID: "acme", StreamAppPK: 8, CallType: "phone", CallID: "number-call", StreamSessionID: "session-1"},
		{CustomerID: "other", StreamAppPK: 7, CallType: "phone", CallID: "number-call", StreamSessionID: "session-1"},
	} {
		found, err := s.store.CallLegs.Refused(s.ctx, other)
		s.Require().NoError(err)
		s.False(found, "%+v", other)
	}
}

func (s *StoreSuite) TestARefusalIsForgottenAfterItsTTL() {
	leg := CallLeg{CustomerID: "acme", StreamAppPK: 7, CallType: "phone", CallID: "call", StreamSessionID: "session-1"}
	s.Require().NoError(s.store.CallLegs.MarkRefused(s.ctx, "acme", 7, "phone", "call", "session-1", time.Second))

	s.Eventually(func() bool {
		found, err := s.store.CallLegs.Refused(s.ctx, leg)
		s.Require().NoError(err)
		return !found
	}, 5*time.Second, 50*time.Millisecond)
}
