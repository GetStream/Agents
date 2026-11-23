//go:build integration

package store

import "time"

func (s *StoreSuite) TestABridgeIsClaimedOnceFromAnyShardAndSweptOnceExpired() {
	waiting := &CallBridge{Token: newID(), CustomerID: "acme", Vendor: "plivo", TrunkURI: "sip:x@example.com", ExpiresAt: time.Now().Add(time.Hour)}
	lapsed := &CallBridge{Token: newID(), CustomerID: "acme", Vendor: "plivo", TrunkURI: "sip:y@example.com", ExpiresAt: time.Now().Add(-time.Minute)}
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.CallBridges.Insert(s.ctx, waiting)) }), "Insert")
	s.Require().NoError(s.store.CallBridges.Insert(s.ctx, lapsed))

	var claimed CallBridge
	var err error
	s.Equal(len(s.store.shards()), s.countQueries(func() {
		claimed, err = s.store.CallBridges.Claim(s.ctx, waiting.Token)
	}), "Claim asks every shard once")
	s.Require().NoError(err)
	s.Equal("sip:x@example.com", claimed.TrunkURI)
	_, err = s.store.CallBridges.Claim(s.ctx, waiting.Token)
	s.Error(err, "a token works once")
	_, err = s.store.CallBridges.Claim(s.ctx, lapsed.Token)
	s.Error(err, "an expired bridge is not handed out")

	var swept int64
	s.Equal(1, s.countQueries(func() {
		swept, err = s.store.CallBridges.Sweep(s.ctx)
	}), "Sweep is this shard's")
	s.Require().NoError(err)
	s.Equal(int64(1), swept)
}
