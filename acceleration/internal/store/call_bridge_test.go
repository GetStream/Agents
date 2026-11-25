//go:build integration

package store

import "time"

func (s *StoreSuite) TestABridgeIsClaimedOnceAndSweptOnceExpired() {
	waiting := &CallBridge{Token: newID(), CustomerID: "acme", Vendor: "plivo", TrunkURI: "sip:x@example.com", ExpiresAt: time.Now().Add(time.Hour)}
	lapsed := &CallBridge{Token: newID(), CustomerID: "acme", Vendor: "plivo", TrunkURI: "sip:y@example.com", ExpiresAt: time.Now().Add(-time.Minute)}
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.CallBridges.Insert(s.ctx, waiting)) }), "Insert")
	s.Require().NoError(s.store.CallBridges.Insert(s.ctx, lapsed))

	var claimed CallBridge
	var err error
	s.Equal(1, s.countQueries(func() {
		claimed, err = s.store.CallBridges.Claim(s.ctx, waiting.Token)
	}), "Claim is this shard's")
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

func (s *StoreSuite) TestABridgeIsClaimedByItsTokenWithOnlyItsHashStored() {
	token := newID()
	s.Require().NoError(s.store.CallBridges.Insert(s.ctx, &CallBridge{Token: token, CustomerID: "acme", Vendor: "plivo",
		TrunkURI: "sip:x@example.com", CallID: "call", ExpiresAt: time.Now().Add(time.Hour)}))

	var rows []string
	s.Require().NoError(s.store.shard.NewSelect().ColumnExpr("to_jsonb(cb)::text").TableExpr("call_bridges AS cb").
		Where("cb.customer_id = ?", "acme").Scan(s.ctx, &rows))
	s.Require().Len(rows, 1)
	s.NotContains(rows[0], token, "the bridge holds the token its fetch presents")
	claimed, err := s.store.CallBridges.Claim(s.ctx, token)
	s.Require().NoError(err)
	s.Equal("call", claimed.CallID)
}

// The release before runs beside this one during a deploy and after a rollback: the token and
// trunk_password columns it reads and writes are still there, so it still parks, claims, places
// and opens by its plain token.
func (s *StoreSuite) TestTheReleaseBeforeStillParksAndPlacesByItsPlainToken() {
	expires := time.Now().Add(time.Hour)
	for _, query := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO call_bridges (token, customer_id, vendor, trunk_uri, trunk_username, trunk_password, call_id, created_at, expires_at)
			VALUES (?, 'acme', 'plivo', 'sip:x@example.com', 'user', 'secret', 'call', now(), ?)`, []any{"old-bridge", expires}},
		{`DELETE FROM call_bridges WHERE token = ? AND expires_at > now()`, []any{"old-bridge"}},
		{`INSERT INTO call_legs (customer_id, call_type, call_id, vendor, direction, local_number, remote_number, token, reports, placed_at)
			VALUES ('acme', 'default', 'call', 'telnyx', 'outbound', '+15125550100', '+15125550199', ?, true, now())`, []any{"old-leg"}},
		{`UPDATE call_legs SET dialed_at = now() WHERE token = ? AND dialed_at IS NULL`, []any{"old-leg"}},
		{`UPDATE call_legs AS cl SET vendor_status = 'ringing' WHERE cl.token = ?`, []any{"old-leg"}},
		{`UPDATE call_legs SET token = NULL WHERE token IS NOT NULL AND placed_at < now() + interval '1 minute'`, nil},
	} {
		result, err := s.store.shard.ExecContext(s.ctx, query.sql, query.args...)
		s.Require().NoError(err, query.sql)
		affected, err := result.RowsAffected()
		s.Require().NoError(err)
		s.Equal(int64(1), affected, query.sql)
	}
}
