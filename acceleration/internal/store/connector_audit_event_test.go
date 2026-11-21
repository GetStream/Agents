//go:build integration

package store

import (
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
)

func (s *StoreSuite) TestTheAuditListsTheCustomersRowsNewestFirstWholeOrForOneConnection() {
	one := s.connection("acme-app", nil)
	two := s.connection("acme-app", nil)
	for _, event := range []*ConnectorAuditEvent{
		{CustomerID: "acme-app", ConnectionID: one.ID, ConnectorID: "acme", OwnerType: OwnerApp, Action: AuditGrantCreated, Reason: AuditReasonConsent, Revision: 2},
		{CustomerID: "acme-app", ConnectionID: two.ID, ConnectorID: "acme", OwnerType: OwnerApp, Action: AuditGrantCreated, Reason: AuditReasonCredentials, Revision: 2},
		{CustomerID: "acme-app", ConnectionID: one.ID, ConnectorID: "acme", OwnerType: OwnerApp, Action: AuditGrantRefreshed, Revision: 3},
		{CustomerID: "other-app", ConnectionID: one.ID, ConnectorID: "acme", OwnerType: OwnerApp, Action: AuditGrantRevoked},
	} {
		s.Require().NoError(s.store.ConnectorAuditEvents.Insert(s.ctx, event))
	}

	all, err := s.store.ConnectorAuditEvents.Select(s.ctx, "acme-app", AuditFilter{})
	s.Require().NoError(err)
	ofOne, err := s.store.ConnectorAuditEvents.Select(s.ctx, "acme-app", AuditFilter{ConnectionID: one.ID, Limit: 1})
	s.Require().NoError(err)
	rest, err := s.store.ConnectorAuditEvents.Select(s.ctx, "acme-app", AuditFilter{ConnectionID: one.ID, Limit: 1,
		After: &AuditPosition{CreatedAt: ofOne[0].CreatedAt, ID: ofOne[0].ID}})

	s.Require().NoError(err)

	s.Len(all, 3, "another customer's row is not listed")
	s.Equal(AuditGrantRefreshed, all[0].Action)
	s.Require().Len(ofOne, 2)
	s.Equal(AuditGrantRefreshed, ofOne[0].Action)
	s.Require().Len(rest, 1)
	s.Equal(AuditGrantCreated, rest[0].Action)
	s.Equal(AuditReasonConsent, rest[0].Reason)
}

// TestAnAuditRowKeepsTheFingerprintsOfItsTokens: a grant row's fingerprints come back with
// it, and a row written without them comes back without.
func (s *StoreSuite) TestAnAuditRowKeepsTheFingerprintsOfItsTokens() {
	connection := s.connection("acme-app", nil)
	expires := s.base.Add(12 * time.Hour)
	refreshed := &ConnectorAuditEvent{CustomerID: "acme-app", ConnectionID: connection.ID, ConnectorID: "acme",
		OwnerType: OwnerApp, Action: AuditGrantRefreshed, Revision: 3,
		Credential: AuditCredential(core.CredentialChange{
			Previous: core.CredentialFingerprints{Access: "0a0a0a0a", Refresh: "1b1b1b1b"},
			Current:  core.CredentialFingerprints{Access: "2c2c2c2c", Refresh: "3d3d3d3d", AccessExpiresAt: expires},
		})}
	s.Require().NoError(s.store.ConnectorAuditEvents.Insert(s.ctx, refreshed))
	s.Require().NoError(s.store.ConnectorAuditEvents.Insert(s.ctx, &ConnectorAuditEvent{CustomerID: "acme-app",
		ConnectionID: connection.ID, ConnectorID: "acme", OwnerType: OwnerApp, Action: AuditProxyCall}))

	rows, err := s.store.ConnectorAuditEvents.Select(s.ctx, "acme-app", AuditFilter{ConnectionID: connection.ID})

	s.Require().NoError(err)
	s.Require().Len(rows, 2)
	s.Nil(rows[0].Credential, "the proxy call names no token")
	got := rows[1].Credential
	s.Require().NotNil(got)
	s.Equal("0a0a0a0a", got.PreviousAccessFingerprint)
	s.Equal("2c2c2c2c", got.AccessFingerprint)
	s.Equal("1b1b1b1b", got.PreviousRefreshFingerprint)
	s.Equal("3d3d3d3d", got.RefreshFingerprint)
	s.True(got.Rotated)
	s.True(expires.Equal(got.AccessExpiresAt))
	s.True(got.RefreshExpiresAt.IsZero(), "an expiry the provider did not give stays unknown")
}

func (s *StoreSuite) TestAnAuditRowWithAnUnknownActionIsRefused() {
	err := s.store.ConnectorAuditEvents.Insert(s.ctx, &ConnectorAuditEvent{
		CustomerID: "acme-app", ConnectionID: "one", ConnectorID: "acme", OwnerType: OwnerApp, Action: "grant_renamed",
	})

	var invalid *ValidationError
	s.Require().ErrorAs(err, &invalid)
	s.Equal("action", invalid.Field)
	rows, err := s.store.ConnectorAuditEvents.Select(s.ctx, "acme-app", AuditFilter{})
	s.Require().NoError(err)
	s.Empty(rows, "nothing was stored")
}

func (s *StoreSuite) TestTheAuditTakesTheProxyAndExportRowsThatComeLater() {
	status, latency := 200, int64(31)
	for _, event := range []*ConnectorAuditEvent{
		{CustomerID: "acme-app", ConnectionID: "one", ConnectorID: "acme", OwnerType: OwnerApp, Action: AuditProxyCall,
			StatusCode: &status, LatencyMs: &latency, Target: "api.acme.test"},
		{CustomerID: "acme-app", ConnectionID: "one", ConnectorID: "acme", OwnerType: OwnerApp, Action: AuditTokenExport},
	} {
		s.Require().NoError(s.store.ConnectorAuditEvents.Insert(s.ctx, event))
	}

	found, err := s.store.ConnectorAuditEvents.Select(s.ctx, "acme-app", AuditFilter{})

	s.Require().NoError(err)
	s.Require().Len(found, 2)
	s.Equal("api.acme.test", found[1].Target)
	s.Equal(200, *found[1].StatusCode)
}

func (s *StoreSuite) TestConnectorAuditEventQueryBudgets() {
	connection := s.connection("acme-app", nil)
	event := func() *ConnectorAuditEvent {
		return &ConnectorAuditEvent{CustomerID: "acme-app", ConnectionID: connection.ID, ConnectorID: "acme",
			OwnerType: OwnerApp, Action: AuditProxyCall}
	}
	for range 30 {
		s.Require().NoError(s.store.ConnectorAuditEvents.Insert(s.ctx, event()))
	}

	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ConnectorAuditEvents.Insert(s.ctx, event())) }), "Insert")
	granted := event()
	granted.Action, granted.Credential = AuditGrantCreated, &ConnectorAuditCredential{AccessFingerprint: "a1"}
	s.Equal(1, s.countQueries(func() { s.Require().NoError(s.store.ConnectorAuditEvents.Insert(s.ctx, granted)) }), "Insert with a credential")
	s.Equal(1, s.countQueries(func() {
		rows, err := s.store.ConnectorAuditEvents.Select(s.ctx, "acme-app", AuditFilter{ConnectionID: connection.ID, Limit: 10})
		s.Require().NoError(err)
		s.Len(rows, 11)
	}), "Select")
}
