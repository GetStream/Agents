package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectorAuditEventValidate(t *testing.T) {
	valid := func() ConnectorAuditEvent {
		return ConnectorAuditEvent{CustomerID: "acme-app", ConnectionID: "connection", Action: AuditGrantCreated}
	}
	for _, action := range []string{AuditGrantCreated, AuditGrantRefreshed, AuditGrantRevoked, AuditProxyCall, AuditTokenExport} {
		event := valid()
		event.Action = action
		require.NoError(t, event.Validate(), action)
	}
	reapproved := valid()
	reapproved.Action, reapproved.Tools = AuditToolReapproved, []ApprovedTool{{Name: "create_issue", SchemaDigest: "d2"}}
	require.NoError(t, reapproved.Validate())

	for _, tc := range []struct {
		field  string
		broken func(*ConnectorAuditEvent)
	}{
		{"customer_id", func(e *ConnectorAuditEvent) { e.CustomerID = "" }},
		{"connection_id", func(e *ConnectorAuditEvent) { e.ConnectionID = "" }},
		{"action", func(e *ConnectorAuditEvent) { e.Action = "grant_lost" }},
		{"tools", func(e *ConnectorAuditEvent) { e.Action = AuditToolReapproved }},
		{"tools", func(e *ConnectorAuditEvent) { e.Tools = []ApprovedTool{{Name: "create_issue"}} }},
	} {
		event := valid()
		tc.broken(&event)
		var invalid *ValidationError
		require.True(t, errors.As(event.Validate(), &invalid), tc.field)
		require.Equal(t, tc.field, invalid.Field)
	}
}
