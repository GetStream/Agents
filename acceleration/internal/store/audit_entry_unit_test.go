package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAuditEntryValidate(t *testing.T) {
	valid := func() AuditEntry {
		return AuditEntry{CustomerID: "acme", ResourceType: AuditPolicy, ResourceID: "policy-1", Action: AuditUpdated,
			Changes: []AuditChange{{Field: "name", Before: "a", After: "b"}}}
	}
	entry := valid()
	require.NoError(t, entry.Validate())
	synced := AuditEntry{CustomerID: "acme", ResourceType: AuditAgentConfig, ResourceID: "config-1", Action: AuditSynced}
	require.NoError(t, synced.Validate(), "a sync that moved nothing is still recorded")
	deployment := AuditEntry{ResourceType: AuditResourcePriceList, ResourceID: "list", Action: AuditCreated,
		Changes: []AuditChange{{Field: "name", After: "list"}}}
	require.NoError(t, deployment.Validate())

	for name, broken := range map[string]struct {
		field string
		apply func(*AuditEntry)
	}{
		"unknown resource":     {"resource_type", func(e *AuditEntry) { e.ResourceType = "galaxy" }},
		"no resource id":       {"resource_id", func(e *AuditEntry) { e.ResourceID = "" }},
		"no customer":          {"customer_id", func(e *AuditEntry) { e.CustomerID = "" }},
		"deployment with one":  {"customer_id", func(e *AuditEntry) { e.ResourceType = AuditResourcePriceList }},
		"unknown action":       {"action", func(e *AuditEntry) { e.Action = "renamed" }},
		"update that moved no": {"changes", func(e *AuditEntry) { e.Changes = nil }},
	} {
		entry := valid()
		broken.apply(&entry)
		var invalid *ValidationError
		require.True(t, errors.As(entry.Validate(), &invalid), name)
		require.Equal(t, broken.field, invalid.Field, name)
	}
}

// Defect: an SDK naming its release read as the API, a known client in another case or with
// spaces around it read as unknown, a name that only resembles the prefix read as the SDK, or a
// caller naming itself router counted as work the router started itself.
func TestAuditSource(t *testing.T) {
	for header, want := range map[string]string{
		"stream-agents-go-v0.3.0":         AuditSourceSDK,
		"Stream-Agents-JS-v0.8.0-browser": AuditSourceSDK,
		" stream-agents-js-v0.8.0-node ":  AuditSourceSDK,
		"sdk":                             AuditSourceSDK,
		" CLI ":                           AuditSourceCLI,
		"dashboard":                       AuditSourceDashboard,
		"api":                             AuditSourceAPI,
		"":                                AuditSourceAPI,
		"manage":                          AuditSourceAPI,
		"router":                          AuditSourceAPI,
		"stream-agents":                   AuditSourceAPI,
		"stream-chat-js-v8.40.0-browser":  AuditSourceAPI,
		"my-stream-agents-go-v0.3.0":      AuditSourceAPI,
		"stream-go-client-7.0.0":          AuditSourceAPI,
	} {
		require.Equal(t, want, AuditSource(header), "%q", header)
	}
}
