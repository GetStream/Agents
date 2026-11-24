package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

// ImportableDefinitionSuite is which imported connector definitions an import would store
// (importableDefinition), without a database.
type ImportableDefinitionSuite struct {
	suite.Suite
}

func TestImportableDefinitionSuite(t *testing.T) {
	suite.Run(t, new(ImportableDefinitionSuite))
}

// importedHTTP is a manifest the API stores for an http connector whose API is at a public IP literal, so checking it
// resolves nothing.
const importedHTTP = `{"id": "custom_crm", "revision": 1, "name": "CRM", "endpoints": {"http": "https://8.8.8.8/api"},
	"schemes": ["bearer"], "sources": [{"kind": "http", "endpoint": "http",
	"http_tools": [{"name": "delete_item", "description": "Deletes an item.", "method": "DELETE", "path": "/items/{id}",
	"parameters": {"id": {"type": "string", "required": true}}}]}]}`

// AI-3676 lifts AI-3674's refusal: an http connector is imported as the API stores one, and an MCP one as before.
func (s *ImportableDefinitionSuite) TestAnHTTPConnectorAndAnMCPOneAreKept() {
	mcp := `{"id": "custom_crm", "revision": 1, "name": "CRM", "endpoints": {"mcp": "https://mcp.crm.example/mcp"},
		"schemes": ["bearer"], "sources": [{"kind": "mcp", "endpoint": "mcp"}]}`
	for _, manifest := range []string{importedHTTP, mcp} {
		row, err := importableDefinition(context.Background(), json.RawMessage(`{"id": "custom_crm", "manifest": `+manifest+`}`))

		s.Require().NoError(err)
		var kept map[string]json.RawMessage
		s.Require().NoError(json.Unmarshal(row, &kept))
		s.JSONEq(manifest, string(kept["manifest"]))
	}
}

// AI-3674 S1.5: an import checks an http connector's base URL as the API does, with AC-7 of AI-3669's addresses, and
// answers each the same, so an import cannot tell a private address from one that does not resolve.
func (s *ImportableDefinitionSuite) TestAnHTTPConnectorWhoseAPIIsNotPublicIsRefused() {
	for _, base := range []string{"https://127.0.0.1", "https://10.0.0.1", "https://169.254.169.254", "https://[::1]",
		"https://router-probe.invalid"} {
		manifest := strings.Replace(importedHTTP, "https://8.8.8.8/api", base, 1)

		_, err := importableDefinition(context.Background(), json.RawMessage(`{"id": "custom_crm", "manifest": `+manifest+`}`))

		s.ErrorIs(err, ErrNotACustomConnector, base)
		s.EqualError(err, `store: imported connector definition "custom_crm": `+ErrNotACustomConnector.Error()+
			": base_url must be a public https URL without userinfo, query or fragment", base)
	}
}
