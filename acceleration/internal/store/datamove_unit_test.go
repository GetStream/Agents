package store

import (
	"encoding/json"
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

// AI-3674 R1.1: the http source would run an imported http connector's tools before AI-3675's
// call-time checks, so an import refuses one until AI-3676 lifts it. An MCP one is still stored.
func (s *ImportableDefinitionSuite) TestAnHTTPConnectorIsRefusedAndAnMCPOneKept() {
	http := `{"id": "custom_crm", "revision": 1, "name": "CRM", "endpoints": {"http": "https://api.crm.example"},
		"schemes": ["bearer"], "sources": [{"kind": "http", "endpoint": "http",
		"http_tools": [{"name": "delete_item", "description": "Deletes an item.", "method": "DELETE", "path": "/items/{id}",
		"parameters": {"id": {"type": "string", "required": true}}}]}]}`
	mcp := `{"id": "custom_crm", "revision": 1, "name": "CRM", "endpoints": {"mcp": "https://mcp.crm.example/mcp"},
		"schemes": ["bearer"], "sources": [{"kind": "mcp", "endpoint": "mcp"}]}`

	_, err := importableDefinition(json.RawMessage(`{"id": "custom_crm", "manifest": ` + http + `}`))

	s.ErrorIs(err, ErrNotACustomConnector)
	s.ErrorContains(err, "an http connector cannot be imported yet")

	row, err := importableDefinition(json.RawMessage(`{"id": "custom_crm", "manifest": ` + mcp + `}`))

	s.Require().NoError(err)
	var kept map[string]json.RawMessage
	s.Require().NoError(json.Unmarshal(row, &kept))
	s.JSONEq(mcp, string(kept["manifest"]))
}
