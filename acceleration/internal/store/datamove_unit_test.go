package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
	// 2048 bytes, the cap itself: pins the cap as maxHTTPPath, not one under it.
	atTheCap := strings.Replace(importedHTTP, "https://8.8.8.8/api", "https://8.8.8.8/"+strings.Repeat("a", 2032), 1)
	for _, manifest := range []string{importedHTTP, mcp, atTheCap} {
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
		"https://router-probe.invalid",
		// S1.2: refused by core's rules too, which would answer each in their own words.
		"http://8.8.8.8/api", "HTTPS://8.8.8.8/api", "https://8.8.8.8/{x}", "https://8.8.8.8/" + strings.Repeat("a", 2048),
		// 2049 bytes, one over the cap: pins the cap as maxHTTPPath, not maxHTTPPath+1.
		"https://8.8.8.8/" + strings.Repeat("a", 2033)} {
		manifest := strings.Replace(importedHTTP, "https://8.8.8.8/api", base, 1)

		_, err := importableDefinition(context.Background(), json.RawMessage(`{"id": "custom_crm", "manifest": `+manifest+`}`))

		s.ErrorIs(err, ErrNotACustomConnector, base)
		s.EqualError(err, `store: imported connector definition "custom_crm": `+ErrNotACustomConnector.Error()+
			": base_url must be a public https URL without userinfo, query or fragment", base)
	}
}

// AI-3676 S1.1: an import has no request body limit to keep an http connector small, so core's caps refuse what the
// API's schema would: a parameter's description over 1000 bytes and a base URL over 2048.
func (s *ImportableDefinitionSuite) TestAnOversizedHTTPConnectorIsRefused() {
	for name, manifest := range map[string]string{
		"a 2 MiB parameter description": strings.Replace(importedHTTP, `"parameters": {"id": {"type": "string", "required": true}}`,
			`"parameters": {"id": {"type": "string", "required": true}, "q": {"type": "string", "description": "`+strings.Repeat("x", 2<<20)+`"}}`, 1),
		"a 4 KiB base_url": strings.Replace(importedHTTP, "https://8.8.8.8/api", "https://8.8.8.8/"+strings.Repeat("a", 4096), 1),
	} {
		_, err := importableDefinition(context.Background(), json.RawMessage(`{"id": "custom_crm", "manifest": `+manifest+`}`))

		s.ErrorIs(err, ErrNotACustomConnector, name)
	}
}

// definitionRow is a connector_definitions row of custom_acme as an export carries it, with
// extra, JSON members, added or written over, and the manifest's own members manifest.
func definitionRow(extra, manifest string) json.RawMessage {
	return json.RawMessage(`{"customer_id": "moved", "id": "custom_acme", "revision": 2, "name": "Acme", "category": "", "description": "",
		"created_at": "2026-11-01T00:00:00Z", "sync_hash": "", "manifest": {"id": "custom_acme", "revision": 2, "name": "Acme",
		"endpoints": {"mcp": "https://mcp.acme.example/mcp"}, "sources": [{"kind": "mcp", "endpoint": "mcp"}], ` + manifest + `}` + extra + `}`)
}

func columnOf(t *testing.T, row json.RawMessage, column string) any {
	var fields map[string]any
	require.NoError(t, json.Unmarshal(row, &fields))
	return fields[column]
}

func TestAnImportedDefinitionPastAnAPILimitIsRefused(t *testing.T) {
	_, err := importableDefinition(context.Background(), definitionRow("", `"schemes": ["bearer"], "description": "`+strings.Repeat("é", 1001)+`"`))

	require.ErrorIs(t, err, ErrNotACustomConnector)
	require.ErrorContains(t, err, `"custom_acme"`)
	require.ErrorContains(t, err, "description is longer than 1000 characters")
}

// The columns a definition is listed by are what the API writes from the manifest, so a row whose
// own name breaks the API's limits arrives with the manifest's.
func TestAnImportedDefinitionsColumnsAreItsManifests(t *testing.T) {
	long := strings.Repeat("x", 500)
	row := definitionRow(`, "name": "`+long+`", "category": "`+long+`", "description": "`+long+`"`,
		`"schemes": ["bearer"], "category": "CRM", "description": "Acme's CRM"`)

	written, err := importableDefinition(context.Background(), row)

	require.NoError(t, err)
	require.Equal(t, "Acme", columnOf(t, written, "name"))
	require.Equal(t, "CRM", columnOf(t, written, "category"))
	require.Equal(t, "Acme's CRM", columnOf(t, written, "description"))
}

func TestAnImportedDefinitionsSyncHashIsPrintableASCIIOfAtMost128(t *testing.T) {
	for _, hash := range []string{`""`, `null`, `"` + strings.Repeat("!~", 64) + `"`} {
		_, err := importableDefinition(context.Background(), definitionRow(`, "sync_hash": `+hash, `"schemes": ["bearer"]`))
		require.NoError(t, err, hash)
	}
	for _, hash := range []string{`"` + strings.Repeat("a", 129) + `"`, `"a b"`, `"a\u0000"`, `"é"`, `12`, `["a"]`} {
		_, err := importableDefinition(context.Background(), definitionRow(`, "sync_hash": `+hash, `"schemes": ["bearer"]`))
		require.ErrorIs(t, err, ErrInvalidImport, hash)
		require.ErrorContains(t, err, "sync_hash is not printable ASCII without a space of at most 128 characters", hash)
	}
}

// A row from a release before sync hashes carries none.
func TestAnImportedDefinitionWithoutASyncHashIsImportable(t *testing.T) {
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(definitionRow("", `"schemes": ["bearer"]`), &fields))
	delete(fields, "sync_hash")
	row, err := json.Marshal(fields)
	require.NoError(t, err)

	_, err = importableDefinition(context.Background(), row)

	require.NoError(t, err)
}

func TestAClientCredentialsDefinitionIsSkippedByItsKey(t *testing.T) {
	for _, schemes := range []string{`["oauth2_client_credentials"]`, `["oauth2_code", "oauth2_client_credentials"]`} {
		require.Equal(t, `connector_definitions:{"id":"custom_acme","revision":2}`,
			SkippedRow("connector_definitions", definitionRow("", `"schemes": `+schemes)), schemes)
	}
}

// What is skipped is only a definition with the scheme: any other is imported or refused as
// before.
func TestOnlyAClientCredentialsDefinitionIsSkipped(t *testing.T) {
	for _, schemes := range []string{`["bearer"]`, `["oauth2_code"]`, `[]`, `["oauth2_client_credentials_x"]`} {
		require.Empty(t, SkippedRow("connector_definitions", definitionRow("", `"schemes": `+schemes)), schemes)
	}
	require.Empty(t, SkippedRow("agent_configs", definitionRow("", `"schemes": ["oauth2_client_credentials"]`)), "another table")
	for _, row := range []string{`["custom_acme"]`, `{"id": "custom_acme"}`, `{"id": "custom_acme", "manifest": "oauth2_client_credentials"}`,
		`{"id": "custom_acme", "Manifest": {"schemes": ["oauth2_client_credentials"]}}`} {
		require.Empty(t, SkippedRow("connector_definitions", json.RawMessage(row)), row)
	}
}

// A row is named only when it is one the API could have stored, by the id and revision it
// holds; any other is the import's to refuse, so its id is never held or echoed.
func TestAClientCredentialsDefinitionWithAnIDOrRevisionTheAPIRefusesIsNotNamed(t *testing.T) {
	for _, extra := range []string{`, "id": "github"`, `, "id": "custom_"`, `, "id": "Custom_acme"`, `, "id": 12`, `, "id": null`,
		`, "id": "custom_a` + strings.Repeat("x", 57) + `"`, `, "id": "custom_a` + strings.Repeat("x", 1<<16) + `"`,
		`, "revision": 0`, `, "revision": -1`, `, "revision": "2"`, `, "revision": 1.5`, `, "revision": null`} {
		require.Empty(t, SkippedRow("connector_definitions", definitionRow(extra, `"schemes": ["oauth2_client_credentials"]`)), extra[:min(len(extra), 40)])
	}
	longest := "custom_a" + strings.Repeat("x", 56)
	require.Equal(t, `connector_definitions:{"id":"`+longest+`","revision":1}`,
		SkippedRow("connector_definitions", definitionRow(`, "id": "`+longest+`", "revision": 1`, `"schemes": ["oauth2_client_credentials"]`)))
}
