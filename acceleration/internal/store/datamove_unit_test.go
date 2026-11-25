package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

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
	_, err := importableDefinition(definitionRow("", `"schemes": ["bearer"], "description": "`+strings.Repeat("é", 1001)+`"`))

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

	written, err := importableDefinition(row)

	require.NoError(t, err)
	require.Equal(t, "Acme", columnOf(t, written, "name"))
	require.Equal(t, "CRM", columnOf(t, written, "category"))
	require.Equal(t, "Acme's CRM", columnOf(t, written, "description"))
}

func TestAnImportedDefinitionsSyncHashIsPrintableASCIIOfAtMost128(t *testing.T) {
	for _, hash := range []string{`""`, `null`, `"` + strings.Repeat("!~", 64) + `"`} {
		_, err := importableDefinition(definitionRow(`, "sync_hash": `+hash, `"schemes": ["bearer"]`))
		require.NoError(t, err, hash)
	}
	for _, hash := range []string{`"` + strings.Repeat("a", 129) + `"`, `"a b"`, `"a\u0000"`, `"é"`, `12`, `["a"]`} {
		_, err := importableDefinition(definitionRow(`, "sync_hash": `+hash, `"schemes": ["bearer"]`))
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

	_, err = importableDefinition(row)

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
