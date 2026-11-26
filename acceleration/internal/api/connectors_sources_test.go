package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ConnectorSourcesSuite covers the sources a Connector shows, without a store.
type ConnectorSourcesSuite struct {
	suite.Suite
}

func TestConnectorSourcesSuite(t *testing.T) {
	suite.Run(t, new(ConnectorSourcesSuite))
}

// sourcesJSON is what a caller reads of the sources of a manifest with these source rules.
func (s *ConnectorSourcesSuite) sourcesJSON(rules ...core.SourceRule) string {
	shown := connectorOf(store.ConnectorDefinition{Manifest: core.Manifest{Sources: rules}}, "")
	raw, err := json.Marshal(shown)
	s.Require().NoError(err)
	var decoded struct {
		Sources json.RawMessage `json:"sources"`
	}
	s.Require().NoError(json.Unmarshal(raw, &decoded))
	return string(decoded.Sources)
}

// Defect: one entry per kind rather than per source, or the order lost, so a connector with an
// http source beside its mcp one reads as only one of them.
func (s *ConnectorSourcesSuite) TestEverySourceIsShownByItsKindInTheManifestsOrder() {
	s.JSONEq(`[{"kind":"mcp"},{"kind":"http"}]`, s.sourcesJSON(
		core.SourceRule{Kind: core.SourceKindMCP, Endpoint: "mcp"}, core.SourceRule{Kind: core.SourceKindHTTP, Endpoint: "http"}))
	s.JSONEq(`[{"kind":"http"},{"kind":"mcp"}]`, s.sourcesJSON(
		core.SourceRule{Kind: core.SourceKindHTTP, Endpoint: "http"}, core.SourceRule{Kind: core.SourceKindMCP, Endpoint: "mcp"}))
}

// Defect: a connector with no source answering null, which a client reads as absent.
func (s *ConnectorSourcesSuite) TestAConnectorWithNoSourceShowsAnEmptyList() {
	s.Equal(`[]`, s.sourcesJSON())
}

// Defect: a source's endpoint, headers or tools copied into the answer with its kind.
func (s *ConnectorSourcesSuite) TestASourceShowsNothingButItsKind() {
	rule := core.SourceRule{
		Kind: core.SourceKindHTTP, Endpoint: "http", Headers: map[string]string{"X-Team": "secret-team"},
		Tools:     []core.ToolRule{{Name: "scoped", NeedsScopes: []string{"s"}}},
		HTTPTools: []core.HTTPTool{{Name: "get_forecast", Method: "GET", Path: "/v1/forecast"}},
	}
	s.JSONEq(`[{"kind":"http"}]`, s.sourcesJSON(rule))
}
