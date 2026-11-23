package core

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// CustomConnector is what a custom connector is made of: the fields an app sends to create one,
// with an MCP endpoint or an http base URL and tools. The rest of its manifest is fixed
// (CustomManifest).
type CustomConnector struct {
	ID          string
	Name        string
	Category    string
	Description string
	// Endpoint is the MCP server.
	Endpoint string
	// BaseURL is the API an http connector's tools call, each at its path under it, and Tools
	// are those tools. Set, Endpoint is not.
	BaseURL string
	Tools   []HTTPTool
	Schemes []string
	Scopes  []string
	Client  ClientPolicy
}

// CustomManifest is the manifest a custom connector is stored as, or why it cannot be: one MCP
// source at Endpoint, or one http source at BaseURL with Tools, and of the client only its
// registration, auth_method and alg. Which schemes the deployment has and whether Endpoint or
// BaseURL is public are the caller's to check.
//
// SourceKindMCP is both the tool source and the endpoint role it runs against, as the built-in
// manifests write them (internal/connectors/providers/linear.yaml: sources: [{kind: mcp,
// endpoint: mcp}]), and SourceKindHTTP is both for an http connector. Its role is not
// api_base, which a connection's direct call goes to (proxyBase, internal/api/connection_proxy.go),
// so an http connector takes no direct call, as an MCP one takes none.
func CustomManifest(c CustomConnector) (Manifest, error) {
	// An operator client is the deployment's own, read from the variables a built-in's
	// client.env names, and the deployment has none registered with an app's own server.
	if slices.Contains(c.Client.Registration, ClientOperator) {
		return Manifest{}, stack.Wrap(errors.New("client.registration cannot be operator for a custom connector: this deployment has no client registered with it"))
	}
	// oauth2_code tries only the mechanisms client.registration names and fails with ErrNoClient when it
	// names none (pickClient in internal/connectors/schemes/oauth2code/client.go), so such a
	// definition could be stored and never connected.
	if slices.Contains(c.Schemes, "oauth2_code") && len(c.Client.Registration) == 0 {
		return Manifest{}, stack.Wrap(errors.New("client.registration is required with oauth2_code: name how the OAuth client is registered (customer, cimd or dcr)"))
	}
	// oauth2_client_credentials gets its token URL from endpoints.token (tokenEndpoint in
	// internal/connectors/schemes/oauth2cc/scheme.go), and a custom connector sets only the MCP endpoint or
	// the http base URL, so such a definition could be stored and never connected.
	if slices.Contains(c.Schemes, "oauth2_client_credentials") {
		return Manifest{}, stack.Wrap(errors.New("oauth2_client_credentials is not available for a custom connector: it needs a token endpoint a custom connector cannot set"))
	}
	manifest := Manifest{
		ID: c.ID,
		// Validated at the first revision; the store numbers the one it is stored as.
		Revision:    1,
		Name:        strings.TrimSpace(c.Name),
		Category:    strings.TrimSpace(c.Category),
		Description: strings.TrimSpace(c.Description),
		Endpoints:   map[string]string{SourceKindMCP: c.Endpoint},
		Schemes:     c.Schemes,
		Client:      ClientPolicy{Registration: c.Client.Registration, AuthMethod: c.Client.AuthMethod, Alg: c.Client.Alg},
		Scopes:      ScopePolicy{List: c.Scopes},
		Sources:     []SourceRule{{Kind: SourceKindMCP, Endpoint: SourceKindMCP}},
	}
	if c.BaseURL != "" || len(c.Tools) > 0 {
		if c.Endpoint != "" {
			return Manifest{}, stack.Wrap(errors.New("a custom connector has an MCP endpoint or an http base_url and tools, not both"))
		}
		// An http source offers only the tools it lists, so one with none could be stored and
		// never offer a tool.
		if len(c.Tools) == 0 {
			return Manifest{}, stack.Wrap(errors.New("an http custom connector needs at least one tool"))
		}
		// oauth2_code finds the authorization server from the manifest's issuer, authorize and token
		// endpoints, which a custom connector cannot set, or from its MCP endpoint, which an http
		// one has none of (discover in internal/connectors/schemes/oauth2code/discovery.go).
		if slices.Contains(c.Schemes, "oauth2_code") {
			return Manifest{}, stack.Wrap(errors.New("oauth2_code is not available for an http custom connector: it needs authorization server endpoints an http connector cannot set"))
		}
		manifest.Endpoints = map[string]string{SourceKindHTTP: c.BaseURL}
		manifest.Sources = []SourceRule{{Kind: SourceKindHTTP, Endpoint: SourceKindHTTP, HTTPTools: c.Tools}}
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// AsCustom is m as a custom connector's manifest under id, or why it cannot be one: m rebuilt by
// CustomManifest from its own custom fields must be m, so a manifest with anything a custom
// connector cannot set (an operator client, client.env, its own token endpoint, a hook) is
// refused. For a manifest that did not come through CustomManifest, such as an imported one.
func (m Manifest) AsCustom(id string) (Manifest, error) {
	var tools []HTTPTool
	if source := HTTPSource(m.Sources); source != nil {
		tools = source.HTTPTools
	}
	rebuilt, err := CustomManifest(CustomConnector{
		ID: id, Name: m.Name, Category: m.Category, Description: m.Description,
		Endpoint: m.Endpoints[SourceKindMCP], BaseURL: m.Endpoints[SourceKindHTTP], Tools: tools,
		Schemes: m.Schemes, Scopes: m.Scopes.List, Client: m.Client,
	})
	if err != nil {
		return Manifest{}, err
	}
	rebuilt.Revision = m.Revision
	// Compared as JSON, the form a manifest is stored in, so an empty list and a missing one,
	// which read back the same, are the same.
	want, err := json.Marshal(rebuilt)
	if err != nil {
		return Manifest{}, stack.Wrap(err)
	}
	got, err := json.Marshal(m)
	if err != nil {
		return Manifest{}, stack.Wrap(err)
	}
	if string(want) != string(got) {
		return Manifest{}, stack.Wrap(errors.New("it is not a manifest a custom connector can have: a custom connector has an id, a name, " +
			"a category, a description, an MCP endpoint or an http base_url and tools, schemes, scopes and a client's registration, " +
			"auth_method and alg, and nothing else"))
	}
	return rebuilt, nil
}
