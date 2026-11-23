package core

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// CustomConnector is what a custom MCP connector is made of: the fields an app sends to create
// one. The rest of its manifest is fixed (CustomManifest).
type CustomConnector struct {
	ID          string
	Name        string
	Category    string
	Description string
	// Endpoint is the MCP server.
	Endpoint string
	Schemes  []string
	Scopes   []string
	Client   ClientPolicy
}

// CustomManifest is the manifest a custom connector is stored as, or why it cannot be: one MCP
// source at Endpoint, and of the client only its registration, auth_method and alg. Which
// schemes the deployment has and whether Endpoint is public are the caller's to check.
//
// SourceKindMCP is both the tool source and the endpoint role it runs against, as the built-in
// manifests write them (internal/connectors/providers/linear.yaml: sources: [{kind: mcp,
// endpoint: mcp}]). MCP is the only source so far (architecture doc, «Layers and interfaces»,
// the Source registry's first member).
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
	// internal/connectors/schemes/oauth2cc/scheme.go), and a custom connector sets only the MCP endpoint, so
	// such a definition could be stored and never connected.
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
	rebuilt, err := CustomManifest(CustomConnector{
		ID: id, Name: m.Name, Category: m.Category, Description: m.Description,
		Endpoint: m.Endpoints[SourceKindMCP], Schemes: m.Schemes, Scopes: m.Scopes.List, Client: m.Client,
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
			"a category, a description, an MCP endpoint, schemes, scopes and a client's registration, auth_method and alg, and nothing else"))
	}
	return rebuilt, nil
}
