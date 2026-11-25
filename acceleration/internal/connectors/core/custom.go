package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

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

// The limits on a custom connector, which the API's request schema documents
// (api.CustomConnectorRequest) and CustomManifest enforces, so a definition an import carries is
// held to them too. A length is in characters, as JSON Schema's maxLength counts them.
//
// The limits on name, category and description are the prototype's
// (CreateConnectorDefinitionRequest in api/openapi.yaml on codex/connector-support at
// cf62af0d). The id is its pattern without the hyphen, which Manifest.Validate refuses in an
// id: custom_ and up to 57 more characters, 64 in all. The prototype set no length on the
// endpoint; 2048 is a guess at a URL nobody needs to exceed, not a measured one. 100 scopes is
// well over the longest list shipped, Slack's 29 (providers/slack.yaml), and is not measured
// either. A scope is RFC 6749 section 3.3's scope-token, %x21 / %x23-5B / %x5D-7E, so it can
// hold neither a space nor a quote.
const (
	CustomIDPattern            = `^custom_[a-z][a-z0-9_]{0,56}$`
	CustomIDPatternDescription = "custom_ then a lowercase letter, then up to 56 lowercase letters, digits or underscores"
	CustomNameMaxLength        = 120
	CustomCategoryMaxLength    = 80
	CustomDescriptionMaxLength = 1000
	CustomEndpointMaxLength    = 2048
	CustomScopesMaxItems       = 100
	ScopeTokenPattern          = `^[!#-\[\]-~]+$`
	ScopeTokenDescription      = "an RFC 6749 scope token"
)

var (
	customID   = regexp.MustCompile(CustomIDPattern)
	scopeToken = regexp.MustCompile(ScopeTokenPattern)
)

// checkLimits refuses a custom connector past one of the limits above.
func (c CustomConnector) checkLimits() error {
	if !customID.MatchString(c.ID) {
		return stack.Wrap(fmt.Errorf("id %q is not %s", c.ID, CustomIDPatternDescription))
	}
	for _, limit := range []struct {
		field, value string
		max          int
	}{
		{"name", c.Name, CustomNameMaxLength},
		{"category", c.Category, CustomCategoryMaxLength},
		{"description", c.Description, CustomDescriptionMaxLength},
		{"endpoint", c.Endpoint, CustomEndpointMaxLength},
	} {
		if utf8.RuneCountInString(limit.value) > limit.max {
			return stack.Wrap(fmt.Errorf("%s is longer than %d characters", limit.field, limit.max))
		}
	}
	if len(c.Scopes) > CustomScopesMaxItems {
		return stack.Wrap(fmt.Errorf("scopes holds more than %d", CustomScopesMaxItems))
	}
	for i, scope := range c.Scopes {
		if !scopeToken.MatchString(scope) {
			return stack.Wrap(fmt.Errorf("scopes[%d] is not %s", i, ScopeTokenDescription))
		}
		if slices.Index(c.Scopes, scope) != i {
			return stack.Wrap(fmt.Errorf("scopes[%d] %q is listed twice", i, scope))
		}
	}
	return nil
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
	if err := c.checkLimits(); err != nil {
		return Manifest{}, err
	}
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
