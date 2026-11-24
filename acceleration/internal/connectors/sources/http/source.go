// Package http is the core.ToolSource for a connector whose manifest lists its own tools, each
// one HTTP request to the connector's API (core.SourceKindHTTP). It builds each tool's input
// schema from the parameters the manifest declares, and sends a granted tool's request through
// the connection's own client (core.ResolvedBinding.HTTP), which applies its credential and the
// egress policy.
package http

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/sources/mcp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Kind is the name a manifest's sources[].kind gives this source, and its key in
// core.Registry.ToolSources.
const Kind = core.SourceKindHTTP

// Separator joins a binding's alias and a tool's name into the name the model is offered: the
// mcp source's, at which the session splits every connector tool's name back
// (internal/session/connector_tools.go).
const Separator = mcp.Separator

// defaultCallTimeout is the least timeout the client of an opened Toolset has, for the same
// reason as the mcp source's (mcp.defaultCallTimeout): longer than the longest call a binding may
// ask for (30 s, api.AgentConnectorBinding.TimeoutMs), so the caller's deadline ends a call
// first, and the client the router builds for a connection cuts a request at 10 s
// (connectorHTTPTimeout, cmd/router/main.go). The 5 s above 30 s is a margin, a choice:
// unverified.
const defaultCallTimeout = 35 * time.Second

// schemaLocation is the URL each tool's input schema is compiled under. .invalid is reserved and
// never resolves (RFC 6761 section 6.4); a schema built from scalar parameters refers to nothing.
const schemaLocation = "https://vision-agents.invalid/http-tool-schema.json"

// Source is the http core.ToolSource. It holds no state, so one serves every connection.
type Source struct {
	callTimeout time.Duration
}

var _ core.ToolSource = (*Source)(nil)

// New is the http source.
func New() *Source {
	return &Source{callTimeout: defaultCallTimeout}
}

// Kind is Kind.
func (*Source) Kind() string {
	return Kind
}

// Discover is every tool the manifest's http source lists, each with its input schema and its
// digest. It sends nothing: the manifest is the list.
func (*Source) Discover(_ context.Context, b core.ResolvedBinding) ([]core.ToolSpec, error) {
	rule, _, err := httpSource(b)
	if err != nil {
		return nil, err
	}
	specs := make([]core.ToolSpec, 0, len(rule.HTTPTools))
	for _, tool := range rule.HTTPTools {
		digest, err := ToolSchemaDigest(tool)
		if err != nil {
			return nil, err
		}
		specs = append(specs, core.ToolSpec{Name: tool.Name, Description: tool.Description,
			InputSchema: inputSchema(tool), SchemaDigest: digest})
	}
	return specs, nil
}

// Open keeps the granted tools whose digest still matches the grant, each offered to the model
// as the binding's alias, Separator and its name. A tool not granted, or whose definition
// changed since it was granted, is not offered and never called.
//
// The alias must not hold Separator, so the name a model calls splits back into one alias and
// one tool: two bindings cannot offer the same name.
func (s *Source) Open(_ context.Context, b core.ResolvedBinding, grants []core.ToolGrant) (core.Toolset, error) {
	alias := b.Binding.Name
	if alias == "" || strings.Contains(alias, Separator) {
		return nil, stack.Wrap(fmt.Errorf("http: binding name %q must be set and must not hold %q", alias, Separator))
	}
	if b.HTTP == nil {
		return nil, stack.Wrap(errors.New("http: the binding has no client: build it with core.Transports"))
	}
	rule, endpoint, err := httpSource(b)
	if err != nil {
		return nil, err
	}
	granted := make(map[string]string, len(grants))
	for _, grant := range grants {
		granted[grant.Name] = grant.SchemaDigest
	}
	set := &Toolset{client: s.callClient(b.HTTP), endpoint: endpoint, headers: rule.Headers,
		timeout: b.Binding.Timeout, targets: map[string]target{}}
	for _, tool := range rule.HTTPTools {
		// A tool not granted pins no digest, which no digest matches.
		want := granted[tool.Name]
		digest, err := ToolSchemaDigest(tool)
		if err != nil || digest != want {
			continue
		}
		schema := inputSchema(tool)
		validator, err := compileSchema(schema)
		if err != nil {
			continue
		}
		name := alias + Separator + tool.Name
		set.targets[name] = target{tool: tool, validator: validator}
		set.tools = append(set.tools, offered(name, tool.Description, schema))
	}
	return set, nil
}

// ToolSchemaDigest is the SHA-256, in hex, of the JSON of a tool's name, description and input
// schema, which the model is shown, and of its method, path and read_only, which decide what a
// call does. A later revision that turns a POST into a read-only GET has another digest, so a
// grant pinned to one never opens the other (AI-3674): a connection pinned to the POST's
// revision is never offered the tool under a grant made for the GET, nor the reverse.
func ToolSchemaDigest(tool core.HTTPTool) (string, error) {
	data, err := json.Marshal(struct {
		Name        string         `json:"name"`
		Description string         `json:"description"`
		InputSchema map[string]any `json:"input_schema"`
		Method      string         `json:"method"`
		Path        string         `json:"path"`
		ReadOnly    bool           `json:"read_only"`
	}{Name: tool.Name, Description: tool.Description, InputSchema: inputSchema(tool),
		Method: tool.Method, Path: tool.Path, ReadOnly: tool.ReadOnly})
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("http: tool %q schema: %w", tool.Name, err))
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

// callClient is the connection's client for an opened Toolset: the same client, with a timeout
// no shorter than callTimeout, so it never cuts a call before the caller's deadline.
func (s *Source) callClient(client *http.Client) *http.Client {
	copied := *client
	if copied.Timeout < s.callTimeout {
		copied.Timeout = s.callTimeout
	}
	return &copied
}

// httpSource is the manifest's http source and the URL of its endpoint.
func httpSource(b core.ResolvedBinding) (*core.SourceRule, string, error) {
	rule := core.HTTPSource(b.Manifest.Sources)
	if rule == nil {
		return nil, "", stack.Wrap(fmt.Errorf("http: connector %q has no http source", b.Manifest.ConnectorID))
	}
	endpoint := b.Manifest.Endpoints[rule.Endpoint]
	if endpoint == "" {
		return nil, "", stack.Wrap(fmt.Errorf("http: connector %q has no %s endpoint resolved", b.Manifest.ConnectorID, rule.Endpoint))
	}
	return rule, endpoint, nil
}

// inputSchema is the JSON Schema of a tool's arguments: an object with one property per
// parameter, of its type and with its description, and the required ones listed in name order.
// It is built of JSON's own shapes (map[string]any, []any), as jsonschema compiles it.
func inputSchema(tool core.HTTPTool) map[string]any {
	properties := make(map[string]any, len(tool.Parameters))
	var required []any
	for _, name := range slices.Sorted(maps.Keys(tool.Parameters)) {
		parameter := tool.Parameters[name]
		property := map[string]any{"type": parameter.Type}
		if parameter.Description != "" {
			property["description"] = parameter.Description
		}
		properties[name] = property
		if parameter.Required {
			required = append(required, name)
		}
	}
	schema := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

// compileSchema compiles a tool's input schema as draft 2020-12, the draft the mcp source
// compiles under (mcp.compileSchema), set here rather than left to jsonschema/v6's newest.
func compileSchema(schema map[string]any) (*jsonschema.Schema, error) {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	if err := compiler.AddResource(schemaLocation, schema); err != nil {
		return nil, err
	}
	return compiler.Compile(schemaLocation)
}
