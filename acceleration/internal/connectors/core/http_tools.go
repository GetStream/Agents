package core

import (
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
)

// SourceKindHTTP is the sources[].kind of the http source: the tools are the ones the manifest
// lists (SourceRule.HTTPTools), each one HTTP request to the source's endpoint.
const SourceKindHTTP = "http"

// HTTPTool is one tool of an http source: one request, whose method and path the manifest
// fixes and whose parameters are the values a model gives.
type HTTPTool struct {
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
	Method      string `yaml:"method" json:"method"`
	// Path is under the source's endpoint and starts with /. Each {name} in it is the value of
	// the parameter of that name.
	Path       string                   `yaml:"path" json:"path"`
	Parameters map[string]HTTPParameter `yaml:"parameters,omitempty" json:"parameters,omitempty"`
	// ReadOnly says the tool only reads, so it sends no data out of the conversation
	// (SourceRule.SendsOutTool), as a name in an mcp source's read_only_tools does. Only a GET
	// or HEAD tool may say it.
	ReadOnly bool `yaml:"read_only,omitempty" json:"read_only,omitempty"`
}

// HTTPParameter is one value a model gives an http tool.
type HTTPParameter struct {
	// Type is a JSON Schema scalar type: string, number, integer or boolean.
	Type        string `yaml:"type" json:"type"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
	// Required parameters must be given. A parameter in the path is always required.
	Required bool `yaml:"required,omitempty" json:"required,omitempty"`
}

// HTTPSource is the first http source of sources, or nil, as MCPSource is for mcp.
func HTTPSource(sources []SourceRule) *SourceRule {
	index := slices.IndexFunc(sources, func(rule SourceRule) bool { return rule.Kind == SourceKindHTTP })
	if index < 0 {
		return nil
	}
	return &sources[index]
}

// The caps of an http source. Each is a choice, with the limit it is taken from:
const (
	// maxHTTPTools is the most tools a binding grants and a session offers from one binding
	// (maxItems on AgentConnectorBinding.tools, internal/api/configs.go), which is the most
	// OpenAI documents taking in one request.
	maxHTTPTools = 128
	// maxHTTPToolDescription is the cap a custom connector's own description has
	// (CustomConnectorRequest.Description, internal/api/connectors.go).
	maxHTTPToolDescription = 1000
	// maxHTTPPath is the cap a custom connector's MCP endpoint URL has
	// (CustomConnectorRequest.Endpoint, internal/api/connectors.go).
	maxHTTPPath = 2048
	// maxHTTPParameters is the cap an agent config has on its connector bindings and its
	// visible_tools (internal/api/configs.go).
	maxHTTPParameters = 64
)

var (
	// httpMethods are the methods an http tool may send: the five a connection's direct call
	// forwards (proxyMethods, internal/api/connection_proxy.go) and HEAD.
	httpMethods = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}
	// httpReadMethods are the methods of httpMethods that RFC 9110 section 9.2.1 calls safe,
	// the only ones a read_only tool may send.
	httpReadMethods = []string{http.MethodGet, http.MethodHead}
	// httpParameterTypes are JSON Schema's scalar types (JSON Schema Validation 2020-12
	// section 6.1.1), which a value in a path, a query or a JSON body can be.
	httpParameterTypes = []string{"string", "number", "integer", "boolean"}
	// httpMethodOverrides are names, lowercase, in which a client names the method a host should
	// run in place of the request's: the three headers the sandbox's egress proxy refuses
	// (methodOverrides, internal/sandbox/egress/proxy.go; Google runs a GET with
	// X-HTTP-Method-Override: POST as the POST, AI-2120) and the _method field Rails and Laravel
	// read from a form. A parameter or header named so could make a read_only GET write.
	httpMethodOverrides = []string{"x-http-method-override", "x-http-method", "x-method-override", "_method"}
	// httpToolName is a name of at most 63 characters: the model is offered it as
	// <alias>__<name> (internal/session), an alias is at most 63 characters
	// (AgentConnectorBinding.name, internal/api/configs.go), and 63+2+63 is the 128 Anthropic
	// caps a tool name at (^[a-zA-Z0-9_-]{1,128}$,
	// https://platform.claude.com/docs/en/agents-and-tools/tool-use/define-tools). OpenAI caps
	// a function name at 64 (https://developers.openai.com/api/reference/resources/chat,
	// tools[].function.name), so a longest alias and name do not fit it: the same holds for an
	// MCP tool today.
	httpToolName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,63}$`)
	// httpPath is visible ASCII (RFC 9110 section 5.5's vchar), so a path holds no space or
	// line break, and no ? or #, so it has no query or fragment of its own.
	httpPath = regexp.MustCompile(`^/[!"$->@-~]*$`)
	// httpParameterName is what Anthropic takes as a key of a tool's input_schema properties,
	// 1 to 64 letters, digits, _, . or -: its 400 answers a key outside it with «Property keys
	// should match pattern '^[a-zA-Z0-9_.-]{1,64}$'» (quoted in anthropics/claude-code issues
	// 24272, 34771 and 92648; not probed live, AI-3674: the keys in .env were refused). Each of
	// these characters is RFC 3986 unreserved, so a name also goes into a query as it is.
	httpParameterName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
)

// checkHTTPSource refuses an http source whose tools a session could not offer or that could
// write while saying they read. field names the source.
func checkHTTPSource(fail func(field, format string, args ...any), field string, source SourceRule) {
	if len(source.ReadOnlyTools) > 0 {
		fail(field+".read_only_tools", "is not set on an http source: each of its tools says read_only")
	}
	if len(source.Tools) > 0 {
		fail(field+".tools", "is not set on an http source: it keeps no needs_scopes, and each of its http_tools says what it needs")
	}
	if !source.SendsOut() {
		fail(field+".outbound", "is not false on an http source: each of its tools says read_only")
	}
	for _, name := range slices.Sorted(maps.Keys(source.Headers)) {
		if slices.Contains(httpMethodOverrides, strings.ToLower(name)) {
			fail(field+".headers."+name, "names a method override")
		}
	}
	if len(source.HTTPTools) > maxHTTPTools {
		fail(field+".http_tools", "has %d tools, over %d", len(source.HTTPTools), maxHTTPTools)
	}
	named := map[string]bool{}
	for i, tool := range source.HTTPTools {
		field := fmt.Sprintf("%s.http_tools[%d]", field, i)
		if !httpToolName.MatchString(tool.Name) {
			fail(field+".name", "%q is not 1 to 63 letters, digits, _ or -", tool.Name)
		}
		if named[tool.Name] {
			fail(field+".name", "%q is named twice", tool.Name)
		}
		named[tool.Name] = true
		if len(tool.Description) > maxHTTPToolDescription {
			fail(field+".description", "is over %d bytes", maxHTTPToolDescription)
		}
		if !slices.Contains(httpMethods, tool.Method) {
			fail(field+".method", "%q is not one of %v", tool.Method, httpMethods)
		}
		if tool.ReadOnly && !slices.Contains(httpReadMethods, tool.Method) {
			fail(field+".read_only", "a read-only tool must be GET or HEAD, and %q is %s", tool.Name, tool.Method)
		}
		checkHTTPPath(fail, field, tool)
		if len(tool.Parameters) > maxHTTPParameters {
			fail(field+".parameters", "has %d parameters, over %d", len(tool.Parameters), maxHTTPParameters)
		}
		for _, name := range slices.Sorted(maps.Keys(tool.Parameters)) {
			field := field + ".parameters." + name
			switch {
			case !httpParameterName.MatchString(name):
				fail(field, "%q is not 1 to 64 letters, digits, ., _ or -", name)
			case slices.Contains(httpMethodOverrides, strings.ToLower(name)):
				fail(field, "names a method override")
			}
			if !slices.Contains(httpParameterTypes, tool.Parameters[name].Type) {
				fail(field+".type", "%q is not one of %v", tool.Parameters[name].Type, httpParameterTypes)
			}
		}
	}
}

// checkHTTPPath refuses a tool's path that has a query, a fragment, a leading // or a \, a dot segment, a stray brace
// or a {name} that is not a required parameter of the tool.
func checkHTTPPath(fail func(field, format string, args ...any), field string, tool HTTPTool) {
	field += ".path"
	switch {
	case len(tool.Path) > maxHTTPPath:
		fail(field, "is over %d bytes", maxHTTPPath)
	case !httpPath.MatchString(tool.Path):
		fail(field, "%q does not start with / or holds a space, a ? or a #", tool.Path)
	case strings.HasPrefix(tool.Path, "//") || strings.Contains(tool.Path, `\`):
		fail(field, "%q starts with // or holds a \\, which a URL join can read as another host or path", tool.Path)
	case slices.ContainsFunc(strings.Split(tool.Path, "/"), isDotSegment):
		fail(field, "%q has a . or .. segment", tool.Path)
	case strings.ContainsAny(placeholder.ReplaceAllString(tool.Path, ""), "{}"):
		fail(field, "%q has a { or } that opens or closes no {name}", tool.Path)
	}
	for _, match := range placeholder.FindAllStringSubmatch(tool.Path, -1) {
		parameter, declared := tool.Parameters[match[1]]
		switch {
		case !declared:
			fail(field, "{%s} is not a parameter of the tool", match[1])
		case !parameter.Required:
			fail(field, "{%s} is a parameter the tool does not require", match[1])
		}
	}
}
