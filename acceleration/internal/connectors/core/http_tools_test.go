package core

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

// HTTPSourceSuite runs the rules of an http source's tools (checkHTTPSource) through Validate.
type HTTPSourceSuite struct {
	suite.Suite
}

func TestHTTPSourceSuite(t *testing.T) {
	suite.Run(t, new(HTTPSourceSuite))
}

// forecast is a valid http tool, which each case changes in one place.
func forecast() HTTPTool {
	return HTTPTool{Name: "get_forecast", Description: "The forecast for a city.", Method: "GET", Path: "/forecast/{city}",
		Parameters: map[string]HTTPParameter{
			"city": {Type: "string", Description: "The city.", Required: true},
			"days": {Type: "integer"},
		}, ReadOnly: true}
}

// httpManifest is a valid manifest whose one source is an http one with tools.
func httpManifest(tools ...HTTPTool) Manifest {
	return Manifest{ID: "example", Revision: 1, Name: "Example", Schemes: []string{"bearer"},
		Endpoints: map[string]string{SourceKindHTTP: "https://api.example.com/v1"},
		Sources:   []SourceRule{{Kind: SourceKindHTTP, Endpoint: SourceKindHTTP, HTTPTools: tools}}}
}

// withTool is httpManifest with forecast changed by change.
func withTool(change func(tool *HTTPTool)) Manifest {
	tool := forecast()
	change(&tool)
	return httpManifest(tool)
}

// withSource is httpManifest with forecast and its source changed by change.
func withSource(change func(source *SourceRule)) Manifest {
	m := httpManifest(forecast())
	change(&m.Sources[0])
	return m
}

// tools are count copies of forecast, each under a name of its own.
func tools(count int) []HTTPTool {
	listed := make([]HTTPTool, count)
	for i := range listed {
		listed[i] = forecast()
		listed[i].Name = fmt.Sprintf("tool_%d", i)
	}
	return listed
}

// parameters are count string parameters, the first of them city.
func parameters(count int) map[string]HTTPParameter {
	listed := map[string]HTTPParameter{"city": {Type: "string", Required: true}}
	for i := 1; i < count; i++ {
		listed[fmt.Sprintf("p%d", i)] = HTTPParameter{Type: "string"}
	}
	return listed
}

func (s *HTTPSourceSuite) TestAValidHTTPSourceIsAccepted() {
	for name, m := range map[string]Manifest{
		"the tool as it is":                   httpManifest(forecast()),
		"no tool, as the stress-test ones":    httpManifest(),
		"HEAD read only":                      withTool(func(t *HTTPTool) { t.Method = "HEAD" }),
		"POST that is not read only":          withTool(func(t *HTTPTool) { t.Method, t.ReadOnly = "POST", false }),
		"PUT that is not read only":           withTool(func(t *HTTPTool) { t.Method, t.ReadOnly = "PUT", false }),
		"PATCH that is not read only":         withTool(func(t *HTTPTool) { t.Method, t.ReadOnly = "PATCH", false }),
		"DELETE that is not read only":        withTool(func(t *HTTPTool) { t.Method, t.ReadOnly = "DELETE", false }),
		"GET that is not read only":           withTool(func(t *HTTPTool) { t.ReadOnly = false }),
		"a number":                            withTool(func(t *HTTPTool) { t.Parameters["days"] = HTTPParameter{Type: "number"} }),
		"a boolean":                           withTool(func(t *HTTPTool) { t.Parameters["days"] = HTTPParameter{Type: "boolean"} }),
		"a name of 63 with capitals and -":    withTool(func(t *HTTPTool) { t.Name = "Get-" + strings.Repeat("x", 59) }),
		"a description of 1000 bytes":         withTool(func(t *HTTPTool) { t.Description = strings.Repeat("d", 1000) }),
		"no description":                      withTool(func(t *HTTPTool) { t.Description = "" }),
		"a path of 2048 bytes":                withTool(func(t *HTTPTool) { t.Path = "/{city}/" + strings.Repeat("p", 2040) }),
		"the root path":                       withTool(func(t *HTTPTool) { t.Path, t.Parameters = "/", nil }),
		"a {name} inside a segment":           withTool(func(t *HTTPTool) { t.Path = "/files/{city}.json" }),
		"an escaped and a reserved character": withTool(func(t *HTTPTool) { t.Path = "/a%20b/c:d;e=f@g!$&'()*+,/{city}" }),
		"128 tools":                           httpManifest(tools(128)...),
		"64 parameters":                       withTool(func(t *HTTPTool) { t.Parameters = parameters(64) }),
		"a parameter of 64 with . _ and -":    withTool(func(t *HTTPTool) { t.Parameters["a.b_c-"+strings.Repeat("d", 58)] = HTTPParameter{Type: "string"} }),
		"a parameter merely holding method":   withTool(func(t *HTTPTool) { t.Parameters["http_method_name"] = HTTPParameter{Type: "string"} }),
		"a header that is no override":        withSource(func(r *SourceRule) { r.Headers = map[string]string{"X-Api-Version": "2"} }),
		"outbound true":                       withSource(func(r *SourceRule) { r.Outbound = new(true) }),
		"an mcp source with an override header": {ID: "example", Revision: 1, Name: "Example", Schemes: []string{"bearer"},
			Endpoints: map[string]string{SourceKindMCP: "https://api.example.com/mcp"},
			Sources:   []SourceRule{{Kind: SourceKindMCP, Endpoint: SourceKindMCP, Headers: map[string]string{"X-HTTP-Method-Override": "POST"}}}},
	} {
		s.NoError(m.Validate(), name)
	}
}

func (s *HTTPSourceSuite) TestAnHTTPSourceBreakingARuleIsRefused() {
	noTool := func(change func(r *SourceRule)) Manifest { m := httpManifest(); change(&m.Sources[0]); return m }
	for name, test := range map[string]struct {
		manifest Manifest
		refusal  string
	}{
		"a lowercase method":   {withTool(func(t *HTTPTool) { t.Method = "get" }), `http_tools[0].method: "get" is not one of`},
		"OPTIONS":              {withTool(func(t *HTTPTool) { t.Method, t.ReadOnly = "OPTIONS", false }), `http_tools[0].method: "OPTIONS" is not one of`},
		"TRACE":                {withTool(func(t *HTTPTool) { t.Method, t.ReadOnly = "TRACE", false }), `http_tools[0].method: "TRACE" is not one of`},
		"no method":            {withTool(func(t *HTTPTool) { t.Method, t.ReadOnly = "", false }), `http_tools[0].method: "" is not one of`},
		"POST read only":       {withTool(func(t *HTTPTool) { t.Method = "POST" }), `http_tools[0].read_only: a read-only tool must be GET or HEAD, and "get_forecast" is POST`},
		"PUT read only":        {withTool(func(t *HTTPTool) { t.Method = "PUT" }), `is PUT`},
		"PATCH read only":      {withTool(func(t *HTTPTool) { t.Method = "PATCH" }), `is PATCH`},
		"DELETE read only":     {withTool(func(t *HTTPTool) { t.Method = "DELETE" }), `is DELETE`},
		"no path":              {withTool(func(t *HTTPTool) { t.Path = "" }), `http_tools[0].path: "" does not start with /`},
		"a relative path":      {withTool(func(t *HTTPTool) { t.Path = "forecast/{city}" }), `does not start with /`},
		"a query":              {withTool(func(t *HTTPTool) { t.Path = "/forecast/{city}?units=metric" }), `holds a space, a ? or a #`},
		"a fragment":           {withTool(func(t *HTTPTool) { t.Path = "/forecast/{city}#today" }), `holds a space, a ? or a #`},
		"a space":              {withTool(func(t *HTTPTool) { t.Path = "/fore cast/{city}" }), `holds a space, a ? or a #`},
		"a line break":         {withTool(func(t *HTTPTool) { t.Path = "/forecast/{city}\r\nX-HTTP-Method-Override: POST" }), `holds a space, a ? or a #`},
		"a non-ASCII byte":     {withTool(func(t *HTTPTool) { t.Path = "/prévision/{city}" }), `holds a space, a ? or a #`},
		"a leading //":         {withTool(func(t *HTTPTool) { t.Path = "//other.example/{city}" }), `starts with // or holds a \`},
		"a backslash":          {withTool(func(t *HTTPTool) { t.Path = `/forecast\{city}` }), `starts with // or holds a \`},
		"a .. segment":         {withTool(func(t *HTTPTool) { t.Path = "/forecast/../admin/{city}" }), `has a . or .. segment`},
		"a . segment":          {withTool(func(t *HTTPTool) { t.Path = "/./{city}" }), `has a . or .. segment`},
		"a trailing ..":        {withTool(func(t *HTTPTool) { t.Path = "/{city}/.." }), `has a . or .. segment`},
		"an unclosed brace":    {withTool(func(t *HTTPTool) { t.Path = "/forecast/{city" }), `has a { or } that opens or closes no {name}`},
		"a stray close brace":  {withTool(func(t *HTTPTool) { t.Path = "/forecast}/{city}" }), `has a { or } that opens or closes no {name}`},
		"a nested brace":       {withTool(func(t *HTTPTool) { t.Path = "/{{city}}" }), `has a { or } that opens or closes no {name}`},
		"an empty {}":          {withTool(func(t *HTTPTool) { t.Path = "/{city}/{}" }), `{} is not a parameter of the tool`},
		"a path over 2048":     {withTool(func(t *HTTPTool) { t.Path = "/{city}/" + strings.Repeat("p", 2041) }), `http_tools[0].path: is over 2048 bytes`},
		"an undeclared {name}": {withTool(func(t *HTTPTool) { t.Path = "/forecast/{town}" }), `http_tools[0].path: {town} is not a parameter of the tool`},
		"an optional {name}":   {withTool(func(t *HTTPTool) { t.Path = "/forecast/{city}/{days}" }), `http_tools[0].path: {days} is a parameter the tool does not require`},
		"an array":             {withTool(func(t *HTTPTool) { t.Parameters["days"] = HTTPParameter{Type: "array"} }), `parameters.days.type: "array" is not one of`},
		"an object":            {withTool(func(t *HTTPTool) { t.Parameters["days"] = HTTPParameter{Type: "object"} }), `parameters.days.type: "object" is not one of`},
		"null":                 {withTool(func(t *HTTPTool) { t.Parameters["days"] = HTTPParameter{Type: "null"} }), `parameters.days.type: "null" is not one of`},
		"no type":              {withTool(func(t *HTTPTool) { t.Parameters["days"] = HTTPParameter{} }), `parameters.days.type: "" is not one of`},
		"a capitalized type":   {withTool(func(t *HTTPTool) { t.Parameters["days"] = HTTPParameter{Type: "String"} }), `parameters.days.type: "String" is not one of`},
		"_method":              {withTool(func(t *HTTPTool) { t.Parameters["_method"] = HTTPParameter{Type: "string"} }), `parameters._method: names a method override`},
		"_METHOD":              {withTool(func(t *HTTPTool) { t.Parameters["_METHOD"] = HTTPParameter{Type: "string"} }), `parameters._METHOD: names a method override`},
		"X-HTTP-Method-Override": {withTool(func(t *HTTPTool) { t.Parameters["X-HTTP-Method-Override"] = HTTPParameter{Type: "string"} }),
			`parameters.X-HTTP-Method-Override: names a method override`},
		"x-http-method":     {withTool(func(t *HTTPTool) { t.Parameters["x-http-method"] = HTTPParameter{Type: "string"} }), `parameters.x-http-method: names a method override`},
		"X-Method-Override": {withTool(func(t *HTTPTool) { t.Parameters["X-Method-Override"] = HTTPParameter{Type: "string"} }), `parameters.X-Method-Override: names a method override`},
		"$httpMethod":       {withTool(func(t *HTTPTool) { t.Parameters["$httpMethod"] = HTTPParameter{Type: "string"} }), `parameters.$httpMethod: "$httpMethod" is not 1 to 64`},
		"a tilde":           {withTool(func(t *HTTPTool) { t.Parameters["a~b"] = HTTPParameter{Type: "string"} }), `parameters.a~b: "a~b" is not 1 to 64 letters, digits, ., _ or -`},
		"a bracket":         {withTool(func(t *HTTPTool) { t.Parameters["page[size]"] = HTTPParameter{Type: "string"} }), `"page[size]" is not 1 to 64`},
		"a brace":           {withTool(func(t *HTTPTool) { t.Parameters["a}b"] = HTTPParameter{Type: "string"} }), `"a}b" is not 1 to 64`},
		"an empty name":     {withTool(func(t *HTTPTool) { t.Parameters[""] = HTTPParameter{Type: "string"} }), `"" is not 1 to 64`},
		"a name of 65":      {withTool(func(t *HTTPTool) { t.Parameters[strings.Repeat("p", 65)] = HTTPParameter{Type: "string"} }), `is not 1 to 64`},
		"65 parameters":     {withTool(func(t *HTTPTool) { t.Parameters = parameters(65) }), `http_tools[0].parameters: has 65 parameters, over 64`},
		"an override header": {withSource(func(r *SourceRule) { r.Headers = map[string]string{"x-HTTP-method-OVERRIDE": "POST"} }),
			`sources[0].headers.x-HTTP-method-OVERRIDE: names a method override`},
		"read_only_tools":   {withSource(func(r *SourceRule) { r.ReadOnlyTools = []string{"get_forecast"} }), `sources[0].read_only_tools: is not set on an http source`},
		"needs_scopes":      {withSource(func(r *SourceRule) { r.Tools = []ToolRule{{Name: "get_forecast", NeedsScopes: []string{"read"}}} }), `sources[0].tools: is not set on an http source`},
		"outbound false":    {withSource(func(r *SourceRule) { r.Outbound = new(false) }), `sources[0].outbound: is not false on an http source`},
		"no tool name":      {withTool(func(t *HTTPTool) { t.Name = "" }), `http_tools[0].name: "" is not 1 to 63 letters, digits, _ or -`},
		"a dot":             {withTool(func(t *HTTPTool) { t.Name = "forecast.get" }), `"forecast.get" is not 1 to 63`},
		"a space in a name": {withTool(func(t *HTTPTool) { t.Name = "get forecast" }), `"get forecast" is not 1 to 63`},
		"a name of 64":      {withTool(func(t *HTTPTool) { t.Name = strings.Repeat("x", 64) }), `is not 1 to 63`},
		"a name twice":      {httpManifest(forecast(), forecast()), `http_tools[1].name: "get_forecast" is named twice`},
		"a description of 1001 bytes in 501 characters": {withTool(func(t *HTTPTool) { t.Description = strings.Repeat("é", 500) + "d" }), `http_tools[0].description: is over 1000 bytes`},
		"outbound false on a source with no tool":       {noTool(func(r *SourceRule) { r.Outbound = new(false) }), `sources[0].outbound: is not false on an http source`},
		"needs_scopes on a source with no tool":         {noTool(func(r *SourceRule) { r.Tools = []ToolRule{{Name: "x", NeedsScopes: []string{"read"}}} }), `sources[0].tools: is not set on an http source`},
		"read_only_tools on a source with no tool":      {noTool(func(r *SourceRule) { r.ReadOnlyTools = []string{"x"} }), `sources[0].read_only_tools: is not set on an http source`},
		"an override header on a source with no tool":   {noTool(func(r *SourceRule) { r.Headers = map[string]string{"X-HTTP-Method": "PUT"} }), `names a method override`},
		"a description over 1000":                       {withTool(func(t *HTTPTool) { t.Description = strings.Repeat("d", 1001) }), `http_tools[0].description: is over 1000 bytes`},
		"129 tools":                                     {httpManifest(tools(129)...), `sources[0].http_tools: has 129 tools, over 128`},
		"http_tools on an mcp source": {Manifest{ID: "example", Revision: 1, Name: "Example", Schemes: []string{"bearer"},
			Endpoints: map[string]string{SourceKindMCP: "https://api.example.com/mcp"},
			Sources:   []SourceRule{{Kind: SourceKindMCP, Endpoint: SourceKindMCP, HTTPTools: []HTTPTool{forecast()}}}},
			`sources[0].http_tools: is set only on a source of kind http`},
		"http_tools on another kind": {withSource(func(r *SourceRule) { r.Kind = "rest" }), `sources[0].http_tools: is set only on a source of kind http`},
	} {
		s.ErrorContains(test.manifest.Validate(), test.refusal, name)
	}
}

// Every rule is told apart by the field it names, so a manifest breaking several hears of each.
func (s *HTTPSourceSuite) TestEveryToolIsChecked() {
	second := forecast()
	second.Name, second.Method = "create_forecast", "POST"

	err := httpManifest(forecast(), second).Validate()

	s.ErrorContains(err, `http_tools[1].read_only: a read-only tool must be GET or HEAD, and "create_forecast" is POST`)
	s.NotContains(err.Error(), "http_tools[0]")
}

func (s *HTTPSourceSuite) TestAnHTTPSourceParsesFromYAML() {
	parsed, err := ParseManifest(minimal("endpoints:\n  http: https://api.example.com/v1\nsources:\n  - kind: http\n    endpoint: http\n" +
		"    http_tools:\n      - name: get_forecast\n        method: GET\n        path: /forecast/{city}\n        read_only: true\n" +
		"        parameters:\n          city: {type: string, required: true}\n"))
	s.Require().NoError(err)

	s.Equal([]HTTPTool{{Name: "get_forecast", Method: "GET", Path: "/forecast/{city}", ReadOnly: true,
		Parameters: map[string]HTTPParameter{"city": {Type: "string", Required: true}}}}, HTTPSource(parsed.Sources).HTTPTools)
}

// A read_only http tool is one its source does not send out, as a name in read_only_tools is
// for an mcp source; any other tool of it may.
func (s *HTTPSourceSuite) TestAReadOnlyHTTPToolSendsNothingOut() {
	writes := forecast()
	writes.Name, writes.Method, writes.ReadOnly = "create_forecast", "POST", false
	m := httpManifest(forecast(), writes)
	source := HTTPSource(m.Sources)

	s.False(source.SendsOutTool("get_forecast"))
	s.True(source.SendsOutTool("create_forecast"))
	s.True(source.SendsOutTool("unlisted"))
	s.False(m.SendsOutTool("get_forecast"))
	s.True(m.SendsOutTool("create_forecast"))
}

// read_only is read from the connector's latest revision, so a revision changing only it reads
// as the one before; a changed method or path is what the connection sends, and reads otherwise.
func (s *HTTPSourceSuite) TestARevisionChangingOnlyReadOnlyReadsAsTheOneBefore() {
	pinned := httpManifest(forecast())

	s.True(pinned.ReadsAsConnection(withTool(func(t *HTTPTool) { t.ReadOnly = false })))
	s.False(pinned.ReadsAsConnection(withTool(func(t *HTTPTool) { t.Method = "HEAD" })))
	s.False(pinned.ReadsAsConnection(withTool(func(t *HTTPTool) { t.Path = "/forecasts/{city}" })))
	s.True(HTTPSource(pinned.Sources).HTTPTools[0].ReadOnly, "the comparison leaves the manifest as it was")
}
