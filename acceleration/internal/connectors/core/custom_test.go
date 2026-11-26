package core

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

// CustomSuite runs CustomManifest and AsCustom, the two ways a custom connector's manifest is made.
type CustomSuite struct {
	suite.Suite
}

func TestCustomSuite(t *testing.T) {
	suite.Run(t, new(CustomSuite))
}

// mcpCustom is a custom MCP connector with every field a custom one may set, the client's env
// among what it may not.
func mcpCustom() CustomConnector {
	return CustomConnector{ID: "custom_acme", Name: " Acme ", Category: " crm ", Description: " Acme CRM ",
		Endpoint: "https://mcp.acme.example/mcp", Schemes: []string{"oauth2_code", "bearer"}, Scopes: []string{"read", "write"},
		Client: ClientPolicy{Registration: []ClientRegistrationMethod{ClientCustomer}, AuthMethod: AuthClientSecretPost, Env: "IGNORED"}}
}

// httpCustom is a custom http connector with a tool that reads and one that writes.
func httpCustom() CustomConnector {
	writes := forecast()
	writes.Name, writes.Method, writes.Path, writes.ReadOnly = "create_alert", "POST", "/alerts", false
	writes.Parameters = map[string]HTTPParameter{"city": {Type: "string", Required: true}}
	return CustomConnector{ID: "custom_weather", Name: "Weather", BaseURL: "https://api.weather.example/v1",
		Tools: []HTTPTool{forecast(), writes}, Schemes: []string{"api_key"}}
}

func (s *CustomSuite) json(m Manifest) string {
	raw, err := json.Marshal(m)
	s.Require().NoError(err)
	return string(raw)
}

// The JSON an MCP connector is stored as, and what AsCustom gives back for it, are what the base
// (8af123d92) gave: each expected string below is the output of a probe run there.
func (s *CustomSuite) TestAnMCPConnectorIsStoredAsBeforeHTTPConnectors() {
	for name, test := range map[string]struct {
		custom CustomConnector
		stored string
	}{
		"every field": {mcpCustom(), `{"id":"custom_acme","revision":1,"name":"Acme","category":"crm","description":"Acme CRM",` +
			`"endpoints":{"mcp":"https://mcp.acme.example/mcp"},"schemes":["oauth2_code","bearer"],` +
			`"client":{"registration":["customer"],"auth_method":"client_secret_post"},"scopes":{"list":["read","write"]},` +
			`"sources":[{"kind":"mcp","endpoint":"mcp"}]}`},
		"a bearer server": {CustomConnector{ID: "custom_acme", Name: "Acme", Endpoint: "https://mcp.acme.example/mcp", Schemes: []string{"bearer"}},
			`{"id":"custom_acme","revision":1,"name":"Acme","endpoints":{"mcp":"https://mcp.acme.example/mcp"},"schemes":["bearer"],` +
				`"sources":[{"kind":"mcp","endpoint":"mcp"}]}`},
	} {
		m, err := CustomManifest(test.custom)
		s.Require().NoError(err, name)
		s.Equal(test.stored, s.json(m), name)

		m.Revision = 3
		back, err := m.AsCustom("custom_acme")
		s.Require().NoError(err, name)
		s.Equal(m, back, name)
	}
}

// The refusals of an MCP connector are the base's (8af123d92), each from the same probe.
func (s *CustomSuite) TestAnMCPConnectorIsRefusedAsBeforeHTTPConnectors() {
	for name, test := range map[string]struct {
		change  func(c *CustomConnector)
		refusal string
	}{
		"client credentials": {func(c *CustomConnector) { c.Schemes = []string{"oauth2_client_credentials"} },
			"oauth2_client_credentials is not available for a custom connector: it needs a token endpoint a custom connector cannot set"},
		"an operator client": {func(c *CustomConnector) { c.Client.Registration = []ClientRegistrationMethod{ClientOperator} },
			"client.registration cannot be operator for a custom connector: this deployment has no client registered with it"},
		"oauth2_code without a registration": {func(c *CustomConnector) { c.Client = ClientPolicy{} },
			"client.registration is required with oauth2_code: name how the OAuth client is registered (customer, cimd or dcr)"},
		"a plain http server": {func(c *CustomConnector) { c.Endpoint = "http://mcp.acme.example/mcp" },
			`manifest "custom_acme": endpoints.mcp: "http://mcp.acme.example/mcp" does not start with https:// or a placeholder`},
		"no endpoint": {func(c *CustomConnector) { c.Endpoint = "" },
			`manifest "custom_acme": endpoints.mcp: "" does not start with https:// or a placeholder`},
	} {
		custom := mcpCustom()
		test.change(&custom)
		_, err := CustomManifest(custom)
		s.EqualError(err, test.refusal, name)
	}
}

func (s *CustomSuite) TestAnHTTPConnectorIsStoredAsOneHTTPSourceAtItsBaseURL() {
	m, err := CustomManifest(httpCustom())

	s.Require().NoError(err)
	s.JSONEq(`{"id":"custom_weather","revision":1,"name":"Weather","endpoints":{"http":"https://api.weather.example/v1"},
		"schemes":["api_key"],"sources":[{"kind":"http","endpoint":"http","http_tools":[
		  {"name":"get_forecast","description":"The forecast for a city.","method":"GET","path":"/forecast/{city}","read_only":true,
		   "parameters":{"city":{"type":"string","description":"The city.","required":true},"days":{"type":"integer"}}},
		  {"name":"create_alert","description":"The forecast for a city.","method":"POST","path":"/alerts",
		   "parameters":{"city":{"type":"string","required":true}}}]}]}`, s.json(m))
	s.False(m.SendsOutTool("get_forecast"))
	s.True(m.SendsOutTool("create_alert"))
}

func (s *CustomSuite) TestAnHTTPConnectorReadsBackAsItself() {
	m, err := CustomManifest(httpCustom())
	s.Require().NoError(err)
	m.Revision = 2

	back, err := m.AsCustom("custom_weather")

	s.Require().NoError(err)
	s.Equal(m, back)
}

// An http connector takes the schemes an MCP one does: oauth2_client_credentials still needs a
// token endpoint it cannot set, and an operator client is still the deployment's own.
func (s *CustomSuite) TestAnHTTPConnectorTakesTheSchemesAnMCPOneTakes() {
	for name, test := range map[string]struct {
		change  func(c *CustomConnector)
		refusal string
	}{
		"api_key": {func(c *CustomConnector) {}, ""},
		"bearer":  {func(c *CustomConnector) { c.Schemes = []string{"bearer"} }, ""},
		"none":    {func(c *CustomConnector) { c.Schemes = []string{"none"} }, ""},
		"oauth2_code": {func(c *CustomConnector) {
			c.Schemes, c.Client.Registration = []string{"oauth2_code"}, []ClientRegistrationMethod{ClientDCR}
		}, "oauth2_code is not available for an http custom connector"},
		"client credentials": {func(c *CustomConnector) { c.Schemes = []string{"oauth2_client_credentials"} },
			"oauth2_client_credentials is not available for a custom connector"},
		"an operator client": {func(c *CustomConnector) { c.Client.Registration = []ClientRegistrationMethod{ClientOperator} },
			"client.registration cannot be operator"},
		"oauth2_code without a registration": {func(c *CustomConnector) { c.Schemes = []string{"oauth2_code"} },
			"client.registration is required with oauth2_code"},
	} {
		custom := httpCustom()
		test.change(&custom)
		_, err := CustomManifest(custom)
		if test.refusal == "" {
			s.NoError(err, name)
		} else {
			s.ErrorContains(err, test.refusal, name)
		}
	}
}

func (s *CustomSuite) TestAnHTTPConnectorOfAnotherShapeIsRefused() {
	for name, test := range map[string]struct {
		change  func(c *CustomConnector)
		refusal string
	}{
		"an MCP endpoint too": {func(c *CustomConnector) { c.Endpoint = "https://mcp.weather.example/mcp" },
			"a custom connector has an MCP endpoint or an http base_url and tools, not both"},
		"no tool":          {func(c *CustomConnector) { c.Tools = nil }, "an http custom connector needs at least one tool"},
		"no base URL":      {func(c *CustomConnector) { c.BaseURL = "" }, `endpoints.http: "" does not start with https://`},
		"a plain http URL": {func(c *CustomConnector) { c.BaseURL = "http://api.weather.example/v1" }, `endpoints.http: "http://api.weather.example/v1" does not start with https://`},
		"a tool that breaks a rule": {func(c *CustomConnector) { c.Tools[1].ReadOnly = true },
			`sources[0].http_tools[1].read_only: a read-only tool must be GET or HEAD, and "create_alert" is POST`},
	} {
		custom := httpCustom()
		test.change(&custom)
		_, err := CustomManifest(custom)
		s.ErrorContains(err, test.refusal, name)
	}
}

// AsCustom keeps an http manifest only when CustomManifest would have made it: anything else a
// manifest says is refused, as for an MCP one.
func (s *CustomSuite) TestAnHTTPManifestWithMoreThanACustomConnectorSetsIsRefused() {
	for name, change := range map[string]func(m *Manifest){
		"a source header": func(m *Manifest) { m.Sources[0].Headers = map[string]string{"X-Api-Version": "2"} },
		"needs_scopes": func(m *Manifest) {
			m.Sources[0].Tools = []ToolRule{{Name: "get_forecast", NeedsScopes: []string{"read"}}}
		},
		"an mcp source too": func(m *Manifest) {
			m.Sources = append(m.Sources, SourceRule{Kind: SourceKindMCP, Endpoint: SourceKindHTTP})
		},
		"another endpoint": func(m *Manifest) { m.Endpoints["api_base"] = "https://api.weather.example" },
		"a hook":           func(m *Manifest) { m.Hooks = map[string]HookName{HookAfterToken: "weather.check"} },
		"sensitive":        func(m *Manifest) { m.Sensitive = true },
	} {
		m, err := CustomManifest(httpCustom())
		s.Require().NoError(err)
		change(&m)

		_, err = m.AsCustom("custom_weather")

		s.ErrorContains(err, "it is not a manifest a custom connector can have", name)
	}
}

// A manifest with both an MCP and an http endpoint is neither connector, whichever source it has.
func (s *CustomSuite) TestAManifestWithBothEndpointsIsRefused() {
	m, err := CustomManifest(httpCustom())
	s.Require().NoError(err)
	m.Endpoints[SourceKindMCP] = "https://mcp.weather.example/mcp"

	_, err = m.AsCustom("custom_weather")

	s.ErrorContains(err, "not both")
}

// CustomConnectorSuite holds CustomManifest to the limits the API's request schema documents,
// each at the limit and one past it. A length is counted in characters, so a name of é, two
// bytes each, is at the limit with as many é as the limit.
type CustomConnectorSuite struct {
	suite.Suite
}

func TestCustomConnectorSuite(t *testing.T) {
	suite.Run(t, new(CustomConnectorSuite))
}

// manifest is CustomManifest's error for a bearer connector custom_acme changed by edit.
func (s *CustomConnectorSuite) manifest(edit func(*CustomConnector)) error {
	connector := CustomConnector{ID: "custom_acme", Name: "Acme", Endpoint: "https://mcp.acme.example/mcp", Schemes: []string{"bearer"}}
	edit(&connector)
	_, err := CustomManifest(connector)
	return err
}

func (s *CustomConnectorSuite) TestAnIDIsCustomThenALetterThenUpTo56More() {
	s.NoError(s.manifest(func(c *CustomConnector) { c.ID = "custom_a" + strings.Repeat("b", 56) }), "64 in all")
	s.NoError(s.manifest(func(c *CustomConnector) { c.ID = "custom_a_9" }))
	for _, id := range []string{"custom_a" + strings.Repeat("b", 57), "custom_9abc", "custom_", "custom_Abc", "custom_a-b", "acme", "xcustom_acme"} {
		s.ErrorContains(s.manifest(func(c *CustomConnector) { c.ID = id }), "is not custom_ then a lowercase letter", id)
	}
}

func (s *CustomConnectorSuite) TestANameIsAtMost120Characters() {
	s.NoError(s.manifest(func(c *CustomConnector) { c.Name = strings.Repeat("é", 120) }))
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Name = strings.Repeat("é", 121) }), "name is longer than 120 characters")
}

func (s *CustomConnectorSuite) TestACategoryIsAtMost80Characters() {
	s.NoError(s.manifest(func(c *CustomConnector) { c.Category = strings.Repeat("é", 80) }))
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Category = strings.Repeat("é", 81) }), "category is longer than 80 characters")
}

func (s *CustomConnectorSuite) TestADescriptionIsAtMost1000Characters() {
	s.NoError(s.manifest(func(c *CustomConnector) { c.Description = strings.Repeat("é", 1000) }))
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Description = strings.Repeat("é", 1001) }),
		"description is longer than 1000 characters")
}

func (s *CustomConnectorSuite) TestAnEndpointIsAtMost2048Characters() {
	base := "https://mcp.acme.example/"
	s.NoError(s.manifest(func(c *CustomConnector) { c.Endpoint = base + strings.Repeat("a", 2048-len(base)) }))
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Endpoint = base + strings.Repeat("a", 2049-len(base)) }),
		"endpoint is longer than 2048 characters")
}

func (s *CustomConnectorSuite) TestAConnectorAsksForAtMost100Scopes() {
	scopes := func(n int) []string {
		list := make([]string, n)
		for i := range list {
			list[i] = fmt.Sprintf("scope%d", i)
		}
		return list
	}
	s.NoError(s.manifest(func(c *CustomConnector) { c.Scopes = scopes(100) }))
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Scopes = scopes(101) }), "scopes holds more than 100")
}

// RFC 6749 section 3.3: scope-token = 1*( %x21 / %x23-5B / %x5D-7E ).
func (s *CustomConnectorSuite) TestAScopeIsAnRFC6749ScopeToken() {
	s.NoError(s.manifest(func(c *CustomConnector) {
		c.Scopes = []string{"!", "#", "[", "]", "~", "repo:read", "https://acme.example/read"}
	}))
	for _, scope := range []string{"", "a b", `a"b`, `a\b`, "é", "a\x00", "a\tb"} {
		s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Scopes = []string{"read", scope} }), "scopes[1] is not an RFC 6749 scope token",
			"%q", scope)
	}
}

func (s *CustomConnectorSuite) TestAScopeListedTwiceIsRefused() {
	s.ErrorContains(s.manifest(func(c *CustomConnector) { c.Scopes = []string{"read", "write", "read"} }), `scopes[2] "read" is listed twice`)
}

// An http connector meets the same limits as an MCP one: CustomManifest checks them before it tells the two apart.
func (s *CustomConnectorSuite) TestAnHTTPConnectorMeetsTheLimitsOfAnMCPOne() {
	for name, test := range map[string]struct {
		edit func(*CustomConnector)
		err  string
	}{
		"an id":                {func(c *CustomConnector) { c.ID = "custom_9weather" }, "is not custom_ then a lowercase letter"},
		"a name":               {func(c *CustomConnector) { c.Name = strings.Repeat("é", 121) }, "name is longer than 120 characters"},
		"a category":           {func(c *CustomConnector) { c.Category = strings.Repeat("é", 81) }, "category is longer than 80 characters"},
		"a description":        {func(c *CustomConnector) { c.Description = strings.Repeat("é", 1001) }, "description is longer than 1000 characters"},
		"a scope with a space": {func(c *CustomConnector) { c.Scopes = []string{"a b"} }, "scopes[0] is not an RFC 6749 scope token"},
		"a scope listed twice": {func(c *CustomConnector) { c.Scopes = []string{"a", "a"} }, "is listed twice"},
	} {
		connector := httpCustom()
		test.edit(&connector)

		_, err := CustomManifest(connector)

		s.ErrorContains(err, test.err, name)
	}

	connector := httpCustom()
	connector.Name, connector.Description = strings.Repeat("é", 120), strings.Repeat("é", 1000)
	_, err := CustomManifest(connector)
	s.NoError(err, "at the limits")
}

// An imported manifest reaches the limits through AsCustom.
func (s *CustomConnectorSuite) TestAManifestPastALimitIsNoCustomOne() {
	manifest, err := CustomManifest(CustomConnector{ID: "custom_acme", Name: "Acme", Endpoint: "https://mcp.acme.example/mcp", Schemes: []string{"bearer"}})
	s.Require().NoError(err)
	_, err = manifest.AsCustom("custom_acme")
	s.Require().NoError(err)

	manifest.Name = strings.Repeat("a", 121)
	_, err = manifest.AsCustom("custom_acme")

	s.ErrorContains(err, "name is longer than 120 characters")
}
