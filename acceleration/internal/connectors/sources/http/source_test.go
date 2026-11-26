package http

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core/contracttest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/bearer"
	"github.com/GetStream/Vision-Agents/acceleration/internal/egress"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
)

// requestTimeout bounds every request a test's client sends, so a broken source fails the test
// instead of hanging it. A choice: loopback answers in milliseconds.
const requestTimeout = 5 * time.Second

// shortTimeout is a timeout a test waits out on purpose: long enough for nothing on loopback,
// short against slowFor. A choice.
const shortTimeout = 100 * time.Millisecond

// slowFor is how long the provider's /slow takes to answer, past shortTimeout. A choice.
const slowFor = 300 * time.Millisecond

func TestHTTPSourceContract(t *testing.T) {
	suite.Run(t, &contracttest.SourceContract{New: func(t *testing.T) contracttest.SourceSubject {
		p := newProvider(t)
		binding := p.binding(t, requestTimeout, contractTools()...)
		return contracttest.SourceSubject{
			Source:  New(),
			Binding: binding,
			Calls:   p.calls,
			// The manifest is the provider's list, so a changed schema is a changed manifest.
			ChangeSchema: func() {
				binding.Manifest.Sources[0].HTTPTools[0].Parameters["loud"] = core.HTTPParameter{Type: "boolean"}
			},
		}
	}})
}

// SourceSuite is what the http source does beyond the contract, against an API behind a TLS
// server on loopback that takes one bearer token, reached through core.Transports with the
// bearer scheme.
type SourceSuite struct {
	suite.Suite
	ctx      context.Context
	provider *provider
	source   *Source
}

func TestSourceSuite(t *testing.T) {
	suite.Run(t, new(SourceSuite))
}

func (s *SourceSuite) SetupTest() {
	s.ctx = context.Background()
	s.provider = newProvider(s.T())
	s.source = New()
}

// AC of AI-3674: a GET sends its {name} in the path, escaped as one segment, and its other
// parameters in the query, each as the model wrote it; one left out is not sent. The manifest's
// headers and the connection's credential are on it.
func (s *SourceSuite) TestAGETSendsItsPathParameterInThePathAndTheOthersInTheQuery() {
	tool := core.HTTPTool{Name: "get_forecast", Description: "Calls the API.", Method: http.MethodGet, Path: "/v1/forecast/{city}", ReadOnly: true,
		Parameters: map[string]core.HTTPParameter{
			"city": {Type: "string", Required: true}, "latitude": {Type: "number", Required: true},
			"current": {Type: "string"}, "days": {Type: "integer"}, "past": {Type: "boolean"}, "hourly": {Type: "boolean"},
		}}

	result, err := s.call(s.binding(tool), "get_forecast",
		`{"city":"New York/old?x","latitude":52.520000000000000001,"current":"temperature_2m","days":7,"past":false}`)

	s.Require().NoError(err)
	s.Equal("ok", llm.TextOf(result.Parts))
	sent := s.only()
	s.Equal(http.MethodGet, sent.method)
	s.Equal("/api/v1/forecast/New%20York%2Fold%3Fx", sent.path)
	s.Equal(url.Values{"latitude": {"52.520000000000000001"}, "current": {"temperature_2m"}, "days": {"7"}, "past": {"false"}},
		sent.query, "no city, which went in the path, and no hourly, which was left out")
	s.Empty(sent.body)
	s.Equal("Bearer token-1", sent.header.Get("Authorization"))
	s.Equal("2", sent.header.Get("X-Api-Version"), "the manifest's source header")
	s.Empty(sent.header.Get("Content-Type"))
}

// AC of AI-3674 and AC-8 of AI-3669: a POST sends its other parameters as a JSON object, numbers
// as the model wrote them, so 2^53+1 is not rounded through a float.
func (s *SourceSuite) TestAPOSTSendsItsOtherParametersAsAJSONBody() {
	tool := core.HTTPTool{Name: "echo", Description: "Calls the API.", Method: http.MethodPost, Path: "/items/{id}",
		Parameters: map[string]core.HTTPParameter{
			"id": {Type: "string", Required: true}, "message": {Type: "string", Required: true},
			"count": {Type: "integer"}, "urgent": {Type: "boolean"},
		}}

	_, err := s.call(s.binding(tool), "echo", `{"id":"42","message":"hello-3669","count":9007199254740993,"urgent":true}`)

	s.Require().NoError(err)
	sent := s.only()
	s.Equal(http.MethodPost, sent.method)
	s.Equal("/api/items/42", sent.path)
	s.Empty(sent.query)
	s.JSONEq(`{"message":"hello-3669","count":9007199254740993,"urgent":true}`, sent.body)
	s.Contains(sent.body, "9007199254740993")
	s.Equal("application/json", sent.header.Get("Content-Type"))
}

// AI-3669 «Decided»: the query for GET, HEAD and DELETE, a JSON body for POST, PUT and PATCH,
// which is an empty object when every parameter went in the path.
func (s *SourceSuite) TestEachMethodSendsItsParametersWhereTheRuleSays() {
	for _, test := range []struct {
		method, arguments string
		query             url.Values
		body              string
	}{
		{http.MethodGet, `{"id":"7","q":"x"}`, url.Values{"q": {"x"}}, ""},
		{http.MethodHead, `{"id":"7","q":"x"}`, url.Values{"q": {"x"}}, ""},
		{http.MethodDelete, `{"id":"7","q":"x"}`, url.Values{"q": {"x"}}, ""},
		{http.MethodPost, `{"id":"7","q":"x"}`, url.Values{}, `{"q":"x"}`},
		{http.MethodPut, `{"id":"7","q":"x"}`, url.Values{}, `{"q":"x"}`},
		{http.MethodPatch, `{"id":"7","q":"x"}`, url.Values{}, `{"q":"x"}`},
		{http.MethodPost, `{"id":"7"}`, url.Values{}, `{}`},
	} {
		s.provider.reset()
		tool := core.HTTPTool{Name: "act", Description: "Calls the API.", Method: test.method, Path: "/items/{id}",
			Parameters: map[string]core.HTTPParameter{"id": {Type: "string", Required: true}, "q": {Type: "string"}}}

		_, err := s.call(s.binding(tool), "act", test.arguments)

		s.Require().NoError(err, test.method)
		sent := s.only()
		s.Equal(test.method, sent.method)
		s.Equal("/api/items/7", sent.path, test.method)
		s.Equal(test.query, sent.query, test.method)
		s.Equal(test.body, sent.body, test.method)
	}
}

// AC-6 of AI-3669: an argument the tool does not declare is refused before anything is sent,
// whatever it is named and whatever the method, with one sentence; the declared ones alone are
// sent.
func (s *SourceSuite) TestAnArgumentTheToolDoesNotDeclareIsRefusedBeforeSending() {
	post := core.HTTPTool{Name: contracttest.ToolEcho, Description: "Calls the API.", Method: http.MethodPost, Path: "/echo",
		Parameters: map[string]core.HTTPParameter{"text": {Type: "string", Required: true}}}
	for _, tool := range []core.HTTPTool{echoTool(), post} {
		for _, arguments := range []string{
			`{"text":"hi","_method":"DELETE"}`,
			`{"text":"hi","X-HTTP-Method-Override":"DELETE"}`,
			`{"text":"hi","x-http-method-override":"DELETE"}`,
			`{"text":"hi","X-Method-Override":"DELETE","X-HTTP-Method":"DELETE"}`,
			`{"text":"hi","Text":"hi"}`,
			`{"text":"hi","extra":null}`,
			`{"text":"hi","nested":{"_method":"DELETE"}}`,
		} {
			s.provider.reset()

			_, err := s.call(s.binding(tool), contracttest.ToolEcho, arguments)

			s.EqualError(err, "http: crm__echo: an argument is not one of the tool's parameters, so nothing was sent",
				tool.Method+" "+arguments)
			s.Empty(s.provider.requests(), tool.Method+" "+arguments)
		}
	}

	_, err := s.call(s.binding(echoTool()), contracttest.ToolEcho, `{"text":"hi"}`)

	s.Require().NoError(err)
	sent := s.only()
	s.Equal(http.MethodGet, sent.method)
	s.Equal(url.Values{"text": {"hi"}}, sent.query)
}

// S1.3 of AI-3674: a segment of the path as built that holds a {name} and is empty, . or .. names
// another resource, so it is refused before anything is sent: the empty id that names the
// collection, and the .. that {a}{b} makes of . and .. A value with a / stays one segment, as AC-6
// of AI-3669 says ../admin does, and a value escaped already keeps its %, so neither is a dot
// segment.
func (s *SourceSuite) TestAPathValueThatNamesAnotherResourceIsRefusedBeforeSending() {
	one := core.HTTPTool{Name: "remove", Description: "Calls the API.", Method: http.MethodDelete, Path: "/items/{id}",
		Parameters: map[string]core.HTTPParameter{"id": {Type: "string", Required: true}}}
	inner := core.HTTPTool{Name: "remove", Description: "Calls the API.", Method: http.MethodDelete, Path: "/items/{id}/notes",
		Parameters: map[string]core.HTTPParameter{"id": {Type: "string", Required: true}}}
	pair := core.HTTPTool{Name: "remove", Description: "Calls the API.", Method: http.MethodDelete, Path: "/items/{a}{b}",
		Parameters: map[string]core.HTTPParameter{"a": {Type: "string", Required: true}, "b": {Type: "string", Required: true}}}
	number := core.HTTPTool{Name: "remove", Description: "Calls the API.", Method: http.MethodDelete, Path: "/items/{id}",
		Parameters: map[string]core.HTTPParameter{"id": {Type: "integer", Required: true}}}

	for _, test := range []struct {
		tool      core.HTTPTool
		arguments string
		// path is what the provider receives, "" when the call is refused.
		path string
	}{
		{one, `{"id":""}`, ""},
		{one, `{"id":"."}`, ""},
		{one, `{"id":".."}`, ""},
		{inner, `{"id":""}`, ""},
		{inner, `{"id":".."}`, ""},
		{pair, `{"a":".","b":"."}`, ""},
		{pair, `{"a":"","b":""}`, ""},
		{pair, `{"a":".","b":""}`, ""},
		{pair, `{"a":"","b":".."}`, ""},
		{one, `{"id":"../admin"}`, "/api/items/..%2Fadmin"},
		{one, `{"id":"..%2F"}`, "/api/items/..%252F"},
		{one, `{"id":"%2e%2e"}`, "/api/items/%252e%252e"},
		{one, `{"id":"..."}`, "/api/items/..."},
		{one, `{"id":" "}`, "/api/items/%20"},
		{inner, `{"id":"7"}`, "/api/items/7/notes"},
		{pair, `{"a":".","b":"x"}`, "/api/items/.x"},
		{pair, `{"a":"","b":"."}`, ""},
		{number, `{"id":0}`, "/api/items/0"},
	} {
		s.provider.reset()

		_, err := s.call(s.binding(test.tool), "remove", test.arguments)

		if test.path == "" {
			s.EqualError(err, "http: crm__remove: a path parameter is empty, . or .., which names another resource "+
				"than the tool's, so nothing was sent", test.tool.Path+" "+test.arguments)
			s.Empty(s.provider.requests(), test.tool.Path+" "+test.arguments)
			continue
		}
		s.Require().NoError(err, test.tool.Path+" "+test.arguments)
		s.Equal(test.path, s.only().path, test.tool.Path+" "+test.arguments)
	}
}

// AI-3673 refuses a manifest whose tool names a method override, or writes while saying it
// reads, when it is stored; Open refuses one again, so a stored manifest that skipped validation
// offers no tool and sends nothing.
func (s *SourceSuite) TestOpenRefusesASourceThatBreaksARuleOfValidation() {
	for name, change := range map[string]func(*core.SourceRule){
		"a parameter _method": func(r *core.SourceRule) {
			r.HTTPTools[0].Parameters["_method"] = core.HTTPParameter{Type: "string"}
		},
		"a parameter X-HTTP-Method-Override": func(r *core.SourceRule) {
			r.HTTPTools[0].Parameters["X-HTTP-Method-Override"] = core.HTTPParameter{Type: "string"}
		},
		"a header x-method-override": func(r *core.SourceRule) { r.Headers["x-method-override"] = "DELETE" },
		"a header X-HTTP-Method":     func(r *core.SourceRule) { r.Headers["X-HTTP-Method"] = "DELETE" },
		"a read-only POST":           func(r *core.SourceRule) { r.HTTPTools[0].Method = http.MethodPost },
		"a read-only DELETE":         func(r *core.SourceRule) { r.HTTPTools[0].Method = http.MethodDelete },
	} {
		tool := echoTool()
		tool.ReadOnly = true
		binding := s.binding(tool)
		grants := s.grants(binding, contracttest.ToolEcho)
		change(&binding.Manifest.Sources[0])

		set, err := s.source.Open(s.ctx, binding, grants)

		s.ErrorContains(err, `http: connector "custom_crm": sources[http]`, name)
		s.Nil(set, name)
	}
	s.Empty(s.provider.requests())
	tool := echoTool()
	tool.ReadOnly = true
	_, err := s.call(s.binding(tool), contracttest.ToolEcho, `{"text":"hi"}`)
	s.Require().NoError(err, "the same source unchanged opens")
}

// R1.6 of AI-3674: a write's redirect is its answer, so a 307 or a 308 never sends its body
// again, nor a DELETE to another path; a GET or a HEAD follows one within the origin. R1.2 of
// AI-3675: a write answered 301, 302 or 303 may have run, so the model reads that as a result.
func (s *SourceSuite) TestOnlyAReadFollowsARedirect() {
	for _, test := range []struct {
		method, path string
		// err is the tool error the model reads, and answer the result, both "" when the
		// redirect is followed.
		err, answer string
	}{
		{http.MethodPost, "/moved-307", "The API answered 307 Temporary Redirect", ""},
		{http.MethodPost, "/moved-308", "The API answered 308 Permanent Redirect", ""},
		{http.MethodPut, "/moved-307", "The API answered 307 Temporary Redirect", ""},
		{http.MethodPatch, "/moved-308", "The API answered 308 Permanent Redirect", ""},
		{http.MethodDelete, "/moved-307", "The API answered 307 Temporary Redirect", ""},
		{http.MethodPost, "/moved-303", "", "The API answered 303 See Other; the request may have run."},
		{http.MethodPut, "/moved-301", "", "The API answered 301 Moved Permanently; the request may have run."},
		{http.MethodPatch, "/moved-302", "", "The API answered 302 Found; the request may have run."},
		{http.MethodDelete, "/moved-303", "", "The API answered 303 See Other; the request may have run."},
		{http.MethodPost, "/moved-reason", "", "The API answered 303 See Other; the request may have run."},
		{http.MethodGet, "/moved-307", "", ""},
		{http.MethodGet, "/moved-301", "", ""},
		{http.MethodGet, "/moved-302", "", ""},
		{http.MethodGet, "/moved-303", "", ""},
		{http.MethodHead, "/moved-308", "", ""},
		{http.MethodHead, "/moved-303", "", ""},
		// A 3xx with no Location is not followed (net/http): a read's is a tool error, as before.
		{http.MethodGet, "/moved-nowhere", "The API answered 302 Found", ""},
	} {
		s.provider.reset()
		tool := core.HTTPTool{Name: "act", Description: "Calls the API.", Method: test.method, Path: test.path,
			Parameters: map[string]core.HTTPParameter{"q": {Type: "string"}}}

		result, err := s.call(s.binding(tool), "act", `{"q":"x"}`)

		received := s.provider.requests()
		if test.answer != "" {
			s.Require().NoError(err, test.method+" "+test.path)
			s.Equal(test.answer, llm.TextOf(result.Parts), test.method+" "+test.path)
			s.Len(received, 1, test.method+" "+test.path+": the target got nothing")
			continue
		}
		if test.err == "" {
			s.Require().NoError(err, test.method+" "+test.path)
			s.Require().Len(received, 2, test.method+" "+test.path)
			s.Equal("/api/target", received[1].path, test.method+" "+test.path)
			s.Equal(test.method, received[1].method, test.method+" "+test.path)
			continue
		}
		var reported *core.ToolError
		s.Require().ErrorAs(err, &reported, test.method+" "+test.path)
		s.Equal(test.err, reported.Message, test.method+" "+test.path)
		s.Len(received, 1, test.method+" "+test.path+": the target got nothing")
	}
}

// A redirect to another origin is never followed, so the credential never reaches it: the
// egress client's policy, which the toolset's client keeps for a GET.
func (s *SourceSuite) TestARedirectToAnotherOriginNeverCarriesTheCredential() {
	var reached []string
	other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = append(reached, r.Header.Get("Authorization"))
	}))
	defer other.Close()
	first := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	defer first.Close()
	transports, err := core.NewTransports(core.TransportsConfig{Resolver: tokenResolver{}, Timeout: requestTimeout,
		NewClient: loopback(first.Client())})
	s.Require().NoError(err)
	binding := s.binding(echoTool())
	binding.Manifest.Endpoints[Kind] = first.URL + "/api"
	binding.HTTP = transports.Client(core.ConnectionRef{CustomerID: "app", ConnectionID: "crm-1"}, bearer.New())

	_, err = s.call(binding, contracttest.ToolEcho, `{"text":"hi"}`)

	s.Contains(core.LoggedMessage(err, nil), "egress: redirect to another host refused")
	s.Empty(reached, "the other origin got no request at all")
}

// A client with no redirect policy of its own stops a read's redirects where net/http does.
func (s *SourceSuite) TestAClientWithNoRedirectPolicyStopsAfterTenRedirects() {
	binding := s.binding(core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/loop"})
	binding.HTTP.CheckRedirect = nil

	_, err := s.call(binding, "get", `{}`)

	s.Contains(core.LoggedMessage(err, nil), "stopped after 10 redirects")
	s.Len(s.provider.requests(), 10, "the first request and the nine redirects before the tenth, which is not followed")
}

// S1.2 of AI-3674: an API that echoes the request's credential in its error does not put it in
// the conversation: the tool error is redacted with the call's Exchange, as the log line is,
// before it is cut, so a credential the cut falls in leaves no start of itself either.
func (s *SourceSuite) TestAToolErrorEchoingTheCredentialIsRedacted() {
	for path, want := range map[string]string{
		"/echo-credential": `The API answered 403 Forbidden: {"error":"key Bearer [redacted] may not read this"}`,
		"/echo-at-the-cut": "",
	} {
		tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: path}
		set, err := s.source.Open(s.ctx, s.binding(tool), s.grants(s.binding(tool), "get"))
		s.Require().NoError(err)
		ctx, exchange := core.WithExchange(s.ctx)

		_, err = set.Call(ctx, llm.ToolCall{Name: "crm__get", Arguments: `{}`})

		var reported *core.ToolError
		s.Require().ErrorAs(err, &reported, path)
		s.NotContains(reported.Message, "tok", path)
		s.NotContains(core.LoggedMessage(err, exchange.Redact), "tok", path)
		s.Equal(http.StatusForbidden, exchange.Status(), "the caller's Exchange saw the answer: %s", path)
		if want != "" {
			s.Equal(want, reported.Message, path)
			continue
		}
		s.True(strings.HasSuffix(reported.Message, "Bearer [re"+core.TruncatedMarker), path)
	}
}

// An API that echoes the request's credential in a 2xx does not put it in the conversation
// either, with or without an Exchange of the caller's; the path and query it names are kept.
func (s *SourceSuite) TestA2xxEchoingTheCredentialIsRedactedAndKeepsItsPath() {
	tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/echo-credential-ok/{id}", ReadOnly: true,
		Parameters: map[string]core.HTTPParameter{"id": {Type: "string", Required: true}, "q": {Type: "string"}}}
	for name, ctx := range map[string]context.Context{"the caller's Exchange": func() context.Context {
		ctx, _ := core.WithExchange(s.ctx)
		return ctx
	}(), "no Exchange": s.ctx} {
		set, err := s.source.Open(s.ctx, s.binding(tool), s.grants(s.binding(tool), "get"))
		s.Require().NoError(err)

		result, err := set.Call(ctx, llm.ToolCall{Name: "crm__get", Arguments: `{"id":"item-7","q":"open"}`})

		s.Require().NoError(err, name)
		s.Equal(`{"self":"/api/echo-credential-ok/item-7?q=open","seen":"Bearer [redacted]"}`, llm.TextOf(result.Parts), name)
	}
}

// A call without an Exchange of its caller's is redacted all the same, with one of its own.
func (s *SourceSuite) TestAToolErrorIsRedactedWithNoExchangeOfTheCallers() {
	_, err := s.call(s.binding(core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/echo-credential"}), "get", `{}`)

	var reported *core.ToolError
	s.Require().ErrorAs(err, &reported)
	s.Equal(`The API answered 403 Forbidden: {"error":"key Bearer [redacted] may not read this"}`, reported.Message)
}

func (s *SourceSuite) TestNoArgumentsAtAllAreAnEmptyObject() {
	tool := core.HTTPTool{Name: "list", Description: "Calls the API.", Method: http.MethodGet, Path: "/items"}

	result, err := s.call(s.binding(tool), "list", "  ")

	s.Require().NoError(err)
	s.Equal("ok", llm.TextOf(result.Parts))
	s.Equal("/api/items", s.only().uri, "no query, not even an empty one")
}

// The endpoint and the path are joined as text: the endpoint's own path stays, which resolving
// the path as a reference would drop, and a / ending the endpoint is not doubled.
func (s *SourceSuite) TestTheEndpointsOwnPathIsKeptAndItsTrailingSlashNotDoubled() {
	binding := s.binding(echoTool())
	binding.Manifest.Endpoints[Kind] = s.provider.URL + "/api/"

	_, err := s.call(binding, contracttest.ToolEcho, `{"text":"hi"}`)

	s.Require().NoError(err)
	s.Equal("/api/echo", s.only().path)
}

// AC-4 of AI-3669: an answer that is not a 2xx is a tool error naming its status, with the
// API's words when it said any.
func (s *SourceSuite) TestAnAnswerThatIsNotA2xxIsAToolErrorNamingItsStatus() {
	for path, want := range map[string]string{
		"/missing":          "The API answered 404 Not Found: no such item",
		"/multiple-choices": "The API answered 300 Multiple Choices",
		"/refused":          "The API answered 401 Unauthorized: bad token",
		"/reason-own":       "The API answered 403 Forbidden",
		"/reason-unknown":   "The API answered 499",
	} {
		tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: path}

		_, err := s.call(s.binding(tool), "get", `{}`)

		var reported *core.ToolError
		s.Require().ErrorAs(err, &reported, path)
		s.Equal(want, reported.Message, path)
	}
	result, err := s.call(s.binding(core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/last-2xx"}), "get", `{}`)
	s.Require().NoError(err, "299 is the last 2xx")
	s.Equal("ok", llm.TextOf(result.Parts))
}

// R1.5: a 2xx with no body, such as a DELETE's 204 or any HEAD's answer, is its status line, so
// the model can tell done from nothing.
func (s *SourceSuite) TestAnEmpty2xxIsItsStatus() {
	for want, tool := range map[string]core.HTTPTool{
		"The API answered 204 No Content.": {Name: "remove", Description: "Calls the API.", Method: http.MethodDelete, Path: "/no-content"},
		"The API answered 200 OK.":         {Name: "check", Description: "Calls the API.", Method: http.MethodHead, Path: "/items"},
		// S1.1 of AI-3675: the API's own reason phrase, here echoing the token, is not read.
		"The API answered 202 Accepted.": {Name: "seen", Description: "Calls the API.", Method: http.MethodGet, Path: "/reason-echo"},
	} {
		result, err := s.call(s.binding(tool), tool.Name, `{}`)

		s.Require().NoError(err, tool.Method)
		s.Equal(want, llm.TextOf(result.Parts), tool.Method)
	}
}

func (s *SourceSuite) TestAToolErrorOverTheCapIsCutWithTheMarker() {
	tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/error-large"}

	_, err := s.call(s.binding(tool), "get", `{}`)

	var reported *core.ToolError
	s.Require().ErrorAs(err, &reported)
	s.LessOrEqual(len(reported.Message), core.MaxResultBytes)
	s.True(strings.HasSuffix(reported.Message, core.TruncatedMarker))
}

func (s *SourceSuite) TestABodyThatIsNotTextReadsAsNotText() {
	tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/binary"}

	result, err := s.call(s.binding(tool), "get", `{}`)

	s.Require().NoError(err)
	s.Equal(nonText, llm.TextOf(result.Parts))
}

// The provider answers 4 MiB of text and then a byte that is not UTF-8: read past the cap, the
// body would be no text at all.
func (s *SourceSuite) TestABodyIsReadOnlyUpToTheCap() {
	tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/huge"}

	result, err := s.call(s.binding(tool), "get", `{}`)

	s.Require().NoError(err)
	text := llm.TextOf(result.Parts)
	s.True(strings.HasPrefix(text, "aaaa"))
	s.True(strings.HasSuffix(text, core.TruncatedMarker))
}

// R1.2: valid text whose cut at the cap falls inside a rune is still text, cut before that rune.
func (s *SourceSuite) TestTextTheCapCutsInsideARuneIsStillText() {
	tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/huge-runes"}

	result, err := s.call(s.binding(tool), "get", `{}`)

	s.Require().NoError(err)
	text := llm.TextOf(result.Parts)
	s.True(strings.HasPrefix(text, "aaaa"), "not %q", nonText)
	s.True(strings.HasSuffix(text, core.TruncatedMarker))
}

// R1.3: the last byte under the 4 MiB cap is read: a body whose last byte there is not UTF-8 is
// not text, under a cap even one byte lower too.
func (s *SourceSuite) TestTheLastByteUnderTheCapIsRead() {
	tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/bad-byte-at-the-cap"}

	result, err := s.call(s.binding(tool), "get", `{}`)

	s.Require().NoError(err)
	s.Equal(nonText, llm.TextOf(result.Parts))
}

// R1.3: a 4xx or a 5xx whose body is not text names its status and says so, never the bytes.
func (s *SourceSuite) TestAToolErrorWithABodyThatIsNotTextSaysItIsNotText() {
	_, err := s.call(s.binding(core.HTTPTool{Name: "broken", Description: "Calls the API.", Method: http.MethodGet, Path: "/broken"}), "broken", `{}`)

	var reported *core.ToolError
	s.Require().ErrorAs(err, &reported)
	s.Equal("The API answered 500 Internal Server Error: "+nonText, reported.Message)
}

// AI-3674, from the AI-3673 review (R1.3): revision 2 turns POST x into a read-only GET. A grant
// made for one revision's x never opens the other's, so a connection still pinned to revision 1
// never runs the POST under a grant made for the read-only GET, nor the reverse.
func (s *SourceSuite) TestAGrantForAnotherRevisionOfAToolIsNeverOpened() {
	post := core.HTTPTool{Name: "x", Description: "Calls the API.", Method: http.MethodPost, Path: "/x"}
	get := core.HTTPTool{Name: "x", Description: "Calls the API.", Method: http.MethodGet, Path: "/x", ReadOnly: true}
	revision1, revision2 := s.binding(post), s.binding(get)
	granted1, granted2 := s.grants(revision1, "x"), s.grants(revision2, "x")

	for name, pair := range map[string]struct {
		binding core.ResolvedBinding
		grants  []core.ToolGrant
	}{
		"pinned to 1, granted at 2": {revision1, granted2},
		"pinned to 2, granted at 1": {revision2, granted1},
	} {
		set, err := s.source.Open(s.ctx, pair.binding, pair.grants)
		s.Require().NoError(err, name)
		_, called := set.Call(s.ctx, llm.ToolCall{Name: "crm__x", Arguments: `{}`})

		s.Empty(set.Tools(), name)
		s.Error(called, name)
	}
	s.Zero(s.provider.calls("x"), "nothing reached the API")

	_, err := s.call(revision1, "x", `{}`)
	s.Require().NoError(err, "the grant made at its own revision opens it")
	s.Equal(http.MethodPost, s.only().method)
}

// What decides a call is in the digest as much as what the model is shown: each change below
// is another digest, so a grant made before it no longer opens the tool.
func (s *SourceSuite) TestTheDigestCoversEveryFieldOfTheTool() {
	base := func() core.HTTPTool {
		return core.HTTPTool{Name: "get_forecast", Description: "The forecast.", Method: http.MethodGet,
			Path: "/v1/forecast/{city}", Parameters: map[string]core.HTTPParameter{"city": {Type: "string", Required: true},
				"days": {Type: "integer", Description: "How many days."}}}
	}
	before, err := ToolSchemaDigest(base())
	s.Require().NoError(err)
	again, err := ToolSchemaDigest(base())
	s.Require().NoError(err)
	s.Equal(before, again)

	for name, change := range map[string]func(*core.HTTPTool){
		"name":        func(t *core.HTTPTool) { t.Name = "get_weather" },
		"description": func(t *core.HTTPTool) { t.Description = "The weather." },
		"method":      func(t *core.HTTPTool) { t.Method = http.MethodHead },
		"path":        func(t *core.HTTPTool) { t.Path = "/v2/forecast/{city}" },
		"read_only":   func(t *core.HTTPTool) { t.ReadOnly = true },
		"a parameter's type": func(t *core.HTTPTool) {
			t.Parameters["days"] = core.HTTPParameter{Type: "number", Description: "How many days."}
		},
		"a parameter required": func(t *core.HTTPTool) {
			t.Parameters["days"] = core.HTTPParameter{Type: "integer", Description: "How many days.", Required: true}
		},
		"a parameter's words": func(t *core.HTTPTool) { t.Parameters["days"] = core.HTTPParameter{Type: "integer"} },
		"one parameter more":  func(t *core.HTTPTool) { t.Parameters["past"] = core.HTTPParameter{Type: "boolean"} },
	} {
		tool := base()
		change(&tool)
		after, err := ToolSchemaDigest(tool)
		s.Require().NoError(err, name)
		s.NotEqual(before, after, name)
	}
}

// The model is shown each declared parameter with its type and its description, and the
// required ones; a tool that requires none lists no required.
func (s *SourceSuite) TestTheInputSchemaIsTheDeclaredParameters() {
	forecast := core.HTTPTool{Name: "get_forecast", Description: "The forecast.", Method: http.MethodGet,
		Path: "/v1/forecast/{city}", Parameters: map[string]core.HTTPParameter{
			"days": {Type: "integer", Description: "How many days."}, "city": {Type: "string", Required: true},
			"at": {Type: "number", Required: true}}}
	list := core.HTTPTool{Name: "list", Description: "Calls the API.", Method: http.MethodGet, Path: "/items",
		Parameters: map[string]core.HTTPParameter{"q": {Type: "string"}}}
	binding := s.binding(forecast, list)

	specs, err := s.source.Discover(s.ctx, binding)

	s.Require().NoError(err)
	s.Require().Len(specs, 2)
	want := map[string]any{"type": "object", "properties": map[string]any{
		"at": map[string]any{"type": "number"}, "city": map[string]any{"type": "string"},
		"days": map[string]any{"type": "integer", "description": "How many days."},
	}, "required": []any{"at", "city"}}
	s.Equal("get_forecast", specs[0].Name)
	s.Equal("The forecast.", specs[0].Description)
	s.Equal(want, specs[0].InputSchema)
	s.Equal(map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}},
		specs[1].InputSchema)
	set, err := s.source.Open(s.ctx, binding, s.grants(binding, "get_forecast"))
	s.Require().NoError(err)
	s.Equal([]llm.Tool{{Name: "crm__get_forecast", Description: "The forecast.", Parameters: want}}, set.Tools())
	s.Empty(set.Instructions(), "an API says nothing when a session opens")
}

func (s *SourceSuite) TestABindingNameMustBeSetAndHoldNoSeparator() {
	for _, name := range []string{"", "my__crm"} {
		binding := s.binding(echoTool())
		binding.Binding.Name = name

		_, err := s.source.Open(s.ctx, binding, s.grants(binding, contracttest.ToolEcho))

		s.Error(err, name)
	}
}

func (s *SourceSuite) TestABindingWithNoClientIsRefused() {
	binding := s.binding(echoTool())
	grants := s.grants(binding, contracttest.ToolEcho)
	binding.HTTP = nil

	_, err := s.source.Open(s.ctx, binding, grants)

	s.ErrorContains(err, "no client")
}

func (s *SourceSuite) TestAManifestWithNoHTTPSourceOrNoEndpointIsRefused() {
	noSource := s.binding(echoTool())
	noSource.Manifest.Sources = []core.SourceRule{{Kind: core.SourceKindMCP, Endpoint: core.SourceKindMCP}}
	noEndpoint := s.binding(echoTool())
	noEndpoint.Manifest.Endpoints = map[string]string{}

	for want, binding := range map[string]core.ResolvedBinding{"has no http source": noSource, "has no http endpoint": noEndpoint} {
		_, err := s.source.Discover(s.ctx, binding)
		s.ErrorContains(err, want)
		_, err = s.source.Open(s.ctx, binding, nil)
		s.ErrorContains(err, want)
	}
}

// The connection's client cuts a request at its own timeout, which validate's requests keep; an
// opened toolset's copy waits at least callTimeout, so the caller's deadline ends a call first.
func (s *SourceSuite) TestAnOpenedToolsetWaitsPastTheConnectionsClientTimeout() {
	tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/slow"}
	binding := s.provider.binding(s.T(), shortTimeout, tool)

	result, err := s.call(binding, "get", `{}`)

	s.Require().NoError(err)
	s.Equal("late", llm.TextOf(result.Parts))
	s.Equal(shortTimeout, binding.HTTP.Timeout, "the binding's own client is left as it was")
}

// A client that already waits longer than callTimeout keeps its own timeout.
func (s *SourceSuite) TestAClientWaitingLongerThanCallTimeoutKeepsItsTimeout() {
	for timeout, want := range map[time.Duration]time.Duration{
		shortTimeout: defaultCallTimeout, defaultCallTimeout: defaultCallTimeout, time.Minute: time.Minute,
	} {
		binding := s.provider.binding(s.T(), timeout, echoTool())

		set, err := s.source.Open(s.ctx, binding, s.grants(binding, contracttest.ToolEcho))

		s.Require().NoError(err)
		s.Equal(want, set.(*Toolset).client.Timeout, timeout)
	}
}

func (s *SourceSuite) TestABodyCutShortIsAnErrorNotAnAnswer() {
	tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/cut"}

	_, err := s.call(s.binding(tool), "get", `{}`)

	s.ErrorIs(err, io.ErrUnexpectedEOF)
	s.EqualError(err, unreachedSentence, "what the model reads")
	s.Contains(core.LoggedMessage(err, nil), "unexpected EOF", "what the log line reads")
	var reported *core.ToolError
	s.False(errors.As(err, &reported), "the API never finished its answer, so it reported nothing")
}

// S1.1 of AI-3674: a host the egress policy refuses and one that does not resolve read the same
// one sentence, with no URL, no argument and nothing of the policy or the resolver; the log line
// keeps why (core.LoggedMessage).
func (s *SourceSuite) TestACallThatReachesNoAPIReadsOneSentence() {
	lookup := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/people/{id}",
		Parameters: map[string]core.HTTPParameter{"id": {Type: "string", Required: true}, "email": {Type: "string"}}}
	transports, err := core.NewTransports(core.TransportsConfig{Resolver: tokenResolver{}, Timeout: requestTimeout})
	s.Require().NoError(err)

	for _, endpoint := range []string{"https://127.0.0.1:8443/api", "https://no-such-host-ai3674.invalid/api"} {
		binding := s.binding(lookup)
		binding.Manifest.Endpoints[Kind] = endpoint
		binding.HTTP = transports.Client(core.ConnectionRef{CustomerID: "app", ConnectionID: "crm-1"}, bearer.New())

		_, err := s.call(binding, "get", `{"id":"u1","email":"alice@example.com"}`)

		s.EqualError(err, unreachedSentence, endpoint)
		s.Contains(core.LoggedMessage(err, nil), "egress", endpoint)
	}
}

// unreachedSentence is what the model reads of a call to crm__get that got no whole answer.
const unreachedSentence = "http: crm__get: the API could not be reached, or its answer did not arrive whole"

func (s *SourceSuite) TestTheBindingsTimeoutBoundsACall() {
	tool := core.HTTPTool{Name: "get", Description: "Calls the API.", Method: http.MethodGet, Path: "/slow"}
	binding := s.binding(tool)
	binding.Binding.Timeout = shortTimeout

	_, err := s.call(binding, "get", `{}`)

	s.ErrorIs(err, context.DeadlineExceeded)
}

// The provider's /slow sends nothing to a client that left. A client's TLS close_notify (crypto/tls
// Conn.Close) reaches the server before its TCP close, so a handler that returned on it would be
// answered 200 with no body, and net/http's client takes an answer that races its own close
// (transport.go, "The pconn closing raced with the response"): a call cut by the binding's
// timeout would then read "The API answered 200 OK." and no error.
func (s *SourceSuite) TestASlowAnswerIsNeverSentToAClientThatLeft() {
	conn, err := tls.Dial("tcp", s.provider.Listener.Addr().String(), s.provider.Client().Transport.(*http.Transport).TLSClientConfig)
	s.Require().NoError(err)
	defer func() { _ = conn.Close() }()
	_, err = io.WriteString(conn, "GET /api/slow HTTP/1.1\r\nHost: crm\r\nAuthorization: Bearer token-1\r\n\r\n")
	s.Require().NoError(err)
	// The close_notify alone, the TCP connection left open to read what the server sends.
	s.Require().NoError(conn.CloseWrite())
	s.Require().NoError(conn.SetReadDeadline(time.Now().Add(requestTimeout)))

	answer, _ := io.ReadAll(conn)

	s.Empty(string(answer))
	s.Len(s.provider.requests(), 1, "the request reached the provider")
}

// call opens a toolset of the binding granting tool at its current digest and calls it.
func (s *SourceSuite) call(binding core.ResolvedBinding, tool, arguments string) (core.Result, error) {
	set, err := s.source.Open(s.ctx, binding, s.grants(binding, tool))
	s.Require().NoError(err)
	s.T().Cleanup(set.Close)
	s.Require().Len(set.Tools(), 1, tool)
	return set.Call(s.ctx, llm.ToolCall{Name: binding.Binding.Name + Separator + tool, Arguments: arguments})
}

// grants grants tools at the digest Discover gives them now.
func (s *SourceSuite) grants(binding core.ResolvedBinding, tools ...string) []core.ToolGrant {
	specs, err := s.source.Discover(s.ctx, binding)
	s.Require().NoError(err)
	var grants []core.ToolGrant
	for _, spec := range specs {
		for _, tool := range tools {
			if spec.Name == tool {
				grants = append(grants, core.ToolGrant{Name: spec.Name, SchemaDigest: spec.SchemaDigest})
			}
		}
	}
	s.Require().Len(grants, len(tools))
	return grants
}

// only is the one request the provider received.
func (s *SourceSuite) only() sent {
	received := s.provider.requests()
	s.Require().Len(received, 1)
	return received[0]
}

func (s *SourceSuite) binding(tools ...core.HTTPTool) core.ResolvedBinding {
	return s.provider.binding(s.T(), requestTimeout, tools...)
}

func echoTool() core.HTTPTool {
	return core.HTTPTool{Name: contracttest.ToolEcho, Description: "Returns the text it is given.", Method: http.MethodGet,
		Path: "/echo", Parameters: map[string]core.HTTPParameter{"text": {Type: "string", Required: true}}}
}

// contractTools are the three tools contracttest.SourceContract calls.
func contractTools() []core.HTTPTool {
	return []core.HTTPTool{
		echoTool(),
		{Name: contracttest.ToolLarge, Description: "Answers at length.", Method: http.MethodGet, Path: "/large"},
		{Name: contracttest.ToolBroken, Description: "Fails with a picture.", Method: http.MethodGet, Path: "/broken"},
	}
}

// sent is one request as the provider received it.
type sent struct {
	method string
	// uri is the request line's target, and path its path as it was sent, escaped.
	uri    string
	path   string
	query  url.Values
	body   string
	header http.Header
}

// provider is an API behind a TLS server that takes only the bearer token token-1, records each
// request and answers by path.
type provider struct {
	*httptest.Server
	mu       sync.Mutex
	received []sent
}

func newProvider(t *testing.T) *provider {
	p := &provider{}
	p.Server = httptest.NewTLSServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.Close)
	return p
}

func (p *provider) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	p.mu.Lock()
	p.received = append(p.received, sent{method: r.Method, uri: r.RequestURI, path: r.URL.EscapedPath(), query: r.URL.Query(),
		body: string(body), header: r.Header.Clone()})
	p.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer token-1" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch strings.TrimPrefix(r.URL.Path, "/api") {
	case "/echo":
		_, _ = io.WriteString(w, r.URL.Query().Get("text"))
	case "/large":
		// Two bytes per character, so the cut can land inside one.
		_, _ = io.WriteString(w, strings.Repeat("é", core.MaxResultBytes))
	case "/broken":
		w.Header().Set("Content-Type", "image/png")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte{0x89, 'P', 'N', 'G', 0xff, 0xfe})
	case "/binary":
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte{0x89, 'P', 'N', 'G', 0xff, 0xfe})
	case "/huge":
		// 4 MiB, the cap AI-3669 decided, written out so a changed maxResponseBytes fails the test.
		_, _ = io.WriteString(w, strings.Repeat("a", 4<<20))
		_, _ = w.Write([]byte{0xff})
	case "/huge-runes":
		// The cut at 4 MiB falls inside the first é.
		_, _ = io.WriteString(w, strings.Repeat("a", 4<<20-1)+"éééé")
	case "/bad-byte-at-the-cap":
		_, _ = io.WriteString(w, strings.Repeat("a", 4<<20-1))
		_, _ = w.Write([]byte{0xff})
	case "/moved-301", "/moved-302", "/moved-303", "/moved-307", "/moved-308":
		code, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/api/moved-"))
		http.Redirect(w, r, "/api/target", code)
	case "/moved-reason":
		answerWithReason(w, "303 seen "+r.Header.Get("Authorization"))
	case "/moved-nowhere":
		w.WriteHeader(http.StatusFound)
	case "/target":
		_, _ = io.WriteString(w, "moved here")
	case "/loop":
		http.Redirect(w, r, "/api/loop", http.StatusTemporaryRedirect)
	case "/echo-credential":
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":"key `+r.Header.Get("Authorization")+` may not read this"}`)
	case "/echo-credential-ok/item-7":
		_, _ = io.WriteString(w, `{"self":"`+r.URL.RequestURI()+`","seen":"`+r.Header.Get("Authorization")+`"}`)
	case "/echo-at-the-cut":
		// The token starts 3 bytes before where core.CutResult cuts the tool error, "The API
		// answered 403 Forbidden: " and this body: a cut made before redacting would keep "tok".
		w.WriteHeader(http.StatusForbidden)
		kept := core.MaxResultBytes - len(core.TruncatedMarker)
		_, _ = io.WriteString(w, strings.Repeat("x", kept-3-len("The API answered 403 Forbidden: Bearer "))+
			r.Header.Get("Authorization")+strings.Repeat("y", 2*len(core.TruncatedMarker)))
	case "/no-content":
		w.WriteHeader(http.StatusNoContent)
	case "/reason-echo":
		answerWithReason(w, "202 seen "+r.Header.Get("Authorization"))
	case "/reason-own":
		answerWithReason(w, "403 Go away")
	case "/reason-unknown":
		answerWithReason(w, "499 Client Closed Request")
	case "/missing":
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "no such item")
	case "/multiple-choices":
		w.WriteHeader(http.StatusMultipleChoices)
	case "/last-2xx":
		w.WriteHeader(299)
		_, _ = io.WriteString(w, "ok")
	case "/cut":
		// Fewer bytes than the length it says: the server then closes the connection.
		w.Header().Set("Content-Length", "100")
		_, _ = io.WriteString(w, "only ten b")
	case "/refused":
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, "bad token")
	case "/error-large":
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, strings.Repeat("x", 2*core.MaxResultBytes))
	case "/slow":
		select {
		case <-time.After(slowFor):
			_, _ = io.WriteString(w, "late")
		case <-r.Context().Done():
			// The client left, so it gets no answer, as from an API that never answered: a handler
			// that returns would be answered 200 with no body, which the client may still read
			// (TestASlowAnswerIsNeverSentToAClientThatLeft).
			panic(http.ErrAbortHandler)
		}
	default:
		_, _ = io.WriteString(w, "ok")
	}
}

// answerWithReason answers with an empty body and the status line "HTTP/1.1 <status>", status
// being a code and a reason phrase of the server's own, which net/http's ResponseWriter never
// writes.
func answerWithReason(w http.ResponseWriter, status string) {
	conn, buffered, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	_, _ = io.WriteString(buffered, "HTTP/1.1 "+status+"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	_ = buffered.Flush()
}

// binding reaches the provider's /api under the alias crm, through core.Transports with the
// bearer scheme and a client that cuts a request at timeout, with tools as its http source's.
func (p *provider) binding(t *testing.T, timeout time.Duration, tools ...core.HTTPTool) core.ResolvedBinding {
	transports, err := core.NewTransports(core.TransportsConfig{Resolver: tokenResolver{}, Timeout: timeout, NewClient: loopback(p.Client())})
	if err != nil {
		t.Fatal(err)
	}
	return core.ResolvedBinding{
		Binding: core.Binding{Name: "crm"},
		Manifest: core.ResolvedManifest{
			ConnectorID: "custom_crm",
			Scheme:      bearer.Name,
			Endpoints:   map[string]string{Kind: p.URL + "/api"},
			Sources: []core.SourceRule{{Kind: Kind, Endpoint: Kind, HTTPTools: tools,
				Headers: map[string]string{"X-Api-Version": "2"}}},
		},
		HTTP: transports.Client(core.ConnectionRef{CustomerID: "app", ConnectionID: "crm-1"}, bearer.New()),
	}
}

// calls is how many requests reached the provider for the tool at /api/<tool>.
func (p *provider) calls(tool string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, request := range p.received {
		if request.path == "/api/"+tool {
			count++
		}
	}
	return count
}

// requests are the requests the provider received, in order.
func (p *provider) requests() []sent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sent{}, p.received...)
}

func (p *provider) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.received = nil
}

// loopback is egress.NewClient for a test: the same redirect policy, but the TLS server's
// transport, because egress refuses loopback, where httptest listens.
func loopback(server *http.Client) core.NewClientFunc {
	policy := egress.NewClient(0, nil).CheckRedirect
	return func(timeout time.Duration, wrap func(http.RoundTripper) http.RoundTripper) *http.Client {
		return &http.Client{Transport: wrap(server.Transport), Timeout: timeout, CheckRedirect: policy}
	}
}

// tokenResolver hands out the bearer token token-1 for every connection.
type tokenResolver struct{}

func (tokenResolver) Resolve(context.Context, core.ConnectionRef, core.CredentialRequest) (core.AccessCredential, error) {
	secret, err := json.Marshal(map[string]string{"token": "token-1"})
	if err != nil {
		return core.AccessCredential{}, err
	}
	credential := core.NewAccessCredential(bearer.Name, time.Time{}, secret)
	credential.Revision = 1
	return credential, nil
}

// Invalidate is told of a 401 nothing renews, as the store's resolver is, and the 401 is then
// the call's answer (core.Transports).
func (tokenResolver) Invalidate(context.Context, core.ConnectionRef, core.AccessCredential, core.Outcome) error {
	return nil
}

func (tokenResolver) Revoke(context.Context, core.ConnectionRef, core.SignalKind, time.Time) error {
	return nil
}
