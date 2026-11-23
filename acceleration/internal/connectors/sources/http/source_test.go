package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	tool := core.HTTPTool{Name: "get_forecast", Method: http.MethodGet, Path: "/v1/forecast/{city}", ReadOnly: true,
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
	tool := core.HTTPTool{Name: "echo", Method: http.MethodPost, Path: "/items/{id}",
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
		tool := core.HTTPTool{Name: "act", Method: test.method, Path: "/items/{id}",
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

// Only what the tool declares is sent: an override the model adds goes nowhere (AC-6 of AI-3669,
// whose refusal before sending is AI-3675's).
func (s *SourceSuite) TestAnArgumentTheToolDoesNotDeclareIsNotSent() {
	_, err := s.call(s.binding(echoTool()), contracttest.ToolEcho,
		`{"text":"hi","_method":"DELETE","X-HTTP-Method-Override":"DELETE"}`)

	s.Require().NoError(err)
	sent := s.only()
	s.Equal(http.MethodGet, sent.method)
	s.Equal(url.Values{"text": {"hi"}}, sent.query)
	s.Empty(sent.header.Get("X-HTTP-Method-Override"))
}

func (s *SourceSuite) TestNoArgumentsAtAllAreAnEmptyObject() {
	tool := core.HTTPTool{Name: "list", Method: http.MethodGet, Path: "/items"}

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
	} {
		tool := core.HTTPTool{Name: "get", Method: http.MethodGet, Path: path}

		_, err := s.call(s.binding(tool), "get", `{}`)

		var reported *core.ToolError
		s.Require().ErrorAs(err, &reported, path)
		s.Equal(want, reported.Message, path)
	}
	result, err := s.call(s.binding(core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/last-2xx"}), "get", `{}`)
	s.Require().NoError(err, "299 is the last 2xx")
	s.Equal("ok", llm.TextOf(result.Parts))
}

func (s *SourceSuite) TestAToolErrorOverTheCapIsCutWithTheMarker() {
	tool := core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/error-large"}

	_, err := s.call(s.binding(tool), "get", `{}`)

	var reported *core.ToolError
	s.Require().ErrorAs(err, &reported)
	s.LessOrEqual(len(reported.Message), core.MaxResultBytes)
	s.True(strings.HasSuffix(reported.Message, core.TruncatedMarker))
}

func (s *SourceSuite) TestABodyThatIsNotTextReadsAsNotText() {
	tool := core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/binary"}

	result, err := s.call(s.binding(tool), "get", `{}`)

	s.Require().NoError(err)
	s.Equal(nonText, llm.TextOf(result.Parts))
}

// The provider answers 4 MiB of text and then a byte that is not UTF-8: read past the cap, the
// body would be no text at all.
func (s *SourceSuite) TestABodyIsReadOnlyUpToTheCap() {
	tool := core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/huge"}

	result, err := s.call(s.binding(tool), "get", `{}`)

	s.Require().NoError(err)
	text := llm.TextOf(result.Parts)
	s.True(strings.HasPrefix(text, "aaaa"))
	s.True(strings.HasSuffix(text, core.TruncatedMarker))
}

// R1.2: valid text whose cut at the cap falls inside a rune is still text, cut before that rune.
func (s *SourceSuite) TestTextTheCapCutsInsideARuneIsStillText() {
	tool := core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/huge-runes"}

	result, err := s.call(s.binding(tool), "get", `{}`)

	s.Require().NoError(err)
	text := llm.TextOf(result.Parts)
	s.True(strings.HasPrefix(text, "aaaa"), "not %q", nonText)
	s.True(strings.HasSuffix(text, core.TruncatedMarker))
}

// R1.3: the last byte under the 4 MiB cap is read: a body whose last byte there is not UTF-8 is
// not text, under a cap even one byte lower too.
func (s *SourceSuite) TestTheLastByteUnderTheCapIsRead() {
	tool := core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/bad-byte-at-the-cap"}

	result, err := s.call(s.binding(tool), "get", `{}`)

	s.Require().NoError(err)
	s.Equal(nonText, llm.TextOf(result.Parts))
}

// R1.3: a 4xx or a 5xx whose body is not text names its status and says so, never the bytes.
func (s *SourceSuite) TestAToolErrorWithABodyThatIsNotTextSaysItIsNotText() {
	_, err := s.call(s.binding(core.HTTPTool{Name: "broken", Method: http.MethodGet, Path: "/broken"}), "broken", `{}`)

	var reported *core.ToolError
	s.Require().ErrorAs(err, &reported)
	s.Equal("The API answered 500 Internal Server Error: "+nonText, reported.Message)
}

// AI-3674, from the AI-3673 review (R1.3): revision 2 turns POST x into a read-only GET. A grant
// made for one revision's x never opens the other's, so a connection still pinned to revision 1
// never runs the POST under a grant made for the read-only GET, nor the reverse.
func (s *SourceSuite) TestAGrantForAnotherRevisionOfAToolIsNeverOpened() {
	post := core.HTTPTool{Name: "x", Method: http.MethodPost, Path: "/x"}
	get := core.HTTPTool{Name: "x", Method: http.MethodGet, Path: "/x", ReadOnly: true}
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
	list := core.HTTPTool{Name: "list", Method: http.MethodGet, Path: "/items",
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
	tool := core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/slow"}
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
	tool := core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/cut"}

	_, err := s.call(s.binding(tool), "get", `{}`)

	s.ErrorIs(err, io.ErrUnexpectedEOF)
	var reported *core.ToolError
	s.False(errors.As(err, &reported), "the API never finished its answer, so it reported nothing")
}

func (s *SourceSuite) TestTheBindingsTimeoutBoundsACall() {
	tool := core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/slow"}
	binding := s.binding(tool)
	binding.Binding.Timeout = shortTimeout

	_, err := s.call(binding, "get", `{}`)

	s.ErrorIs(err, context.DeadlineExceeded)
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
		}
	default:
		_, _ = io.WriteString(w, "ok")
	}
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
