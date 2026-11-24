package http

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/fakeprovider"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/apikey"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
)

// AcceptanceSuite is AC-6 and AC-9 of AI-3669 against the fake provider's HTTP API, with an
// api_key connection whose key the API takes.
type AcceptanceSuite struct {
	suite.Suite
	ctx      context.Context
	provider *fakeprovider.Server
	source   *Source
}

func TestAcceptanceSuite(t *testing.T) {
	suite.Run(t, new(AcceptanceSuite))
}

func (s *AcceptanceSuite) SetupTest() {
	s.ctx = context.Background()
	s.provider = fakeprovider.New(s.T())
	s.source = New()
}

// AC-6 of AI-3669: a read-only GET called with _method=DELETE and X-HTTP-Method-Override=DELETE
// is refused before sending, and the provider records no request; with valid arguments it records
// exactly one GET with no override header, the path value ../admin escaped as one segment.
func (s *AcceptanceSuite) TestAnOverrideIsRefusedBeforeSendingAndAValidCallIsOneGET() {
	tool := core.HTTPTool{Name: "get_item", Method: http.MethodGet, Path: "/items/{id}", ReadOnly: true,
		Parameters: map[string]core.HTTPParameter{"id": {Type: "string", Required: true}}}

	_, err := s.call(tool, `{"id":"1","_method":"DELETE","X-HTTP-Method-Override":"DELETE"}`)

	s.EqualError(err, "http: crm__get_item: an argument is not one of the tool's parameters, so nothing was sent")
	s.Empty(s.provider.APIRequests())

	result, err := s.call(tool, `{"id":"../admin"}`)

	s.Require().NoError(err)
	s.Equal("ok", llm.TextOf(result.Parts))
	requests := s.provider.APIRequests()
	s.Require().Len(requests, 1)
	s.Equal(http.MethodGet, requests[0].Method)
	s.Equal(fakeprovider.PathAPI+"/items/..%2Fadmin", requests[0].URI)
	for _, header := range []string{"X-HTTP-Method-Override", "X-Method-Override", "X-HTTP-Method"} {
		s.Empty(requests[0].Header.Values(header), header)
	}
	s.Empty(requests[0].Body)
}

// AC-9 of AI-3669: a 100 KiB answer reads as at most 32 KiB ending with the marker, and of a
// 5 MiB one only the first 4 MiB is read: past them the fake's bytes are not UTF-8, which would
// read as nonText.
func (s *AcceptanceSuite) TestAnAnswerIsReadUpTo4MiBAndReadAsAtMost32KiB() {
	tool := core.HTTPTool{Name: "get_bytes", Method: http.MethodGet, Path: "/bytes/{size}", ReadOnly: true,
		Parameters: map[string]core.HTTPParameter{"size": {Type: "integer", Required: true}, "then": {Type: "integer"}}}

	for _, arguments := range []string{`{"size":102400}`, `{"size":4194304,"then":1048576}`} {
		result, err := s.call(tool, arguments)

		s.Require().NoError(err, arguments)
		text := llm.TextOf(result.Parts)
		s.LessOrEqual(len(text), 32<<10, arguments)
		s.True(strings.HasPrefix(text, "aaaa"), arguments)
		s.True(strings.HasSuffix(text, core.TruncatedMarker), arguments)
	}
}

// S1.2 of AI-3674: an API answering 403 with the api_key and the Authorization it got does not
// put the key in the conversation.
func (s *AcceptanceSuite) TestA403EchoingTheKeyIsRedacted() {
	_, err := s.call(core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/echo-credential"}, `{}`)

	var reported *core.ToolError
	s.Require().ErrorAs(err, &reported)
	s.Equal(`The API answered 403 Forbidden: {"error":"key [redacted] and  may not read this"}`, reported.Message)
	s.NotContains(reported.Message, s.provider.APIKey)
}

// An API answering 200 with the api_key it got does not put the key in the conversation either;
// the rest of the answer stays as it came.
func (s *AcceptanceSuite) TestA200EchoingTheKeyIsRedacted() {
	tool := core.HTTPTool{Name: "get", Method: http.MethodGet, Path: "/echo-credential", ReadOnly: true,
		Parameters: map[string]core.HTTPParameter{"status": {Type: "string"}}}

	result, err := s.call(tool, `{"status":"200"}`)

	s.Require().NoError(err)
	s.Equal(`{"error":"key [redacted] and  may not read this"}`, llm.TextOf(result.Parts))
}

// The log line of a call (core.LoggedMessage, with the call's Exchange.Redact, as the session's
// dispatcher writes it) holds no argument value and no credential: the URL the client's error
// names is cut to its scheme and host, and the tool error is redacted.
func (s *AcceptanceSuite) TestALogLineHoldsNoArgumentAndNoCredential() {
	lookup := core.HTTPTool{Name: "lookup", Method: http.MethodGet, Path: "/people/{id}",
		Parameters: map[string]core.HTTPParameter{"id": {Type: "string", Required: true}, "email": {Type: "string"}}}
	transports, err := core.NewTransports(core.TransportsConfig{Resolver: keyResolver{key: s.provider.APIKey}, Timeout: requestTimeout})
	s.Require().NoError(err)
	unreached := s.binding(lookup)
	unreached.Manifest.Endpoints[Kind] = "https://127.0.0.1:8443/rest"
	unreached.HTTP = transports.Client(core.ConnectionRef{CustomerID: "app", ConnectionID: "crm-1"}, apikey.New())
	echoed := s.binding(core.HTTPTool{Name: "lookup", Method: http.MethodGet, Path: "/echo-credential",
		Parameters: map[string]core.HTTPParameter{"email": {Type: "string"}}})

	for name, binding := range map[string]core.ResolvedBinding{"egress refuses the host": unreached, "the API echoes the key": echoed} {
		arguments := `{"id":"u-3675","email":"alice@example.com"}`
		if name == "the API echoes the key" {
			arguments = `{"email":"alice@example.com"}`
		}
		set, err := s.source.Open(s.ctx, binding, s.grants(binding, "lookup"))
		s.Require().NoError(err)
		ctx, exchange := core.WithExchange(s.ctx)

		_, err = set.Call(ctx, llm.ToolCall{Name: "crm__lookup", Arguments: arguments})

		s.Require().Error(err, name)
		logged := core.LoggedMessage(err, exchange.Redact)
		for _, secret := range []string{"alice", "example.com", "u-3675", "/people", s.provider.APIKey} {
			s.NotContains(logged, secret, name)
		}
	}
}

// call opens tool, granted at its digest, on an api_key connection to the fake's API and calls it.
func (s *AcceptanceSuite) call(tool core.HTTPTool, arguments string) (core.Result, error) {
	binding := s.binding(tool)
	set, err := s.source.Open(s.ctx, binding, s.grants(binding, tool.Name))
	s.Require().NoError(err)
	s.Require().Len(set.Tools(), 1)
	return set.Call(s.ctx, llm.ToolCall{Name: "crm" + Separator + tool.Name, Arguments: arguments})
}

func (s *AcceptanceSuite) grants(binding core.ResolvedBinding, name string) []core.ToolGrant {
	specs, err := s.source.Discover(s.ctx, binding)
	s.Require().NoError(err)
	for _, spec := range specs {
		if spec.Name == name {
			return []core.ToolGrant{{Name: name, SchemaDigest: spec.SchemaDigest}}
		}
	}
	s.FailNow("no such tool", name)
	return nil
}

// binding reaches the fake's API under the alias crm, through core.Transports with the api_key
// scheme and the key the API takes.
func (s *AcceptanceSuite) binding(tools ...core.HTTPTool) core.ResolvedBinding {
	transports, err := core.NewTransports(core.TransportsConfig{Resolver: keyResolver{key: s.provider.APIKey},
		Timeout: requestTimeout, NewClient: loopback(s.provider.Client())})
	s.Require().NoError(err)
	return core.ResolvedBinding{
		Binding: core.Binding{Name: "crm"},
		Manifest: core.ResolvedManifest{ConnectorID: "custom_crm", Scheme: apikey.Name,
			Endpoints: map[string]string{Kind: s.provider.URL + fakeprovider.PathAPI},
			Sources:   []core.SourceRule{{Kind: Kind, Endpoint: Kind, HTTPTools: tools}}},
		HTTP: transports.Client(core.ConnectionRef{CustomerID: "app", ConnectionID: "crm-1"}, apikey.New()),
	}
}

// keyResolver hands out key, sent in fakeprovider.APIKeyHeader, for every connection.
type keyResolver struct {
	key string
}

func (r keyResolver) Resolve(context.Context, core.ConnectionRef, core.CredentialRequest) (core.AccessCredential, error) {
	secret, err := json.Marshal(map[string]string{"header": fakeprovider.APIKeyHeader, "key": r.key})
	if err != nil {
		return core.AccessCredential{}, err
	}
	credential := core.NewAccessCredential(apikey.Name, time.Time{}, secret)
	credential.Revision = 1
	return credential, nil
}

func (keyResolver) Invalidate(context.Context, core.ConnectionRef, core.AccessCredential, core.Outcome) error {
	return nil
}

func (keyResolver) Revoke(context.Context, core.ConnectionRef, core.SignalKind, time.Time) error {
	return nil
}
