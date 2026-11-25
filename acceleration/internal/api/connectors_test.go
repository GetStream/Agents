//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/providers"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/none"
	httpsource "github.com/GetStream/Vision-Agents/acceleration/internal/connectors/sources/http"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ConnectorsSuite is about the connector catalog: the built-ins, an app's own custom MCP
// connectors, and what of a manifest a caller is shown.
type ConnectorsSuite struct {
	RouterSuite
}

func TestConnectorsSuite(t *testing.T) {
	runSuite(t, new(ConnectorsSuite))
}

// SetupSuite registers the scheme the built-ins name, none and the http source, for a
// connection to an http connector, and seeds the built-ins as a router start does. Seeding is
// idempotent, so suites running beside this one see the same rows.
func (s *ConnectorsSuite) SetupSuite() {
	s.connectors = core.Registry{Schemes: map[string]core.Scheme{
		"oauth2_code":               namedScheme("oauth2_code"),
		"oauth2_client_credentials": namedScheme("oauth2_client_credentials"),
		"api_key":                   namedScheme("api_key"),
		none.Name:                   none.New(),
	}, ToolSources: map[string]core.ToolSource{httpsource.Kind: httpsource.New()}}
	s.RouterSuite.SetupSuite()
	s.Require().NoError(s.store.ConnectorDefinitions.Seed(context.Background(), providers.FS))
}

// SetupTest gives every test an app of its own, so a list holds the built-ins and only what
// the test made.
func (s *ConnectorsSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *ConnectorsSuite) TestABuiltInShowsItsSchemesInputsScopesAndClientRegistrations() {
	slack := s.get("slack")

	s.False(slack.Custom)
	s.Equal("Slack", slack.Name)
	s.GreaterOrEqual(slack.Revision, 1)
	s.Equal([]string{"oauth2_code"}, slack.Schemes)
	s.Empty(slack.Inputs, "Slack is connected with nothing but a consent")
	s.Contains(slack.Scopes, "chat:write")
	s.Equal([]ConnectorClientRegistrationMethod{"operator"}, slack.Client.Registration)
	s.Equal(ConnectorClientAuthMethod("client_secret_post"), slack.Client.AuthMethod)
}

// TestOnlyAConnectorThatReadsMessagesIsAChannel (AI-1053): the manifest's channel block with
// messages makes slack_bot one, in a read and in the list; Slack, whose block reads only
// signals, and a custom MCP connector are not.
func (s *ConnectorsSuite) TestOnlyAConnectorThatReadsMessagesIsAChannel() {
	s.True(s.get("slack_bot").Channel)
	s.False(s.get("slack").Channel)
	listed := map[string]bool{}
	for _, item := range s.list("").Items {
		listed[item.ID] = item.Channel
	}
	s.True(listed["slack_bot"])
	s.False(listed["slack"])
}

// TestGitHubShowsWhatASandboxGetsForItsAPI: github's sandbox block (revision 9, kept by 10 and
// 11), in a read and in the list, is what code in a sandbox reads (env) and where its token may go
// (hosts). Revision 11's LFS download host gets no token, so it is not shown.
func (s *ConnectorsSuite) TestGitHubShowsWhatASandboxGetsForItsAPI() {
	want := &ConnectorSandbox{Env: "GITHUB_TOKEN", Hosts: []string{"api.github.com", "uploads.github.com", "github.com"}}
	github := s.get("github")
	s.Equal(11, github.Revision)
	s.Equal(want, github.Sandbox)
	for _, item := range s.list("").Items {
		if item.ID == "github" {
			s.Equal(want, item.Sandbox)
		}
	}
}

// TestNoOtherConnectorShowsASandbox is the control: on the base, b7187f62, no connector in the
// list, built-in or custom, was answered with a sandbox key (pr-w13-man/author-1.md, «Control
// probe»). On 8e132bd37 only github.yaml has a sandbox block; google_drive.yaml adds the second
// and linear_app.yaml (revision 2, AI-2082) the third, bigquery.yaml (revision 4, AI-2114) the
// fourth, each asserted exactly below.
func (s *ConnectorsSuite) TestNoOtherConnectorShowsASandbox() {
	s.create(s.customConnector(s.customID()))
	status, raw := s.serverClient.call(http.MethodGet, "/v1/agents/connectors?limit=200", nil)
	s.Require().Equal(http.StatusOK, status)
	var page struct {
		Items []map[string]any `json:"items"`
	}
	s.Require().NoError(json.Unmarshal(raw, &page))
	s.Require().Greater(len(page.Items), 20)
	seen := map[string]bool{}
	for _, item := range page.Items {
		id, _ := item["id"].(string)
		seen[id] = true
		if item["id"] == "google_drive" {
			s.Equal(map[string]any{"env": "GOOGLE_DRIVE_TOKEN", "hosts": []any{"www.googleapis.com"}}, item["sandbox"])
		} else if item["id"] == "linear_app" {
			s.Equal(map[string]any{"env": "LINEAR_ACCESS_TOKEN", "hosts": []any{"api.linear.app", "uploads.linear.app"}}, item["sandbox"])
		} else if item["id"] == "bigquery" {
			s.Equal(map[string]any{"env": "BIGQUERY_TOKEN", "hosts": []any{"bigquery.googleapis.com"}}, item["sandbox"])
		} else if item["id"] == "github" {
			s.Contains(item, "sandbox", item["id"])
		} else {
			s.NotContains(item, "sandbox", item["id"])
		}
	}
	// A connector missing from the list would skip its branch above and pass.
	for _, id := range []string{"github", "google_drive", "linear_app", "bigquery"} {
		s.True(seen[id], "%s is in the list", id)
	}
}

// TestAMovedPluginShowsItsSetupSteps: the steps the removed plugin catalog showed before a
// client was pasted in come with its connector (T58), and a connector whose manifest has none shows none.
func (s *ConnectorsSuite) TestAMovedPluginShowsItsSetupSteps() {
	calendar := s.get("google_calendar")
	calcom := s.get("calcom")

	s.Require().NotNil(calendar.Setup)
	s.Equal("https://console.cloud.google.com/auth/clients", calendar.Setup.URL)
	s.Len(calendar.Setup.Steps, 5)
	s.Equal("Enable the API", calendar.Setup.Steps[0].Title)
	s.Equal([]ConnectorClientRegistrationMethod{"operator", "customer"}, calendar.Client.Registration)
	s.Nil(calcom.Setup)
}

// nessy is internal (nessy.yaml): an app Stream marks internal lists and reads it with the
// client and setup its staff need.
func (s *ConnectorsSuite) TestAnAppMarkedInternalListsAndReadsNessy() {
	s.useApp(s.data.createAppAdmitting(store.AppSettings{InternalConnectors: new(true)}))

	s.Contains(connectorIDs(s.list("").Items), "nessy")
	nessy := s.get("nessy")
	s.Equal([]string{"oauth2_code"}, nessy.Schemes)
	s.Equal([]string{"mcp", "openid"}, nessy.Scopes)
	s.Equal([]ConnectorClientRegistrationMethod{"customer"}, nessy.Client.Registration)
	s.Equal(ConnectorClientAuthMethod("client_secret_basic"), nessy.Client.AuthMethod)
	s.Require().NotNil(nessy.Setup)
	s.Equal("https://nessy.getstream.io/admin", nessy.Setup.URL)
	s.Len(nessy.Setup.Steps, 6)
}

// An app not marked internal, whether its settings say nothing or say false, lists every
// built-in an internal app does but nessy, and reading nessy is a 404.
func (s *ConnectorsSuite) TestAnAppNotMarkedInternalNeitherListsNorReadsNessy() {
	s.useApp(s.data.createAppAdmitting(store.AppSettings{InternalConnectors: new(true)}))
	everything := connectorIDs(s.list("").Items)
	s.Require().Contains(everything, "nessy")
	public := slices.DeleteFunc(slices.Clone(everything), func(id string) bool { return id == "nessy" })

	for _, settings := range []store.AppSettings{{}, {InternalConnectors: new(false)}} {
		s.useApp(s.data.createAppAdmitting(settings))
		s.Equal(public, connectorIDs(s.list("").Items))
		status, _ := s.serverClient.failure(http.MethodGet, "/v1/agents/connectors/nessy", nil)
		s.Equal(http.StatusNotFound, status)
	}
}

func (s *ConnectorsSuite) TestWhatTheRouterReadsToConnectIsNeverShown() {
	// slack.yaml has endpoints, capture and identity rules, a refresh and a rate limit,
	// sources, and client.env naming the operator's variables.
	status, raw := s.serverClient.call(http.MethodGet, "/v1/agents/connectors/slack", nil)
	s.Require().Equal(http.StatusOK, status)

	var shown map[string]any
	s.Require().NoError(json.Unmarshal(raw, &shown))
	for _, withheld := range []string{
		"endpoints", "vars", "authorize_params", "token_params", "identity", "capture",
		"refresh", "rate_limit", "sources", "hooks", "manifest",
	} {
		s.NotContains(shown, withheld)
	}
	// The manifest's channel block stays withheld; AI-1053 shows only whether there is one.
	s.IsType(false, shown["channel"])
	client, ok := shown["client"].(map[string]any)
	s.Require().True(ok, string(raw))
	s.NotContains(client, "env")
	for _, value := range []string{"SLACK", "mcp.slack.com", "slack.com/api", "$.team.id"} {
		s.NotContains(string(raw), value)
	}
}

func (s *ConnectorsSuite) TestACustomConnectorsEndpointIsNeverShown() {
	created := s.create(s.customConnector(s.customID()))

	for _, path := range []string{"/v1/agents/connectors/" + created.ID, "/v1/agents/connectors"} {
		status, raw := s.serverClient.call(http.MethodGet, path, nil)
		s.Require().Equal(http.StatusOK, status)
		s.NotContains(string(raw), "8.8.8.8", "an MCP URL can carry a secret in its path")
		s.NotContains(string(raw), `"endpoint"`)
	}
}

// TestABuiltInReadsAsBeforeOnARouterWithoutAPublicURL is the control for AI-1046 and AI-1047:
// the keys a built-in is answered with are the ones accelerate at 26051062 answered with,
// probed there with this suite (pr-w7/author-1.md), plus channel (AI-1053). Without ROUTER_PUBLIC_URL there is no
// redirect URI.
func (s *ConnectorsSuite) TestABuiltInReadsAsBeforeOnARouterWithoutAPublicURL() {
	for _, id := range []string{"slack", "linear", "telnyx"} {
		status, raw := s.serverClient.call(http.MethodGet, "/v1/agents/connectors/"+id, nil)
		s.Require().Equal(http.StatusOK, status)
		var shown map[string]any
		s.Require().NoError(json.Unmarshal(raw, &shown))
		s.Equal([]string{"category", "channel", "client", "created_at", "custom", "description", "duration", "id", "inputs",
			"name", "revision", "schemes", "scopes"}, slices.Sorted(maps.Keys(shown)), id)
	}
}

func (s *ConnectorsSuite) TestACustomMCPConnectorIsStoredAndReadBack() {
	id := s.customID()
	sent := s.customConnector(id)
	sent["category"] = "  CRM  "
	sent["scopes"] = []string{"crm.read", "crm.write"}
	sent["client"] = map[string]any{"registration": []string{"dcr", "customer"}, "auth_method": "client_secret_basic"}

	created := s.create(sent)
	s.Equal(id, created.ID)
	s.Equal(1, created.Revision)
	s.True(created.Custom)
	s.Equal("CRM", created.Category, "trimmed")
	s.Equal([]string{"oauth2_code"}, created.Schemes)
	s.Equal([]string{"crm.read", "crm.write"}, created.Scopes)
	s.Equal([]ConnectorClientRegistrationMethod{"dcr", "customer"}, created.Client.Registration)
	s.Equal(ConnectorClientAuthMethod("client_secret_basic"), created.Client.AuthMethod)

	s.Equal(created, s.get(id), "the answer to the create is the stored row")
}

func (s *ConnectorsSuite) TestAnIdThatWouldShadowABuiltInIsRefused() {
	before := s.get("slack")

	status, raw := s.serverClient.call(http.MethodPost, "/v1/agents/connectors", s.customConnector("slack"))

	s.Equal(http.StatusBadRequest, status)
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(raw, &answered), "the error envelope: %s", raw)
	s.Contains(answered.Error.Message, "custom_")
	s.Equal(ErrorTypeInvalidRequest, answered.Error.Type)
	s.Equal(before, s.get("slack"), "the built-in is untouched")
}

func (s *ConnectorsSuite) TestAnEndpointOnAPrivateNetworkIsRefused() {
	for _, endpoint := range []string{
		"https://127.0.0.1/mcp",       // loopback
		"https://10.0.0.7/mcp",        // private-use, RFC 1918
		"https://169.254.169.254/mcp", // link-local, where cloud metadata servers answer
		"https://[::1]/mcp",           // IPv6 loopback
	} {
		sent := s.customConnector(s.customID())
		sent["endpoint"] = endpoint

		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)
		s.Equal(http.StatusBadRequest, status, endpoint)
		s.Contains(failure, "endpoint", endpoint)
	}
}

func (s *ConnectorsSuite) TestAnEndpointThatDoesNotResolveIsRefusedLikeAPrivateOne() {
	answers := map[string]string{}
	for _, endpoint := range []string{
		"https://router-probe.invalid/mcp", // .invalid never resolves, RFC 6761 section 6.4
		"https://10.0.0.7/mcp",             // private-use, RFC 1918
	} {
		sent := s.customConnector(s.customID())
		sent["endpoint"] = endpoint

		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)
		s.Equal(http.StatusBadRequest, status, endpoint)
		answers[endpoint] = failure
	}
	s.Equal(answers["https://10.0.0.7/mcp"], answers["https://router-probe.invalid/mcp"],
		"a caller cannot tell which names the router's resolver knows")
}

func (s *ConnectorsSuite) TestAnEndpointThatIsNotPlainHTTPSIsRefused() {
	for _, endpoint := range []string{
		"http://8.8.8.8/mcp",
		"https://8.8.8.8/mcp?token=secret",
		"https://user:secret@8.8.8.8/mcp",
		"https://8.8.8.8/{tenant}/mcp",
	} {
		sent := s.customConnector(s.customID())
		sent["endpoint"] = endpoint

		status, _ := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)
		s.Equal(http.StatusBadRequest, status, endpoint)
	}
}

func (s *ConnectorsSuite) TestASchemeThisDeploymentDoesNotHaveIsRefused() {
	sent := s.customConnector(s.customID())
	sent["schemes"] = []string{"aws_sigv4"}

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "aws_sigv4")
}

func (s *ConnectorsSuite) TestAnOperatorClientIsRefusedForACustomConnector() {
	sent := s.customConnector(s.customID())
	sent["client"] = map[string]any{"registration": []string{"operator"}}

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "operator")
}

func (s *ConnectorsSuite) TestAnOperatorClientListedAfterAnotherIsRefusedForACustomConnector() {
	sent := s.customConnector(s.customID())
	sent["client"] = map[string]any{"registration": []string{"customer", "operator"}}

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "operator")
}

func (s *ConnectorsSuite) TestAnOAuthConnectorWithoutClientRegistrationsIsRefused() {
	sent := s.customConnector(s.customID())
	delete(sent, "client")

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "client.registration")
}

func (s *ConnectorsSuite) TestClientCredentialsIsRefusedForACustomConnectorAndAnotherSchemeIsNot() {
	refused := s.customConnector(s.customID())
	refused["schemes"] = []string{"oauth2_client_credentials"}
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", refused)
	s.Equal(http.StatusBadRequest, status)
	s.Equal("oauth2_client_credentials is not available for a custom connector: it needs a token endpoint a custom connector cannot set", failure)

	beside := s.customConnector(s.customID())
	beside["schemes"] = []string{"api_key", "oauth2_client_credentials"}
	status, _ = s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", beside)
	s.Equal(http.StatusBadRequest, status, "listed beside another scheme")

	first := s.customConnector(s.customID())
	first["schemes"] = []string{"oauth2_client_credentials", "api_key"}
	status, _ = s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", first)
	s.Equal(http.StatusBadRequest, status, "listed first, before another scheme")

	allowed := s.customConnector(s.customID())
	allowed["schemes"] = []string{"api_key"}
	delete(allowed, "client")
	s.Equal([]string{"api_key"}, s.create(allowed).Schemes)
}

func (s *ConnectorsSuite) TestARepeatedScopeOrClientRegistrationIsRefused() {
	for name, change := range map[string]func(map[string]any){
		"scope":  func(sent map[string]any) { sent["scopes"] = []string{"crm.read", "crm.read"} },
		"source": func(sent map[string]any) { sent["client"] = map[string]any{"registration": []string{"dcr", "dcr"}} },
	} {
		sent := s.customConnector(s.customID())
		change(sent)

		status, _ := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)
		s.Equal(http.StatusBadRequest, status, name)
	}
}

func (s *ConnectorsSuite) TestAFieldTheRequestDoesNotHaveIsRefused() {
	for field, value := range map[string]any{
		"hooks":     map[string]string{"before_complete": "shopify.callback_hmac"},
		"endpoints": map[string]string{"token": "https://8.8.8.8/token"},
	} {
		sent := s.customConnector(s.customID())
		sent[field] = value

		status, _ := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)
		s.Equal(http.StatusBadRequest, status, field)
	}
	sent := s.customConnector(s.customID())
	sent["client"] = map[string]any{"registration": []string{"dcr"}, "env": "SLACK"}
	status, _ := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)
	s.Equal(http.StatusBadRequest, status, "client.env")
}

func (s *ConnectorsSuite) TestAScopeThatIsNotAnRFC6749ScopeTokenIsRefused() {
	sent := s.customConnector(s.customID())
	sent["scopes"] = []string{"read write"}

	status, _ := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)

	s.Equal(http.StatusBadRequest, status)
}

func (s *ConnectorsSuite) TestSendingTheSameConnectorAgainChangesNothingAndAChangeIsTheNextRevision() {
	id := s.customID()
	s.Equal(1, s.create(s.customConnector(id)).Revision)
	s.Equal(1, s.create(s.customConnector(id)).Revision, "the same definition again")

	changed := s.customConnector(id)
	changed["name"] = "Renamed"
	s.Equal(2, s.create(changed).Revision)
	s.Equal("Renamed", s.get(id).Name)
}

func (s *ConnectorsSuite) TestAnotherAppsCustomConnectorIsNeitherListedNorRead() {
	id := s.customID()
	s.create(s.customConnector(id))
	stranger := s.data.backendOfAnotherApp()

	var theirs ConnectorPage
	s.Require().Equal(http.StatusOK, stranger.do(http.MethodGet, "/v1/agents/connectors?limit=200", nil, &theirs))
	s.NotContains(connectorIDs(theirs.Items), id)
	s.Contains(connectorIDs(theirs.Items), "slack", "the built-ins are everybody's")
	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodGet, "/v1/agents/connectors/"+id, nil, nil)
	})
}

func (s *ConnectorsSuite) TestAnotherAppMayUseTheSameCustomIdWithoutTouchingThisOne() {
	id := s.customID()
	s.create(s.customConnector(id))

	theirs := s.customConnector(id)
	theirs["name"] = "Theirs"
	var created Connector
	s.Require().Equal(http.StatusOK,
		s.data.backendOfAnotherApp().do(http.MethodPost, "/v1/agents/connectors", theirs, &created))
	s.Equal(1, created.Revision)

	s.Equal("Our CRM", s.get(id).Name)
	s.Equal(1, s.get(id).Revision)
}

func (s *ConnectorsSuite) TestTheListIsTheBuiltInsThenTheAppsOwnEachById() {
	second, first := "custom_b"+s.suffix(), "custom_a"+s.suffix()
	s.create(s.customConnector(second))
	s.create(s.customConnector(first))

	listed := s.list("")
	s.False(listed.HasMore)
	s.Nil(listed.NextCursor)
	custom := slices.IndexFunc(listed.Items, func(d Connector) bool { return d.Custom })
	s.Require().Positive(custom, "the built-ins come first")
	builtIns := connectorIDs(listed.Items[:custom])
	s.True(slices.IsSorted(builtIns), "%v", builtIns)
	s.Contains(builtIns, "linear")
	s.Contains(builtIns, "slack")
	s.Equal([]string{first, second}, connectorIDs(listed.Items[custom:]))
}

func (s *ConnectorsSuite) TestPagingWalksTheWholeListWithoutRepeatingOrSkipping() {
	// An app of its own: the fixture's app is shared, and a connector another suite made there
	// between the whole list and the walk showed up as one the walk did not expect.
	s.useApp(s.data.createApp())
	for range 3 {
		s.create(s.customConnector(s.customID()))
	}
	whole := connectorIDs(s.list("").Items)

	var walked []string
	cursor := ""
	for pages := 0; ; pages++ {
		s.Require().Less(pages, len(whole), "paging does not end")
		var listed ConnectorPage
		s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
			"/v1/agents/connectors?limit=2&cursor="+url.QueryEscape(cursor), nil, &listed))
		s.LessOrEqual(len(listed.Items), 2)
		walked = append(walked, connectorIDs(listed.Items)...)
		if !listed.HasMore {
			s.Nil(listed.NextCursor)
			break
		}
		s.Require().NotNil(listed.NextCursor)
		cursor = *listed.NextCursor
	}
	s.Equal(whole, walked)
}

func (s *ConnectorsSuite) TestACursorThisListDidNotHandOutIsRefused() {
	status, _ := s.serverClient.failure(http.MethodGet, "/v1/agents/connectors?cursor=not-a-cursor", nil)

	s.Equal(http.StatusBadRequest, status)
}

func (s *ConnectorsSuite) TestTheSearchMatchesTheIdNameCategoryOrDescriptionIgnoringCase() {
	id := s.customID()
	sent := s.customConnector(id)
	sent["category"] = "Ticketing"
	sent["description"] = "Reads the helpdesk queue."
	s.create(sent)

	for _, q := range []string{id, "OUR crm", "ticketing", "HELPDESK"} {
		s.Equal([]string{id}, connectorIDs(s.list(q).Items), q)
	}
	s.Contains(connectorIDs(s.list("slack").Items), "slack", "a built-in is searched too")
	s.Empty(s.list("CRM Ticketing").Items, "the end of the name and the start of the category are two fields")
}

func (s *ConnectorsSuite) TestTheSearchReadsTheNewestRevisionOnly() {
	id := s.customID()
	s.create(s.customConnector(id))
	renamed := s.customConnector(id)
	renamed["name"] = "Ledger"
	s.create(renamed)

	s.Empty(s.list("Our CRM").Items, "revision 1 was called that, and is not the connector any more")
	s.Equal([]string{id}, connectorIDs(s.list("ledger").Items))
}

func (s *ConnectorsSuite) TestASearchForAWildcardMatchesOnlyItself() {
	s.create(s.customConnector(s.customID()))

	s.Empty(s.list("%").Items, "nothing has a percent sign in it")
	s.Empty(s.list("Our_CRM").Items, "as a wildcard, _ would match the space in Our CRM")
}

func (s *ConnectorsSuite) TestAnUnknownConnectorIsNotFound() {
	status, _ := s.serverClient.failure(http.MethodGet, "/v1/agents/connectors/custom_nobody", nil)

	s.Equal(http.StatusNotFound, status)
}

func (s *ConnectorsSuite) TestOnlyTheAppsBackendMayListConnectors() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		return as.do(http.MethodGet, "/v1/agents/connectors", nil, nil)
	})
}

func (s *ConnectorsSuite) TestOnlyTheAppsBackendMayReadAConnector() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		return as.do(http.MethodGet, "/v1/agents/connectors/slack", nil, nil)
	})
}

func (s *ConnectorsSuite) TestOnlyTheAppsBackendMayAddAConnector() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		return as.do(http.MethodPost, "/v1/agents/connectors", s.customConnector(s.customID()), nil)
	})
}

func (s *ConnectorsSuite) TestOnlyTheAppsBackendMayDeleteAConnector() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		return as.do(http.MethodDelete, "/v1/agents/connectors/"+s.create(s.customConnector(s.customID())).ID, nil, nil)
	})
}

func (s *ConnectorsSuite) TestAnUnknownOrBuiltInConnectorIsNotFoundToDelete() {
	unknown, missing := s.serverClient.failure(http.MethodDelete, "/v1/agents/connectors/custom_nobody", nil)
	builtin, answered := s.serverClient.failure(http.MethodDelete, "/v1/agents/connectors/slack?force=true", nil)

	s.Equal(http.StatusNotFound, unknown)
	s.Equal(http.StatusNotFound, builtin)
	s.Equal(missing, answered, "one answer for both")
	s.Equal("Slack", s.get("slack").Name, "the built-in is untouched")
}

func (s *ConnectorsSuite) TestAnotherAppCannotDeleteTheAppsConnector() {
	id := s.create(s.customConnector(s.customID())).ID

	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodDelete, "/v1/agents/connectors/"+id+"?force=true", nil, nil)
	})
	s.Equal(id, s.get(id).ID)
}

func (s *ConnectorsSuite) TestAnUnusedConnectorIsDeletedWithItsOAuthClientAndMayBeCreatedAgain() {
	sent := s.customConnector(s.customID())
	sent["client"] = map[string]any{"registration": []string{"customer"}}
	id := s.create(sent).ID
	sent["name"] = "Our CRM, renamed"
	s.Require().Equal(2, s.create(sent).Revision)
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPut, "/v1/agents/connectors/"+id+"/oauth-client",
		map[string]any{"client_id": "crm-client", "client_secret": "crm-secret"}, nil))

	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/agents/connectors/"+id, nil, nil))

	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+id, nil, nil))
	s.NotContains(connectorIDs(s.list("").Items), id)
	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+id+"/oauth-client", nil, nil),
		"the client and its sealed secret went with it")
	s.Equal(1, s.create(sent).Revision, "created again, it starts over")
}

func (s *ConnectorsSuite) TestAConnectorAConnectionUsesIsRefusedNamingTheConnection() {
	id := s.create(s.customConnector(s.customID())).ID
	var connection Connection
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/connections", appOwned(id), &connection))

	status, raw := s.serverClient.call(http.MethodDelete, "/v1/agents/connectors/"+id, nil)

	s.Equal(http.StatusConflict, status)
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(raw, &answered), "the error envelope: %s", raw)
	s.Equal(ErrorTypeConflict, answered.Error.Type)
	s.Equal("conflict", answered.Error.Code)
	s.Equal(id+" is used by connections "+connection.ID+": delete or unbind them first, or delete with force=true",
		answered.Error.Message)
	s.Equal(id, s.get(id).ID)
	s.Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/connections/"+connection.ID, nil, nil))
}

func (s *ConnectorsSuite) TestAConnectorAConfigBindsIsRefusedNamingTheBinding() {
	id := s.create(s.customConnector(s.customID())).ID
	binding := sessionSlack("crm")
	binding["connector_id"] = id
	var config AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		map[string]any{"name": "support-" + s.suffix(), "connectors": []map[string]any{binding}}, &config))

	status, failure := s.serverClient.failure(http.MethodDelete, "/v1/agents/connectors/"+id, nil)

	s.Equal(http.StatusConflict, status)
	s.Equal(id+` is used by agent config bindings "`+config.Name+`" as crm: delete or unbind them first, or delete with force=true`, failure)
}

func (s *ConnectorsSuite) TestARefusalNamesTenUsersOfEachKindAndCountsTheRest() {
	id := s.create(s.customConnector(s.customID())).ID
	var ids []string
	for range usesNamed + 2 {
		var connection Connection
		s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/connections", appOwned(id), &connection))
		ids = append(ids, connection.ID)
	}
	var bound []string
	for i := range usesNamed + 2 {
		binding := sessionSlack(fmt.Sprintf("crm%02d", i))
		binding["connector_id"] = id
		var config AgentConfig
		s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
			map[string]any{"name": fmt.Sprintf("support-%02d-%s", i, s.suffix()), "connectors": []map[string]any{binding}}, &config))
		bound = append(bound, fmt.Sprintf("%q as crm%02d", config.Name, i))
	}

	status, failure := s.serverClient.failure(http.MethodDelete, "/v1/agents/connectors/"+id, nil)

	s.Equal(http.StatusConflict, status)
	s.Equal(id+" is used by connections "+strings.Join(ids[:usesNamed], ", ")+
		" and 2 more and by agent config bindings "+strings.Join(bound[:usesNamed], ", ")+
		" and 2 more: delete or unbind them first, or delete with force=true", failure)
}

func (s *ConnectorsSuite) TestAForcedDeleteDeletesTheConnectionsAndLeavesTheBindings() {
	id := s.create(s.customConnector(s.customID())).ID
	var connection Connection
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/connections", appOwned(id), &connection))
	binding := sessionSlack("crm")
	binding["connector_id"] = id
	var config AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		map[string]any{"name": "support-" + s.suffix(), "connectors": []map[string]any{binding}}, &config))

	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/agents/connectors/"+id+"?force=true", nil, nil))

	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+id, nil, nil))
	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connections/"+connection.ID, nil, nil))
	var read AgentConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/configs/"+config.Id, nil, &read))
	s.Require().Len(value(read.Connectors), 1, "left in place, as a forced connection delete leaves its binding")
	s.Equal(id, value(read.Connectors)[0].ConnectorId)
}

// TestAConnectionToAConnectorDeletedMeanwhileIsRefusedAsUnknown: the create read the connector
// before the delete committed and stores the connection after, so the store finds it gone.
func (s *ConnectorsSuite) TestAConnectionToAConnectorDeletedMeanwhileIsRefusedAsUnknown() {
	id := s.create(s.customConnector(s.customID())).ID

	status, failure := s.duringADeleteOf(id, func() (int, string) {
		return s.serverClient.failure(http.MethodPost, "/v1/agents/connections", appOwned(id))
	})

	s.Equal(http.StatusBadRequest, status)
	s.Equal(`no such connector: "`+id+`"`, failure)
}

func (s *ConnectorsSuite) TestAnOAuthClientForAConnectorDeletedMeanwhileIsNotFound() {
	sent := s.customConnector(s.customID())
	sent["client"] = map[string]any{"registration": []string{"customer"}}
	id := s.create(sent).ID

	status, failure := s.duringADeleteOf(id, func() (int, string) {
		return s.serverClient.failure(http.MethodPut, "/v1/agents/connectors/"+id+"/oauth-client",
			map[string]any{"client_id": "crm-client", "client_secret": "crm-secret"})
	})

	s.Equal(http.StatusNotFound, status)
	s.Equal("no such connector", failure)
}

func (s *ConnectorsSuite) TestABindingToAConnectorDeletedMeanwhileIsRefused() {
	id := s.create(s.customConnector(s.customID())).ID
	binding := sessionSlack("crm")
	binding["connector_id"] = id

	status, failure := s.duringADeleteOf(id, func() (int, string) {
		return s.serverClient.failure(http.MethodPost, "/v1/agents/configs",
			map[string]any{"name": "support-" + s.suffix(), "connectors": []map[string]any{binding}})
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, id)
}

// duringADeleteOf makes call while a delete of the app's connector id holds its lock on the
// definition: a SHARE lock on connector_oauth_clients holds the delete at its DELETE there,
// which comes after that lock (store.DeleteConnectorDefinition). The call is let go once it
// waits for the definition, and it answers after the delete committed. Suites beside this one
// may wait on the table meanwhile; the waits counted are the delete's and the call's alone.
func (s *ConnectorsSuite) duringADeleteOf(id string, call func() (int, string)) (int, string) {
	held, err := s.store.GlobalDB().BeginTx(context.Background(), nil)
	s.Require().NoError(err)
	defer held.Rollback() //nolint:errcheck // after the commit below there is nothing to roll back
	_, err = held.ExecContext(context.Background(), "LOCK TABLE connector_oauth_clients IN SHARE MODE")
	s.Require().NoError(err)
	deleted := make(chan int, 1)
	go func() { deleted <- s.serverClient.do(http.MethodDelete, "/v1/agents/connectors/"+id, nil, nil) }()
	// The two seconds and the ten milliseconds are assertTheWaitEnded's (internal/store/credentials_test.go).
	s.Require().Eventually(func() bool { return s.waitingFor(`DELETE FROM "connector_oauth_clients"`, id) == 1 },
		2*time.Second, 10*time.Millisecond, "the delete waits at its DELETE")
	type answer struct {
		status  int
		failure string
	}
	answered := make(chan answer, 1)
	go func() {
		status, failure := call()
		answered <- answer{status, failure}
	}()
	s.Require().Eventually(func() bool { return s.waitingFor("FOR KEY SHARE", id) == 1 },
		2*time.Second, 10*time.Millisecond, "the call waits for the delete's lock on the definition")
	s.Require().NoError(held.Commit())
	s.Require().Equal(http.StatusNoContent, <-deleted)
	got := <-answered
	return got.status, got.failure
}

// waitingFor counts the statements in this database waiting for a lock whose text holds both
// statement and the connector id (https://www.postgresql.org/docs/current/monitoring-stats.html#WAIT-EVENT-TABLE).
func (s *ConnectorsSuite) waitingFor(statement, id string) int {
	var count int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(context.Background(), `
SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND wait_event_type = 'Lock'
  AND strpos(query, ?) > 0 AND strpos(query, ?) > 0`, statement, id).Scan(&count))
	return count
}

// TestAnMCPConnectorIsAnsweredAsBefore is the control for AI-3676: a custom MCP connector's
// create, and the refusals of its endpoint, answer as on 5a798ad5d, probed there with this
// request (AI-3676/base-probe.txt): the same keys and values, and the same messages.
func (s *ConnectorsSuite) TestAnMCPConnectorIsAnsweredAsBefore() {
	id := s.customID()
	status, raw := s.serverClient.call(http.MethodPost, "/v1/agents/connectors", s.customConnector(id))

	s.Require().Equal(http.StatusOK, status)
	var shown map[string]any
	s.Require().NoError(json.Unmarshal(raw, &shown))
	s.Equal([]string{"channel", "client", "created_at", "custom", "duration", "id", "inputs", "name", "revision", "schemes", "scopes"},
		slices.Sorted(maps.Keys(shown)))
	for _, key := range []string{"created_at", "duration"} {
		delete(shown, key)
	}
	s.Equal(map[string]any{"channel": false, "client": map[string]any{"registration": []any{"dcr"}}, "custom": true, "id": id,
		"inputs": []any{}, "name": "Our CRM", "revision": float64(1), "schemes": []any{"oauth2_code"}, "scopes": []any{}}, shown)

	for endpoint, message := range map[string]string{
		"https://10.0.0.7/mcp":    "endpoint must be a public https URL without userinfo, query or fragment",
		"https://8.8.8.8/mcp?x=1": "endpoint must be a public https URL without userinfo, query or fragment",
		"http://8.8.8.8/mcp":      `manifest "%s": endpoints.mcp: "http://8.8.8.8/mcp" does not start with https:// or a placeholder`,
	} {
		sent := s.customConnector(s.customID())
		sent["endpoint"] = endpoint

		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)

		s.Equal(http.StatusBadRequest, status, endpoint)
		s.Equal(strings.ReplaceAll(message, "%s", sent["id"].(string)), failure, endpoint)
	}
}

// TestACustomHTTPConnectorIsStoredAndItsAPIIsNeverShown is AC-1 of AI-3669 through the API:
// open_meteo's file as the CLI posts it is stored at revision 1 as the manifest the http
// source runs, and no answer shows its base URL or its tools.
func (s *ConnectorsSuite) TestACustomHTTPConnectorIsStoredAndItsAPIIsNeverShown() {
	id := s.customID()

	created := s.create(s.httpConnector(id))

	s.Equal(id, created.ID)
	s.Equal(1, created.Revision)
	s.True(created.Custom)
	s.Equal([]string{none.Name}, created.Schemes)
	s.Equal(created, s.get(id), "the answer to the create is the stored row")
	stored, err := s.store.ConnectorDefinitions.Latest(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	s.Equal(map[string]string{core.SourceKindHTTP: "https://8.8.8.8"}, stored.Manifest.Endpoints)
	s.Equal([]core.SourceRule{{Kind: core.SourceKindHTTP, Endpoint: core.SourceKindHTTP, HTTPTools: []core.HTTPTool{{
		Name: "get_forecast", Description: "The weather at a place.", Method: http.MethodGet, Path: "/v1/forecast", ReadOnly: true,
		Parameters: map[string]core.HTTPParameter{
			"latitude":  {Type: "number", Description: "Degrees north.", Required: true},
			"longitude": {Type: "number", Description: "Degrees east.", Required: true},
			"current":   {Type: "string"},
		}}}}}, stored.Manifest.Sources)
	for _, path := range []string{"/v1/agents/connectors/" + id, "/v1/agents/connectors?limit=200"} {
		status, raw := s.serverClient.call(http.MethodGet, path, nil)
		s.Require().Equal(http.StatusOK, status)
		s.Contains(string(raw), id, path)
		for _, hidden := range []string{"8.8.8.8", "base_url", `"http"`, "/v1/forecast", "get_forecast"} {
			s.NotContains(string(raw), hidden, path)
		}
	}
}

// TestAConnectionToAnHTTPConnectorListsItsReadOnlyTool is AC-2 of AI-3669 through the API: a
// connection with scheme none validates as connected, and its tools listing says the GET
// tool only reads. Discover sends nothing (internal/connectors/sources/http), so the API at
// the public address is never called.
func (s *ConnectorsSuite) TestAConnectionToAnHTTPConnectorListsItsReadOnlyTool() {
	id := s.customID()
	sent := s.httpConnector(id)
	tools := sent["http"].(map[string]any)["tools"].([]map[string]any)
	sent["http"].(map[string]any)["tools"] = append(tools, map[string]any{
		"name": "add_note", "description": "Adds a note.", "method": "POST", "path": "/v1/notes",
		"parameters": map[string]any{"text": map[string]any{"type": "string", "required": true}}},
		// A GET tool is read-only only when the file says so.
		map[string]any{"name": "list_notes", "description": "Lists the notes.", "method": "GET", "path": "/v1/notes"})
	s.create(sent)
	var created Connection
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/connections", appOwned(id), &created))
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/connections/"+created.ID+"/credentials",
		map[string]any{"expected_revision": 1}, nil))

	var validation ConnectionValidation
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/connections/"+created.ID+"/validate", nil, &validation))
	var listed ConnectionTools
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/connections/"+created.ID+"/tools", nil, &listed))

	s.Equal(validationConnected, string(validation.Status), validation.Error)
	readOnly := map[string]bool{}
	for _, tool := range listed.Tools {
		readOnly[tool.Name] = tool.ReadOnly
	}
	s.Equal(map[string]bool{"get_forecast": true, "add_note": false, "list_notes": false}, readOnly)
}

// TestAReadOnlyToolThatWritesIsRefusedNamingIt is AC-5 of AI-3669 through the API: a POST or
// a DELETE tool saying read_only is a 400 naming the tool, and nothing is stored.
func (s *ConnectorsSuite) TestAReadOnlyToolThatWritesIsRefusedNamingIt() {
	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		id := s.customID()
		sent := s.httpConnector(id)
		tool := sent["http"].(map[string]any)["tools"].([]map[string]any)[0]
		tool["name"], tool["method"] = "remove_forecast", method

		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)

		s.Equal(http.StatusBadRequest, status, method)
		s.Contains(failure, fmt.Sprintf(`a read-only tool must be GET or HEAD, and "remove_forecast" is %s`, method))
		s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+id, nil, nil), method)
	}
}

// TestAnHTTPConnectorWhoseAPIIsNotPublicHTTPSIsRefused is AC-7 of AI-3669 through the API,
// with AI-3674 S1.5's addresses and a few more: each is a 400 with one fixed message, so a
// caller cannot tell a name that does not resolve from a private one, and nothing is stored.
func (s *ConnectorsSuite) TestAnHTTPConnectorWhoseAPIIsNotPublicHTTPSIsRefused() {
	for _, base := range []string{
		"https://127.0.0.1",             // loopback
		"https://10.0.0.1",              // private-use, RFC 1918
		"https://169.254.169.254",       // link-local, where cloud metadata servers answer
		"http://api.open-meteo.com",     // not https
		"https://[::1]",                 // IPv6 loopback
		"https://router-probe.invalid",  // .invalid never resolves, RFC 6761 section 6.4
		"https://8.8.8.8/v1?key=secret", // a query
		"https://user:secret@8.8.8.8",   // userinfo
		"https://8.8.8.8/v1#top",        // a fragment
		"HTTPS://8.8.8.8",               // public to egress, which reads the scheme in any case, not to the manifest's rules
		"https://8.8.8.8/{x}",           // a {name} the manifest's rules refuse, since a custom connector declares no input
	} {
		id := s.customID()
		sent := s.httpConnector(id)
		sent["http"].(map[string]any)["base_url"] = base

		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)

		s.Equal(http.StatusBadRequest, status, base)
		s.Equal("base_url must be a public https URL without userinfo, query or fragment", failure, base)
		s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+id, nil, nil), base)
	}
}

// TestANewRevisionOfAnHTTPConnectorIsCheckedAgain: AI-3674 S1.5, the base URL is checked on
// every revision as on the first. A refused one leaves the stored revision as it was.
func (s *ConnectorsSuite) TestANewRevisionOfAnHTTPConnectorIsCheckedAgain() {
	id := s.customID()
	s.Equal(1, s.create(s.httpConnector(id)).Revision)
	private := s.httpConnector(id)
	private["http"].(map[string]any)["base_url"] = "https://169.254.169.254/v1"

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", private)

	s.Equal(http.StatusBadRequest, status)
	s.Equal("base_url must be a public https URL without userinfo, query or fragment", failure)
	stored, err := s.store.ConnectorDefinitions.Latest(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	s.Equal(1, stored.Revision)
	s.Equal("https://8.8.8.8", stored.Manifest.Endpoints[core.SourceKindHTTP])
	moved := s.httpConnector(id)
	moved["http"].(map[string]any)["base_url"] = "https://1.1.1.1"
	s.Equal(2, s.create(moved).Revision)
}

// TestAConnectorWithBothOrNeitherOfEndpointAndHTTPIsRefused: a custom connector is an MCP
// server or an http API, so one naming both or neither is refused and nothing is stored.
func (s *ConnectorsSuite) TestAConnectorWithBothOrNeitherOfEndpointAndHTTPIsRefused() {
	both := s.httpConnector(s.customID())
	both["endpoint"] = "https://8.8.8.8/mcp"
	neither := s.customConnector(s.customID())
	delete(neither, "endpoint")
	empty := s.customConnector(s.customID())
	empty["endpoint"] = ""
	for name, sent := range map[string]map[string]any{"both": both, "neither": neither, "an empty endpoint": empty} {
		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)

		s.Equal(http.StatusBadRequest, status, name)
		s.Equal("a custom connector has an MCP endpoint or an http API, and exactly one of them", failure, name)
		s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+sent["id"].(string), nil, nil), name)
	}
}

// TestAnHTTPConnectorTheManifestRefusesIsRefusedWithItsReason: an http block the schema takes
// but the manifest's rules refuse is a 400 with core's reason, and nothing is stored.
func (s *ConnectorsSuite) TestAnHTTPConnectorTheManifestRefusesIsRefusedWithItsReason() {
	for name, test := range map[string]struct {
		change func(sent, api map[string]any)
		reason string
	}{
		"no tool": {func(_, api map[string]any) { api["tools"] = []any{} }, "an http custom connector needs at least one tool"},
		"oauth2_code": {func(sent, _ map[string]any) {
			sent["schemes"], sent["client"] = []string{"oauth2_code"}, map[string]any{"registration": []string{"dcr"}}
		}, "oauth2_code is not available for an http custom connector"},
		"a method override parameter": {func(_, api map[string]any) {
			api["tools"].([]map[string]any)[0]["parameters"].(map[string]any)["_method"] = map[string]any{"type": "string"}
		}, "parameters._method: names a method override"},
	} {
		id := s.customID()
		sent := s.httpConnector(id)
		test.change(sent, sent["http"].(map[string]any))

		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/connectors", sent)

		s.Equal(http.StatusBadRequest, status, name)
		s.Contains(failure, test.reason, name)
		s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+id, nil, nil), name)
	}
}

// TestAnHTTPToolTheSchemaRefusesIsAValidationFailure: what the request's schema says of an
// http block (CustomHTTPAPI) is held before any rule of the manifest, an unknown field included.
func (s *ConnectorsSuite) TestAnHTTPToolTheSchemaRefusesIsAValidationFailure() {
	for name, change := range map[string]func(api, tool, parameter map[string]any){
		"a method that is not one of the six": func(_, tool, _ map[string]any) { tool["method"] = "TRACE" },
		"a lowercase method":                  func(_, tool, _ map[string]any) { tool["method"] = "get" },
		"a parameter type that is no scalar":  func(_, _, parameter map[string]any) { parameter["type"] = "object" },
		"a base_url over 2048":                func(api, _, _ map[string]any) { api["base_url"] = "https://8.8.8.8/" + strings.Repeat("a", 2033) },
		"a header on a tool": func(_, tool, _ map[string]any) {
			tool["headers"] = map[string]string{"X-HTTP-Method-Override": "DELETE"}
		},
		"an unknown field of the api":     func(api, _, _ map[string]any) { api["headers"] = map[string]string{"X-HTTP-Method-Override": "DELETE"} },
		"an unknown field of a parameter": func(_, _, parameter map[string]any) { parameter["in"] = "header" },
		"no base_url":                     func(api, _, _ map[string]any) { delete(api, "base_url") },
		"tools null":                      func(api, _, _ map[string]any) { api["tools"] = nil },
		"a parameter with no type":        func(_, _, parameter map[string]any) { delete(parameter, "type") },
	} {
		id := s.customID()
		sent := s.httpConnector(id)
		api := sent["http"].(map[string]any)
		tool := api["tools"].([]map[string]any)[0]
		change(api, tool, tool["parameters"].(map[string]any)["latitude"].(map[string]any))

		status, raw := s.serverClient.call(http.MethodPost, "/v1/agents/connectors", sent)

		s.Equal(http.StatusBadRequest, status, name)
		var answered ErrorResponse
		s.Require().NoError(json.Unmarshal(raw, &answered), name)
		s.Equal("validation_failed", answered.Error.Code, "%s: %s", name, raw)
		s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+id, nil, nil), name)
	}
}

// httpConnector is AC-1 of AI-3669's open_meteo as the CLI posts it, scheme none, at a public
// IP literal, so checking it resolves nothing.
func (s *ConnectorsSuite) httpConnector(id string) map[string]any {
	return map[string]any{
		"id":      id,
		"name":    "Open-Meteo",
		"schemes": []string{none.Name},
		"http": map[string]any{
			"base_url": "https://8.8.8.8",
			"tools": []map[string]any{{
				"name": "get_forecast", "description": "The weather at a place.", "method": "GET", "path": "/v1/forecast",
				"read_only": true,
				"parameters": map[string]any{
					"latitude":  map[string]any{"type": "number", "description": "Degrees north.", "required": true},
					"longitude": map[string]any{"type": "number", "description": "Degrees east.", "required": true},
					"current":   map[string]any{"type": "string"},
				},
			}},
		},
	}
}

// customConnector is a custom MCP connector the router accepts. Its endpoint is a public IP
// literal, so checking it resolves nothing.
func (s *ConnectorsSuite) customConnector(id string) map[string]any {
	return map[string]any{
		"id":       id,
		"name":     "Our CRM",
		"endpoint": "https://8.8.8.8/mcp",
		"schemes":  []string{"oauth2_code"},
		"client":   map[string]any{"registration": []string{"dcr"}},
	}
}

// customID is a custom connector id nothing else has.
func (s *ConnectorsSuite) customID() string { return "custom_t" + s.suffix() }

// suffix is a fresh UUID written as an id may have it.
func (s *ConnectorsSuite) suffix() string { return strings.ReplaceAll(s.utils.uuid(), "-", "") }

func (s *ConnectorsSuite) create(body map[string]any) Connector {
	var created Connector
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/connectors", body, &created))
	return created
}

func (s *ConnectorsSuite) get(id string) Connector {
	var read Connector
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+id, nil, &read))
	return read
}

// list is one page of everything, which fits while the test's app has a few of its own.
func (s *ConnectorsSuite) list(q string) ConnectorPage {
	var listed ConnectorPage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/connectors?limit=200&q="+url.QueryEscape(q), nil, &listed))
	return listed
}

func connectorIDs(definitions []Connector) []string {
	ids := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		ids = append(ids, definition.ID)
	}
	return ids
}
