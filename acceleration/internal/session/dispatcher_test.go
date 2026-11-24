//go:build integration

package session

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// DispatcherSuite is what a session's connector tool call does once the session is open: the
// check against the config and the connection as they are now, before anything is sent, and
// the envelope around the call.
type DispatcherSuite struct {
	connectorFixture
}

func TestDispatcherSuite(t *testing.T) {
	suite.Run(t, new(DispatcherSuite))
}

// TestTwoAccountsOfOneUserNeverCross: the prototype's
// TestSessionSelectsBetweenTwoAccountsAndDisconnectBlocksAnOpenRuntime, first half.
func (s *DispatcherSuite) TestTwoAccountsOfOneUserNeverCross() {
	primary, secondary := s.connection("alice", "primary"), s.connection("alice", "secondary")
	config := s.config(s.chosen("primary", "whoami"), s.chosen("secondary", "whoami"))
	d, tools, _, err := s.attach(s.spec(config, "alice", map[string]string{"primary": primary, "secondary": secondary}))
	s.Require().NoError(err)

	said, err := s.call(d, "primary__whoami", "{}")

	s.Require().NoError(err)
	s.ElementsMatch([]string{"primary__whoami", "secondary__whoami"}, names(tools))
	s.Equal("primary", said)
	s.Equal(1, s.provider.calls("primary"))
	s.Zero(s.provider.calls("secondary"))
}

// TestAGrantRemovedFromAnOpenSessionIsNotCalled: the prototype's case, and a grant given
// back works again.
func (s *DispatcherSuite) TestAGrantRemovedFromAnOpenSessionIsNotCalled() {
	app := s.connection("", "primary")
	config := s.config(s.fixed("crm", app, "whoami"))
	d, _, _, err := s.attach(s.spec(config, "", nil))
	s.Require().NoError(err)

	s.rebind(config, s.fixed("crm", app))
	_, refused := s.call(d, "crm__whoami", "{}")
	s.rebind(config, s.fixed("crm", app, "whoami"))
	said, err := s.call(d, "crm__whoami", "{}")

	s.ErrorContains(refused, `connector "crm": the tool is no longer granted, so crm__whoami was not called`)
	s.Require().NoError(err)
	s.Equal("primary", said)
	s.Equal(1, s.provider.calls("primary"), "the refused call never reached the provider")
}

// TestAPatternMatchedToolCanBeCalled: a session binding that grants whoami by pattern offers
// it and the call reaches the connection. recheck allows it because the current patterns
// still match, not because tools lists the pin.
func (s *DispatcherSuite) TestAPatternMatchedToolCanBeCalled() {
	alice := s.connection("alice", "primary")
	d, tools, unavailable, err := s.attach(s.spec(s.config(withPatterns(s.chosen("crm"), "whoami")), "alice", map[string]string{"crm": alice}))
	s.Require().NoError(err)
	s.Empty(unavailable)
	s.Equal([]string{"crm__whoami"}, names(tools))

	said, err := s.call(d, "crm__whoami", "{}")

	s.Require().NoError(err)
	s.Equal("primary", said)
	s.Equal(1, s.provider.calls("primary"))
}

// TestAFixedPatternMatchedToolCanBeCalled: a fixed binding with patterns and no digests
// offers whoami, and the call reaches the app's connection.
func (s *DispatcherSuite) TestAFixedPatternMatchedToolCanBeCalled() {
	app := s.connection("", "primary")
	d, tools, _, err := s.attach(s.spec(s.config(withPatterns(s.fixed("crm", app), "whoami")), "", nil))
	s.Require().NoError(err)
	s.Equal([]string{"crm__whoami"}, names(tools))

	said, err := s.call(d, "crm__whoami", "{}")

	s.Require().NoError(err)
	s.Equal("primary", said)
	s.Equal(1, s.provider.calls("primary"))
}

// TestAPatternNarrowedMidSessionIsNotCalled: the config drops whoami from the patterns after
// the session opened. The call is refused and never sent; a tool the patterns still match is.
func (s *DispatcherSuite) TestAPatternNarrowedMidSessionIsNotCalled() {
	alice := s.connection("alice", "primary")
	config := s.config(withPatterns(s.chosen("crm"), "whoami", "secret"))
	d, _, _, err := s.attach(s.spec(config, "alice", map[string]string{"crm": alice}))
	s.Require().NoError(err)

	s.rebind(config, withPatterns(s.chosen("crm"), "secret"))
	_, refused := s.call(d, "crm__whoami", "{}")
	said, err := s.call(d, "crm__secret", "{}")

	s.ErrorContains(refused, `connector "crm": the tool is no longer granted, so crm__whoami was not called`)
	s.Require().NoError(err)
	s.Equal("the secret", said)
	s.Equal(1, s.provider.calls("primary"), "the refused call never reached the provider")
}

func (s *DispatcherSuite) TestABindingRemovedFromAnOpenSessionIsNotCalled() {
	app := s.connection("", "primary")
	config := s.config(s.fixed("crm", app, "whoami"))
	d, _, _, err := s.attach(s.spec(config, "", nil))
	s.Require().NoError(err)

	s.rebind(config)
	_, err = s.call(d, "crm__whoami", "{}")

	s.ErrorContains(err, "the agent config no longer binds it")
	s.Zero(s.provider.calls("primary"))
}

// TestAConnectionThatNowReachesElsewhereIsNotCalled: the prototype's changed endpoint. The
// connection's input now builds another endpoint, which must not receive the call or the
// credential.
func (s *DispatcherSuite) TestAConnectionThatNowReachesElsewhereIsNotCalled() {
	app := s.connection("", "primary")
	d, _, _, err := s.attach(s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.Require().NoError(err)

	_, err = s.store.GlobalDB().NewUpdate().Model((*store.ConnectorConnection)(nil)).
		Set("inputs = ?", `{"account":"moved"}`).Where("id = ?", app).Exec(s.ctx)
	s.Require().NoError(err)
	_, err = s.call(d, "crm__whoami", "{}")

	s.ErrorContains(err, "the connection changed where it reaches since the session opened")
	s.Zero(s.provider.calls("primary"))
	s.Empty(s.provider.sent("moved"))
}

// TestADisconnectedAccountIsNotCalledAndTheOtherIsNotUsedInstead: the prototype's second
// half. A deleted connection is refused, and the session does not fall back to the
// user's other account.
func (s *DispatcherSuite) TestADisconnectedAccountIsNotCalledAndTheOtherIsNotUsedInstead() {
	primary, secondary := s.connection("alice", "primary"), s.connection("alice", "secondary")
	config := s.config(s.chosen("primary", "whoami"), s.chosen("secondary", "whoami"))
	d, _, _, err := s.attach(s.spec(config, "alice", map[string]string{"primary": primary, "secondary": secondary}))
	s.Require().NoError(err)

	s.Require().NoError(s.store.ConnectorConnections.Delete(s.ctx, s.customerID, primary))
	_, refused := s.call(d, "primary__whoami", "{}")
	said, err := s.call(d, "secondary__whoami", "{}")

	s.ErrorContains(refused, "the connection is gone")
	s.Zero(s.provider.calls("primary"))
	s.Require().NoError(err)
	s.Equal("secondary", said)
}

func (s *DispatcherSuite) TestAConnectionThatNeedsReauthorizationIsNotCalled() {
	app := s.connection("", "primary")
	d, _, _, err := s.attach(s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.Require().NoError(err)

	s.setState(app, func(state *core.CredentialState) { state.Status = store.ConnectionNeedsReauthorization })
	_, err = s.call(d, "crm__whoami", "{}")

	s.ErrorContains(err, "the connection is needs_reauthorization")
	s.Zero(s.provider.calls("primary"))
}

// TestAnUngrantedToolUnderABoundAliasIsNotCallable: the model names a tool the provider has
// and the grant does not. It is not handed to the rest of the session either.
func (s *DispatcherSuite) TestAnUngrantedToolUnderABoundAliasIsNotCallable() {
	app := s.connection("", "primary")
	d, _, _, err := s.attach(s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.Require().NoError(err)
	d.next = &answering{}

	_, err = s.call(d, "crm__secret", "{}")

	s.ErrorContains(err, "crm__secret is not a tool this session can run")
	s.Zero(s.provider.calls("primary"))
	s.Empty(d.next.(*answering).asked)
}

func (s *DispatcherSuite) TestANameItDidNotOpenGoesToTheRestOfTheSession() {
	app := s.connection("", "primary")
	d, _, _, err := s.attach(s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.Require().NoError(err)
	d.next = &answering{}

	said, err := s.call(d, "lookup_order", "{}")

	s.Require().NoError(err)
	s.Equal("the caller's answer", said)
	s.Equal([]string{"lookup_order"}, d.next.(*answering).asked)
}

// TestACallTheTimeoutCutsOffIsAnUnknownOutcome: the provider may have done the work, so the
// model is not told the call failed.
func (s *DispatcherSuite) TestACallTheTimeoutCutsOffIsAnUnknownOutcome() {
	app := s.connection("", "primary")
	binding := s.fixed("crm", app, "slow")
	binding.TimeoutMs = 200
	d, _, _, err := s.attach(s.spec(s.config(binding), "", nil))
	s.Require().NoError(err)

	said, err := s.call(d, "crm__slow", "{}")

	s.Require().NoError(err, "not an error the model would retry")
	s.Contains(said, "outcome_unknown: crm__slow did not answer in time")
	s.Equal(1, s.provider.calls("primary"))
}

// TestACallThatCannotReachItsServerFailsWithoutTheEndpointsPath: a hosted MCP server's path can
// hold its access secret, so the error the model and the session's events are given keeps the
// endpoint to its host, and still says what failed underneath.
func (s *DispatcherSuite) TestACallThatCannotReachItsServerFailsWithoutTheEndpointsPath() {
	secret := uuid.NewString()
	hosted := s.hostedEndpoint("/hosted/"+secret+"/mcp", "", "")
	d, _, _, err := s.attach(s.spec(s.config(s.fixed("crm", s.connection("", "primary"), "whoami")), "", nil))
	s.Require().NoError(err)
	hosted.Close()

	_, err = s.call(d, "crm__whoami", "{}")

	s.Require().Error(err)
	s.NotContains(err.Error(), secret)
	s.Contains(err.Error(), hosted.URL, "the host is still named")
	s.ErrorIs(err, syscall.ECONNREFUSED)
}

// TestAProvidersErrorThatEchoesItsPathFailsWithoutIt: a provider's own error message may quote
// the path it was sent to, with no scheme or host before it, and name another endpoint whole.
func (s *DispatcherSuite) TestAProvidersErrorThatEchoesItsPathFailsWithoutIt() {
	path, moved := "/hosted/"+uuid.NewString()+"/mcp", uuid.NewString()
	s.hostedEndpoint(path, "", "")
	d, _, _, err := s.attach(s.spec(s.config(s.fixed("crm", s.connection("", "primary"), "whoami")), "", nil))
	s.Require().NoError(err)
	s.provider.answerRPC("primary", http.StatusBadRequest,
		`"error":{"code":-32600,"message":"no server at `+path+`, it moved to https://mcp.example/hosted/`+moved+`/mcp"}`)

	_, err = s.call(d, "crm__whoami", "{}")

	s.Require().ErrorContains(err, "no server at")
	s.Contains(err.Error(), "it moved to https://mcp.example")
	s.NotContains(err.Error(), path)
	s.NotContains(err.Error(), moved)
}

// TestAToolsOwnLongErrorReachesTheModelWholeWithoutTheEndpointsPath: an isError is the tool's
// words for the model to act on, so a long one is not cut to a log line and a record it names
// by URL keeps its path; the endpoint's path and the call's token are still taken out.
func (s *DispatcherSuite) TestAToolsOwnLongErrorReachesTheModelWholeWithoutTheEndpointsPath() {
	d, _, _, err := s.attach(s.spec(s.config(s.fixed("crm", s.connection("", "primary"), "whoami")), "", nil))
	s.Require().NoError(err)
	issue := "Issue https://tracker.example/acme/repo/issues/12 is closed."
	words := issue + strings.Repeat(" Reopen it before you comment.", 100) +
		" Asked as " + tokenOf("primary") + " at /primary/mcp. That is all."
	s.provider.answerRPC("primary", http.StatusOK, `"result":{"content":[{"type":"text","text":"`+words+`"}],"isError":true}`)

	_, err = s.call(d, "crm__whoami", "{}")

	s.Require().Error(err)
	s.True(strings.HasPrefix(err.Error(), issue), err.Error())
	s.True(strings.HasSuffix(err.Error(), "Asked as [redacted] at [redacted]. That is all."), err.Error())
	s.NotContains(err.Error(), tokenOf("primary"))
}

// TestAProvidersErrorAfterARedirectFailsWithoutTheFirstPath: the endpoint redirects the call
// within its origin, and the answer there quotes the path the call was first sent to. That
// path is taken out as well as the one the answer came from, which is a prefix of it.
func (s *DispatcherSuite) TestAProvidersErrorAfterARedirectFailsWithoutTheFirstPath() {
	secret := uuid.NewString()
	path := "/hosted/" + secret + "/mcp"
	s.hostedEndpoint(path, "/hosted", path)
	d, _, _, err := s.attach(s.spec(s.config(s.fixed("crm", s.connection("", "primary"), "whoami")), "", nil))
	s.Require().NoError(err)
	s.provider.answer("primary", http.StatusTemporaryRedirect, http.Header{"Location": {"/hosted"}}, "")

	_, err = s.call(d, "crm__whoami", "{}")

	s.Require().ErrorContains(err, "no server at")
	s.NotContains(err.Error(), secret)
}

// TestAProvidersErrorAfterARedirectFailsWithoutThePathItMovedTo: the endpoint "/mcp" redirects
// the call within its origin to a path that holds it, and the answer there quotes that path.
// The path is taken out whole, not cut apart where it holds the first one.
func (s *DispatcherSuite) TestAProvidersErrorAfterARedirectFailsWithoutThePathItMovedTo() {
	secret := uuid.NewString()
	moved := "/t/" + secret + "/mcp"
	s.hostedEndpoint("/mcp", moved, moved)
	d, _, _, err := s.attach(s.spec(s.config(s.fixed("crm", s.connection("", "primary"), "whoami")), "", nil))
	s.Require().NoError(err)
	s.provider.answer("primary", http.StatusTemporaryRedirect, http.Header{"Location": {moved}}, "")

	_, err = s.call(d, "crm__whoami", "{}")

	s.Require().ErrorContains(err, "no server at")
	s.NotContains(err.Error(), secret)
}

// hostedEndpoint has the test's connector reach the provider's primary account through a
// server of its own at path, which the test may close, and returns that server. At moved,
// unless it is "", the server answers a 400 whose JSON-RPC error quotes quoted.
func (s *DispatcherSuite) hostedEndpoint(path, moved, quoted string) *httptest.Server {
	target, err := url.Parse(s.provider.URL)
	s.Require().NoError(err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport, proxy.FlushInterval = s.provider.Client().Transport, -1
	hosted := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if moved != "" && r.URL.Path == moved {
			var message struct {
				ID json.RawMessage `json:"id"`
			}
			_ = json.NewDecoder(r.Body).Decode(&message)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":`+string(message.ID)+`,"error":{"code":-32600,"message":"no server at `+quoted+`"}}`)
			return
		}
		if r.URL.Path != path {
			http.NotFound(w, r)
			return
		}
		r.URL.Path = "/primary/mcp"
		proxy.ServeHTTP(w, r)
	}))
	s.T().Cleanup(hosted.Close)
	manifest, err := core.ParseManifest([]byte(strings.Replace(s.crmManifest(s.connectorID, s.revision+1),
		s.provider.URL+"/{account}/mcp", hosted.URL+path, 1)))
	s.Require().NoError(err)
	definition, err := s.store.ConnectorDefinitions.Revise(s.ctx, s.customerID, manifest)
	s.Require().NoError(err)
	s.revision = definition.Revision
	return hosted
}

// TestACallTheConnectionsClientCutsOffIsAnUnknownOutcome: a binding may wait longer than the
// connection's client lets one request run (connectorHTTPTimeout, 10 s in the router, against
// timeout_ms up to 30000). The client gives up after the request was sent, so the model is
// told the outcome is unknown, not that the call failed.
func (s *DispatcherSuite) TestACallTheConnectionsClientCutsOffIsAnUnknownOutcome() {
	defer func(kept *Manager) { s.manager = kept }(s.manager)
	s.manager = s.managerWith(300 * time.Millisecond)
	app := s.connection("", "primary")
	binding := s.fixed("crm", app, "slow")
	binding.TimeoutMs = 1500
	d, _, _, err := s.attach(s.spec(s.config(binding), "", nil))
	s.Require().NoError(err)

	said, err := s.call(d, "crm__slow", "{}")

	s.Require().NoError(err, "not an error the model would retry")
	s.Contains(said, "outcome_unknown: crm__slow did not answer in time")
	s.Equal(1, s.provider.calls("primary"))
}

// TestACallToAServerThatStreamsIsAnUnknownOutcomePastTheClientsLimit: the server sends its
// SSE headers at once and is still working when the connection's client would have cut the
// request. The call runs to the binding's deadline instead, and its outcome is unknown.
func (s *DispatcherSuite) TestACallToAServerThatStreamsIsAnUnknownOutcomePastTheClientsLimit() {
	defer func(kept *Manager) { s.manager = kept }(s.manager)
	s.manager = s.managerWith(300 * time.Millisecond)
	s.provider.streamFirst()
	app := s.connection("", "primary")
	binding := s.fixed("crm", app, "slow")
	binding.TimeoutMs = 1500
	d, _, _, err := s.attach(s.spec(s.config(binding), "", nil))
	s.Require().NoError(err)
	started := time.Now()

	said, err := s.call(d, "crm__slow", "{}")

	s.Require().NoError(err, "not an error the model would retry")
	s.Contains(said, "outcome_unknown: crm__slow did not answer in time")
	s.Equal(1, s.provider.calls("primary"))
	s.GreaterOrEqual(time.Since(started), 1500*time.Millisecond, "the binding's deadline, not the client's, ended it")
}

// TestAnInterruptedCallIsCancelledAtTheProvider: the turn's context ends, and the provider
// is sent notifications/cancelled for the call.
func (s *DispatcherSuite) TestAnInterruptedCallIsCancelledAtTheProvider() {
	app := s.connection("", "primary")
	binding := s.fixed("crm", app, "slow")
	binding.TimeoutMs = 30000
	d, _, _, err := s.attach(s.spec(s.config(binding), "", nil))
	s.Require().NoError(err)
	turn, interrupt := context.WithCancel(s.ctx)
	time.AfterFunc(200*time.Millisecond, interrupt)

	_, err = d.Run(turn, llm.ToolCall{ID: uuid.NewString(), Name: "crm__slow", Arguments: "{}"})

	s.ErrorIs(err, context.Canceled)
	s.Eventually(func() bool { return slices.Contains(s.provider.sent("primary"), "notifications/cancelled") },
		5*time.Second, 20*time.Millisecond)
}

// interrupted runs crm__slow through d on a turn interrupted 200 ms in, and says what the
// turn was told and how long it waited.
func (s *DispatcherSuite) interrupted(d *dispatcher) (string, time.Duration, error) {
	turn, interrupt := context.WithCancel(s.ctx)
	time.AfterFunc(200*time.Millisecond, interrupt)
	started := time.Now()
	parts, err := d.Run(turn, llm.ToolCall{ID: uuid.NewString(), Name: "crm__slow", Arguments: "{}"})
	return llm.TextOf(parts), time.Since(started), err
}

// slowWith is a fixed binding of the app's primary account granting slow, with policy.
func (s *DispatcherSuite) slowWith(policy *store.BindingPolicy) (*dispatcher, string) {
	app := s.connection("", "primary")
	binding := s.fixed("crm", app, "slow")
	binding.TimeoutMs = 30000
	binding.Policy = policy
	d, _, _, err := s.attach(s.spec(s.config(binding), "", nil))
	s.Require().NoError(err)
	return d, app
}

// TestAWaitBindingFinishesItsCallAfterAnInterruption: on_interrupt wait lets an interrupted
// call finish. The turn waits for the answer, and the provider is never sent the cancel.
func (s *DispatcherSuite) TestAWaitBindingFinishesItsCallAfterAnInterruption() {
	d, _ := s.slowWith(&store.BindingPolicy{OnInterrupt: store.InterruptWait})

	said, waited, err := s.interrupted(d)

	s.Require().NoError(err)
	s.Equal("done", said)
	s.GreaterOrEqual(waited, slowFor, "the call ran to its answer")
	s.NotContains(s.provider.sent("primary"), "notifications/cancelled")
}

// TestACancelBindingSendsTheCancelAsToday: on_interrupt cancel, written out or left empty,
// is TestAnInterruptedCallIsCancelledAtTheProvider.
func (s *DispatcherSuite) TestACancelBindingSendsTheCancelAsToday() {
	cancellable := true
	for name, policy := range map[string]*store.BindingPolicy{
		"written out": {OnInterrupt: store.InterruptCancel, Cancellable: &cancellable},
		"empty":       {},
	} {
		s.Run(name, func() {
			s.provider.forget()
			d, _ := s.slowWith(policy)

			_, waited, err := s.interrupted(d)

			s.ErrorIs(err, context.Canceled)
			s.Less(waited, slowFor)
			s.Eventually(func() bool { return slices.Contains(s.provider.sent("primary"), "notifications/cancelled") },
				5*time.Second, 20*time.Millisecond)
		})
	}
}

// TestABindingThatIsNotCancellableStopsWaitingAndLeavesTheCallRunning: the turn moves on at
// the interruption, the provider is never told to stop, and the row says the outcome is
// unknown.
func (s *DispatcherSuite) TestABindingThatIsNotCancellableStopsWaitingAndLeavesTheCallRunning() {
	cancellable := false
	d, app := s.slowWith(&store.BindingPolicy{Cancellable: &cancellable})

	_, waited, err := s.interrupted(d)

	s.ErrorIs(err, context.Canceled)
	s.Less(waited, slowFor, "the turn did not wait for the answer")
	s.Never(func() bool { return slices.Contains(s.provider.sent("primary"), "notifications/cancelled") },
		slowFor, 50*time.Millisecond)
	s.Equal(1, s.provider.calls("primary"))
	s.Eventually(func() bool {
		rows, err := s.store.ConnectorInvocations.Select(s.ctx, s.customerID, store.InvocationFilter{ConnectionID: app, Limit: 0, After: nil})
		return err == nil && len(rows) == 1 && rows[0].ErrorType == store.InvocationOutcomeUnknown
	}, 5*time.Second, 20*time.Millisecond)
}

// TestAToolsPolicyIsWhatItsOwnBindingAsksFor: a phrase, a wait and a cancel belong to their
// binding's tools, and a binding without a policy, or a name under no bound alias, asks for
// nothing.
func (s *DispatcherSuite) TestAToolsPolicyIsWhatItsOwnBindingAsksFor() {
	app := s.connection("", "primary")
	speaking := s.fixed("crm", app, "slow")
	speaking.Policy = &store.BindingPolicy{PreSpeech: "Let me pull that up.", OnInterrupt: store.InterruptWait}
	cancelling := s.fixed("tickets", app, "whoami")
	cancelling.Policy = &store.BindingPolicy{OnInterrupt: store.InterruptCancel}
	d, _, _, err := s.attach(s.spec(s.config(speaking, cancelling, s.fixed("quiet", app, "whoami")), "", nil))
	s.Require().NoError(err)

	s.Equal(agent.ToolPolicy{PreSpeech: "Let me pull that up.", Waits: true}, d.toolPolicy("crm__slow"))
	s.Equal(agent.ToolPolicy{Cancels: true}, d.toolPolicy("tickets__whoami"))
	s.Equal(agent.ToolPolicy{}, d.toolPolicy("quiet__whoami"))
	s.Equal(agent.ToolPolicy{}, d.toolPolicy("lookup_order"))
	s.Equal(agent.ToolPolicy{}, d.toolPolicy("crm"), "a bare alias is no tool of the binding")
}

// answering is the rest of a session's tool chain, which answers whatever it is asked.
type answering struct {
	asked []string
}

func (a *answering) Run(_ context.Context, call llm.ToolCall) ([]llm.ContentPart, error) {
	a.asked = append(a.asked, call.Name)
	return llm.TextParts("the caller's answer"), nil
}
