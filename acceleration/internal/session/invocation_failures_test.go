//go:build integration

package session

import (
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// The provider's status and the error message an invocation row keeps (AI-1244): the line's
// provider_status when an answer came, and its provider_message, else its provider_body.

func (s *CallLogSuite) TestAFailedCallsRowKeepsTheStatusAndItsLineTheMessage() {
	app := s.connection("", "primary")
	d, logged := s.logging(s.spec(s.config(s.fixed("crm", app, "fails")), "", nil))

	_, err := s.call(d, "crm__fails", "{}")

	s.Require().Error(err)
	row := s.row(app)
	s.Equal(store.InvocationExternalServer, row.ErrorType)
	s.Equal(statusOf(http.StatusOK), row.ProviderStatus, "an MCP isError comes in a 200")
	s.Equal("the call failed: external_server", row.ErrorMessage, "what the provider said is the line's")
	s.Contains(s.onlyFailedLine(logged), `provider_message="the record is locked"`)
}

// TestARowOfA4xxSaysOnlyHowItFailed: a 403 with a JSON body, whose error the MCP client words
// from the status alone; the line keeps the body, which says why, and the row its error type.
func (s *CallLogSuite) TestARowOfA4xxSaysOnlyHowItFailed() {
	app := s.connection("", "primary")
	d, logged := s.logging(s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.provider.answer("primary", http.StatusForbidden, http.Header{"Content-Type": {"application/json"}},
		`{"error":"forbidden","message":"this app may not read contacts"}`)

	_, err := s.call(d, "crm__whoami", "{}")

	s.Require().Error(err)
	row := s.row(app)
	s.Equal(statusOf(http.StatusForbidden), row.ProviderStatus)
	s.Equal("the call failed: customer_auth", row.ErrorMessage)
	line := s.onlyFailedLine(logged)
	s.Contains(line, `provider_message="mcp: crm__whoami: calling \"tools/call\": sending \"tools/call\": `+
		`rejected by transport: mcp: the server refused the request with 403 Forbidden"`)
	s.Contains(line, `this app may not read contacts`)
}

// echoed is a provider's error that names the token the call was sent with and the request's
// path, as Express's default 404 names the path ("Cannot POST /primary/mcp").
var echoed = "invalid token " + tokenOf("primary") + " for /primary/mcp"

// TestAJSONRPCErrorThatEchoesTheTokenIsKeptWithoutIt: a 200 whose JSON-RPC error names the
// token and the path, which the MCP client puts in the call's error; the line keeps it without
// either, and the row says only how the call failed. The model reads only that the tool failed.
func (s *CallLogSuite) TestAJSONRPCErrorThatEchoesTheTokenIsKeptWithoutIt() {
	app := s.connection("", "primary")
	d, logged := s.logging(s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.provider.answerRPC("primary", http.StatusOK, `"error":{"code":-32603,"message":"`+echoed+`"}`)

	_, err := s.call(d, "crm__whoami", "{}")

	s.Require().ErrorContains(err, echoed)
	s.Equal("the tool failed", harness.ToolFailure(err))
	row := s.row(app)
	s.Equal(statusOf(http.StatusOK), row.ProviderStatus)
	s.Equal("the call failed: external_server", row.ErrorMessage)
	line := s.onlyFailedLine(logged)
	s.Contains(line, `provider_message="mcp: crm__whoami: calling \"tools/call\": invalid token [redacted] for [redacted]"`)
	s.NotContains(line, tokenOf("primary"))
	s.NotContains(line, "/primary/mcp")
}

// TestAJSONRPCErrorIn4xxThatEchoesTheTokenIsKeptWithoutIt: the same error in a 400, which the
// MCP client also puts in the call's error; the line keeps it, with neither the token nor the
// path, and the row says only how the call failed.
func (s *CallLogSuite) TestAJSONRPCErrorIn4xxThatEchoesTheTokenIsKeptWithoutIt() {
	app := s.connection("", "primary")
	d, logged := s.logging(s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.provider.answer("primary", http.StatusBadRequest, http.Header{"Content-Type": {"application/json"}},
		`{"jsonrpc":"2.0","id":2,"error":{"code":-32001,"message":"`+echoed+`"}}`)

	_, err := s.call(d, "crm__whoami", "{}")

	s.Require().ErrorContains(err, echoed)
	row := s.row(app)
	s.Equal(statusOf(http.StatusBadRequest), row.ProviderStatus)
	s.Equal("the call failed: external_server", row.ErrorMessage)
	line := s.onlyFailedLine(logged)
	s.Contains(line, `provider_message="mcp: crm__whoami: calling \"tools/call\": sending \"tools/call\": `+
		`invalid token [redacted] for [redacted]: rejected by transport: Bad Request"`)
	s.NotContains(line, tokenOf("primary"))
	s.NotContains(line, "/primary/mcp")
}

// TestAnIsErrorThatEchoesTheTokenIsKeptWithoutIt: a tool's isError text that names the token
// and the path; the line keeps the text without either, and the row says only how the call
// failed. What the model reads is the tool's own words without the token or the path.
func (s *CallLogSuite) TestAnIsErrorThatEchoesTheTokenIsKeptWithoutIt() {
	app := s.connection("", "primary")
	d, logged := s.logging(s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.provider.answerRPC("primary", http.StatusOK, `"result":{"content":[{"type":"text","text":"`+echoed+`"}],"isError":true}`)

	_, err := s.call(d, "crm__whoami", "{}")

	s.Require().Error(err)
	s.NotContains(err.Error(), tokenOf("primary"))
	s.NotContains(err.Error(), "/primary/mcp")
	s.Contains(err.Error(), "[redacted]")
	s.ErrorIs(err, harness.ErrCorrectable)
	row := s.row(app)
	s.Equal(store.InvocationExternalServer, row.ErrorType)
	s.Equal("the call failed: external_server", row.ErrorMessage)
	line := s.onlyFailedLine(logged)
	s.Contains(line, `provider_message="invalid token [redacted] for [redacted]"`)
	s.NotContains(line, tokenOf("primary"))
	s.NotContains(line, "/primary/mcp")
}

// TestARowWithNoErrorSaysHowTheCallFailed: a 429 with a body and a Retry-After, which the
// limiter holds, returns no error; the row still says how the call failed.
func (s *CallLogSuite) TestARowWithNoErrorSaysHowTheCallFailed() {
	manager, _ := s.loggingManager()
	manager.options.Connectors.Limiter = s.limiter()
	s.limitPerApp()
	app := s.connection("", "primary")
	d := s.attachOn(manager, s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.provider.answer("primary", http.StatusTooManyRequests, http.Header{"Retry-After": {"30"}}, "slow down, see https://crm.example/limits?key=abc")

	said, err := s.call(d, "crm__whoami", "{}")

	s.Require().NoError(err)
	s.Contains(said, "connector_rate_limited")
	row := s.row(app)
	s.Equal(store.InvocationExternalServer, row.ErrorType)
	s.Equal(statusOf(http.StatusTooManyRequests), row.ProviderStatus)
	s.Equal("the call failed: external_server", row.ErrorMessage)
}

func (s *CallLogSuite) TestACallThatAnswersKeepsNoStatusAndNoMessage() {
	app := s.connection("", "primary")
	d, _ := s.logging(s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))

	said, err := s.call(d, "crm__whoami", "{}")

	s.Require().NoError(err)
	s.Equal("primary", said)
	row := s.row(app)
	s.Empty(row.ErrorType)
	s.Nil(row.ProviderStatus)
	s.Empty(row.ErrorMessage)
}

// TestARefusedCallSaysItWasDeniedAndKeepsNoStatus: nothing was sent, so no status.
func (s *CallLogSuite) TestARefusedCallSaysItWasDeniedAndKeepsNoStatus() {
	app := s.connection("", "primary")
	config := s.config(s.fixed("crm", app, "whoami"))
	d, _ := s.logging(s.spec(config, "", nil))
	s.rebind(config, s.fixed("crm", app))

	_, err := s.call(d, "crm__whoami", "{}")

	s.Require().Error(err)
	row := s.row(app)
	s.Equal(store.InvocationDenied, row.ErrorType)
	s.Nil(row.ProviderStatus)
	s.Equal("the call failed: denied", row.ErrorMessage)
}

// TestARowKeepsNothingOfABodyTheProviderEchoesTheTokenIn: a 400 whose body names the token it
// was sent, the request's path and a URL with a query, and then stalls past the binding's
// timeout, so the call is outcome_unknown with no error and the row says only that.
func (s *CallLogSuite) TestARowKeepsNothingOfABodyTheProviderEchoesTheTokenIn() {
	app := s.connection("", "primary")
	binding := s.fixed("crm", app, "whoami")
	binding.TimeoutMs = 1000
	d, _ := s.logging(s.spec(s.config(binding), "", nil))
	s.provider.answerThen("primary", http.StatusBadRequest, http.Header{"Content-Type": {"text/plain"}},
		"invalid token "+tokenOf("primary")+" for /primary/mcp, see https://status.crm.example/i?token="+queryToken+" ",
		holdUntilGone)

	_, err := s.call(d, "crm__whoami", "{}")

	s.Require().NoError(err)
	row := s.row(app)
	s.Equal(store.InvocationOutcomeUnknown, row.ErrorType)
	s.Equal(statusOf(http.StatusBadRequest), row.ProviderStatus)
	s.Equal("the call failed: outcome_unknown", row.ErrorMessage)
}

// TestARowNamesNoCredential: the provider refuses the token it was sent, and the model put
// another in the call's arguments; the row keeps neither, nor the Authorization header.
func (s *CallLogSuite) TestARowNamesNoCredential() {
	app := s.connection("", "primary")
	d, _ := s.logging(s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.sendWrongToken(app)

	_, err := s.call(d, "crm__whoami", `{"api_key": "`+tokenOf("moved")+`"}`)

	s.Require().Error(err)
	row := s.row(app)
	s.Equal(store.InvocationCustomerAuth, row.ErrorType)
	s.Equal(statusOf(http.StatusUnauthorized), row.ProviderStatus)
	s.Equal("the call failed: customer_auth", row.ErrorMessage)
}

// heldOnTheBase is what the model read of a call the limiter held before sending, the
// provider's Retry-After being 30, on 11fae4b1 (probe/base-11fae4b1.txt in the AI-1244 ledger,
// by probe/zz_probe_test.go there). Its row's error_type was denied, and its line's
// provider_message "".
const heldOnTheBase = "connector_rate_limited: the provider limits how often crm__whoami may be called, and it " +
	"was not run. retry_after_seconds: 30. Do not call it again before then."

// TestACallTheLimiterHoldsSaysSoWithItsRetryAfter: the call is not sent; its row stays denied,
// as on the base, with no status and the router's reason, which its line says too. What the
// model reads is the base's.
func (s *CallLogSuite) TestACallTheLimiterHoldsSaysSoWithItsRetryAfter() {
	manager, logged := s.loggingManager()
	manager.options.Connectors.Limiter = s.limiter()
	s.limitPerApp()
	app := s.connection("", "primary")
	d := s.attachOn(manager, s.spec(s.config(s.fixed("crm", app, "whoami")), "", nil))
	s.provider.limit("primary", "30")
	_, err := s.call(d, "crm__whoami", "{}")
	s.Require().NoError(err)
	s.provider.lift()
	logged.Reset()

	held, err := s.call(d, "crm__whoami", "{}")

	s.Require().NoError(err)
	s.Equal(heldOnTheBase, held)
	s.Equal(1, s.provider.calls("primary"), "the held call was not sent")
	rows := s.rows(app, 2)
	s.Equal(store.InvocationDenied, rows[0].ErrorType)
	s.Nil(rows[0].ProviderStatus)
	s.Equal("rate limited by the router: retry after 30s", rows[0].ErrorMessage)
	line := s.onlyFailedLine(logged)
	s.Contains(line, "error_type=denied")
	s.Contains(line, `provider_message="rate limited by the router: retry after 30s"`)
}

// TestAnIncognitoSessionsRowSaysHowTheCallFailed: as any row does, by the owner's decision
// (AI-1242, 2026-10-14), with no session.
func (s *CallLogSuite) TestAnIncognitoSessionsRowSaysHowTheCallFailed() {
	app := s.connection("", "primary")
	spec := s.spec(s.config(s.fixed("crm", app, "fails")), "", nil)
	spec.ID, spec.Incognito = uuid.NewString(), true
	d, _ := s.logging(spec)

	_, err := s.call(d, "crm__fails", "{}")

	s.Require().Error(err)
	row := s.row(app)
	s.Empty(row.SessionID)
	s.Equal("the call failed: external_server", row.ErrorMessage)
}

// rows are connection's invocation rows, newest first, once count are written.
func (s *CallLogSuite) rows(connection string, count int) []store.ConnectorInvocation {
	var rows []store.ConnectorInvocation
	s.Require().Eventually(func() bool {
		var err error
		rows, err = s.store.ConnectorInvocations.Select(s.ctx, s.customerID, store.InvocationFilter{ConnectionID: connection, Limit: 0, After: nil})
		return err == nil && len(rows) == count
	}, 5*time.Second, 20*time.Millisecond)
	return rows
}

// statusOf is status as a row's ProviderStatus holds it.
func statusOf(status int) *int { return &status }
