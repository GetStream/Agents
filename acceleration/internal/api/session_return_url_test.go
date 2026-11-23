//go:build integration

package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
)

// A session's return_url (AI-1384) is where the logins and step-ups its chat asks for go back
// to, as createAuthorization's return_url is for a consent the backend begins. These run in
// ChatLoginsSuite, whose model reaches for crm's echo whenever a person asks.

// appOrigin is appReturnURL's origin, which the launch page takes the handoff from.
const appOrigin = "https://app.example"

// badReturnURL is plain http on a host that is not loopback, which checkReturnURL refuses
// (RFC 8252 sections 7.3 and 8.3).
const badReturnURL = "http://app.example/connectors/done"

func (s *ChatLoginsSuite) TestAChatLoginOfASessionWithAReturnURLIsLaunchedFromItsOriginAndGoesBackThere() {
	s.devicesMayReturnTo(appOrigin)
	connector, grant := s.connector()
	opened := s.client.createSession(s.returningTo(s.session(s.config(connector, grant), nil), appReturnURL))
	events := s.client.opens("/v1/agents/sessions/" + opened.Id + "/events")

	s.ask(opened.Id)
	asked := s.loginOn(events, "")
	page, location, _ := s.loggedIn(asked)

	s.Equal(launchPageFrom(appOrigin), page, "the device named a page of an origin its app's policy lists")
	s.Equal(s.backAtTheApp(asked["connection_id"].(string)), location)
	s.Equal(connectorEchoText, s.carryOn(events).ran["result"], "the chat carries on as after any login")
}

func (s *ChatLoginsSuite) TestAStepUpOfASessionWithAReturnURLIsLaunchedFromItsOriginAndGoesBackThere() {
	s.devicesMayReturnTo(appOrigin)
	mine, events, opened := s.steppingUpReturningTo(pointerTo(appReturnURL))

	s.ask(opened)
	page, location, _ := s.loggedIn(s.stepUpOn(events))

	s.Equal(launchPageFrom(appOrigin), page)
	s.Equal(s.backAtTheApp(mine), location)
}

// TestAChatLoginOfASessionWithoutAReturnURLIsLaunchedAndEndsExactlyAsOnBase: the launch page,
// Location and body base b7cea98f served for a chat login (probe-base-author.txt in the
// AI-1384 ledger, 2026-10-16).
func (s *ChatLoginsSuite) TestAChatLoginOfASessionWithoutAReturnURLIsLaunchedAndEndsExactlyAsOnBase() {
	for _, returnURL := range []*string{nil, pointerTo("")} {
		connector, grant := s.connector()
		request := s.session(s.config(connector, grant), nil)
		request.ReturnUrl = returnURL
		opened := s.client.createSession(request)
		events := s.client.opens("/v1/agents/sessions/" + opened.Id + "/events")

		s.ask(opened.Id)
		asked := s.loginOn(events, "")
		page, location, body := s.loggedIn(asked)

		s.assertEndsAsOnBase(asked["connection_id"].(string), page, location, body)
		s.Eventually(func() bool { return s.storedWithoutReturnURL(opened.Id) }, settleFor, 20*time.Millisecond,
			"the row has return_url NULL, not an empty string")
	}
}

// TestAStepUpOfASessionWithoutAReturnURLIsLaunchedAndEndsExactlyAsOnBase: as base b7cea98f
// served a step-up (probe-base-author.txt).
func (s *ChatLoginsSuite) TestAStepUpOfASessionWithoutAReturnURLIsLaunchedAndEndsExactlyAsOnBase() {
	mine, events, opened := s.steppingUp()

	s.ask(opened)
	page, location, body := s.loggedIn(s.stepUpOn(events))

	s.assertEndsAsOnBase(mine, page, location, body)
}

// TestAChatCarriedOnAfterItEndedStillGoesBackToItsReturnURL: the session ended, so the next
// message carries it on from its row (reopenedFromRow), as another process would.
func (s *ChatLoginsSuite) TestAChatCarriedOnAfterItEndedStillGoesBackToItsReturnURL() {
	s.devicesMayReturnTo(appOrigin)
	connector, grant := s.connector()
	opened := s.client.createSession(s.returningTo(s.session(s.config(connector, grant), nil), appReturnURL))
	s.Eventually(func() bool { return s.storedReturnURL(opened.Id) == appReturnURL }, settleFor, 20*time.Millisecond,
		"the row keeps the return_url")
	s.client.stopSession(opened.Id)

	s.ask(opened.Id)

	s.Equal(launchPageFrom(appOrigin), s.launchedFor(connector))
}

// TestAForkOfAnEndedSessionKeepsItsParentsReturnURL: the parent is read from its row, and the
// fork takes the return_url as it takes the parent's connector_bindings. The fork's row is what
// its own chat is carried on from. (A fork of a stored parent runs on the default model rather
// than the config's, so the logging-in model does not ask for a login here; the next test does.)
func (s *ChatLoginsSuite) TestAForkOfAnEndedSessionKeepsItsParentsReturnURL() {
	s.devicesMayReturnTo(appOrigin)
	connector, grant := s.connector()
	opened := s.client.createSession(s.returningTo(s.session(s.config(connector, grant), nil), appReturnURL))
	s.Require().Eventually(func() bool { return s.storedReturnURL(opened.Id) == appReturnURL }, settleFor, 20*time.Millisecond,
		"the parent's row is written")
	s.client.stopSession(opened.Id)

	forked := s.fork(opened.Id, ForkSessionRequest{Messages: pointerTo(false)})

	s.Eventually(func() bool { return s.storedReturnURL(forked.Id) == appReturnURL }, settleFor, 20*time.Millisecond,
		"the fork's row keeps the parent's return_url")
}

// TestAForkNamingAConfigGoesBackToItsParentsReturnURL: a config named on the fork replaces the
// parent's models, not where the caller's consents go back to.
func (s *ChatLoginsSuite) TestAForkNamingAConfigGoesBackToItsParentsReturnURL() {
	s.devicesMayReturnTo(appOrigin)
	connector, grant := s.connector()
	config := s.config(connector, grant)
	opened := s.client.createSession(s.returningTo(s.session(config, nil), appReturnURL))

	forked := s.fork(opened.Id, ForkSessionRequest{Messages: pointerTo(false), ConfigId: &config})
	s.ask(forked.Id)

	s.Equal(launchPageFrom(appOrigin), s.launchedFor(connector))
}

// TestADeviceSessionsReturnURLItsAppNoLongerListsIsNotCarriedOn: the device named a listed
// origin, which the app then took out of its policy. The chat carried on from its row goes
// back to the dashboard, as one opened without a return_url does.
func (s *ChatLoginsSuite) TestADeviceSessionsReturnURLItsAppNoLongerListsIsNotCarriedOn() {
	s.devicesMayReturnTo(appOrigin)
	connector, grant := s.connector()
	opened := s.client.createSession(s.returningTo(s.session(s.config(connector, grant), nil), appReturnURL))
	s.Require().Eventually(func() bool { return s.storedReturnURL(opened.Id) == appReturnURL }, settleFor, 20*time.Millisecond,
		"the row keeps the return_url")
	s.client.stopSession(opened.Id)
	s.devicesMayReturnTo()

	s.ask(opened.Id)

	s.Equal(baseLaunchPage, s.launchedFor(connector))
}

// TestAForkOfADeviceSessionDropsAReturnURLItsAppNoLongerLists: whether the parent is live or
// read from its row, the fork carries no return_url the app took out of its policy.
func (s *ChatLoginsSuite) TestAForkOfADeviceSessionDropsAReturnURLItsAppNoLongerLists() {
	for _, ended := range []bool{false, true} {
		s.devicesMayReturnTo(appOrigin)
		connector, grant := s.connector()
		opened := s.client.createSession(s.returningTo(s.session(s.config(connector, grant), nil), appReturnURL))
		s.Require().Eventually(func() bool { return s.storedReturnURL(opened.Id) == appReturnURL }, settleFor, 20*time.Millisecond,
			"the parent's row is written")
		if ended {
			s.client.stopSession(opened.Id)
		}
		s.devicesMayReturnTo()

		forked := s.fork(opened.Id, ForkSessionRequest{Messages: pointerTo(false)})

		s.Eventually(func() bool { return s.storedWithoutReturnURL(forked.Id) }, settleFor, 20*time.Millisecond,
			"ended %v: the fork has no return_url", ended)
	}
}

// TestABackendsReturnURLIsCarriedOnWithoutAnEntry: a session the backend opened goes on to
// its page whatever the policy lists, carried on and forked alike.
func (s *ChatLoginsSuite) TestABackendsReturnURLIsCarriedOnWithoutAnEntry() {
	backend := s.serverClient.actingFor(s.client)
	connector, grant := s.connector()
	opened := backend.createSession(s.returningTo(s.session(s.config(connector, grant), nil), appReturnURL))
	s.Require().Eventually(func() bool { return s.storedReturnURL(opened.Id) == appReturnURL }, settleFor, 20*time.Millisecond,
		"the row keeps the return_url")
	backend.stopSession(opened.Id)

	forked := s.fork(opened.Id, ForkSessionRequest{Messages: pointerTo(false)})
	s.ask(opened.Id)

	s.Equal(launchPageFrom(appOrigin), s.launchedFor(connector))
	s.Eventually(func() bool { return s.storedReturnURL(forked.Id) == appReturnURL }, settleFor, 20*time.Millisecond,
		"the fork's row keeps the backend's return_url")
}

func (s *SessionCreateSuite) TestASessionWithAPlainHTTPReturnURLIsRefusedAndNotCreated() {
	id := s.utils.uuid()
	request := textSession(&id)
	request.ReturnUrl = pointerTo(badReturnURL)

	status, message := s.client.failure(http.MethodPost, "/v1/agents/sessions", request)

	s.Equal(http.StatusBadRequest, status)
	s.Equal("return_url: "+checkReturnURL(badReturnURL).Error(), message)
	s.Equal(http.StatusNotFound, s.client.do(http.MethodGet, "/v1/agents/sessions/"+id, nil, nil))
}

// A start frame is refused the return_url POST /v1/agents/sessions refuses, with the same
// message, code and type.
func (s *SocketSessionSuite) TestASocketSessionWithAPlainHTTPReturnURLIsRefused() {
	sent := map[string]any{"return_url": badReturnURL}
	_, body := s.client.call(http.MethodPost, "/v1/agents/sessions", sent)
	var created ErrorResponse
	s.Require().NoError(json.Unmarshal(body, &created))

	connection := s.client.opens("/v1/agents/socket")
	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "start", "session": sent}))
	refused := s.readFrame(connection)

	s.Equal("error", refused["type"])
	s.Equal("return_url: "+checkReturnURL(badReturnURL).Error(), refused["error"])
	s.Equal(created.Error.Message, refused["error"])
	s.Equal(created.Error.Code, refused["code"])
	s.Equal(string(created.Error.Type), refused["error_type"])
	_, _, err := connection.ReadMessage()
	s.True(websocket.IsCloseError(err, websocket.ClosePolicyViolation), "the socket closes: %v", err)
}

// returningTo is request with returnURL as its return_url.
func (s *ChatLoginsSuite) returningTo(request CreateSessionRequest, returnURL string) CreateSessionRequest {
	request.ReturnUrl = &returnURL
	return request
}

// loggedIn is the consent asked opened and finished in one browser: its launch page with the
// nonce written NONCE, and the callback's Location and body.
func (s *ChatLoginsSuite) loggedIn(asked map[string]any) (string, string, string) {
	b := newBrowser(&s.RouterSuite, s.provider)
	page := launchPageIn(&s.RouterSuite, b.get(asked["launch_url"].(string)))
	finished := b.get(s.consent(b.handOff(s.started(asked))).String())
	defer finished.Body.Close()
	body, err := readAll(finished)
	s.Require().NoError(err)
	return page, finished.Header.Get("Location"), string(body)
}

// assertEndsAsOnBase checks a consent on connection was launched and ended as base b7cea98f's
// did (probe-base-author.txt).
func (s *ChatLoginsSuite) assertEndsAsOnBase(connection, page, location, body string) {
	s.T().Helper()
	s.Equal(baseLaunchPage, page)
	s.Equal("https://dashboard.example/connections?connection_id="+connection+"&status=connected", location)
	s.Equal(`<a href="https://dashboard.example/connections?connection_id=`+connection+`&amp;status=connected">Found</a>.`+"\n\n", body)
}

// backAtTheApp is where the callback sends the browser of a connected consent on connection
// begun with appReturnURL: its own query kept, connection_id and status added.
func (s *ChatLoginsSuite) backAtTheApp(connection string) string {
	return "https://app.example/connectors/done?" + url.Values{
		"connection_id": {connection}, "status": {consentConnected}, "tab": {"tools"}, "from": {"chat"},
	}.Encode()
}

// storedReturnURL is the session row's return_url, empty for NULL or no row yet.
func (s *RouterSuite) storedReturnURL(id string) string {
	var stored sql.NullString
	err := s.store.ShardDB().QueryRowContext(context.Background(),
		"SELECT return_url FROM agent_sessions WHERE id = ?", id).Scan(&stored)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	s.Require().NoError(err)
	return stored.String
}

// storedWithoutReturnURL is whether the session's row exists with return_url NULL.
func (s *ChatLoginsSuite) storedWithoutReturnURL(id string) bool {
	var count int
	err := s.store.ShardDB().QueryRowContext(context.Background(),
		"SELECT count(*) FROM agent_sessions WHERE id = ? AND return_url IS NULL", id).Scan(&count)
	s.Require().NoError(err)
	return count == 1
}

// launchedFor is the launch page, nonce written NONCE, of the one consent the chat began on
// s.client's connection to connector, waiting for it to be begun.
func (s *ChatLoginsSuite) launchedFor(connector string) string {
	var attempt string
	s.Require().Eventually(func() bool {
		return s.store.GlobalDB().QueryRowContext(context.Background(),
			`SELECT a.id FROM connector_authorization_attempts a JOIN connector_connections c ON c.id = a.connection_id
			WHERE c.connector_id = ? AND c.owner_id = ?`, connector, s.client.userID).Scan(&attempt) == nil
	}, settleFor, 20*time.Millisecond, "the chat begins a consent")
	return launchPageIn(&s.RouterSuite, newBrowser(&s.RouterSuite, s.provider).get(consentPublicURL+connectorLaunchPath+attempt))
}

// fork is s.client's backend forking id.
func (s *ChatLoginsSuite) fork(id string, request ForkSessionRequest) Session {
	var forked Session
	s.Require().Equal(http.StatusCreated, s.serverClient.actingFor(s.client).do(http.MethodPost,
		"/v1/agents/sessions/"+id+"/fork", request, &forked))
	return forked
}

// TestADeviceReturnURLOfAnOriginItsAppDoesNotListIsRefusedAndNotCreated: only the listed
// origin itself is taken, not a host that starts with it, another port or another host.
func (s *SessionCreateSuite) TestADeviceReturnURLOfAnOriginItsAppDoesNotListIsRefusedAndNotCreated() {
	s.devicesMayReturnTo(appOrigin, "com.example.app")
	for _, returnURL := range []string{
		"https://elsewhere.example/connectors/done",
		"https://app.example.elsewhere.example/connectors/done",
		"https://app.example:8443/connectors/done",
		"http://localhost:3000/connectors/done",
	} {
		id := s.utils.uuid()
		request := textSession(&id)
		request.ReturnUrl = pointerTo(returnURL)

		status, message := s.client.failure(http.MethodPost, "/v1/agents/sessions", request)

		s.Equal(http.StatusForbidden, status, returnURL)
		s.Equal(errDeviceReturnURL.Message, message, returnURL)
		s.Equal(http.StatusNotFound, s.client.do(http.MethodGet, "/v1/agents/sessions/"+id, nil, nil), returnURL)
	}
}

// TestADeviceReturnURLOfANativeSchemeIsTakenOnlyWhenItsAppListsIt: the scheme must be listed
// itself; an origin or another app's scheme does not admit it.
func (s *SessionCreateSuite) TestADeviceReturnURLOfANativeSchemeIsTakenOnlyWhenItsAppListsIt() {
	const native = "com.example.app:/connected"
	for _, listed := range [][]string{nil, {appOrigin, "com.example.other", "com.example.app.other"}} {
		s.devicesMayReturnTo(listed...)
		request := textSession(nil)
		request.ReturnUrl = pointerTo(native)

		status, message := s.client.failure(http.MethodPost, "/v1/agents/sessions", request)

		s.Equal(http.StatusForbidden, status, listed)
		s.Equal(errDeviceReturnURL.Message, message, listed)
	}

	s.devicesMayReturnTo("com.example.app")
	request := textSession(nil)
	request.ReturnUrl = pointerTo(native)
	opened := s.client.createSession(request)
	s.Eventually(func() bool { return s.storedReturnURL(opened.Id) == native }, settleFor, 20*time.Millisecond)
}

// TestAServerSideReturnURLNeedsNoEntryInThePolicy: the backend decides where its own users'
// consents go back to, whatever the policy lists.
func (s *SessionCreateSuite) TestAServerSideReturnURLNeedsNoEntryInThePolicy() {
	backend := s.serverClient.actingFor(s.client)
	for _, returnURL := range []string{"https://elsewhere.example/connectors/done", "com.example.other:/connected"} {
		request := textSession(nil)
		request.ReturnUrl = pointerTo(returnURL)

		opened := backend.createSession(request)

		s.Eventually(func() bool { return s.storedReturnURL(opened.Id) == returnURL }, settleFor, 20*time.Millisecond, returnURL)
	}
}

// A start frame is refused the device return_url POST /v1/agents/sessions refuses, with the
// same message, code and type.
func (s *SocketSessionSuite) TestASocketSessionWithADeviceReturnURLItsAppDoesNotListIsRefused() {
	sent := map[string]any{"return_url": "https://elsewhere.example/connectors/done"}
	_, body := s.client.call(http.MethodPost, "/v1/agents/sessions", sent)
	var created ErrorResponse
	s.Require().NoError(json.Unmarshal(body, &created))

	connection := s.client.opens("/v1/agents/socket")
	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "start", "session": sent}))
	refused := s.readFrame(connection)

	s.Equal("error", refused["type"])
	s.Equal(errDeviceReturnURL.Message, refused["error"])
	s.Equal(created.Error.Message, refused["error"])
	s.Equal(created.Error.Code, refused["code"])
	s.Equal(string(created.Error.Type), refused["error_type"])
}

// TestASocketSessionWithADeviceReturnURLItsAppListsIsHeld: the start frame is refused only for
// an unlisted page, not for every device's.
func (s *SocketSessionSuite) TestASocketSessionWithADeviceReturnURLItsAppListsIsHeld() {
	s.devicesMayReturnTo(appOrigin)
	connection := s.client.opens("/v1/agents/socket")
	defer connection.Close()
	s.Require().NoError(connection.WriteJSON(map[string]any{
		"type": "start", "sample_rate": 24_000, "session": map[string]any{"return_url": appReturnURL},
	}))

	answered := s.readFrame(connection)

	s.Require().Equal("session", answered["type"], "the socket answered %v", answered)
	id := answered["session"].(map[string]any)["id"].(string)
	s.Eventually(func() bool { return s.storedReturnURL(id) == appReturnURL }, settleFor, 20*time.Millisecond)
}

func (s *PoliciesSuite) TestDeviceReturnURLsAreStoredAndReadBack() {
	listed := []string{"https://app.example", "http://localhost:3000", "com.example.app"}

	stored := s.put(map[string]any{"device_return_urls": listed})

	s.Equal(listed, stored.DeviceReturnURLs)
	var read Policy
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/policies/app", nil, &read))
	s.Equal(listed, read.DeviceReturnURLs)
}

// TestADeviceReturnURLThatIsNotAnOriginOrASchemeIsRefused: an entry is matched exactly, so one
// with a path, a query, a capital, a default port or a wildcard would never match, and plain
// http, a scheme with its colon or path, a browser's own scheme and a dotless name are refused
// as return_url is.
func (s *PoliciesSuite) TestADeviceReturnURLThatIsNotAnOriginOrASchemeIsRefused() {
	s.put(map[string]any{"device_return_urls": []string{"https://app.example"}})
	for _, entry := range []string{
		"https://app.example/connectors/done", "https://app.example?tab=tools", "https://App.example",
		"https://app.example:443", "https://*.example", "*", "http://app.example",
		"com.example.app:", "com.example.app:/connected", "javascript", "app", "",
	} {
		status, _ := s.serverClient.failure(http.MethodPut, "/v1/policies/app", map[string]any{"device_return_urls": []string{entry}})

		s.Equal(http.StatusBadRequest, status, entry)
	}
	var read Policy
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/policies/app", nil, &read))
	s.Equal([]string{"https://app.example"}, read.DeviceReturnURLs, "a refused policy writes nothing")
}

func (s *PoliciesSuite) TestAnOrganizationsPolicyHasNoDeviceReturnURLs() {
	status, message := s.serverClient.failure(http.MethodPut, "/v1/policies/organization",
		map[string]any{"device_return_urls": []string{"https://app.example"}})

	s.Equal(http.StatusBadRequest, status)
	s.Equal("an organization has no device_return_urls: set them on each app's policy", message)
}

// devicesMayReturnTo sets the app's device_return_urls to entries for the test, and puts the
// app's policy back after it.
func (s *RouterSuite) devicesMayReturnTo(entries ...string) {
	var before Policy
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/policies/app", nil, &before))
	policy := before
	policy.DeviceReturnURLs = entries
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/policies/app", policy, nil))
	s.T().Cleanup(func() {
		s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/policies/app", before, nil))
	})
}
