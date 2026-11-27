//go:build integration

package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/activity"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ActivitySuite covers the activity record the router keeps of every request an app makes.
// Every test has an app of its own, whose rows are only what the test asked.
type ActivitySuite struct {
	RouterSuite

	// other is a second node of the deployment, running the sessions a test opens there.
	other *httptest.Server
}

func TestActivitySuite(t *testing.T) {
	runSuite(t, new(ActivitySuite))
}

func (s *ActivitySuite) SetupSuite() {
	s.RouterSuite.SetupSuite()
	s.other = s.otherNode()
}

func (s *ActivitySuite) SetupTest() {
	s.useApp(s.data.createApp())
}

// counted is the app's row for area and client on any day: its requests, its refusals, the last
// refusal's code and its organization, summed over days so a test that runs across midnight
// still reads every request.
type counted struct {
	requests, refused int64
	code, org         string
}

func (s *RouterSuite) counted(area, client string) counted {
	var (
		row       counted
		code, org sql.NullString
	)
	err := s.store.GlobalDB().QueryRowContext(context.Background(), `
SELECT COALESCE(sum(requests), 0), COALESCE(sum(refused), 0),
       (array_agg(last_refusal_code ORDER BY last_refusal_at DESC NULLS LAST))[1], max(organization_id)
FROM customer_activity WHERE app_id = ? AND area = ? AND client = ?`,
		s.customerID(), area, client).Scan(&row.requests, &row.refused, &code, &org)
	s.Require().NoError(err)
	row.code, row.org = code.String, org.String
	return row
}

// countedOnce waits for one request under area and client to be written and returns its row.
func (s *RouterSuite) countedOnce(area, client string) counted {
	var row counted
	s.Require().Eventually(func() bool {
		row = s.counted(area, client)
		return row.requests > 0
	}, settleFor, 10*time.Millisecond, "nothing was counted under %s from %s", area, client)
	s.Equal(int64(1), row.requests)
	return row
}

// errorCode is the code a failure was answered with.
func (s *ActivitySuite) errorCode(payload []byte) string {
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answered), string(payload))
	s.Require().NotEmpty(answered.Error.Code, string(payload))
	return answered.Error.Code
}

// Defect: the area read from the path alone, so the dashboard reading an agent and the CLI
// writing one are one count.
func (s *ActivitySuite) TestAReadAndAWriteOfAnAgentAreCountedUnderTheirAreasAndClients() {
	s.Require().Equal(http.StatusOK, s.serverClient.with(clientHeader, store.AuditSourceDashboard).
		do(http.MethodGet, "/v1/agents/configs", nil, nil))
	status, _ := s.serverClient.with(clientHeader, store.AuditSourceCLI).
		call(http.MethodPost, "/v1/agents/configs", map[string]any{"name": "agent-" + s.utils.uuid()})
	s.Require().Less(status, http.StatusInternalServerError)

	read := s.countedOnce(activity.AreaDashboardReads, store.AuditSourceDashboard)
	s.Equal(s.app.organization.ID, read.org)
	s.Equal(int64(0), read.refused)
	s.countedOnce(activity.AreaAgentSetup, store.AuditSourceCLI)
}

// Defect: every route under /v1/phone counted as phone use whatever the method, so the dashboard
// opening the phone page counted as the organization having reached the phone step.
func (s *ActivitySuite) TestAPhoneReadIsADashboardReadAndAPhoneWriteIsPhoneUse() {
	dashboard := s.serverClient.with(clientHeader, store.AuditSourceDashboard)
	for _, path := range []string{"/v1/phone/sandbox", "/v1/phone/numbers"} {
		s.Require().Equal(http.StatusOK, dashboard.do(http.MethodGet, path, nil, nil), path)
	}
	s.Require().Eventually(func() bool {
		return s.counted(activity.AreaDashboardReads, store.AuditSourceDashboard).requests == 2
	}, settleFor, 10*time.Millisecond, "the phone reads were not counted as dashboard reads")
	s.Equal(int64(0), s.counted(activity.AreaPhoneSMS, store.AuditSourceDashboard).requests)

	// Answered or refused, a write is counted under the area of its route.
	writer := s.serverClient.with(clientHeader, store.AuditSourceSDK)
	writer.call(http.MethodPut, "/v1/phone/sandbox/recipients", map[string]any{"recipients": []string{}})
	writer.call(http.MethodPost, "/v1/phone/numbers", map[string]any{})
	s.Require().Eventually(func() bool {
		return s.counted(activity.AreaPhoneSMS, store.AuditSourceSDK).requests == 2
	}, settleFor, 10*time.Millisecond, "the phone writes were not counted as phone use")
	s.Equal(int64(0), s.counted(activity.AreaDashboardReads, store.AuditSourceSDK).requests)
}

// Defect: a search of the sessions counted as holding a conversation, so a dashboard listing
// sessions moved furthest_step to conversations.
func (s *ActivitySuite) TestASessionSearchIsADashboardReadNotAConversation() {
	s.Require().Equal(http.StatusOK, s.serverClient.with(clientHeader, store.AuditSourceDashboard).
		do(http.MethodPost, "/v1/agents/sessions/query", SessionQuery{}, nil))
	s.Require().Eventually(func() bool {
		return s.counted(activity.AreaDashboardReads, store.AuditSourceDashboard).requests == 1
	}, settleFor, 10*time.Millisecond, "the session search was not counted as a dashboard read")
	s.Equal(int64(0), s.counted(activity.AreaConversations, store.AuditSourceDashboard).requests)
}

// Defect: the client stored as the caller named it, which lets any caller add rows without
// bound; or a known name in another case or with spaces around it read as unknown.
func (s *ActivitySuite) TestAClientNobodyKnowsIsCountedAsTheAPI() {
	s.Require().Equal(http.StatusOK, s.serverClient.with(clientHeader, "anything-else").
		do(http.MethodGet, "/v1/agents/skills", nil, nil))
	s.Require().Equal(http.StatusOK, s.serverClient.with(clientHeader, " CLI ").
		do(http.MethodGet, "/v1/agents/skills", nil, nil))

	s.countedOnce(activity.AreaDashboardReads, store.AuditSourceAPI)
	s.countedOnce(activity.AreaDashboardReads, store.AuditSourceCLI)
	s.Equal(int64(0), s.counted(activity.AreaDashboardReads, "anything-else").requests)
}

// Defect: an SDK that names its release (stream-agents-<language>-v<version>, in any case)
// counted as the API, or plain sdk from an older release no longer counted as the SDK; or
// another Stream product's client, or the product's name with no SDK after it, counted as ours.
func (s *ActivitySuite) TestAnSDKNamingItsReleaseIsCountedAsTheSDK() {
	for _, client := range []string{"stream-agents-go-v1.2.3", "Stream-Agents-JS-v0.8.0-browser", "sdk",
		"stream-chat-js-v8.40.0-browser", "stream-agents"} {
		s.Require().Equal(http.StatusOK, s.serverClient.with(clientHeader, client).
			do(http.MethodGet, "/v1/agents/skills", nil, nil), client)
	}

	s.Require().Eventually(func() bool {
		return s.counted(activity.AreaDashboardReads, store.AuditSourceSDK).requests+
			s.counted(activity.AreaDashboardReads, store.AuditSourceAPI).requests == 5
	}, settleFor, 10*time.Millisecond)
	s.Equal(int64(3), s.counted(activity.AreaDashboardReads, store.AuditSourceSDK).requests)
	s.Equal(int64(2), s.counted(activity.AreaDashboardReads, store.AuditSourceAPI).requests)
}

// Defect: a refusal answered before the request reached its route (withServerSide) counted as
// a success, under no area, or without the code the caller was told.
func (s *ActivitySuite) TestARequestRefusedBeforeItsRouteIsCountedAsRefusedWithItsCode() {
	status, payload := s.client.with(clientHeader, store.AuditSourceDashboard).
		call(http.MethodGet, "/v1/usage/events", nil)
	s.Require().Equal(http.StatusForbidden, status)

	row := s.countedOnce(activity.AreaDashboardReads, store.AuditSourceDashboard)
	s.Equal(int64(1), row.refused)
	s.Equal(codeServerSideOnly, row.code)
	s.Equal(s.errorCode(payload), row.code)
}

// Defect: the code of an APIError an operation returned, which Huma writes itself, left out.
func (s *ActivitySuite) TestARefusalAnOperationReturnedIsCountedWithItsCode() {
	status, payload := s.serverClient.with(clientHeader, store.AuditSourceSDK).
		call(http.MethodGet, "/v1/agents/configs/"+s.utils.uuid(), nil)
	s.Require().Equal(http.StatusNotFound, status)

	row := s.countedOnce(activity.AreaDashboardReads, store.AuditSourceSDK)
	s.Equal(int64(1), row.refused)
	s.Equal(codeAgentConfigNotFound, row.code)
	s.Equal(s.errorCode(payload), row.code)
}

// Defect: a 400 counted as a success, or a request that did not validate, which Huma refuses
// with an error of its own making, counted without its code.
func (s *ActivitySuite) TestARequestThatDidNotValidateIsCountedAsRefused() {
	status, payload := s.serverClient.with(clientHeader, store.AuditSourceSDK).
		call(http.MethodPost, "/v1/agents/configs", map[string]any{"name": 7})
	s.Require().Equal(http.StatusBadRequest, status)

	row := s.countedOnce(activity.AreaAgentSetup, store.AuditSourceSDK)
	s.Equal(int64(1), row.refused)
	s.Equal(codeValidationFailed, row.code)
	s.Equal(s.errorCode(payload), row.code)
}

// Defect: the code of a refusal a Huma middleware wrote (writeOperationError) left out.
func (s *ActivitySuite) TestARefusalAnOperationsMiddlewareWroteIsCountedWithItsCode() {
	status, payload := s.serverClient.with(clientHeader, store.AuditSourceCLI).
		call(http.MethodGet, "/v1/ops/use-cases", nil)
	s.Require().Equal(http.StatusUnauthorized, status)

	row := s.countedOnce(activity.AreaOther, store.AuditSourceCLI)
	s.Equal(int64(1), row.refused)
	s.Equal(s.errorCode(payload), row.code)
}

// Defect: a successful health probe an app made with its own credential left out of the activity
// record, since its access line is skipped (answeredHealthy) and the record was written after it.
func (s *ActivitySuite) TestASuccessfulHealthProbeIsStillCounted() {
	s.Require().Equal(http.StatusOK, s.serverClient.with(clientHeader, store.AuditSourceCLI).
		do(http.MethodGet, "/health", nil, nil))

	s.countedOnce(activity.AreaOther, store.AuditSourceCLI)
}

// Defect: a request nobody authenticated counted under an empty app, which is everybody's.
func (s *ActivitySuite) TestARequestNobodyAuthenticatedIsNotCounted() {
	asked := time.Now()
	s.Require().Equal(http.StatusUnauthorized, s.unauthenticatedClient.with(clientHeader, store.AuditSourceDashboard).
		do(http.MethodGet, "/v1/agents/configs", nil, nil))
	// Behind it, so its write is the latest a nobody's row could have been written beside.
	s.Require().Equal(http.StatusOK, s.serverClient.with(clientHeader, store.AuditSourceDashboard).
		do(http.MethodGet, "/v1/agents/configs", nil, nil))
	s.countedOnce(activity.AreaDashboardReads, store.AuditSourceDashboard)

	// Since the test began: suites run beside each other and count their own requests.
	var nobody int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(context.Background(),
		"SELECT count(*) FROM customer_activity WHERE app_id = '' AND last_at >= ?", asked).Scan(&nobody))
	s.Equal(0, nobody)
}

// Defect: a request about a session that landed on a node not running it counted by neither
// node (as withMetrics leaves a forwarded request to the node it landed on), or by both.
func (s *ActivitySuite) TestARequestHandedToTheNodeRunningItsSessionIsCountedOnce() {
	backend := s.serverClient.with(clientHeader, store.AuditSourceSDK)
	opened := backend.on(s.other).createSession(CreateSessionRequest{Incognito: pointerTo(true), Llm: pointerTo("llm-flow")})

	read := backend.getSession(opened.Id)
	s.Require().Equal(opened.Id, read.Id)

	s.countedOnce(activity.AreaConversations, store.AuditSourceSDK)
	s.Require().Eventually(func() bool {
		return s.counted(activity.AreaDashboardReads, store.AuditSourceSDK).requests >= 1
	}, settleFor, 10*time.Millisecond, "the forwarded read was not counted")
	s.Never(func() bool { return s.counted(activity.AreaDashboardReads, store.AuditSourceSDK).requests > 1 },
		10*suiteActivityInterval, suiteActivityInterval, "a forwarded read was counted twice")
}

// Defect: a request counted at the time its answer ended, so its first and last time are after
// the work it asked for and a slow request lands on the next day at midnight.
func (s *ActivitySuite) TestARequestIsCountedWhenItArrived() {
	recorder, err := activity.New(activity.Options{Store: s.store, Interval: time.Hour, Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)
	var began time.Time
	routes := chi.NewRouter()
	routes.Get("/v1/agents/skills", func(w http.ResponseWriter, _ *http.Request) {
		began = time.Now()
		for time.Since(began) < 5*time.Millisecond {
		}
		w.WriteHeader(http.StatusOK)
	})
	request := httptest.NewRequest(http.MethodGet, "/v1/agents/skills", nil)
	request = request.WithContext(context.WithValue(request.Context(), customerContextKey{}, s.customerID()))
	(&Server{logger: slog.New(slog.DiscardHandler), activity: recorder}).withRequestLog(routes, routes).ServeHTTP(httptest.NewRecorder(), request)
	recorder.Close()

	var first time.Time
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(context.Background(),
		"SELECT first_at FROM customer_activity WHERE app_id = ?", s.customerID()).Scan(&first))
	s.True(first.Before(began), "first_at %s, handler began %s", first, began)
}

// refusedSocket opens the session socket from the SDK, sends start and returns the error frame
// it was refused with.
func (s *ActivitySuite) refusedSocket(start map[string]any) map[string]any {
	connection := s.serverClient.with(clientHeader, store.AuditSourceSDK).opens("/v1/agents/socket")
	s.Require().NoError(connection.WriteJSON(start))
	return s.await(connection, "error")
}

// browserSocket opens the session socket as a browser does, with no X-Stream-Client header and
// the client named in the query, and waits for the error frame a hello is refused with, so the
// socket has been served by the time the test reads its row.
func (s *ActivitySuite) browserSocket(client string) {
	connection := s.client.with(clientHeader, "").opens("/v1/agents/socket?X-Stream-Client=" + client)
	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "hello"}))
	s.await(connection, "error")
}

// Defect: a browser's socket, which cannot send headers, counted as the API although the SDK
// named itself in the query.
func (s *ActivitySuite) TestABrowserSocketNamingTheSDKInItsQueryIsCountedAsTheSDK() {
	s.browserSocket("stream-agents-js-v0.9.0-browser")

	s.countedOnce(activity.AreaConversations, store.AuditSourceSDK)
	s.Equal(int64(0), s.counted(activity.AreaConversations, store.AuditSourceAPI).requests)
}

// Defect: the query read for the path asked, not the route pattern served, so a socket whose
// path holds an id (a session's events) stayed counted as the API.
func (s *ActivitySuite) TestASessionEventsSocketNamingTheSDKInItsQueryIsCountedAsTheSDK() {
	status, _ := s.client.with(clientHeader, "").call(http.MethodGet,
		"/v1/agents/sessions/sess-nobody/events?X-Stream-Client=stream-agents-js-v0.9.0-browser", nil)
	s.Require().Less(status, http.StatusInternalServerError)

	s.countedOnce(activity.AreaConversations, store.AuditSourceSDK)
	s.Equal(int64(0), s.counted(activity.AreaConversations, store.AuditSourceAPI).requests)
}

// Defect: a model stream's query not read, so a browser's /v1/{modality}/stream stayed counted as
// the API.
func (s *ActivitySuite) TestAModelStreamSocketNamingTheSDKInItsQueryIsCountedAsTheSDK() {
	status, _ := s.client.with(clientHeader, "").call(http.MethodGet,
		"/v1/stt/stream?X-Stream-Client=stream-agents-js-v0.9.0-browser", nil)
	s.Require().Less(status, http.StatusInternalServerError)

	s.countedOnce(activity.AreaModelAPI, store.AuditSourceSDK)
	s.Equal(int64(0), s.counted(activity.AreaModelAPI, store.AuditSourceAPI).requests)
}

// Defect: a client named in a socket's query stored as sent, or a name nobody knows counted as
// some client rather than the API.
func (s *ActivitySuite) TestABrowserSocketNamingAClientNobodyKnowsIsCountedAsTheAPI() {
	s.browserSocket("anything-else")

	s.countedOnce(activity.AreaConversations, store.AuditSourceAPI)
	s.Equal(int64(0), s.counted(activity.AreaConversations, "anything-else").requests)
}

// Defect: a socket refused in a frame, after its 101, counted as answered.
func (s *ActivitySuite) TestASocketWhoseFirstFrameIsNoStartIsCountedAsRefused() {
	refusal := s.refusedSocket(map[string]any{"type": "hello"})
	s.NotContains(refusal, "code", "the frame says what it always said")

	row := s.countedOnce(activity.AreaConversations, store.AuditSourceSDK)
	s.Equal(int64(1), row.refused)
	s.Equal(invalidRequest("").Code, row.code)
}

// Defect: a socket refused for an agent config it named counted with no code, or the code of
// some other failure than the one POST /v1/agents/sessions answers.
func (s *ActivitySuite) TestASocketNamingAConfigNobodyHoldsIsCountedWithItsCode() {
	s.refusedSocket(map[string]any{"type": "start", "session": map[string]any{"config_id": s.utils.uuid()}})

	row := s.countedOnce(activity.AreaConversations, store.AuditSourceSDK)
	s.Equal(int64(1), row.refused)
	s.Equal(codeAgentConfigNotFound, row.code)
}

// Defect: a socket naming a session that is already open counted with the code of a request the
// caller got wrong, not the conflict POST /v1/agents/sessions answers it with.
func (s *ActivitySuite) TestASocketNamingAnOpenSessionIsCountedAsAConflict() {
	opened := s.serverClient.with(clientHeader, store.AuditSourceCLI).
		createSession(CreateSessionRequest{Incognito: pointerTo(true), Llm: pointerTo("llm-flow")})

	s.refusedSocket(map[string]any{"type": "start", "session": map[string]any{"id": opened.Id}})

	row := s.countedOnce(activity.AreaConversations, store.AuditSourceSDK)
	s.Equal(int64(1), row.refused)
	s.Equal(conflict("").Code, row.code)
}

// Defect: a socket whose session would not start counted with no code.
func (s *ActivitySuite) TestASocketWhoseSessionWouldNotStartIsCountedAsAnInvalidRequest() {
	s.refusedSocket(map[string]any{"type": "start", "session": map[string]any{"llm": "nobody/no-model"}})

	row := s.countedOnce(activity.AreaConversations, store.AuditSourceSDK)
	s.Equal(int64(1), row.refused)
	s.Equal(invalidRequest("").Code, row.code)
}
