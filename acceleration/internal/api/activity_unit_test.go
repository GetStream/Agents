package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/activity"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/channels"
)

// AreaSuite covers which area of the API a route is counted under (areaOf).
type AreaSuite struct {
	suite.Suite
}

func TestAreaSuite(t *testing.T) {
	suite.Run(t, new(AreaSuite))
}

// Defect: the method ignored, so the dashboard reading the agents and the CLI syncing them are
// one area.
func (s *AreaSuite) TestAWriteOfTheAgentConfigsIsAgentSetupAndAReadIsADashboardRead() {
	for _, method := range []string{http.MethodPost, http.MethodPatch, http.MethodPut, http.MethodDelete} {
		s.Equal(activity.AreaAgentSetup, areaOf(method, "/v1/agents/configs"), method)
		s.Equal(activity.AreaAgentSetup, areaOf(method, "/v1/agents/configs/{id}"), method)
	}
	s.Equal(activity.AreaDashboardReads, areaOf(http.MethodGet, "/v1/agents/configs"))
	s.Equal(activity.AreaDashboardReads, areaOf(http.MethodGet, "/v1/agents/configs/{id}"))
}

// Defect: a resource's prefix read before the hook under it, so a vendor's deliveries count as
// the customer setting up that resource.
func (s *AreaSuite) TestAHookUnderAResourcesPathIsAnInboundHook() {
	s.Equal(activity.AreaHooksIn, areaOf(http.MethodPost, "/v1/agents/channels/hooks/{token}"))
	s.Equal(activity.AreaHooksIn, areaOf(http.MethodPost, "/v1/agents/connectors/events/{connector_id}"))
	s.Equal(activity.AreaHooksIn, areaOf(http.MethodPost, "/v1/phone/hooks/telnyx"))
	s.Equal(activity.AreaHooksIn, areaOf(http.MethodPost, "/v1/phone/status/{token}"))
	s.Equal(activity.AreaHooksIn, areaOf(http.MethodPost, "/v1/phone/machine/{token}"))
	s.Equal(activity.AreaHooksIn, areaOf(http.MethodGet, "/v1/agents/channels/hooks/{token}"))
	s.Equal(activity.AreaHooksIn, areaOf(http.MethodGet, "/v1/phone/answer/{token}"))
	s.Equal(activity.AreaConnectors, areaOf(http.MethodGet, "/v1/agents/connectors/{id}"))
}

// Defect: a read of the phone settings counted as phone use, so an organization that only opened
// the phone page was said to have reached the last step.
func (s *AreaSuite) TestAReadOfThePhoneIsADashboardReadAndAWriteOrACallIsPhoneUse() {
	for _, pattern := range []string{"/v1/phone/sandbox", "/v1/phone/numbers", "/v1/phone/numbers/available",
		"/v1/phone/vendors", "/v1/phone/trunks/{id}", "/v1/phone/use-cases/{id}/reviews",
		"/v1/phone/opt-outs", "/v1/phone/business-profile"} {
		s.Equal(activity.AreaDashboardReads, areaOf(http.MethodGet, pattern), "GET %s", pattern)
	}
	for method, pattern := range map[string]string{
		http.MethodPut:    "/v1/phone/sandbox/recipients",
		http.MethodPost:   "/v1/phone/numbers",
		http.MethodDelete: "/v1/phone/numbers/{e164}",
		http.MethodPatch:  "/v1/phone/trunks/{id}",
	} {
		s.Equal(activity.AreaPhoneSMS, areaOf(method, pattern), "%s %s", method, pattern)
	}
	s.Equal(activity.AreaPhoneSMS, areaOf(http.MethodPost, "/v1/phone/calls"))
	s.Equal(activity.AreaPhoneSMS, areaOf(http.MethodPost, "/v1/phone/calls/transfer"))
	s.Equal(activity.AreaPhoneSMS, areaOf(http.MethodPost, "/v1/agents/channels"))
	s.Equal(activity.AreaPhoneSMS, areaOf(http.MethodPost, "/v1/agents/campaigns/{id}/launch"))
}

// Defect: a read of any other area that is a step counted as use of it, as the phone's did.
func (s *AreaSuite) TestAReadOfAStepIsADashboardReadAndAWriteIsUseOfIt() {
	for pattern, use := range map[string]string{
		"/v1/agents/skills/{id}":                  activity.AreaAgentSetup,
		"/v1/agents/knowledge/urls":               activity.AreaAgentSetup,
		"/v1/{modality}/routes":                   activity.AreaAgentSetup,
		"/v1/agents/sessions/{id}":                activity.AreaConversations,
		"/v1/agents/conversations/{cid}/messages": activity.AreaConversations,
		"/v1/agents/calls/{id}/transcript":        activity.AreaVoiceVideo,
		"/v1/agents/campaigns":                    activity.AreaPhoneSMS,
		"/v1/agents/channels":                     activity.AreaPhoneSMS,
	} {
		s.Equal(activity.AreaDashboardReads, areaOf(http.MethodGet, pattern), "GET %s", pattern)
		s.Equal(use, areaOf(http.MethodPost, pattern), "POST %s", pattern)
	}
	s.Equal(activity.AreaWorkers, areaOf(http.MethodPost, "/v1/agents/runs"))
	s.Equal(activity.AreaVoiceVideo, areaOf(http.MethodPost, "/v1/agents/calls/{id}/token"))
}

// Defect: a search of the sessions, a POST that only reads, counted as holding a conversation, so
// a dashboard listing sessions moved furthest_step to conversations.
func (s *AreaSuite) TestASearchOfTheSessionsIsADashboardReadAndAnyOtherSessionPostIsAConversation() {
	s.Equal(activity.AreaDashboardReads, areaOf(http.MethodPost, "/v1/agents/sessions/query"))
	s.Equal(activity.AreaConversations, areaOf(http.MethodPost, "/v1/agents/sessions"))
	s.Equal(activity.AreaConversations, areaOf(http.MethodPost, "/v1/agents/sessions/{id}/say"))
}

// Defect: a GET that opens a socket counted as a read, so a conversation held or a worker
// taking calls over a socket was no use of its area.
func (s *AreaSuite) TestAGetThatOpensASocketIsUseOfItsArea() {
	s.Equal(activity.AreaConversations, areaOf(http.MethodGet, "/v1/agents/socket"))
	s.Equal(activity.AreaConversations, areaOf(http.MethodGet, "/v1/agents/sessions/{id}/events"))
	s.Equal(activity.AreaWorkers, areaOf(http.MethodGet, "/v1/dispatch"))
}

// Defect: a read of an area that is no step moved to the dashboard's, so the area's own count
// was lost.
func (s *AreaSuite) TestAReadOfAnAreaThatIsNoStepIsCountedUnderIt() {
	s.Equal(activity.AreaModelAPI, areaOf(http.MethodGet, "/v1/{modality}/stream"))
	s.Equal(activity.AreaConnectors, areaOf(http.MethodGet, "/v1/agents/connectors"))
	s.Equal(activity.AreaScheduling, areaOf(http.MethodGet, "/v1/agents/scheduled-tasks"))
	s.Equal(activity.AreaEvals, areaOf(http.MethodGet, "/v1/agents/simulations"))
	s.Equal(activity.AreaData, areaOf(http.MethodGet, "/v1/data/export"))
}

// Defect: a prefix matched inside a path segment, so a route merely beginning with the same
// letters is counted under another area.
func (s *AreaSuite) TestAPrefixCoversWholePathSegmentsOnly() {
	s.Equal(activity.AreaOther, areaOf(http.MethodGet, "/v1/agents/sessionsx"))
	s.Equal(activity.AreaOther, areaOf(http.MethodGet, "/v1/agents/configsx"))
	s.Equal(activity.AreaConversations, areaOf(http.MethodPost, "/v1/agents/sessions"))
	s.Equal(activity.AreaConversations, areaOf(http.MethodDelete, "/v1/agents/sessions/{id}"))
}

// Defect: a route no area names, or a path nothing serves, dropped or counted under an area.
func (s *AreaSuite) TestARouteNoAreaNamesIsOther() {
	s.Equal(activity.AreaOther, areaOf(http.MethodGet, "/v1/manage/status"))
	s.Equal(activity.AreaOther, areaOf(http.MethodGet, unmatchedRoute))
}

// staffOrPublic are the routes no app calls under its own credential: Stream's staff's, and
// what anybody may read.
var staffOrPublic = []string{"/v1/manage", "/v1/ops", "/health", "/.well-known"}

// Defect: an area's prefix misspelt, so the routes it was meant to cover are counted as other.
// Every operation in the spec an app calls has an area.
func (s *AreaSuite) TestEveryOperationAnAppCallsHasAnArea() {
	document := (&Server{}).newAPI(chi.NewRouter()).OpenAPI()
	s.Require().NotEmpty(document.Paths)
	for path, item := range document.Paths {
		if slices.ContainsFunc(staffOrPublic, func(prefix string) bool { return strings.HasPrefix(path, prefix) }) {
			continue
		}
		for method, operation := range map[string]*huma.Operation{
			http.MethodGet: item.Get, http.MethodPost: item.Post, http.MethodPut: item.Put,
			http.MethodPatch: item.Patch, http.MethodDelete: item.Delete,
		} {
			if operation != nil {
				s.NotEqual(activity.AreaOther, areaOf(method, path), "%s %s", method, path)
			}
		}
	}
}

// RefusalCodeSuite covers the code a socket session's refusal is counted with (refusalCode).
type RefusalCodeSuite struct {
	suite.Suite
}

func TestRefusalCodeSuite(t *testing.T) {
	suite.Run(t, new(RefusalCodeSuite))
}

// Defect: a store failure, which is no APIError, counted with no code, so the activity record
// shows the socket answered rather than refused.
func (s *RefusalCodeSuite) TestAFailureThatIsNoAPIErrorIsCountedAsAnInternalError() {
	s.Equal(internalError().Code, refusalCode(errors.New("store: connection refused")))
}

// Defect: an APIError, wrapped on its way up or not, counted with another code than the one an
// API route answers it with.
func (s *RefusalCodeSuite) TestAnAPIErrorIsCountedWithItsOwnCode() {
	s.Equal(errUnknownConfig.Code, refusalCode(errUnknownConfig))
	s.Equal(errUnknownConfig.Code, refusalCode(fmt.Errorf("config: %w", errUnknownConfig)))
}

// AnsweredCodeSuite covers the code noted for a failure answered outside the error envelope
// (noteAnswered), which the access line names as error_code and the activity record counts.
type AnsweredCodeSuite struct {
	suite.Suite
}

func TestAnsweredCodeSuite(t *testing.T) {
	suite.Run(t, new(AnsweredCodeSuite))
}

// Defect: a channel's refusal, answered in the provider's shape, noted with no code or another
// one than its status's; or the 400 boundary left out.
func (s *AnsweredCodeSuite) TestAChannelsRefusalIsNotedWithTheCodeOfItsStatus() {
	for status, code := range map[int]string{
		http.StatusBadRequest:          "invalid_request",
		http.StatusUnauthorized:        "unauthenticated",
		http.StatusForbidden:           "forbidden",
		http.StatusGone:                "gone",
		http.StatusInternalServerError: "internal_error",
	} {
		answered := &loggedResponse{ResponseWriter: httptest.NewRecorder()}

		writeChannelAnswer(answered, channels.Answer{Status: status, Body: map[string]string{"error": "probe"}})

		s.Equal(code, answered.refusal, status)
		s.Equal(status, answered.status(), status)
	}
}

// Defect: a delivery taken or a challenge echoed noted as a refusal.
func (s *AnsweredCodeSuite) TestAChannelsSuccessIsNotedWithNoCode() {
	for _, answer := range []channels.Answer{
		{Status: http.StatusAccepted, Body: map[string]string{}},
		{Status: http.StatusOK, Text: "challenge"},
	} {
		answered := &loggedResponse{ResponseWriter: httptest.NewRecorder()}

		writeChannelAnswer(answered, answer)

		s.Empty(answered.refusal, answer.Status)
	}
}

// Defect: a handshake gorilla refuses noted with no code, or answered otherwise than gorilla
// answers it by default (its body and Sec-Websocket-Version).
func (s *AnsweredCodeSuite) TestARefusedHandshakeIsNotedWithTheCodeOfItsStatusAndAnsweredAsBefore() {
	for _, refused := range []struct {
		mode   auth.Mode
		origin string
		status int
		code   string
	}{
		// Not an upgrade at all.
		{auth.APIKey, "", http.StatusBadRequest, "invalid_request"},
		// A page this machine did not serve, which NoAuth refuses (originAllowed).
		{auth.NoAuth, "https://elsewhere.example", http.StatusForbidden, "forbidden"},
	} {
		recorder := httptest.NewRecorder()
		answered := &loggedResponse{ResponseWriter: recorder}
		request := httptest.NewRequest(http.MethodGet, "/v1/agents/socket", nil)
		if refused.origin != "" {
			request.Header.Set("Origin", refused.origin)
			request.Header.Set("Connection", "Upgrade")
			request.Header.Set("Upgrade", "websocket")
			request.Header.Set("Sec-Websocket-Version", "13")
			request.Header.Set("Sec-Websocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		}
		upgrader := newUpgrader(refused.mode)

		_, err := upgrader.Upgrade(answered, request, nil)

		s.Require().Error(err)
		s.Equal(refused.code, answered.refusal, refused.status)
		s.Equal(refused.status, recorder.Code)
		s.Equal(http.StatusText(refused.status)+"\n", recorder.Body.String())
		s.Equal("13", recorder.Header().Get("Sec-Websocket-Version"))
	}
}
