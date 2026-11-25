//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
)

// SelfHostedSessionsSuite opens sessions on a router in deployment mode, as a self-hosted one
// runs, whose own Stream app is missing or refuses its keys. The router keeps no history of
// its own, so a session that is not incognito needs that app (AI-3566).
type SelfHostedSessionsSuite struct {
	RouterSuite
	// logged is what the router logged, where the operator reads why a session was refused.
	logged *lockedLog
}

func TestSelfHostedSessionsSuite(t *testing.T) {
	runSuite(t, new(SelfHostedSessionsSuite))
}

func (s *SelfHostedSessionsSuite) SetupSuite() {
	s.logged = &lockedLog{}
	s.logs = s.logged
	s.RouterSuite.SetupSuite()
}

func (s *SelfHostedSessionsSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

// errorOf makes a request and returns the failure it was answered with, and the raw body.
func (s *SelfHostedSessionsSuite) errorOf(method, path string, body any) (int, ErrorDetail, string) {
	status, payload := s.serverClient.call(method, path, body)
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answered), string(payload))
	return status, answered.Error, string(payload)
}

func (s *SelfHostedSessionsSuite) TestASessionOnKeysStreamRefusesNamesOnlyIncognito() {
	// It used to answer 400 «QueryUsers failed with error: "api_key not valid"», naming neither.
	s.refuseTheKeys()

	status, failure, body := s.errorOf(http.MethodPost, "/v1/agents/sessions", textSession(nil))

	s.Equal(http.StatusBadRequest, status)
	s.Equal(codeNotConfigured, failure.Code)
	s.Equal(errUnkept.Message, failure.Message, "the same words as for no Stream app: the keys are the operator's")
	s.NotContains(body, "api_key not valid", "Stream's own words stay in the log")
}

func (s *SelfHostedSessionsSuite) TestARefusalForTheKeysIsLoggedUnderItsRequestID() {
	s.refuseTheKeys()

	status, _ := s.serverClient.withRequestID("refused-keys").call(http.MethodPost, "/v1/agents/sessions", textSession(nil))

	s.Equal(http.StatusBadRequest, status)
	logged := lineWith(s.logged, "level=WARN", "request_id=refused-keys")
	s.Contains(logged, "STREAM_API_KEY", "the operator reads which setting to correct")
	s.Contains(logged, "api_key not valid", "and what Stream answered")
}

func (s *SelfHostedSessionsSuite) TestASessionTheDeploymentIsNotSetUpForIsAnsweredInFixedWords() {
	// The suite routes no speech-to-speech model.
	native := textSession(nil)
	native.Sts = pointerTo("stub/stub-sts")

	status, failure := s.serverClient.withRequestID("refused-sts").failure(http.MethodPost, "/v1/agents/sessions", native)

	s.Equal(http.StatusBadRequest, status)
	s.Equal(errSessionNotConfigured.Message, failure)
	s.Contains(lineWith(s.logged, "level=WARN", "request_id=refused-sts"), "routes no speech-to-speech model",
		"the operator reads what the deployment lacks under the request id")
}

func (s *SelfHostedSessionsSuite) TestAForkOnKeysStreamRefusesNamesOnlyIncognito() {
	// A fork is refused through refusedRequest, which answered this refusal as a 500 saying
	// only that something went wrong.
	parent := s.serverClient.createSession(textSession(nil))
	s.refuseTheKeys()

	status, failure, body := s.errorOf(http.MethodPost, "/v1/agents/sessions/"+parent.Id+"/fork",
		ForkSessionRequest{Messages: pointerTo(false)})

	s.Equal(http.StatusBadRequest, status, body)
	s.Equal("not_configured", failure.Code, "the same refusal as a create's")
	s.Equal(errUnkept.Message, failure.Message)
	s.NotContains(body, "api_key not valid", "Stream's own words stay in the log")
}

// refuseTheKeys has the deployment's app answer every request as Stream answers a key it does
// not know, so it does not matter which one a session makes first.
func (s *SelfHostedSessionsSuite) refuseTheKeys() {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		s.chat.RefuseWith(method, "", chattest.Refusal{Status: http.StatusUnauthorized, Code: 2,
			Message: `QueryUsers failed with error: "api_key not valid"`})
		s.T().Cleanup(func() { s.chat.Heal(method, "") })
	}
}

func (s *SelfHostedSessionsSuite) TestASessionOnADeploymentWithNoStreamAppNamesOnlyIncognito() {
	// It used to answer 400 «streamapp: no Stream app is configured for this customer».
	s.setApps(s.customerID(), func(apps *suiteApps) { apps.keyless[s.customerID()] = true })
	s.T().Cleanup(func() { s.setApps(s.customerID(), func(apps *suiteApps) { delete(apps.keyless, s.customerID()) }) })

	status, failure, body := s.errorOf(http.MethodPost, "/v1/agents/sessions", textSession(nil))

	s.Equal(http.StatusBadRequest, status)
	s.Equal(codeNotConfigured, failure.Code)
	s.Equal(errUnkept.Message, failure.Message)
	s.NotContains(body, "streamapp:", "the internal error stays in the log")
}

func (s *SelfHostedSessionsSuite) TestAnIncognitoSessionNeedsNoStreamApp() {
	s.setApps(s.customerID(), func(apps *suiteApps) { apps.keyless[s.customerID()] = true })
	s.T().Cleanup(func() { s.setApps(s.customerID(), func(apps *suiteApps) { delete(apps.keyless, s.customerID()) }) })
	incognito := textSession(nil)
	incognito.Incognito = pointerTo(true)

	created := s.serverClient.createSession(incognito)

	s.NotEmpty(created.Id)
}
