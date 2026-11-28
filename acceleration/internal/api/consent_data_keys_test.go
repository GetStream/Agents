//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/fakeprovider"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/oauth2code"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/slackapps"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// The two record labels, written out so a test fails when a rename would leave dashboards
// without their series.
const (
	attemptKind     = "connector_authorization_attempts"
	configTokenKind = "connector_config_tokens"
)

// ConsentDataKeysSuite is an authorization attempt and a configuration token under the app's
// data key: a consent started, handed off and called back, and a configuration token rotated
// and used. Each server of a test is one router, and a router built with new data keys holds
// none of them yet, as another pod would. It does not run in parallel, so the counters it
// reads move only with its own tests.
type ConsentDataKeysSuite struct {
	suite.Suite
	db        *store.Store
	sealer    *auth.Sealer
	provider  *fakeprovider.Server
	registry  core.Registry
	slackApps *slackapps.Client
	kms       *appkeystest.KMS
	app       string
	// connection is the test's app-owned connection to a custom connector at the fake provider.
	connection store.ConnectorConnection
	// servedAtStart is whether both counters were served before any test of the suite counted.
	servedAtStart bool
	// logged is what the test's routers logged.
	logged *lockedLog
}

func TestConsentDataKeysSuite(t *testing.T) {
	suite.Run(t, new(ConsentDataKeysSuite))
}

func (s *ConsentDataKeysSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	s.servedAtStart = appkeystest.Served(s.T(), attemptKind) && appkeystest.Served(s.T(), configTokenKind)
	db, err := store.Open(testenv.Database(dsn, "api_consentkeys"))
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })
	s.Require().NoError(db.Migrate(s.T().Context()))
	s.db = db
	s.sealer, err = auth.NewSealerWithKeyring(1, map[int]string{1: "consent data keys test key"})
	s.Require().NoError(err)
	s.provider = fakeprovider.New(s.T(), fakeprovider.CommaScopes)
	s.provider.AllowRedirect(consentPublicURL + ConnectorCallbackPath)
	scheme, err := oauth2code.New(oauth2code.Config{HTTP: s.provider.Client(),
		Clients: func(context.Context, core.ConnectionRef, core.ResolvedManifest, core.ClientRegistrationMethod) (oauth2code.Client, bool, error) {
			return oauth2code.Client{ID: s.provider.ClientID, Secret: s.provider.ClientSecret}, true, nil
		},
		PublicEndpoint: loopbackOrPublic})
	s.Require().NoError(err)
	s.registry = core.Registry{Schemes: map[string]core.Scheme{oauth2code.Name: scheme}}
	s.slackApps, err = slackapps.New(slackapps.Config{HTTP: s.provider.Client(), BaseURL: s.provider.URL + fakeprovider.PathSlackAPI})
	s.Require().NoError(err)
}

func (s *ConsentDataKeysSuite) SetupTest() {
	s.kms = appkeystest.NewKMS()
	s.logged = &lockedLog{}
	s.app = "app-" + rand.Text()
	connector := store.CustomPrefix + "consentkeys" + strings.ToLower(rand.Text())
	manifest, err := core.ParseManifest([]byte(`
id: ` + connector + `
revision: 1
name: Fake
endpoints:
  authorize: ` + s.provider.URL + fakeprovider.PathAuthorize + `
  token: ` + s.provider.URL + fakeprovider.PathToken + `
  mcp: ` + s.provider.URL + fakeprovider.PathMCP + `
schemes: [oauth2_code]
client:
  registration: [customer]
  auth_method: client_secret_post
scopes:
  list: [read]
sources:
  - kind: mcp
    endpoint: mcp
`))
	s.Require().NoError(err)
	_, err = s.db.ConnectorDefinitions.Revise(s.T().Context(), s.app, manifest)
	s.Require().NoError(err)
	s.connection = store.ConnectorConnection{CustomerID: s.app, ConnectorID: connector, DefinitionRevision: 1,
		OwnerType: store.OwnerApp, AuthScheme: oauth2code.Name}
	s.Require().NoError(s.db.ConnectorConnections.Insert(s.T().Context(), s.registry, &s.connection))
}

func (s *ConsentDataKeysSuite) TestBothRecordsAreCountedFromTheStart() {
	s.True(s.servedAtStart)
}

// AC-6: a consent sealed under the app's data key at its start and at its handoff reaches the
// provider at its callback, which refuses the made-up code, so the browser gets the failed
// redirect. A data-key attempt that did not open would be answered 400 instead.
func (s *ConsentDataKeysSuite) TestAConsentUnderTheDataKeyEndsInTheFailedRedirectForAMadeUpCode() {
	router := s.server(s.keys(true))
	opens := appkeystest.KEKOpens(s.T(), attemptKind)

	started := s.start(router)
	row := s.attempt(started.ID)
	s.Equal(new(1), row.AttemptDataKeyVersion)
	s.Equal(1, row.KEKVersion, "the table's CHECK needs a key version beside a sealed attempt")
	s.Error(s.openUnderRoutersKey(row), "the router's key does not open it")
	state, cookies := s.handOff(router, started)
	row = s.attempt(started.ID)
	s.Equal(new(1), row.AttemptDataKeyVersion)
	s.Error(s.openUnderRoutersKey(row))
	answer := s.callback(router, state, cookies)

	s.Equal(http.StatusFound, answer.Code, answer.Body.String())
	s.Equal(s.failedLanding(), answer.Header().Get("Location"))
	s.Equal(opens, appkeystest.KEKOpens(s.T(), attemptKind), "every open was under the data key")
}

// A router that holds none of the app's data keys while KMS is down cannot open the attempt.
// The browser gets the failed redirect, not the 400 of an unknown consent, and the attempt is
// left open: the router that holds the key still finishes it.
func (s *ConsentDataKeysSuite) TestAKeyFaultAtTheCallbackEndsInTheFailedRedirectAndConsumesNothing() {
	warm := s.server(s.keys(true))
	started := s.start(warm)
	state, cookies := s.handOff(warm, started)
	outage := "kms: the test's outage " + rand.Text()
	s.kms.Fail(errors.New(outage))

	for name, router := range map[string]*Server{"KMS down": s.server(s.keys(true)), "no data keys": s.server(nil)} {
		answer := s.callback(router, state, cookies)
		s.Equal(http.StatusFound, answer.Code, "%s: %s", name, answer.Body.String())
		s.Equal(s.failedLanding(), answer.Header().Get("Location"), name)
		s.NotContains(answer.Body.String(), outage, name)
	}
	s.Contains(s.logged.String(), "the key that protects", "the request log has the cause")

	_, err := s.db.ConnectorAuthorizationAttempts.GetByState(s.T().Context(), state)
	s.Require().NoError(err, "the attempt is still open")
	answer := s.callback(warm, state, cookies)
	s.Equal(http.StatusFound, answer.Code)
	_, err = s.db.ConnectorAuthorizationAttempts.GetByState(s.T().Context(), state)
	s.ErrorIs(err, store.ErrNoAuthorizationAttempt, "the router that holds the key consumed it")
}

// The launch page and the handoff answer a key fault with fixed text, so neither the KMS error
// nor a setting name reaches the browser, and the handoff writes nothing.
func (s *ConsentDataKeysSuite) TestAKeyFaultAtTheLaunchPageOrTheHandoffIsA503InFixedWords() {
	started := s.start(s.server(s.keys(true)))
	before := s.attempt(started.ID)
	outage := "kms: the test's outage " + rand.Text()
	s.kms.Fail(errors.New(outage))

	for name, router := range map[string]*Server{"KMS down": s.server(s.keys(true)), "no data keys": s.server(nil)} {
		page := httptest.NewRecorder()
		router.Handler().ServeHTTP(page, httptest.NewRequest(http.MethodGet, started.LaunchURL, nil))
		handoff := s.handOffAnswer(router, started)
		for _, answer := range []*httptest.ResponseRecorder{page, handoff} {
			s.Equal(http.StatusServiceUnavailable, answer.Code, "%s: %s", name, answer.Body.String())
			s.Contains(answer.Body.String(), errAttemptKeyUnavailable.Message, name)
			s.NotContains(answer.Body.String(), outage, name)
			s.NotContains(answer.Body.String(), "app_keys", name)
		}
	}
	s.Equal(before.AttemptSealed, s.attempt(started.ID).AttemptSealed)
	s.Contains(s.logged.String(), "the key that protects", "the request log has the cause")
}

// A failed read of the app's data key, here a deadline that has passed before the database
// answers, says nothing about the attempt. The callback, the launch page and the handoff
// answer 500 and log the cause, instead of calling a live consent unknown.
func (s *ConsentDataKeysSuite) TestAFailedReadOfTheDataKeyIsA500AndNotAnUnknownConsent() {
	warm := s.server(s.keys(true))
	state, cookies := s.handOff(warm, s.start(warm))
	fresh := s.start(warm)
	options := appkeystest.Options()
	options.Seal = true
	options.KMSTimeout = time.Nanosecond
	keys, err := appkeys.New(s.db, s.kms, options)
	s.Require().NoError(err)
	cold := s.server(keys)

	page := httptest.NewRecorder()
	cold.Handler().ServeHTTP(page, httptest.NewRequest(http.MethodGet, fresh.LaunchURL, nil))
	answers := map[string]*httptest.ResponseRecorder{"callback": s.callback(cold, state, cookies),
		"launch page": page, "handoff": s.handOffAnswer(cold, fresh)}

	for name, answer := range answers {
		s.Equal(http.StatusInternalServerError, answer.Code, "%s: %s", name, answer.Body.String())
		s.NotContains(answer.Body.String(), "deadline", name)
	}
	s.Contains(s.logged.String(), context.DeadlineExceeded.Error(), "the request log has the cause")
	_, err = s.db.ConnectorAuthorizationAttempts.GetByState(s.T().Context(), state)
	s.NoError(err, "the attempt is still open")
}

// An attempt's blob copied onto another attempt of the same app opens under neither row: the
// AAD binds it to its own row, so the callback answers as for an unknown consent. That holds
// under the app's data key and under the router's key alike.
func (s *ConsentDataKeysSuite) TestAnAttemptCopiedOntoAnotherIsAnUnknownConsent() {
	for name, dataKeys := range map[string]*appkeys.Keys{"data key": s.keys(true), "router's key": nil} {
		router := s.server(dataKeys)
		source := s.attempt(s.start(router).ID)
		target := s.start(router)
		state, cookies := s.handOff(router, target)
		_, err := s.db.GlobalDB().ExecContext(s.T().Context(),
			"UPDATE connector_authorization_attempts SET attempt_sealed = ? WHERE id = ?", source.AttemptSealed, target.ID)
		s.Require().NoError(err)

		answer := s.callback(router, state, cookies)

		s.Equal(http.StatusBadRequest, answer.Code, "%s: %s", name, answer.Body.String())
		s.Contains(answer.Body.String(), errNoAttempt.Message, name)
	}
}

// An attempt started before sealing was on opens under the router's key, which counts. Its
// handoff writes a new blob, which goes under the data key, and its callback does not seal it
// again.
func (s *ConsentDataKeysSuite) TestARouterKeyAttemptMovesAtItsHandoffAndCountsEachOpen() {
	started := s.start(s.server(nil))
	s.Nil(s.attempt(started.ID).AttemptDataKeyVersion)
	router := s.server(s.keys(true))
	opens := appkeystest.KEKOpens(s.T(), attemptKind)

	state, cookies := s.handOff(router, started)

	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), attemptKind))
	s.Equal(new(1), s.attempt(started.ID).AttemptDataKeyVersion)

	routerKey := s.server(nil)
	again := s.start(routerKey)
	state, cookies = s.handOff(routerKey, again)
	handedOff := s.attempt(again.ID)
	opens = appkeystest.KEKOpens(s.T(), attemptKind)
	answer := s.callback(router, state, cookies)
	s.Equal(http.StatusFound, answer.Code, answer.Body.String())
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), attemptKind), "the callback counts its open")
	var sealed []byte
	var dataKeyVersion *int
	s.Require().NoError(s.db.GlobalDB().QueryRowContext(s.T().Context(),
		"SELECT attempt_sealed, attempt_data_key_version FROM connector_authorization_attempts WHERE id = ?", again.ID).
		Scan(&sealed, &dataKeyVersion))
	s.Equal(handedOff.AttemptSealed, sealed, "the callback consumes the attempt and does not seal it again")
	s.Nil(dataKeyVersion)
}

func (s *ConsentDataKeysSuite) TestAConsentWhileKMSIsDownIsSealedUnderTheRoutersKey() {
	s.kms.Fail(errors.New("kms: the test's outage"))
	router := s.server(s.keys(true))
	fallbacks := appkeystest.SealFallbacks(s.T(), attemptKind)

	started := s.start(router)
	s.Nil(s.attempt(started.ID).AttemptDataKeyVersion)
	state, cookies := s.handOff(router, started)
	row := s.attempt(started.ID)
	answer := s.callback(router, state, cookies)

	s.Nil(row.AttemptDataKeyVersion)
	s.NoError(s.openUnderRoutersKey(row))
	s.Equal(fallbacks+2, appkeystest.SealFallbacks(s.T(), attemptKind), "the start and the handoff fell back")
	s.Equal(http.StatusFound, answer.Code, answer.Body.String())
	s.Equal(s.failedLanding(), answer.Header().Get("Location"))
}

// Rollback: with sealing turned off, an attempt sealed under the data key still finishes, and a
// new one goes under the router's key.
func (s *ConsentDataKeysSuite) TestWithSealingTurnedOffADataKeyAttemptStillFinishes() {
	sealing := s.server(s.keys(true))
	started := s.start(sealing)
	state, cookies := s.handOff(sealing, started)
	off := s.server(s.keys(false))

	answer := s.callback(off, state, cookies)

	s.Equal(http.StatusFound, answer.Code, answer.Body.String())
	s.Equal(s.failedLanding(), answer.Header().Get("Location"))
	next := s.start(off)
	s.Nil(s.attempt(next.ID).AttemptDataKeyVersion)
	s.handOff(off, next)
	s.Nil(s.attempt(next.ID).AttemptDataKeyVersion)
}

// A consent a session starts for its caller is sealed under the data key too, and so is its
// handoff.
func (s *ConsentDataKeysSuite) TestASessionsConsentIsSealedUnderTheAppsDataKey() {
	keys := s.keys(true)
	consent, err := ConnectorConsents(s.db, s.registry, s.sealer, keys, consentPublicURL)(s.T().Context(),
		session.ConsentRequest{CustomerID: s.app, ConnectorID: s.connection.ConnectorID, UserID: "user-" + rand.Text()})
	s.Require().NoError(err)

	s.Equal(new(1), s.attempt(consent.AuthorizationID).AttemptDataKeyVersion)
	s.handOff(s.server(keys), Authorization{LaunchURL: consent.LaunchURL, HandoffToken: consent.HandoffToken})
	s.Equal(new(1), s.attempt(consent.AuthorizationID).AttemptDataKeyVersion)
}

// The values come from a probe of the same consent on the base, before data keys: key version
// 1 at the start and at the handoff, the attempt opens under the router's key with this AAD,
// the made-up code ends in this redirect, and a second callback is an unknown consent.
func (s *ConsentDataKeysSuite) TestWithoutDataKeysAConsentIsSealedAndAnsweredAsBefore() {
	router := s.server(nil)

	started := s.start(router)
	s.Equal(1, s.attempt(started.ID).KEKVersion)
	s.NoError(s.openUnderRoutersKey(s.attempt(started.ID)))
	state, cookies := s.handOff(router, started)
	s.Len(cookies, 1)
	row := s.attempt(started.ID)
	s.Equal(1, row.KEKVersion)
	s.NoError(s.openUnderRoutersKey(row))
	answer := s.callback(router, state, cookies)
	again := s.callback(router, state, cookies)

	s.Equal(http.StatusFound, answer.Code)
	s.Equal("https://dashboard.example/connections?connection_id="+s.connection.ID+"&status=failed", answer.Header().Get("Location"))
	s.Equal(http.StatusBadRequest, again.Code)
	s.Contains(again.Body.String(), "this consent is unknown, expired or already finished: start it again")
}

func (s *ConsentDataKeysSuite) TestARotatedConfigTokenIsSealedUnderTheAppsDataKey() {
	router := s.server(s.keys(true))
	token := s.rotate(router)
	opens := appkeystest.KEKOpens(s.T(), configTokenKind)

	row := s.configToken()
	s.Equal(new(1), row.TokensDataKeyVersion)
	s.Equal(1, row.KEKVersion, "the table's CHECK needs a key version beside sealed tokens")
	_, err := s.openTokenUnderRoutersKey(row)
	s.Error(err, "the router's key does not open it")
	s.Equal(token, s.use(router))
	s.Equal(opens, appkeystest.KEKOpens(s.T(), configTokenKind))
	s.Equal(row, s.configToken(), "a token already under the data key is not written again")
}

func (s *ConsentDataKeysSuite) TestARouterKeyConfigTokenMovesUnderTheDataKeyOnItsFirstUseAndCountsOnce() {
	token := s.rotate(s.server(nil))
	router := s.server(s.keys(true))
	opens := appkeystest.KEKOpens(s.T(), configTokenKind)

	s.Equal(token, s.use(router))

	row := s.configToken()
	s.Equal(new(1), row.TokensDataKeyVersion)
	s.Equal(1, row.KEKVersion)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), configTokenKind))
	s.Equal(token, s.use(router))
	s.Equal(token, s.use(s.server(s.keys(false))), "sealing off still opens it")
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), configTokenKind), "later uses open under the data key")
}

func (s *ConsentDataKeysSuite) TestWithSealingOffAConfigTokenUseMovesNothing() {
	token := s.rotate(s.server(nil))
	before := s.configToken()

	s.Equal(token, s.use(s.server(s.keys(false))))

	s.Equal(before, s.configToken())
	s.NotContains(s.logged.String(), "level=WARN")
}

// A rotation while KMS is down keeps the new tokens under the router's key, and a use then
// keeps the row as it is, since the reseal cannot reach KMS either.
func (s *ConsentDataKeysSuite) TestAConfigTokenWhileKMSIsDownStaysUnderTheRoutersKey() {
	s.kms.Fail(errors.New("kms: the test's outage"))
	router := s.server(s.keys(true))
	fallbacks := appkeystest.SealFallbacks(s.T(), configTokenKind)

	token := s.rotate(router)
	row := s.configToken()
	s.Nil(row.TokensDataKeyVersion)
	s.Equal(fallbacks+1, appkeystest.SealFallbacks(s.T(), configTokenKind))
	s.Equal(token, s.use(router))
	s.Equal(row, s.configToken())
	s.Contains(s.logged.String(), "could not move an app configuration token under the app's data key")
	s.Contains(s.logged.String(), "record="+configTokenKind)
}

// A kept token that KMS cannot open, or one a router without data keys finds, is answered 503
// with fixed text, and the token is kept.
func (s *ConsentDataKeysSuite) TestAConfigTokenTheDataKeyCannotOpenIsA503InFixedWords() {
	s.rotate(s.server(s.keys(true)))
	before := s.configToken()
	outage := "kms: the test's outage " + rand.Text()
	s.kms.Fail(errors.New(outage))

	for name, router := range map[string]*Server{"KMS down": s.server(s.keys(true)), "no data keys": s.server(nil)} {
		_, err := router.configToken(s.T().Context(), s.app, "slack_bot", "")
		s.Equal(errConfigTokenKeyUnavailable, err, name)
		s.Equal(http.StatusServiceUnavailable, errConfigTokenKeyUnavailable.Status())
	}
	s.Equal(before, s.configToken())
}

// A row that names a data key version the app does not have fails, and never falls back to the
// router's key, even though the blob is one the router's key opens. It is not a key fault: the
// row is wrong.
func (s *ConsentDataKeysSuite) TestAConfigTokenUnderAMissingDataKeyVersionFailsClosed() {
	s.rotate(s.server(nil))
	_, err := s.db.GlobalDB().ExecContext(s.T().Context(),
		"UPDATE connector_config_tokens SET tokens_data_key_version = 7 WHERE customer_id = ?", s.app)
	s.Require().NoError(err)

	token, err := s.server(s.keys(true)).configToken(s.T().Context(), s.app, "slack_bot", "")

	s.ErrorIs(err, appkeys.ErrDoesNotOpen)
	s.NotEqual(errConfigTokenKeyUnavailable, err)
	s.Empty(token)
}

// Rollback: with sealing turned off, a token under the data key still opens, and the next
// rotation goes under the router's key.
func (s *ConsentDataKeysSuite) TestWithSealingTurnedOffADataKeyConfigTokenStillOpens() {
	token := s.rotate(s.server(s.keys(true)))
	before := s.configToken()
	off := s.server(s.keys(false))

	s.Equal(token, s.use(off))
	s.Equal(before, s.configToken())

	s.rotate(off)
	s.Nil(s.configToken().TokensDataKeyVersion)
}

// The values come from a probe of the same rotation and use on the base, before data keys: key
// version 1, the blob opens under the router's key with this AAD as JSON with the token and its
// refresh token, and a use while the token is good rewrites nothing.
func (s *ConsentDataKeysSuite) TestWithoutDataKeysAConfigTokenIsSealedAndUsedAsBefore() {
	router := s.server(nil)

	token := s.rotate(router)
	row := s.configToken()
	opened, err := s.openTokenUnderRoutersKey(row)
	again := s.use(router)

	s.Equal(1, row.KEKVersion)
	s.Require().NoError(err)
	s.Len(opened, 2)
	s.Equal(token, opened["token"])
	s.NotEmpty(opened["refresh_token"])
	s.Equal(token, again)
	s.Equal(row, s.configToken())
}

// server is one router with dataKeys, nil when no KMS key wraps data keys.
func (s *ConsentDataKeysSuite) server(dataKeys *appkeys.Keys) *Server {
	server, err := NewServer(Options{Routers: map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		Store: s.db, ConnectorSecrets: s.sealer, ConnectorDataKeys: dataKeys, Connectors: s.registry,
		PublicURL: consentPublicURL, DashboardURL: consentDashboard, SlackApps: s.slackApps,
		Logger: slog.New(slog.NewTextHandler(s.logged, nil))})
	s.Require().NoError(err)
	return server
}

// keys are data keys wrapped by the test's KMS, holding none of them yet.
func (s *ConsentDataKeysSuite) keys(seal bool) *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = seal
	keys, err := appkeys.New(s.db, s.kms, options)
	s.Require().NoError(err)
	return keys
}

// start is the app's backend starting a consent for the test's connection.
func (s *ConsentDataKeysSuite) start(router *Server) Authorization {
	started, err := router.consents().begin(s.T().Context(), s.connection, nil, "")
	s.Require().NoError(err)
	return started
}

// handOff is the launch page's handoff, and returns the state the provider sends back and the
// cookie that binds the attempt to the browser.
func (s *ConsentDataKeysSuite) handOff(router *Server, started Authorization) (string, []*http.Cookie) {
	answer := s.handOffAnswer(router, started)
	s.Require().Equal(http.StatusOK, answer.Code, answer.Body.String())
	var handed struct {
		AuthorizationURL string `json:"authorization_url"`
	}
	s.Require().NoError(json.Unmarshal(answer.Body.Bytes(), &handed))
	authorize, err := url.Parse(handed.AuthorizationURL)
	s.Require().NoError(err)
	return authorize.Query().Get("state"), answer.Result().Cookies()
}

func (s *ConsentDataKeysSuite) handOffAnswer(router *Server, started Authorization) *httptest.ResponseRecorder {
	body, err := json.Marshal(map[string]string{"handoff_token": started.HandoffToken})
	s.Require().NoError(err)
	request := httptest.NewRequest(http.MethodPost, started.LaunchURL, bytes.NewReader(body))
	request.Header.Set("Origin", consentPublicURL)
	answer := httptest.NewRecorder()
	router.Handler().ServeHTTP(answer, request)
	return answer
}

// callback is the provider's redirect with state and a code it never issued, in the browser
// that holds cookies.
func (s *ConsentDataKeysSuite) callback(router *Server, state string, cookies []*http.Cookie) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, consentPublicURL+ConnectorCallbackPath+"?"+
		url.Values{"state": {state}, "code": {"made-up-" + rand.Text()}}.Encode(), nil)
	for _, cookie := range cookies {
		request.AddCookie(cookie)
	}
	answer := httptest.NewRecorder()
	router.Handler().ServeHTTP(answer, request)
	return answer
}

// failedLanding is the dashboard page a failed consent of the test's connection ends on.
func (s *ConsentDataKeysSuite) failedLanding() string {
	return consentDashboard + "?" + url.Values{"connection_id": {s.connection.ID}, "status": {consentFailed}}.Encode()
}

func (s *ConsentDataKeysSuite) attempt(id string) store.ConnectorAuthorizationAttempt {
	row, err := s.db.ConnectorAuthorizationAttempts.Get(s.T().Context(), id)
	s.Require().NoError(err)
	return row
}

// openUnderRoutersKey opens an attempt with the router's key and the AAD the base sealed with.
func (s *ConsentDataKeysSuite) openUnderRoutersKey(row store.ConnectorAuthorizationAttempt) error {
	_, err := s.sealer.OpenWithAADVersion(row.AttemptSealed, fmt.Appendf(nil, "accelerate:connector-attempt:v1:%d:%s:%d:%s:%d:%s",
		len(s.app), s.app, len(row.ConnectionID), row.ConnectionID, len(row.ID), row.ID), 1)
	return err
}

// rotate is a PUT provider-app with a refresh token the fake Slack's admin just generated, and
// returns the configuration token it rotated to.
func (s *ConsentDataKeysSuite) rotate(router *Server) string {
	token, err := router.configToken(s.T().Context(), s.app, "slack_bot", s.provider.NewConfigToken())
	s.Require().NoError(err)
	return token
}

// use is a PUT provider-app without a refresh token, which uses the kept configuration token.
func (s *ConsentDataKeysSuite) use(router *Server) string {
	token, err := router.configToken(s.T().Context(), s.app, "slack_bot", "")
	s.Require().NoError(err)
	return token
}

func (s *ConsentDataKeysSuite) configToken() store.ConnectorConfigToken {
	row, err := s.db.ConnectorConfigTokens.Get(s.T().Context(), s.app, "slack_bot")
	s.Require().NoError(err)
	return row
}

// openTokenUnderRoutersKey opens a configuration token with the router's key and the AAD the
// base sealed with.
func (s *ConsentDataKeysSuite) openTokenUnderRoutersKey(row store.ConnectorConfigToken) (map[string]string, error) {
	raw, err := s.sealer.OpenWithAADVersion(row.TokensSealed,
		fmt.Appendf(nil, "accelerate:connector-config-token:v1:%d:%s:9:slack_bot", len(s.app), s.app), 1)
	if err != nil {
		return nil, err
	}
	var opened map[string]string
	return opened, json.Unmarshal([]byte(raw), &opened)
}
