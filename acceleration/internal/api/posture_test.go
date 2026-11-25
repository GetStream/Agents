//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/golang-jwt/jwt/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// PostureSuite is about who may reach what, asked of the whole surface rather than of one
// endpoint: the spec is the list of operations, so nothing is left out because no test
// named it.
type PostureSuite struct {
	RouterSuite
}

func TestPostureSuite(t *testing.T) {
	runSuite(t, new(PostureSuite))
}

func (s *PostureSuite) SetupSuite() {
	s.RouterSuite.SetupSuite()
	// Room to keep the connection of each caller that calls at once (answersOf). The default
	// keeps two, and every other call of the walk would leave a closed one behind.
	s.server.Client().Transport.(*http.Transport).MaxIdleConnsPerHost = 16
}

func (s *PostureSuite) SetupTest() {
	s.useFixture("standard")
}

func (s *PostureSuite) TestTheCommittedSpecIsRenderedFromTheOperations() {
	// api/openapi.yaml is what every client generator reads, so one edited by hand or
	// left behind by a change to an operation is a client out of step with the server.
	committed, err := os.ReadFile("../../api/openapi.yaml")
	s.Require().NoError(err)
	rendered, err := Spec()
	s.Require().NoError(err)

	s.Equal(string(rendered), string(committed), "run go run ./cmd/openapi in acceleration/")
}

func (s *PostureSuite) TestEveryOperationTheSpecDoesNotOpenIsRefusedToEveryDevice() {
	// The middleware reads the spec, so this is what proves the default reaches every
	// operation rather than only the ones a test happened to name. It is the whole point
	// of the inverted default: an operation nobody thought about is refused here.
	devices, refused := s.devices(), 0
	for _, operation := range s.operations() {
		if operation.public || operation.open {
			continue
		}
		refused++

		for name, status := range s.answersOf(operation, devices) {
			s.Equal(http.StatusForbidden, status, "%s %s by %s", operation.method, operation.path, name)
		}
	}
	s.NotZero(refused, "the spec leaves nothing server-side only")
}

func (s *PostureSuite) TestEveryOperationTheSpecOpensIsReachableByEveryDevice() {
	// The other half. A default that refused everything would pass the test above and
	// leave nobody able to hold a conversation.
	devices, opened := s.devices(), 0
	for _, operation := range s.operations() {
		if !operation.open {
			continue
		}
		opened++

		for name, status := range s.answersOf(operation, devices) {
			s.NotContains([]int{http.StatusUnauthorized, http.StatusForbidden}, status,
				"%s %s by %s", operation.method, operation.path, name)
		}
	}
	s.NotZero(opened, "the spec opens nothing to a client")
}

func (s *PostureSuite) TestEveryOperationTellsACallerWithoutBothCredentialsToAuthenticateFirst() {
	// Opened operations and the routes served by hand, sockets among them, as well as the
	// server-side ones: withServerSide leaves this caller to each handler's own 401.
	tokenOnly, keyOnly := s.serverClient.without(auth.APIKeyHeader), s.serverClient.without("Authorization")
	callers := map[string]*testClient{
		"nobody": s.unauthenticatedClient, "a token alone": tokenOnly, "a key alone": keyOnly,
	}
	asked := 0
	for _, operation := range s.operations() {
		if operation.public {
			continue
		}
		asked++

		for name, status := range s.answersOf(operation, callers) {
			s.Equal(http.StatusUnauthorized, status, "%s %s by %s", operation.method, operation.path, name)
		}
	}
	s.NotZero(asked, "the spec asks nobody for a credential")
}

func (s *PostureSuite) TestHealthStaysReachableWithoutACredential() {
	// A liveness probe holds no API key, and neither does the vendor fetching a call plan.
	status, _ := s.unauthenticatedClient.call(http.MethodGet, "/health", nil)

	s.Equal(http.StatusOK, status)
}

// The four ways a credential can be wrong have to be one answer. Telling a caller that the
// key was real but the token was not is a free way to find out which keys exist.

func (s *PostureSuite) TestAnUnknownKeyIsRefusedLikeNoCredentialAtAll() {
	stranger, _, err := auth.NewCredential(auth.Test)
	s.Require().NoError(err)

	s.assertRefusedAlike(func(header http.Header) { header.Set(auth.APIKeyHeader, stranger) })
}

func (s *PostureSuite) TestAKeyThatIsNotAKeyIsRefusedLikeNoCredentialAtAll() {
	s.assertRefusedAlike(func(header http.Header) { header.Set(auth.APIKeyHeader, "nonsense") })
}

func (s *PostureSuite) TestATokenSignedWithTheWrongSecretIsRefusedLikeNoCredentialAtAll() {
	elsewhere := s.data.createApp()

	s.assertRefusedAlike(func(header http.Header) {
		header.Set("Authorization", "Bearer "+s.signedFor(elsewhere.secret))
	})
}

func (s *PostureSuite) TestAnAppTurnsAwayALevelOfUserWithAForbidden() {
	// A level an app refuses is not a caller that gets a narrower API, it is a caller
	// that does not get in, so it is answered at the door.
	no := false
	s.useApp(s.data.createAppAdmitting(store.AppSettings{AllowAnonymous: &no}))

	status, failure := s.anonymousClient.failure(http.MethodPost, "/v1/agents/sessions/query", empty)

	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "level of user",
		"a caller that has proved who it is should be told what the problem is")
}

func (s *PostureSuite) TestAnAppTakesTheLevelsItHasNotTurnedAway() {
	// The other half: the default admits, so an app with no settings written is not one
	// whose users have all been locked out.
	status, _ := s.anonymousClient.call(http.MethodPost, "/v1/agents/sessions/query", empty)

	s.Equal(http.StatusOK, status)
}

// assertRefusedAlike checks a credential wrong in one way is refused the same way as one
// that is missing altogether, body and all.
func (s *PostureSuite) assertRefusedAlike(wrongly func(http.Header)) {
	presenting := *s.client
	presenting.header = s.client.header.Clone()
	wrongly(presenting.header)

	status, refusal := presenting.call(http.MethodGet, "/v1/stt/providers", nil)
	s.Equal(http.StatusUnauthorized, status)

	_, nothingAtAll := s.unauthenticatedClient.call(http.MethodGet, "/v1/stt/providers", nil)
	s.Equal(withoutDuration(nothingAtAll), withoutDuration(refusal))
}

// withoutDuration is an answer with the time it took taken out of it, which two refusals
// are never going to agree on. What is left is what a caller could tell them apart by.
func withoutDuration(body []byte) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return string(body)
	}
	delete(fields, "duration")
	rendered, err := json.Marshal(fields)
	if err != nil {
		return string(body)
	}
	return string(rendered)
}

// devices are every caller the router takes for an end user's device. A server token is one
// unless the server header comes with it, and that header makes no user's token a server's.
func (s *PostureSuite) devices() map[string]*testClient {
	serverToken := jwt.MapClaims{"server": true}
	return map[string]*testClient{
		"a user":                             s.client,
		"a guest":                            s.guestClient,
		"an anonymous caller":                s.anonymousClient,
		"a server token with the jwt header": s.signedIn(serverToken, auth.AuthTypeJWT, anonymous, ""),
		"a server token with the anonymous header": s.signedIn(serverToken, auth.AuthTypeAnonymous, anonymous, ""),
		"a server token with no header":            s.signedIn(serverToken, "", anonymous, "").without(auth.AuthTypeHeader),
		"a user's token with the server header":    s.signedIn(jwt.MapClaims{"user_id": s.client.userID}, auth.AuthTypeServer, user, s.client.userID),
		"a user's server token, with both":         s.signedIn(jwt.MapClaims{"user_id": s.client.userID, "server": true}, auth.AuthTypeServer, user, s.client.userID),
	}
}

// answersOf is the status each caller is answered by one operation. They call at once: the
// session socket waits seconds for a session still starting before it says there is none.
func (s *PostureSuite) answersOf(operation operationSummary, callers map[string]*testClient) map[string]int {
	var mu sync.Mutex
	var calls sync.WaitGroup
	statuses := map[string]int{}
	failed := map[string]error{}
	for name, caller := range callers {
		request, err := http.NewRequest(operation.method, caller.base()+fill(operation.path), strings.NewReader("{}"))
		s.Require().NoError(err)
		request.Header = caller.header.Clone()
		request.Header.Set("Content-Type", "application/json")
		calls.Go(func() {
			response, err := s.server.Client().Do(request)
			if err == nil {
				// Read to the end, so the connection is used again.
				_, err = readAll(response)
				_ = response.Body.Close()
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed[name] = err
				return
			}
			statuses[name] = response.StatusCode
		})
	}
	calls.Wait()
	s.Require().Empty(failed, "%s %s", operation.method, operation.path)
	return statuses
}

// without is the same caller leaving one header out.
func (c *testClient) without(header string) *testClient {
	less := *c
	less.header = c.header.Clone()
	less.header.Del(header)
	return &less
}

// operations are the ones the spec describes, which is what the middleware reads. The routes
// served by hand are among them, declared so that nothing is open because nothing saw it.
func (s *PostureSuite) operations() []operationSummary {
	return specifiedOperations((&Server{}).newAPI(chi.NewRouter()).OpenAPI())
}

// empty is a body that validates nowhere, for a call that has to be refused before a
// handler reads it.
var empty = map[string]any{}

// pathParameter fills in for an id, because a refusal comes before the handler that would
// look the resource up.
var pathParameter = regexp.MustCompile(`\{[^}]+\}`)

func fill(path string) string { return pathParameter.ReplaceAllString(path, "x") }
