//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/relay"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// AgentTeamsSuite covers an app that keeps each team's agent configs to its own users
// (AppSettings.AgentTeams): an end user opens a config only if they share a team with it,
// by Chat's shared-team rule, their teams read from their user in the app's Stream.
type AgentTeamsSuite struct {
	RouterSuite
	// teamsChat is the customer's own Stream app, where its users and their teams live.
	teamsChat *chattest.Server
	logged    *lockedLog
}

func (s *AgentTeamsSuite) SetupSuite() {
	s.logged = &lockedLog{}
	s.logs = s.logged
	s.deploymentKnown = true
	s.RouterSuite.SetupSuite()
}

const teamsAppKey = "own-key"

func TestAgentTeamsSuite(t *testing.T) {
	runSuite(t, new(AgentTeamsSuite))
}

func (s *AgentTeamsSuite) SetupTest() {
	s.useTeamsApp(store.AppSettings{AgentTeams: new(true)})
}

// useTeamsApp is an app with settings whose customer acts in a Stream app of its own, the
// only one whose users' teams the customer's backend sets.
func (s *AgentTeamsSuite) useTeamsApp(settings store.AppSettings) {
	s.useApp(s.data.createAppAdmitting(settings))
	s.teamsChat = s.giveApp(s.customerID(), 4242, teamsAppKey)
}

// useDeploymentApp is an app with settings that is the deployment's own Stream app, where its
// users and their teams live. Its customer acts there as app mode has it (Stored.For): with the
// deployment's key, pinned to the app's id, holding no token of its own.
func (s *AgentTeamsSuite) useDeploymentApp(settings store.AppSettings) {
	id := streamAppID()
	organization := store.Organization{Name: "organization-" + s.utils.uuid()}
	s.Require().NoError(s.store.Organizations.Insert(context.Background(), &organization))
	app := store.App{
		ID: streamapp.CustomerOf(id), OrganizationID: organization.ID, Name: "app-" + s.utils.uuid(), Settings: settings,
	}
	s.Require().NoError(s.store.Apps.Insert(context.Background(), &app))
	s.useApp(s.data.keyed(organization, app))
	s.teamsChat = chattest.NewServer(s.T())
	s.setApps(s.customerID(), func(apps *suiteApps) {
		apps.own[s.customerID()] = s.inDeploymentApp(id, teamsAppKey, teamsAppKey+"-secret", s.teamsChat.URL)
	})
}

// inDeploymentApp is the identity app mode acts in the deployment's app with, pinned to app: the
// deployment's key and secret, and no token, whether the customer is the app's or not.
func (s *AgentTeamsSuite) inDeploymentApp(app int64, key, secret, url string) streamapp.Identity {
	return streamapp.Identity{
		CustomerID: s.customerID(), StreamApp: app, APIKey: key, Secret: streamapp.NewSecret(secret), BaseURL: url,
	}
}

// A user's teams are asked of Stream once for teamsFor: two messages to a running chat within it
// ask nothing more, and one after it asks again.
func (s *AgentTeamsSuite) TestAUsersTeamsAreAskedOfStreamOnceForAsLongAsTheyAreHeld() {
	anya := s.inTeams("red")
	asked := s.teamReads()
	opened := anya.createSession(s.naming(s.configOf("red")))
	s.Require().Equal(asked+1, s.teamReads(), "opening the chat asked")

	s.Equal(http.StatusAccepted, s.status(anya.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses", s.asking())))
	s.Equal(http.StatusAccepted, s.status(anya.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses", s.asking())))
	s.Equal(asked+1, s.teamReads(), "two messages within teamsFor asked nothing more")

	s.teamsLapse(s.router)
	s.Equal(http.StatusAccepted, s.status(anya.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses", s.asking())))
	s.Equal(asked+2, s.teamReads(), "a message after teamsFor asked again")
}

// However many users ask, a router holds the teams of at most maxHeldTeams of them.
func (s *AgentTeamsSuite) TestARouterHoldsTheTeamsOfAtMostMaxHeldTeamsUsers() {
	held := newHeldTeams()

	for user := range maxHeldTeams + 1 {
		held.keep(strconv.Itoa(user), []string{"red"})
	}

	s.LessOrEqual(len(held.read), maxHeldTeams)
}

// The teams a router holds are one user's in one app: the same user id in another app is asked
// of that app's Stream.
func (s *AgentTeamsSuite) TestTheTeamsHeldForAUserAreNotLentToTheSameIdInAnotherApp() {
	anya := s.inTeams("red")
	s.Require().Equal(http.StatusCreated, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))

	s.useTeamsApp(store.AppSettings{AgentTeams: new(true)})
	namesake := s.data.signedInAs(anya.userID)
	s.teamsChat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})

	s.Equal(http.StatusNotFound, namesake.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
}

// Teams held that refuse are asked of Stream again before the refusal stands, so a user added to
// a team right after being refused its agent is let in on the next ask, not once teamsFor passed.
func (s *AgentTeamsSuite) TestAUserAddedToATeamRightAfterARefusalIsLetInOnTheNextAsk() {
	anya := s.inTeams("blue")
	config := s.configOf("red")
	s.Require().Equal(http.StatusNotFound, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(config), nil))
	s.teamsChat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue", "red"}})

	s.Equal(http.StatusCreated, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(config), nil))
}

// A user taken out of a team who goes on writing to its agent is refused each time, and costs
// Stream one more read of their teams each teamsRecheck, not one a message.
func (s *AgentTeamsSuite) TestAUserRefusedOverAndOverAsksStreamOnceEachTeamsRecheck() {
	anya, opened := s.runningChatAnyaLeft()
	asked := s.teamReads()
	writes := func() {
		for range 5 {
			s.Equal(http.StatusNotFound, s.status(anya.call(http.MethodPost, "/v1/agents/sessions/"+opened+"/responses", s.asking())))
		}
	}

	writes()
	s.Equal(asked+2, s.teamReads(), "read once the held teams lapsed, then asked again once")
	s.teamsAge(s.router, teamsRecheck)
	writes()
	s.Equal(asked+3, s.teamReads(), "asked again once more after teamsRecheck")
}

// While Stream cannot be asked again, the held teams' refusal stands rather than turning into
// a 503, and each refusal does not try Stream again.
func (s *AgentTeamsSuite) TestARefusalOfHeldTeamsStandsWhileStreamCannotBeAskedAgain() {
	anya := s.inTeams("blue")
	config := s.configOf("red")
	s.Require().Equal(http.StatusNotFound, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(config), nil))
	s.teamsChat.Fail(http.MethodGet, "/users")
	s.T().Cleanup(func() { s.teamsChat.Heal(http.MethodGet, "/users") })
	asked := s.teamReads()

	for range 3 {
		status, message := anya.failure(http.MethodPost, "/v1/agents/sessions", s.naming(config))
		s.Equal(http.StatusNotFound, status, message)
		s.Equal(errUnknownConfig.Message, message)
	}
	s.Equal(asked+1, s.teamReads(), "a failed read counts as the one ask of teamsRecheck")
}

// Refusals of one user's held teams made while Stream is asked again wait for that one read and
// take what it told, rather than asking it each or standing on the teams held before.
func (s *AgentTeamsSuite) TestRefusalsWhileStreamIsAskedAgainWaitForItsOneAnswer() {
	synctest.Test(s.T(), func(t *testing.T) {
		held := newHeldTeams()
		held.keep("anya", nil)
		var reads atomic.Int32
		told := make(chan struct{})
		read := func(context.Context) ([]string, error) {
			reads.Add(1)
			<-told
			return []string{"red"}, nil
		}
		answers := make(chan []string, 3)
		for range 3 {
			go func() {
				teams, _, _ := held.again(t.Context(), "anya", read)
				answers <- teams
			}()
		}

		synctest.Wait()
		asked := reads.Load()
		close(told)
		require.Equal(t, int32(1), asked, "each refusal asked Stream")
		for range 3 {
			require.Equal(t, []string{"red"}, <-answers, "a refusal took the teams held before the read")
		}
	})
}

func (s *AgentTeamsSuite) TestAConfigOfAnotherTeamIsAnsweredAsAnUnknownOne() {
	anya := s.inTeams("red")

	status, payload := anya.call(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("blue")))
	unknownStatus, unknown := anya.call(http.MethodPost, "/v1/agents/sessions", s.naming(AgentConfig{Id: s.utils.uuid()}))

	s.Equal(http.StatusNotFound, status, string(payload))
	s.Equal(unknownStatus, status)
	var refused, missing ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &refused))
	s.Require().NoError(json.Unmarshal(unknown, &missing))
	s.Equal(missing, refused, "the same answer as a config that does not exist")
}

func (s *AgentTeamsSuite) TestAConfigOfAnotherTeamNamedByNameIsAnsweredAsAnUnknownName() {
	anya := s.inTeams("red")
	config := s.configOf("blue")

	status, message := anya.failure(http.MethodPost, "/v1/agents/sessions", CreateSessionRequest{Agent: &config.Name})

	s.Equal(http.StatusNotFound, status, message)
	s.Equal("there is no agent called "+config.Name, message)
}

func (s *AgentTeamsSuite) TestAUserInSeveralTeamsOpensAConfigOfAnyOfThem() {
	carl := s.inTeams("red", "blue")

	s.Equal(http.StatusCreated, carl.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("blue")), nil))
	s.Equal(http.StatusCreated, carl.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
	s.Equal(http.StatusNotFound, carl.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("green")), nil))
}

func (s *AgentTeamsSuite) TestAConfigsTeamIsReadAsItsUsageTeamIsWithoutTheSpacesAround() {
	anya := s.inTeams("red")

	s.Equal(http.StatusCreated, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf(" red ")), nil))
}

func (s *AgentTeamsSuite) TestAConfigOfNoTeamIsClosedToAUserInATeam() {
	anya := s.inTeams("red")

	s.Equal(http.StatusNotFound, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("")), nil))
}

func (s *AgentTeamsSuite) TestAUserInNoTeamOpensOnlyConfigsOfNoTeam() {
	dana := s.inTeams()

	s.Equal(http.StatusCreated, dana.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("")), nil))
	s.Equal(http.StatusNotFound, dana.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
}

func (s *AgentTeamsSuite) TestAUserStreamDoesNotHaveIsInNoTeam() {
	stranger := s.data.createUser()

	s.Equal(http.StatusCreated, stranger.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("")), nil))
	s.Equal(http.StatusNotFound, stranger.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
}

func (s *AgentTeamsSuite) TestAGuestIsCheckedByTheTeamsOfTheirStreamUser() {
	guest := s.data.createGuest()
	s.teamsChat.PutUser(map[string]any{"id": guest.userID, "teams": []any{"red"}})

	s.Equal(http.StatusCreated, guest.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
	s.Equal(http.StatusNotFound, guest.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("")), nil))
}

// An anonymous caller goes by any user id it likes, so it cannot take that user's teams.
func (s *AgentTeamsSuite) TestACallerWithNoTokenIsInNoTeamWhoeverItClaimsToBe() {
	anya := s.inTeams("red")
	claiming := s.data.claiming(anya.userID)
	asked := s.teamReads()

	s.Equal(http.StatusNotFound, claiming.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
	s.Equal(http.StatusCreated, claiming.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("")), nil))
	s.Equal(asked, s.teamReads(), "Stream is not asked about the user it claims to be")
}

// Nor does it borrow the teams the router holds for that user from a request of their own.
func (s *AgentTeamsSuite) TestACallerWithNoTokenDoesNotBorrowTheTeamsHeldForTheUserItClaimsToBe() {
	anya := s.inTeams("red")
	s.Require().Equal(http.StatusCreated, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
	claiming := s.data.claiming(anya.userID)

	s.Equal(http.StatusNotFound, claiming.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
}

func (s *AgentTeamsSuite) TestAFailedTeamsReadOpensNothing() {
	dana := s.inTeams()
	config := s.configOf("")
	s.teamsChat.Fail(http.MethodGet, "/users")
	s.T().Cleanup(func() { s.teamsChat.Heal(http.MethodGet, "/users") })

	status, payload := dana.call(http.MethodPost, "/v1/agents/sessions", s.naming(config))

	s.Equal(http.StatusServiceUnavailable, status, string(payload))
	s.Contains(string(payload), errTeamsUnreadable.Message)
}

// What Stream failing tells must not depend on whether the name or id exists, or the failure
// would let anybody list the configs of the app.
func (s *AgentTeamsSuite) TestAFailedReadAnswersAlikeForAConfigThatExistsAndOneThatDoesNot() {
	dana := s.inTeams()
	config := s.configOf("")
	missing := AgentConfig{Id: s.utils.uuid(), Name: "nope-" + s.utils.uuid()}
	s.teamsChat.Fail(http.MethodGet, "/users")
	s.T().Cleanup(func() { s.teamsChat.Heal(http.MethodGet, "/users") })

	for name, pair := range map[string][2]CreateSessionRequest{
		"by id":   {s.naming(config), s.naming(missing)},
		"by name": {{Agent: &config.Name}, {Agent: &missing.Name}},
	} {
		existsStatus, exists := dana.failure(http.MethodPost, "/v1/agents/sessions", pair[0])
		missingStatus, absent := dana.failure(http.MethodPost, "/v1/agents/sessions", pair[1])
		s.Equal(http.StatusServiceUnavailable, existsStatus, name)
		s.Equal(existsStatus, missingStatus, name)
		s.Equal(exists, absent, name)
	}
}

func (s *AgentTeamsSuite) TestAnAppWhoseSettingsCannotBeReadAnswersAlikeForAnUnknownConfig() {
	dana := s.inTeams()
	config := s.configOf("")
	_, err := s.store.GlobalDB().NewRaw(`UPDATE apps SET settings = '{"agent_teams": "unreadable"}'::jsonb WHERE id = ?`,
		s.customerID()).Exec(s.T().Context())
	s.Require().NoError(err)

	existsStatus, exists := dana.failure(http.MethodPost, "/v1/agents/sessions", s.naming(config))
	missingStatus, absent := dana.failure(http.MethodPost, "/v1/agents/sessions", s.naming(AgentConfig{Id: s.utils.uuid()}))

	s.Equal(http.StatusInternalServerError, existsStatus)
	s.Equal(existsStatus, missingStatus)
	s.Equal(exists, absent)
}

func (s *AgentTeamsSuite) TestAnAppWithNoStreamToReadTeamsFromOpensNothingToAUser() {
	dana := s.inTeams()
	config := s.configOf("")
	s.setApps(s.customerID(), func(apps *suiteApps) { apps.nowhere[s.customerID()] = true })

	status, message := dana.failure(http.MethodPost, "/v1/agents/sessions", s.naming(config))

	s.Equal(http.StatusServiceUnavailable, status, message)
	s.Equal(errTeamsUnreadable.Message, message)
}

// In the deployment's shared app the user ids are nobody's own and its teams are written by
// whoever holds that app, so a customer acting there cannot tell its users' teams from it:
// in deployment mode, which pins nothing, nor in app mode, which pins the deployment's app.
func (s *AgentTeamsSuite) TestACustomerInTheDeploymentsSharedAppOpensNothingToAUser() {
	for name, pin := range map[string]int64{"deployment mode": 0, "app mode": suiteStreamApp} {
		s.useTeamsApp(store.AppSettings{AgentTeams: new(true)})
		anya := s.inTeams("red")
		config := s.configOf("red")
		s.chat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"red"}})
		s.setApps(s.customerID(), func(apps *suiteApps) {
			delete(apps.own, s.customerID())
			if pin != 0 {
				apps.own[s.customerID()] = s.inDeploymentApp(pin, suiteStreamKey, suiteStreamSecret, s.chat.URL)
			}
		})

		status, message := anya.failure(http.MethodPost, "/v1/agents/sessions", s.naming(config))

		s.Equal(http.StatusServiceUnavailable, status, name+": "+message)
		s.Equal(errTeamsUnreadable.Message, message, name)
	}
}

// The deployment's own Stream app is its customer's own, so that customer reads its users'
// teams there as any customer does in an app it holds a token for.
func (s *AgentTeamsSuite) TestTheDeploymentAppsOwnCustomerReadsItsUsersTeamsThere() {
	s.useDeploymentApp(store.AppSettings{AgentTeams: new(true)})
	anya, dana := s.inTeams("red"), s.inTeams()
	asked := s.teamReads()

	s.Equal(http.StatusCreated, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
	status, payload := anya.call(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("blue")))
	_, unknown := anya.call(http.MethodPost, "/v1/agents/sessions", s.naming(AgentConfig{Id: s.utils.uuid()}))
	s.Equal(http.StatusNotFound, status, string(payload))
	var refused, missing ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &refused))
	s.Require().NoError(json.Unmarshal(unknown, &missing))
	s.Equal(missing, refused, "the same answer as a config that does not exist")
	s.Equal(http.StatusCreated, dana.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("")), nil))
	s.Equal(http.StatusNotFound, dana.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
	s.Equal(asked+4, s.teamReads(), "each user's teams are asked of the deployment's app, and again for their refusal")
}

// In deployment tenancy the customer of the deployment's app has no app of its own: it acts in
// the deployment's app with no pin, and Clients knows which app that is (Deployment.App()).
func (s *AgentTeamsSuite) TestTheDeploymentAppsOwnCustomerWithNoPinReadsItsUsersTeamsThere() {
	ctx := context.Background()
	id := streamapp.CustomerOf(suiteStreamApp)
	settings := store.AppSettings{AgentTeams: new(true)}
	var app store.App
	if err := s.store.GlobalDB().NewSelect().Model(&app).Where("id = ?", id).Scan(ctx); err == nil {
		app.Settings = settings
		_, err = s.store.GlobalDB().NewUpdate().Model(&app).Column("settings").WherePK().Exec(ctx)
		s.Require().NoError(err)
		s.useApp(s.data.keyed(store.Organization{ID: app.OrganizationID}, app))
	} else {
		organization := store.Organization{Name: "organization-" + s.utils.uuid()}
		s.Require().NoError(s.store.Organizations.Insert(ctx, &organization))
		app = store.App{ID: id, OrganizationID: organization.ID, Name: "app-" + s.utils.uuid(), Settings: settings}
		s.Require().NoError(s.store.Apps.Insert(ctx, &app))
		s.useApp(s.data.keyed(organization, app))
	}
	s.teamsChat = s.chat
	anya, dana := s.inTeams("red"), s.inTeams()
	asked := s.teamReadsOn(suiteStreamKey)

	s.Equal(http.StatusCreated, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
	status, payload := anya.call(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("blue")))
	_, unknown := anya.call(http.MethodPost, "/v1/agents/sessions", s.naming(AgentConfig{Id: s.utils.uuid()}))
	s.Equal(http.StatusNotFound, status, string(payload))
	var refused, missing ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &refused))
	s.Require().NoError(json.Unmarshal(unknown, &missing))
	s.Equal(missing, refused, "the same answer as a config that does not exist")
	s.Equal(http.StatusCreated, dana.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("")), nil))
	s.Equal(http.StatusNotFound, dana.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("red")), nil))
	s.Equal(asked+4, s.teamReadsOn(suiteStreamKey), "each user's teams are asked of the deployment's app, and again for their refusal")
}

func (s *AgentTeamsSuite) TestTheDeploymentAppsOwnCustomersForkIsCheckedAgainstItsUsersTeams() {
	s.useDeploymentApp(store.AppSettings{AgentTeams: new(true)})
	anya := s.inTeams("red")
	parent := anya.createSession(s.recorded(s.configOf("red")))
	blue := s.configOf("blue")

	status, message := anya.failure(http.MethodPost, "/v1/agents/sessions/"+parent.Id+"/fork", ForkSessionRequest{ConfigId: &blue.Id})

	s.Equal(http.StatusNotFound, status, message)
	s.Equal(errUnknownConfig.Message, message)
	s.Equal(http.StatusCreated, anya.do(http.MethodPost, s.forking(parent.Id), s.offRecord(), nil))
}

// With the setting off, the deployment app's own customer opens another team's config asking
// Stream nothing, as on base b95e971ec, which answered it 201.
func (s *AgentTeamsSuite) TestWithTheSettingOffTheDeploymentAppsOwnCustomerIsNotChecked() {
	for _, settings := range []store.AppSettings{{}, {AgentTeams: new(false)}} {
		s.useDeploymentApp(settings)
		anya := s.inTeams("red")
		config := s.configOf("blue")
		asked := s.teamReads()

		s.Equal(http.StatusCreated, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(config), nil), "settings %+v", settings)
		s.Equal(asked, s.teamReads(), "settings %+v", settings)
	}
}

func (s *AgentTeamsSuite) TestAnAppWhoseSettingsCannotBeReadOpensNothingToAUser() {
	dana := s.inTeams()
	config := s.configOf("")
	_, err := s.store.GlobalDB().NewRaw(`UPDATE apps SET settings = '{"agent_teams": "unreadable"}'::jsonb WHERE id = ?`,
		s.customerID()).Exec(s.T().Context())
	s.Require().NoError(err)

	s.Equal(http.StatusInternalServerError, dana.do(http.MethodPost, "/v1/agents/sessions", s.naming(config), nil))
	s.Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", s.naming(config), nil),
		"a backend is not limited by teams, so it needs no settings")
}

// Chat compares team names exactly.
func (s *AgentTeamsSuite) TestATeamIsMatchedExactlyNotByCase() {
	anya := s.inTeams("red")

	s.Equal(http.StatusNotFound, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("Red")), nil))
}

func (s *AgentTeamsSuite) TestABackendOpensAConfigOfAnyTeamWithoutAskingStream() {
	anya := s.inTeams("red")
	asked := s.teamReads()

	s.Equal(http.StatusCreated, s.serverClient.actingFor(anya).do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("blue")), nil))
	s.Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", s.naming(s.configOf("")), nil))
	s.Equal(asked, s.teamReads())
}

func (s *AgentTeamsSuite) TestWithTheSettingOffAUserOpensAnotherTeamsConfigAndStreamIsNotAsked() {
	for _, settings := range []store.AppSettings{{}, {AgentTeams: new(false)}} {
		s.useTeamsApp(settings)
		anya := s.inTeams("red")
		config := s.configOf("blue")
		asked := s.teamReads()

		s.Equal(http.StatusCreated, anya.do(http.MethodPost, "/v1/agents/sessions", s.naming(config), nil), "settings %+v", settings)
		s.Equal(asked, s.teamReads(), "settings %+v", settings)
	}
}

func (s *AgentTeamsSuite) TestAForkNamingAConfigOfAnotherTeamIsAnsweredAsAnUnknownOne() {
	anya := s.inTeams("red")
	parent := anya.createSession(s.recorded(s.configOf("red")))
	blue := s.configOf("blue")

	status, message := anya.failure(http.MethodPost, "/v1/agents/sessions/"+parent.Id+"/fork", ForkSessionRequest{ConfigId: &blue.Id})

	s.Equal(http.StatusNotFound, status, message)
	s.Equal(errUnknownConfig.Message, message)
}

func (s *AgentTeamsSuite) TestASocketSessionWithAConfigOfAnotherTeamIsRefused() {
	anya := s.inTeams("red")
	config := s.configOf("blue")
	connection := anya.opens("/v1/agents/socket")
	s.Require().NoError(connection.WriteJSON(map[string]any{
		"type": "start", "sample_rate": 24_000, "session": map[string]any{"config_id": config.Id},
	}))

	var refused map[string]any
	s.Require().NoError(connection.ReadJSON(&refused))

	s.Equal("error", refused["type"], "%v", refused)
	s.Equal(errUnknownConfig.Message, refused["error"])
}

// A conversation carried on in another process is checked again: its user moved to another
// team since it was opened, so its config is no longer theirs.
func (s *AgentTeamsSuite) TestAChatCarriedOnFromItsRowIsCheckedAgainstTheUsersTeamsNow() {
	anya := s.inTeams("red")
	opened := anya.createSession(s.recorded(s.configOf("red")))
	anya.stopSession(opened.Id)
	_, other := s.otherRouter(nil)
	ctx := context.WithValue(context.Background(), customerContextKey{}, s.customerID())
	ctx = context.WithValue(ctx, kindContextKey{}, auth.KindAuthenticated)
	ctx = routing.WithCaller(ctx, routing.Caller{UserID: anya.userID, Verified: true})

	_, failure := other.reopenedFromRow(ctx, opened.Id)
	s.Require().NoError(failure)

	s.teamsChat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	s.teamsLapse(other)
	_, failure = other.reopenedFromRow(ctx, opened.Id)
	s.Equal(errUnknownConfig, failure)
}

// A fork that names no config goes on with its parent's, which is checked as if it had been
// named: Anya moved to team blue since she opened her red session, live or ended.
func (s *AgentTeamsSuite) TestAForkOfASessionWhoseConfigIsOfAnotherTeamNowIsRefused() {
	for _, ended := range []bool{false, true} {
		anya := s.inTeams("red")
		parent := anya.createSession(s.recorded(s.configOf("red")))
		s.Equal(http.StatusCreated, anya.do(http.MethodPost, s.forking(parent.Id), s.offRecord(), nil),
			"ended %v: still in its team", ended)
		if ended {
			anya.stopSession(parent.Id)
		}
		s.teamsChat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
		s.teamsLapse(s.router)

		status, message := anya.failure(http.MethodPost, s.forking(parent.Id), s.offRecord())

		s.Equal(http.StatusNotFound, status, "ended %v: %s", ended, message)
		s.Equal(errUnknownConfig.Message, message, "ended %v", ended)
	}
}

// A config deleted since leaves no team to check the fork against.
func (s *AgentTeamsSuite) TestAForkOfASessionWhoseConfigWasDeletedIsRefused() {
	for _, ended := range []bool{false, true} {
		anya := s.inTeams("red")
		parent := anya.createSession(s.recorded(s.configOf("red")))
		if ended {
			anya.stopSession(parent.Id)
		}
		s.deleteConfig(value(parent.ConfigId))

		status, message := anya.failure(http.MethodPost, s.forking(parent.Id), s.offRecord())

		s.Equal(http.StatusNotFound, status, "ended %v: %s", ended, message)
		s.Equal(errUnknownConfig.Message, message, "ended %v", ended)
	}
}

// What Stream failing tells must not say whether the parent's config is still there.
func (s *AgentTeamsSuite) TestAFailedTeamsReadAnswersAForkAlikeWhetherItsConfigExistsOrNot() {
	anya := s.inTeams("red")
	kept := anya.createSession(s.recorded(s.configOf("red")))
	deleted := anya.createSession(s.recorded(s.configOf("red")))
	s.deleteConfig(value(deleted.ConfigId))
	s.teamsChat.Fail(http.MethodGet, "/users")
	s.T().Cleanup(func() { s.teamsChat.Heal(http.MethodGet, "/users") })
	s.teamsLapse(s.router)

	keptStatus, keptMessage := anya.failure(http.MethodPost, s.forking(kept.Id), s.offRecord())
	deletedStatus, deletedMessage := anya.failure(http.MethodPost, s.forking(deleted.Id), s.offRecord())

	s.Equal(http.StatusServiceUnavailable, keptStatus, keptMessage)
	s.Equal(errTeamsUnreadable.Message, keptMessage)
	s.Equal(keptStatus, deletedStatus)
	s.Equal(keptMessage, deletedMessage)
}

func (s *AgentTeamsSuite) TestAForkOfASessionOpenedWithNoConfigIsNotChecked() {
	anya := s.inTeams("red")
	parent := anya.createSession(textSession(nil))
	asked := s.teamReads()

	s.Equal(http.StatusCreated, anya.do(http.MethodPost, s.forking(parent.Id), s.offRecord(), nil))
	s.Equal(asked, s.teamReads())
}

// A chat whose session ended in this node is carried on with the spec it ran on, which is
// checked against the config it was opened with as it is now, on every way of writing to it.
func (s *AgentTeamsSuite) TestAChatThatEndedHereIsCarriedOnOnlyForAUserStillInItsConfigsTeam() {
	for path, body := range map[string]any{"/responses": s.asking(), "/voice": nil} {
		anya := s.inTeams("red")
		opened := anya.createSession(s.recorded(s.configOf("red")))
		s.endHere(opened.Id)
		s.Equal(http.StatusAccepted, s.status(anya.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses", s.asking())),
			"%s: still in its team", path)
		s.endHere(opened.Id)
		s.teamsChat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
		s.teamsLapse(s.router)

		status, message := anya.failure(http.MethodPost, "/v1/agents/sessions/"+opened.Id+path, body)

		s.Equal(http.StatusNotFound, status, "%s: %s", path, message)
		s.Equal(errUnknownConfig.Message, message, path)
	}
}

func (s *AgentTeamsSuite) TestAChatThatEndedHereWhoseConfigWasDeletedIsNotCarriedOn() {
	anya := s.inTeams("red")
	opened := anya.createSession(s.recorded(s.configOf("red")))
	s.endHere(opened.Id)
	s.deleteConfig(value(opened.ConfigId))

	status, message := anya.failure(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses", s.asking())

	s.Equal(http.StatusNotFound, status, message)
	s.Equal(errUnknownConfig.Message, message)
}

func (s *AgentTeamsSuite) TestAFailedTeamsReadAnswersAReopenAlikeWhetherItsConfigExistsOrNot() {
	anya := s.inTeams("red")
	kept := anya.createSession(s.recorded(s.configOf("red")))
	deleted := anya.createSession(s.recorded(s.configOf("red")))
	s.endHere(kept.Id)
	s.endHere(deleted.Id)
	s.deleteConfig(value(deleted.ConfigId))
	s.teamsChat.Fail(http.MethodGet, "/users")
	s.T().Cleanup(func() { s.teamsChat.Heal(http.MethodGet, "/users") })
	s.teamsLapse(s.router)

	keptStatus, keptMessage := anya.failure(http.MethodPost, "/v1/agents/sessions/"+kept.Id+"/responses", s.asking())
	deletedStatus, deletedMessage := anya.failure(http.MethodPost, "/v1/agents/sessions/"+deleted.Id+"/responses", s.asking())

	s.Equal(http.StatusServiceUnavailable, keptStatus, keptMessage)
	s.Equal(errTeamsUnreadable.Message, keptMessage)
	s.Equal(keptStatus, deletedStatus)
	s.Equal(keptMessage, deletedMessage)
}

func (s *AgentTeamsSuite) TestAChatThatEndedHereOpenedWithNoConfigIsNotChecked() {
	anya := s.inTeams("red")
	opened := anya.createSession(textSession(nil))
	s.endHere(opened.Id)
	asked := s.teamReads()

	s.Equal(http.StatusAccepted, s.status(anya.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses", s.asking())))
	s.Equal(asked, s.teamReads())
}

// A config that cannot be read is not taken for a deleted one: the fork and the reopen fail
// rather than open on what the user is not checked against. With the setting off a reopen does
// not read it: a fork answered 500 and a reopen 202 on base b4d31ea60, as they do here.
func (s *AgentTeamsSuite) TestAConfigThatCannotBeReadFailsAReopenOnlyWhenChecked() {
	for settings, reopened := range map[*bool]int{new(true): http.StatusInternalServerError, nil: http.StatusAccepted} {
		s.useTeamsApp(store.AppSettings{AgentTeams: settings})
		anya := s.inTeams("red")
		parent := anya.createSession(s.recorded(s.configOf("red")))
		ended := anya.createSession(s.recorded(s.configOf("red")))
		s.endHere(ended.Id)
		for _, opened := range []Session{parent, ended} {
			_, err := s.store.GlobalDB().NewRaw(`UPDATE agent_configs SET skills = '"unreadable"'::jsonb WHERE id = ?`,
				value(opened.ConfigId)).Exec(s.T().Context())
			s.Require().NoError(err)
			s.store.AgentConfigs.ForgetCached(s.T().Context(), s.customerID(), value(opened.ConfigId))
		}

		s.Equal(http.StatusInternalServerError, s.status(anya.call(http.MethodPost, s.forking(parent.Id), s.offRecord())), "flag %v", settings != nil)
		s.Equal(reopened, s.status(anya.call(http.MethodPost, "/v1/agents/sessions/"+ended.Id+"/responses", s.asking())), "flag %v", settings != nil)
	}
}

// The socket reopens a chat that ended here as the REST writes do (sessionws.go:123), so
// a user who moved to another team is answered as for an unknown config.
func (s *AgentTeamsSuite) TestASocketReopenOfAChatThatEndedHereIsCheckedAgainstTheUsersTeamsNow() {
	anya := s.inTeams("red")
	opened := anya.createSession(s.recorded(s.configOf("red")))
	s.endHere(opened.Id)
	_, status := anya.watch("/v1/agents/sessions/" + opened.Id + "/events")
	s.Equal(http.StatusSwitchingProtocols, status, "still in its team")
	s.endHere(opened.Id)
	s.teamsChat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	s.teamsLapse(s.router)

	status, message := anya.refusal("/v1/agents/sessions/" + opened.Id + "/events")

	s.Equal(http.StatusNotFound, status, message)
	s.Equal(errUnknownConfig.Message, message)
}

func (s *AgentTeamsSuite) TestAFailedTeamsReadAnswersASocketReopenAlikeWhetherItsConfigExistsOrNot() {
	anya := s.inTeams("red")
	kept := anya.createSession(s.recorded(s.configOf("red")))
	deleted := anya.createSession(s.recorded(s.configOf("red")))
	s.endHere(kept.Id)
	s.endHere(deleted.Id)
	s.deleteConfig(value(deleted.ConfigId))
	s.teamsChat.Fail(http.MethodGet, "/users")
	s.T().Cleanup(func() { s.teamsChat.Heal(http.MethodGet, "/users") })
	s.teamsLapse(s.router)

	keptStatus, keptMessage := anya.refusal("/v1/agents/sessions/" + kept.Id + "/events")
	deletedStatus, deletedMessage := anya.refusal("/v1/agents/sessions/" + deleted.Id + "/events")

	s.Equal(http.StatusServiceUnavailable, keptStatus, keptMessage)
	s.Equal(errTeamsUnreadable.Message, keptMessage)
	s.Equal(keptStatus, deletedStatus)
	s.Equal(keptMessage, deletedMessage)
}

func (s *AgentTeamsSuite) TestASocketReopenOfAChatWhoseConfigWasDeletedIsAnsweredAsAnUnknownConfig() {
	anya := s.inTeams("red")
	gone := anya.createSession(s.recorded(s.configOf("red")))
	s.endHere(gone.Id)
	s.deleteConfig(value(gone.ConfigId))

	status, message := anya.refusal("/v1/agents/sessions/" + gone.Id + "/events")

	s.Equal(http.StatusNotFound, status, message)
	s.Equal(errUnknownConfig.Message, message)
}

func (s *AgentTeamsSuite) TestWithTheSettingOffASocketReopenIsNotCheckedAndStreamIsNotAsked() {
	s.useTeamsApp(store.AppSettings{})
	anya := s.inTeams("blue")
	opened := anya.createSession(s.recorded(s.configOf("red")))
	s.endHere(opened.Id)
	gone := anya.createSession(s.recorded(s.configOf("red")))
	s.endHere(gone.Id)
	s.deleteConfig(value(gone.ConfigId))
	asked := s.teamReads()

	_, status := anya.watch("/v1/agents/sessions/" + opened.Id + "/events")
	s.Equal(http.StatusSwitchingProtocols, status)
	_, status = anya.watch("/v1/agents/sessions/" + gone.Id + "/events")
	s.Equal(http.StatusSwitchingProtocols, status)
	s.Equal(asked, s.teamReads())
}

// A chat still running is carried on only for a user still in its config's team, as one that
// ended is: Anya left team red while her red chat ran.
func (s *AgentTeamsSuite) TestAMessageToAChatStillRunningIsRefusedToAUserWhoLeftItsConfigsTeam() {
	anya, opened := s.runningChatAnyaLeft()

	status, message := anya.failure(http.MethodPost, "/v1/agents/sessions/"+opened+"/responses", s.asking())

	s.Equal(http.StatusNotFound, status, message)
	s.Equal(errUnknownConfig.Message, message)
}

func (s *AgentTeamsSuite) TestAResumeOfAChatStillRunningIsRefusedToAUserWhoLeftItsConfigsTeam() {
	anya, opened := s.runningChatAnyaLeft()

	status, message := anya.failure(http.MethodPost, "/v1/agents/sessions/"+opened+"/resume", nil)

	s.Equal(http.StatusNotFound, status, message)
	s.Equal(errUnknownConfig.Message, message)
}

// The events socket of a chat still running is opened only for a user still in its config's
// team, as one onto a chat that ended is.
func (s *AgentTeamsSuite) TestAChatStillRunningIsWatchedOnlyByAUserStillInItsConfigsTeam() {
	anya := s.inTeams("red")
	opened := anya.createSession(s.recorded(s.configOf("red")))
	_, status := anya.watch("/v1/agents/sessions/" + opened.Id + "/events")
	s.Equal(http.StatusSwitchingProtocols, status, "still in its team")
	s.teamsChat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	s.teamsLapse(s.router)
	s.running(opened.Id)

	status, message := anya.refusal("/v1/agents/sessions/" + opened.Id + "/events")

	s.Equal(http.StatusNotFound, status, message)
	s.Equal(errUnknownConfig.Message, message)
}

// The node running a chat asks the same of a socket on another node, which is then answered as
// for a session no node is running.
func (s *AgentTeamsSuite) TestAChatRunningOnAnotherNodeIsWatchedOnlyByAUserStillInItsConfigsTeam() {
	other, _ := s.otherRouter(nil)
	anya := s.inTeams("red")
	opened := anya.createSession(s.naming(s.configOf("red")))
	_, status := anya.on(other).watch("/v1/agents/sessions/" + opened.Id + "/events")
	s.Equal(http.StatusSwitchingProtocols, status, "still in its team")
	s.teamsChat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	s.teamsLapse(s.router)
	s.running(opened.Id)

	_, status = anya.on(other).watch("/v1/agents/sessions/" + opened.Id + "/events")

	s.Equal(http.StatusNotFound, status)
}

// The node running a chat takes a caller with no token for nobody in particular, as a node
// taking its request would: the user it claims to be does not lend it their teams.
func (s *AgentTeamsSuite) TestACallerWithNoTokenWatchingFromAnotherNodeIsInNoTeamWhoeverItClaimsToBe() {
	other, _ := s.otherRouter(nil)
	claiming := s.data.claiming(s.inTeams("red").userID)
	opened := claiming.createSession(s.naming(s.configOf("")))
	asked := s.teamReads()

	_, status := claiming.on(other).watch("/v1/agents/sessions/" + opened.Id + "/events")

	s.Equal(http.StatusSwitchingProtocols, status)
	s.Equal(asked, s.teamReads(), "Stream is not asked about the user it claims to be")
}

// A guest's token is signed as a user's is, so the node running a chat takes a guest watching
// from another node for the Stream user they are, in that user's teams.
func (s *AgentTeamsSuite) TestASignedGuestInTheTeamWatchesAChatRunningOnAnotherNode() {
	other, _ := s.otherRouter(nil)
	guest := s.data.createGuest()
	s.teamsChat.PutUser(map[string]any{"id": guest.userID, "teams": []any{"red"}})
	opened := guest.createSession(s.naming(s.configOf("red")))

	_, status := guest.on(other).watch("/v1/agents/sessions/" + opened.Id + "/events")

	s.Equal(http.StatusSwitchingProtocols, status)
}

// A backend is not limited by teams on whichever node it watches from, and a watcher relayed
// for it asks Stream nothing.
func (s *AgentTeamsSuite) TestABackendWatchesAChatOfAnyTeamRunningOnAnotherNodeWithoutAskingStream() {
	other, _ := s.otherRouter(nil)
	backend := s.serverClient.actingFor(s.inTeams("blue"))
	opened := backend.createSession(s.naming(s.configOf("red")))
	asked := s.teamReads()

	_, status := backend.on(other).watch("/v1/agents/sessions/" + opened.Id + "/events")

	s.Equal(http.StatusSwitchingProtocols, status)
	s.Equal(asked, s.teamReads())
}

// A socket on another node that gives up while the node running the chat asks Stream about its
// user leaves no watcher held there for it: its Detach overtakes the check, and the attach the
// check then lets through finds it gone, rather than standing until pongWait.
func (s *AgentTeamsSuite) TestASocketThatGivesUpDuringItsTeamCheckLeavesNoWatcherHeld() {
	other, _ := s.otherRouter(nil)
	anya := s.inTeams("red")
	opened := anya.createSession(s.naming(s.configOf("red")))
	backends := s.serverClient.createSession(s.naming(s.configOf("")))
	detached := s.detachOf(opened.Id)
	s.teamsLapse(s.router)
	asking, release := s.teamsChat.Hold("/users")

	giveUp := anya.on(other).handshaking("/v1/agents/sessions/" + opened.Id + "/events")
	s.closedWithin(asking, "the node running the chat never asked Stream")
	giveUp()
	s.closedWithin(detached, "the other node sent no Detach")
	// Taken after that Detach on the one subscription every node's requests come in on.
	s.serverClient.on(other).opens("/v1/agents/sessions/" + backends.Id + "/events")
	release()

	s.Require().Eventually(func() bool { return s.proxyHeldFor(anya.userID) || s.teamChecksRunning() == 0 },
		settleFor, 10*time.Millisecond, "the team check never ended")
	s.False(s.proxyHeldFor(anya.userID), "a watcher is held for a socket that gave up")
}

// An attach the node running the chat refuses leaves nothing of it there, though the node that
// asked may be gone before it says it gave up.
func (s *AgentTeamsSuite) TestARefusedRelayedAttachLeavesNothingPending() {
	anya := s.inTeams("red")
	opened := anya.createSession(s.naming(s.configOf("red")))
	s.teamsChat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	s.teamsLapse(s.router)
	asked, watcher, bus := s.teamReads(), s.utils.uuid(), s.router.relayed.bus

	s.Require().NoError(bus.Publish(s.T().Context(), bus.Commands(), relay.Request{
		Type: relay.Attach, Node: "gone-" + s.utils.uuid(), Session: opened.Id, Watcher: watcher,
		Owner: relay.Owner{CustomerID: s.customerID(), UserID: anya.userID, Kind: string(auth.KindAuthenticated)},
	}))

	s.Require().Eventually(func() bool {
		s.router.relayed.mu.Lock()
		_, pending := s.router.relayed.pending[watcher]
		s.router.relayed.mu.Unlock()
		return s.teamReads() > asked && !pending
	}, settleFor, 10*time.Millisecond, "the refused attach is still pending")
	s.False(s.proxyHeldFor(anya.userID))
}

// The node running a chat asks Stream about a relayed watcher's user off the subscription every
// node's requests come in on, so a slow answer holds up no other socket's attach.
func (s *AgentTeamsSuite) TestASlowTeamsReadHoldsUpNoOtherSocketOnTheNode() {
	other, _ := s.otherRouter(nil)
	anya := s.inTeams("red")
	opened := anya.createSession(s.naming(s.configOf("red")))
	backends := s.serverClient.createSession(s.naming(s.configOf("")))
	s.teamsLapse(s.router)
	asking, release := s.teamsChat.Hold("/users")
	giveUp := anya.on(other).handshaking("/v1/agents/sessions/" + opened.Id + "/events")
	defer giveUp()
	defer release()
	s.closedWithin(asking, "the node running the chat never asked Stream")

	_, status := s.serverClient.on(other).watch("/v1/agents/sessions/" + backends.Id + "/events")

	s.Equal(http.StatusSwitchingProtocols, status, "the backend's attach waited for anya's teams")
}

// A node running a chat that cannot read a relayed watcher's teams says so, and the socket is
// told to try again, rather than taken for a chat no node runs and opened a second time on the
// node it came to, whose own teams of Anya would let her through.
func (s *AgentTeamsSuite) TestAWatcherFromAnotherNodeWhoseTeamsCannotBeReadIsToldToTryAgain() {
	other, anya, opened := s.chatAnyasTeamsAreHeldForOnAnotherNode()
	s.teamsChat.Fail(http.MethodGet, "/users")
	s.T().Cleanup(func() { s.teamsChat.Heal(http.MethodGet, "/users") })

	status, message := anya.on(other).refusal("/v1/agents/sessions/" + opened + "/events")

	s.Equal(http.StatusServiceUnavailable, status, message)
	s.Equal(errTeamsUnreadable.Message, message)
	s.running(opened)
}

// The same holds for a read that takes longer than the socket waits: the node running the chat
// stops asking in time to say so.
func (s *AgentTeamsSuite) TestAWatcherFromAnotherNodeWhoseTeamsAreReadTooSlowlyIsToldToTryAgain() {
	other, anya, opened := s.chatAnyasTeamsAreHeldForOnAnotherNode()
	asking, _ := s.teamsChat.Hold("/users")

	status, message := anya.on(other).refusal("/v1/agents/sessions/" + opened + "/events")

	s.closedWithin(asking, "the node running the chat never asked Stream")
	s.Equal(http.StatusServiceUnavailable, status, message)
	s.Equal(errTeamsUnreadable.Message, message)
	s.running(opened)
}

// A backend forks and carries on any session of another team or a deleted config, without
// asking Stream, as it opens any config.
func (s *AgentTeamsSuite) TestABackendForksAndCarriesOnASessionOfAnyTeamWithoutAskingStream() {
	backend := s.serverClient.actingFor(s.inTeams("blue"))
	opened := backend.createSession(s.recorded(s.configOf("red")))
	other := backend.createSession(s.recorded(s.configOf("red")))
	s.deleteConfig(value(other.ConfigId))
	asked := s.teamReads()

	for _, parent := range []Session{opened, other} {
		status, payload := backend.call(http.MethodPost, s.forking(parent.Id), s.offRecord())
		s.Equal(http.StatusCreated, status, string(payload))
		s.endHere(parent.Id)
		status, payload = backend.call(http.MethodPost, "/v1/agents/sessions/"+parent.Id+"/responses", s.asking())
		s.Equal(http.StatusAccepted, status, string(payload))
	}
	s.Equal(asked, s.teamReads())
}

// With the setting off, forks and reopens of another team's or a deleted config's session go
// on as on base b4d31ea60, which answered these requests 201 and 202 asking Stream nothing.
func (s *AgentTeamsSuite) TestWithTheSettingOffForksAndReopensAreNotCheckedAndStreamIsNotAsked() {
	for _, settings := range []store.AppSettings{{}, {AgentTeams: new(false)}} {
		s.useTeamsApp(settings)
		anya := s.inTeams("blue")
		live := anya.createSession(s.recorded(s.configOf("red")))
		stopped := anya.createSession(s.recorded(s.configOf("red")))
		anya.stopSession(stopped.Id)
		deleted := anya.createSession(s.recorded(s.configOf("red")))
		s.deleteConfig(value(deleted.ConfigId))
		ended := anya.createSession(s.recorded(s.configOf("red")))
		s.endHere(ended.Id)
		gone := anya.createSession(s.recorded(s.configOf("red")))
		s.endHere(gone.Id)
		s.deleteConfig(value(gone.ConfigId))
		asked := s.teamReads()

		for _, parent := range []Session{live, stopped, deleted} {
			s.Equal(http.StatusCreated, anya.do(http.MethodPost, s.forking(parent.Id), s.offRecord(), nil), "settings %+v", settings)
		}
		for _, opened := range []Session{ended, gone} {
			s.Equal(http.StatusAccepted, s.status(anya.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses", s.asking())),
				"settings %+v", settings)
		}
		s.Equal(asked, s.teamReads(), "settings %+v", settings)
	}
}

// deleteConfig deletes an agent config of the app, as its backend does.
func (s *AgentTeamsSuite) deleteConfig(id string) {
	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/agents/configs/"+id, nil, nil))
}

// offRecord is a fork kept off the record and started from nothing, which opens no Stream
// channel and so asks Stream nothing about its users: the only user query it makes is the
// teams read.
func (s *AgentTeamsSuite) offRecord() ForkSessionRequest {
	return ForkSessionRequest{Incognito: pointerTo(true), Messages: pointerTo(false)}
}

// asking is a question from an end user's device, which names its request.
func (s *AgentTeamsSuite) asking() CreateResponseRequest {
	return CreateResponseRequest{Text: "Still there?", RequestId: pointerTo(s.utils.uuid())}
}

// forking is the path that forks the session id.
func (s *AgentTeamsSuite) forking(id string) string {
	return "/v1/agents/sessions/" + id + "/fork"
}

// runningChatAnyaLeft is Anya and the id of a red chat she opened and wrote in while in team red,
// still running here once she moved to team blue.
func (s *AgentTeamsSuite) runningChatAnyaLeft() (*testClient, string) {
	anya := s.inTeams("red")
	opened := anya.createSession(s.recorded(s.configOf("red")))
	s.Require().Equal(http.StatusAccepted, s.status(anya.call(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses", s.asking())),
		"still in its team")
	s.teamsChat.PutUser(map[string]any{"id": anya.userID, "teams": []any{"blue"}})
	s.teamsLapse(s.router)
	s.running(opened.Id)
	return anya, opened.Id
}

// chatAnyasTeamsAreHeldForOnAnotherNode is another node, Anya in team red, and the id of a red
// chat of hers kept and running on the suite's node. The other node holds her teams, read by a
// chat she opened there; the suite's node holds none of hers any more.
func (s *AgentTeamsSuite) chatAnyasTeamsAreHeldForOnAnotherNode() (*httptest.Server, *testClient, string) {
	other, _ := s.otherRouter(nil)
	anya := s.inTeams("red")
	config := s.configOf("red")
	anya.on(other).createSession(s.naming(config))
	opened := anya.createSession(s.recorded(config))
	s.teamsLapse(s.router)
	return other, anya, opened.Id
}

// running requires the session to be running on the suite's node, so that what a test asks
// next is asked of a running session rather than of one carried on.
func (s *AgentTeamsSuite) running(id string) {
	held, ok := s.manager.Get(id, session.Owner{CustomerID: s.customerID(), Kind: auth.KindServer})
	s.Require().True(ok && held.State() != session.Ended, "session %s is running here", id)
}

// teamsLapse has node take every user's teams it holds as read teamsFor ago, as they are once that
// long has passed: what a test changed of a user's teams in Stream is read from then on.
func (s *RouterSuite) teamsLapse(node *Server) {
	s.teamsAge(node, teamsFor)
}

// teamsAge has node take every user's teams it holds, and every time it asked Stream again, as
// by older than they are.
func (s *RouterSuite) teamsAge(node *Server, by time.Duration) {
	held := node.teams
	held.mu.Lock()
	defer held.mu.Unlock()
	clock := held.now
	held.now = func() time.Time { return clock().Add(by) }
}

// detachOf is closed once a node has said on the relay that a watcher of the session id is no
// longer wanted.
func (s *AgentTeamsSuite) detachOf(id string) <-chan struct{} {
	detached := make(chan struct{})
	var once sync.Once
	bus := s.router.relayed.bus
	_, err := bus.Subscribe(s.T().Context(), bus.Commands(), func(payload []byte) {
		var request relay.Request
		if json.Unmarshal(payload, &request) == nil && request.Type == relay.Detach && request.Session == id {
			once.Do(func() { close(detached) })
		}
	})
	s.Require().NoError(err)
	return detached
}

// closedWithin waits for done to close, failing the test as why when it does not within settleFor.
func (s *AgentTeamsSuite) closedWithin(done <-chan struct{}, why string) {
	select {
	case <-done:
	case <-time.After(settleFor):
		s.Require().FailNow(why)
	}
}

// proxyHeldFor is whether the suite's node holds a watcher for a socket of user's on another node.
func (s *AgentTeamsSuite) proxyHeldFor(user string) bool {
	state := s.router.relayed
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, proxy := range state.proxies {
		if proxy.owner.UserID == user {
			return true
		}
	}
	return false
}

// teamChecksRunning is how many relayed watchers' team checks are running, on any node of the
// process: each ends within teamCheckWait.
func (s *AgentTeamsSuite) teamChecksRunning() int {
	var dump bytes.Buffer
	s.Require().NoError(pprof.Lookup("goroutine").WriteTo(&dump, 1))
	total := 0
	for _, stack := range strings.Split(dump.String(), "\n\n") {
		if !strings.Contains(stack, "internal/api.(*Server).attachProxy.func") {
			continue
		}
		var count int
		_, err := fmt.Sscanf(stack, "%d @", &count)
		s.Require().NoError(err)
		total += count
	}
	return total
}

// status is the status of a call, for an assertion that reads only that.
func (s *AgentTeamsSuite) status(status int, _ []byte) int {
	return status
}

// inTeams is a new end user of the app whose Stream user is in teams, none for no teams.
func (s *AgentTeamsSuite) inTeams(teams ...string) *testClient {
	person := s.data.createUser()
	user := map[string]any{"id": person.userID}
	if len(teams) > 0 {
		listed := make([]any, 0, len(teams))
		for _, team := range teams {
			listed = append(listed, team)
		}
		user["teams"] = listed
	}
	s.teamsChat.PutUser(user)
	return person
}

// configOf is a new agent config of the app tagged with team, untagged for "".
func (s *AgentTeamsSuite) configOf(team string) AgentConfig {
	tags := map[string]string{}
	if team != "" {
		tags["team"] = team
	}
	var created AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", AgentConfigRequest{
		Name: "agent-" + s.utils.uuid(), Llm: pointerTo("en-low-latency"), Tags: &tags,
	}, &created))
	return created
}

// naming is an incognito text session with config, which opens no Stream channel and so
// asks Stream nothing about its users: the only user query it makes is the teams read.
func (s *AgentTeamsSuite) naming(config AgentConfig) CreateSessionRequest {
	request := s.recorded(config)
	request.Incognito = pointerTo(true)
	return request
}

// recorded is a text session with config that is kept, for a test that needs its row.
func (s *AgentTeamsSuite) recorded(config AgentConfig) CreateSessionRequest {
	request := textSession(nil)
	request.ConfigId = &config.Id
	return request
}

// teamReads is how many user queries the suite's Stream has been sent.
func (s *AgentTeamsSuite) teamReads() int {
	return s.teamReadsOn(teamsAppKey)
}

func (s *AgentTeamsSuite) teamReadsOn(key string) int {
	read := 0
	for _, request := range s.teamsChat.Requests(key) {
		if request.Method == http.MethodGet && strings.HasSuffix(request.Path, "/users") {
			read++
		}
	}
	return read
}
