//go:build integration

package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// A guest this router minted holds a token that names no role, so it authenticates as a full
// user does, and the router knows it for a guest by its row.

// mintedGuest is a guest of the suite's app the router minted, holding the token it was handed.
func (s *RouterSuite) mintedGuest() *testClient {
	var minted GuestUser
	s.Require().Equal(http.StatusCreated, s.data.createAnonymous().do(http.MethodPost, "/v1/agents/guests",
		GuestUserRequest{}, &minted))
	client := s.data.signedInAs(minted.Id)
	client.token = minted.Token
	return client
}

// recordedGuest is a minted guest of an app that no longer admits guests, so it is not minted
// over HTTP.
func (s *RouterSuite) recordedGuest() *testClient {
	id := guestPrefix + s.utils.uuid()
	s.Require().NoError(s.store.Users.InsertGuest(context.Background(), &store.User{
		ID: id, CustomerID: s.customerID(), Name: "Guest",
	}))
	return s.data.signedInAs(id)
}

// appsOwnUser is a user of the app whose id happens to start as a minted guest's does.
func (s *RouterSuite) appsOwnUser() *testClient {
	return s.data.signedInAs(guestPrefix + "app-" + s.utils.uuid())
}

// offeredTo is the tools a new session on the config offers the user once they say something.
func (s *ScheduledTasksSuite) offeredTo(user *testClient, configID string) []string {
	opened := s.openScheduling(user, configID, nil)
	watching := s.watch(user, opened.Id)
	s.say(user, opened.Id, "what can you do")
	s.await(watching, "responded")
	return s.model().toolsOffered()
}

func (s *ScheduledTasksSuite) TestAMintedGuestIsOfferedNoSchedulingTools() {
	config := s.agentWithScheduling(nil)

	s.NotContains(s.offeredTo(s.mintedGuest(), config), "schedule_task")
	s.NotContains(s.offeredTo(s.data.createGuest(), config), "schedule_task", "nor is a guest by role")
	s.Contains(s.offeredTo(s.data.createUser(), config), "schedule_task")
	s.Contains(s.offeredTo(s.appsOwnUser(), config), "schedule_task", "an app's own user may be called guest-")
}

func (s *ScheduledTasksSuite) TestABackendActingForAMintedGuestIsOfferedTheSchedulingTools() {
	config, guest := s.agentWithScheduling(nil), s.mintedGuest()
	opened := s.serverClient.actingFor(guest).createSession(CreateSessionRequest{ConfigId: &config})
	watching := s.watch(guest, opened.Id)

	s.say(guest, opened.Id, "what can you do")
	s.await(watching, "responded")

	s.Contains(s.model().toolsOffered(), "schedule_task")
}

func (s *ScheduledTasksSuite) TestAForkAMintedGuestMakesOffersNoSchedulingTools() {
	config, guest := s.agentWithScheduling(nil), s.mintedGuest()
	opened := s.openScheduling(guest, config, nil)
	var forked Session
	s.Require().Equal(http.StatusCreated, guest.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/fork", ForkSessionRequest{}, &forked))
	watching := s.watch(guest, forked.Id)

	s.say(guest, forked.Id, "what can you do")
	s.await(watching, "responded")

	s.NotContains(s.model().toolsOffered(), "schedule_task")
}

func (s *ScheduledTasksSuite) TestWhileAGuestsRowCannotBeReadAnIdAGuestCouldHaveIsOfferedNoSchedulingTools() {
	config, guest, own, other := s.agentWithScheduling(nil), s.mintedGuest(), s.appsOwnUser(), s.data.createUser()
	s.failQueries("(minted)")

	s.NotContains(s.offeredTo(guest, config), "schedule_task")
	s.NotContains(s.offeredTo(own, config), "schedule_task")
	s.Contains(s.offeredTo(other, config), "schedule_task", "no other id is looked up")

	s.failing.value.Store(nil)
	s.Contains(s.offeredTo(own, config), "schedule_task", "the failure is not kept")
	s.NotContains(s.offeredTo(guest, config), "schedule_task")
}

// guestsTask is a task of the guest's, scheduled before minted guests stopped being offered the
// tools, with no session yet.
func (s *ScheduledTasksSuite) guestsTask(guest *testClient, configID string) store.ScheduledTask {
	next := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Second)
	task := store.ScheduledTask{
		CustomerID: s.customerID(), UserID: guest.userID, CallerKind: "authenticated", APIKey: s.app.key,
		ConfigID: configID, Title: "Stretch", Instructions: "Remind them to stretch.",
		StartLocal: next, Timezone: "UTC", RRule: "FREQ=DAILY", NextRunAt: &next,
		State: store.ScheduledTaskActive, Channels: []string{store.ScheduledTaskDeliveredConversation}, CreatedBy: "agent",
	}
	s.Require().NoError(s.store.ScheduledTasks.Insert(context.Background(), &task, 25))
	return task
}

func (s *ScheduledTasksSuite) TestAMintedGuestsTaskStillRunsInSessionsThatOfferThemNoSchedulingTools() {
	config, guest := s.agentWithScheduling(nil), s.mintedGuest()
	task := s.guestsTask(guest, config)
	s.runWorker()

	s.makeDue(task)
	s.delivered(task)

	s.awaitHeard("Remind them to stretch.", "the task never ran")
	s.NotContains(s.model().toolsOffered(), "schedule_task", "in the session its first run opened")

	stored := s.taskReloaded(task)
	owner := session.Owner{CustomerID: s.customerID(), UserID: guest.userID, Kind: auth.KindAuthenticated}
	_, err := s.router.sessions.Close(stored.SessionID, owner)
	s.Require().NoError(err)
	s.makeDue(s.taskReloaded(task))
	s.Require().Eventually(func() bool {
		runs := s.runsOf(task)
		return len(runs) == 2 && runs[0].State == store.ScheduledTaskRunDelivered
	}, 30*time.Second, 100*time.Millisecond)
	s.awaitHeard("Remind them to stretch.", "the second run never ran")
	live, running := s.router.sessions.Get(stored.SessionID, owner)
	s.Require().True(running, "the second run carried the conversation on")
	s.True(live.Spec().Caller.Guest)
	s.NotContains(s.model().toolsOffered(), "schedule_task", "in the session a later run carried on")
}

func (s *ScheduledTasksSuite) TestACallAMintedGuestsTaskPlacesIsHeldWithThemAsAGuest() {
	held := func(user *testClient, kind auth.Kind) bool {
		return s.router.callSpec(context.Background(), store.AgentConfig{}, scheduledDelivery{
			CustomerID: s.customerID(), UserID: user.userID, CallerKind: string(kind),
		}, "agent", phone.Placed{}, "+15550100100", "+15550100101").Caller.Guest
	}

	s.True(held(s.mintedGuest(), auth.KindAuthenticated))
	s.True(held(s.data.createUser(), auth.KindGuest))
	s.False(held(s.data.createUser(), auth.KindAuthenticated))
	s.False(held(s.mintedGuest(), auth.KindServer), "a task their backend made")
}

func (s *ScheduledTasksSuite) TestACallATaskPlacesIsTheRoutersActingForItsUser() {
	for _, user := range []*testClient{s.data.createUser(), s.mintedGuest()} {
		caller := s.router.callSpec(context.Background(), store.AgentConfig{}, scheduledDelivery{
			CustomerID: s.customerID(), UserID: user.userID, CallerKind: string(auth.KindAuthenticated),
		}, "agent", phone.Placed{}, "+15550100100", "+15550100101").Caller

		s.True(caller.ServerSide)
		s.Equal(user.userID, caller.UserID)
	}
}

func (s *ScheduledTasksSuite) TestAMintedGuestManagesATaskTheyHaveOverREST() {
	guest := s.mintedGuest()
	task := s.guestsTask(guest, s.agentWithScheduling(nil))
	path := "/v1/agents/scheduled-tasks/" + task.ID

	var listed ScheduledTaskPage
	s.Require().Equal(http.StatusOK, guest.do(http.MethodGet, "/v1/agents/scheduled-tasks", nil, &listed))
	s.Require().Len(listed.Items, 1)
	s.Equal(task.ID, listed.Items[0].Id)
	s.Equal(http.StatusOK, guest.do(http.MethodGet, path, nil, nil))
	s.Equal(http.StatusOK, guest.do(http.MethodPost, path+"/pause", nil, nil))
	s.Equal(http.StatusOK, guest.do(http.MethodPost, path+"/resume", nil, nil))
	s.Equal(http.StatusOK, guest.do(http.MethodPatch, path, map[string]any{"title": "Walk"}, nil))
	s.Equal(http.StatusNoContent, guest.do(http.MethodDelete, path, nil, nil))
}

// MintedGuestsSuite covers what a minted guest may reach, asked of two routers.
type MintedGuestsSuite struct {
	RouterSuite
	other *httptest.Server
}

func TestMintedGuestsSuite(t *testing.T) {
	runSuite(t, new(MintedGuestsSuite))
}

func (s *MintedGuestsSuite) SetupSuite() {
	s.shard, s.otherShard = "here", "there"
	s.RouterSuite.SetupSuite()
	s.other = s.otherNode()
}

func (s *MintedGuestsSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

// closedToGuests makes the suite's app one that takes no guests.
func (s *MintedGuestsSuite) closedToGuests() {
	no := false
	s.useApp(s.data.createAppAdmitting(store.AppSettings{AllowGuest: &no}))
}

func (s *MintedGuestsSuite) lists(as *testClient) int {
	return as.do(http.MethodPost, "/v1/agents/sessions/query", SessionQuery{}, nil)
}

func (s *MintedGuestsSuite) TestAMintedGuestOfAnAppThatAdmitsGuestsIsServed() {
	guest := s.mintedGuest()

	s.Equal(http.StatusOK, s.lists(guest))
	s.Equal(http.StatusOK, s.lists(guest.on(s.other)))
}

func (s *MintedGuestsSuite) TestAnAppThatTakesNoGuestsRefusesTheOnesTheRouterMinted() {
	s.closedToGuests()
	guest := s.recordedGuest()

	status, failure := guest.failure(http.MethodPost, "/v1/agents/sessions/query", SessionQuery{})
	s.Equal(http.StatusForbidden, status)
	s.Contains(failure, "this app does not accept requests from this level of user")
	s.Equal(http.StatusForbidden, s.lists(guest.on(s.other)))
	s.Equal(http.StatusForbidden, s.lists(s.data.createGuest()))
	_, status = guest.watch("/v1/agents/sessions/any/events")
	s.Equal(http.StatusForbidden, status)
}

func (s *MintedGuestsSuite) TestAnAppThatTakesNoGuestsServesEverybodyElse() {
	s.closedToGuests()
	guest := s.recordedGuest()

	s.Equal(http.StatusOK, s.lists(s.data.createUser()))
	s.Equal(http.StatusOK, s.lists(s.appsOwnUser()))
	s.Equal(http.StatusOK, s.lists(s.serverClient.actingFor(guest)))
	s.Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/guests/claim",
		ClaimGuestRequest{GuestId: guest.userID, UserId: s.utils.uuid()}, nil))
}

func (s *MintedGuestsSuite) TestOneAppsGuestIsNotAnothersWhateverTheirSettings() {
	guest := s.recordedGuest()
	s.closedToGuests()

	s.Equal(http.StatusOK, s.lists(s.data.signedInAs(guest.userID)))
}

func (s *MintedGuestsSuite) TestAClaimedGuestsTokenIsStillAGuests() {
	guest := s.recordedGuest()
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/guests/claim",
		ClaimGuestRequest{GuestId: guest.userID, UserId: s.utils.uuid()}, nil))

	s.True(s.router.guestUser(context.Background(), s.customerID(), guest.userID, auth.KindAuthenticated))
}

func (s *MintedGuestsSuite) TestWhileAGuestsRowCannotBeReadAnAppThatTakesNoGuestsRefusesTheId() {
	s.closedToGuests()
	own, account := s.appsOwnUser(), s.data.createUser()
	s.failQueries("(minted)")

	s.Equal(http.StatusForbidden, s.lists(own))
	s.Equal(http.StatusOK, s.lists(account))

	s.failing.value.Store(nil)
	s.Equal(http.StatusOK, s.lists(own), "the failure is not kept")
}

func (s *MintedGuestsSuite) TestAnAppThatAdmitsGuestsServesThemWhileTheirRowCannotBeRead() {
	guest := s.mintedGuest()
	s.failQueries("(minted)")

	s.Equal(http.StatusOK, s.lists(guest))
}

func (s *MintedGuestsSuite) TestWhileSettingsCannotBeReadAnAppTakesGuestsAsItsKeySays() {
	guest := s.recordedGuest()
	s.Require().Equal(http.StatusOK, s.lists(s.data.createUser()))
	s.failQueries(`"ap"."settings"`)
	s.Equal(http.StatusOK, s.lists(guest))

	s.failing.value.Store(nil)
	s.closedToGuests()
	guest = s.recordedGuest()
	s.Require().Equal(http.StatusOK, s.lists(s.data.createUser()))
	s.failQueries(`"ap"."settings"`)
	s.Equal(http.StatusForbidden, s.lists(guest))
}

func (s *MintedGuestsSuite) TestBehindAProxyAMintedGuestIsAnsweredA500WhileSettingsCannotBeRead() {
	s.closedToGuests()
	minted, own := s.recordedGuest().userID, guestPrefix+"app-"+s.utils.uuid()
	guest, other, backend := s.behindProxy(minted, auth.AuthTypeJWT), s.behindProxy(own, auth.AuthTypeJWT), s.behindProxy(minted, auth.AuthTypeServer)
	s.failQueries(`"ap"."settings"`)

	status, body := guest.call(http.MethodPost, "/v1/agents/sessions/query", SessionQuery{})
	s.Equal(http.StatusInternalServerError, status)
	s.Contains(string(body), "something went wrong")
	s.NotContains(string(body), "level of user", "one answer")
	s.Equal(http.StatusOK, s.lists(other))
	s.Equal(http.StatusOK, s.lists(backend))

	s.failing.value.Store(nil)
	s.Equal(http.StatusForbidden, s.lists(guest), "the failure is not kept")
}

// admitGuests turns guests back on for the suite's app, and ends the minute-long copies of what
// it takes, which Redis does itself in production.
func (s *MintedGuestsSuite) admitGuests() {
	ctx := context.Background()
	_, err := s.store.GlobalDB().ExecContext(ctx, "UPDATE apps SET settings = '{}' WHERE id = ?", s.customerID())
	s.Require().NoError(err)
	client := s.live.Redis()
	for _, key := range []string{"appcfg:key:" + s.app.key, "appcfg:app-guests:" + s.customerID()} {
		left, err := client.Do(ctx, client.B().Pttl().Key(key).Build()).AsInt64()
		s.Require().NoError(err)
		s.LessOrEqual(left, int64(60_000), key)
		s.Require().NoError(client.Do(ctx, client.B().Del().Key(key).Build()).Error())
	}
}

func (s *MintedGuestsSuite) TestAnAppThatTurnsGuestsBackOnServesANewOneWithinTheMinute() {
	s.closedToGuests()
	_, err := s.configs.Apps.GetSettings(context.Background(), s.customerID())
	s.Require().NoError(err)
	s.Require().Equal(http.StatusForbidden, s.lists(s.recordedGuest()))

	s.admitGuests()

	s.Eventually(func() bool { return s.lists(s.mintedGuest()) == http.StatusOK }, settleFor, 20*time.Millisecond)
}

func (s *MintedGuestsSuite) TestAnAppThatTurnsGuestsBackOnServesANewOneWithinTheMinuteBehindAProxy() {
	s.closedToGuests()
	_, err := s.configs.Apps.GetSettings(context.Background(), s.customerID())
	s.Require().NoError(err)
	s.Require().Equal(http.StatusForbidden, s.lists(s.behindProxy(s.recordedGuest().userID, auth.AuthTypeJWT)))

	s.admitGuests()

	next := s.behindProxy(s.mintedGuest().userID, auth.AuthTypeJWT)
	s.Eventually(func() bool { return s.lists(next) == http.StatusOK }, settleFor, 20*time.Millisecond)
}

func (s *MintedGuestsSuite) TestARefusedGuestOpensNoSession() {
	s.closedToGuests()

	s.Equal(http.StatusForbidden, s.recordedGuest().do(http.MethodPost, "/v1/agents/sessions", textSession(nil), nil))

	s.Empty(s.serverClient.querySessions(SessionQuery{}).Items)
}

func (s *MintedGuestsSuite) TestAnAppThatTakesNoGuestsServesAnAnonymousCaller() {
	s.closedToGuests()

	s.Equal(http.StatusOK, s.lists(s.data.createAnonymous()))
}

func (s *MintedGuestsSuite) TestWhileNoRowCanBeReadABackendMintsAndClaimsGuests() {
	guest := s.recordedGuest()
	s.failQueries("(minted)")

	s.mintedGuest()
	s.Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/guests/claim",
		ClaimGuestRequest{GuestId: guest.userID, UserId: s.utils.uuid()}, nil))
}

// The key says which levels its app takes sooner than the cached settings do.
func (s *MintedGuestsSuite) TestAGuestByRoleIsAdmittedAsTheirKeySays() {
	s.closedToGuests()
	settings, err := s.configs.Apps.GetSettings(context.Background(), s.customerID())
	s.Require().NoError(err)
	s.Require().False(settings.GuestAllowed())
	_, err = s.store.GlobalDB().ExecContext(context.Background(), "UPDATE apps SET settings = '{}' WHERE id = ?", s.customerID())
	s.Require().NoError(err)

	s.Equal(http.StatusOK, s.lists(s.data.createGuest()))
}

// behindProxy is a client of a second router, one behind a proxy that declares kinds, as the user.
func (s *MintedGuestsSuite) behindProxy(userID, kind string) *testClient {
	router, _, _ := s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler), func(o *Options) {
		o.Auth, o.AuthMode, o.ProxyDeclaresKind = auth.NewProxy(auth.ProxyOptions{DeclaresKind: true}), auth.Proxy, true
	})
	header := http.Header{}
	header.Set(auth.AppHeader, s.customerID())
	header.Set(auth.UserHeader, userID)
	header.Set(auth.AuthTypeHeader, kind)
	return (&testClient{suite: &s.RouterSuite, header: header, userID: userID}).on(router)
}

func (s *MintedGuestsSuite) TestAnAppThatTakesNoGuestsRefusesTheOnesTheRouterMintedBehindAProxy() {
	s.closedToGuests()
	guest := s.recordedGuest().userID

	s.Equal(http.StatusForbidden, s.lists(s.behindProxy(guest, auth.AuthTypeJWT)))
	s.Equal(http.StatusOK, s.lists(s.behindProxy(guest, auth.AuthTypeServer)))
	s.Equal(http.StatusOK, s.lists(s.behindProxy(s.utils.uuid(), auth.AuthTypeJWT)))
	s.Equal(http.StatusOK, s.lists(s.behindProxy(guestPrefix+"app-"+s.utils.uuid(), auth.AuthTypeJWT)))
}

func (s *MintedGuestsSuite) TestAMintedGuestIsServedBehindAProxyByAnAppThatAdmitsGuests() {
	s.Equal(http.StatusOK, s.lists(s.behindProxy(s.recordedGuest().userID, auth.AuthTypeJWT)))
}

func (s *MintedGuestsSuite) TestAMintedGuestsSessionsAreKeptAsAnAuthenticatedUsers() {
	guest := s.mintedGuest()
	opened := guest.createSession(textSession(nil))
	sameIDByRole := s.signedIn(jwt.MapClaims{"user_id": guest.userID, "role": "guest"}, auth.AuthTypeJWT, user, guest.userID)

	s.Equal(opened.Id, guest.getSession(opened.Id).Id)
	s.Equal(http.StatusNotFound, sameIDByRole.do(http.MethodGet, "/v1/agents/sessions/"+opened.Id, nil, nil))
	live, running := s.sessionsHere().Get(opened.Id, session.Owner{CustomerID: s.customerID(), UserID: guest.userID, Kind: auth.KindAuthenticated})
	s.Require().True(running)
	s.Equal("authenticated", string(live.Spec().CallerKind))
	s.True(live.Spec().Caller.Guest)
	s.Equal("user", string(live.Spec().Attribution.CallerKind), "and its usage as a user's")
	guest.stopSession(opened.Id)
}

func (s *MintedGuestsSuite) sessionsHere() *session.Manager { return s.router.sessions }

// The connector tests run beside the ones of sessions the router opens, whose suite holds the fake.
func (s *RouterOpenedConnectorsSuite) TestAMintedGuestUsesNoConnectionOfTheirOwn() {
	connector := s.connector()
	guest := s.mintedGuest()
	grant := s.grant(s.userConnection(connector, guest.userID))
	owner := s.data.createUser()
	s.userConnection(connector, owner.userID)
	config := func(required bool) string {
		bound := binding(connector, grant)
		bound[0]["required"] = required
		var created AgentConfig
		s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", map[string]any{
			"name": "bound-" + s.utils.uuid(), "mode": "text", "llm": "logging-in/login-model", "connectors": bound,
		}, &created))
		return created.Id
	}
	optional, required := config(false), config(true)

	opened := guest.createSession(CreateSessionRequest{ConfigId: &optional})
	s.Equal("caller_unverified", s.await(guest.opens("/v1/agents/sessions/"+opened.Id+"/events"), "connector_unavailable")["reason"])
	status, failure := guest.failure(http.MethodPost, "/v1/agents/sessions", CreateSessionRequest{ConfigId: &required})
	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "caller_unverified")

	owner.createSession(CreateSessionRequest{ConfigId: &required})
	s.serverClient.actingFor(guest).createSession(CreateSessionRequest{ConfigId: &required})
}
