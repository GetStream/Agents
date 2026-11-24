//go:build integration

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
)

// SessionOwnershipSuite covers whose conversation a session is: who reaches it, who is
// told there is no such thing, and what a name is worth without a token behind it.
type SessionOwnershipSuite struct {
	RouterSuite
}

func TestSessionOwnershipSuite(t *testing.T) {
	runSuite(t, new(SessionOwnershipSuite))
}

func (s *SessionOwnershipSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

// An empty id is the mistake of the caller, not a failure of the router.
func (s *SessionOwnershipSuite) TestAnEmptySessionIdIsNoSession() {
	_, status := s.data.createUser().watch("/v1/agents/sessions//events")

	s.Equal(http.StatusNotFound, status)
}

// A 404 tells the client that the session is gone. The client then drops a conversation that
// is still there.
func (s *SessionOwnershipSuite) TestASessionTheDatabaseCannotReadIsAFailureAndNotAMissingSession() {
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))
	alice.stopSession(opened.Id)
	s.Require().NoError(s.manager.FlushRecords(context.Background()))
	s.failQueries(opened.Id)

	s.Equal(http.StatusInternalServerError, alice.do(http.MethodGet, "/v1/agents/sessions/"+opened.Id, nil, nil))
}

func (s *SessionOwnershipSuite) TestTheUserWhoOpenedASessionReachesItAndMayEndIt() {
	alice := s.data.createUser()

	opened := alice.createSession(textSession(nil))

	s.assertReaches(alice, opened.Id)
	alice.stopSession(opened.Id)
}

func (s *SessionOwnershipSuite) TestASessionClosedOverItsSocketStopsBeingOneOfTheirs() {
	// The socket is how every SDK closes a conversation it is holding. Closing this way
	// once ended the conversation and left it in the manager, which listed it as live for
	// as long as the router ran.
	alice := s.data.createUser()
	opened := alice.createSession(s.onACall())
	watching := alice.opens("/v1/agents/sessions/" + opened.Id + "/events")

	s.Require().NoError(watching.WriteJSON(map[string]any{"type": "close"}))

	// The close is applied on the reading goroutine, so this waits for it rather than
	// assuming the write was enough.
	s.Require().Eventually(func() bool {
		for _, listed := range alice.querySessions(SessionQuery{}).Items {
			if listed.Id == opened.Id {
				return listed.State != Live
			}
		}
		return false
	}, settleFor, 20*time.Millisecond, "it is still listed as a call they are on")

	s.Equal(http.StatusNotFound,
		alice.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/stop", nil, nil),
		"a session that closed itself is over rather than stoppable twice")
}

func (s *SessionOwnershipSuite) TestWritingIntoASessionIsNotForDevicesEvenTheOwners() {
	// Ownership is not the only thing between a device and a session. Everything the spec
	// does not open is server-side whoever asks, so the owner is refused these too, and
	// with a 403 rather than the 404 a stranger gets: what is being withheld is the
	// operation and not the session.
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))

	s.assertReaches(alice, opened.Id)
	s.Equal(http.StatusForbidden,
		alice.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/say",
			SayRequest{Text: "hello"}, nil),
		"putting words in the agent's mouth")
	s.Equal(http.StatusForbidden,
		alice.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/respond",
			SayRequest{Text: "hello"}, nil),
		"making the agent answer")
}

func (s *SessionOwnershipSuite) TestOneUsersConversationIsOutOfAnothersReach() {
	// The whole point of the token: two signed-in users of the same app, and neither can
	// read what the other said.
	opened := s.data.createUser().createSession(textSession(nil))

	s.assertDoesNotReach(s.data.createUser(), opened.Id)
}

func (s *SessionOwnershipSuite) TestClaimingAUsersNameWithoutATokenReachesNothing() {
	// A user id is only worth what proved it. An anonymous caller may name itself
	// anything, so if the name were the whole of the identity, typing somebody else's
	// into a header would be enough to read their conversation.
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))

	s.assertDoesNotReach(s.data.claiming(alice.userID), opened.Id)
}

func (s *SessionOwnershipSuite) TestClaimingAUsersNameWithoutATokenOpensNoneOfTheirConversations() {
	// Naming a conversation's channel takes a verified caller, whatever name the caller goes by.
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))
	alice.stopSession(opened.Id)
	resume := textSession(nil)
	resume.LegacyConversationId = opened.ConversationId

	status, body := s.data.claiming(alice.userID).call(http.MethodPost, "/v1/agents/sessions", resume)

	s.assertConversationRefused(status, body)
}

func (s *SessionOwnershipSuite) TestANamelessCallerOpensNoConversationOfTheBackends() {
	// A conversation a backend opened for no user is the backend's to carry on.
	opened := s.serverClient.createSession(textSession(nil))
	s.serverClient.stopSession(opened.Id)
	s.Require().NotNil(opened.ConversationId)
	resume := textSession(nil)
	resume.LegacyConversationId = opened.ConversationId

	status, body := s.data.claiming("").call(http.MethodPost, "/v1/agents/sessions", resume)

	s.assertConversationRefused(status, body)
}

func (s *SessionOwnershipSuite) TestAnAnonymousCallerNamesNoChannelOfAnyType() {
	resume := textSession(nil)
	resume.LegacyConversationId = pointerTo("messaging:" + s.utils.uuid())

	status, body := s.data.createAnonymous().call(http.MethodPost, "/v1/agents/sessions", resume)

	s.assertConversationRefused(status, body)
}

func (s *SessionOwnershipSuite) TestAnAnonymousCallerSendingAnEmptyConversationIdOpensANewConversation() {
	request := textSession(nil)
	request.LegacyConversationId = pointerTo("")

	opened := s.data.createAnonymous().createSession(request)

	s.Equal("agent:"+opened.Id, value(opened.ConversationId))
}

func (s *SessionOwnershipSuite) TestAGuestCarriesOnItsOwnConversationByItsChannel() {
	// A guest's token is verified, and clients built on an older SDK resume a guest's
	// conversation this way.
	guest := s.data.createGuest()
	opened := guest.createSession(textSession(nil))
	guest.stopSession(opened.Id)
	resume := textSession(nil)
	resume.LegacyConversationId = opened.ConversationId

	resumed := guest.createSession(resume)

	s.Require().NotNil(opened.ConversationId)
	s.Equal(*opened.ConversationId, value(resumed.ConversationId))
}

func (s *SessionOwnershipSuite) TestAnAnonymousCallerCarriesOnItsOwnConversationBySessionId() {
	// conversation_id is not open to it, so the session id is how it goes on with a chat that
	// ended.
	someone := s.data.createAnonymous()
	opened := someone.createSession(textSession(nil))
	someone.stopSession(opened.Id)

	s.Require().Equal(http.StatusAccepted, someone.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses",
		CreateResponseRequest{Text: "Still there?", RequestId: pointerTo(s.utils.uuid())}, nil))

	reopened := someone.getSession(opened.Id)
	s.Require().NotNil(opened.ConversationId)
	s.Equal(*opened.ConversationId, value(reopened.ConversationId))
}

func (s *SessionOwnershipSuite) TestAUsersOwnDeviceCarriesOnTheirConversationByItsChannel() {
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))
	alice.stopSession(opened.Id)
	resume := textSession(nil)
	resume.LegacyConversationId = opened.ConversationId

	resumed := alice.createSession(resume)

	s.Require().NotNil(opened.ConversationId)
	s.Equal(*opened.ConversationId, value(resumed.ConversationId))
}

func (s *SessionOwnershipSuite) TestAGuestIsNotTheSignedInUserOfTheSameName() {
	// Guest ids are issued per device and a customer's user ids are their own, so the two
	// namespaces can collide. Nothing in a token tells them apart, which is why the
	// declaration does.
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))

	s.assertDoesNotReach(s.guestNamed(alice.userID), opened.Id)
}

func (s *SessionOwnershipSuite) TestAGuestReachesItsOwnConversationAndNobodyElsesReachesIt() {
	guest := s.guestNamed(s.utils.uuid())

	opened := guest.createSession(textSession(nil))

	s.assertReaches(guest, opened.Id)
	s.assertDoesNotReach(s.guestNamed(s.utils.uuid()), opened.Id)
	s.assertDoesNotReach(s.data.signedInAs(guest.userID), opened.Id)
}

func (s *SessionOwnershipSuite) TestAGuestDoesNotCarryOnTheConversationOfTheSignedInUserOfTheSameName() {
	alice := s.data.createUser()

	s.refusesToCarryOn(s.guestNamed(alice.userID), s.ended(alice))
}

func (s *SessionOwnershipSuite) TestASignedInUserDoesNotCarryOnTheConversationOfTheGuestOfTheSameName() {
	guest := s.guestNamed(s.utils.uuid())

	s.refusesToCarryOn(s.data.signedInAs(guest.userID), s.ended(guest))
}

func (s *SessionOwnershipSuite) TestAGuestCarriesOnItsConversationWithATokenOfNoRole() {
	var minted GuestUser
	s.Require().Equal(http.StatusCreated,
		s.data.createAnonymous().do(http.MethodPost, "/v1/agents/guests", GuestUserRequest{}, &minted))
	// The token the router mints a guest carries no role, so it reads as a signed-in user's.
	page := s.data.signedInAs(minted.Id)

	s.carriesOn(page, s.ended(page))
}

func (s *SessionOwnershipSuite) TestAGuestOfTheAppsOwnTokenCarriesOnItsConversation() {
	guest := s.data.createGuest()

	s.carriesOn(guest, s.ended(guest))
}

func (s *SessionOwnershipSuite) TestAUserAndAGuestCarryOnTheConversationTheirBackendOpenedForThem() {
	for _, device := range []*testClient{s.data.createUser(), s.data.createGuest()} {
		s.carriesOn(device, s.ended(s.backendFor(device.userID)))
	}
}

func (s *SessionOwnershipSuite) TestABackendCarriesOnTheConversationOfTheUserItNames() {
	for _, device := range []*testClient{s.data.createUser(), s.data.createGuest()} {
		s.carriesOn(s.backendFor(device.userID), s.ended(device))
	}
}

func (s *SessionOwnershipSuite) TestAnAnonymousCallerCarriesOnItsOwnEndedSession() {
	someone := s.data.createAnonymous()
	opened := s.ended(someone)

	s.Require().Equal(http.StatusAccepted, someone.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses",
		CreateResponseRequest{Text: "Still there?", RequestId: pointerTo(s.utils.uuid())}, nil))

	s.Equal(*opened.ConversationId, value(someone.getSession(opened.Id).ConversationId))
}

func (s *SessionOwnershipSuite) TestAUserForksTheConversationTheirBackendOpenedForThem() {
	alice := s.data.createUser()
	opened := s.ended(s.backendFor(alice.userID))

	var forked Session
	s.Require().Equal(http.StatusCreated,
		alice.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/fork", ForkSessionRequest{}, &forked))

	s.NotEqual(value(opened.ConversationId), value(forked.ConversationId))
}

func (s *SessionOwnershipSuite) TestABackendReadsTheConversationOfTheUserItNames() {
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))
	s.Require().NotNil(opened.ConversationId)
	asked := s.utils.uuid()
	s.Require().Equal(http.StatusAccepted, alice.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/responses",
		CreateResponseRequest{Text: "Where is my order?", RequestId: &asked}, nil))
	conversation := "/v1/agents/conversations/" + *opened.ConversationId
	backend := s.backendFor(alice.userID)

	s.Equal(http.StatusOK, backend.do(http.MethodGet, conversation+"/messages?agent_id="+opened.AgentId, nil, nil))
	s.Equal(http.StatusOK, backend.do(http.MethodGet, conversation+"/commands/"+asked+"?agent_id="+opened.AgentId, nil, nil))
}

func (s *SessionOwnershipSuite) TestAnAnonymousCallerReachesTheSessionItOpenedAndNotAnothers() {
	// Anonymous is a supported way to hold a conversation, not a way to be refused one:
	// the name is unverified, so it is kept apart from every verified user of the same
	// name, and from other anonymous callers by being a different name.
	someone := s.data.createAnonymous()

	opened := someone.createSession(textSession(nil))

	s.assertReaches(someone, opened.Id)
	s.assertDoesNotReach(s.data.createAnonymous(), opened.Id)
}

func (s *SessionOwnershipSuite) TestAnAnonymousCallerThatNamesNobodyIsListedNothing() {
	// It has the session id, so it can go on with the conversation it opened. What it
	// cannot have is a listing, because every caller naming nobody is the same owner and
	// a listing would hand one stranger another's conversation.
	nameless := s.data.claiming("")

	opened := nameless.createSession(textSession(nil))

	s.assertReaches(nameless, opened.Id)
	s.Empty(nameless.querySessions(SessionQuery{}).Items)
}

func (s *SessionOwnershipSuite) TestClaimingAUsersNameWithoutATokenListsNoneOfTheirSessions() {
	// The same rule as reaching one by id, over a listing and a search: ended sessions are
	// listed from their rows, which is where the name alone once sufficed.
	alice := s.data.createUser()
	running := alice.createSession(s.titled("Refund for a duplicate charge"))
	ended := alice.createSession(s.titled("Refund for a missing parcel"))
	alice.stopSession(ended.Id)
	claimant := s.data.claiming(alice.userID)

	s.Empty(claimant.querySessions(SessionQuery{}).Items)
	s.Empty(claimant.querySessions(s.searchingFor("refund")).Items)
	s.ElementsMatch([]string{running.Id, ended.Id}, ids(alice.querySessions(SessionQuery{}).Items))
	s.ElementsMatch([]string{running.Id, ended.Id}, ids(alice.querySessions(s.searchingFor("refund")).Items))
}

func (s *SessionOwnershipSuite) TestEachKindOfCallerOfOneNameIsListedOnlyItsOwnSessions() {
	name := s.utils.uuid()
	callers := map[string]*testClient{
		"signed in": s.data.signedInAs(name),
		"guest":     s.guestNamed(name),
		"anonymous": s.data.claiming(name),
	}
	opened := map[string]string{}
	for kind, caller := range callers {
		opened[kind] = caller.createSession(s.titled("Refund")).Id
	}

	for kind, caller := range callers {
		s.Equal([]string{opened[kind]}, ids(caller.querySessions(SessionQuery{}).Items), "the %s caller's list", kind)
		s.Equal([]string{opened[kind]}, ids(caller.querySessions(s.searchingFor("refund")).Items), "the %s caller's search", kind)
	}
	s.Len(s.serverClient.querySessions(SessionQuery{}).Items, len(callers), "the backend lists every one")
}

func (s *SessionOwnershipSuite) TestASessionABackendOpenedForAUserIsListedToThemAndNotToANamesakeWithoutAToken() {
	// A backend opening the conversation and handing it over is the ordinary integration,
	// so a verified user of that name lists it; an unchecked claim to the name does not.
	alice := s.data.createUser()
	handed := s.serverClient.actingFor(alice).createSession(s.titled("Refund"))

	s.Equal([]string{handed.Id}, ids(alice.querySessions(SessionQuery{}).Items))
	s.Equal([]string{handed.Id}, ids(s.guestNamed(alice.userID).querySessions(SessionQuery{}).Items))
	s.Empty(s.data.claiming(alice.userID).querySessions(SessionQuery{}).Items)
	s.Empty(s.data.claiming(alice.userID).querySessions(s.searchingFor("refund")).Items)
}

func (s *SessionOwnershipSuite) TestAnAnonymousCallersPagesAreFullAmongANamesakesSessions() {
	// The namesake's sessions sit between the caller's own, so a page cut before the rows
	// were narrowed by kind would come back short or empty.
	alice := s.data.createUser()
	claimant := s.data.claiming(alice.userID)
	var own []string
	for range 3 {
		alice.createSession(textSession(nil))
		own = append(own, claimant.createSession(textSession(nil)).Id)
		alice.createSession(textSession(nil))
	}
	slices.Reverse(own)

	var listed []string
	var sizes []int
	query := SessionQuery{Limit: 2}
	for {
		page := claimant.querySessions(query)
		listed = append(listed, ids(page.Items)...)
		sizes = append(sizes, len(page.Items))
		if !page.HasMore {
			break
		}
		s.Require().NotNil(page.NextCursor)
		query.Cursor = *page.NextCursor
	}

	s.Equal([]int{2, 1}, sizes)
	s.Equal(own, listed)
}

func (s *SessionOwnershipSuite) TestAServerTokenInASocketsQueryStringIsNotABackend() {
	// A socket's credentials travel in the query string, where a URL is copied into places
	// a header never goes. If that were enough to be server-side, a leaked server token
	// would be every session the customer has; without the header it is a caller naming
	// nobody, which reaches nothing it did not open.
	opened := s.data.createUser().createSession(textSession(nil))
	quiet := *s.serverClient
	quiet.header = s.serverClient.header.Clone()
	quiet.header.Del(auth.AuthTypeHeader)

	_, status := quiet.watch("/v1/agents/sessions/" + opened.Id + "/events")

	s.Equal(http.StatusNotFound, status)
}

func (s *SessionOwnershipSuite) TestABackendOpensASessionForAUserWithTheQueryAndNotTheBody() {
	// The body's user_id is the name the agent joins the call as. The owner comes from the
	// caller's auth, and a backend names the user it acts for in the query, so the user's own
	// device reaches the session and a body-only user_id leaves it with nobody's device.
	named := s.data.createUser()
	bodyOnly := s.data.createUser()
	request := textSession(nil)

	var forQuery, forBody Session
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost,
		"/v1/agents/sessions?"+url.Values{auth.UserParam: {named.userID}}.Encode(), request, &forQuery))
	request.UserId = &bodyOnly.userID
	s.Require().Equal(http.StatusCreated,
		s.serverClient.do(http.MethodPost, "/v1/agents/sessions", request, &forBody))

	_, status := named.watch("/v1/agents/sessions/" + forQuery.Id + "/events")
	s.Equal(http.StatusSwitchingProtocols, status, "the user the query named reaches its socket")
	_, status = bodyOnly.watch("/v1/agents/sessions/" + forBody.Id + "/events")
	s.Equal(http.StatusNotFound, status, "a user only the body named is a stranger to it")
	s.Equal(bodyOnly.userID, forBody.UserId, "the body's user_id is echoed as the agent's name")
}

func (s *SessionOwnershipSuite) TestTheAppsBackendEndsAUsersCallWithoutReadingIt() {
	// A call is kept like a conversation in writing: it is the user's to read, and the
	// application's to clean up after a device that went away.
	alice := s.data.createUser()
	opened := alice.createSession(s.onACall())

	s.assertDoesNotReach(s.serverClient, opened.Id)
	s.serverClient.deleteSession(opened.Id)
	s.assertDoesNotReach(alice, opened.Id)
}

func (s *SessionOwnershipSuite) TestAnotherAppsUserReachesNothingEvenByTheSameName() {
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))

	mine := s.app
	s.app = s.data.createApp()
	stranger := s.data.signedInAs(alice.userID)
	s.app = mine

	s.assertDoesNotReach(stranger, opened.Id)
}

func (s *SessionOwnershipSuite) TestASessionWithNoTokenAtAllIsRefusedBeforeItIsOwned() {
	// The ownership rules are about telling verified callers apart. A caller with no
	// credential never gets far enough to have any.
	status, _ := s.unauthenticatedClient.call(http.MethodPost, "/v1/agents/sessions/query", nil)

	s.Equal(http.StatusUnauthorized, status)
}

// assertConversationRefused checks a create was refused for the conversation it named.
func (s *SessionOwnershipSuite) assertConversationRefused(status int, body []byte) {
	s.Require().Equal(http.StatusForbidden, status, "%s", body)
	var refused ErrorResponse
	s.Require().NoError(json.Unmarshal(body, &refused))
	s.Equal("forbidden", refused.Error.Code)
	s.Contains(refused.Error.Message, "conversation_id")
}

// onACall is a session with an agent in a call, which records no conversation of its own.
func (s *SessionOwnershipSuite) onACall() CreateSessionRequest {
	return CreateSessionRequest{StartVoice: pointerTo(true)}
}

// guestNamed is a client holding a guest token that goes by name, for the collisions
// between a guest id and one of the customer's own user ids.
func (s *SessionOwnershipSuite) guestNamed(name string) *testClient {
	return s.signedIn(jwt.MapClaims{"user_id": name, "role": "guest"}, auth.AuthTypeJWT, guest, name)
}

// ended is a written session caller opened, over now.
func (s *SessionOwnershipSuite) ended(caller *testClient) Session {
	opened := caller.createSession(textSession(nil))
	caller.stopSession(opened.Id)
	s.Require().NotNil(opened.ConversationId)
	return opened
}

// backendFor is the app's backend, acting for the user.
func (s *SessionOwnershipSuite) backendFor(userID string) *testClient {
	backend := s.signedIn(jwt.MapClaims{"server": true}, auth.AuthTypeServer, server, "")
	backend.header.Set(auth.UserHeader, userID)
	return backend
}

// resuming is a written session on the conversation opened was held on.
func (s *SessionOwnershipSuite) resuming(opened Session) CreateSessionRequest {
	request := textSession(nil)
	request.LegacyConversationId = opened.ConversationId
	return request
}

func (s *SessionOwnershipSuite) carriesOn(caller *testClient, opened Session) {
	s.Equal(*opened.ConversationId, value(caller.createSession(s.resuming(opened)).ConversationId))
}

func (s *SessionOwnershipSuite) refusesToCarryOn(caller *testClient, opened Session) {
	status, body := caller.call(http.MethodPost, "/v1/agents/sessions", s.resuming(opened))

	s.Equal(http.StatusBadRequest, status, "%s", body)
	s.Contains(string(body), "another user")
}

// titled is a written session with a title a search can find it by.
func (s *SessionOwnershipSuite) titled(title string) CreateSessionRequest {
	request := textSession(nil)
	request.Title = &title
	return request
}

// searchingFor is a query for the sessions whose title holds the words.
func (s *SessionOwnershipSuite) searchingFor(words string) SessionQuery {
	return SessionQuery{Filter: sessionFilter(map[string]any{"text": map[string]any{"$q": words}})}
}
