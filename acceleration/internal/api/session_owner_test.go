//go:build integration

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"
	"github.com/golang-jwt/jwt/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// SessionOwnershipSuite covers whose conversation a session is: who reaches it, who is
// told there is no such thing, and what a name is worth without a token behind it.
type SessionOwnershipSuite struct {
	RouterSuite
}

func TestSessionOwnershipSuite(t *testing.T) {
	runSuite(t, new(SessionOwnershipSuite))
}

func (s *SessionOwnershipSuite) SetupSuite() {
	s.deploymentKnown = true
	s.RouterSuite.SetupSuite()
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

// A user reopening their own conversation, which a session nobody is watching still holds,
// carries on in a new session and ends the one left behind.
func (s *SessionOwnershipSuite) TestReopeningYourOwnConversationEndsTheSessionNobodyWatches() {
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))
	s.Require().NotNil(opened.ConversationId)

	reopen := textSession(nil)
	reopen.LegacyConversationId = opened.ConversationId
	reopened := alice.createSession(reopen)

	s.Equal(*opened.ConversationId, value(reopened.ConversationId))
	s.False(s.manager.Running(s.customerID(), opened.Id), "the session left behind is ended")
	s.True(s.manager.Running(s.customerID(), reopened.Id))
}

// Another user naming a conversation that is not theirs is told it is open, as a watched
// one is answered, and the session holding it carries on, unwatched or not.
func (s *SessionOwnershipSuite) TestAnotherUsersReopenLeavesTheSessionHoldingTheConversationRunning() {
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))
	s.Require().NotNil(opened.ConversationId)

	reopen := textSession(nil)
	reopen.LegacyConversationId = opened.ConversationId
	status, message := s.data.createUser().failure(http.MethodPost, "/v1/agents/sessions", reopen)

	s.Equal(http.StatusConflict, status, message)
	s.True(s.manager.Running(s.customerID(), opened.Id), "alice's session is still running")
	s.assertReaches(alice, opened.Id)
}

// A guest going by a signed-in user's id does not reach their session, so it is told the
// conversation is open and the session carries on.
func (s *SessionOwnershipSuite) TestAGuestOfTheSameNameLeavesTheSessionHoldingAUsersConversationRunning() {
	alice := s.data.createUser()
	opened := alice.createSession(textSession(nil))
	s.Require().NotNil(opened.ConversationId)

	reopen := textSession(nil)
	reopen.LegacyConversationId = opened.ConversationId
	status, message := s.guestNamed(alice.userID).failure(http.MethodPost, "/v1/agents/sessions", reopen)

	s.Equal(http.StatusConflict, status, message)
	s.True(s.manager.Running(s.customerID(), opened.Id), "alice's session is still running")
}

// And the other way about: the user is not the guest of their name.
func (s *SessionOwnershipSuite) TestAUserIsToldAGuestOfTheirNameHoldsTheConversationOpen() {
	alice := s.data.createUser()
	opened := s.guestNamed(alice.userID).createSession(textSession(nil))
	s.Require().NotNil(opened.ConversationId)

	reopen := textSession(nil)
	reopen.LegacyConversationId = opened.ConversationId
	status, message := alice.failure(http.MethodPost, "/v1/agents/sessions", reopen)

	s.Equal(http.StatusConflict, status, message)
	s.True(s.manager.Running(s.customerID(), opened.Id), "the guest's session is still running")
}

// A chat whose conversation another session holds open is answered as a create of it is.
func (s *SessionOwnershipSuite) TestCarryingOnAChatWhoseConversationIsOpenElsewhereIsAConflict() {
	alice := s.data.createUser()
	ended := alice.createSession(textSession(nil))
	alice.stopSession(ended.Id)
	request := textSession(nil)
	request.LegacyConversationId = ended.ConversationId
	holding := alice.createSession(request)
	watching := alice.opens("/v1/agents/sessions/" + holding.Id + "/events")
	defer func() { _ = watching.Close() }()

	status, body := alice.call(http.MethodPost, "/v1/agents/sessions/"+ended.Id+"/responses",
		CreateResponseRequest{Text: "Still there?", RequestId: pointerTo(s.utils.uuid())})

	s.Equal(http.StatusConflict, status, string(body))
	s.True(s.manager.Running(s.customerID(), holding.Id))
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

// A browser chat whose every visitor talks to one named agent opens each visitor's session under
// the same agent id. The agent id is who writes in a conversation, not whose it is: the visitor
// is, so one visitor reaches nothing of another's.
func (s *SessionOwnershipSuite) TestGuestsOfOneAgentIdReachNothingOfEachOthers() {
	agentID := "agent-" + s.utils.uuid()
	alice, bob := s.data.createGuest(), s.data.createGuest()
	opened := func(guest *testClient) Session {
		request := textSession(nil)
		request.AgentId = &agentID
		return s.backendFor(guest.userID).createSession(request)
	}
	alices, bobs := opened(alice), opened(bob)
	s.Require().Equal(agentID, bobs.AgentId)

	s.assertSharedAgentKeepsGuestsApart(alice, alices, bobs)
	bob.stopSession(bobs.Id)
	s.refusesToCarryOn(alice, bobs)
	s.carriesOn(bob, bobs)
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

// A conversation made before channels recorded their owner's kind is its owner's by id alone, so
// a guest going by a signed-in user's id carries it on. Once the backfill records the kind, the
// guest is refused, their backend reads it, and the user carries it on.
func (s *SessionOwnershipSuite) TestABackfilledUsersConversationIsRefusedToAGuestOfTheirId() {
	alice := s.data.createUser()
	opened := s.withNoKindRecorded(s.ended(alice), alice.userID)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 1}, s.backfilled(true))

	s.refusesToCarryOn(s.guestNamed(alice.userID), opened)
	s.Equal(http.StatusOK, s.backendFor(alice.userID).do(http.MethodGet, messagesOf(opened), nil, nil))
	s.carriesOn(alice, opened)
}

// The same for a guest's conversation and a signed-in user of the guest's id.
func (s *SessionOwnershipSuite) TestABackfilledGuestsConversationIsRefusedToAUserOfTheirId() {
	guest := s.data.createGuest()
	opened := s.withNoKindRecorded(s.ended(guest), guest.userID)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 1}, s.backfilled(true))

	s.refusesToCarryOn(s.data.signedInAs(guest.userID), opened)
	s.carriesOn(guest, opened)
}

// A guest whose token said the role came back from a refresh with a token that said none, before
// a refreshed token kept it, and held a conversation as a signed-in user. Their token says the role
// again now, so that conversation is left as it is, and they carry it on.
func (s *SessionOwnershipSuite) TestAConversationAGuestHeldWithATokenOfNoRoleIsLeftAlone() {
	id := s.utils.uuid()
	s.heldAs(s.ended(s.guestNamed(id)), id, auth.KindGuest)
	opened := s.withNoKindRecorded(s.ended(s.data.signedInAs(id)), id)

	s.Equal(map[conversation.OwnerKindBackfill]int{
		conversation.OwnerKindLeftAlone: 1, conversation.OwnerKindRecorded: 1,
	}, s.backfilled(true))

	s.carriesOn(s.guestNamed(id), opened)
}

// A guest the router minted carries no role in its tokens, refreshed or not, so its conversation
// is recorded as a signed-in user's, and it carries it on.
func (s *SessionOwnershipSuite) TestAMintedGuestCarriesOnItsBackfilledConversation() {
	minted := s.mintedGuest()
	opened := s.withNoKindRecorded(s.ended(s.data.signedInAs(minted)), minted)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 1}, s.backfilled(true))

	s.carriesOn(s.data.signedInAs(minted), opened)
}

// A minted guest of no record of it reads as one whose app signed its token: its refresh says the
// role, though its sessions say none. Its conversation is left as it is, and the token it is
// refreshed to carries it on.
func (s *SessionOwnershipSuite) TestAConversationOfAMintedGuestFromBeforeTheRecordIsLeftAloneInAnOwnApp() {
	s.inAnAppOfItsOwn()
	s.TestAConversationOfAMintedGuestFromBeforeTheRecordIsLeftAlone()
}

func (s *SessionOwnershipSuite) TestAConversationOfAMintedGuestFromBeforeTheRecordIsLeftAlone() {
	minted := s.mintedGuest()
	opened := s.withNoKindRecorded(s.ended(s.data.signedInAs(minted)), minted)
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.User)(nil)).Set("minted = false").
		Where("customer_id = ?", s.customerID()).Where("id = ?", minted).Exec(s.T().Context())
	s.Require().NoError(err)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindLeftAlone: 1}, s.backfilled(true))

	s.carriesOn(s.guestNamed(minted), opened)
}

// A minted guest an import brought is refreshed to a token of no role in its customer's own Stream
// app, so its conversation is recorded as a signed-in user's, and it carries it on.
func (s *SessionOwnershipSuite) TestAMintedGuestAnImportBroughtCarriesOnItsBackfilledConversation() {
	s.inAnAppOfItsOwn()
	minted := s.mintedGuest()
	opened := s.withNoKindRecorded(s.ended(s.data.signedInAs(minted)), minted)
	s.importedUsersAgain(minted)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 1}, s.backfilled(true))

	s.carriesOn(s.refreshedByItsBackend(minted), opened)
}

// In an app shared with other customers a guest an import brought is refreshed to the role, so its
// conversation is left as it is, and it carries it on with the token of that role.
func (s *SessionOwnershipSuite) TestAConversationOfAMintedGuestAnImportBroughtToASharedAppIsLeftAlone() {
	minted := s.mintedGuest()
	opened := s.withNoKindRecorded(s.ended(s.data.signedInAs(minted)), minted)
	s.importedUsersAgain(minted)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindLeftAlone: 1}, s.backfilled(true))

	s.carriesOn(s.guestNamed(minted), opened)
}

// The app that counts is the one the customer acts in, where the guest's token is refreshed: a
// conversation kept in the shared app is recorded once the customer acts in its own.
func (s *SessionOwnershipSuite) TestABackfillGoesByTheAppAMovedGuestsCustomerActsIn() {
	id := streamAppID()
	s.useApp(s.numberedApp(id))
	minted := s.mintedGuest()
	s.withNoKindRecorded(s.ended(s.data.signedInAs(minted)), minted)
	s.importedUsersAgain(minted)
	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindLeftAlone: 1}, s.backfilled(false))

	s.giveApp(s.customerID(), id, "key-"+s.customerID())

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 1}, s.backfilled(false))
}

// A moved minted guest that held a conversation by a token of the role is left alone in either
// kind of app.
func (s *SessionOwnershipSuite) TestAConversationAMovedMintedGuestHeldByTheRoleIsLeftAlone() {
	s.Run("shared", func() {
		minted := s.mintedGuest()
		s.withNoKindRecorded(s.ended(s.guestNamed(minted)), minted)
		s.importedUsersAgain(minted)
		s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindLeftAlone: 1}, s.backfilled(true))
	})
	s.Run("own", func() {
		s.inAnAppOfItsOwn()
		minted := s.mintedGuest()
		s.withNoKindRecorded(s.ended(s.guestNamed(minted)), minted)
		s.importedUsersAgain(minted)
		s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindLeftAlone: 1}, s.backfilled(true))
	})
}

// A guest minted here stays so once it was moved away and back.
func (s *SessionOwnershipSuite) TestAConversationOfAMintedGuestMovedAwayAndBackIsRecorded() {
	minted := s.mintedGuest()
	opened := s.withNoKindRecorded(s.ended(s.data.signedInAs(minted)), minted)
	status, body := s.serverClient.call(http.MethodPost, "/v1/data/import?full=true", s.exportedUsers(minted))
	s.Require().Equal(http.StatusOK, status, "%s", body)
	held, err := s.store.Users.GetGuest(s.T().Context(), s.customerID(), minted)
	s.Require().NoError(err)
	s.Require().True(held.Minted && held.MintedElsewhere)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 1}, s.backfilled(true))

	s.carriesOn(s.data.signedInAs(minted), opened)
}

func (s *SessionOwnershipSuite) TestAnAppsOwnGuestCarriesOnItsBackfilledConversationInItsOwnApp() {
	s.inAnAppOfItsOwn()
	seen := s.guestSeenInAToken()
	opened := s.withNoKindRecorded(s.ended(s.guestNamed(seen)), seen)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 1}, s.backfilled(true))

	s.carriesOn(s.refreshedByItsBackend(seen), opened)
}

// unknownDeployment is the suite's apps as a command that does not know which app is the
// deployment's.
type unknownDeployment struct{ conversation.Chats }

func (unknownDeployment) DeploymentApp() int64 { return 0 }

// The deployment's app is its own customer's, so a moved minted guest is refreshed there to a token
// of no role. A command that does not know which app that is leaves their conversations as they
// are, held by either token, and one that does records the one held by a token of no role.
func (s *SessionOwnershipSuite) TestABackfillInTheDeploymentAppOfItsOwnCustomer() {
	app, err := s.store.Apps.Get(s.T().Context(), strconv.Itoa(suiteStreamApp))
	if err != nil {
		s.useApp(s.numberedApp(suiteStreamApp))
	} else {
		s.useApp(s.data.keyed(store.Organization{ID: app.OrganizationID}, app))
	}
	minted, byRole := s.mintedGuest(), s.mintedGuest()
	opened := s.withNoKindRecorded(s.ended(s.data.signedInAs(minted)), minted)
	held := s.withNoKindRecorded(s.ended(s.guestNamed(byRole)), byRole)
	s.importedUsersAgain(minted, byRole)
	command := conversation.NewForChats(unknownDeployment{conversation.StreamApps(s.stream)})
	defer command.Close()
	command.SetPins(s.store.Sessions.ConversationPin)

	_, err = command.BackfillOwnerKinds(s.T().Context(), s.store, s.run(true))
	s.Require().NoError(err)
	s.False(s.kindRecorded(opened))
	refused := conversation.NewForChats(placedAs{conversation.StreamApps(s.stream), streamapp.ErrNoIdentity})
	defer refused.Close()
	refused.SetPins(s.store.Sessions.ConversationPin)
	_, err = refused.BackfillOwnerKinds(s.T().Context(), s.store, s.run(true))
	s.Require().NoError(err)
	s.False(s.kindRecorded(opened))
	s.False(s.kindRecorded(held))

	s.backfilled(true)
	s.True(s.kindRecorded(opened))
	s.False(s.kindRecorded(held))
	s.refusesToCarryOn(s.guestNamed(minted), opened)
	s.carriesOn(s.refreshedByItsBackend(minted), opened)
	s.carriesOn(s.refreshedByItsBackend(byRole), held)
}

// placedAs is the suite's apps, answering err for where a customer acts.
type placedAs struct {
	conversation.Chats
	err error
}

func (p placedAs) Placed(context.Context, string) (int64, error) { return 0, p.err }

// Where a moved minted guest's customer acts is asked only when it decides something. A customer
// refused an app leaves the conversation as it is, and one that could not be asked about fails.
func (s *SessionOwnershipSuite) TestABackfillOfACustomerWhoseAppIsRefusedOrUnknown() {
	failing := errors.New("the app could not be read")
	const alone, failed, stamped = conversation.OwnerKindLeftAlone, conversation.OwnerKindFailed, conversation.OwnerKindStamped
	for _, row := range []struct {
		name   string
		guest  bool
		mode   auth.Mode
		err    error
		expect conversation.OwnerKindBackfill
	}{
		{"no app", false, auth.APIKey, streamapp.ErrNoIdentity, alone},
		{"disconnected", false, auth.APIKey, streamapp.ErrStreamAppDisconnected, alone},
		{"moved", false, auth.APIKey, streamapp.ErrStreamAppMoved, alone},
		{"read only", false, auth.APIKey, streamapp.ErrReadOnly, alone},
		{"not known yet", false, auth.APIKey, streamapp.ErrDeploymentAppUnknown, failed},
		{"could not be read", false, auth.APIKey, failing, failed},
		{"behind a proxy", false, auth.Proxy, failing, stamped},
		{"held by the role", true, auth.APIKey, failing, alone},
		{"held by the role behind a proxy", true, auth.Proxy, failing, alone},
	} {
		s.Run(row.name, func() {
			s.useApp(s.data.createApp())
			minted := s.mintedGuest()
			holder := s.data.signedInAs(minted)
			if row.guest {
				holder = s.guestNamed(minted)
			}
			s.withNoKindRecorded(s.ended(holder), minted)
			s.importedUsersAgain(minted)
			service := conversation.NewForChats(placedAs{conversation.StreamApps(s.stream), row.err})
			defer service.Close()
			service.SetPins(s.store.Sessions.ConversationPin)

			run := s.run(false)
			run.Mode = row.mode
			result, err := service.BackfillOwnerKinds(s.T().Context(), s.store, run)

			s.Equal(map[conversation.OwnerKindBackfill]int{row.expect: 1}, result.Counted)
			s.Equal(row.expect == failed, err != nil)
		})
	}
}

// A guest whose id went on as a signed-in user's account is no guest the role is minted for any
// more, so the conversation it held as a guest is left as it is.
func (s *SessionOwnershipSuite) TestAGuestsConversationIsLeftAloneOnceTheirIdIsAnAccount() {
	guest := s.data.createGuest()
	opened := s.withNoKindRecorded(s.ended(guest), guest.userID)
	s.data.signedInAs(guest.userID).querySessions(SessionQuery{})
	s.Require().Eventually(func() bool {
		_, err := s.store.Users.GetGuest(s.T().Context(), s.customerID(), guest.userID)
		return errors.Is(err, store.ErrNotAGuest)
	}, 5*time.Second, 20*time.Millisecond)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindLeftAlone: 1}, s.backfilled(true))

	s.carriesOn(s.data.signedInAs(guest.userID), opened)
}

// Behind a proxy a guest's refreshed token says no role, so a guest's conversation is left as it
// is there.
func (s *SessionOwnershipSuite) TestAGuestsConversationIsLeftAloneBehindAProxy() {
	guest := s.data.createGuest()
	opened := s.withNoKindRecorded(s.ended(guest), guest.userID)
	service, err := s.manager.Conversations()
	s.Require().NoError(err)

	run := s.run(true)
	run.Mode = auth.Proxy
	result, err := service.BackfillOwnerKinds(s.T().Context(), s.store, run)

	s.Require().NoError(err)
	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindLeftAlone: 1}, result.Counted)
	s.False(s.kindRecorded(opened))
}

// Sessions of two kinds of caller under the owner's id leave the channel as it is: nothing says
// which of them it was made for.
func (s *SessionOwnershipSuite) TestAConversationTwoKindsOfCallerHeldUnderItsOwnersIdIsLeftAlone() {
	alice := s.data.createUser()
	opened := s.withNoKindRecorded(s.ended(s.backendFor(alice.userID)), alice.userID)
	s.carriesOn(alice, opened)
	s.heldAs(opened, alice.userID, auth.KindAuthenticated)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindLeftAlone: 1}, s.backfilled(true))

	s.False(s.kindRecorded(opened))
}

// A row naming another customer's conversation, as one an import brought may, leaves its channel
// as it is: the channel says whose it is.
func (s *SessionOwnershipSuite) TestAnotherCustomersConversationARowNamesIsLeftAsItIs() {
	alice := s.data.createUser()
	theirs := s.withNoKindRecorded(s.ended(alice), alice.userID)
	s.useApp(s.data.createApp())
	s.Require().NoError(s.store.Sessions.Upsert(s.T().Context(), &store.AgentSession{
		ID: "session-" + s.utils.uuid(), CustomerID: s.customerID(), ConversationID: *theirs.ConversationId,
		UserID: alice.userID, CallerKind: string(auth.KindAuthenticated),
	}))

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindNoOwner: 1}, s.backfilled(true))

	s.False(s.kindRecorded(theirs))
}

// An anonymous caller's own conversation is left as it is: the name it went by is one nobody
// checked.
func (s *SessionOwnershipSuite) TestAnAnonymousCallersConversationIsLeftAlone() {
	someone := s.data.createAnonymous()
	opened := s.withNoKindRecorded(s.ended(someone), someone.userID)

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindLeftAlone: 1}, s.backfilled(true))

	s.False(s.kindRecorded(opened))
}

// walked is every channel BackfillOwnerKinds tells of for run, and how the walk went.
func (s *SessionOwnershipSuite) walked(ctx context.Context, run conversation.OwnerKindRun) ([]conversation.OwnerKindLine, conversation.OwnerKindResult, error) {
	service, err := s.manager.Conversations()
	s.Require().NoError(err)
	var lines []conversation.OwnerKindLine
	run.Line = func(line conversation.OwnerKindLine) { lines = append(lines, line) }
	result, err := service.BackfillOwnerKinds(ctx, s.store, run)
	return lines, result, err
}

// unrecorded is n conversations of signed-in users made before channels recorded their owner's
// kind, in the order a walk lists them.
func (s *SessionOwnershipSuite) unrecorded(n int) []Session {
	var made []Session
	for range n {
		alice := s.data.createUser()
		made = append(made, s.withNoKindRecorded(s.ended(alice), alice.userID))
	}
	slices.SortFunc(made, func(a, b Session) int { return strings.Compare(*a.ConversationId, *b.ConversationId) })
	return made
}

// lineOf is the line that tells of opened's channel in lines, in the form kind and action.
func (s *SessionOwnershipSuite) lineOf(lines []conversation.OwnerKindLine, opened Session) [3]string {
	for _, line := range lines {
		if line.ConversationID == *opened.ConversationId {
			return [3]string{line.CustomerID, string(line.Kind), line.Action()}
		}
	}
	s.Failf("no line", "for %s", *opened.ConversationId)
	return [3]string{}
}

// Each channel is listed with its customer, its kind and what was or would be done, dry run or not.
func (s *SessionOwnershipSuite) TestTheBackfillListsEachChannelWithWhatItDoesOrDid() {
	alice, anonymous, bob := s.data.createUser(), s.data.createAnonymous(), s.data.createUser()
	stamped := s.withNoKindRecorded(s.ended(alice), alice.userID)
	left := s.withNoKindRecorded(s.ended(anonymous), anonymous.userID)
	recorded := s.ended(bob)
	s.heldAs(recorded, bob.userID, auth.KindAuthenticated)

	for _, write := range []bool{false, true} {
		lines, _, err := s.walked(s.T().Context(), s.run(write))

		s.Require().NoError(err)
		s.Require().Len(lines, 3)
		s.True(slices.IsSortedFunc(lines, func(a, b conversation.OwnerKindLine) int {
			return strings.Compare(a.ConversationID, b.ConversationID)
		}), "in the order of the walk")
		action := map[bool]string{false: "would stamp", true: "stamped"}[write]
		s.Equal([3]string{s.customerID(), "authenticated", action}, s.lineOf(lines, stamped))
		s.Equal([3]string{s.customerID(), "", "skipped:left alone"}, s.lineOf(lines, left))
		s.Equal([3]string{s.customerID(), "", "skipped:already recorded"}, s.lineOf(lines, recorded))
		s.Equal(write, s.kindRecorded(stamped))
	}
}

// A channel whose owner's guest row, sessions or kinds cannot be read is a failure, and so is the
// run: it exits non-zero, with the other channels still walked.
func (s *SessionOwnershipSuite) TestAChannelWhoseOwnerCannotBeReadFailsTheRunAndIsListedAsFailed() {
	for name, failing := range map[string]string{
		"the guest row":  "(kind = 'guest')",
		"the guest held": "EXISTS",
		"the kinds":      `."caller_kind" FROM`,
	} {
		s.Run(name, func() {
			s.useApp(s.data.createApp())
			made := s.unrecorded(2)
			s.failQueries(failing)

			lines, result, err := s.walked(s.T().Context(), s.run(true))

			s.Error(err)
			s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindFailed: 2}, result.Counted)
			s.True(result.Complete)
			for _, opened := range made {
				line := s.lineOf(lines, opened)
				s.True(strings.HasPrefix(line[2], "failed:"), line[2])
				s.Empty(line[1])
				s.False(s.kindRecorded(opened))
			}
		})
	}
}

// A channel Stream cannot be asked about is a failure that is no rate limit: it is not waited out,
// and the walk goes on.
func (s *SessionOwnershipSuite) TestAChannelStreamFailsToReadIsListedAsFailedAndNotWaitedOut() {
	made := s.unrecorded(2)
	s.chat.Fail(http.MethodPost, "/chat/channels")
	s.T().Cleanup(func() { s.chat.Heal(http.MethodPost, "/chat/channels") })
	run := s.run(true)
	run.Sleep = func(context.Context, time.Duration) error {
		s.Fail("waited out a failure that is no rate limit")
		return nil
	}

	lines, result, err := s.walked(s.T().Context(), run)

	s.Error(err)
	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindFailed: 2}, result.Counted)
	s.True(result.Complete)
	s.True(strings.HasPrefix(s.lineOf(lines, made[0])[2], "failed:"))
}

// A walk that cannot read the conversations says so, and has done none.
func (s *SessionOwnershipSuite) TestABackfillThatCannotReadTheConversationsFailsAtOnce() {
	s.unrecorded(1)
	s.failQueries("DISTINCT")

	lines, result, err := s.walked(s.T().Context(), s.run(true))

	s.Error(err)
	s.Empty(lines)
	s.False(result.Complete)
}

// A channel that names no owner is skipped, whatever sessions are held under an empty id.
func (s *SessionOwnershipSuite) TestAChannelWithNoOwnerIsSkipped() {
	opened := s.unrecorded(1)[0]
	s.chat.ForgetCustom(*opened.ConversationId, "support_owner_id")

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindNoOwner: 1}, s.backfilled(true))

	s.False(s.kindRecorded(opened))
}

// Only an agent channel is its owner's by the kind recorded on it: a channel of another type that
// names an owner and the customer is left as it is.
func (s *SessionOwnershipSuite) TestAChannelOfAnotherTypeIsLeftAsItIs() {
	channel, owner := "messaging:"+s.utils.uuid(), s.utils.uuid()
	_, err := s.chat.Client.Chat().GetOrCreateChannel(s.T().Context(), "messaging", strings.TrimPrefix(channel, "messaging:"),
		&getstream.GetOrCreateChannelRequest{Data: &getstream.ChannelInput{CreatedByID: &owner, Custom: map[string]any{
			"support_owner_id": owner, conversation.CustomerField: s.customerID(),
		}}})
	s.Require().NoError(err)
	s.Require().NoError(s.store.Sessions.Upsert(s.T().Context(), &store.AgentSession{
		ID: "session-" + s.utils.uuid(), CustomerID: s.customerID(), ConversationID: channel,
		UserID: owner, CallerKind: string(auth.KindAuthenticated),
	}))

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindNoOwner: 1}, s.backfilled(true))

	data, _ := s.chat.Channel(channel)
	s.NotContains(data["custom"], "support_owner_kind")
}

// Conversations are read a page at a time, each once and in order, whichever page they are on.
func (s *SessionOwnershipSuite) TestTheBackfillWalksEveryPageOfConversationsOnce() {
	made := s.unrecorded(5)
	run := s.run(false)
	run.Page = 2

	lines, result, err := s.walked(s.T().Context(), run)

	s.Require().NoError(err)
	var walked []string
	for _, line := range lines {
		walked = append(walked, line.ConversationID)
	}
	s.Equal([]string{*made[0].ConversationId, *made[1].ConversationId, *made[2].ConversationId,
		*made[3].ConversationId, *made[4].ConversationId}, walked)
	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 5}, result.Counted)
	s.True(result.Complete)
	s.Equal(store.HeldConversation{CustomerID: s.customerID(), ConversationID: *made[4].ConversationId}, result.Done)
}

// A walk starts after the conversation it is given, whichever it was: the last one done carries
// on with the next.
func (s *SessionOwnershipSuite) TestTheBackfillStartsAfterTheConversationItIsGiven() {
	made := s.unrecorded(3)
	run := s.run(true)
	run.After = store.HeldConversation{CustomerID: s.customerID(), ConversationID: *made[0].ConversationId}

	lines, result, err := s.walked(s.T().Context(), run)

	s.Require().NoError(err)
	s.Equal([]string{*made[1].ConversationId, *made[2].ConversationId},
		[]string{lines[0].ConversationID, lines[1].ConversationID})
	s.Len(lines, 2)
	s.False(s.kindRecorded(made[0]))
	s.True(s.kindRecorded(made[1]) && s.kindRecorded(made[2]))
	s.Equal(*made[2].ConversationId, result.Done.ConversationID)

	run.After = result.Done
	lines, result, err = s.walked(s.T().Context(), run)
	s.Require().NoError(err)
	s.Empty(lines)
	s.Equal(run.After, result.Done)
}

// Stream's rate limit is waited out as long as it says, and the channel it refused is read again.
func (s *SessionOwnershipSuite) TestTheBackfillWaitsOutARateLimitOnReadingAChannel() {
	opened := s.unrecorded(1)[0]
	s.chat.RateLimit("/chat/channels")
	s.T().Cleanup(func() { s.chat.LiftRateLimit("/chat/channels") })
	var waited []time.Duration
	run := s.run(true)
	run.Sleep = func(_ context.Context, wait time.Duration) error {
		waited = append(waited, wait)
		s.chat.LiftRateLimit("/chat/channels")
		return nil
	}

	_, result, err := s.walked(s.T().Context(), run)

	s.Require().NoError(err)
	s.Equal([]time.Duration{time.Minute}, waited, "the Retry-After the limit answered with")
	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 1}, result.Counted)
	s.True(s.kindRecorded(opened))
}

// A limit that does not lift stops the walk at the channel it refused, which a rerun from the
// last one done starts with.
func (s *SessionOwnershipSuite) TestARateLimitThatDoesNotLiftStopsTheBackfillAndARerunResumes() {
	made := s.unrecorded(3)
	refused := strings.TrimPrefix(*made[1].ConversationId, "agent:")
	s.chat.RateLimit("/" + refused)
	s.T().Cleanup(func() { s.chat.LiftRateLimit("/" + refused) })
	var waited []time.Duration
	run := s.run(true)
	run.Sleep = func(_ context.Context, wait time.Duration) error {
		waited = append(waited, wait)
		return nil
	}

	lines, result, err := s.walked(s.T().Context(), run)

	s.Error(err)
	s.Len(waited, 5, "five waits, then it gives up")
	s.Len(lines, 2, "the walk stops at the channel the limit did not lift for")
	s.Equal("stamped", s.lineOf(lines, made[0])[2])
	s.Equal([2]string{"authenticated", "failed:"}, [2]string{s.lineOf(lines, made[1])[1], s.lineOf(lines, made[1])[2][:7]},
		"a write that failed names the kind it was to record")
	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 1, conversation.OwnerKindFailed: 1}, result.Counted)
	s.False(result.Complete)
	s.Equal(store.HeldConversation{CustomerID: s.customerID(), ConversationID: *made[0].ConversationId}, result.Done)

	s.chat.LiftRateLimit("/" + refused)
	run.After = result.Done
	lines, result, err = s.walked(s.T().Context(), run)

	s.Require().NoError(err)
	s.Len(lines, 2)
	s.Equal("stamped", s.lineOf(lines, made[1])[2])
	s.Equal("stamped", s.lineOf(lines, made[2])[2])
	s.True(result.Complete)
}

// A walk whose context ends while it waits stops there, and does not count the channel it was on.
func (s *SessionOwnershipSuite) TestTheBackfillStopsWhenItsContextEndsWhileItWaits() {
	opened := s.unrecorded(1)[0]
	s.chat.RateLimit("/chat/channels")
	s.T().Cleanup(func() { s.chat.LiftRateLimit("/chat/channels") })
	ctx, cancel := context.WithCancel(s.T().Context())
	run := s.run(true)
	run.Sleep = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}

	lines, result, err := s.walked(ctx, run)

	s.ErrorIs(err, context.Canceled)
	s.Empty(lines)
	s.Empty(result.Counted)
	s.False(result.Complete)
	s.False(s.kindRecorded(opened))
}

// A conversation whose session was deleted is not walked: its channel outlives the session, and
// no session is left to say whose it was.
func (s *SessionOwnershipSuite) TestAConversationWhoseSessionWasDeletedIsLeftAsItIs() {
	alice := s.data.createUser()
	opened := s.withNoKindRecorded(s.ended(alice), alice.userID)
	alice.deleteSession(opened.Id)

	s.Empty(s.backfilled(true))

	s.False(s.kindRecorded(opened))
}

// A claimed guest's sessions are the account's now, under its own id, while the channel still
// names the guest: no session of the owner's id is left, and the channel is left as it is.
func (s *SessionOwnershipSuite) TestAClaimedGuestsConversationIsLeftAlone() {
	guest := s.data.createGuest()
	opened := s.withNoKindRecorded(s.ended(guest), guest.userID)
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/guests/claim",
		ClaimGuestRequest{GuestId: guest.userID, UserId: s.utils.uuid()}, nil))

	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindLeftAlone: 1}, s.backfilled(true))

	s.False(s.kindRecorded(opened))
}

// Without being told to write, the backfill only says what it would do; told to, it records the
// kind once, and a second run finds it recorded. A channel that recorded its owner's kind when it
// was made keeps it.
func (s *SessionOwnershipSuite) TestTheBackfillWritesOnlyWhenToldAndOnce() {
	alice := s.data.createUser()
	opened := s.withNoKindRecorded(s.ended(alice), alice.userID)
	guest := s.data.createGuest()
	guests := s.ended(guest)
	s.heldAs(guests, guest.userID, auth.KindGuest)

	dry := s.backfilled(false)
	s.False(s.kindRecorded(opened))
	written := s.backfilled(true)
	again := s.backfilled(true)

	recorded := map[conversation.OwnerKindBackfill]int{conversation.OwnerKindStamped: 1, conversation.OwnerKindRecorded: 1}
	s.Equal(recorded, dry)
	s.Equal(recorded, written)
	s.Equal(map[conversation.OwnerKindBackfill]int{conversation.OwnerKindRecorded: 2}, again)
	s.refusesToCarryOn(s.data.signedInAs(guest.userID), guests)
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

// assertSharedAgentKeepsGuestsApart checks guest reaches its own session and none of what
// another guest's session of the same agent id holds: not the session, its socket, a turn in it
// or its messages, read by the guest or by the backend acting for the guest.
func (s *SessionOwnershipSuite) assertSharedAgentKeepsGuestsApart(guest *testClient, own, another Session) {
	s.Require().Equal(own.AgentId, another.AgentId, "both sessions are written as one agent")
	s.Require().NotEqual(value(own.ConversationId), value(another.ConversationId))
	s.assertReaches(guest, own.Id)
	s.assertDoesNotReach(guest, another.Id)
	_, status := guest.watch("/v1/agents/sessions/" + another.Id + "/events")
	s.Equal(http.StatusNotFound, status, "its socket")
	s.Equal(http.StatusNotFound, guest.do(http.MethodPost, "/v1/agents/sessions/"+another.Id+"/responses",
		CreateResponseRequest{Text: "What did they ask?", RequestId: pointerTo(s.utils.uuid())}, nil), "a turn in it")
	s.Equal(http.StatusForbidden, guest.do(http.MethodGet, messagesOf(another), nil, nil), "its messages")
	status, body := s.backendFor(guest.userID).call(http.MethodGet, messagesOf(another), nil)
	s.Equal(http.StatusBadRequest, status, "its messages, read by the backend for the guest: %s", body)
	s.Contains(string(body), "another user")
	s.Equal(http.StatusOK, s.backendFor(guest.userID).do(http.MethodGet, messagesOf(own), nil, nil), "its own messages")
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

// withNoKindRecorded is opened as a channel made before channels recorded their owner's kind
// keeps it, once the store holds its session of owner's.
func (s *SessionOwnershipSuite) withNoKindRecorded(opened Session, owner string) Session {
	s.Require().Eventually(func() bool {
		kinds, err := s.store.Sessions.CallerKinds(s.T().Context(), s.customerID(), *opened.ConversationId, owner)
		return err == nil && len(kinds) > 0
	}, 5*time.Second, 20*time.Millisecond)
	s.chat.ForgetCustom(*opened.ConversationId, "support_owner_kind")
	return opened
}

// heldAs waits until the store holds a session of kind of owner's on opened's conversation.
func (s *SessionOwnershipSuite) heldAs(opened Session, owner string, kind auth.Kind) {
	s.Require().Eventually(func() bool {
		kinds, err := s.store.Sessions.CallerKinds(s.T().Context(), s.customerID(), *opened.ConversationId, owner)
		return err == nil && slices.Contains(kinds, string(kind))
	}, 5*time.Second, 20*time.Millisecond)
}

// backfilled records the owner's kind on the app's conversations made before channels recorded
// it, as router conversations backfill-owner-kinds does, and says what came of each.
func (s *SessionOwnershipSuite) backfilled(write bool) map[conversation.OwnerKindBackfill]int {
	service, err := s.manager.Conversations()
	s.Require().NoError(err)
	result, err := service.BackfillOwnerKinds(s.T().Context(), s.store, s.run(write))
	s.Require().NoError(err)
	return result.Counted
}

// run is a walk of the suite's customer's conversations, in the suite's authentication mode.
func (s *SessionOwnershipSuite) run(write bool) conversation.OwnerKindRun {
	return conversation.OwnerKindRun{Mode: s.authMode(), Customer: s.customerID(), Write: write}
}

// inAnAppOfItsOwn makes the suite's app a customer that acts in a Stream app of its own.
func (s *SessionOwnershipSuite) inAnAppOfItsOwn() {
	id := streamAppID()
	s.useApp(s.numberedApp(id))
	shared := s.chat
	s.chat = s.giveApp(s.customerID(), id, "key-"+s.customerID())
	s.T().Cleanup(func() { s.chat = shared })
}

// refreshedByItsBackend is the guest holding what the token its backend refreshes it to says.
func (s *SessionOwnershipSuite) refreshedByItsBackend(id string) *testClient {
	var refreshed GuestUser
	s.Require().Equal(http.StatusCreated,
		s.serverClient.do(http.MethodPost, "/v1/agents/guests", GuestUserRequest{Id: &id}, &refreshed))
	claims := jwt.MapClaims{}
	_, _, err := jwt.NewParser().ParseUnverified(refreshed.Token, claims)
	s.Require().NoError(err)
	return s.signedIn(claims, auth.AuthTypeJWT, guest, id)
}

// mintedGuest is the id of a guest the router minted.
func (s *SessionOwnershipSuite) mintedGuest() string {
	var minted GuestUser
	s.Require().Equal(http.StatusCreated,
		s.data.createAnonymous().do(http.MethodPost, "/v1/agents/guests", GuestUserRequest{}, &minted))
	return minted.Id
}

// kindRecorded reports whether opened's channel records its owner's kind.
func (s *SessionOwnershipSuite) kindRecorded(opened Session) bool {
	data, found := s.chat.Channel(*opened.ConversationId)
	s.Require().True(found)
	custom, _ := data["custom"].(map[string]any)
	_, recorded := custom["support_owner_kind"]
	return recorded
}

// messagesOf is where opened's conversation is read back.
func messagesOf(opened Session) string {
	return "/v1/agents/conversations/" + *opened.ConversationId + "/messages?agent_id=" + opened.AgentId
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
