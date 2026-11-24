//go:build integration

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"
	"github.com/golang-jwt/jwt/v5"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/chatlog"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// GuestsSuite covers minting a guest so somebody can talk to an agent before they sign up,
// and moving their conversations onto the account they turn out to be.
//
// Minting the token itself reaches Stream, which this deployment has no real keys for, so
// what is asserted about minting is everything the router decides before it calls out.
type GuestsSuite struct {
	RouterSuite
}

func TestGuestsSuite(t *testing.T) {
	runSuite(t, new(GuestsSuite))
}

func (s *GuestsSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *GuestsSuite) TestAnAppThatDoesNotAdmitGuestsRefusesToMintOne() {
	no := false
	s.useApp(s.data.createAppAdmitting(store.AppSettings{AllowGuest: &no}))

	status, _ := s.data.createAnonymous().call(http.MethodPost, "/v1/agents/guests", nil)

	s.Equal(http.StatusForbidden, status)
}

func (s *GuestsSuite) TestAnIdThatIsNotAGuestOfThisAppIsRefusedToTheBackend() {
	// The backend naming an id could otherwise be handed a token for a real user, and
	// whether a name belongs to one is not something this can tell apart.
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/guests",
		GuestUserRequest{Id: pointerTo(s.utils.uuid())})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "not a guest of this app")
}

func (s *GuestsSuite) TestAnAnonymousCallerNamingAnIdThatIsNotAGuestIsMintedANewGuest() {
	// Refusing would tell whoever is guessing which ids are guests and which are accounts.
	named := s.utils.uuid()

	minted := s.mint(s.data.createAnonymous(), GuestUserRequest{Id: pointerTo(named)})

	s.NotEqual(named, minted.Id)
	s.Contains(minted.Id, guestPrefix)
}

func (s *GuestsSuite) TestAGuestOnlySeenInTheAppsOwnTokenIsNotMintedAgainForTheBackend() {
	// The app signs its guest tokens itself, for any id it likes, so a guest seen in one is
	// no sign the id is nobody else's.
	seen := s.guestSeenInAToken()

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/guests",
		GuestUserRequest{Id: pointerTo(seen)})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "not a guest of this app")
}

func (s *GuestsSuite) TestAnAnonymousCallerNamingAGuestOnlySeenInTheAppsOwnTokenIsMintedANewGuest() {
	seen := s.guestSeenInAToken()

	minted := s.mint(s.data.createAnonymous(), GuestUserRequest{Id: pointerTo(seen)})

	s.NotEqual(seen, minted.Id)
}

func (s *GuestsSuite) TestAGuestWhoHasSignedUpIsNotMintedAgainForTheBackend() {
	guest := s.recordGuest()
	s.claim(guest, s.utils.uuid())

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/guests",
		GuestUserRequest{Id: pointerTo(guest)})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "has been claimed")
}

func (s *GuestsSuite) TestAGuestWhoHasSignedUpIsNotMintedAgainForTheirOwnToken() {
	guest := s.recordGuest()
	s.claim(guest, s.utils.uuid())

	status, failure := s.data.signedInAs(guest).failure(http.MethodPost, "/v1/agents/guests",
		GuestUserRequest{Id: pointerTo(guest)})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "has been claimed")
}

func (s *GuestsSuite) TestAnAnonymousCallerNamingAGuestWhoHasSignedUpIsMintedANewGuest() {
	guest := s.recordGuest()
	s.claim(guest, s.utils.uuid())

	minted := s.mint(s.data.createAnonymous(), GuestUserRequest{Id: pointerTo(guest)})

	s.NotEqual(guest, minted.Id)
}

func (s *GuestsSuite) TestAFirstGuestIsHandedARefreshSecret() {
	minted := s.mint(s.data.createAnonymous(), GuestUserRequest{})

	s.Require().NotNil(minted.RefreshSecret)
	s.NotEmpty(*minted.RefreshSecret)
	s.Equal(store.GuestSecretHash(*minted.RefreshSecret), s.storedGuest(minted.Id).RefreshSecretHash,
		"only the hash is kept, and it is the hash of what was handed out")
}

func (s *GuestsSuite) TestAnAnonymousCallerNamingAnExistingGuestCannotTakeThemOver() {
	// The takeover: a guest id is the author of their messages, so anybody who shared a
	// channel with them knows it. Naming it is no proof of being them.
	victim := s.mint(s.data.createAnonymous(), GuestUserRequest{
		Name: pointerTo("Alice"), Custom: &map[string]any{"plan": "pro"},
	})
	before := s.storedGuest(victim.Id)

	minted := s.mint(s.data.createAnonymous(), GuestUserRequest{
		Id: pointerTo(victim.Id), Name: pointerTo("Mallory"), Custom: &map[string]any{"plan": "free"},
	})

	s.NotEqual(victim.Id, minted.Id, "the caller is somebody new")
	s.Equal("Mallory", value(minted.Name), "and is called what it asked to be")
	after := s.storedGuest(victim.Id)
	s.Equal("Alice", after.Name)
	s.Equal(map[string]any{"plan": "pro"}, after.Custom)
	s.Equal(before.RefreshSecretHash, after.RefreshSecretHash, "the guest's secret still works")
	s.assertStreamProfile(victim.Id, "Alice", map[string]any{"plan": "pro"})
}

func (s *GuestsSuite) TestAnAnonymousCallerNamingTheGuestInTheUserHeaderIsStillMintedANewGuest() {
	// A header is something a caller writes about itself, so going by the guest's id there
	// makes it anonymous all the same.
	victim := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Alice")})

	minted := s.mint(s.data.claiming(victim.Id), GuestUserRequest{Id: pointerTo(victim.Id)})

	s.NotEqual(victim.Id, minted.Id)
	s.Equal("Alice", s.storedGuest(victim.Id).Name)
	s.assertStreamProfile(victim.Id, "Alice", nil)
}

func (s *GuestsSuite) TestAnotherSignedInUserNamingAGuestIsMintedANewGuest() {
	victim := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Alice")})
	before := s.storedGuest(victim.Id)

	minted := s.mint(s.data.createUser(), GuestUserRequest{Id: pointerTo(victim.Id), Name: pointerTo("Mallory")})

	s.NotEqual(victim.Id, minted.Id)
	after := s.storedGuest(victim.Id)
	s.Equal("Alice", after.Name)
	s.Equal(before.RefreshSecretHash, after.RefreshSecretHash)
	s.assertStreamProfile(victim.Id, "Alice", nil)
}

func (s *GuestsSuite) TestAGuestsOwnTokenRefreshesThemWithoutASecretAndWithoutTouchingTheirProfile() {
	// The token the router mints carries no role, so it arrives as a signed-in user's.
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{
		Name: pointerTo("Alice"), Custom: &map[string]any{"plan": "pro"},
	})
	writes := s.profileWrites()

	again := s.mint(s.data.signedInAs(first.Id), GuestUserRequest{
		Id: pointerTo(first.Id), Name: pointerTo("Mallory"), Custom: &map[string]any{"plan": "free"},
	})

	s.Equal(first.Id, again.Id)
	s.NotEmpty(again.Token)
	s.Equal("Alice", value(again.Name), "a guest's own token is no profile editor")
	s.Require().NotNil(again.Custom)
	s.Equal(map[string]any{"plan": "pro"}, *again.Custom)
	s.Nil(again.RefreshSecret, "the guest already holds a secret, which keeps working")
	s.Equal(store.GuestSecretHash(*first.RefreshSecret), s.storedGuest(first.Id).RefreshSecretHash)
	s.Equal(writes, s.profileWrites(), "Stream is not asked to change who they are")
	s.assertStreamProfile(first.Id, "Alice", map[string]any{"plan": "pro"})
}

func (s *GuestsSuite) TestAGuestWithNoSecretIsGivenOneByTheirOwnToken() {
	// A guest minted before secrets existed holds none, and gets one the next time they refresh.
	guest := s.recordGuest()

	again := s.mint(s.data.signedInAs(guest), GuestUserRequest{Id: pointerTo(guest)})

	s.Equal(guest, again.Id)
	s.Require().NotNil(again.RefreshSecret)
	s.Equal(store.GuestSecretHash(*again.RefreshSecret), s.storedGuest(guest).RefreshSecretHash)
}

func (s *GuestsSuite) TestAnAccountNamingItsOwnIdIsNotMadeAGuest() {
	account := s.data.createUser()
	writes := s.profileWrites()

	status, failure := account.failure(http.MethodPost, "/v1/agents/guests",
		GuestUserRequest{Id: pointerTo(account.userID)})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "not a guest of this app")
	s.Equal(writes, s.profileWrites(), "Stream is not told the account is a guest")
}

func (s *GuestsSuite) TestOneGuestsSecretDoesNotBringBackAnother() {
	alice := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Alice")})
	bob := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Bob")})

	answered := s.mint(s.data.createAnonymous(), GuestUserRequest{Id: pointerTo(bob.Id), RefreshSecret: alice.RefreshSecret})

	s.NotEqual(bob.Id, answered.Id)
	s.NotEqual(alice.Id, answered.Id)
	s.Equal(store.GuestSecretHash(*bob.RefreshSecret), s.storedGuest(bob.Id).RefreshSecretHash)
	s.Equal(store.GuestSecretHash(*alice.RefreshSecret), s.storedGuest(alice.Id).RefreshSecretHash)
}

func (s *GuestsSuite) TestTwoRequestsRedeemingOneSecretBringTheGuestBackOnce() {
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{})
	request := GuestUserRequest{Id: pointerTo(first.Id), RefreshSecret: first.RefreshSecret}

	answers := make(chan GuestUser, 2)
	for range 2 {
		go func() {
			var minted GuestUser
			s.data.createAnonymous().do(http.MethodPost, "/v1/agents/guests", request, &minted)
			answers <- minted
		}()
	}
	same := 0
	for range 2 {
		if (<-answers).Id == first.Id {
			same++
		}
	}

	s.Equal(1, same, "a secret is spent once")
}

func (s *GuestsSuite) TestTheBackendMintingAGuestAgainReplacesTheirSecret() {
	// The app hands the new one to the device it decided is this guest.
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{})

	again := s.mint(s.serverClient, GuestUserRequest{Id: pointerTo(first.Id)})
	stale := s.mint(s.data.createAnonymous(), GuestUserRequest{Id: pointerTo(first.Id), RefreshSecret: first.RefreshSecret})

	s.Require().NotNil(again.RefreshSecret)
	s.Equal(store.GuestSecretHash(*again.RefreshSecret), s.storedGuest(first.Id).RefreshSecretHash)
	s.NotEqual(first.Id, stale.Id, "the secret the backend replaced no longer works")
}

func (s *GuestsSuite) TestAGuestTokenWithTheGuestRoleRefreshesTheSameGuest() {
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Alice")})

	again := s.mint(s.asGuest(first.Id), GuestUserRequest{Id: pointerTo(first.Id)})

	s.Equal(first.Id, again.Id)
	s.Nil(again.RefreshSecret, "the guest keeps the secret they hold")
	s.Equal(store.GuestSecretHash(*first.RefreshSecret), s.storedGuest(first.Id).RefreshSecretHash)
}

func (s *GuestsSuite) TestARefreshSecretBringsTheSameGuestBackWithANewSecret() {
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{
		Name: pointerTo("Alice"), Custom: &map[string]any{"plan": "pro"},
	})

	again := s.mint(s.data.createAnonymous(), GuestUserRequest{
		Id: pointerTo(first.Id), RefreshSecret: first.RefreshSecret, Name: pointerTo("Mallory"),
	})

	s.Equal(first.Id, again.Id)
	s.NotEmpty(again.Token)
	s.Equal("Alice", value(again.Name), "a secret proves who they are, not what to call them")
	s.Require().NotNil(again.Custom)
	s.Equal(map[string]any{"plan": "pro"}, *again.Custom)
	s.Require().NotNil(again.RefreshSecret)
	s.NotEqual(*first.RefreshSecret, *again.RefreshSecret)
	s.Equal(store.GuestSecretHash(*again.RefreshSecret), s.storedGuest(first.Id).RefreshSecretHash)
}

func (s *GuestsSuite) TestARefreshSecretWorksOnce() {
	// Whoever copied a secret off a device must not be able to come back with it after the
	// guest has.
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Alice")})
	again := s.mint(s.data.createAnonymous(), GuestUserRequest{Id: pointerTo(first.Id), RefreshSecret: first.RefreshSecret})
	s.Require().Equal(first.Id, again.Id)

	replayed := s.mint(s.data.createAnonymous(), GuestUserRequest{Id: pointerTo(first.Id), RefreshSecret: first.RefreshSecret})

	s.NotEqual(first.Id, replayed.Id)
	s.Equal(store.GuestSecretHash(*again.RefreshSecret), s.storedGuest(first.Id).RefreshSecretHash,
		"the guest's current secret still works")
}

func (s *GuestsSuite) TestAWrongRefreshSecretMintsANewGuest() {
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Alice")})
	before := s.storedGuest(first.Id)

	minted := s.mint(s.data.createAnonymous(), GuestUserRequest{
		Id: pointerTo(first.Id), RefreshSecret: pointerTo("not-" + *first.RefreshSecret),
	})

	s.NotEqual(first.Id, minted.Id)
	s.Equal(before.RefreshSecretHash, s.storedGuest(first.Id).RefreshSecretHash)
}

func (s *GuestsSuite) TestAClaimedGuestsRefreshSecretMintsANewGuest() {
	// A guest who signed up has an account to sign in with; the secret they held is no way
	// back into the guest.
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Alice")})
	s.claim(first.Id, s.utils.uuid())

	minted := s.mint(s.data.createAnonymous(), GuestUserRequest{Id: pointerTo(first.Id), RefreshSecret: first.RefreshSecret})

	s.NotEqual(first.Id, minted.Id)
}

func (s *GuestsSuite) TestTheBackendMintingAGuestAgainWithoutANameKeepsTheirName() {
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Alice")})

	again := s.mint(s.serverClient, GuestUserRequest{Id: pointerTo(first.Id)})

	s.Equal(first.Id, again.Id)
	s.Equal("Alice", value(again.Name), "not Guest")
	s.Require().NotNil(again.RefreshSecret)
	s.Equal(store.GuestSecretHash(*again.RefreshSecret), s.storedGuest(first.Id).RefreshSecretHash)
	s.assertStreamProfile(first.Id, "Alice", nil)
}

func (s *GuestsSuite) TestABackendRenameIsKeptForTheGuestsNextToken() {
	// Stream and the stored row must agree, or the next re-mint sends the old name back.
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Alice")})
	s.mint(s.serverClient, GuestUserRequest{Id: pointerTo(first.Id), Name: pointerTo("Bob")})

	again := s.mint(s.serverClient, GuestUserRequest{Id: pointerTo(first.Id)})
	self := s.mint(s.data.signedInAs(first.Id), GuestUserRequest{Id: pointerTo(first.Id)})

	s.Equal("Bob", value(again.Name), "the backend minting them again without a name")
	s.Equal("Bob", value(self.Name), "the guest refreshing with their own token")
	s.Equal("Bob", s.storedGuest(first.Id).Name)
	s.assertStreamProfile(first.Id, "Bob", nil)
}

func (s *GuestsSuite) TestTheBackendMayRenameAGuestItMintsAgain() {
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{Name: pointerTo("Alice")})

	again := s.mint(s.serverClient, GuestUserRequest{
		Id: pointerTo(first.Id), Name: pointerTo("Alicia"), Custom: &map[string]any{"plan": "pro"},
	})

	s.Equal(first.Id, again.Id)
	s.Equal("Alicia", value(again.Name))
	s.assertStreamProfile(first.Id, "Alicia", map[string]any{"plan": "pro"})
}

func (s *GuestsSuite) TestClaimingAGuestMovesTheirConversationsOntoTheAccount() {
	guest, account := s.recordGuest(), s.data.createUser()
	talked := s.talked(guest)

	claimed := s.claim(guest, account.userID)

	s.Equal(1, claimed.SessionsMoved)
	s.Contains(ids(account.querySessions(SessionQuery{}).Items), talked,
		"the conversation is the account's now")
}

func (s *GuestsSuite) TestTheGuestNoLongerOwnsWhatTheyTalkedAbout() {
	guest := s.recordGuest()
	talked := s.talked(guest)

	s.claim(guest, s.data.createUser().userID)

	s.NotContains(ids(s.data.signedInAs(guest).querySessions(SessionQuery{}).Items), talked)
}

func (s *GuestsSuite) TestTheSameClaimArrivingTwiceIsNotAConflict() {
	// A retried request must not read as somebody else claiming the same guest.
	guest := s.recordGuest()
	account := s.utils.uuid()
	s.claim(guest, account)

	again := s.claim(guest, account)

	s.Equal(0, again.SessionsMoved, "the conversations moved the first time")
}

// A claim that stopped once the guest was marked, before the account joined the channels of
// the conversations it moved, was answered as done when sent again, and the account never
// could read them. The store's half is all the first claim got through.
func (s *GuestsSuite) TestAClaimThatStoppedAfterMovingTheRowsJoinsTheChannelsWhenSentAgain() {
	guest, account := s.recordGuest(), s.data.createUser()
	talked := s.talked(guest)
	_, err := s.store.ClaimGuest(context.Background(), s.customerID(), guest, account.userID)
	s.Require().NoError(err)
	s.Require().NotContains(s.chat.Members(s.transcriptOf(talked)), account.userID)

	s.claim(guest, account.userID)

	s.Contains(s.chat.Members(s.transcriptOf(talked)), account.userID, "the account reads what the guest said")
}

// A claim sent again once every step of it ran did them all again: a backend that claims at
// every sign-in put the account back into the channel of a conversation it had left, with two
// Stream calls for each of the account's conversations every time.
func (s *GuestsSuite) TestAFinishedClaimSentAgainLeavesTheChannelsAsTheyAre() {
	guest, account := s.recordGuest(), s.data.createUser()
	talked := s.talked(guest)
	s.claim(guest, account.userID)
	channel := s.transcriptOf(talked)
	s.Require().Contains(s.chat.Members(channel), account.userID)
	_, err := s.chat.Client.Chat().UpdateChannel(context.Background(), chatlog.ChannelType, channel,
		&getstream.UpdateChannelRequest{RemoveMembers: []string{account.userID}})
	s.Require().NoError(err)

	s.claim(guest, account.userID)

	s.NotContains(s.chat.Members(channel), account.userID, "the claim had nothing left to do")
}

func (s *GuestsSuite) TestAGuestSomebodyElseClaimedIsAConflict() {
	guest := s.recordGuest()
	s.claim(guest, s.utils.uuid())

	status, _ := s.serverClient.call(http.MethodPost, "/v1/agents/guests/claim",
		ClaimGuestRequest{GuestId: guest, UserId: s.utils.uuid()})

	s.Equal(http.StatusConflict, status)
}

func (s *GuestsSuite) TestAGuestNobodyMintedCannotBeClaimed() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/guests/claim",
		ClaimGuestRequest{GuestId: s.utils.uuid(), UserId: s.utils.uuid()})

	s.Equal(http.StatusNotFound, status)
	s.Contains(failure, "no such guest")
}

// TestClaimsRacingForOneGuestLoseWithAConflict: claims sent at once all read the guest
// unclaimed, and those the store then refuses lost to the one that won, which is a 409 as a
// claim arriving after it is, not a 500.
func (s *GuestsSuite) TestClaimsRacingForOneGuestLoseWithAConflict() {
	guest := s.recordGuest()
	const claims = 20
	statuses := make(chan int, claims)
	var wg sync.WaitGroup
	for range claims {
		wg.Go(func() {
			status, _ := s.serverClient.call(http.MethodPost, "/v1/agents/guests/claim",
				ClaimGuestRequest{GuestId: guest, UserId: s.utils.uuid()})
			statuses <- status
		})
	}
	wg.Wait()
	close(statuses)

	won := 0
	for status := range statuses {
		if status == http.StatusOK {
			won++
			continue
		}
		s.Equal(http.StatusConflict, status)
	}
	s.Equal(1, won)
}

// TestAClaimTheDatabaseCannotMoveIsAnsweredAsInternal (AI-3575): a move the database fails is a
// 500 that quotes none of the query, not a 400 the caller could correct.
func (s *GuestsSuite) TestAClaimTheDatabaseCannotMoveIsAnsweredAsInternal() {
	guest := s.recordGuest()
	s.failQueries("caller_kind = CASE")

	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/guests/claim",
		ClaimGuestRequest{GuestId: guest, UserId: s.utils.uuid()})

	s.Equal(http.StatusInternalServerError, status)
	s.Contains(string(body), `"code":"internal_error"`)
	s.NotContains(string(body), "claim guest")
	s.NotContains(string(body), "deadline")
}

// TestAGuestCannotClaimItself is the control: the one refusal the move made is still a 400
// saying what is wrong.
func (s *GuestsSuite) TestAGuestCannotClaimItself() {
	guest := s.recordGuest()

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/guests/claim",
		ClaimGuestRequest{GuestId: guest, UserId: guest})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "claimed by itself")
}

func (s *GuestsSuite) TestAClaimHasToSayWhoIsClaimingWhom() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/guests/claim",
		ClaimGuestRequest{GuestId: s.recordGuest()})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "user id")
}

func (s *GuestsSuite) TestAnotherAppsGuestIsNotTheirsToClaim() {
	guest := s.recordGuest()

	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodPost, "/v1/agents/guests/claim",
			ClaimGuestRequest{GuestId: guest, UserId: s.utils.uuid()}, nil)
	})
}

func (s *GuestsSuite) TestOnlyTheAppsOwnBackendMayClaimAGuest() {
	// A page allowed to ask this could claim anybody's conversations by guessing an id.
	guest := s.recordGuest()

	s.assertPosture(serverOnly, func(as *testClient) int {
		status, _ := as.call(http.MethodPost, "/v1/agents/guests/claim",
			ClaimGuestRequest{GuestId: guest, UserId: s.utils.uuid()})
		return status
	})
}

func (s *GuestsSuite) TestAClaimMovesTheGuestsScheduledTasks() {
	guest := s.recordGuest()
	ctx := context.Background()
	task := store.ScheduledTask{
		CustomerID: s.customerID(), UserID: guest, CallerKind: string(auth.KindServer), APIKey: s.app.key,
		Title: "Stretch", Instructions: "Remind them to stretch.",
		StartLocal: time.Now().UTC().Add(48 * time.Hour).Truncate(time.Second), Timezone: "UTC",
	}
	s.Require().NoError(s.store.ScheduledTasks.Insert(ctx, &task, 0))
	s.T().Cleanup(func() { _ = s.store.ScheduledTasks.Delete(ctx, task.CustomerID, task.ID) })
	account := s.utils.uuid()

	s.claim(guest, account)

	moved, err := s.store.ScheduledTasks.Get(ctx, s.customerID(), task.ID)
	s.Require().NoError(err)
	s.Equal(account, moved.UserID, "the task follows up for the account the guest turned out to be")
}

func (s *GuestsSuite) TestAClaimedGuestsConversationIsTheAccountsByListAndById() {
	// A guest talks with a guest's token, so the row says a guest wrote it. Once claimed it
	// is the account's, reached the way the account reaches its own.
	guestID, account := s.recordGuest(), s.data.createUser()
	talked := s.talkedAs(s.asGuest(guestID))

	s.claim(guestID, account.userID)

	s.Contains(ids(account.querySessions(SessionQuery{}).Items), talked, "the account lists it")
	s.Equal(http.StatusOK, account.do(http.MethodGet, "/v1/agents/sessions/"+talked, nil, nil), "and opens it by id")
}

func (s *GuestsSuite) TestAClaimedGuestsScheduledTaskIsListedToTheAccount() {
	guestID, account := s.recordGuest(), s.data.createUser()
	task := s.scheduledFor(guestID, auth.KindGuest)

	s.claim(guestID, account.userID)

	var page ScheduledTaskPage
	s.Require().Equal(http.StatusOK, account.do(http.MethodGet, "/v1/agents/scheduled-tasks", nil, &page))
	s.Require().Len(page.Items, 1)
	s.Equal(task, page.Items[0].Id)
}

func (s *GuestsSuite) TestAClaimLeavesWhatAnAnonymousNamesakeSaidWhereItWas() {
	// A caller with no token picks its own name, so going by the guest's id or the account's
	// makes it neither: its conversations are not moved, and the account is not let into them.
	guestID, account := s.recordGuest(), s.data.createUser()
	genuine := s.talkedAs(s.asGuest(guestID))
	underTheGuestsName := s.talkedAs(s.data.claiming(guestID))
	underTheAccountsName := s.underTheName(account.userID)
	task := s.scheduledFor(guestID, auth.KindAnonymous)

	claimed := s.claim(guestID, account.userID)

	s.Equal(1, claimed.SessionsMoved, "only the guest's own conversation moved")
	s.Equal([]string{genuine}, ids(account.querySessions(SessionQuery{}).Items))
	s.Equal([]string{underTheGuestsName}, ids(s.data.claiming(guestID).querySessions(SessionQuery{}).Items),
		"the namesake keeps its conversation")
	s.Contains(s.chat.Members(s.transcriptOf(genuine)), account.userID, "the account reads what the guest said")
	s.NotContains(s.chat.Members(s.transcriptOf(underTheGuestsName)), account.userID)
	s.NotContains(s.chat.Members(s.transcriptOf(underTheAccountsName)), account.userID)
	left, err := s.store.ScheduledTasks.Get(context.Background(), s.customerID(), task)
	s.Require().NoError(err)
	s.Equal(guestID, left.UserID, "the namesake's task is not moved")
}

func (s *GuestsSuite) TestAClaimDoesNotLetTheAccountIntoAnotherAppsChannel() {
	// A session row comes in through the import as the file writes it, so it can name a
	// channel the router stamped for another app. The claim joins the account to the
	// channels of the sessions it moved, and that one is not the app's.
	otherUser := s.data.createUser()
	foreign := s.transcriptOf(s.talkedAs(otherUser))
	own := s.talkedAs(s.data.createUser())
	var raw string
	s.Require().NoError(s.store.GlobalDB().NewRaw("SELECT row_to_json(a)::text FROM agent_sessions AS a WHERE id = ?", own).
		Scan(context.Background(), &raw))
	row := map[string]any{}
	s.Require().NoError(json.Unmarshal([]byte(raw), &row))

	s.useApp(s.data.createApp())
	guest, account := s.recordGuest(), "acct-"+s.utils.uuid()
	row["id"], row["user_id"], row["conversation_id"], row["agent_id"] = store.NewID(), guest, "", foreign
	encoded, err := json.Marshal(row)
	s.Require().NoError(err)
	// full: the app already holds the guest.
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import?full=true",
		ndjson([]dataLine{{Table: "agent_sessions", Row: encoded}}), nil))
	honest := s.talked(guest)

	s.claim(guest, account)

	s.NotContains(s.chat.Members(foreign), account, "the other app's channel is not the account's to join")
	s.Contains(s.chat.Members(s.transcriptOf(honest)), account, "the app's own conversation still is")
}

func (s *GuestsSuite) TestAClaimStillJoinsAChannelWithNoCustomerStamp() {
	// A channel created before the router stamped channels names no customer. The transcript
	// reader takes it as the app's, so a claim must still add the account to it.
	channel := "agent-" + s.utils.uuid()
	creator := "vision-agent"
	_, err := s.chat.Client.Chat().GetOrCreateChannel(context.Background(), chatlog.ChannelType, channel,
		&getstream.GetOrCreateChannelRequest{Data: &getstream.ChannelInput{CreatedByID: &creator}})
	s.Require().NoError(err)
	_, exists := s.chat.Channel(channel)
	s.Require().True(exists, "the channel exists")

	own := s.talkedAs(s.data.createUser())
	var raw string
	s.Require().NoError(s.store.GlobalDB().NewRaw("SELECT row_to_json(a)::text FROM agent_sessions AS a WHERE id = ?", own).
		Scan(context.Background(), &raw))
	row := map[string]any{}
	s.Require().NoError(json.Unmarshal([]byte(raw), &row))

	s.useApp(s.data.createApp())
	guest, account := s.recordGuest(), "acct-"+s.utils.uuid()
	row["id"], row["user_id"], row["conversation_id"], row["agent_id"] = store.NewID(), guest, "", channel
	encoded, err := json.Marshal(row)
	s.Require().NoError(err)
	// full: the app already holds the guest.
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import?full=true",
		ndjson([]dataLine{{Table: "agent_sessions", Row: encoded}}), nil))

	s.claim(guest, account)

	s.Contains(s.chat.Members(channel), account, "a channel with no stamp is the app's to join")
}

func (s *GuestsSuite) TestAnAddressIsRefusedItsSixtyFirstNewGuestOfAnAppInAnHour() {
	s.mintSixtyGuests()
	written := s.profileWrites()

	status, payload := s.data.createAnonymous().call(http.MethodPost, "/v1/agents/guests", GuestUserRequest{})

	s.Require().Equal(http.StatusTooManyRequests, status, string(payload))
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answered))
	s.Equal(ErrorTypeRateLimited, answered.Error.Type)
	s.Contains(answered.Error.Message, "60 guests of this app this hour")
	s.Equal(written, s.profileWrites(), "a refused guest is never written to Stream")
}

func (s *GuestsSuite) TestTheBackendStillMintsGuestsOnceAnAddressHasMintedItsHour() {
	s.mintSixtyGuests()

	minted := s.mint(s.serverClient, GuestUserRequest{})

	s.Contains(minted.Id, guestPrefix)
}

func (s *GuestsSuite) TestAGuestComingBackWithTheirSecretIsNotCountedAsANewGuest() {
	first := s.mint(s.data.createAnonymous(), GuestUserRequest{})
	for range 59 {
		s.mint(s.data.createAnonymous(), GuestUserRequest{})
	}

	again := s.mint(s.data.createAnonymous(), GuestUserRequest{Id: pointerTo(first.Id), RefreshSecret: first.RefreshSecret})

	s.Equal(first.Id, again.Id)
}

func (s *GuestsSuite) TestAnAddressThatMintedItsHourInOneAppStillMintsGuestsOfAnother() {
	s.mintSixtyGuests()
	s.useApp(s.data.createApp())

	minted := s.mint(s.data.createAnonymous(), GuestUserRequest{})

	s.Contains(minted.Id, guestPrefix)
}

// mintSixtyGuests mints as many new guests of the suite's app as one address may in an hour,
// each by a caller of its own.
func (s *GuestsSuite) mintSixtyGuests() {
	awayFromTheHour()
	for range 60 {
		s.mint(s.data.createAnonymous(), GuestUserRequest{})
	}
}

// GuestsProxiedSuite mints guests behind a proxy with no trusted ranges, where every request
// arrives from the proxy's address.
type GuestsProxiedSuite struct {
	RouterSuite
}

func TestGuestsProxiedSuite(t *testing.T) {
	proxied := new(GuestsProxiedSuite)
	proxied.proxied = true
	runSuite(t, proxied)
}

func (s *GuestsProxiedSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *GuestsProxiedSuite) TestCallersTheProxyNamesAreNotCountedTogetherByTheProxysAddress() {
	// The address is the proxy's, so counting a caller it names by it would hold every
	// visitor of the app to one count.
	for range 61 {
		status, payload := s.viaProxy(s.utils.uuid()).call(http.MethodPost, "/v1/agents/guests", GuestUserRequest{})
		s.Require().Equal(http.StatusCreated, status, string(payload))
	}
}

func (s *GuestsProxiedSuite) TestCallersTheProxyNamesNobodyForShareTheProxysAddress() {
	awayFromTheHour()
	for range 60 {
		status, payload := s.viaProxy("").call(http.MethodPost, "/v1/agents/guests", GuestUserRequest{})
		s.Require().Equal(http.StatusCreated, status, string(payload))
	}

	status, _ := s.viaProxy("").call(http.MethodPost, "/v1/agents/guests", GuestUserRequest{})

	s.Equal(http.StatusTooManyRequests, status)
}

// viaProxy is an anonymous caller of the app as the proxy forwards it, going by user, or by
// nobody when user is empty.
func (s *GuestsProxiedSuite) viaProxy(user string) *testClient {
	header := http.Header{}
	header.Set(auth.AppHeader, s.customerID())
	header.Set(auth.OrganizationHeader, s.app.organization.ID)
	header.Set(auth.AuthTypeHeader, auth.AuthTypeAnonymous)
	if user != "" {
		header.Set(auth.UserHeader, user)
	}
	return &testClient{suite: &s.RouterSuite, header: header}
}

// awayFromTheHour waits out the last minute of a clock hour, so the guests a test mints next
// are counted in one hour's window: a count that started again at the hour would let the next
// guest through.
func awayFromTheHour() {
	if left := time.Until(time.Now().Truncate(time.Hour).Add(time.Hour)); left < time.Minute {
		time.Sleep(left)
	}
}

// mint asks for a guest as the caller and expects one.
func (s *GuestsSuite) mint(as *testClient, request GuestUserRequest) GuestUser {
	var minted GuestUser
	s.Require().Equal(http.StatusCreated, as.do(http.MethodPost, "/v1/agents/guests", request, &minted))
	return minted
}

// storedGuest is the guest's row as the store holds it.
func (s *GuestsSuite) storedGuest(id string) store.User {
	held, err := s.store.Guest(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	return held
}

// profileWrites is how many user upserts Stream has been sent for the suite's app.
func (s *GuestsSuite) profileWrites() int {
	written := 0
	for _, request := range s.chat.Requests(suiteStreamKey) {
		if request.Method == http.MethodPost && strings.HasSuffix(request.Path, "/users") {
			written++
		}
	}
	return written
}

// assertStreamProfile checks who Stream holds the user to be. A nil custom is not checked.
func (s *GuestsSuite) assertStreamProfile(id, name string, custom map[string]any) {
	user, ok := s.chat.User(id)
	s.Require().True(ok, "Stream holds the guest")
	s.Equal(name, user["name"])
	if custom != nil {
		s.Equal(custom, user["custom"])
	}
}

// recordGuest is a guest of the suite's app, written the way minting one writes it. Minting
// over HTTP would reach Stream for the token.
func (s *GuestsSuite) recordGuest() string {
	id := guestPrefix + s.utils.uuid()
	s.Require().NoError(s.store.RecordGuest(context.Background(), &store.User{
		ID: id, CustomerID: s.customerID(), Name: "Guest",
	}))
	return id
}

// guestSeenInAToken is an id the suite's app called as once, with a guest token it signed
// itself: recorded as its guest, though the router never minted it.
func (s *RouterSuite) guestSeenInAToken() string {
	id := "visitor-" + s.utils.uuid()
	s.signedIn(jwt.MapClaims{"user_id": id, "role": "guest"}, auth.AuthTypeJWT, guest, id).
		do(http.MethodGet, "/v1/agents/calls", nil, nil)
	seen, err := s.store.Guest(context.Background(), s.customerID(), id)
	s.Require().NoError(err, "the call is what records the guest")
	s.Require().False(seen.Minted)
	return id
}

// talked is a conversation the guest had and has finished, which is the history a claim
// moves. The row is written off the request path, so it is waited for: a claim that
// arrives before it moves nothing.
func (s *GuestsSuite) talked(guest string) string {
	return s.talkedAs(s.data.signedInAs(guest))
}

// talkedAs is a finished conversation the caller had, waited for the same way as talked.
func (s *GuestsSuite) talkedAs(talking *testClient) string {
	opened := talking.createSession(textSession(nil))
	talking.stopSession(opened.Id)
	s.Require().Eventually(func() bool {
		return slices.Contains(ids(talking.querySessions(SessionQuery{}).Items), opened.Id)
	}, settleFor, 20*time.Millisecond, "the conversation was never written down")
	return opened.Id
}

func (s *GuestsSuite) claim(guest, account string) ClaimGuestResult {
	var claimed ClaimGuestResult
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost,
		"/v1/agents/guests/claim", ClaimGuestRequest{GuestId: guest, UserId: account}, &claimed))
	return claimed
}

// asGuest is a client holding a guest's token for the id, as a minted guest does.
func (s *GuestsSuite) asGuest(id string) *testClient {
	return s.signedIn(jwt.MapClaims{"user_id": id, "role": "guest"}, auth.AuthTypeJWT, guest, id)
}

// scheduledFor is a task for the user, written as a caller of that kind would have.
func (s *GuestsSuite) scheduledFor(userID string, kind auth.Kind) string {
	ctx := context.Background()
	task := store.ScheduledTask{
		CustomerID: s.customerID(), UserID: userID, CallerKind: string(kind), APIKey: s.app.key,
		Title: "Stretch", Instructions: "Remind them to stretch.",
		StartLocal: time.Now().UTC().Add(48 * time.Hour).Truncate(time.Second), Timezone: "UTC",
	}
	s.Require().NoError(s.store.ScheduledTasks.Insert(ctx, &task, 0))
	s.T().Cleanup(func() { _ = s.store.ScheduledTasks.Delete(ctx, task.CustomerID, task.ID) })
	return task.ID
}

// transcriptOf is the Stream Chat channel the session's words were written to.
func (s *GuestsSuite) transcriptOf(sessionID string) string {
	var row store.AgentSession
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&row).Where("id = ?", sessionID).Scan(context.Background()))
	channel := transcriptChannel(row)
	_, kept := s.chat.Channel(channel)
	s.Require().True(kept, "the session wrote a transcript")
	return channel
}

// underTheName is an ended conversation an anonymous caller had that the row files under the
// name, in a channel the name's owner was never put in. An anonymous caller opening one under
// a name would put the name in its channel, so the row is moved under it afterwards.
func (s *GuestsSuite) underTheName(name string) string {
	talked := s.talkedAs(s.data.createAnonymous())
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.AgentSession)(nil)).
		Set("user_id = ?", name).Where("id = ?", talked).Exec(context.Background())
	s.Require().NoError(err)
	s.Require().NotContains(s.chat.Members(s.transcriptOf(talked)), name)
	return talked
}
