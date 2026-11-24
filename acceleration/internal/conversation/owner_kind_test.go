package conversation

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	getstream "github.com/GetStream/getstream-go/v5"
)

// OwnerKindSuite covers the kind of caller a conversation is kept for, beside their user id.
type OwnerKindSuite struct {
	suite.Suite
	db      *chatStore
	client  *getstream.Stream
	service *Service
}

func TestOwnerKindSuite(t *testing.T) {
	suite.Run(t, new(OwnerKindSuite))
}

func (s *OwnerKindSuite) SetupTest() {
	s.db, s.client = newChat(s.T())
	s.restarts()
}

func (s *OwnerKindSuite) TestANewChannelRecordsItsOwnersKind() {
	cid := s.keptFor(auth.KindGuest)

	s.Equal(map[string]any{"owner": "alice", "kind": "guest"},
		map[string]any{"owner": s.custom(cid)["support_owner_id"], "kind": s.custom(cid)["support_owner_kind"]})
}

func (s *OwnerKindSuite) TestTheCallersCustomDoesNotNameTheOwnersKind() {
	opened, _, _, err := s.service.OpenInApp(s.as(auth.KindGuest), 0, "customer", "agent", "", "alice", "",
		map[string]any{"support_owner_kind": "server", "plan": "pro"}, "")
	s.Require().NoError(err)

	s.Equal("guest", s.custom(opened.CID())["support_owner_kind"])
	s.Equal("pro", s.custom(opened.CID())["plan"])
}

func (s *OwnerKindSuite) TestASignedInUsersConversationIsTheirsAndTheirBackends() {
	cid := s.keptFor(auth.KindAuthenticated)

	s.reachedOnlyBy(cid, auth.KindAuthenticated, auth.KindServer)
}

func (s *OwnerKindSuite) TestAGuestsConversationIsTheirsAndTheBackends() {
	cid := s.keptFor(auth.KindGuest)

	s.reachedOnlyBy(cid, auth.KindGuest, auth.KindServer)
}

func (s *OwnerKindSuite) TestAnAnonymousCallersConversationIsTheirsAndTheBackends() {
	cid := s.keptFor(auth.KindAnonymous)

	s.reachedOnlyBy(cid, auth.KindAnonymous, auth.KindServer)
}

func (s *OwnerKindSuite) TestAConversationABackendOpenedForAUserIsTheUsersToo() {
	cid := s.keptFor(auth.KindServer)

	s.reachedOnlyBy(cid, auth.KindServer, auth.KindAuthenticated, auth.KindGuest)
}

func (s *OwnerKindSuite) TestAConversationABackendOpenedForNobodyIsTheBackends() {
	opened, _, _, err := s.service.OpenInApp(s.as(auth.KindServer), 0, "customer", "agent", "", "", "", nil, "")
	s.Require().NoError(err)

	s.Equal([]auth.Kind{auth.KindServer}, s.readableBy(opened.CID(), ""))
}

func (s *OwnerKindSuite) TestAChannelTheRouterStampedAKindOnIsNotOpenedOnAWorkersWord() {
	custom := map[string]any{"support_owner_kind": "authenticated"}

	s.False(opensDispatched(RouterOpensDispatched(s.T().Context(), "agent:support-1"), "agent:support-1", custom, false))
}

func (s *OwnerKindSuite) TestABackendNamingNobodyOpensNoUsersConversation() {
	cid := s.keptFor(auth.KindAuthenticated)

	_, err := s.service.HistoryForCaller(s.as(auth.KindServer), "customer", "agent", cid, "", "")

	s.ErrorContains(err, "another user")
}

func (s *OwnerKindSuite) TestAConversationWithNoKindRecordedIsItsOwnersById() {
	cid := s.keptFor(auth.KindAuthenticated)
	delete(s.custom(cid), "support_owner_kind")

	s.reachedOnlyBy(cid, auth.KindServer, auth.KindAuthenticated, auth.KindGuest, auth.KindAnonymous, "")
	_, err := s.service.HistoryForCaller(s.as(auth.KindAuthenticated), "customer", "agent", cid, "", "bob")
	s.ErrorContains(err, "another user")
}

func (s *OwnerKindSuite) TestARouterOpeningWithNoKindRecordsNone() {
	opened, _, _, err := s.service.OpenInApp(s.as(""), 0, "customer", "agent", "", "alice", "",
		map[string]any{"support_owner_kind": "anonymous"}, "")
	s.Require().NoError(err)

	s.NotContains(s.custom(opened.CID()), "support_owner_kind")
}

func (s *OwnerKindSuite) TestAMalformedKindIsRefusedToEveryCaller() {
	cid := s.keptFor(auth.KindAuthenticated)
	for _, kind := range []any{nil, "", true, map[string]any{"kind": "authenticated"}} {
		s.custom(cid)["support_owner_kind"] = kind

		s.reachedOnlyBy(cid)
	}
}

func (s *OwnerKindSuite) TestAConversationStillHeldIsItsOwnersKindsAlone() {
	opened, _, _, err := s.service.OpenInApp(s.as(auth.KindAuthenticated), 0, "customer", "agent", "", "alice", "", nil, "")
	s.Require().NoError(err)
	opened.Release()

	_, _, _, err = s.service.OpenInApp(s.as(auth.KindGuest), 0, "customer", "agent", opened.CID(), "alice", "", nil, "")
	s.Require().ErrorContains(err, "another user")
	_, _, _, err = s.service.OpenInApp(s.as(auth.KindAuthenticated), 0, "customer", "agent", opened.CID(), "alice", "", nil, "")
	s.NoError(err)
}

func (s *OwnerKindSuite) TestASharedConversationsOwnerIsHeldToTheirKind() {
	cid := s.shared(auth.KindAuthenticated)

	s.reachedOnlyBy(cid, auth.KindAuthenticated, auth.KindServer)
}

func (s *OwnerKindSuite) TestASharedConversationsOtherMemberIsAVerifiedUser() {
	cid := s.shared(auth.KindAuthenticated)

	s.Equal([]auth.Kind{auth.KindServer, auth.KindAuthenticated, auth.KindGuest}, s.readableBy(cid, "bob"))
}

func (s *OwnerKindSuite) TestAMemberOfASharedConversationWithNoKindRecordedIsAVerifiedUser() {
	cid := s.shared("")

	s.Equal([]auth.Kind{auth.KindServer, auth.KindAuthenticated, auth.KindGuest}, s.readableBy(cid, "bob"))
	s.reachedOnlyBy(cid, auth.KindServer, auth.KindAuthenticated, auth.KindGuest, auth.KindAnonymous, "")
}

func (s *OwnerKindSuite) TestASharedConversationsCallerIsCheckedAgainAsTheKindThatOpenedIt() {
	// Kept with no kind and opened for none, then opened by bob while it is still held.
	cid := s.shared("")
	opened, _, _, err := s.service.OpenInApp(s.as(""), 0, "customer", "agent", cid, "alice", "", nil, "")
	s.Require().NoError(err)
	opened.Release()
	held, _, _, err := s.service.OpenInApp(s.as(auth.KindAuthenticated), 0, "customer", "agent", cid, "bob", "", nil, "")
	s.Require().NoError(err)
	s.NoError(held.CheckCaller(s.T().Context(), "bob"))
	held.Release()

	s.restarts()
	fresh, _, _, err := s.service.OpenInApp(s.as(auth.KindGuest), 0, "customer", "agent", cid, "bob", "", nil, "")
	s.Require().NoError(err)
	s.NoError(fresh.CheckCaller(s.T().Context(), "bob"))
}

func (s *OwnerKindSuite) TestABranchSwitchReadsTheConversationAsTheKindThatOpenedIt() {
	opened, _, _, err := s.service.OpenInApp(s.as(auth.KindAuthenticated), 0, "customer", "agent", "", "alice", "", nil, "")
	s.Require().NoError(err)

	_, err = opened.readUpTo(s.T().Context(), s.client, "", nil)

	s.NoError(err)
}

// reachedOnlyBy checks that of the callers named alice, those of kinds alone have the
// conversation, whichever way it is asked for.
func (s *OwnerKindSuite) reachedOnlyBy(cid string, kinds ...auth.Kind) {
	s.ElementsMatch(kinds, s.readableBy(cid, "alice"), "reading its history")

	var restored, commanded, opened []auth.Kind
	for _, kind := range s.kinds() {
		if _, _, err := s.service.ContextForCaller(s.as(kind), "customer", "agent", cid, "alice"); err == nil {
			restored = append(restored, kind)
		}
		if _, err := s.service.CommandForCaller(s.as(kind), "customer", "agent", cid, "alice", "alices-command"); err == nil {
			commanded = append(commanded, kind)
		}
		s.restarts()
		if c, _, _, err := s.service.OpenInApp(s.as(kind), 0, "customer", "agent", cid, "alice", "", nil, ""); err == nil {
			opened = append(opened, kind)
			c.Release()
		}
	}
	s.ElementsMatch(kinds, restored, "restoring its context")
	s.ElementsMatch(kinds, commanded, "reading a command")
	s.ElementsMatch(kinds, opened, "opening it")
}

// readableBy is the kinds of caller named user that read the conversation's history.
func (s *OwnerKindSuite) readableBy(cid, user string) []auth.Kind {
	var read []auth.Kind
	for _, kind := range s.kinds() {
		if _, err := s.service.HistoryForCaller(s.as(kind), "customer", "agent", cid, "", user); err == nil {
			read = append(read, kind)
		}
	}
	return read
}

func (s *OwnerKindSuite) kinds() []auth.Kind {
	return []auth.Kind{auth.KindServer, auth.KindAuthenticated, auth.KindGuest, auth.KindAnonymous, ""}
}

// keptFor is a conversation opened for alice, a caller of kind, with one command of hers, which
// no router holds.
func (s *OwnerKindSuite) keptFor(kind auth.Kind) string {
	opened, _, _, err := s.service.OpenInApp(s.as(kind), 0, "customer", "agent", "", "alice", "", nil, "")
	s.Require().NoError(err)
	_, err = opened.BeginCommand("alices-command", "Hello", "")
	s.Require().NoError(err)
	opened.Cancel()
	saved(s.T(), opened)
	opened.Release()
	s.restarts()
	return opened.CID()
}

// shared is alice's conversation as keptFor keeps it, open to its members, of whom bob is one.
func (s *OwnerKindSuite) shared(kind auth.Kind) string {
	cid := s.keptFor(kind)
	s.custom(cid)["support_access"] = "members"
	channel := s.channel(cid)
	channel["members"] = append(channel["members"].([]any), map[string]any{"user_id": "bob"})
	return cid
}

func (s *OwnerKindSuite) as(kind auth.Kind) context.Context {
	return WithCallerKind(s.T().Context(), kind)
}

// restarts is a router that holds no conversation.
func (s *OwnerKindSuite) restarts() {
	if s.service != nil {
		s.service.Close()
	}
	s.service = NewForChat(s.client)
	s.T().Cleanup(s.service.Close)
}

func (s *OwnerKindSuite) channel(cid string) map[string]any {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	return s.db.channels[strings.TrimPrefix(cid, "agent:")]
}

func (s *OwnerKindSuite) custom(cid string) map[string]any {
	return s.channel(cid)["custom"].(map[string]any)
}
