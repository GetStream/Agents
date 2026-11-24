package session

import (
	"maps"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	persistent "github.com/GetStream/Vision-Agents/acceleration/internal/conversation"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	getstream "github.com/GetStream/getstream-go/v5"
)

// sharedConversation is a Chat holding a conversation alice and bob are members of, and the spec
// of a session of alice's, signed in, on it.
func (s *SessionSuite) sharedConversation() (*chattest.Server, Spec) {
	server := chattest.NewServer(s.T())
	service := persistent.NewForChat(server.Client)
	s.T().Cleanup(service.Close)
	s.conversations = service
	s.manages()
	const channelID, agentID = "support-12345678-1234-1234-1234-123456789abe", "test-agent"
	creator := agentID
	_, err := server.Client.Chat().GetOrCreateChannel(s.ctx, "agent", channelID, &getstream.GetOrCreateChannelRequest{
		Data: &getstream.ChannelInput{CreatedByID: &creator,
			Members: []getstream.ChannelMemberRequest{{UserID: agentID}, {UserID: "alice"}, {UserID: "bob"}},
			Custom: map[string]any{
				"support_customer_id": "acme", "support_agent_id": agentID,
				"support_owner_id": "alice", "support_access": "members",
				persistent.TriggerField: persistent.SessionCommandTrigger,
			}},
	})
	s.Require().NoError(err)
	return server, Spec{CustomerID: "acme", Text: true, PersistConversation: true,
		AgentID: agentID, ConversationID: "agent:" + channelID, LLMTarget: "en-low-latency",
		Caller: routing.Caller{UserID: "alice"}, CallerKind: auth.KindAuthenticated}
}

// Only a signed-in member ends the session another member stopped watching: a member is known
// by user id, and a user id is the member's own only for a signed-in caller.
func (s *SessionSuite) TestOnlyASignedInMemberTakesOverASharedConversation() {
	for _, opener := range []struct {
		name  string
		user  string
		kind  auth.Kind
		takes bool
	}{
		{"a signed-in member", "bob", auth.KindAuthenticated, true},
		{"a signed-in user who is no member", "carol", auth.KindAuthenticated, false},
		{"an anonymous caller with a member's id", "bob", auth.KindAnonymous, false},
		{"a guest with a member's id", "bob", auth.KindGuest, false},
		{"a guest with the owner's id", "alice", auth.KindGuest, false},
		{"a caller of no kind with a member's id", "bob", "", false},
	} {
		s.Run(opener.name, func() {
			_, spec := s.sharedConversation()
			alice, err := s.manager.Create(s.ctx, spec)
			s.Require().NoError(err)
			spec.Caller.UserID, spec.CallerKind = opener.user, opener.kind

			_, err = s.manager.Create(s.ctx, spec)

			if opener.takes {
				s.Require().NoError(err)
				s.Equal(Ended, alice.State())
				return
			}
			s.Require().ErrorIs(err, persistent.ErrAlreadyOpen)
			s.NotEqual(Ended, alice.State())
		})
	}
}

// A signed-in user is not another member than a guest of the same id whose session holds the
// conversation.
func (s *SessionSuite) TestASignedInUserLeavesASharedSessionOfAGuestWithTheirID() {
	_, spec := s.sharedConversation()
	spec.CallerKind = auth.KindGuest
	guest, err := s.manager.Create(s.ctx, spec)
	s.Require().NoError(err)
	spec.CallerKind = auth.KindAuthenticated

	_, err = s.manager.Create(s.ctx, spec)

	s.Require().ErrorIs(err, persistent.ErrAlreadyOpen)
	s.NotEqual(Ended, guest.State())
}

// An anonymous caller going by the user a backend opened a conversation for does not reach the
// backend's session.
func (s *SessionSuite) TestAnUnverifiedCallerLeavesTheSessionABackendOpenedForThatName() {
	_, spec := s.sharedConversation()
	spec.ConversationID, spec.CallerKind = "", auth.KindServer
	backend, err := s.manager.Create(s.ctx, spec)
	s.Require().NoError(err)
	spec.ConversationID, spec.CallerKind = backend.Spec().ConversationID, auth.KindAnonymous
	spec.ID = ""

	_, err = s.manager.Create(s.ctx, spec)

	s.Require().ErrorIs(err, persistent.ErrAlreadyOpen)
	s.NotEqual(Ended, backend.State())
}

func (s *SessionSuite) TestASessionWatchedWhileItsConversationIsReadIsNotTakenOver() {
	server, spec := s.sharedConversation()
	alice, err := s.manager.Create(s.ctx, spec)
	s.Require().NoError(err)
	bobs := spec
	bobs.Caller.UserID = "bob"
	waiting, release := server.Hold("/query")
	refused := make(chan error, 1)
	go func() {
		_, err := s.manager.Create(s.ctx, bobs)
		refused <- err
	}()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		s.FailNow("the conversation was not read")
	}
	_, detach := alice.Watch()
	defer detach()
	release()

	s.Require().ErrorIs(<-refused, persistent.ErrAlreadyOpen)
	s.NotEqual(Ended, alice.State())
	_, held := s.manager.Get(alice.ID(), OwnerOf(alice.Spec()))
	s.True(held, "she still finds it")
}

// A session closed while the conversation is read is let go of once: one held under its id since
// stays.
func (s *SessionSuite) TestASessionReplacedWhileItsConversationIsReadKeepsItsReplacement() {
	server, spec := s.sharedConversation()
	spec.ID = "shared-1"
	alice, err := s.manager.Create(s.ctx, spec)
	s.Require().NoError(err)
	bobs := spec
	bobs.ID, bobs.Caller.UserID = "shared-2", "bob"
	waiting, release := server.Hold("/query")
	done := make(chan error, 1)
	go func() {
		_, err := s.manager.Create(s.ctx, bobs)
		done <- err
	}()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		s.FailNow("the conversation was not read")
	}
	// Off the map and still ending, as a session whose close is under way: Close removes it first.
	s.manager.mu.Lock()
	delete(s.manager.sessions, alice.Key())
	s.manager.mu.Unlock()
	replacing := spec
	replacing.ConversationID = ""
	replacement, err := s.manager.Create(s.ctx, replacing)
	s.Require().NoError(err)
	release()
	<-done

	held, found := s.manager.Get("shared-1", OwnerOf(replacement.Spec()))
	s.Require().True(found, "the session held under the id since is still held")
	s.Same(replacement, held)
	s.Require().NoError(alice.Close())
}

// Defect: the owner was read off the spec its session writes a resume's timeout to.
func (s *SessionSuite) TestATakeOverReadsASessionsOwnerWhileItsTimeoutIsSet() {
	_, spec := s.sharedConversation()
	alice, err := s.manager.Create(s.ctx, spec)
	s.Require().NoError(err)
	spec.Caller.UserID = "carol"
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				alice.SetToolTimeout(i%1000 + 1)
			}
		}
	}()
	for range 20 {
		_, err = s.manager.Create(s.ctx, spec)
		s.ErrorIs(err, persistent.ErrAlreadyOpen)
	}
	close(stop)
	<-stopped
	s.NotEqual(Ended, alice.State())
}

// Only an open of the thread channel as one takes over a session held on it, and it takes over
// none of another kind.
func (s *SessionSuite) TestATakeOverEndsOnlyASessionHoldingTheChannelAsItDoes() {
	for _, thread := range []bool{true, false} {
		_, spec := s.sharedConversation()
		spec.CallerKind, spec.Thread = auth.KindServer, thread
		held, err := s.manager.Create(s.ctx, spec)
		s.Require().NoError(err)
		spec.Thread = !thread

		_, err = s.manager.Create(s.ctx, spec)

		s.Require().Error(err)
		s.True(strings.Contains(err.Error(), "already open"), err.Error())
		s.NotEqual(Ended, held.State())
	}
}

// A conversation no longer open to its members is its owner's alone, but the session another
// member holds on it is not: the open is refused, and ends nothing.
func (s *SessionSuite) TestATakeOverLeavesAMembersSessionOnAConversationNoLongerShared() {
	server, spec := s.sharedConversation()
	owners := spec
	spec.Caller.UserID = "bob"
	bob, err := s.manager.Create(s.ctx, spec)
	s.Require().NoError(err)
	s.restamp(server, "support_access", "owner")

	_, err = s.manager.Create(s.ctx, owners)

	s.Require().ErrorIs(err, persistent.ErrAlreadyOpen)
	s.NotEqual(Ended, bob.State())
}

// Two customers' Stream apps may each have a channel of one id.
func (s *SessionSuite) TestATakeOverLeavesAnotherCustomersSessionOnAChannelOfTheSameID() {
	server, spec := s.sharedConversation()
	alice, err := s.manager.Create(s.ctx, spec)
	s.Require().NoError(err)
	s.restamp(server, "support_customer_id", "other")
	spec.CustomerID, spec.Caller.UserID = "other", "bob"

	_, err = s.manager.Create(s.ctx, spec)

	s.Require().NoError(err)
	s.NotEqual(Ended, alice.State())
}

// restamp changes a custom field of sharedConversation's channel.
func (s *SessionSuite) restamp(server *chattest.Server, field, to string) {
	const channelID = "support-12345678-1234-1234-1234-123456789abe"
	channel, found := server.Channel(channelID)
	s.Require().True(found)
	custom := maps.Clone(channel["custom"].(map[string]any))
	custom[field] = to
	_, err := server.Client.Chat().UpdateChannelPartial(s.ctx, "agent", channelID,
		&getstream.UpdateChannelPartialRequest{Set: map[string]any{"custom": custom}})
	s.Require().NoError(err)
}
