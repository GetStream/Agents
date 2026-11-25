package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
)

func TestAUserIdStartingWithTheReservedPrefixIsRefused(t *testing.T) {
	failure, refused := refuseReservedUserID("user_id", "!anon")

	require.True(t, refused)
	require.Equal(t, ErrorTypeInvalidRequest, failure.Type)
	require.Contains(t, failure.Message, "user_id may not start with !")
}

func TestAReservedUserIdIsRefusedThroughLeadingSpace(t *testing.T) {
	_, refused := refuseReservedUserID("user_id", "  !anon")

	require.True(t, refused)
}

func TestAnOrdinaryOrEmptyUserIdIsLetThrough(t *testing.T) {
	for _, id := range []string{"", "alice", "a!ice", "@alice"} {
		_, refused := refuseReservedUserID("user_id", id)

		require.False(t, refused, id)
	}
}

func TestABackendNamingAReservedUserIdForASessionIsRefused(t *testing.T) {
	for field, sent := range map[string]CreateSessionRequest{
		"user_id":        {UserId: new("!anon")},
		"agent_id":       {AgentId: new("!anon")},
		"memory.user_id": {Memory: &SessionMemory{UserId: new("!anon")}},
	} {
		failure, refused := refuseSessionRequest(backendAsking(), sent, nil)

		require.True(t, refused, field)
		require.Contains(t, failure.Message, field+" may not start with !", field)
	}
}

func TestABackendNamingOrdinaryIdsForASessionIsLetThrough(t *testing.T) {
	_, refused := refuseSessionRequest(backendAsking(), CreateSessionRequest{
		UserId: new("support-bot"), AgentId: new("support"), Memory: &SessionMemory{UserId: new("alice")},
	}, nil)

	require.False(t, refused)
}

// actingFor is a backend's request made for userID, named in X-Stream-User-Id.
func actingFor(userID string) context.Context {
	return routing.WithCaller(backendAsking(), routing.Caller{UserID: userID, ServerSide: true})
}

func TestABackendActingForAReservedUserIdOpensNoSession(t *testing.T) {
	t.Setenv(reservedIDsSwitch.Env, "true")

	failure, refused := refuseSessionRequest(actingFor("!anon"), CreateSessionRequest{}, nil)

	require.True(t, refused)
	require.Contains(t, failure.Message, "may not start with !")
}

func TestABackendActingForAnOrdinaryUserIdOpensASession(t *testing.T) {
	_, refused := refuseSessionRequest(actingFor("alice"), CreateSessionRequest{}, nil)

	require.False(t, refused)
}

// A voice session that names no agent id is keyed under its call id (Spec.KeyedAgentID).
func TestABackendsCallIdThatWouldBecomeAReservedAgentIdIsRefused(t *testing.T) {
	t.Setenv(reservedIDsSwitch.Env, "true")

	failure, refused := refuseSessionRequest(backendAsking(), CreateSessionRequest{
		Call: &SessionCall{Type: "default", Id: "!anon"}, StartVoice: new(true),
	}, nil)

	require.True(t, refused)
	require.Contains(t, failure.Message, "call.id may not start with !")
}

// Until its switch is on, the change keeps the old answer: the request is let through.
func TestWithTheSwitchOffAReservedActingForIdOrCallIdIsLetThrough(t *testing.T) {
	_, refused := refuseSessionRequest(actingFor("!anon"), CreateSessionRequest{
		Call: &SessionCall{Type: "default", Id: "!anon"}, StartVoice: new(true),
	}, nil)

	require.False(t, refused)
}
