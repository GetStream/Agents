package api

import (
	"testing"

	"github.com/stretchr/testify/require"
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
		failure, refused := refuseSessionRequest(backendAsking(), sent)

		require.True(t, refused, field)
		require.Contains(t, failure.Message, field+" may not start with !", field)
	}
}

func TestABackendNamingOrdinaryIdsForASessionIsLetThrough(t *testing.T) {
	_, refused := refuseSessionRequest(backendAsking(), CreateSessionRequest{
		UserId: new("support-bot"), AgentId: new("support"), Memory: &SessionMemory{UserId: new("alice")},
	})

	require.False(t, refused)
}
