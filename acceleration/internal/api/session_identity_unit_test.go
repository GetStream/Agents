package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
)

// deviceAsking is an end user's device of the given kind, going by userID.
func deviceAsking(kind auth.Kind, userID string) context.Context {
	ctx := context.WithValue(context.Background(), kindContextKey{}, kind)
	return routing.WithCaller(ctx, routing.Caller{UserID: userID})
}

func TestADeviceMayNotNameAnotherUserForTheAgentToJoinAndWriteAs(t *testing.T) {
	for _, kind := range []auth.Kind{auth.KindAuthenticated, auth.KindGuest, auth.KindAnonymous} {
		failure, refused := refuseSessionRequest(deviceAsking(kind, "alice"), CreateSessionRequest{UserId: new("bob")})

		require.True(t, refused, string(kind))
		require.Equal(t, errDeviceAgentUser, failure, string(kind))
	}
}

func TestAnAnonymousCallerMayNotNameItsOwnClaimedIdForTheAgent(t *testing.T) {
	failure, refused := refuseSessionRequest(deviceAsking(auth.KindAnonymous, "bob"), CreateSessionRequest{UserId: new("bob")})

	require.True(t, refused, "an anonymous caller's name is a claim nobody checked")
	require.Equal(t, errDeviceAgentUser, failure)
}

func TestASignedInDeviceMayNameItsOwnUserIdForTheAgent(t *testing.T) {
	for _, kind := range []auth.Kind{auth.KindAuthenticated, auth.KindGuest} {
		_, refused := refuseSessionRequest(deviceAsking(kind, "alice"), CreateSessionRequest{UserId: new("alice")})

		require.False(t, refused, string(kind))
	}
}

func TestADeviceMayNotNameTheAgentsId(t *testing.T) {
	for _, kind := range []auth.Kind{auth.KindAuthenticated, auth.KindGuest, auth.KindAnonymous} {
		failure, refused := refuseSessionRequest(deviceAsking(kind, "alice"), CreateSessionRequest{AgentId: new("alice")})

		require.True(t, refused, string(kind))
		require.Equal(t, errDeviceAgentID, failure, string(kind))
	}
}

func TestADeviceMayNotRenameTheAgentsChatUser(t *testing.T) {
	failure, refused := refuseSessionRequest(deviceAsking(auth.KindAuthenticated, "alice"),
		CreateSessionRequest{UserName: new("Your Bank")})

	require.True(t, refused)
	require.Equal(t, errDeviceAgentUserName, failure)
}

// Generated clients fill in the schema defaults, so a device that sends them is let through.
func TestADeviceSendingTheDefaultAgentIdentityIsLetThrough(t *testing.T) {
	sent := CreateSessionRequest{
		UserId: new(session.DefaultUserID), UserName: new(session.DefaultUserName), AgentId: new(""),
	}
	for _, kind := range []auth.Kind{auth.KindAuthenticated, auth.KindGuest, auth.KindAnonymous} {
		_, refused := refuseSessionRequest(deviceAsking(kind, "alice"), sent)

		require.False(t, refused, string(kind))
	}
}

func TestABackendNamesAnyAgentIdentity(t *testing.T) {
	_, refused := refuseSessionRequest(backendAsking(),
		CreateSessionRequest{UserId: new("support-bot"), UserName: new("Support"), AgentId: new("support")})

	require.False(t, refused)
}
