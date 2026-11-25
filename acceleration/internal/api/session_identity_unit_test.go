package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
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

// A device's voice session is keyed under its call id, which is the session id it chose, so it
// is given an agent id nobody chose instead.
func TestADevicesVoiceSessionIsNotKeyedUnderTheIdItChose(t *testing.T) {
	for _, kind := range []auth.Kind{auth.KindAuthenticated, auth.KindGuest, auth.KindAnonymous} {
		spec := session.Spec{ID: "bob"}
		keyDeviceAgent(deviceAsking(kind, "alice"), &spec)

		require.NotEmpty(t, spec.AgentID, string(kind))
		require.NotEqual(t, "bob", spec.AgentID, string(kind))
	}
}

func TestABackendsVoiceSessionIsStillKeyedUnderItsCall(t *testing.T) {
	spec := session.Spec{ID: "bob"}
	keyDeviceAgent(backendAsking(), &spec)

	require.Empty(t, spec.AgentID, "Normalize keys it under its call id")
}

// A text session gets a fresh agent id from Normalize, and a resumed one its conversation's.
func TestADevicesTextOrResumedSessionKeepsItsAgentIDUnset(t *testing.T) {
	for name, spec := range map[string]session.Spec{
		"text":    {ID: "bob", Text: true},
		"resumed": {ID: "bob", ConversationID: "agent:mine"},
	} {
		keyDeviceAgent(deviceAsking(auth.KindAuthenticated, "alice"), &spec)

		require.Empty(t, spec.AgentID, name)
	}
}

// A config's identity fills in only what the request left to it.
func TestASessionTakesTheConfigsIdentityWhereTheRequestNamedNone(t *testing.T) {
	config := store.AgentIdentity{AgentID: "support", UserID: "support-bot", UserName: "Support"}
	for name, c := range map[string]struct {
		asked, want session.Spec
	}{
		"nothing named": {
			asked: session.Spec{},
			want:  session.Spec{AgentID: "support", AgentIDName: "Support", UserID: "support-bot", UserName: "Support"},
		},
		"the defaults named": {
			asked: session.Spec{UserID: session.DefaultUserID, UserName: session.DefaultUserName},
			want:  session.Spec{AgentID: "support", AgentIDName: "Support", UserID: "support-bot", UserName: "Support"},
		},
		"its own named": {
			asked: session.Spec{AgentID: "own", UserID: "alice", UserName: "Own"},
			want:  session.Spec{AgentID: "own", UserID: "alice", UserName: "Own"},
		},
		"carrying on a conversation": {
			asked: session.Spec{ConversationID: "agent:held"},
			want:  session.Spec{ConversationID: "agent:held", UserID: "support-bot", UserName: "Support"},
		},
		"incognito": {
			asked: session.Spec{Incognito: true},
			want:  session.Spec{Incognito: true, UserID: "support-bot", UserName: "Support"},
		},
	} {
		spec := c.asked
		identifyAs(&spec, config)

		require.Equal(t, c.want, spec, name)
	}
}

// Each field of the identity stands alone: one the config leaves empty leaves the session as it was.
func TestAConfigsIdentityFieldLeftEmptyNamesNothing(t *testing.T) {
	for name, c := range map[string]struct {
		identity store.AgentIdentity
		want     session.Spec
	}{
		"none":         {want: session.Spec{UserID: session.DefaultUserID, UserName: session.DefaultUserName}},
		"agent id":     {identity: store.AgentIdentity{AgentID: "support"}, want: session.Spec{AgentID: "support", UserID: session.DefaultUserID, UserName: session.DefaultUserName}},
		"user id":      {identity: store.AgentIdentity{UserID: "support-bot"}, want: session.Spec{UserID: "support-bot", UserName: session.DefaultUserName}},
		"display name": {identity: store.AgentIdentity{UserName: "Support"}, want: session.Spec{UserID: session.DefaultUserID, UserName: "Support"}},
	} {
		spec := session.Spec{UserID: session.DefaultUserID, UserName: session.DefaultUserName}
		identifyAs(&spec, c.identity)

		require.Equal(t, c.want, spec, name)
	}
}
