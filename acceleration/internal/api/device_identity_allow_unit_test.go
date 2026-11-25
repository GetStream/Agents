package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
)

// The setting is read once at start, and one that does not read stops the router, since
// ignoring it would bring the refusal back for the device it was set for.
func TestDeviceIdentityAllowReadsEachEntryExactly(t *testing.T) {
	good := map[string]struct {
		setting string
		want    DeviceIdentityAllow
	}{
		"empty":                   {"", nil},
		"blank":                   {" \n", nil},
		"one entry":               {"app-1|docs|u1|Docs: AI", DeviceIdentityAllow{{"app-1", "docs", "u1", "Docs: AI"}}},
		"no user":                 {"app-1|docs||", DeviceIdentityAllow{{"app-1", "docs", "", ""}}},
		"two apps":                {"app-1|a|u|n;app-2|b|v|m", DeviceIdentityAllow{{"app-1", "a", "u", "n"}, {"app-2", "b", "v", "m"}}},
		"whitespace":              {" app-1|a|u|Name With Spaces \n", DeviceIdentityAllow{{"app-1", "a", "u", "Name With Spaces"}}},
		"space after a separator": {"app-1|a|u|n;\n app-2|b|v|m", DeviceIdentityAllow{{"app-1", "a", "u", "n"}, {"app-2", "b", "v", "m"}}},
	}
	for name, read := range good {
		t.Run(name, func(t *testing.T) {
			allow, err := ParseDeviceIdentityAllow(read.setting)
			require.NoError(t, err)
			require.Equal(t, read.want, allow)
		})
	}

	bad := map[string]string{
		"three fields":   "x-secret|x-secret|x-secret",
		"five fields":    "x-secret|x-secret|x-secret|x-secret|x-secret",
		"no separators":  "x-secret",
		"no app":         "|x-secret|x-secret|x-secret",
		"no agent":       "x-secret||x-secret|x-secret",
		"second is bad":  "x-secret|x-secret|x-secret|x-secret;x-secret|x-secret",
		"empty entry":    "x-secret|x-secret|x-secret|x-secret;;x-secret|x-secret|x-secret|x-secret",
		"only separator": ";",
	}
	for name, setting := range bad {
		t.Run(name, func(t *testing.T) {
			_, err := ParseDeviceIdentityAllow(setting)
			require.Error(t, err)
			require.Contains(t, err.Error(), "ROUTER_DEVICE_IDENTITY_ALLOW")
			require.NotContains(t, err.Error(), "x-secret", "the error never repeats the value")
		})
	}
}

// deviceOf is a device of an app, going by userID, as the gateway verified it.
func deviceOf(app string, kind auth.Kind, userID string) context.Context {
	return context.WithValue(deviceAsking(kind, userID), customerContextKey{}, app)
}

// TestAnAllowedIdentityPassesOnlyForItsAppAndEveryFieldExactly: the exception is one entry, of
// one app, for a session that keeps its conversation; anything else is the refusal it was.
func TestAnAllowedIdentityPassesOnlyForItsAppAndEveryFieldExactly(t *testing.T) {
	allow := DeviceIdentityAllow{{"app-1", "docs", "docs-user", "Docs AI"}, {"app-2", "other", "", ""}}
	exact := CreateSessionRequest{AgentId: new("docs"), UserId: new("docs-user"), UserName: new("Docs AI")}
	with := func(change func(*CreateSessionRequest)) CreateSessionRequest {
		request := exact
		change(&request)
		return request
	}
	device := deviceOf("app-1", auth.KindAnonymous, "visitor")

	cases := map[string]struct {
		ctx     context.Context
		sent    CreateSessionRequest
		allowed bool
		refusal APIError
	}{
		"exact":                   {device, exact, true, APIError{}},
		"agent only":              {device, CreateSessionRequest{AgentId: new("docs")}, true, APIError{}},
		"defaults and agent":      {device, CreateSessionRequest{AgentId: new("docs"), UserId: new(session.DefaultUserID), UserName: new(session.DefaultUserName)}, true, APIError{}},
		"own id and agent":        {deviceOf("app-1", auth.KindAuthenticated, "alice"), CreateSessionRequest{AgentId: new("docs"), UserId: new("alice")}, true, APIError{}},
		"incognito false":         {device, with(func(r *CreateSessionRequest) { r.Incognito = new(false) }), true, APIError{}},
		"another agent":           {device, with(func(r *CreateSessionRequest) { r.AgentId = new("docs2") }), false, errDeviceAgentID},
		"another user":            {device, with(func(r *CreateSessionRequest) { r.UserId = new("docs-user2") }), false, errDeviceAgentID},
		"another name":            {device, with(func(r *CreateSessionRequest) { r.UserName = new("Docs AI 2") }), false, errDeviceAgentID},
		"agent of the other app":  {device, CreateSessionRequest{AgentId: new("other")}, false, errDeviceAgentID},
		"an anonymous own id":     {deviceOf("app-1", auth.KindAnonymous, "visitor"), CreateSessionRequest{AgentId: new("docs"), UserId: new("visitor")}, false, errDeviceAgentID},
		"incognito":               {device, with(func(r *CreateSessionRequest) { r.Incognito = new(true) }), false, errDeviceAgentID},
		"the same values, app 3":  {deviceOf("app-3", auth.KindAnonymous, "visitor"), exact, false, errDeviceAgentID},
		"no app":                  {deviceAsking(auth.KindAnonymous, "visitor"), exact, false, errDeviceAgentID},
		"entry names no user":     {deviceOf("app-2", auth.KindAnonymous, "visitor"), CreateSessionRequest{AgentId: new("other"), UserId: new("docs-user")}, false, errDeviceAgentID},
		"entry names no name":     {deviceOf("app-2", auth.KindAnonymous, "visitor"), CreateSessionRequest{AgentId: new("other"), UserName: new("Docs AI")}, false, errDeviceAgentID},
		"entry of another app":    {deviceOf("app-2", auth.KindAnonymous, "visitor"), exact, false, errDeviceAgentID},
		"fields of two entries":   {deviceOf("app-2", auth.KindAnonymous, "visitor"), CreateSessionRequest{AgentId: new("other"), UserName: new("Docs AI")}, false, errDeviceAgentID},
		"the second entry's own":  {deviceOf("app-2", auth.KindAnonymous, "visitor"), CreateSessionRequest{AgentId: new("other")}, true, APIError{}},
		"user only":               {device, CreateSessionRequest{UserId: new("docs-user")}, true, APIError{}},
		"another user only":       {device, CreateSessionRequest{UserId: new("docs-user2")}, false, errDeviceAgentUser},
		"another name only":       {device, CreateSessionRequest{UserName: new("Docs AI 2")}, false, errDeviceAgentUserName},
		"an empty agent id":       {device, CreateSessionRequest{AgentId: new(""), UserId: new("docs-user")}, true, APIError{}},
		"an empty identity field": {device, CreateSessionRequest{AgentId: new("docs"), UserId: new(""), UserName: new("")}, true, APIError{}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			failure, refused := refuseSessionRequest(c.ctx, c.sent, allow)

			require.Equal(t, !c.allowed, refused)
			require.Equal(t, c.refusal, failure)
		})
	}

	t.Run("the backend names whatever it likes with or without the setting", func(t *testing.T) {
		backend := context.WithValue(context.Background(), serverSideContextKey{}, true)
		for _, withAllow := range []DeviceIdentityAllow{nil, allow} {
			_, refused := refuseSessionRequest(backend, with(func(r *CreateSessionRequest) { r.AgentId = new("anything") }), withAllow)
			require.False(t, refused)
		}
	})

	t.Run("without the setting the identity is refused as it was", func(t *testing.T) {
		failure, refused := refuseSessionRequest(device, exact, nil)
		require.True(t, refused)
		require.Equal(t, errDeviceAgentID, failure)
	})
}
