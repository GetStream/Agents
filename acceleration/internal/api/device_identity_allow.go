package api

import (
	"context"
	"fmt"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
)

// DeviceIdentity is the one identity a device of a Stream app may send for the agent.
type DeviceIdentity struct {
	// App is the Stream app id the device belongs to, as the gateway sets it on the request
	// (X-Stream-App-Id) and the Stream dashboard shows it.
	App string
	// AgentID, UserID and UserName are what the device may send. An empty UserID or UserName
	// means the device may not send that field (its default is still allowed).
	AgentID, UserID, UserName string
}

// DeviceIdentityAllow is the identities ROUTER_DEVICE_IDENTITY_ALLOW lets a device send.
//
// TEMPORARY (AI-3959): it is here until a docs chat widget stops sending an identity
// (getstream.io#1264 live on the prod site), and goes with it. AI-3900 is the lasting fix.
type DeviceIdentityAllow []DeviceIdentity

// deviceIdentityAllowSetting names the setting in the error ParseDeviceIdentityAllow returns.
const deviceIdentityAllowSetting = "device_identity_allow (ROUTER_DEVICE_IDENTITY_ALLOW)"

// ParseDeviceIdentityAllow reads the setting: entries separated by ";", each
// "app|agent_id|user_id|user_name". Empty is no entry. An error names the entry by its
// position and never repeats the value.
func ParseDeviceIdentityAllow(setting string) (DeviceIdentityAllow, error) {
	setting = strings.TrimSpace(setting)
	if setting == "" {
		return nil, nil
	}
	var allow DeviceIdentityAllow
	for i, entry := range strings.Split(setting, ";") {
		fields := strings.Split(strings.TrimSpace(entry), "|")
		if len(fields) != 4 {
			return nil, fmt.Errorf("api: %s: entry %d is not app|agent_id|user_id|user_name", deviceIdentityAllowSetting, i+1)
		}
		identity := DeviceIdentity{App: fields[0], AgentID: fields[1], UserID: fields[2], UserName: fields[3]}
		if identity.App == "" || identity.AgentID == "" {
			return nil, fmt.Errorf("api: %s: entry %d has no app or no agent_id", deviceIdentityAllowSetting, i+1)
		}
		allow = append(allow, identity)
	}
	return allow, nil
}

// lets reports whether a device's new session names an identity an entry of its app allows.
// It is the case only for a session that keeps its conversation, since an incognito one has
// nothing the agent's identity could be checked against afterwards, and when every field the
// request sends that refuseDeviceAgentIdentity would refuse equals the entry's.
func (a DeviceIdentityAllow) lets(ctx context.Context, request CreateSessionRequest) bool {
	app, _ := CustomerFrom(ctx)
	if value(request.Incognito) {
		return false
	}
	for _, entry := range a {
		if entry.App != app {
			continue
		}
		agent, user, name := value(request.AgentId), value(request.UserId), value(request.UserName)
		if (agent == "" || agent == entry.AgentID) &&
			(user == "" || user == session.DefaultUserID || namesOwnUser(ctx, user) || user == entry.UserID) &&
			(name == "" || name == session.DefaultUserName || name == entry.UserName) {
			return true
		}
	}
	return false
}

// namesOwnUser reports whether user, which is not empty, is the verified user id of the caller.
func namesOwnUser(ctx context.Context, user string) bool {
	return KindFrom(ctx).Verified() && user == CallerFrom(ctx).UserID
}
