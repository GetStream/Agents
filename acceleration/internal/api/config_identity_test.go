//go:build integration

package api

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
)

// routerMadeAgentID is an agent id the router makes for a session that names none (session.newID).
var routerMadeAgentID = regexp.MustCompile(`^[0-9a-f]{32}$`)

func (s *ConfigsSuite) TestAConfigKeepsItsAgentIdentityUntilAWriteNamesAnother() {
	created := s.createConfig(map[string]any{"name": "support", "identity": map[string]any{
		"agent_id": "support", "user_id": "support-bot", "user_name": "Support"}})
	s.Equal(&AgentIdentity{AgentId: "support", UserId: "support-bot", UserName: "Support"}, s.read(created.Id).Identity)

	var saved AgentConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/configs/"+created.Id,
		map[string]any{"name": "support", "instructions": "Be brief."}, &saved))
	s.Equal(&AgentIdentity{AgentId: "support", UserId: "support-bot", UserName: "Support"}, saved.Identity,
		"a client that does not know the identity keeps it by saving")
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPatch, "/v1/agents/configs/"+created.Id,
		map[string]any{"instructions": "Be kind."}, nil))
	s.Equal("support-bot", s.read(created.Id).Identity.UserId, "a patch that does not send it keeps it")

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPatch, "/v1/agents/configs/"+created.Id,
		map[string]any{"identity": map[string]any{"agent_id": "help"}}, nil))
	s.Equal(&AgentIdentity{AgentId: "help"}, s.read(created.Id).Identity, "a patch replaces it whole")

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/configs/"+created.Id,
		map[string]any{"name": "support", "identity": map[string]any{"user_name": "Help"}}, nil))
	s.Equal(&AgentIdentity{UserName: "Help"}, s.read(created.Id).Identity, "an update sending it replaces it whole")
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/configs/"+created.Id,
		map[string]any{"name": "support", "identity": map[string]any{}}, nil))
	s.Nil(s.read(created.Id).Identity, "an empty object removes it")
}

func (s *ConfigsSuite) TestAConfigMadeWithoutAnAgentIdentityHasNone() {
	s.Nil(s.read(s.createConfig(map[string]any{"name": "support"}).Id).Identity)
}

// A sync names no identity, and starts from the stored config, so it keeps the one the backend set.
func (s *ConfigsSuite) TestASyncKeepsTheAgentIdentityTheConfigHolds() {
	synced := s.syncFloor("support", "first", map[string]any{"instructions": "Be brief."})
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPatch, "/v1/agents/configs/"+synced.Config.Id,
		map[string]any{"identity": map[string]any{"agent_id": "support"}}, nil))

	again := s.syncFloor("support", "second", map[string]any{"instructions": "Be kind."})

	s.Require().False(again.Unchanged)
	s.Equal(&AgentIdentity{AgentId: "support"}, s.read(synced.Config.Id).Identity)
}

// The router acts as the agent id and the user with its own credentials, so an id Stream keeps
// for itself is refused as it is on a session.
func (s *ConfigsSuite) TestAnAgentIdentityNamingAnIdStreamKeepsIsRefused() {
	created := s.createConfig(map[string]any{"name": "support"})
	for field, identity := range map[string]map[string]any{
		"identity.agent_id": {"agent_id": "!support"},
		"identity.user_id":  {"user_id": " !support-bot"},
	} {
		for _, write := range []struct{ method, path string }{
			{http.MethodPost, "/v1/agents/configs"},
			{http.MethodPut, "/v1/agents/configs/" + created.Id},
			{http.MethodPatch, "/v1/agents/configs/" + created.Id},
		} {
			body := map[string]any{"identity": identity}
			if write.method != http.MethodPatch {
				body["name"] = "support-" + s.utils.uuid()
			}
			status, code, message := s.refusedWith(s.serverClient, write.method, write.path, body)

			s.Equal(http.StatusBadRequest, status, "%s %s", write.method, field)
			s.Equal("invalid_request", code, "%s %s", write.method, field)
			s.True(strings.HasPrefix(message, field+" may not start with !"), "%s: %s", write.method, message)
		}
	}
	s.Nil(s.read(created.Id).Identity)

	var named AgentConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPatch, "/v1/agents/configs/"+created.Id,
		map[string]any{"identity": map[string]any{"user_name": "!Support", "agent_id": "sup!port"}}, &named))
	s.Equal(&AgentIdentity{AgentId: "sup!port", UserName: "!Support"}, named.Identity,
		"only an id starting with ! is Stream's, and a name is not an id")
}

func (s *ConfigsSuite) TestAnAgentIdentityFieldNobodyKnowsIsRefused() {
	created := s.createConfig(map[string]any{"name": "support"})

	status, code, _ := s.refusedWith(s.serverClient, http.MethodPatch, "/v1/agents/configs/"+created.Id,
		map[string]any{"identity": map[string]any{"agentid": "support"}})

	s.Equal(http.StatusBadRequest, status)
	s.Equal("validation_failed", code)
}

// Who the agent is is the backend's to say: a device writing a config with an identity is
// refused, as every config write is.
func (s *ConfigsSuite) TestAnEndUsersDeviceMayNotSetAnAgentIdentity() {
	created := s.createConfig(map[string]any{"name": "support"})
	identity := map[string]any{"identity": map[string]any{"agent_id": "support"}}
	for _, device := range []*testClient{s.data.createUser(), s.data.createGuest()} {
		s.Equal(http.StatusForbidden, device.do(http.MethodPatch, "/v1/agents/configs/"+created.Id, identity, nil), string(device.kind))
		s.Equal(http.StatusForbidden, device.do(http.MethodPut, "/v1/agents/configs/"+created.Id,
			map[string]any{"name": "support", "identity": identity["identity"]}, nil), string(device.kind))
		s.Equal(http.StatusForbidden, device.do(http.MethodPost, "/v1/agents/configs",
			map[string]any{"name": "mine", "identity": identity["identity"]}, nil), string(device.kind))
	}
	s.Nil(s.read(created.Id).Identity)
}

// A browser chat whose every visitor should talk to one named agent names the config, and its
// sessions are written as the agent the config names.
func (s *SessionOwnershipSuite) TestADevicesSessionNamingTheConfigIsTheAgentItNames() {
	name, identity := s.identifiedConfig()
	for _, device := range []*testClient{s.data.createGuest(), s.data.createUser(), s.data.createAnonymous()} {
		written := device.createSession(CreateSessionRequest{Agent: &name})
		spoken := device.createSession(CreateSessionRequest{Agent: &name, StartVoice: pointerTo(true)})

		for kind, opened := range map[string]Session{"text": written, "voice": spoken} {
			s.Equal(identity.AgentId, opened.AgentId, "%s %s", device.kind, kind)
			s.Equal(identity.UserId, opened.UserId, "%s %s", device.kind, kind)
			s.Equal(identity.UserName, s.agentNameOf(opened), "%s %s", device.kind, kind)
		}
	}
}

// The config's identity takes nothing from what keeps one visitor's conversation apart from
// another's: each one's is still theirs alone.
func (s *SessionOwnershipSuite) TestGuestsOfAConfigsAgentReachNothingOfEachOthers() {
	name, identity := s.identifiedConfig()
	alice, bob := s.data.createGuest(), s.data.createGuest()
	alices := alice.createSession(CreateSessionRequest{Agent: &name})
	bobs := bob.createSession(CreateSessionRequest{Agent: &name})
	s.Require().Equal(identity.AgentId, bobs.AgentId)

	s.assertSharedAgentKeepsGuestsApart(alice, alices, bobs)
	bob.stopSession(bobs.Id)
	s.refusesToCarryOn(alice, bobs)
	resume := s.resuming(bobs)
	resume.Agent = &name
	s.Equal(identity.AgentId, bob.createSession(resume).AgentId)
}

// A device naming who the agent is is refused whatever the config says: the config's
// identity is what it gets instead, never a permission to choose one.
func (s *SessionOwnershipSuite) TestADeviceNamingTheAgentsIdentityIsRefusedWhenTheConfigSetsOne() {
	name, _ := s.identifiedConfig()
	guest := s.data.createGuest()
	for asked, refusal := range map[string]APIError{
		"agent_id": errDeviceAgentID, "user_id": errDeviceAgentUser, "user_name": errDeviceAgentUserName,
	} {
		status, code, message := s.refusedWith(guest, http.MethodPost, "/v1/agents/sessions",
			map[string]any{"agent": name, asked: "someone-" + s.utils.uuid()})

		s.Equal(http.StatusForbidden, status, asked)
		s.Equal(refusal.Code, code, asked)
		s.Equal(refusal.Message, message, asked)
	}
}

// The config decides only what the request leaves to it: a backend's own identity wins, and so
// does a signed-in device's own user id. The defaults a generated client sends name nobody.
func (s *SessionOwnershipSuite) TestWhatASessionRequestNamesWinsOverTheConfigsIdentity() {
	name, identity := s.identifiedConfig()

	own := s.serverClient.createSession(CreateSessionRequest{Agent: &name, AgentId: pointerTo("own-agent"),
		UserId: pointerTo("own-bot"), UserName: pointerTo("Own")})
	s.Equal("own-agent", own.AgentId)
	s.Equal("own-bot", own.UserId)
	s.Equal("Own", s.agentNameOf(own))

	alice := s.data.createUser()
	s.Equal(alice.userID, alice.createSession(CreateSessionRequest{Agent: &name, UserId: &alice.userID}).UserId)

	defaulted := s.data.createGuest().createSession(CreateSessionRequest{Agent: &name,
		UserId: pointerTo(session.DefaultUserID), UserName: pointerTo(session.DefaultUserName), AgentId: pointerTo("")})
	s.Equal(identity.AgentId, defaulted.AgentId)
	s.Equal(identity.UserId, defaulted.UserId)
	s.Equal(identity.UserName, s.agentNameOf(defaulted))
}

// A conversation is kept under the agent id it was opened with, and opening it refuses any
// other, so one begun before its config named an agent carries on as the agent it was.
func (s *SessionOwnershipSuite) TestAConversationBegunBeforeTheConfigNamedItsAgentCarriesOnAsItWas() {
	name := "support-" + s.utils.uuid()
	var config AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		map[string]any{"name": name}, &config))
	guest := s.data.createGuest()
	before := guest.createSession(CreateSessionRequest{Agent: &name})
	guest.stopSession(before.Id)
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPatch, "/v1/agents/configs/"+config.Id,
		map[string]any{"identity": map[string]any{"agent_id": "support-" + s.utils.uuid(), "user_id": "support-bot"}}, nil))

	resume := s.resuming(before)
	resume.Agent = &name
	resumed := guest.createSession(resume)

	s.Equal(*before.ConversationId, value(resumed.ConversationId))
	s.Equal(before.AgentId, resumed.AgentId)
	s.Equal("support-bot", resumed.UserId, "the user the agent writes as is the config's")
}

// Defect (R1.1): a session carried on by its id after it ended, or forked from its row, is
// rebuilt from the config, whose identity it lost: it wrote as the default user. Each takes the
// config's user and name; the agent id stays the conversation's, and a fork's is its own.
func (s *SessionOwnershipSuite) TestASessionCarriedOnOrForkedWritesAsTheConfigsUser() {
	name, identity := s.identifiedConfig()
	guest := s.data.createGuest()
	opened := guest.createSession(CreateSessionRequest{Agent: &name})
	var live Session
	s.Require().Equal(http.StatusCreated, guest.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/fork", map[string]any{}, &live))
	guest.stopSession(opened.Id)
	var stored Session
	s.Require().Equal(http.StatusCreated, guest.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/fork", map[string]any{}, &stored))
	var resumed Session
	s.Require().Equal(http.StatusOK, guest.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/resume", map[string]any{}, &resumed))

	s.Equal(identity.AgentId, resumed.AgentId, "resumed")
	for kind, carried := range map[string]Session{"resumed": resumed, "a live parent's fork": live, "a stored parent's fork": stored} {
		s.Equal(identity.UserId, carried.UserId, kind)
		s.Equal(identity.UserName, s.agentNameOf(carried), kind)
	}
	for kind, forked := range map[string]Session{"a live parent's fork": live, "a stored parent's fork": stored} {
		s.Regexp(routerMadeAgentID, forked.AgentId, kind)
	}
}

// Defect (R1.1): a fork naming another config runs on it, and so writes as that config's user.
func (s *SessionOwnershipSuite) TestAForkNamingAConfigWritesAsItsUser() {
	plain := "plain-" + s.utils.uuid()
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", map[string]any{"name": plain}, nil))
	name, identity := s.identifiedConfig()
	guest := s.data.createGuest()
	opened := guest.createSession(CreateSessionRequest{Agent: &plain})

	var forked Session
	s.Require().Equal(http.StatusCreated, guest.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/fork",
		ForkSessionRequest{Agent: &name}, &forked))

	s.Equal(identity.UserId, forked.UserId)
	s.Equal(identity.UserName, s.agentNameOf(forked))
}

// Control: what a session of a config that names no agent is, as on aa4c2d4d9. A device's
// session is a nameless agent of the router's own making, fresh each time, writing as the
// default user; a backend's voice session is keyed under its call.
func (s *SessionOwnershipSuite) TestAConfigNamingNoAgentLeavesTheAgentToTheRouter() {
	name := "support-" + s.utils.uuid()
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		map[string]any{"name": name}, nil))
	guest := s.data.createGuest()

	first := guest.createSession(CreateSessionRequest{Agent: &name})
	second := guest.createSession(CreateSessionRequest{Agent: &name})
	spoken := guest.createSession(CreateSessionRequest{Agent: &name, StartVoice: pointerTo(true)})
	backend := s.serverClient.createSession(CreateSessionRequest{Agent: &name, StartVoice: pointerTo(true)})

	for _, opened := range []Session{first, second, spoken} {
		s.Regexp(routerMadeAgentID, opened.AgentId)
		s.Equal(session.DefaultUserID, opened.UserId)
		s.Equal(session.DefaultUserName, s.agentNameOf(opened))
	}
	s.NotEqual(first.AgentId, second.AgentId)
	s.NotEqual(spoken.Id, spoken.AgentId)
	s.Equal(backend.Id, backend.AgentId)
}

// agentNameOf is the display name of the user a running session's agent joins and writes as,
// which no answer of the API shows.
func (s *SessionOwnershipSuite) agentNameOf(opened Session) string {
	held, ok := s.manager.Get(opened.Id, session.Owner{CustomerID: s.customerID(), Kind: auth.KindServer})
	s.Require().True(ok, "session %s is held here", opened.Id)
	return held.Spec().UserName
}

// identifiedConfig is a config of the app's that names its agent, and the identity it names.
func (s *SessionOwnershipSuite) identifiedConfig() (string, AgentIdentity) {
	identity := AgentIdentity{AgentId: "support-" + s.utils.uuid(), UserId: "support-bot-" + s.utils.uuid(), UserName: "Support"}
	name := "support-" + s.utils.uuid()
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		map[string]any{"name": name, "identity": identity}, nil))
	return name, identity
}

// The socket's start frame is opened as createSession opens a session, config and all.
func (s *SocketSessionSuite) TestASocketSessionNamingTheConfigIsTheAgentItNames() {
	identity := AgentIdentity{AgentId: "support-" + s.utils.uuid(), UserId: "support-bot-" + s.utils.uuid()}
	name := "support-" + s.utils.uuid()
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		map[string]any{"name": name, "identity": identity}, nil))
	for _, device := range []*testClient{s.client, s.guestClient, s.anonymousClient} {
		answered := s.socketStartedWith(device, map[string]any{"agent": name})

		s.Equal(identity.AgentId, answered["agent_id"], string(device.kind))
		s.Equal(identity.UserId, answered["user_id"], string(device.kind))
	}
}
