//go:build integration

package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// ProjectTagSuite checks which project a session's usage is tagged with: the one a backend
// named for it, never the one a device did.
type ProjectTagSuite struct {
	RouterSuite
}

func TestProjectTagSuite(t *testing.T) {
	runSuite(t, new(ProjectTagSuite))
}

func (s *ProjectTagSuite) SetupSuite() {
	s.llmInstances = []routing.ProviderConfig{{Provider: "fanning", Model: "fanning-model", Languages: []string{"la"}}}
	s.llmFactories = func(registry *llmrouter.Registry) {
		// Asks one child, then recites what came back.
		registry.Register("fanning", func(routing.Spec) (llmrouter.Provider, error) {
			return &scriptedLLM{recites: true, calls: []llm.ToolCall{
				{ID: store.NewID(), Name: "ask_a", Arguments: `{"input":"review this"}`}}}, nil
		})
	}
	s.RouterSuite.SetupSuite()
}

func (s *ProjectTagSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *ProjectTagSuite) TestADevicesProjectTagsNoUsage() {
	for _, device := range s.devices() {
		s.Run(string(device.kind), func() {
			opened := s.open(device, "alpha")

			s.Equal([]string{""}, s.answered(device, opened.Id))
		})
	}
}

func (s *ProjectTagSuite) TestADevicesProjectStillGroupsItsSession() {
	for _, device := range s.devices() {
		s.Run(string(device.kind), func() {
			project := s.utils.uuid()

			opened := s.open(device, project)

			s.Equal(project, value(opened.ProjectId))
			s.Equal(project, value(device.getSession(opened.Id).ProjectId))
			s.Require().Eventually(func() bool { return s.stored(opened.Id) }, settleFor, 20*time.Millisecond)
			s.Equal([]string{opened.Id}, ids(device.querySessions(inProject(project)).Items))
		})
	}
}

func (s *ProjectTagSuite) TestADevicesProjectIsAsShortAsATag() {
	request := textSession(nil)
	request.ProjectId = pointerTo(strings.Repeat("a", 4000))

	status, message := s.client.failure(http.MethodPost, "/v1/agents/sessions", request)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(message, `"project" is longer than`)
}

func (s *ProjectTagSuite) TestABackendsProjectTagsItsUsage() {
	opened := s.open(s.serverClient, "alpha")

	s.Equal([]string{"alpha"}, s.answered(s.serverClient, opened.Id))
}

func (s *ProjectTagSuite) TestTheProjectABackendNamesForAUserTagsWhatTheUserAsks() {
	owner := s.data.createUser()
	opened := s.open(s.serverClient.actingFor(owner), "alpha")

	s.Equal([]string{"alpha"}, s.answered(owner, opened.Id))
}

func (s *ProjectTagSuite) TestAnAgentsProjectTagStandsOverADevicesProject() {
	config := s.taggedConfig(map[string]string{"project": "support"})
	owner := s.data.createUser()
	request := textSession(nil)
	request.ConfigId, request.ProjectId = &config, pointerTo("alpha")
	opened := owner.createSession(request)

	s.Equal([]string{"support"}, s.answered(owner, opened.Id))
}

func (s *ProjectTagSuite) TestASocketDevicesProjectTagsNoUsage() {
	for _, device := range s.devices() {
		s.Run(string(device.kind), func() {
			s.Equal([]string{""}, s.spokenOnASocket(device))
		})
	}
}

func (s *ProjectTagSuite) TestASocketBackendsProjectTagsItsUsage() {
	s.Equal([]string{"alpha"}, s.spokenOnASocket(s.serverClient))
}

func (s *ProjectTagSuite) TestADevicesForkKeepsItsBackendsTag() {
	for name, end := range s.parents() {
		s.Run(name, func() {
			owner := s.data.createUser()
			parent := s.open(s.serverClient.actingFor(owner), "alpha")
			end(parent.Id)

			forked := s.fork(owner, parent.Id, ForkSessionRequest{})

			s.Equal("alpha", value(forked.ProjectId))
			s.Equal([]string{"alpha"}, s.answered(owner, forked.Id))
			s.Equal([]string{"alpha"}, s.reopened(owner, forked.Id), "its own row says the tag")
		})
	}
}

func (s *ProjectTagSuite) TestADevicesForkNamingAProjectKeepsItsBackendsTag() {
	for name, end := range s.parents() {
		s.Run(name, func() {
			owner := s.data.createUser()
			parent := s.open(s.serverClient.actingFor(owner), "alpha")
			end(parent.Id)

			forked := s.fork(owner, parent.Id, ForkSessionRequest{ProjectId: pointerTo("beta")})

			s.Equal("beta", value(forked.ProjectId))
			s.Equal([]string{"alpha"}, s.answered(owner, forked.Id))
			s.Equal([]string{"alpha"}, s.reopened(owner, forked.Id))
			s.Equal([]string{"alpha"}, s.reopenedElsewhere(owner, forked.Id))
			again := s.fork(owner, forked.Id, ForkSessionRequest{ProjectId: pointerTo("")})
			s.Equal([]string{"alpha"}, s.reopened(owner, again.Id), "nor does a device's fork naming none drop it")
		})
	}
}

// Every fork: who opened the parent, who forks it, the project named, if any, and the tag.
func (s *ProjectTagSuite) TestAForkIsTaggedWithTheProjectABackendNamedLast() {
	for state, end := range s.parents() {
		for _, tried := range []struct{ opener, forker, project, want string }{
			{"backend", "backend", "-", "alpha"}, {"backend", "backend", "beta", "beta"}, {"backend", "backend", "", ""},
			{"backend", "device", "-", "alpha"}, {"backend", "device", "beta", "alpha"}, {"backend", "device", "", "alpha"},
			{"device", "backend", "-", ""}, {"device", "backend", "beta", "beta"}, {"device", "backend", "", ""},
			{"device", "device", "-", ""}, {"device", "device", "beta", ""}, {"device", "device", "", ""},
		} {
			s.Run(strings.Join([]string{state, tried.opener, tried.forker, tried.project}, " "), func() {
				owner := s.data.createUser()
				as := map[string]*testClient{"device": owner, "backend": s.serverClient.actingFor(owner)}
				parent := s.open(as[tried.opener], "alpha")
				end(parent.Id)
				var request ForkSessionRequest
				if tried.project != "-" {
					request.ProjectId = &tried.project
				}

				forked := s.fork(as[tried.forker], parent.Id, request)

				s.Equal([]string{tried.want}, s.answered(as[tried.forker], forked.Id))
				s.Equal([]string{tried.want}, s.reopened(owner, forked.Id))
				s.Equal([]string{tried.want}, s.reopenedElsewhere(owner, forked.Id))
			})
		}
	}
}

// A backend naming a project on a fork of its running session leaves the session its own tag,
// and an agent's project tag stands over both.
func (s *ProjectTagSuite) TestABackendsForkNamingAProjectLeavesItsParentsTags() {
	running := s.open(s.serverClient, "alpha")
	config := s.taggedConfig(map[string]string{"project": "support"})
	request := textSession(nil)
	request.ConfigId, request.ProjectId = &config, pointerTo("alpha")
	supported := s.serverClient.createSession(request)

	renamed := s.fork(s.serverClient, running.Id, ForkSessionRequest{ProjectId: pointerTo("beta")})
	ofSupported := s.fork(s.serverClient, supported.Id, ForkSessionRequest{ProjectId: pointerTo("beta")})

	s.Equal([]string{"beta"}, s.answered(s.serverClient, renamed.Id))
	s.Equal([]string{"alpha"}, s.answered(s.serverClient, running.Id))
	s.Equal([]string{"support"}, s.answered(s.serverClient, ofSupported.Id))
}

func (s *ProjectTagSuite) TestALongProjectOnAForkIsRefused() {
	for name, end := range s.parents() {
		s.Run(name, func() {
			owner := s.data.createUser()
			parent := s.open(s.serverClient.actingFor(owner), "alpha")
			end(parent.Id)

			status, message := owner.failure(http.MethodPost, "/v1/agents/sessions/"+parent.Id+"/fork",
				ForkSessionRequest{ProjectId: pointerTo(strings.Repeat("a", 8000)), Messages: pointerTo(false)})

			s.Equal(http.StatusBadRequest, status)
			s.Contains(message, `"project" is longer than`)
		})
	}
}

func (s *ProjectTagSuite) TestABackendsForkOfADevicesSessionGainsNoTag() {
	for name, end := range s.parents() {
		s.Run(name, func() {
			owner := s.data.createUser()
			parent := s.open(owner, "alpha")
			end(parent.Id)
			backend := s.serverClient.actingFor(owner)

			forked := s.fork(backend, parent.Id, ForkSessionRequest{})

			s.Equal("alpha", value(forked.ProjectId))
			s.Equal([]string{""}, s.answered(backend, forked.Id))
			s.Equal([]string{""}, s.reopened(backend, forked.Id))
		})
	}
}

// The running parent's tags stay its own: naming a project on its fork does not tag it.
func (s *ProjectTagSuite) TestABackendsForkNamingAProjectTagsTheForkAlone() {
	config := s.taggedConfig(map[string]string{"environment": "dev"})
	owner := s.data.createUser()
	request := textSession(nil)
	request.ConfigId, request.ProjectId = &config, pointerTo("alpha")
	parent := owner.createSession(request)
	backend := s.serverClient.actingFor(owner)

	forked := s.fork(backend, parent.Id, ForkSessionRequest{ProjectId: pointerTo("beta")})

	s.Equal([]string{"beta"}, s.answered(backend, forked.Id))
	s.Equal([]string{"beta"}, s.reopened(backend, forked.Id))
	s.Equal([]string{""}, s.answered(owner, parent.Id))
}

func (s *ProjectTagSuite) TestABackendsForkNamingAProjectOfAnEndedDevicesSessionIsTagged() {
	owner := s.data.createUser()
	parent := s.open(owner, "alpha")
	s.stop(parent.Id)
	backend := s.serverClient.actingFor(owner)

	forked := s.fork(backend, parent.Id, ForkSessionRequest{ProjectId: pointerTo("beta")})

	s.Equal([]string{"beta"}, s.answered(backend, forked.Id))
}

func (s *ProjectTagSuite) TestAForkOntoAnotherAgentKeepsWhoNamedTheProject() {
	config := s.taggedConfig(map[string]string{"environment": "dev"})
	owner := s.data.createUser()
	backend := s.serverClient.actingFor(owner)
	named, devices := s.open(backend, "alpha"), s.open(owner, "alpha")

	ofNamed := s.fork(owner, named.Id, ForkSessionRequest{ConfigId: &config})
	ofDevices := s.fork(backend, devices.Id, ForkSessionRequest{ConfigId: &config})

	s.Equal("alpha", value(ofNamed.ProjectId))
	s.Equal([]string{"alpha"}, s.answered(owner, ofNamed.Id))
	s.Equal([]string{""}, s.answered(backend, ofDevices.Id))
}

func (s *ProjectTagSuite) TestACarriedOnSessionIsTaggedAsItWasOpened() {
	for name, carry := range map[string]func(*testClient, string) []string{
		"ended here": func(client *testClient, id string) []string {
			s.endHere(id)
			return s.answered(client, id)
		},
		"from its row":      s.reopened,
		"on another router": s.reopenedElsewhere,
	} {
		s.Run(name, func() {
			owner := s.data.createUser()
			backend := s.serverClient.actingFor(owner)
			backends, devices := s.open(backend, "alpha"), s.open(owner, "alpha")

			s.Equal([]string{"alpha"}, carry(owner, backends.Id), "a device carries on its backend's session")
			s.Equal([]string{"alpha"}, carry(backend, backends.Id))
			s.Equal([]string{""}, carry(backend, devices.Id), "a backend carries on its user's session")
			s.Equal([]string{""}, carry(owner, devices.Id))
		})
	}
}

func (s *ProjectTagSuite) TestARowSaysItsProjectTag() {
	for want, opened := range map[string]Session{
		"alpha:alpha": s.open(s.serverClient, "alpha"), ":alpha": s.open(s.client, "alpha"),
		":": s.serverClient.createSession(textSession(nil)),
	} {
		s.Require().Eventually(func() bool { return s.stored(opened.Id) }, settleFor, 20*time.Millisecond)
		var got string
		s.Require().NoError(s.store.ShardDB().QueryRowContext(context.Background(),
			"SELECT project_tag || ':' || project FROM agent_sessions WHERE id = ?", opened.Id).Scan(&got))
		s.Equal(want, got)
	}
}

// A row from before the project tag was kept: the kind of caller that opened it says.
func (s *ProjectTagSuite) TestARowFromBeforeIsTaggedByWhoOpenedIt() {
	owner := s.data.createUser()
	backend := s.serverClient.actingFor(owner)
	backends, devices := s.open(backend, "alpha"), s.open(owner, "alpha")
	s.fromBefore(backends.Id)
	s.fromBefore(devices.Id)

	s.Equal([]string{"alpha"}, s.reopened(owner, backends.Id))
	s.Equal([]string{""}, s.reopened(backend, devices.Id))
	s.stop(backends.Id)
	s.Equal([]string{"alpha"}, s.answered(owner, s.fork(owner, backends.Id, ForkSessionRequest{}).Id))
}

func (s *ProjectTagSuite) TestAForksRowFromBeforeIsNotTagged() {
	owner := s.data.createUser()
	backend := s.serverClient.actingFor(owner)
	forked := s.fork(backend, s.open(owner, "alpha").Id, ForkSessionRequest{})
	s.fromBefore(forked.Id)

	s.Equal([]string{""}, s.reopened(backend, forked.Id))
}

// A guest's claim moves the session to the account, which is a device too.
func (s *ProjectTagSuite) TestAClaimedGuestsSessionStaysUntagged() {
	owner := s.data.createUser()
	opened := s.open(s.guestClient, "alpha")
	s.stop(opened.Id)
	_, err := s.store.Users.ClaimGuest(context.Background(), s.customerID(), s.guestClient.userID, owner.userID)
	s.Require().NoError(err)

	s.Equal([]string{""}, s.answered(owner, s.fork(owner, opened.Id, ForkSessionRequest{}).Id))
	s.fromBefore(opened.Id)
	s.Equal([]string{""}, s.answered(owner, s.fork(owner, opened.Id, ForkSessionRequest{}).Id))
}

func (s *ProjectTagSuite) TestAChildAgentIsTaggedAsItsParentIs() {
	var child, parent AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", map[string]any{
		"name": "child-" + s.utils.uuid(), "mode": "text", "llm": "echo/echo-model"}, &child))
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", map[string]any{
		"name": "lead-" + s.utils.uuid(), "mode": "text", "llm": "fanning/fanning-model",
		"agents": []ChildAgent{{Config: child.Name, Tool: "ask_a", Description: pointerTo("reviews")}}}, &parent))
	owner := s.data.createUser()
	for want, opener := range map[string]*testClient{"": owner, "alpha": s.serverClient.actingFor(owner)} {
		since := time.Now()
		opened := opener.createSession(CreateSessionRequest{ConfigId: &parent.Id, ProjectId: pointerTo("alpha")})

		respondIn(&s.RouterSuite, opener, opened.Id, "hello")

		s.Equal([]string{want}, s.tags("model = 'echo-model' AND purpose = 'reply' AND customer_id = ? AND started_at >= ?",
			s.customerID(), since), "the child's own request")
	}
}

// devices are a client of every kind of end user's device.
func (s *ProjectTagSuite) devices() []*testClient {
	return []*testClient{s.client, s.guestClient, s.anonymousClient}
}

// parents are the states a session is forked in, by name, each with what brings one there.
func (s *ProjectTagSuite) parents() map[string]func(id string) {
	return map[string]func(id string){"running": func(string) {}, "ended": s.stop}
}

func (s *ProjectTagSuite) open(client *testClient, project string) Session {
	request := textSession(nil)
	request.ProjectId = &project
	return client.createSession(request)
}

func (s *ProjectTagSuite) fork(client *testClient, id string, request ForkSessionRequest) Session {
	request.Messages = pointerTo(false)
	var forked Session
	s.Require().Equal(http.StatusCreated, client.do(http.MethodPost, "/v1/agents/sessions/"+id+"/fork", request, &forked))
	return forked
}

// answered asks a session a question and returns the project its usage since is tagged with,
// empty for none.
func (s *ProjectTagSuite) answered(client *testClient, id string) []string {
	since := time.Now()
	respondIn(&s.RouterSuite, client, id, "hello")
	return s.tags("session_id = ? AND modality = 'llm' AND started_at >= ?", id, since)
}

// reopened is answered for a session carried on from its row.
func (s *ProjectTagSuite) reopened(client *testClient, id string) []string {
	s.stop(id)
	return s.answered(client, id)
}

// reopenedElsewhere is reopened on a router that never held the session.
func (s *ProjectTagSuite) reopenedElsewhere(client *testClient, id string) []string {
	s.stop(id)
	other, server := s.otherRouter(nil)
	tags := s.answered(client.on(other), id)
	s.closeOn(server.sessions, id)
	return tags
}

// spokenOnASocket has client open a socket session in a project, whose greeting is spoken, and
// returns the project that usage is tagged with, empty for none.
func (s *ProjectTagSuite) spokenOnASocket(client *testClient) []string {
	config := s.greetingConfig()
	connection := client.opens("/v1/agents/socket")
	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "start", "sample_rate": 24_000,
		"session": map[string]any{"config_id": config, "project_id": "alpha"}}))
	frame := s.await(connection, "session")
	id, _ := frame["session"].(map[string]any)["id"].(string)
	s.Require().NotEmpty(id)
	return s.tags("session_id = ? AND modality = 'tts'", id)
}

// tags waits for the usage rows query reads and returns the projects they are tagged with,
// each once, empty for none.
func (s *ProjectTagSuite) tags(query string, args ...any) []string {
	var projects []string
	s.Require().Eventually(func() bool {
		err := s.store.ShardDB().NewSelect().Model((*store.Request)(nil)).
			ColumnExpr("DISTINCT COALESCE(tags->>'project', '')").Where(query, args...).
			Scan(context.Background(), &projects)
		return err == nil && len(projects) > 0
	}, 10*time.Second, 50*time.Millisecond, "nothing was recorded")
	return projects
}

// stop ends a session, which this router then no longer holds, once its row is written.
func (s *ProjectTagSuite) stop(id string) {
	s.Require().Eventually(func() bool { return s.stored(id) }, settleFor, 20*time.Millisecond)
	s.closeOn(s.manager, id)
}

// closeOn ends a session sessions may hold, and writes what it recorded.
func (s *ProjectTagSuite) closeOn(sessions *session.Manager, id string) {
	_, _ = sessions.Close(id, session.Owner{CustomerID: s.customerID(), Kind: auth.KindServer})
	s.Require().NoError(sessions.FlushRecords(context.Background()))
}

func (s *ProjectTagSuite) stored(id string) bool {
	_, err := s.store.Sessions.Get(context.Background(), s.customerID(), id)
	return err == nil
}

// fromBefore makes a session's row one written before it kept its project tag.
func (s *ProjectTagSuite) fromBefore(id string) {
	s.stop(id)
	_, err := s.store.ShardDB().ExecContext(context.Background(),
		"UPDATE agent_sessions SET project_tag = NULL WHERE id = ?", id)
	s.Require().NoError(err)
}

func (s *ProjectTagSuite) taggedConfig(tags map[string]string) string {
	var created AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", AgentConfigRequest{
		Name: "agent-" + s.utils.uuid(), Llm: pointerTo("en-low-latency"), Tags: &tags,
	}, &created))
	return created.Id
}

func (s *ProjectTagSuite) greetingConfig() string {
	var created AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", map[string]any{
		"name": "agent-" + s.utils.uuid(), "llm": "en-low-latency", "greeting": map[string]any{"text": "welcome"}}, &created))
	return created.Id
}
