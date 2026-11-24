//go:build integration

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/dispatch"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// shareLocation is a tool a device runs.
const shareLocation = "share_location"

func (s *SessionToolsSuite) TestADevicesNewSessionDoesNotDeclareAToolItsAgentsWorkerHosts() {
	config := s.ordersAgent()
	s.hostsLookupOrder(config.Name)

	for _, device := range []*testClient{s.client, s.guestClient, s.anonymousClient} {
		s.Run(string(device.kind), func() {
			s.assertWorkersTool(device, map[string]any{"config_id": config.ID, "tools": s.toolsNamed(shareLocation, lookupOrder)})
		})
	}
}

func (s *SessionToolsSuite) TestADevicesNewSessionDoesNotDeclareAToolHostedForEveryAgent() {
	config := s.ordersAgent()
	s.hostsLookupOrder(dispatch.EveryAgent)

	s.assertWorkersTool(s.client, map[string]any{"config_id": config.ID, "tools": s.toolsNamed(lookupOrder)})
}

func (s *SessionToolsSuite) TestARefusedSessionOpensNoConversation() {
	config := s.ordersAgent()
	s.hostsLookupOrder(config.Name)
	id := "session-" + s.utils.uuid()

	s.Require().Equal(http.StatusConflict, s.client.do(http.MethodPost, "/v1/agents/sessions",
		CreateSessionRequest{Id: &id, ConfigId: &config.ID, Tools: s.toolsNamed(lookupOrder)}, nil))

	_, kept := s.chat.Channel(id)
	s.False(kept, "a refused session keeps no conversation")
	opened := s.client.createSession(CreateSessionRequest{Id: &id, ConfigId: &config.ID})
	s.Equal(id, opened.Id, "the id is still free")
	_, kept = s.chat.Channel(id)
	s.True(kept, "the session's conversation is kept under its id")
}

// A conversation carried on by its id says the agent it is kept under only once it is open.
func (s *SessionToolsSuite) TestADevicesNewSessionOnAConversationDoesNotDeclareAToolItsAgentsWorkerHosts() {
	device := s.data.createUser()
	agent := "agent-" + s.utils.uuid()
	request := textSession(nil)
	request.AgentId = &agent
	opened := s.serverClient.actingFor(device).createSession(request)
	device.stopSession(opened.Id)
	s.hostsLookupOrder(agent)

	carried := CreateSessionRequest{LegacyConversationId: opened.ConversationId, Tools: s.toolsNamed(lookupOrder)}
	s.Equal(http.StatusConflict, device.do(http.MethodPost, "/v1/agents/sessions", carried, nil))

	carried.Tools = s.toolsNamed(shareLocation)
	sources := s.toolSourcesAs(s.serverClient.actingFor(device), device.createSession(carried).Id)
	s.Equal("session", sources[shareLocation])
	s.Equal("worker", sources[lookupOrder])
}

func (s *SessionToolsSuite) TestADevicesOwnToolsAreOfferedBesideItsWorkers() {
	config := s.ordersAgent()
	s.hostsLookupOrder(config.Name)

	opened := s.client.createSession(CreateSessionRequest{ConfigId: &config.ID, Tools: s.toolsNamed(shareLocation)})

	sources := s.toolSourcesAs(s.serverClient.actingFor(s.client), opened.Id)
	s.Equal("session", sources[shareLocation])
	s.Equal("worker", sources[lookupOrder])
}

func (s *SessionToolsSuite) TestADeviceDeclaresAToolNoWorkerHosts() {
	config := s.ordersAgent()

	opened := s.client.createSession(CreateSessionRequest{ConfigId: &config.ID, Tools: s.toolsNamed(lookupOrder)})
	s.hostsLookupOrder(config.Name)

	s.Equal("session", s.toolSourcesAs(s.serverClient.actingFor(s.client), opened.Id)[lookupOrder],
		"a worker that connects later leaves the session's tools as they are")
}

func (s *SessionToolsSuite) TestADeviceDeclaresAToolItsWorkerHostsForAnotherAgent() {
	config := s.ordersAgent()
	s.hostsLookupOrder("agent-" + s.utils.uuid())

	opened := s.client.createSession(CreateSessionRequest{ConfigId: &config.ID, Tools: s.toolsNamed(lookupOrder)})

	s.Equal("session", s.toolSourcesAs(s.serverClient.actingFor(s.client), opened.Id)[lookupOrder])
}

func (s *SessionToolsSuite) TestABackendsSessionKeepsItsOwnToolOverItsWorkers() {
	config := s.ordersAgent()
	s.hostsLookupOrder(config.Name)

	opened := s.serverClient.createSession(CreateSessionRequest{ConfigId: &config.ID, Tools: s.toolsNamed(lookupOrder)})

	s.Equal("session", s.toolSources(opened.Id)[lookupOrder])
}

func (s *SessionToolsSuite) TestADevicesConversationIsCarriedOnWithTheToolAWorkerHostsNow() {
	config := s.ordersAgent()
	device := s.data.createUser()
	opened := device.createSession(CreateSessionRequest{ConfigId: &config.ID, Tools: s.toolsNamed(shareLocation, lookupOrder)})
	s.hostsLookupOrder(config.Name)

	s.endHere(opened.Id)
	s.Require().Equal(http.StatusOK,
		device.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/resume", ResumeSessionRequest{}, nil))
	s.assertTakenOver(device, opened.Id)

	s.endHere(opened.Id)
	s.Require().Equal(http.StatusOK, device.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/resume",
		ResumeSessionRequest{Tools: s.toolsNamed(shareLocation, lookupOrder)}, nil))
	s.assertTakenOver(device, opened.Id)
}

func (s *SessionToolsSuite) TestADevicesForkHasTheToolAWorkerHostsNow() {
	config := s.ordersAgent()
	device := s.data.createUser()
	opened := device.createSession(CreateSessionRequest{ConfigId: &config.ID, Tools: s.toolsNamed(shareLocation, lookupOrder)})
	s.hostsLookupOrder(config.Name)

	var forked Session
	s.Require().Equal(http.StatusCreated, device.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/fork",
		ForkSessionRequest{Messages: pointerTo(false)}, &forked))

	s.assertTakenOver(device, forked.Id)
	parent, held := s.manager.Get(opened.Id, session.Owner{CustomerID: s.customerID(), Kind: auth.KindServer})
	s.Require().True(held)
	var declared []string
	for _, tool := range parent.Spec().Tools {
		declared = append(declared, tool.Name)
	}
	s.Equal([]string{shareLocation, lookupOrder}, declared, "the running parent keeps its tools")
}

// A worker's call in a conversation carried on is the worker's to run, and is not shown as the
// tool the device declared.
func (s *SessionToolsSuite) TestACallInADevicesConversationCarriedOnIsTheWorkersToRun() {
	config := s.ordersAgent()
	device := s.data.createUser()
	tools := s.toolsNamed(lookupOrder)
	(*tools)[0].DisplayTitle, (*tools)[0].Executor = pointerTo("The device's title"), pointerTo(SessionToolExecutorClient)
	opened := device.createSession(CreateSessionRequest{ConfigId: &config.ID, Tools: tools})
	worker := s.hostsLookupOrder(config.Name)
	s.endHere(opened.Id)
	s.Require().Equal(http.StatusOK, device.do(http.MethodPost, "/v1/agents/sessions/"+opened.Id+"/resume", ResumeSessionRequest{}, nil))

	events := device.opens("/v1/agents/sessions/" + opened.Id + "/events")

	status, body := s.serverClient.actingFor(device).call(http.MethodPost,
		"/v1/agents/sessions/"+opened.Id+"/respond", RespondRequest{Text: "where is my order", RequestId: pointerTo("request-" + s.utils.uuid())})
	s.Require().Equal(http.StatusOK, status, string(body))
	s.hostedCallUser(worker)

	s.Require().NoError(events.SetReadDeadline(time.Now().Add(settleFor)))
	for {
		var frame map[string]any
		s.Require().NoError(events.ReadJSON(&frame))
		message, _ := frame["message"].(map[string]any)
		parts, _ := message["parts"].([]any)
		for _, part := range parts {
			if step, _ := part.(map[string]any); step["name"] == lookupOrder {
				s.Equal("server", step["executor"])
				s.NotEqual("The device's title", step["display_title"])
				return
			}
		}
	}
}

// assertTakenOver checks the session has the worker's lookup_order and the device's other
// tool, and that the device can still give it more.
func (s *SessionToolsSuite) assertTakenOver(device *testClient, id string) {
	sources := s.toolSourcesAs(s.serverClient.actingFor(device), id)
	s.Equal("session", sources[shareLocation])
	s.Equal("worker", sources[lookupOrder])
	s.Equal(http.StatusConflict, s.addTools(device, id, s.lookupOrder()))
	s.Equal(http.StatusNoContent, s.addTools(device, id, SessionTool{Name: "open_camera", Description: "open the camera"}))
	s.Equal(http.StatusNoContent, device.do(http.MethodDelete, "/v1/agents/sessions/"+id+"/tools/open_camera", nil, nil))
}

// assertWorkersTool checks createSession and the session socket refuse the device's session
// with the same answer, naming lookup_order alone.
func (s *SessionToolsSuite) assertWorkersTool(device *testClient, sent map[string]any) {
	const message = "session: the session offers a tool by that name that is not its caller's: lookup_order"
	status, body := device.call(http.MethodPost, "/v1/agents/sessions", sent)
	s.Require().Equal(http.StatusConflict, status, string(body))
	var created ErrorResponse
	s.Require().NoError(json.Unmarshal(body, &created))
	s.Equal(message, created.Error.Message)
	s.Equal("conflict", created.Error.Code)

	connection := device.opens("/v1/agents/socket")
	s.Require().NoError(connection.WriteJSON(map[string]any{"type": "start", "sample_rate": 24_000, "session": sent}))
	refused := s.await(connection, "error")
	s.Equal(message, refused["error"])
	_, _, err := connection.ReadMessage()
	s.True(websocket.IsCloseError(err, websocket.ClosePolicyViolation), "the socket closes: %v", err)
}

// ordersAgent is an agent config whose tool groups name a device's tools and a worker's.
func (s *SessionToolsSuite) ordersAgent() store.AgentConfig {
	config := store.AgentConfig{CustomerID: s.customerID(), Name: "orders-" + s.utils.uuid(),
		Mode: store.AgentModeText, Models: store.AgentModels{LLM: "tooling/tool-model"},
		VisibleTools: []string{lookupOrder},
		ToolGroups:   []store.ToolGroup{{Name: "orders", Tools: []string{lookupOrder, shareLocation, "open_camera"}}}}
	s.Require().NoError(s.store.AgentConfigs.Insert(context.Background(), &config))
	return config
}

// toolsNamed are the tools a caller declares by name.
func (s *SessionToolsSuite) toolsNamed(names ...string) *[]SessionTool {
	var tools []SessionTool
	for _, name := range names {
		tools = append(tools, SessionTool{Name: name, Description: "the caller's " + name})
	}
	return &tools
}
