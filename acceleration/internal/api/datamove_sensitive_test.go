//go:build integration

package api

import (
	"encoding/json"
	"maps"
	"net/http"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// An import holds the agent configs it writes to the sensitive-connector rule a config write
// holds them to (store.SetConfigRule): a row that breaks it, alone or with the agents it works
// with, is refused by name and not written.

func (s *DataMoveSuite) TestAnImportedAgentBindingASensitiveConnectorBesideAnOutboundOneIsRefused() {
	template := s.agentTemplate()
	s.useInternalApp()
	line, id := s.importedAgent(template, map[string]any{
		"connectors": []map[string]any{sessionNessy("customers"), sessionGitHubGranting("code", "create_issue")}})

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", ndjson([]dataLine{line}))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `store: import agent_configs:{"id":"`+id+`"}: connector binding "customers" binds "nessy", `+
		`whose tools read private data, and connector binding "code" can send data out with no person's approval`)
	s.False(s.holds(id), "the refused row is not written")
}

func (s *DataMoveSuite) TestAnImportedAgentWhoseOutboundToolsNeedApprovalBesideASensitiveConnectorIsWritten() {
	template := s.agentTemplate()
	s.useInternalApp()
	approved := sessionGitHubGranting("code", "create_issue")
	approved["require_approval"] = []string{"create_issue"}
	line, id := s.importedAgent(template, map[string]any{"connectors": []map[string]any{sessionNessy("customers"), approved}})

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson([]dataLine{line}), nil))

	s.True(s.holds(id))
}

// The rule weighs an imported config's sandbox too, read back with the rest it weighs.
func (s *DataMoveSuite) TestAnImportedAgentWhoseSandboxReachesTheNetworkBesideASensitiveConnectorIsRefused() {
	template := s.agentTemplate()
	s.useInternalApp()
	line, id := s.importedAgent(template, map[string]any{"connectors": []map[string]any{sessionNessy("customers")},
		"sandbox_provider": "daytona", "sandbox_options": map[string]any{"network": true}})

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", ndjson([]dataLine{line}))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `store: import agent_configs:{"id":"`+id+`"}: connector binding "customers" binds "nessy", `+
		`whose tools read private data, and the sandbox's network access can send data out with no person's approval`)
	s.False(s.holds(id))
}

// Each row is written on its own, so the one refused is whichever of the two comes second.
func (s *DataMoveSuite) TestAnImportedParentAndChildThatBreakTheRuleTogetherAreRefusedInEitherOrder() {
	template := s.agentTemplate()
	for _, parentFirst := range []bool{true, false} {
		s.useInternalApp()
		childName := "agent-" + s.utils.uuid()
		child, childID := s.importedAgent(template, map[string]any{"name": childName,
			"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
		parent, parentID := s.importedAgent(template, map[string]any{"connectors": []map[string]any{sessionNessy("customers")},
			"agents": []map[string]any{{"config": childName, "tool": "ask_child"}}})
		lines, second := []dataLine{parent, child}, childID
		if !parentFirst {
			lines, second = []dataLine{child, parent}, parentID
		}

		status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", ndjson(lines))

		s.Equal(http.StatusBadRequest, status, "parent first: %v", parentFirst)
		s.Contains(failure, `store: import agent_configs:{"id":"`+second+`"}: the agent `)
		s.Contains(failure, `'s connector binding "customers" binds "nessy", whose tools read private data, and connector binding "code" of the agent "`+childName+`"`)
		s.False(s.holds(second), "parent first: %v", parentFirst)
	}
}

func (s *DataMoveSuite) TestAnImportedParentIsWeighedWithTheChildItCallsHere() {
	template := s.agentTemplate()
	s.useInternalApp()
	writer := s.textAgent(map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	parent, id := s.importedAgent(template, map[string]any{"connectors": []map[string]any{sessionNessy("customers")},
		"agents": []map[string]any{{"config": writer.Name, "tool": "ask_writer"}}})

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{parent}))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `store: import agent_configs:{"id":"`+id+`"}: the agent `)
	s.Contains(failure, `and connector binding "code" of the agent "`+writer.Name+`" can send data out`)
	s.False(s.holds(id))
}

// A change is weighed with the parent that calls the config here, as a row is.
func (s *DataMoveSuite) TestAChangedChildIsWeighedWithTheParentThatCallsItHere() {
	s.useInternalApp()
	child := s.textAgent(nil)
	s.textAgent(map[string]any{"connectors": []map[string]any{sessionNessy("customers")},
		"agents": []map[string]any{{"config": child.Name, "tool": "ask_child"}}})
	changed := s.with(s.exportedAgent(child.Id), map[string]any{
		"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	line := dataLine{Change: &store.DataChange{Table: "agent_configs", Op: store.ChangeUpdate,
		Key: json.RawMessage(`{"id":"` + child.Id + `"}`), Payload: changed}}

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", ndjson([]dataLine{line}))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `store: import agent_configs:{"id":"`+child.Id+`"}: the agent `)
	s.Contains(failure, `and connector binding "code" of the agent "`+child.Name+`" can send data out`)
	var stored AgentConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/configs/"+child.Id, nil, &stored))
	s.Nil(stored.Connectors, "the refused change is not written")
}

// Control: with no sensitive connector, a parent and child that both send data out import as
// they did before the rule held for imports (a probe on main at 22120e73e answered 200).
func (s *DataMoveSuite) TestImportedAgentsWithNoSensitiveConnectorAreWritten() {
	template := s.agentTemplate()
	s.useInternalApp()
	childName := "agent-" + s.utils.uuid()
	child, childID := s.importedAgent(template, map[string]any{"name": childName,
		"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	parent, parentID := s.importedAgent(template, map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")},
		"agents": []map[string]any{{"config": childName, "tool": "ask_child"}}})

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson([]dataLine{parent, child}), nil))

	s.True(s.holds(parentID))
	s.True(s.holds(childID))
}

// A config the source deleted runs nothing, so one that broke the rule before it held does not
// keep its app from moving.
func (s *DataMoveSuite) TestAnImportedDeletedAgentThatBreaksTheRuleIsWritten() {
	template := s.agentTemplate()
	s.useInternalApp()
	line, id := s.importedAgent(template, map[string]any{"deleted_at": "2026-11-01T00:00:00Z",
		"connectors": []map[string]any{sessionNessy("customers"), sessionGitHubGranting("code", "create_issue")}})

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson([]dataLine{line}), nil))

	var deleted bool
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.T().Context(),
		"SELECT deleted_at IS NOT NULL FROM agent_configs WHERE id = ?", id).Scan(&deleted))
	s.True(deleted)
}

// A parent's children are the configs of its own app by those names, not another app's.
func (s *DataMoveSuite) TestAnImportedParentIsNotWeighedWithAnotherAppsAgentOfItsChildsName() {
	template := s.agentTemplate()
	s.useInternalApp()
	name := "helper-" + s.utils.uuid()
	s.textAgent(map[string]any{"name": name, "connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	s.useInternalApp()
	s.textAgent(map[string]any{"name": name})
	parent, id := s.importedAgent(template, map[string]any{"connectors": []map[string]any{sessionNessy("customers")},
		"agents": []map[string]any{{"config": name, "tool": "ask_helper"}}})

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{parent}), nil))

	s.True(s.holds(id))
}

// An export carries agent configs before connector definitions, so a config binding a custom
// connector beside a sensitive one is weighed again once the definition is written.
func (s *DataMoveSuite) TestAnImportedConnectorAnAgentBindsBesideASensitiveOneIsRefused() {
	template := s.agentTemplate()
	s.useInternalApp()
	connector := "custom_a" + store.NewID()
	agent, _ := s.importedAgent(template, map[string]any{
		"connectors": []map[string]any{sessionNessy("customers"), sessionCustom("crm", connector)}})

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", ndjson([]dataLine{agent, s.customConnector(connector)}))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `store: import connector_definitions:{"id":"`+connector+`","revision":1}: the agent "`)
	s.Contains(failure, `" binds it: connector binding "customers" binds "nessy", whose tools read private data, `+
		`and connector binding "crm" can send data out`)
	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+connector, nil, nil),
		"the refused definition is not written")
}

// The control: the same import, with the custom connector's tool held for a person's approval.
func (s *DataMoveSuite) TestAnImportedConnectorAnAgentBindsWithApprovalBesideASensitiveOneIsWritten() {
	template := s.agentTemplate()
	s.useInternalApp()
	connector := "custom_a" + store.NewID()
	approved := sessionCustom("crm", connector)
	approved["require_approval"] = []string{"create_contact"}
	agent, _ := s.importedAgent(template, map[string]any{"connectors": []map[string]any{sessionNessy("customers"), approved}})

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson([]dataLine{agent, s.customConnector(connector)}), nil))

	s.Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+connector, nil, nil))
}

// A definition is weighed with the agents of a config binding it too: a child binding it, whose
// parent binds nessy.
func (s *DataMoveSuite) TestAnImportedConnectorAChildBindsUnderASensitiveParentIsRefused() {
	template := s.agentTemplate()
	s.useInternalApp()
	connector := "custom_a" + store.NewID()
	childName := "agent-" + s.utils.uuid()
	child, _ := s.importedAgent(template, map[string]any{"name": childName,
		"connectors": []map[string]any{sessionCustom("crm", connector)}})
	parent, _ := s.importedAgent(template, map[string]any{"connectors": []map[string]any{sessionNessy("customers")},
		"agents": []map[string]any{{"config": childName, "tool": "ask_child"}}})

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import",
		ndjson([]dataLine{parent, child, s.customConnector(connector)}))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `store: import connector_definitions:{"id":"`+connector+`","revision":1}: the agent "`+childName+`" binds it: the agent `)
	s.Contains(failure, `and connector binding "crm" of the agent "`+childName+`" can send data out`)
}

// A config the source deleted, or another app's, runs nothing here beside the definition.
func (s *DataMoveSuite) TestAnImportedConnectorADeletedOrAnotherAppsAgentBindsIsWritten() {
	template := s.agentTemplate()
	s.useInternalApp()
	connector := "custom_a" + store.NewID()
	theirs := s.textAgent(map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	_, err := s.store.GlobalDB().ExecContext(s.T().Context(), "UPDATE agent_configs SET connectors = connectors || ?::jsonb WHERE id = ?",
		`[{"name": "crm", "connector_id": "`+connector+`", "connection": {"type": "session"}, "tools": [{"name": "create_contact"}]}]`, theirs.Id)
	s.Require().NoError(err)
	s.useInternalApp()
	deleted, _ := s.importedAgent(template, map[string]any{"deleted_at": "2026-11-01T00:00:00Z",
		"connectors": []map[string]any{sessionNessy("customers"), sessionCustom("crm", connector)}})

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson([]dataLine{deleted, s.customConnector(connector)}), nil))

	s.Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+connector, nil, nil))
}

// customConnector is an export's line of a custom definition id with a bearer scheme.
func (s *DataMoveSuite) customConnector(id string) dataLine {
	return s.importedConnector(id, `"schemes": ["bearer"], "endpoints": {"mcp": "https://mcp.acme.example/mcp"}`)
}

// sessionCustom binds the custom connector id under alias with a session connection, granting
// a tool its source does not say only reads.
func sessionCustom(alias, id string) map[string]any {
	return map[string]any{"name": alias, "connector_id": id, "connection": map[string]any{"type": "session"},
		"tools": []map[string]any{{"name": "create_contact"}}}
}

// useInternalApp gives the test an app of its own that Stream marks internal, the only kind
// that sees nessy.
func (s *DataMoveSuite) useInternalApp() {
	s.useApp(s.data.createAppAdmitting(store.AppSettings{InternalConnectors: new(true)}))
}

// agentTemplate is the export's row of a text agent of the app's, to import agents from.
func (s *DataMoveSuite) agentTemplate() json.RawMessage {
	return s.exportedAgent(s.textAgent(nil).Id)
}

// textAgent is a text agent config the app stores through the API, with fields.
func (s *DataMoveSuite) textAgent(fields map[string]any) AgentConfig {
	request := map[string]any{"name": "agent-" + s.utils.uuid(), "mode": "text", "llm": "llm-flow", "instructions": "be brief"}
	maps.Copy(request, fields)
	status, payload := s.serverClient.call(http.MethodPost, "/v1/agents/configs", request)
	s.Require().Equal(http.StatusCreated, status, string(payload))
	var created AgentConfig
	s.Require().NoError(json.Unmarshal(payload, &created))
	return created
}

// exportedAgent is the export's row of the app's agent config id.
func (s *DataMoveSuite) exportedAgent(id string) json.RawMessage {
	for _, line := range s.export() {
		if line.Table == "agent_configs" && strings.Contains(string(line.Row), `"`+id+`"`) {
			return line.Row
		}
	}
	s.FailNow("the export has no agent config " + id)
	return nil
}

// importedAgent is an export's line of an agent config from template, under an id and a name
// of its own unless fields names one, and the fields as the source stored them.
func (s *DataMoveSuite) importedAgent(template json.RawMessage, fields map[string]any) (dataLine, string) {
	id := store.NewID()
	set := map[string]any{"id": id, "name": "agent-" + s.utils.uuid()}
	maps.Copy(set, fields)
	return dataLine{Table: "agent_configs", Row: s.with(template, set)}, id
}

// with is an exported row with fields set.
func (s *DataMoveSuite) with(row json.RawMessage, fields map[string]any) json.RawMessage {
	decoded := map[string]any{}
	s.Require().NoError(json.Unmarshal(row, &decoded))
	maps.Copy(decoded, fields)
	encoded, err := json.Marshal(decoded)
	s.Require().NoError(err)
	return encoded
}

// holds is whether the app holds the agent config id.
func (s *DataMoveSuite) holds(id string) bool {
	status, _ := s.serverClient.call(http.MethodGet, "/v1/agents/configs/"+id, nil)
	return status == http.StatusOK
}
