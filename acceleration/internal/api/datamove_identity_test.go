//go:build integration

package api

import (
	"encoding/json"
	"net/http"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// An import holds an agent config's identity to what a config write holds it to
// (identityComplaint): a row naming an id Stream keeps for itself, or a user no session can
// write as, is refused by name and not written.

func (s *DataMoveSuite) TestAnImportedAgentWhoseIdentityNamesAReservedIDIsRefused() {
	template := s.agentTemplate()
	for column, field := range map[string]string{"identity_agent_id": "identity.agent_id", "identity_user_id": "identity.user_id"} {
		s.useInternalApp()
		line, id := s.importedAgent(template, map[string]any{column: "!anon"})

		status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", ndjson([]dataLine{line}))

		s.Equal(http.StatusBadRequest, status, column)
		s.Contains(failure, `store: import agent_configs:{"id":"`+id+`"}: `+field+` may not start with !: Stream keeps those user ids for itself`)
		s.False(s.holds(id), "the refused row is not written: %s", column)
	}
}

func (s *DataMoveSuite) TestAnImportedAgentWhoseIdentityUserCannotWriteIsRefused() {
	template := s.agentTemplate()
	s.useInternalApp()
	line, id := s.importedAgent(template, map[string]any{"identity_user_id": "support bot"})

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", ndjson([]dataLine{line}))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `store: import agent_configs:{"id":"`+id+`"}: identity.user_id may hold only letters, digits, _, . and -, up to 128 of them`)
	s.False(s.holds(id))
}

// A change is weighed as a row is, and the stored identity stays.
func (s *DataMoveSuite) TestAChangeGivingAnAgentAReservedIdentityIsRefused() {
	s.useInternalApp()
	agent := s.textAgent(map[string]any{"identity": map[string]any{"user_id": "support-bot"}})
	changed := s.with(s.exportedAgent(agent.Id), map[string]any{"identity_user_id": "!anon"})
	line := dataLine{Change: &store.DataChange{Table: "agent_configs", Op: store.ChangeUpdate,
		Key: json.RawMessage(`{"id":"` + agent.Id + `"}`), Payload: changed}}

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", ndjson([]dataLine{line}))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `store: import agent_configs:{"id":"`+agent.Id+`"}: identity.user_id may not start with !`)
	var stored AgentConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/configs/"+agent.Id, nil, &stored))
	s.Require().NotNil(stored.Identity)
	s.Equal("support-bot", stored.Identity.UserId, "the refused change is not written")
}

// Control: an identity a config write takes imports as it is.
func (s *DataMoveSuite) TestAnImportedAgentWithAnIdentityIsWritten() {
	template := s.agentTemplate()
	s.useInternalApp()
	line, id := s.importedAgent(template, map[string]any{
		"identity_agent_id": "docs-helper", "identity_user_id": "docs-helper.bot", "identity_user_name": "Docs Helper"})

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson([]dataLine{line}), nil))

	var stored AgentConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/configs/"+id, nil, &stored))
	s.Equal(&AgentIdentity{AgentId: "docs-helper", UserId: "docs-helper.bot", UserName: "Docs Helper"}, stored.Identity)
}
