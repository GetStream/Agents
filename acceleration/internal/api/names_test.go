//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// NamesSuite holds what a create or a rename to a name that is already taken is answered
// with, for each resource a customer names.
type NamesSuite struct {
	RouterSuite
}

func TestNamesSuite(t *testing.T) {
	runSuite(t, new(NamesSuite))
}

// SetupTest gives every test an app of its own, so the names it takes are its own.
func (s *NamesSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

// refused calls the router and returns the failure it answered with.
func (s *NamesSuite) refused(method, path string, body any) (int, ErrorDetail) {
	status, payload := s.serverClient.call(method, path, body)
	var answer ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answer), string(payload))
	return status, answer.Error
}

// taken checks a failure is the 409 for a name already taken, saying so in its own words.
func (s *NamesSuite) taken(status int, failure ErrorDetail, message string) {
	s.Equal(http.StatusConflict, status)
	s.Equal(ErrorTypeConflict, failure.Type)
	s.Equal(codeNameTaken, failure.Code)
	s.Equal(message, failure.Message)
}

func (s *NamesSuite) agent(name string) AgentConfig {
	var created AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		map[string]any{"name": name}, &created))
	return created
}

func (s *NamesSuite) TestAnAgentCannotBeCreatedUnderANameAnotherHas() {
	s.agent("support")

	status, failure := s.refused(http.MethodPost, "/v1/agents/configs", map[string]any{"name": "support"})

	s.taken(status, failure, "an agent with this name already exists")
}

func (s *NamesSuite) TestAnAgentCannotBeRenamedToANameAnotherHas() {
	s.agent("support")
	sales := s.agent("sales")

	status, failure := s.refused(http.MethodPut, "/v1/agents/configs/"+sales.Id,
		map[string]any{"name": "support"})
	s.taken(status, failure, "an agent with this name already exists")

	status, failure = s.refused(http.MethodPatch, "/v1/agents/configs/"+sales.Id,
		map[string]any{"name": "support"})
	s.taken(status, failure, "an agent with this name already exists")
}

func (s *NamesSuite) TestADeletedAgentsNameIsFreeAgain() {
	gone := s.agent("support")
	s.Require().Equal(http.StatusNoContent,
		s.serverClient.do(http.MethodDelete, "/v1/agents/configs/"+gone.Id, nil, nil))

	s.agent("support")
}

func (s *NamesSuite) skill(configID, name string) (int, ErrorDetail) {
	return s.refused(http.MethodPost, "/v1/agents/skills", map[string]any{
		"config_id": configID, "name": name, "description": "work out what a caller is owed",
		"instructions": "Read the order and the policy, then say what to refund.",
	})
}

func (s *NamesSuite) TestASkillNameIsTakenOnlyOnItsOwnAgent() {
	support, sales := s.agent("support"), s.agent("sales")
	status, _ := s.skill(support.Id, "refund")
	s.Require().Equal(http.StatusCreated, status)

	status, failure := s.skill(support.Id, "refund")
	s.taken(status, failure, "this agent already has a skill with this name")

	status, _ = s.skill(sales.Id, "refund")
	s.Equal(http.StatusCreated, status, "another agent may have a skill of the same name")
}

func (s *NamesSuite) TestARouterCannotTakeANameAnotherHas() {
	var cheap RouterConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/router/configs",
		map[string]any{"name": "fast"}, nil))
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/router/configs",
		map[string]any{"name": "cheap"}, &cheap))

	status, failure := s.refused(http.MethodPost, "/v1/router/configs", map[string]any{"name": "fast"})
	s.taken(status, failure, "a router with this name already exists")

	status, failure = s.refused(http.MethodPut, "/v1/router/configs/"+cheap.Id, map[string]any{"name": "fast"})
	s.taken(status, failure, "a router with this name already exists")
}

func (s *NamesSuite) TestAVoiceCannotTakeANameAnotherHas() {
	var narrator Voice
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/voices",
		map[string]any{"name": "founder"}, nil))
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/voices",
		map[string]any{"name": "narrator"}, &narrator))

	status, failure := s.refused(http.MethodPost, "/v1/agents/voices", map[string]any{"name": "founder"})
	s.taken(status, failure, "a voice with this name already exists")

	status, failure = s.refused(http.MethodPut, "/v1/agents/voices/"+narrator.Id,
		map[string]any{"name": "founder"})
	s.taken(status, failure, "a voice with this name already exists")
}

func (s *NamesSuite) TestEveryNameIndexTheStoreKnowsIsOneTheMigrationsMake() {
	// The store tells a name taken by the unique index it violated, so an index renamed in
	// a migration would quietly turn a 409 back into a 500.
	for _, index := range []string{
		"agent_configs_name_idx", "skills_name_idx", "router_configs_name_idx", "voices_name_idx",
	} {
		var found bool
		s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.T().Context(),
			"SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = ?)", index).Scan(&found))
		s.True(found, index)
	}
}

// skillWith is the skill named name sent with this method, whose config runs its scripts.
func (s *NamesSuite) skillWith(method, path, configID, name string, files map[string]string) (int, ErrorDetail) {
	return s.refused(method, path, map[string]any{
		"config_id": configID, "name": name, "description": "fill PDF forms", "instructions": "Fill the form.", "files": files,
	})
}

// notAFolder are names a skill's folder in the sandbox could not have.
var notAFolder = []string{".", "..", "../../tmp", "a/b", "/a", `a\b`, "-p", ".hidden", "a\x00b", "a\nb", "a\u0085b", "  .hidden", strings.Repeat("n", 256)}

func (s *NamesSuite) TestASkillCannotBeGivenANameItsFolderCouldNotHave() {
	config := s.agent("support")
	var named Skill
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/skills", map[string]any{
		"config_id": config.Id, "name": "refund", "description": "refund", "instructions": "Refund.",
	}, &named))

	for _, name := range notAFolder {
		status, failure := s.skill(config.Id, name)
		s.Equal(http.StatusBadRequest, status, name)
		s.Contains(failure.Message, "is not a folder's name", name)
		s.Less(len(failure.Message), 300)

		status, failure = s.skillWith(http.MethodPut, "/v1/agents/skills/"+named.Id, config.Id, name, nil)
		s.Equal(http.StatusBadRequest, status, name)
		s.Contains(failure.Message, "is not a folder's name", name)

		status, failure = s.refused(http.MethodPost, "/v1/agents/sync", map[string]any{"name": "synced", "hash": "v1",
			"skills": []map[string]any{{"config_id": "", "name": name, "description": "d", "instructions": "i"}}})
		s.Equal(http.StatusBadRequest, status, name)
		s.Contains(failure.Message, "is not a folder's name", name)
	}
	var kept []AgentConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/configs?name=synced", nil, &kept))
	for _, config := range kept {
		s.NotEqual("synced", config.Name, "a refused sync stores nothing")
	}
}

func (s *NamesSuite) TestASkillMayHaveAnyNameItsFolderCanHave() {
	config := s.agent("support")
	for _, name := range []string{"Refund Decision", "refund decision", "50% off", "café", "返金", "v1.2", "a..b", "it's \"ok\"",
		"refund_v2-final", "$(x)", strings.Repeat("返", 255)} {
		status, failure := s.skill(config.Id, name)
		s.Equal(http.StatusCreated, status, name+failure.Message)
	}
	status, _ := s.skill(config.Id, "  spaced\t\n")
	s.Equal(http.StatusCreated, status)

	status, failure := s.skill(config.Id, "   ")
	s.Equal(http.StatusBadRequest, status)
	s.Equal("a skill needs a name", failure.Message)
	status, failure = s.refused(http.MethodPost, "/v1/agents/sync", map[string]any{"name": "support", "hash": "v1",
		"skills": []map[string]any{{"config_id": "", "name": " ", "description": "d", "instructions": "i"}}})
	s.Equal(http.StatusBadRequest, status)
	s.Equal("a skill needs a name", failure.Message)
}

func (s *NamesSuite) TestASkillStoredUnderAnotherNameStaysUsable() {
	var config AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		map[string]any{"name": "support", "mode": "text", "llm": "en-low-latency", "skills": []string{"billing/refund"}}, &config))
	// Stored before such a name was refused, or moved here.
	stored := store.Skill{CustomerID: s.customerID(), ConfigID: config.Id, Name: "billing/refund", Description: "d", Instructions: "i",
		Files: map[string]string{"notes.md": "print(1)"}}
	s.Require().NoError(s.store.Skills.Insert(s.T().Context(), &stored))

	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/sessions", CreateSessionRequest{ConfigId: &config.Id}, nil))

	status, failure := s.skillWith(http.MethodPut, "/v1/agents/skills/"+stored.ID, config.Id, "billing/refund", map[string]string{"notes.md": "print(2)"})
	s.Equal(http.StatusOK, status, failure.Message)
	status, failure = s.refused(http.MethodPost, "/v1/agents/sync", map[string]any{"name": "support", "hash": "v1",
		"skills": []map[string]any{{"config_id": "", "name": "billing/refund", "description": "synced", "instructions": "i"}}})
	s.Equal(http.StatusOK, status, failure.Message)
	var listed []Skill
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/skills?config_id="+config.Id, nil, &listed))
	s.Require().Len(listed, 1)
	s.Equal("billing/refund", listed[0].Name)
	s.Equal("synced", listed[0].Description)

	status, failure = s.refused(http.MethodPost, "/v1/agents/sync", map[string]any{"name": "support", "hash": "v2",
		"skills": []map[string]any{{"config_id": "", "name": "billing/refund", "description": "d", "instructions": "i"},
			{"config_id": "", "name": "billing/other", "description": "d", "instructions": "i"}}})
	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure.Message, `"billing/other"`)

	status, _ = s.skillWith(http.MethodPut, "/v1/agents/skills/"+stored.ID, config.Id, "refund", nil)
	s.Equal(http.StatusOK, status, "it can be renamed to a name its folder can have")
	s.Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/agents/skills/"+stored.ID, nil, nil))

	padded := store.Skill{CustomerID: s.customerID(), ConfigID: config.Id, Name: " billing/refund ", Description: "d", Instructions: "i"}
	s.Require().NoError(s.store.Skills.Insert(s.T().Context(), &padded))
	status, failure = s.skillWith(http.MethodPut, "/v1/agents/skills/"+padded.ID, config.Id, " billing/refund ", nil)
	s.Equal(http.StatusOK, status, failure.Message)
}

func (s *NamesSuite) TestASkillsFileIsAPathInsideItsFolder() {
	var config AgentConfig
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs",
		map[string]any{"name": "support", "sandbox": "daytona"}, &config))

	for _, key := range []string{"", ".", "..", "../a.py", "/a.py", "a/", "a//b", "a/../b", `a\b`, "a\x00b", strings.Repeat("a/", 512) + "a", strings.Repeat("a", 256), strings.Repeat("a", 256) + "/a.md", "a/" + strings.Repeat("a", 256)} {
		status, failure := s.skillWith(http.MethodPost, "/v1/agents/skills", config.Id, "pdf", map[string]string{key: "x"})
		s.Equal(http.StatusBadRequest, status, key)
		s.Contains(failure.Message, "is not a path inside the skill's folder", key)
		s.Less(len(failure.Message), 300)
	}
	status, failure := s.skillWith(http.MethodPost, "/v1/agents/skills", config.Id, "pdf",
		map[string]string{"-v.py": "x", ".env.sample": "x", "a b/返金.md": "x", "..notes": "x", strings.Repeat("a", 255) + "/" + strings.Repeat("b/", 383) + "bc": "x", "Icon\r": ""})
	s.Equal(http.StatusCreated, status, failure.Message)
}
