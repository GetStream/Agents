//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// The graphs below are wired through the config store, past the API's checks, as a graph
// saved before the rule held across agents, or changed since, is stored.

func (s *ChildAgentsSuite) TestADelegationIsRefusedWhenTheParentIsSensitiveAndAChildSendsOut() {
	s.useInternalApp()
	parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	writer := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	s.delegate(parent, map[string]AgentConfig{"ask_a": writer})

	run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

	s.Contains(run.FinalText, "tool: That did not work: ask_a was not run: the agent "+strconv.Quote(parent.Name)+
		`'s connector binding "customers" binds "nessy", whose tools read private data, and connector binding "code" `+
		"of the agent "+strconv.Quote(writer.Name)+" can send data out with no person's approval")
	s.Contains(run.FinalText, "or add to each binding's require_approval its tools that do not only read: the agent "+
		strconv.Quote(writer.Name)+`'s "code" create_issue`)
	s.refusedAll(run)
}

func (s *ChildAgentsSuite) TestADelegationIsRefusedWhenAChildIsSensitiveAndTheParentSendsOut() {
	s.useInternalApp()
	parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	reader := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	s.delegate(parent, map[string]AgentConfig{"ask_b": reader})

	run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

	s.Contains(run.FinalText, "tool: That did not work: ask_b was not run: the agent "+strconv.Quote(reader.Name)+
		`'s connector binding "customers" binds "nessy"`)
	s.Contains(run.FinalText, `and connector binding "code" of the agent `+strconv.Quote(parent.Name)+" can send data out")
	s.refusedAll(run)
}

// Neither child alone breaks the rule, nor does the parent: the parent hands one child's
// answer to the other.
func (s *ChildAgentsSuite) TestADelegationIsRefusedWhenOneChildIsSensitiveAndASiblingSendsOut() {
	s.useInternalApp()
	parent := s.agentWith("fanning/fanning-model", nil)
	reader := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	writer := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	s.delegate(parent, map[string]AgentConfig{"ask_a": reader, "ask_b": writer})

	run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

	s.Contains(run.FinalText, "tool: That did not work: ask_a was not run: the agent "+strconv.Quote(reader.Name)+
		`'s connector binding "customers" binds "nessy", whose tools read private data, and connector binding "code" `+
		"of the agent "+strconv.Quote(writer.Name)+" can send data out")
	s.refusedAll(run)
}

func (s *ChildAgentsSuite) TestADelegationIsRefusedWhenAChildsSandboxReachesTheNetworkBesideASensitiveParent() {
	s.useInternalApp()
	parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	coder := s.agentWith("echo/echo-model", map[string]any{"sandbox": "daytona", "sandbox_options": map[string]any{"network": true}})
	s.delegate(parent, map[string]AgentConfig{"ask_c": coder})

	run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

	s.Contains(run.FinalText, "tool: That did not work: ask_c was not run: the agent "+strconv.Quote(parent.Name)+
		`'s connector binding "customers" binds "nessy", whose tools read private data, and the sandbox's network `+
		"access of the agent "+strconv.Quote(coder.Name)+" can send data out")
	s.NotContains(run.FinalText, "require_approval", "a sandbox has no tool a require_approval could hold")
	s.refusedAll(run)
}

// The false-positive side: a write tool a person must allow first sends nothing out unasked.
// A sibling deleted since the parent was saved is skipped, as its own call is refused.
func (s *ChildAgentsSuite) TestADelegationBesideASensitiveConnectorRunsWhenEachOutboundToolNeedsApproval() {
	s.useInternalApp()
	parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	held := sessionGitHubGranting("code", "create_issue")
	held["require_approval"] = []string{"create_issue"}
	writer := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{held}})
	deleted := s.agentWith("echo/echo-model", nil)
	s.delegate(parent, map[string]AgentConfig{"ask_a": writer, "ask_c": deleted})
	s.Require().NoError(s.store.AgentConfigs.Delete(s.T().Context(), s.customerID(), deleted.Id))

	run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

	s.NotContains(run.FinalText, "was not run")
	s.Contains(run.FinalText, "tool: be brief", "the held child answered")
	s.Contains(run.FinalText, "tool: That did not work: the agent "+strconv.Quote(deleted.Name)+" behind ask_c no longer exists")
}

// TestADelegationWithNoSensitiveConnectorRuns is the control: on the base, dca845c2e, the
// same graph answered the same (author-1.md, «Control probe»).
func (s *ChildAgentsSuite) TestADelegationWithNoSensitiveConnectorRuns() {
	parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	writer := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	s.delegate(parent, map[string]AgentConfig{"ask_a": writer})

	run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

	s.NotContains(run.FinalText, "That did not work")
	s.Contains(run.FinalText, "tool: be brief")
}

// useInternalApp gives the test an app Stream marks internal, the only kind that sees nessy.
func (s *ChildAgentsSuite) useInternalApp() {
	s.useApp(s.data.createAppAdmitting(store.AppSettings{InternalConnectors: new(true)}))
}

// agentWith is a text agent config of the app answered by model, with fields.
func (s *ChildAgentsSuite) agentWith(model string, fields map[string]any) AgentConfig {
	request := map[string]any{"name": "agent-" + s.utils.uuid(), "mode": "text", "llm": model, "instructions": "be brief"}
	for key, value := range fields {
		request[key] = value
	}
	status, payload := s.serverClient.call(http.MethodPost, "/v1/agents/configs", request)
	s.Require().Equal(http.StatusCreated, status, string(payload))
	var created AgentConfig
	s.Require().NoError(json.Unmarshal(payload, &created))
	return created
}

// delegate makes parent call each of fanned: the config children names for it, or else an
// agent with no connector.
func (s *ChildAgentsSuite) delegate(parent AgentConfig, children map[string]AgentConfig) {
	var agents []store.ChildAgent
	for _, tool := range fanned {
		child, ok := children[tool]
		if !ok {
			child = s.agentWith("echo/echo-model", nil)
		}
		agents = append(agents, store.ChildAgent{Config: child.Name, Tool: tool})
	}
	s.change(parent.Id, func(config *store.AgentConfig) { config.Agents = agents })
}

// refusedAll requires every delegation of the run refused, and the parent to have answered.
func (s *ChildAgentsSuite) refusedAll(run Run) {
	for _, tool := range fanned {
		s.Contains(run.FinalText, "tool: That did not work: "+tool+" was not run: ", "the graph breaks the rule whichever child is called")
	}
	s.NotContains(run.FinalText, "tool: be brief", "no child ran")
}
