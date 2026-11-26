//go:build integration

package api

import (
	"encoding/json"
	"maps"
	"net/http"
	"strconv"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	httpsource "github.com/GetStream/Vision-Agents/acceleration/internal/connectors/sources/http"
	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
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

	s.refusedAll(run)
	s.Contains(s.refusals(parent), strconv.Quote(parent.Name)+
		`'s connector binding "customers" binds "nessy", whose tools read private data, and connector binding "code" `+
		"of the agent "+strconv.Quote(writer.Name)+" can send data out with no person's approval")
}

func (s *ChildAgentsSuite) TestADelegationIsRefusedWhenAChildIsSensitiveAndTheParentSendsOut() {
	s.useInternalApp()
	parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	reader := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	s.delegate(parent, map[string]AgentConfig{"ask_b": reader})

	run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

	s.refusedAll(run)
	s.Contains(s.refusals(parent), strconv.Quote(reader.Name)+`'s connector binding "customers" binds "nessy"`)
	s.Contains(s.refusals(parent), `and connector binding "code" of the agent `+strconv.Quote(parent.Name)+" can send data out")
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

	s.refusedAll(run)
	s.Contains(s.refusals(reader), strconv.Quote(reader.Name)+
		`'s connector binding "customers" binds "nessy", whose tools read private data, and connector binding "code" `+
		"of the agent "+strconv.Quote(writer.Name)+" can send data out")
}

func (s *ChildAgentsSuite) TestADelegationIsRefusedWhenAChildsSandboxReachesTheNetworkBesideASensitiveParent() {
	s.useInternalApp()
	parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	coder := s.agentWith("echo/echo-model", map[string]any{"sandbox": "daytona", "sandbox_options": map[string]any{"network": true}})
	s.delegate(parent, map[string]AgentConfig{"ask_c": coder})

	run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

	s.refusedAll(run)
	s.Contains(s.refusals(coder), strconv.Quote(parent.Name)+
		`'s connector binding "customers" binds "nessy", whose tools read private data, and the sandbox's network `+
		"access of the agent "+strconv.Quote(coder.Name)+" can send data out")
	s.NotContains(s.refusals(coder), "require_approval", "a sandbox has no tool a require_approval could hold")
}

// The parent's own sandbox is part of the graph: it can send out what a sensitive child answered.
func (s *ChildAgentsSuite) TestADelegationIsRefusedWhenTheParentsSandboxReachesTheNetworkBesideASensitiveChild() {
	s.useInternalApp()
	parent := s.agentWith("fanning/fanning-model", map[string]any{"sandbox": "daytona", "sandbox_options": map[string]any{"network": true}})
	reader := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	s.delegate(parent, map[string]AgentConfig{"ask_a": reader})

	run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

	s.refusedAll(run)
	s.Contains(s.refusals(parent), strconv.Quote(reader.Name)+
		`'s connector binding "customers" binds "nessy", whose tools read private data, and the sandbox's network `+
		"access of the agent "+strconv.Quote(parent.Name)+" can send data out")
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

	s.Empty(s.refusals(parent))
	s.Contains(run.FinalText, "tool: be brief", "the held child answered")
	s.Contains(run.FinalText, "tool: That did not work: the tool failed", "the deleted child is a failed tool call")
}

// A child whose tool has the name of another tool is not offered, so it runs nothing a
// sibling could read.
func (s *ChildAgentsSuite) TestADelegationRunsBesideASensitiveChildTheParentDoesNotOffer() {
	s.useInternalApp()
	parent := s.agentWith("fanning/fanning-model", nil)
	reader := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	writer := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	s.delegate(parent, map[string]AgentConfig{"ask_a": writer})
	s.change(parent.Id, func(config *store.AgentConfig) {
		config.Agents = append(config.Agents, store.ChildAgent{Config: reader.Name, Tool: harness.LoadSkillTool})
	})

	run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

	s.Empty(s.refusals(parent))
	s.Contains(run.FinalText, "tool: be brief")
}

// An http tool that only reads at the latest revision reads only for a grant that pins that
// revision's digest: a session opens it at the digest its grant pins, or pins one on first use.
// Beside a sensitive connector, in the parent or in a child, a delegation so runs for the pinned
// grant and is refused for a grant that pins no digest or another one (AI-3675).
func (s *ChildAgentsSuite) TestADelegationJudgesAnHTTPToolReadOnlyOnlyAtTheDigestItsGrantPins() {
	s.useInternalApp()
	id, digest := s.readOnlyHTTPConnector()
	for name, c := range map[string]struct {
		grant   store.ToolGrant
		refused bool
	}{
		"pinned":         {store.ToolGrant{Name: "get", SchemaDigest: digest}, false},
		"pinned nothing": {store.ToolGrant{Name: "get"}, true},
		"pinned another": {store.ToolGrant{Name: "get", SchemaDigest: strings.Repeat("0", len(digest))}, true},
	} {
		api := store.ConnectorBinding{Name: "api", ConnectorID: id, Connection: store.ConnectionBinding{Type: "session"},
			Tools: []store.ToolGrant{c.grant}}
		nessy := store.ConnectorBinding{Name: "customers", ConnectorID: "nessy", Connection: store.ConnectionBinding{Type: "session"},
			Tools: []store.ToolGrant{{Name: "org_lookup"}}}
		for layout, bindings := range map[string][2]store.ConnectorBinding{"sensitive parent": {nessy, api}, "sensitive child": {api, nessy}} {
			s.Run(name+", "+layout, func() {
				parent := s.agentWith("fanning/fanning-model", nil)
				child := s.agentWith("echo/echo-model", nil)
				s.delegate(parent, map[string]AgentConfig{"ask_a": child})
				s.change(parent.Id, func(config *store.AgentConfig) { config.Connectors = []store.ConnectorBinding{bindings[0]} })
				s.change(child.Id, func(config *store.AgentConfig) { config.Connectors = []store.ConnectorBinding{bindings[1]} })

				run := s.run(CreateRunRequest{ConfigId: parent.Id, Input: "review this"})

				if c.refused {
					s.refusedAll(run)
				} else {
					s.NotContains(run.FinalText, "was not run")
					s.Contains(run.FinalText, "tool: be brief")
				}
			})
		}
	}
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

// A config stored past the config writes' check opens no session while it breaks the rule
// alone. With nessy alone, a config runs (TestADelegationIsRefusedWhenTheParentIsSensitiveAndAChildSendsOut).
func (s *ChildAgentsSuite) TestASessionOfAConfigThatBreaksTheRuleAloneIsRefused() {
	s.useInternalApp()
	agent := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	s.change(agent.Id, func(config *store.AgentConfig) {
		config.Connectors = append(config.Connectors, store.ConnectorBinding{Name: "code", ConnectorID: "github",
			Connection: store.ConnectionBinding{Type: "session"}, Tools: []store.ToolGrant{{Name: "create_issue"}}})
	})

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/sessions", CreateSessionRequest{ConfigId: &agent.Id})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `session: connector binding "customers" binds "nessy", whose tools read private data, `+
		`and connector binding "code" can send data out with no person's approval`)
}

// The session's own sandbox is weighed too: one that reaches the network beside nessy.
func (s *ChildAgentsSuite) TestASessionOfAConfigWhoseSandboxReachesTheNetworkBesideASensitiveConnectorIsRefused() {
	s.useInternalApp()
	agent := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	s.change(agent.Id, func(config *store.AgentConfig) {
		config.Sandbox = store.AgentSandbox{Provider: "daytona", Options: sandbox.Config{Network: true}}
	})

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/sessions", CreateSessionRequest{ConfigId: &agent.Id})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `session: connector binding "customers" binds "nessy", whose tools read private data, `+
		`and the sandbox's network access can send data out with no person's approval`)
}

// A run is refused as a session is: the config's to correct, not the router's failure.
func (s *ChildAgentsSuite) TestARunOfAConfigThatBreaksTheRuleAloneIsRefused() {
	s.useInternalApp()
	agent := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	s.change(agent.Id, func(config *store.AgentConfig) {
		config.Connectors = append(config.Connectors, store.ConnectorBinding{Name: "code", ConnectorID: "github",
			Connection: store.ConnectionBinding{Type: "session"}, Tools: []store.ToolGrant{{Name: "create_issue"}}})
	})

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/runs", CreateRunRequest{ConfigId: agent.Id, Input: "review this"})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, `session: connector binding "customers" binds "nessy", whose tools read private data, `+
		`and connector binding "code" can send data out with no person's approval`)
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

// refusals is the log's lines about a refused delegation that name config, with what the tool error leaves out.
func (s *ChildAgentsSuite) refusals(config AgentConfig) string {
	var lines []string
	for line := range strings.SplitSeq(s.logged.String(), "\n") {
		if strings.Contains(line, "a delegation was refused") && strings.Contains(line, config.Name) {
			lines = append(lines, line)
		}
	}
	// The text handler quotes the complaint, and its own quotes with it.
	return strings.ReplaceAll(strings.Join(lines, "\n"), `\`, "")
}

// refusedAll requires every delegation of the run refused, and the parent to have answered. What
// the model and the end user read is that the tool failed: the log has the complaint.
func (s *ChildAgentsSuite) refusedAll(run Run) {
	s.Equal(len(fanned), strings.Count(run.FinalText, "tool: That did not work: the tool failed"),
		"the graph breaks the rule whichever child is called")
	s.NotContains(run.FinalText, "tool: be brief", "no child ran")
	for _, name := range []string{"nessy", "customers", "create_issue", "binding", "connector", "sandbox", "require_approval"} {
		s.NotContains(run.FinalText, name, "the tool error names nothing of a server-side config")
	}
}

// readOnlyHTTPConnector is a custom connector whose latest revision has one http tool, get, a
// read-only GET, and the digest a grant pins to open it as listed there. Its manifest is
// linear.yaml's with an http source in place of its mcp one (as in ConfigsSuite).
func (s *ChildAgentsSuite) readOnlyHTTPConnector() (string, string) {
	ctx := s.T().Context()
	linear, err := s.store.ConnectorDefinitions.Latest(ctx, s.customerID(), "linear")
	s.Require().NoError(err)
	tool := core.HTTPTool{Name: "get", Description: "Reads the API.", Method: http.MethodGet, Path: "/get", ReadOnly: true}
	manifest := linear.Manifest
	manifest.ID = "custom_t" + strings.ReplaceAll(s.utils.uuid(), "-", "")
	manifest.Endpoints = maps.Clone(manifest.Endpoints)
	manifest.Endpoints[core.SourceKindHTTP] = "https://8.8.8.8/api"
	manifest.Sources = []core.SourceRule{{Kind: core.SourceKindHTTP, Endpoint: core.SourceKindHTTP, HTTPTools: []core.HTTPTool{tool}}}
	_, err = s.store.ConnectorDefinitions.Revise(ctx, s.customerID(), manifest)
	s.Require().NoError(err)
	digest, err := httpsource.ToolSchemaDigest(tool)
	s.Require().NoError(err)
	return manifest.ID, digest
}
