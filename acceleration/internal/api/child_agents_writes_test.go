//go:build integration

package api

import (
	"encoding/json"
	"maps"
	"net/http"
	"strconv"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// configWrite is one of the ways a config is written: written names it, with its id when it is
// stored, which it is before an update, a patch or a sync, and fields are what the write sends
// besides its name, mode and model.
type configWrite struct {
	name   string
	stored bool
	write  func(s *ChildAgentsSuite, written AgentConfig, fields map[string]any) (int, string)
}

var configWrites = []configWrite{
	{name: "create", write: func(s *ChildAgentsSuite, written AgentConfig, fields map[string]any) (int, string) {
		return s.writeConfig(http.MethodPost, "/v1/agents/configs", written.Name, fields)
	}},
	{name: "update", stored: true, write: func(s *ChildAgentsSuite, written AgentConfig, fields map[string]any) (int, string) {
		return s.writeConfig(http.MethodPut, "/v1/agents/configs/"+written.Id, written.Name, fields)
	}},
	{name: "patch", stored: true, write: func(s *ChildAgentsSuite, written AgentConfig, fields map[string]any) (int, string) {
		status, payload := s.serverClient.call(http.MethodPatch, "/v1/agents/configs/"+written.Id, fields)
		return status, string(payload)
	}},
	{name: "sync", stored: true, write: func(s *ChildAgentsSuite, written AgentConfig, fields map[string]any) (int, string) {
		synced := maps.Clone(fields)
		synced["hash"] = s.utils.uuid()
		return s.writeConfig(http.MethodPost, "/v1/agents/sync", written.Name, synced)
	}},
}

// graphWrite is a set of agents around the written config, which holds before when it is stored,
// built by around once the written config has its name, and what writing fields into it is
// answered: a 400 saying each of says, or a success when says is empty.
type graphWrite struct {
	name   string
	before map[string]any
	around func(s *ChildAgentsSuite, written string) (fields map[string]any, says []string)
}

var graphWrites = []graphWrite{
	{name: "a sensitive parent beside a child that sends out", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		writer := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
		return map[string]any{"connectors": []map[string]any{sessionNessy("customers")}, "agents": []ChildAgent{{Config: writer.Name, Tool: "ask"}}},
			[]string{strconv.Quote(written) + `'s connector binding "customers" binds "nessy", whose tools read private data`,
				`connector binding "code" of the agent ` + strconv.Quote(writer.Name) + " can send data out"}
	}},
	{name: "a parent that sends out beside a sensitive child", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		reader := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
		return map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}, "agents": []ChildAgent{{Config: reader.Name, Tool: "ask"}}},
			[]string{strconv.Quote(reader.Name) + `'s connector binding "customers" binds "nessy"`,
				`connector binding "code" of the agent ` + strconv.Quote(written) + " can send data out"}
	}},
	{name: "a parent of a sensitive child and of a child that sends out", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		reader := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
		writer := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
		return map[string]any{"agents": []ChildAgent{{Config: reader.Name, Tool: "ask_a"}, {Config: writer.Name, Tool: "ask_b"}}},
			[]string{strconv.Quote(reader.Name) + `'s connector binding "customers" binds "nessy"`,
				`connector binding "code" of the agent ` + strconv.Quote(writer.Name) + " can send data out"}
	}},
	{name: "a child gaining a tool that sends out under a sensitive parent", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
		s.calls(parent, written)
		return map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}},
			[]string{strconv.Quote(parent.Name) + `'s connector binding "customers" binds "nessy"`,
				`connector binding "code" of the agent ` + strconv.Quote(written) + " can send data out"}
	}},
	// A child the parent still names but that is gone runs nothing: it is left out, and the
	// children after it are still weighed.
	{name: "a child gaining a tool that sends out under a sensitive parent that names a gone child first", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
		s.calls(parent, "gone-"+s.utils.uuid(), written)
		return map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}},
			[]string{strconv.Quote(parent.Name) + `'s connector binding "customers" binds "nessy"`,
				`connector binding "code" of the agent ` + strconv.Quote(written) + " can send data out"}
	}},
	// The parent that is not sensitive is the older one, which is the one a single lookup finds.
	{name: "a child gaining a tool that sends out under the second of two parents", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		s.calls(s.agentWith("fanning/fanning-model", nil), written)
		parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
		s.calls(parent, written)
		return map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}},
			[]string{strconv.Quote(parent.Name) + `'s connector binding "customers" binds "nessy"`,
				`connector binding "code" of the agent ` + strconv.Quote(written) + " can send data out"}
	}},
	{name: "a child gaining a tool that sends out beside a sensitive sibling", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		reader := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
		s.calls(s.agentWith("fanning/fanning-model", nil), reader.Name, written)
		return map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}},
			[]string{strconv.Quote(reader.Name) + `'s connector binding "customers" binds "nessy"`,
				`connector binding "code" of the agent ` + strconv.Quote(written) + " can send data out"}
	}},
	{name: "a child gaining a sensitive binding under a parent whose sandbox reaches the network", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		parent := s.agentWith("fanning/fanning-model", map[string]any{"sandbox": "daytona", "sandbox_options": map[string]any{"network": true}})
		s.calls(parent, written)
		return map[string]any{"connectors": []map[string]any{sessionNessy("customers")}},
			[]string{strconv.Quote(written) + `'s connector binding "customers" binds "nessy"`,
				"the sandbox's network access of the agent " + strconv.Quote(parent.Name) + " can send data out"}
	}},
	// The set broke the rule before this save, which changes nothing the rule reads.
	{name: "a save of another field of a child in a set that breaks the rule", before: map[string]any{
		"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")},
	}, around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
		s.calls(parent, written)
		return map[string]any{"instructions": "be terse"},
			[]string{strconv.Quote(parent.Name) + `'s connector binding "customers" binds "nessy"`,
				`connector binding "code" of the agent ` + strconv.Quote(written) + " can send data out"}
	}},
	// Children are one level deep, so no set holds a grandchild.
	{name: "a child gaining agents of its own", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		s.calls(s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}}), written)
		writer := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
		return map[string]any{"agents": []ChildAgent{{Config: writer.Name, Tool: "ask"}}}, []string{"agents are one level deep"}
	}},
	// The false-positive side: a tool a person must allow first sends nothing out unasked. A
	// child the parent still names but that is gone runs nothing, so it is left out.
	{name: "a child gaining a tool a person allows under a sensitive parent", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
		s.calls(parent, written, "gone-"+s.utils.uuid())
		held := sessionGitHubGranting("code", "create_issue")
		held["require_approval"] = []string{"create_issue"}
		return map[string]any{"connectors": []map[string]any{held}}, nil
	}},
	{name: "a sensitive parent beside a child whose tool a person allows", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		held := sessionGitHubGranting("code", "create_issue")
		held["require_approval"] = []string{"create_issue"}
		writer := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{held}})
		return map[string]any{"connectors": []map[string]any{sessionNessy("customers")}, "agents": []ChildAgent{{Config: writer.Name, Tool: "ask"}}}, nil
	}},
	// Taking out what breaks the rule is how a set stored before it held is mended: the config's
	// stored copy is not weighed beside what is about to replace it.
	{name: "a child taking out its tool that sends out under a sensitive parent", before: map[string]any{
		"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")},
	}, around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		s.calls(s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}}), written)
		return map[string]any{"connectors": []map[string]any{}}, nil
	}},
	// The controls: on the base, 4a6dd1132, each of the four writes answered these the same
	// (author-1.md, «Base probe»).
	{name: "a child gaining a tool that sends out with nothing sensitive in the set", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		s.calls(s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}}), written)
		return map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}}, nil
	}},
	{name: "a parent that sends out beside a child that sends out", around: func(s *ChildAgentsSuite, written string) (map[string]any, []string) {
		writer := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
		return map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}, "agents": []ChildAgent{{Config: writer.Name, Tool: "ask"}}}, nil
	}},
}

func (s *ChildAgentsSuite) TestAWriteIsHeldToTheSensitiveConnectorRuleOverTheAgentsItWorksWith() {
	for _, path := range configWrites {
		for _, row := range graphWrites {
			s.Run(path.name+"/"+row.name, func() {
				s.useInternalApp()
				written := AgentConfig{Name: "agent-" + s.utils.uuid()}
				fields := map[string]any{}
				if path.stored {
					written = s.agentWith("echo/echo-model", row.before)
				} else {
					maps.Copy(fields, row.before)
				}
				sent, says := row.around(s, written.Name)
				maps.Copy(fields, sent)

				status, answered := path.write(s, written, fields)

				if len(says) == 0 {
					s.Contains([]int{http.StatusOK, http.StatusCreated}, status, answered)
					return
				}
				s.Equal(http.StatusBadRequest, status, answered)
				for _, said := range says {
					s.Contains(s.failureMessage(answered), said)
				}
			})
		}
	}
}

// A config renamed to the name a parent calls becomes that parent's child.
func (s *ChildAgentsSuite) TestARenameIntoTheNameASensitiveParentCallsIsRefused() {
	s.useInternalApp()
	written := s.agentWith("echo/echo-model", map[string]any{"connectors": []map[string]any{sessionGitHubGranting("code", "create_issue")}})
	parent := s.agentWith("fanning/fanning-model", map[string]any{"connectors": []map[string]any{sessionNessy("customers")}})
	slot := "agent-" + s.utils.uuid()
	s.calls(parent, slot)

	for _, method := range []string{http.MethodPut, http.MethodPatch} {
		status, answered := s.writeConfig(method, "/v1/agents/configs/"+written.Id, slot, nil)

		s.Equal(http.StatusBadRequest, status, answered)
		s.Contains(s.failureMessage(answered), `of the agent `+strconv.Quote(slot)+" can send data out")
	}
}

// A child with two parents names the older one when it is refused for having one.
func (s *ChildAgentsSuite) TestAChildWithTwoParentsNamesTheOlderOne() {
	child := s.textConfig("echo/echo-model")
	older, newer := s.agentWith("fanning/fanning-model", nil), s.agentWith("fanning/fanning-model", nil)
	s.calls(older, child.Name)
	s.calls(newer, child.Name)

	status, payload := s.serverClient.call(http.MethodDelete, "/v1/agents/configs/"+child.Id, nil)

	s.Equal(http.StatusConflict, status, string(payload))
	s.Contains(s.failureMessage(string(payload)), strconv.Quote(older.Name)+" hands work to this agent")
}

// calls makes parent hand work to each of children by name, past the API's checks, as a set
// saved before the rule held across agents is stored, and one named before it exists.
func (s *ChildAgentsSuite) calls(parent AgentConfig, children ...string) {
	s.change(parent.Id, func(config *store.AgentConfig) {
		for i, child := range children {
			config.Agents = append(config.Agents, store.ChildAgent{Config: child, Tool: "ask_" + strconv.Itoa(i)})
		}
	})
}

// writeConfig sends a text agent on echo called name, with fields, and answers the status and
// the body.
func (s *ChildAgentsSuite) writeConfig(method, path, name string, fields map[string]any) (int, string) {
	request := map[string]any{"name": name, "mode": "text", "llm": "echo/echo-model"}
	maps.Copy(request, fields)
	status, payload := s.serverClient.call(method, path, request)
	return status, string(payload)
}

// failureMessage is the message of the failure answered.
func (s *ChildAgentsSuite) failureMessage(answered string) string {
	var failure ErrorResponse
	s.Require().NoError(json.Unmarshal([]byte(answered), &failure), answered)
	return failure.Error.Message
}
