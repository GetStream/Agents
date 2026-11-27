//go:build integration

package session

import (
	"context"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/llmtest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/metered"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
)

// askKid is the tool the parent reaches its child by.
const askKid = "ask_kid"

// childProbeLLM answers every request, and on the parent's first one calls the child. For each
// request it keeps the tools offered, and the spec of the child's session when the request is the
// child's, read from the manager while the child runs.
type childProbeLLM struct {
	mu       sync.Mutex
	manager  *Manager
	childID  string
	asked    []probed
	askedKid bool
}

// probed is one request the model was asked, by the session of the config it ran for.
type probed struct {
	tools []string
	child *Spec
}

func (p *childProbeLLM) Start(context.Context) error { return nil }

func (p *childProbeLLM) Create(_ context.Context, params llm.ResponseParams) (*llm.Stream, error) {
	var tools []string
	for _, tool := range params.Tools {
		tools = append(tools, tool.Name)
	}
	request := probed{tools: tools}
	p.manager.mu.Lock()
	for _, held := range p.manager.sessions {
		if held.spec.ConfigID == p.childID {
			spec := held.spec
			request.child = &spec
		}
	}
	p.manager.mu.Unlock()

	p.mu.Lock()
	p.asked = append(p.asked, request)
	calls := request.child == nil && !p.askedKid && slices.Contains(tools, askKid)
	p.askedKid = p.askedKid || calls
	p.mu.Unlock()

	script := llmtest.New(llm.StreamOptions{ResponseID: params.ID, Provider: p.Provider(), Model: p.Model()})
	script.OutputText("Done.")
	if calls {
		script.ToolCalls(llm.ToolCall{ID: uuid.NewString(), Name: askKid, Arguments: `{"input":"look into it"}`})
	}
	script.Done()
	return script.Stream(), nil
}

func (p *childProbeLLM) Provider() string               { return "stub" }
func (p *childProbeLLM) Model() string                  { return "stub-llm" }
func (p *childProbeLLM) Capabilities() llm.Capabilities { return llm.Capabilities{} }
func (p *childProbeLLM) Close() error                   { return nil }

// child is what the model was asked as the child agent's session: the last such request.
func (p *childProbeLLM) child() (probed, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, request := range slices.Backward(p.asked) {
		if request.child != nil {
			return request, true
		}
	}
	return probed{}, false
}

// ChildConversationSuite is what a child agent a session hands work to keeps of the
// conversation it was handed from: who it answers for, and whether the conversation is one
// person's or everyone's in a thread.
type ChildConversationSuite struct {
	connectorFixture
	model *childProbeLLM
	child store.AgentConfig
}

func TestChildConversationSuite(t *testing.T) {
	suite.Run(t, new(ChildConversationSuite))
}

func (s *ChildConversationSuite) SetupTest() {
	s.connectorFixture.SetupTest()
	logger := slog.New(slog.DiscardHandler)
	transports, err := core.NewTransports(core.TransportsConfig{Resolver: s.resolver, Timeout: fixtureRequestTimeout,
		NewClient: func(timeout time.Duration, wrap func(http.RoundTripper) http.RoundTripper) *http.Client {
			return &http.Client{Transport: wrap(s.provider.Client().Transport), Timeout: timeout}
		}})
	s.Require().NoError(err)
	s.model = &childProbeLLM{}
	reasoning := llmrouter.NewRegistry()
	reasoning.Register("stub", func(routing.Spec) (llmrouter.Provider, error) { return s.model, nil })
	reasoner, err := llmrouter.New(llmrouter.Options{Config: stubConfig(), Registry: reasoning, Logger: logger})
	s.Require().NoError(err)
	s.T().Cleanup(reasoner.Close)
	manager, err := NewManager(ManagerOptions{
		LLM: reasoner, Store: s.store, Logger: logger,
		Connectors: Connectors{Registry: s.registry, Transports: transports},
		Sandboxes:  metered.NewSandboxes(metered.SandboxesOptions{Logger: logger}),
		Edge: func(context.Context, Spec, streamapp.Bound, *slog.Logger) (agent.Edge, error) {
			return newQuietEdge(), nil
		},
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.Require().NoError(manager.Shutdown()) })
	s.manager, s.model.manager = manager, manager

	// The child is a text agent that binds the connector as the caller's own.
	s.child = store.AgentConfig{CustomerID: s.customerID, Name: "kid-" + uuid.NewString(), Mode: store.AgentModeText,
		Models: store.AgentModels{LLM: "en-low-latency"}, Connectors: []store.ConnectorBinding{s.chosen("crm", "whoami")}}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, &s.child))
	s.model.childID = s.child.ID
}

// handsOff opens a text session of the child's parent for alice, changed by shape, has it hand
// work to the child, and returns what the child's model was asked once the parent has the answer.
func (s *ChildConversationSuite) handsOff(shape func(*Spec)) probed {
	spec := Spec{
		CustomerID: s.customerID, Text: true, LLMTarget: "en-low-latency",
		Caller: routing.Caller{UserID: "alice"}, CallerKind: auth.KindAuthenticated,
		Agents: []store.ChildAgent{{Config: s.child.Name, Tool: askKid}},
	}
	shape(&spec)
	parent, err := s.manager.Create(s.ctx, spec)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = parent.Close() })

	_, err = parent.Respond(s.ctx, "Ask the kid.", nil, options.LLM{})
	s.Require().NoError(err)
	// The parent asks again once the child answered, with its answer.
	s.Require().Eventually(func() bool {
		s.model.mu.Lock()
		defer s.model.mu.Unlock()
		replies := 0
		for _, request := range s.model.asked {
			if request.child == nil && slices.Contains(request.tools, askKid) {
				replies++
			}
		}
		return replies >= 2
	}, 10*time.Second, 10*time.Millisecond, "the parent never read the child's answer")
	asked, ok := s.model.child()
	s.Require().True(ok, "the child was never asked anything")
	return asked
}

// TestAChildOfABackendOpenedThreadUsesNoOnesConnection: a backend opens a thread for one of its
// users (api.threadConversation): Thread with a server-side caller naming alice. The child gets none
// of alice's connection either.
func (s *ChildConversationSuite) TestAChildOfABackendOpenedThreadUsesNoOnesConnection() {
	s.connection("alice", "primary")

	asked := s.handsOff(func(spec *Spec) { spec.Thread, spec.CallerKind = true, auth.KindServer })

	s.NotContains(asked.tools, "crm__whoami", "the child of a backend-opened thread was handed alice's connection")
}

// TestAThreadChildKeepsTheAppsOwnConnection: sharing takes away a person's connection, not the app's
// own, so a child bound to the app's connection keeps it in a thread.
func (s *ChildConversationSuite) TestAThreadChildKeepsTheAppsOwnConnection() {
	app := s.connection("", "primary")
	s.child = store.AgentConfig{CustomerID: s.customerID, Name: "kid-" + uuid.NewString(), Mode: store.AgentModeText,
		Models: store.AgentModels{LLM: "en-low-latency"}, Connectors: []store.ConnectorBinding{s.fixed("crm", app, "whoami")}}
	s.Require().NoError(s.store.AgentConfigs.Insert(s.ctx, &s.child))
	s.model.childID = s.child.ID

	asked := s.handsOff(func(spec *Spec) { spec.Thread, spec.CallerKind = true, auth.KindServer })

	s.Contains(asked.tools, "crm__whoami", "the child of a thread lost the app's own connection")
}

// TestAChildOfAThreadEveryoneWritesInUsesNoOnesConnection: alice has one connected connection
// the child's binding would use for her. A thread's other participants' messages reach the
// parent as hers, so in a thread the child is offered none of it, as the parent is not.
func (s *ChildConversationSuite) TestAChildOfAThreadEveryoneWritesInUsesNoOnesConnection() {
	s.connection("alice", "primary")

	asked := s.handsOff(func(spec *Spec) { spec.Thread = true })

	s.NotContains(asked.tools, "crm__whoami", "the child was handed alice's connection in a thread everyone writes in")
	s.True(asked.child.Shared())
	s.True(asked.child.Thread)
}

// TestAChildOfAMembersSharedConversationUsesNoOnesConnection: in a conversation open to its
// members, what the child reads through alice's connection reaches every member.
func (s *ChildConversationSuite) TestAChildOfAMembersSharedConversationUsesNoOnesConnection() {
	s.connection("alice", "primary")

	asked := s.handsOff(func(spec *Spec) { spec.MembersShared = true })

	s.NotContains(asked.tools, "crm__whoami", "the child of a members-shared conversation was handed alice's connection")
	s.True(asked.child.MembersShared)
}

// TestAChildOfOnePersonsConversationKeepsTheirConnection is the control: outside a thread the
// child uses alice's connection, as it did before a child kept what its parent's conversation is.
func (s *ChildConversationSuite) TestAChildOfOnePersonsConversationKeepsTheirConnection() {
	s.connection("alice", "primary")

	asked := s.handsOff(func(*Spec) {})

	s.Contains(asked.tools, "crm__whoami", "alice's own connection reaches the child of her own conversation")
	s.False(asked.child.Shared())
	s.False(asked.child.Thread)
	s.False(asked.child.Dispatched)
	s.Empty(asked.child.HandedOutBy)
}

// TestAChildOfAHandedOutMessageIsMarkedAsTheSameHandOut: the child keeps Dispatched and
// HandedOutBy only to mark it as the same hand-out as its parent; nothing reads them for a child
// today. The parent's shape (dispatched with a verified caller) is no shape a router path builds.
func (s *ChildConversationSuite) TestAChildOfAHandedOutMessageIsMarkedAsTheSameHandOut() {
	asked := s.handsOff(func(spec *Spec) { spec.Dispatched, spec.HandedOutBy = true, "writer-1" })

	s.True(asked.child.Dispatched)
	s.Equal("writer-1", asked.child.HandedOutBy)
	s.False(asked.child.Thread, "a hand-out is no thread")
}
