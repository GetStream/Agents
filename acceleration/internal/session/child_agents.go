package session

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/agent"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/sensitive"
	"github.com/GetStream/Vision-Agents/acceleration/internal/harness"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// maxChildrenAtOnce is how many child agents one session runs at once. A call past it waits
// for one to finish, within its own deadline.
const maxChildrenAtOnce = 8

// defaultChildTimeout is how long a call to a child agent may take when its entry names no
// timeout: as long as a run that names none.
const defaultChildTimeout = 2 * time.Minute

// childInput is the one argument a child agent's tool takes.
const childInput = "input"

// childAgents offers a text session the app's other text agents its config names, each as a
// tool. A call runs the child on its input as an incognito run of its own and answers with
// what the child said last. A name any other tool has stays that tool's.
//
// A child's run is held to the parent's: it ends when the parent's tool call does, and never
// after the parent's RunDeadline. It takes none of the app's run slots: the parent's slot
// covers it, and maxChildrenAtOnce bounds how many one parent holds.
func (m *Manager) childAgents(spec Spec, parent *Session, declared []harness.Tool, next agent.ToolRunner) ([]harness.Tool, agent.ToolRunner) {
	if !spec.Text || len(spec.Agents) == 0 || m.options.Store == nil {
		return declared, next
	}
	// The agent offers these itself, past the tools it is handed.
	taken := map[string]bool{harness.LoadSkillTool: true, sandbox.ToolName: true, "confirm_tool_call": true}
	for _, tool := range declared {
		taken[tool.Name] = true
	}
	runner := &childRunner{
		manager: m, parent: parent, spec: spec, next: next,
		children: map[string]store.ChildAgent{}, slots: make(chan struct{}, maxChildrenAtOnce),
	}
	for _, child := range spec.Agents {
		if taken[child.Tool] {
			m.logger.Warn("a child agent's tool has the name of another tool, which keeps it",
				"config", spec.ConfigID, "tool", child.Tool)
			continue
		}
		taken[child.Tool] = true
		runner.children[child.Tool] = child
		declared = append(declared, harness.Tool{
			Name: child.Tool,
			Description: cmp.Or(strings.TrimSpace(child.Description),
				"Hands work to the "+child.Config+" agent and answers with what it said."),
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{childInput: map[string]any{
					"type":        "string",
					"description": "Everything the agent needs for the work. It sees nothing else of this conversation.",
				}},
				"required": []string{childInput},
			},
		})
	}
	if len(runner.children) == 0 {
		return declared, next
	}
	return declared, runner
}

// childRunner runs the child agents childAgents offered, and hands every other tool on.
type childRunner struct {
	manager  *Manager
	parent   *Session
	spec     Spec
	next     agent.ToolRunner
	children map[string]store.ChildAgent
	slots    chan struct{}
}

func (r *childRunner) Run(ctx context.Context, call llm.ToolCall) ([]llm.ContentPart, error) {
	child, ok := r.children[call.Name]
	if !ok {
		return r.next.Run(ctx, call)
	}
	var arguments map[string]any
	_ = json.Unmarshal([]byte(call.Arguments), &arguments)
	input, _ := arguments[childInput].(string)
	if strings.TrimSpace(input) == "" {
		return nil, fmt.Errorf("%s was called with no %s", call.Name, childInput)
	}

	timeout := defaultChildTimeout
	if child.TimeoutMs > 0 {
		timeout = time.Duration(child.TimeoutMs) * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	if !r.spec.RunDeadline.IsZero() && r.spec.RunDeadline.Before(deadline) {
		deadline = r.spec.RunDeadline
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	case <-ctx.Done():
		return nil, childEnded(call.Name, ctx.Err())
	}

	config, err := r.manager.options.Store.AgentConfigs.GetByName(ctx, r.spec.CustomerID, child.Config)
	if err != nil && !errors.Is(err, store.ErrNoAgentConfig) {
		return nil, err
	}
	// Checked again as the child runs, since it may have changed since the parent was saved.
	switch {
	case err != nil:
		return nil, fmt.Errorf("the agent %q behind %s no longer exists", child.Config, call.Name)
	case config.Mode != store.AgentModeText:
		return nil, fmt.Errorf("the agent %q behind %s is no text agent", child.Config, call.Name)
	case len(config.Agents) > 0:
		return nil, fmt.Errorf("the agent %q behind %s calls agents of its own", child.Config, call.Name)
	}
	if message, ok, err := r.sensitiveComplaint(ctx, config); err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("%s was not run: %s", call.Name, message)
	}

	spec := FromConfig(config)
	spec.CustomerID = r.spec.CustomerID
	spec.Caller, spec.CallerKind = r.spec.Caller, r.spec.CallerKind
	// What the parent's conversation is, as the router opened it, goes on to the child: a child of a
	// thread everyone writes in is as shared as the thread (Spec.Shared), so it attaches no one
	// person's connection. Dispatched and HandedOutBy only mark the child as the same hand-out as its
	// parent; nothing reads them for a child today.
	spec.Thread, spec.Dispatched, spec.HandedOutBy = r.spec.Thread, r.spec.Dispatched, r.spec.HandedOutBy
	spec.MaxToolRounds = child.MaxToolRounds
	// The parent's cost labels go on the child's requests too, so its spend is counted
	// where the parent's is.
	if len(r.spec.Tags) > 0 {
		tags := routing.Tags{}
		maps.Copy(tags, spec.Tags)
		maps.Copy(tags, r.spec.Tags)
		spec.Tags = tags
	}

	run, err := r.manager.Run(ctx, spec, input, nil)
	// Spent is spent, whether or not the child answered.
	r.parent.broadcast(ChildRan{Tool: call.Name, Usage: run.Usage})
	switch {
	case err != nil:
		return nil, err
	case ctx.Err() != nil:
		return nil, childEnded(call.Name, ctx.Err())
	case run.Failed != nil:
		return nil, run.Failed
	case run.FinalText == "":
		return nil, fmt.Errorf("%s gave no answer", call.Name)
	}
	return llm.TextParts(run.FinalText), nil
}

// sensitiveComplaint is sensitive.GraphComplaint over the parent as it runs, called and every
// other child the parent offers as the store holds it now: a stored graph may break the rule,
// having been saved before the rule held across agents or changed since, as a child can be.
func (r *childRunner) sensitiveComplaint(ctx context.Context, called store.AgentConfig) (string, bool, error) {
	configs := []store.AgentConfig{{
		Name: r.spec.AgentName, Connectors: r.spec.ConnectorBindings,
		Sandbox: store.AgentSandbox{Provider: r.spec.Sandbox, Options: r.spec.SandboxOptions},
	}, called}
	read := map[string]bool{called.Name: true}
	for _, child := range r.spec.Agents {
		if _, offered := r.children[child.Tool]; !offered || read[child.Config] {
			continue
		}
		read[child.Config] = true
		config, err := r.manager.options.Store.AgentConfigs.GetByName(ctx, r.spec.CustomerID, child.Config)
		// A child deleted since runs nothing, as its own call says.
		if errors.Is(err, store.ErrNoAgentConfig) {
			continue
		}
		if err != nil {
			return "", false, err
		}
		configs = append(configs, config)
	}
	return sensitive.GraphComplaint(ctx, r.manager.options.Store.ConnectorDefinitions, r.spec.CustomerID, configs)
}

// childEnded is a child given up on before it answered.
func childEnded(tool string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return endedError{message: tool + " did not finish in the time it had", cause: err}
	}
	return endedError{message: tool + " was stopped before it finished", cause: err}
}

// endedError says what the model reads and keeps why the child ended, so the tool call's
// line names a timeout or a cancellation rather than a plain error.
type endedError struct {
	message string
	cause   error
}

func (e endedError) Error() string { return e.message }

func (e endedError) Unwrap() error { return e.cause }
