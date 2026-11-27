//go:build integration

package main

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/api"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/llmtest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/metered"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/daytona/daytonatest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/session"
)

func TestTheSessionsSandboxesAreAdmittedByTheGate(t *testing.T) {
	money := policytest.Start(t, "router_sandboxes")
	server := daytonatest.Start(t)
	t.Setenv("DAYTONA_API_KEY", "key-1")
	t.Setenv("DAYTONA_API_URL", server.APIURL())
	owner := money.Owner(t, 1_000_000)
	owner.SessionID = "session-" + uuid.NewString()
	money.Usage.Suspend(t, owner.OrganizationID, "fraud")
	require.Eventually(t, func() bool {
		_, err := money.Enforcer.Admit(t.Context(), owner)
		return errors.Is(err, policy.ErrSuspended)
	}, 5*time.Second, 10*time.Millisecond, "the suspension is read")
	box, err := newSandboxes(config.Defaults(), money.Writer, nil, nil, money.Enforcer, slog.New(slog.DiscardHandler)).
		Open(t.Context(), owner, metered.SandboxSpec{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = box.Close() })

	_, err = box.Run(t.Context(), "print(1)", nil)

	require.ErrorIs(t, err, policy.ErrSuspended)
	require.Empty(t, server.Created(), "nothing is made on the deployment's Daytona")
}

// codeRunner is a model that answers the first request offering it tools by running code, as a
// model given run_code does, and every other with text.
type codeRunner struct {
	called atomic.Bool
}

func (m *codeRunner) Start(context.Context) error { return nil }

func (m *codeRunner) Create(_ context.Context, params llm.ResponseParams) (*llm.Stream, error) {
	script := llmtest.New(llm.StreamOptions{ResponseID: params.ID, Provider: m.Provider(), Model: m.Model()})
	if len(params.Tools) > 0 && params.ToolChoice != "none" && m.called.CompareAndSwap(false, true) {
		script.ToolCalls(llm.ToolCall{ID: "call-1", Name: sandbox.ToolName, Arguments: `{"code":"print(1)"}`})
	} else {
		script.OutputText("Done.")
	}
	script.Done()
	return script.Stream(), nil
}

func (m *codeRunner) Provider() string               { return "stub" }
func (m *codeRunner) Model() string                  { return "stub-llm" }
func (m *codeRunner) Capabilities() llm.Capabilities { return llm.Capabilities{} }
func (m *codeRunner) Close() error                   { return nil }

func TestTheSessionsBuiltForTheRouterRunCodeOnlyForAnOrganizationThatMayPay(t *testing.T) {
	money := policytest.Start(t, "router_session_sandboxes")
	server := daytonatest.Start(t)
	t.Setenv("DAYTONA_API_KEY", "key-1")
	t.Setenv("DAYTONA_API_URL", server.APIURL())
	logger := slog.New(slog.DiscardHandler)
	models := llmrouter.NewRegistry()
	models.Register("stub", func(routing.Spec) (llmrouter.Provider, error) { return &codeRunner{}, nil })
	model, err := llmrouter.New(llmrouter.Options{Config: routing.ModalityConfig{
		Providers: []routing.ProviderConfig{{Provider: "stub", Model: "stub-llm", Languages: []string{"en"}}},
		Aliases:   map[string]routing.Alias{"llm-fast": {Languages: []string{"en"}}},
	}, Registry: models, Logger: logger})
	require.NoError(t, err)
	t.Cleanup(model.Close)
	manager, err := buildSessions(config.Defaults(), &api.Streams{LLM: model}, nil, nil, nil, money.Writer, nil, nil, nil, nil, nil, nil, nil,
		session.Connectors{}, nil, money.Enforcer, nil, nil, nil, logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
	run := func(owner routing.Owner) {
		_, err := manager.Run(t.Context(), session.Spec{CustomerID: owner.CustomerID, Attribution: owner.Attribution,
			Sandbox: "daytona"}, "Work it out in code.", nil)
		require.NoError(t, err)
	}
	broke := money.Owner(t, 0)

	run(broke)

	require.Empty(t, server.Created(), "an organization with no credit has no sandbox made, though the model may run")
	run(money.Owner(t, 1_000_000))
	require.Equal(t, []string{"sandbox-1"}, server.Created(), "one that may pay has its code run")
}
