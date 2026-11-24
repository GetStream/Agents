//go:build integration

package main

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/metered"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy"
	"github.com/GetStream/Vision-Agents/acceleration/internal/policy/policytest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox/daytona/daytonatest"
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
