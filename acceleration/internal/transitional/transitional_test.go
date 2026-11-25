package transitional

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestASwitchIsOffUntilItsVariableSaysOn(t *testing.T) {
	change := Switch{Name: "security.test.off", Env: "ROUTER_SECURITY_TEST_OFF"}

	require.False(t, change.Applies("app-1", "test"))
	require.InDelta(t, 1, testutil.ToFloat64(wouldChange.WithLabelValues(change.Name, "app-1", OutcomeWouldChange)), 0)
}

func TestASwitchIsOnWhenItsVariableSaysSo(t *testing.T) {
	change := Switch{Name: "security.test.on", Env: "ROUTER_SECURITY_TEST_ON"}
	t.Setenv(change.Env, "true")

	require.True(t, change.Applies("app-1", "test"))
	require.InDelta(t, 1, testutil.ToFloat64(wouldChange.WithLabelValues(change.Name, "app-1", OutcomeEnforced)), 0)
}

func TestASwitchListingAppsIsOnForThoseAlone(t *testing.T) {
	change := Switch{Name: "security.test.listed", Env: "ROUTER_SECURITY_TEST_LISTED"}
	t.Setenv(change.Env, "app-1, app-2")

	require.True(t, change.On("app-2"))
	require.False(t, change.On("app-3"))
	require.False(t, change.On(""))
}

func TestASwitchEnabledForAnAppIsOnForItUntilUndone(t *testing.T) {
	change := Switch{Name: "security.test.forced", Env: "ROUTER_SECURITY_TEST_FORCED"}

	undo := change.EnableForApp("app-1")
	require.True(t, change.On("app-1"))
	require.False(t, change.On("app-2"))
	undo()
	require.False(t, change.On("app-1"))
}
