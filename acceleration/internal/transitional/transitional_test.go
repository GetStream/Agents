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
