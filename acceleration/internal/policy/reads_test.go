package policy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Defect: a router with no enforcer (no database) or no usage service read as funded false, or
// as a nil enforcer's panic, rather than as nothing read.
func TestFundedIsNotReadWithNoEnforcerOrUsageService(t *testing.T) {
	for name, enforcer := range map[string]*Enforcer{"no enforcer": nil, "no usage service": {}} {
		funded, ok, err := enforcer.Funded(t.Context(), []string{"org-1"})
		require.NoError(t, err, name)
		require.False(t, ok, name)
		require.Nil(t, funded, name)
	}
}
