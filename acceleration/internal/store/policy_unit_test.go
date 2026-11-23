package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPolicyValidate(t *testing.T) {
	for _, scope := range []PolicyScope{ScopeOrganization, ScopeApp, ScopeUser} {
		require.NoError(t, (&Policy{Scope: scope, ScopeID: "id-1"}).Validate(), scope)
	}

	for name, test := range map[string]struct {
		policy Policy
		field  string
	}{
		"a global scope": {Policy{Scope: "global", ScopeID: "id-1"}, "scope"},
		"no scope":       {Policy{ScopeID: "id-1"}, "scope"},
		"no scope id":    {Policy{Scope: ScopeApp}, "scope_id"},
	} {
		var invalid *ValidationError
		require.True(t, errors.As(test.policy.Validate(), &invalid), name)
		require.Equal(t, test.field, invalid.Field, name)
	}
}
