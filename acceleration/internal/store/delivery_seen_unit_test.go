package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeliverySeenValidate(t *testing.T) {
	cases := map[string]struct {
		seen  DeliverySeen
		field string
	}{
		"hook":             {seen: DeliverySeen{Scope: DeliveryScopeHook, Key: "k"}},
		"connection event": {seen: DeliverySeen{Scope: DeliveryScopeConnectionEvent, Key: ConnectionEventKey("sub", "evt")}},
		"unknown scope":    {seen: DeliverySeen{Scope: "channel", Key: "k"}, field: "scope"},
		"no scope":         {seen: DeliverySeen{Key: "k"}, field: "scope"},
		"no key":           {seen: DeliverySeen{Scope: DeliveryScopeHook}, field: "key"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.seen.Validate()
			if tc.field == "" {
				assert.NoError(t, err)
				return
			}
			var invalid *ValidationError
			require.ErrorAs(t, err, &invalid)
			assert.Equal(t, tc.field, invalid.Field)
		})
	}
}
