package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProviderKeyValidate(t *testing.T) {
	valid := ProviderKey{CustomerID: "app-1", Provider: "openai", Region: "global", APIKeySealed: []byte("sealed")}
	require.NoError(t, valid.Validate())

	for name, test := range map[string]struct {
		broken func(*ProviderKey)
		field  string
	}{
		"no customer":         {func(k *ProviderKey) { k.CustomerID = "" }, "customer_id"},
		"no provider":         {func(k *ProviderKey) { k.Provider = "" }, "provider"},
		"a provider with a /": {func(k *ProviderKey) { k.Provider = "open/ai" }, "provider"},
		"no region":           {func(k *ProviderKey) { k.Region = "" }, "region"},
		"a region with space": {func(k *ProviderKey) { k.Region = "us east" }, "region"},
		"no sealed key":       {func(k *ProviderKey) { k.APIKeySealed = nil }, "api_key_sealed"},
	} {
		key := valid
		test.broken(&key)
		var invalid *ValidationError
		require.True(t, errors.As(key.Validate(), &invalid), name)
		require.Equal(t, test.field, invalid.Field, name)
	}
}
