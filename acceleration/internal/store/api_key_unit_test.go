package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPIKeyValidate(t *testing.T) {
	valid := APIKey{ID: "key-1", AppID: "app-1", Sealed: []byte("sealed")}
	require.NoError(t, valid.Validate())

	for field, broken := range map[string]func(*APIKey){
		"id":            func(k *APIKey) { k.ID = "" },
		"app_id":        func(k *APIKey) { k.AppID = "" },
		"secret_sealed": func(k *APIKey) { k.Sealed = []byte{} },
	} {
		key := valid
		broken(&key)
		var invalid *ValidationError
		require.True(t, errors.As(key.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
