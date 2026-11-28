package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppDataKeyValidateNamesTheFieldItRefuses(t *testing.T) {
	valid := AppDataKey{CustomerID: "app-1", Version: 1, WrappedKey: []byte("wrapped")}
	require.NoError(t, valid.Validate())

	for field, broken := range map[string]func(*AppDataKey){
		"customer_id": func(k *AppDataKey) { k.CustomerID = "" },
		"version":     func(k *AppDataKey) { k.Version = 0 },
		"wrapped_key": func(k *AppDataKey) { k.WrappedKey = nil },
	} {
		key := valid
		broken(&key)
		var invalid *ValidationError
		require.True(t, errors.As(key.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
