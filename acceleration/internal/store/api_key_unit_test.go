package store

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
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

// A database nobody can reach is not a key nobody issued: the lookup says the database
// failed, so the caller is not told their credential is wrong.
func TestALookupTheDatabaseCannotAnswerIsNotAnUnknownKey(t *testing.T) {
	broken, err := Open("postgres://router_app:hunter2@127.0.0.1:1/agents_prod?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(func() { _ = broken.Close() })
	key, _, err := auth.NewCredential(auth.Live)
	require.NoError(t, err)

	_, err = broken.APIKeys.Lookup(nil)(context.Background(), key)

	require.Error(t, err)
	require.NotErrorIs(t, err, auth.ErrUnauthenticated)
	require.ErrorContains(t, err, "127.0.0.1:1")
}
