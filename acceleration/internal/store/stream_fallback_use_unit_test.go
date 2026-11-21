package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamFallbackUseValidate(t *testing.T) {
	valid := StreamFallbackUse{CustomerID: "app-1", Uses: 1, LastAt: time.Now()}
	require.NoError(t, valid.Validate())

	for field, broken := range map[string]func(*StreamFallbackUse){
		"customer_id": func(u *StreamFallbackUse) { u.CustomerID = "" },
		"uses":        func(u *StreamFallbackUse) { u.Uses = 0 },
		"last_at":     func(u *StreamFallbackUse) { u.LastAt = time.Time{} },
	} {
		use := valid
		broken(&use)
		var invalid *ValidationError
		require.True(t, errors.As(use.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
