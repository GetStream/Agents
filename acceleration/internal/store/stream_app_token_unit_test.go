package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamAppTokenValidate(t *testing.T) {
	valid := StreamAppToken{StreamAppPK: 7, CustomerID: "app-1", APIKey: "key", Sealed: []byte("sealed"), ExpiresAt: time.Now()}
	require.NoError(t, valid.Validate())
	placeholder := valid
	placeholder.Sealed = []byte{}
	require.NoError(t, placeholder.Validate(), "an app not yet minted for is kept with an empty token")

	for field, broken := range map[string]func(*StreamAppToken){
		"stream_app_pk": func(s *StreamAppToken) { s.StreamAppPK = 0 },
		"customer_id":   func(s *StreamAppToken) { s.CustomerID = "" },
		"api_key":       func(s *StreamAppToken) { s.APIKey = "" },
		"token_sealed":  func(s *StreamAppToken) { s.Sealed = nil },
		"expires_at":    func(s *StreamAppToken) { s.ExpiresAt = time.Time{} },
	} {
		token := valid
		broken(&token)
		var invalid *ValidationError
		require.True(t, errors.As(token.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
