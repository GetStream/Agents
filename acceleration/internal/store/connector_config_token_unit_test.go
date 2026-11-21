package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConnectorConfigTokenValidate(t *testing.T) {
	valid := func() ConnectorConfigToken {
		return ConnectorConfigToken{CustomerID: "acme-app", ConnectorID: "slack", TokensSealed: []byte("sealed"),
			KEKVersion: 1, ExpiresAt: time.Now().Add(12 * time.Hour)}
	}
	token := valid()
	require.NoError(t, token.Validate())

	for field, broken := range map[string]func(*ConnectorConfigToken){
		"customer_id":   func(c *ConnectorConfigToken) { c.CustomerID = "" },
		"connector_id":  func(c *ConnectorConfigToken) { c.ConnectorID = "" },
		"tokens_sealed": func(c *ConnectorConfigToken) { c.TokensSealed = []byte{} },
		"kek_version":   func(c *ConnectorConfigToken) { c.KEKVersion = 0 },
		"expires_at":    func(c *ConnectorConfigToken) { c.ExpiresAt = time.Time{} },
	} {
		token := valid()
		broken(&token)
		var invalid *ValidationError
		require.True(t, errors.As(token.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
