package store

import (
	"context"
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

func TestAConfigTokenResealWithoutBothBlobsAndAKeyVersionIsRefused(t *testing.T) {
	for name, reseal := range map[string]struct {
		was, sealed []byte
		kekVersion  int
	}{
		"no blob it read": {nil, []byte("sealed"), 1},
		"no new blob":     {[]byte("was"), nil, 1},
		"no key version":  {[]byte("was"), []byte("sealed"), 0},
	} {
		_, err := (&Store{}).ConnectorConfigTokens.Reseal(context.Background(), "acme-app", "slack", reseal.was, reseal.sealed,
			reseal.kekVersion, 1)

		require.ErrorContains(t, err, "a reseal replaces one sealed token with another", name)
	}
}
