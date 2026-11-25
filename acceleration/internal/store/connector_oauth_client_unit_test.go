package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
)

func TestConnectorOAuthClientValidate(t *testing.T) {
	valid := func() ConnectorOAuthClient {
		return ConnectorOAuthClient{CustomerID: "acme-app", ConnectorID: "acme_chat", Registration: core.ClientManaged,
			ClientID: "created", SecretSealed: []byte("sealed"), KEKVersion: 1, ProviderAppID: "A012ABCD0A0",
			SigningSecretSealed: []byte("signing"), SigningKEKVersion: 2}
	}
	require.NoError(t, (&ConnectorOAuthClient{CustomerID: "acme-app", ConnectorID: "github", Registration: core.ClientCustomer,
		ClientID: "id"}).Validate(), "a public client has no secret")
	require.NoError(t, (&ConnectorOAuthClient{CustomerID: "acme-app", ConnectorID: "acme_chat", Registration: core.ClientOperator,
		ProviderAppID: "A012ABCD0A0", SigningSecretSealed: []byte("signing"), SigningKEKVersion: 1}).Validate(),
		"a provider app with a signing secret alone needs no client id")

	cases := []struct {
		field  string
		broken func(*ConnectorOAuthClient)
	}{
		{"customer_id", func(c *ConnectorOAuthClient) { c.CustomerID = "" }},
		{"connector_id", func(c *ConnectorOAuthClient) { c.ConnectorID = "" }},
		{"registration", func(c *ConnectorOAuthClient) { c.Registration = core.ClientDCR }},
		{"client_id", func(c *ConnectorOAuthClient) { c.ClientID, c.SigningSecretSealed, c.SigningKEKVersion = "", nil, 0 }},
		{"kek_version", func(c *ConnectorOAuthClient) { c.KEKVersion = 0 }},
		{"kek_version", func(c *ConnectorOAuthClient) { c.SecretSealed = nil }},
		{"provider_app_id", func(c *ConnectorOAuthClient) { c.ProviderAppID = "" }},
		{"signing_kek_version", func(c *ConnectorOAuthClient) { c.SigningKEKVersion = 0 }},
		{"provider_app_id", func(c *ConnectorOAuthClient) { c.Registration, c.ProviderAppID = core.ClientCustomer, "" }},
	}
	// RFC 3986: each is more than one path segment, escaped, or a dot segment.
	for _, id := range []string{"A0/../B0", "A0 B0", "A0%2FB0", ".", "..", "A0?x=1"} {
		cases = append(cases, struct {
			field  string
			broken func(*ConnectorOAuthClient)
		}{"provider_app_id", func(c *ConnectorOAuthClient) { c.ProviderAppID = id }})
	}
	for i, tc := range cases {
		client := valid()
		tc.broken(&client)
		var invalid *ValidationError
		require.True(t, errors.As(client.Validate(), &invalid), "case %d: %s", i, tc.field)
		require.Equal(t, tc.field, invalid.Field, "case %d", i)
	}
}
