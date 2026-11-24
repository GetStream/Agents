package store

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
)

// testScheme is a scheme that is registered under a name and does nothing else: the store
// asks only whether a name is registered. No real scheme is registered on accelerate yet.
type testScheme string

func (n testScheme) Name() string { return string(n) }

func (testScheme) Begin(context.Context, core.BeginInput) (core.BeginOutput, error) {
	return core.BeginOutput{Done: true}, nil
}

func (testScheme) Complete(context.Context, core.CompleteInput) (core.StoredCredentials, core.AccountInfo, error) {
	return core.StoredCredentials{}, core.AccountInfo{}, nil
}

func (testScheme) Retrieve(_ context.Context, stored core.StoredCredentials, _ core.ResolvedManifest, _ core.RetrieveOptions) (core.AccessCredential, core.StoredCredentials, error) {
	return core.AccessCredential{}, stored, nil
}

func (testScheme) Wrap(base http.RoundTripper, _ core.AccessCredential) http.RoundTripper {
	return base
}

func (testScheme) Classify(*http.Response, []byte, error) core.Outcome { return core.Outcome{} }

func (testScheme) Revoke(context.Context, core.StoredCredentials, core.ResolvedManifest) error {
	return nil
}

// testSchemes is the registry the tests create connections against. acme lists test_key
// and test_mtls; test_unlisted is registered but listed by no test manifest.
var testSchemes = core.Registry{Schemes: map[string]core.Scheme{
	"test_key":      testScheme("test_key"),
	"test_mtls":     testScheme("test_mtls"),
	"test_unlisted": testScheme("test_unlisted"),
}}

// appConnection is an app-owned connection to the acme test built-in at revision 1.
func appConnection() *ConnectorConnection {
	return &ConnectorConnection{
		CustomerID:         "acme-app",
		ConnectorID:        "acme",
		DefinitionRevision: 1,
		OwnerType:          OwnerApp,
		AuthScheme:         "test_key",
	}
}

func TestConnectorConnectionValidate(t *testing.T) {
	valid := func() ConnectorConnection {
		connection := *appConnection()
		connection.Status, connection.Revision = ConnectionPending, 1
		return connection
	}
	require.NoError(t, (&ConnectorConnection{CustomerID: "acme-app", ConnectorID: "acme", DefinitionRevision: 2,
		OwnerType: OwnerUser, OwnerID: "alice", AuthScheme: "test_key", Status: ConnectionConnected, Revision: 3}).Validate())

	for _, tc := range []struct {
		field  string
		broken func(*ConnectorConnection)
	}{
		{"customer_id", func(c *ConnectorConnection) { c.CustomerID = "" }},
		{"connector_id", func(c *ConnectorConnection) { c.ConnectorID = " " }},
		{"definition_revision", func(c *ConnectorConnection) { c.DefinitionRevision = 0 }},
		{"owner_type", func(c *ConnectorConnection) { c.OwnerType = "team" }},
		{"owner_id", func(c *ConnectorConnection) { c.OwnerID = "alice" }},
		{"owner_id", func(c *ConnectorConnection) { c.OwnerType = OwnerUser }},
		{"auth_scheme", func(c *ConnectorConnection) { c.AuthScheme = "" }},
		{"status", func(c *ConnectorConnection) { c.Status = "revoked" }},
		{"revision", func(c *ConnectorConnection) { c.Revision = 0 }},
	} {
		connection := valid()
		tc.broken(&connection)
		var invalid *ValidationError
		require.True(t, errors.As(connection.Validate(), &invalid), tc.field)
		require.Equal(t, tc.field, invalid.Field)
	}
}

func TestAConnectionWithAnUnregisteredAuthSchemeIsRefused(t *testing.T) {
	connection := appConnection()
	connection.AuthScheme = "oauth2_code"

	err := (&Store{}).ConnectorConnections.Insert(context.Background(), testSchemes, connection)

	require.ErrorIs(t, err, ErrUnregisteredScheme)
	require.ErrorContains(t, err, `"oauth2_code"`)
}

func TestAnEmptyRegistryRegistersNoScheme(t *testing.T) {
	err := (&Store{}).ConnectorConnections.Insert(context.Background(), core.Registry{}, appConnection())

	require.ErrorIs(t, err, ErrUnregisteredScheme)
}

func TestAConnectionIsNotCreatedWithCredentials(t *testing.T) {
	connection := appConnection()
	connection.CredentialsSealed = []byte("sealed elsewhere")

	err := (&Store{}).ConnectorConnections.Insert(context.Background(), testSchemes, connection)

	require.ErrorContains(t, err, "after it exists")
}

func TestListingConnectionsNeedsAValidOwner(t *testing.T) {
	_, err := (&Store{}).ConnectorConnections.Select(context.Background(), "acme-app", ConnectionFilter{OwnerType: OwnerUser})

	require.ErrorContains(t, err, "a user owner has one")
}

func TestEveryUsersConnectionsNameNoOwner(t *testing.T) {
	for _, filter := range []ConnectionFilter{
		{OwnerType: OwnerUser, OwnerID: "alice", EveryUser: true},
		{OwnerType: OwnerApp, EveryUser: true},
	} {
		_, err := (&Store{}).ConnectorConnections.Select(context.Background(), "acme-app", filter)

		require.ErrorContains(t, err, "every user's connections are owner", "%+v", filter)
	}
}
