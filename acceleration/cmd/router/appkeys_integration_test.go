//go:build integration

package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing/fstest"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/bearer"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// bearerConnector is a connector whose one scheme is bearer, so a connection holds a token.
const bearerConnector = `
id: datakeys
revision: 1
name: Data keys
category: Testing
endpoints:
  mcp: https://mcp.datakeys.example/mcp
schemes: [bearer]
sources:
  - kind: mcp
    endpoint: mcp
`

// TestTheResolverOpensCredentialsUnderAnAppsDataKeyThroughCloudKMS: the router's own wiring,
// from auth.app_keys_kms_key to the resolver, opens a token sealed under the app's data key.
func (s *ConnectorSealerSuite) TestTheResolverOpensCredentialsUnderAnAppsDataKeyThroughCloudKMS() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	ctx := s.T().Context()
	db, err := store.Open(testenv.Database(dsn, "router_appkeys"))
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })
	s.Require().NoError(db.Migrate(ctx))
	s.Require().NoError(db.ConnectorDefinitions.Seed(ctx, fstest.MapFS{"datakeys.yaml": {Data: []byte(bearerConnector)}}))
	fake := s.appKeysOn()
	keys, closeKeys, err := newAppKeys(ctx, s.settings, db)
	s.Require().NoError(err)
	s.T().Cleanup(closeKeys)
	registry, err := newConnectorRegistry(s.settings, nil)
	s.Require().NoError(err)

	app := "app-" + rand.Text()
	connection := &store.ConnectorConnection{CustomerID: app, ConnectorID: "datakeys", DefinitionRevision: 1,
		OwnerType: store.OwnerApp, AuthScheme: bearer.Name}
	s.Require().NoError(db.ConnectorConnections.Insert(ctx, registry, connection))
	// pgsealed's credentialsAAD, written out so a change to it that stored rows cannot follow fails here.
	aad := fmt.Appendf(nil, "accelerate:connector-material:v1:%d:%s:%d:%s:%d", len(app), app, len(connection.ID), connection.ID, 1)
	blob := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), db, fake.kms.KMS, app, 1),
		[]byte(`{"scheme":"bearer","version":1,"payload":{"token":"data-key-token"}}`), aad)
	_, err = db.GlobalDB().ExecContext(ctx, "UPDATE connector_connections SET status = ?, credentials_sealed = ?,"+
		" credentials_data_key_version = 1 WHERE id = ?", store.ConnectionConnected, blob, connection.ID)
	s.Require().NoError(err)

	sealer, err := auth.NewSealerWithKeyring(1, map[int]string{1: "data key wiring test key"})
	s.Require().NoError(err)
	resolver, err := newConnectorResolver(registry, db, sealer, keys)
	s.Require().NoError(err)
	credential, err := resolver.Resolve(ctx, core.ConnectionRef{CustomerID: app, ConnectionID: connection.ID}, core.CredentialRequest{})
	s.Require().NoError(err)
	s.JSONEq(`{"token":"data-key-token"}`, string(credential.Secret()))
	s.Equal(int64(1), fake.decrypts.Load())
}
