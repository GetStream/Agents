//go:build integration

package api

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/bearer"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// ConnectorDataKeysSuite covers the server's own credential store: the one consents and
// credentials writes go through, apart from the resolver's.
type ConnectorDataKeysSuite struct {
	suite.Suite
	db *store.Store
}

func TestConnectorDataKeysSuite(t *testing.T) {
	suite.Run(t, new(ConnectorDataKeysSuite))
}

func (s *ConnectorDataKeysSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	db, err := store.Open(testenv.Database(dsn, "api_datakeys"))
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })
	s.Require().NoError(db.Migrate(s.T().Context()))
	s.Require().NoError(db.ConnectorDefinitions.Seed(s.T().Context(), fstest.MapFS{"datakeys.yaml": {Data: []byte(`
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
`)}}))
	s.db = db
}

func (s *ConnectorDataKeysSuite) TestTheServerOpensCredentialsUnderTheAppsDataKey() {
	ctx := s.T().Context()
	kms := appkeystest.NewKMS()
	keys, err := appkeys.New(s.db, kms)
	s.Require().NoError(err)
	sealer, err := auth.NewSealerWithKeyring(1, map[int]string{1: "server data keys test key"})
	s.Require().NoError(err)
	server, err := NewServer(Options{Routers: map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		Store: s.db, ConnectorSecrets: sealer, ConnectorDataKeys: keys})
	s.Require().NoError(err)

	app := "app-" + rand.Text()
	connection := &store.ConnectorConnection{CustomerID: app, ConnectorID: "datakeys", DefinitionRevision: 1,
		OwnerType: store.OwnerApp, AuthScheme: bearer.Name}
	s.Require().NoError(s.db.ConnectorConnections.Insert(ctx, core.Registry{Schemes: map[string]core.Scheme{bearer.Name: &bearer.Scheme{}}}, connection))
	aad := fmt.Appendf(nil, "accelerate:connector-material:v1:%d:%s:%d:%s:%d", len(app), app, len(connection.ID), connection.ID, 1)
	blob := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, kms, app, 1),
		[]byte(`{"scheme":"bearer","version":1,"payload":{"token":"data-key-token"}}`), aad)
	_, err = s.db.GlobalDB().ExecContext(ctx, "UPDATE connector_connections SET status = ?, credentials_sealed = ?,"+
		" credentials_data_key_version = 1 WHERE id = ?", store.ConnectionConnected, blob, connection.ID)
	s.Require().NoError(err)

	var seen core.CredentialState
	s.Require().NoError(server.credentials.Update(ctx, core.ConnectionRef{CustomerID: app, ConnectionID: connection.ID},
		func(state *core.CredentialState, _ func() error) (bool, error) {
			seen = *state
			return false, nil
		}))
	s.JSONEq(`{"token":"data-key-token"}`, string(seen.Credentials.Payload))
	s.Equal(store.ConnectionConnected, seen.Status)
}
