//go:build integration

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing/fstest"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/apikey"
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

// TestARunningRouterOpensCredentialsUnderAnAppsDataKey: run() hands the data keys to both the
// resolver and the API server. A token export goes through the resolver and a credentials
// write through the server's own credential store, and each opens the stored key with one
// Decrypt. A router that left the keys out of either answers that request with a 503.
func (s *RouterStartSuite) TestARunningRouterOpensCredentialsUnderAnAppsDataKey() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	fake := serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey = testKMSKey
	customer := "customer-" + rand.Text()
	connector := store.CustomPrefix + "datakeys" + strings.ToLower(rand.Text())
	manifest, err := core.ParseManifest([]byte(`
id: ` + connector + `
revision: 1
name: Data keys
endpoints:
  mcp: https://mcp.datakeys.example/mcp
schemes: [api_key]
sources:
  - kind: mcp
    endpoint: mcp
`))
	s.Require().NoError(err)
	_, err = s.db.ConnectorDefinitions.Revise(s.ctx, customer, manifest)
	s.Require().NoError(err)
	registry, err := newConnectorRegistry(settings, nil)
	s.Require().NoError(err)
	connection := &store.ConnectorConnection{CustomerID: customer, ConnectorID: connector, DefinitionRevision: 1,
		OwnerType: store.OwnerApp, AuthScheme: apikey.Name}
	s.Require().NoError(s.db.ConnectorConnections.Insert(s.ctx, registry, connection))
	// pgsealed's credentialsAAD, written out as in the resolver test above.
	aad := fmt.Appendf(nil, "accelerate:connector-material:v1:%d:%s:%d:%s:%d",
		len(customer), customer, len(connection.ID), connection.ID, 1)
	blob := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, fake.kms.KMS, customer, 1),
		[]byte(`{"scheme":"api_key","version":1,"payload":{"header":"X-Api-Key","key":"data-key-secret"}}`), aad)
	_, err = s.db.GlobalDB().ExecContext(s.ctx, "UPDATE connector_connections SET status = ?, credentials_sealed = ?,"+
		" credentials_data_key_version = 1 WHERE id = ?", store.ConnectionConnected, blob, connection.ID)
	s.Require().NoError(err)
	s.start(settings)
	path := "http://" + settings.Addr + "/v1/agents/connections/" + connection.ID

	status, body := s.ask(http.MethodPost, path+"/token", customer, nil)
	s.Require().Equal(http.StatusOK, status, "the resolver opens the stored key: %s", body)
	var token struct{ Value string }
	s.Require().NoError(json.Unmarshal(body, &token))
	s.Equal("data-key-secret", token.Value)
	s.Equal(int64(1), fake.decrypts.Load())

	status, body = s.ask(http.MethodPut, path+"/credentials", customer, map[string]any{"expected_revision": 1,
		"values": map[string]string{apikey.SuppliedKey: "new-secret", apikey.SuppliedHeader: "X-Api-Key"}})
	s.Require().Equal(http.StatusOK, status, "the server's credential store opens the stored key: %s", body)
	s.Equal(int64(2), fake.decrypts.Load())
}

// ask sends a request as customer's backend and returns the status and body of the answer.
func (s *RouterStartSuite) ask(method, url, customer string, body any) (int, []byte) {
	var sent bytes.Buffer
	if body != nil {
		s.Require().NoError(json.NewEncoder(&sent).Encode(body))
	}
	request, err := http.NewRequest(method, url, &sent)
	s.Require().NoError(err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Customer-Id", customer)
	response, err := http.DefaultClient.Do(request)
	s.Require().NoError(err)
	defer response.Body.Close()
	var answer bytes.Buffer
	_, err = answer.ReadFrom(response.Body)
	s.Require().NoError(err)
	return response.StatusCode, answer.Bytes()
}
