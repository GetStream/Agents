//go:build integration

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing/fstest"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
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
// from auth.app_keys_kms_key to the resolver, opens a token sealed under the app's data key,
// and keeps the key it unwrapped.
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
	_, err = keys.Open(ctx, app, 1, blob, aad)
	s.Require().NoError(err)
	s.Equal(int64(1), fake.decrypts.Load(), "the key is held for auth.app_keys_ttl, so a second open asks KMS nothing")

	s.settings.Auth.AppKeysTTL = time.Nanosecond
	stale, closeStale, err := newAppKeys(ctx, s.settings, db)
	s.Require().NoError(err)
	s.T().Cleanup(closeStale)
	_, err = stale.Open(ctx, app, 1, blob, aad)
	s.Require().NoError(err)
	fake.kms.KMS.Fail(errors.New("kms: unavailable"))
	_, err = stale.Open(ctx, app, 1, blob, aad)
	s.NoError(err, "within auth.app_keys_stale_limit the key unwrapped before is used while KMS fails")
	s.Equal(int64(3), fake.decrypts.Load())
}

// TestARunningRouterOpensCredentialsUnderAnAppsDataKey: run() hands the data keys to both the
// resolver and the API server. A token export goes through the resolver and a credentials
// write through the server's own credential store. A router that left the keys out of either
// answers that request with a 503. Both share one Keys, so only the first open asks KMS.
func (s *RouterStartSuite) TestARunningRouterOpensCredentialsUnderAnAppsDataKey() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	fake := serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey = testKMSKey
	customer, connection := s.apiKeyConnection(settings)
	// pgsealed's credentialsAAD, written out as in the resolver test above.
	aad := fmt.Appendf(nil, "accelerate:connector-material:v1:%d:%s:%d:%s:%d",
		len(customer), customer, len(connection), connection, 1)
	blob := appkeystest.Seal(s.T(), appkeystest.AddKey(s.T(), s.db, fake.kms.KMS, customer, 1),
		[]byte(`{"scheme":"api_key","version":1,"payload":{"header":"X-Api-Key","key":"data-key-secret"}}`), aad)
	_, err := s.db.GlobalDB().ExecContext(s.ctx, "UPDATE connector_connections SET status = ?, credentials_sealed = ?,"+
		" credentials_data_key_version = 1 WHERE id = ?", store.ConnectionConnected, blob, connection)
	s.Require().NoError(err)
	s.start(settings)
	path := "http://" + settings.Addr + "/v1/agents/connections/" + connection

	s.Equal("data-key-secret", s.exported(path, customer), "the resolver opens the stored key")
	s.Equal(int64(1), fake.decrypts.Load())

	status, body := s.ask(http.MethodPut, path+"/credentials", customer, map[string]any{"expected_revision": 1,
		"values": map[string]string{apikey.SuppliedKey: "new-secret", apikey.SuppliedHeader: "X-Api-Key"}})
	s.Require().Equal(http.StatusOK, status, "the server's credential store opens the stored key: %s", body)
	s.Equal(int64(1), fake.decrypts.Load(), "the server uses the key the resolver unwrapped")
	s.Zero(fake.encrypts.Load(), "with auth.app_keys_seal off, nothing is sealed under a data key")
}

// TestARunningRouterWithSealingOnSealsNewCredentialsUnderTheAppsDataKey: a credentials write
// through the API makes the app's key with one Encrypt and marks the row. A token export then
// opens it through the resolver, with the key the write made and no Decrypt. A write while
// KMS refuses still succeeds, under the router's key.
func (s *RouterStartSuite) TestARunningRouterWithSealingOnSealsNewCredentialsUnderTheAppsDataKey() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	fake := serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey, settings.Auth.AppKeysSeal = testKMSKey, true
	customer, connection := s.apiKeyConnection(settings)
	s.start(settings)
	path := "http://" + settings.Addr + "/v1/agents/connections/" + connection
	write := func(revision int, secret string) map[string]any {
		return map[string]any{"expected_revision": revision,
			"values": map[string]string{apikey.SuppliedKey: secret, apikey.SuppliedHeader: "X-Api-Key"}}
	}

	fake.refusal = status.Error(codes.PermissionDenied, "permission denied on "+testKMSKey)
	answer, body := s.ask(http.MethodPut, path+"/credentials", customer, write(1, "kek-secret"))
	s.Require().Equal(http.StatusOK, answer, "a write while KMS refuses is sealed under the router's key: %s", body)
	s.NotContains(string(body), testKMSKey)
	stored, err := s.db.ConnectorConnections.Get(s.ctx, customer, connection)
	s.Require().NoError(err)
	s.Nil(stored.CredentialsDataKeyVersion)
	s.Equal("kek-secret", s.exported(path, customer))
	fake.refusal = nil
	answer, body = s.ask(http.MethodPut, path+"/credentials", customer, write(2, "sealed-secret"))
	s.Require().Equal(http.StatusOK, answer, "%s", body)
	stored, err = s.db.ConnectorConnections.Get(s.ctx, customer, connection)
	s.Require().NoError(err)
	s.Equal(new(1), stored.CredentialsDataKeyVersion)
	s.Equal(int64(2), fake.encrypts.Load())

	s.Equal("sealed-secret", s.exported(path, customer))
	s.Zero(fake.decrypts.Load())
	// Another replica unwraps the key the write stored, under the AAD it was wrapped with.
	other, closeOther, err := newAppKeys(s.ctx, settings, s.db)
	s.Require().NoError(err)
	s.T().Cleanup(closeOther)
	_, err = other.Open(s.ctx, customer, 1, stored.CredentialsSealed, fmt.Appendf(nil,
		"accelerate:connector-material:v1:%d:%s:%d:%s:%d", len(customer), customer, len(connection), connection, stored.Revision))
	s.Require().NoError(err)
	s.Equal(int64(1), fake.decrypts.Load())
}

// apiKeyConnection makes a custom connector of a new customer, and an api_key connection to it
// that holds no credentials, and returns the customer and the connection's id.
func (s *RouterStartSuite) apiKeyConnection(settings config.Config) (string, string) {
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
	return customer, connection.ID
}

// exported is the key a token export of the connection at path gives customer.
func (s *RouterStartSuite) exported(path, customer string) string {
	status, body := s.ask(http.MethodPost, path+"/token", customer, nil)
	s.Require().Equal(http.StatusOK, status, "%s", body)
	var token struct{ Value string }
	s.Require().NoError(json.Unmarshal(body, &token))
	return token.Value
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
