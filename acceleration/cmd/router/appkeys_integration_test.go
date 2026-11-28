//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/apikey"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/bearer"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/oauth2code"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/anthropic"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/providerkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/twiliosig"
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
	aad := appkeystest.RecordAAD("connector_credentials", app,
		fmt.Appendf(nil, "accelerate:connector-material:v1:%d:%s:%d:%s:%d", len(app), app, len(connection.ID), connection.ID, 1))
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
	aad := appkeystest.RecordAAD("connector_credentials", customer, fmt.Appendf(nil,
		"accelerate:connector-material:v1:%d:%s:%d:%s:%d", len(customer), customer, len(connection), connection, 1))
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
// KMS refuses still succeeds, under the router's key, and so does an export that cannot move
// the row to the data key then. For 30 s after the refusal, the app's writes and exports ask
// KMS nothing, so they go under the router's key even once KMS is back.
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
	stored, err = s.db.ConnectorConnections.Get(s.ctx, customer, connection)
	s.Require().NoError(err)
	s.Nil(stored.CredentialsDataKeyVersion, "the export could not move the row while KMS refused, and still worked")
	fake.refusal = nil
	answer, body = s.ask(http.MethodPut, path+"/credentials", customer, write(2, "kek-secret-2"))
	s.Require().Equal(http.StatusOK, answer, "%s", body)
	stored, err = s.db.ConnectorConnections.Get(s.ctx, customer, connection)
	s.Require().NoError(err)
	s.Nil(stored.CredentialsDataKeyVersion)
	s.Equal("kek-secret-2", s.exported(path, customer))
	s.Equal(int64(1), fake.encrypts.Load(), "a write or export within 30 s of the refusal asked KMS")

	customer, connection = s.apiKeyConnection(settings)
	path = "http://" + settings.Addr + "/v1/agents/connections/" + connection
	answer, body = s.ask(http.MethodPut, path+"/credentials", customer, write(1, "sealed-secret"))
	s.Require().Equal(http.StatusOK, answer, "%s", body)
	stored, err = s.db.ConnectorConnections.Get(s.ctx, customer, connection)
	s.Require().NoError(err)
	s.Equal(new(1), stored.CredentialsDataKeyVersion, "another app's write was held back by the refusal")
	s.Equal(int64(2), fake.encrypts.Load())

	s.Equal("sealed-secret", s.exported(path, customer))
	s.Zero(fake.decrypts.Load())
	// Another replica unwraps the key the write stored, under the AAD it was wrapped with.
	other, closeOther, err := newAppKeys(s.ctx, settings, s.db)
	s.Require().NoError(err)
	s.T().Cleanup(closeOther)
	_, err = other.Open(s.ctx, customer, 1, stored.CredentialsSealed, appkeystest.RecordAAD("connector_credentials",
		customer, fmt.Appendf(nil, "accelerate:connector-material:v1:%d:%s:%d:%s:%d",
			len(customer), customer, len(connection), connection, stored.Revision)))
	s.Require().NoError(err)
	s.Equal(int64(1), fake.decrypts.Load())
}

// A consent opens the app's own OAuth client secret under its data key, and seals its attempt
// under it. A router that did not hand its data keys to ConnectorClients would refuse every
// consent of such an app, and one that did not hand them to the API would seal the attempt
// under the router's key.
func (s *RouterStartSuite) TestARunningRouterOpensTheAppsClientSecretAndSealsTheConsentUnderItsDataKey() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	settings.PublicURL = "https://router.example"
	serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey, settings.Auth.AppKeysSeal = testKMSKey, true
	customer := "customer-" + rand.Text()
	connector := store.CustomPrefix + "oauthkeys" + strings.ToLower(rand.Text())
	manifest, err := core.ParseManifest([]byte(`
id: ` + connector + `
revision: 1
name: OAuth keys
endpoints:
  authorize: https://1.1.1.1/authorize
  token: https://1.1.1.1/token
  mcp: https://1.1.1.1/mcp
schemes: [oauth2_code]
client:
  registration: [customer]
  auth_method: client_secret_post
scopes:
  list: [read]
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
		OwnerType: store.OwnerApp, AuthScheme: oauth2code.Name}
	s.Require().NoError(s.db.ConnectorConnections.Insert(s.ctx, registry, connection))
	s.start(settings)
	base := "http://" + settings.Addr

	status, body := s.ask(http.MethodPut, base+"/v1/agents/connectors/"+connector+"/oauth-client", customer,
		map[string]any{"client_id": "the-apps-client", "client_secret": "the-apps-secret"})
	s.Require().Contains([]int{http.StatusOK, http.StatusCreated}, status, "%s", body)
	record, err := s.db.ConnectorOAuthClients.Get(s.ctx, customer, connector)
	s.Require().NoError(err)
	s.Require().NotNil(record.SecretDataKeyVersion, "the server seals the put under the data key")

	status, body = s.ask(http.MethodPost, base+"/v1/agents/connections/"+connection.ID+"/authorizations", customer, nil)
	s.Require().Equal(http.StatusCreated, status, "the consent opens the app's client secret: %s", body)
	var started struct {
		ID string `json:"id"`
	}
	s.Require().NoError(json.Unmarshal(body, &started))
	attempt, err := s.db.ConnectorAuthorizationAttempts.Get(s.ctx, started.ID)
	s.Require().NoError(err)
	s.NotNil(attempt.AttemptDataKeyVersion, "the server seals the attempt under the data key")
}

// A new event destination's signing secret is sealed under the app's data key. A router that
// did not hand its data keys to the event forwarder would seal it under the router's key.
func (s *RouterStartSuite) TestARunningRouterSealsAnEventDestinationSecretUnderTheAppsDataKey() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey, settings.Auth.AppKeysSeal = testKMSKey, true
	customer := "customer-" + rand.Text()
	s.start(settings)

	status, body := s.ask(http.MethodPost, "http://"+settings.Addr+"/v1/agents/connectors/"+store.StreamSource+"/event-destinations",
		customer, map[string]any{"url": "https://1.1.1.1/hooks", "forward": store.ForwardAll})
	s.Require().Equal(http.StatusCreated, status, "%s", body)
	destinations, err := s.db.EventDestinations.Select(s.ctx, customer, store.EventDestinationFilter{ConnectorID: store.StreamSource})
	s.Require().NoError(err)
	s.Require().Len(destinations, 1)
	s.Equal(new(1), destinations[0].SecretDataKeyVersion)
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

// TestARunningRouterMovesAProviderKeyToTheAppsDataKeyWhenWorkIsRoutedOnIt: run() hands the data
// keys to the keyring routing reads the customers' own keys through. With sealing on, a key
// still under the router's key is routed on, and moved to the app's data key on that use. A
// keyring without the data keys would leave the row as it was.
func (s *RouterStartSuite) TestARunningRouterMovesAProviderKeyToTheAppsDataKeyWhenWorkIsRoutedOnIt() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	settings.Limits.Mode = config.LimitsOff
	serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey, settings.Auth.AppKeysSeal = testKMSKey, true
	vendor := startAnthropic(s.T())
	s.T().Setenv("ROUTER_TEST_ANTHROPIC_KEY", "sk-ant-deployment-0000000000000")
	settings.RoutingConfig = filepath.Join(s.T().TempDir(), "routing.yaml")
	s.Require().NoError(os.WriteFile(settings.RoutingConfig, []byte(`llm:
  providers:
    - provider: anthropic
      model: claude-haiku-5-5
      languages: [en]
      endpoint: `+vendor.URL+`
      api_key_env: ROUTER_TEST_ANTHROPIC_KEY
`), 0o600))
	customer := "customer-" + rand.Text()
	const customerKey = "sk-ant-api03-the-customers-own-0000"
	secrets, err := newSecretSealer(s.ctx, settings)
	s.Require().NoError(err)
	sealed, err := providerkeys.Seal(s.ctx, secrets, nil, customer, anthropic.ProviderName, pricing.RegionUS, customerKey)
	s.Require().NoError(err)
	_, _, err = s.db.ProviderKeys.Upsert(s.ctx, &sealed)
	s.Require().NoError(err)
	s.start(settings)

	header := http.Header{"X-Customer-Id": {customer}}
	socket, _, err := websocket.DefaultDialer.Dial("ws://"+settings.Addr+"/v1/llm/stream", header)
	s.Require().NoError(err)
	defer socket.Close() //nolint:errcheck // the test is done with it
	s.Require().NoError(socket.WriteJSON(map[string]any{"type": "start", "target": "anthropic/claude-haiku-5-5"}))
	s.awaitFrame(socket, "started")
	s.Require().NoError(socket.WriteJSON(map[string]any{"type": "respond", "id": "r1",
		"messages": []map[string]any{{"role": "user", "content": "hello"}}}))
	s.awaitFrame(socket, "complete")

	s.Equal([]string{customerKey}, vendor.asked())
	rows, err := s.db.ProviderKeys.SelectAll(s.ctx, customer)
	s.Require().NoError(err)
	s.Require().Len(rows, 1)
	s.Equal(new(1), rows[0].DataKeyVersion, "the key moved to the app's data key on its use")
	s.Zero(rows[0].KEKVersion)
}

// TestARunningRouterSweepsSecretsInACustomersOrganizationOnAKeyUnderTheAppsDataKey: run() hands
// the data keys to the secret sweeper, which reaches each customer's own Daytona organization
// on the key the customer keeps. A sweeper without them could not open a key under a data key.
func (s *RouterStartSuite) TestARunningRouterSweepsSecretsInACustomersOrganizationOnAKeyUnderTheAppsDataKey() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	// Its own prefix, so a run beside it on the same Redis has its own sweep lease.
	settings.CodeSandbox.SecretPrefix = "dk" + strings.ToLower(rand.Text())
	serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey, settings.Auth.AppKeysSeal = testKMSKey, true
	var mu sync.Mutex
	var listedOn []string
	daytona := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The SDK's client also opens Daytona's event socket, which the sweep never uses.
		if r.URL.Path != "/api/secret/paginated" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		mu.Lock()
		listedOn = append(listedOn, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[]}`)
	}))
	s.T().Cleanup(daytona.Close)
	s.T().Setenv("DAYTONA_API_KEY", "key-1")
	s.T().Setenv("DAYTONA_API_URL", daytona.URL+"/api")
	customerKey := "dtn_customer_" + strings.ToLower(rand.Text())
	secrets, err := newSecretSealer(s.ctx, settings)
	s.Require().NoError(err)
	keys, closeKeys, err := newAppKeys(s.ctx, settings, s.db)
	s.Require().NoError(err)
	s.T().Cleanup(closeKeys)
	sealed, err := providerkeys.Seal(s.ctx, secrets, keys, "customer-"+rand.Text(), providerkeys.Daytona, pricing.RegionUS, customerKey)
	s.Require().NoError(err)
	s.Require().NotNil(sealed.DataKeyVersion)
	_, _, err = s.db.ProviderKeys.Upsert(s.ctx, &sealed)
	s.Require().NoError(err)

	s.start(settings)

	s.Eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(listedOn, customerKey)
	}, 30*time.Second, 50*time.Millisecond,
		"the sweep lists the secrets in the customer's organization on their key")
}

// TestARunningRouterMovesACustomModelsKeyToTheAppsDataKeyWhenASessionUsesIt: run() hands the
// data keys to the custom models routing resolves. With sealing on, a key still under the
// router's key is sent to the model's endpoint, and moved to the app's data key on that use.
// Custom models without the data keys would leave the row as it was.
func (s *RouterStartSuite) TestARunningRouterMovesACustomModelsKeyToTheAppsDataKeyWhenASessionUsesIt() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	settings.Limits.Mode = config.LimitsOff
	settings.Models.PrivateEndpoints = true
	serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey, settings.Auth.AppKeysSeal = testKMSKey, true
	vendor := startAnthropic(s.T())
	s.T().Setenv("ROUTER_TEST_ANTHROPIC_KEY", "sk-ant-deployment-0000000000000")
	settings.RoutingConfig = filepath.Join(s.T().TempDir(), "routing.yaml")
	s.Require().NoError(os.WriteFile(settings.RoutingConfig, []byte(`llm:
  providers:
    - provider: anthropic
      model: claude-haiku-5-5
      languages: [en]
      endpoint: `+vendor.URL+`
      api_key_env: ROUTER_TEST_ANTHROPIC_KEY
`), 0o600))
	var mu sync.Mutex
	var bearers []string
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		bearers = append(bearers, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, `data: {"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":"stop"}]}`+"\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	s.T().Cleanup(endpoint.Close)
	customer := "customer-" + rand.Text()
	const modelKey = "sk-the-customers-own-endpoint-0000"
	secrets, err := newSecretSealer(s.ctx, settings)
	s.Require().NoError(err)
	model := store.CustomModel{ID: store.NewID(), CustomerID: customer, Name: "tuned", BaseURL: endpoint.URL + "/v1", Model: "acme/tuned"}
	s.Require().NoError(llmrouter.SealKey(s.ctx, secrets, nil, &model, modelKey))
	s.Require().NoError(s.db.CustomModels.Insert(s.ctx, &model))
	s.start(settings)

	header := http.Header{"X-Customer-Id": {customer}}
	socket, _, err := websocket.DefaultDialer.Dial("ws://"+settings.Addr+"/v1/llm/stream", header)
	s.Require().NoError(err)
	defer socket.Close() //nolint:errcheck // the test is done with it
	s.Require().NoError(socket.WriteJSON(map[string]any{"type": "start", "target": "custom/tuned"}))
	s.awaitFrame(socket, "started")
	s.Require().NoError(socket.WriteJSON(map[string]any{"type": "respond", "id": "r1",
		"messages": []map[string]any{{"role": "user", "content": "hello"}}}))
	s.awaitFrame(socket, "complete")

	mu.Lock()
	s.Equal([]string{"Bearer " + modelKey}, bearers)
	mu.Unlock()
	stored, err := s.db.CustomModels.Get(s.ctx, customer, model.ID)
	s.Require().NoError(err)
	s.Equal(new(1), stored.DataKeyVersion, "the key moved to the app's data key on its use")
	s.Zero(stored.KEKVersion)
}

// awaitFrame reads frames off socket until one of the wanted type arrives.
func (s *RouterStartSuite) awaitFrame(socket *websocket.Conn, wanted string) {
	s.Require().NoError(socket.SetReadDeadline(time.Now().Add(30 * time.Second)))
	for {
		var received map[string]any
		s.Require().NoError(socket.ReadJSON(&received), "the socket closed before %s arrived", wanted)
		s.Require().NotEqual("error", received["type"], "%v", received)
		if received["type"] == wanted {
			return
		}
	}
}

// anthropicVendor is an Anthropic that answers every message with "hello" and keeps the key
// each request was made with.
type anthropicVendor struct {
	*httptest.Server
	mu   sync.Mutex
	keys []string
}

func startAnthropic(t *testing.T) *anthropicVendor {
	vendor := &anthropicVendor{}
	events := []string{
		`{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude","content":[],"usage":{"input_tokens":5,"output_tokens":1}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}`,
		`{"type":"message_stop"}`,
	}
	vendor.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vendor.mu.Lock()
		vendor.keys = append(vendor.keys, r.Header.Get("X-Api-Key"))
		vendor.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range events {
			var typed struct {
				Type string `json:"type"`
			}
			_ = json.Unmarshal([]byte(event), &typed)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", typed.Type, event)
		}
	}))
	t.Cleanup(vendor.Close)
	return vendor
}

// asked is every key the vendor was asked with, in order.
func (v *anthropicVendor) asked() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return slices.Clone(v.keys)
}

// A call to a number whose trunk password is under the app's data key is dialled with that
// password. Unwired, the router opens it under its own key only, and hangs up the call.
func (s *RouterStartSuite) TestARunningRouterSendsACallToATrunkUnderTheAppsDataKey() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	settings.PublicURL = "https://router.example.test"
	settings.Limits.Mode = config.LimitsOff
	settings.Twilio.WebhookAuthToken = rand.Text()
	settings.Twilio.AccountSID = "AC" + strings.Repeat("0", 31) + "2"
	fake := serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey = testKMSKey
	customer := "customer-" + rand.Text()
	e164 := "+15125550177"
	_, err := s.db.GlobalDB().ExecContext(s.ctx,
		"UPDATE phone_numbers SET released_at = now() WHERE e164 = ? AND released_at IS NULL", e164)
	s.Require().NoError(err)
	s.Require().NoError(s.db.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{
		E164: e164, Vendor: "twilio", Country: "US", CustomerID: customer, PurchasedAt: time.Now().UTC(),
	}))
	key := appkeystest.AddKey(s.T(), s.db, fake.kms.KMS, customer, 1)
	aad := appkeystest.RecordAAD("stream_trunk", customer, []byte("phone_numbers/"+customer+"/"+e164))
	version := 1
	attachment := store.NumberAttachment{TrunkID: "trunk-" + rand.Text(), CallType: "default"}
	group := store.TrunkGroup{CustomerID: customer, Vendor: "twilio"}
	s.Require().NoError(s.db.StreamTrunks.InGroup(s.ctx, group, func(_ context.Context, tx *store.TrunkGroupTx) error {
		if err := tx.SaveTrunk(store.StreamTrunk{
			ID: attachment.TrunkID, BridgeURI: "sip:" + e164 + "@y", BridgeUsername: "x",
			BridgePasswordSealed:         appkeystest.Seal(s.T(), key, []byte("data-key-password"), aad),
			BridgePasswordDataKeyVersion: &version, Own: true,
		}); err != nil {
			return err
		}
		return tx.Attach(e164, attachment)
	}))
	stop := s.start(settings)

	form := url.Values{"AccountSid": {settings.Twilio.AccountSID}, "Direction": {"inbound"}, "To": {e164},
		"From": {"+15550001111"}, "CallSid": {fmt.Sprintf("CA%032x", time.Now().UnixNano())}, "CallStatus": {"ringing"}}
	signature := twiliosig.Sign(settings.Twilio.WebhookAuthToken, settings.PublicURL+"/v1/phone/hooks/twilio", form)
	request, err := http.NewRequest(http.MethodPost, "http://"+settings.Addr+"/v1/phone/hooks/twilio",
		strings.NewReader(form.Encode()))
	s.Require().NoError(err)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set(twiliosig.SignatureHeader, signature)
	response, err := http.DefaultClient.Do(request)
	s.Require().NoError(err)
	body, err := io.ReadAll(response.Body)
	s.Require().NoError(err)
	s.Require().NoError(response.Body.Close())
	s.Require().NoError(stop())

	s.Equal(http.StatusOK, response.StatusCode)
	s.Contains(string(body), `password="data-key-password"`)
}
