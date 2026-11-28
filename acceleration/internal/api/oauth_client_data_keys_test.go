//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/credentialstores/pgsealed"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/providers"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/resolver"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/oauth2code"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/slackapps"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/verifiers/hmacheader"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// The two record labels, written out so a test fails when a rename would leave dashboards
// without their series.
const (
	clientSecretKind  = "connector_oauth_client_secrets"
	signingSecretKind = "connector_signing_secrets"
)

// OAuthClientDataKeysSuite is an app's own OAuth client and provider app under the app's data
// key: put, opened by the OAuth flow's lookup (ConnectorClients) and the events route
// (ProviderApp), and moved there on use. It does not run in parallel, so the KEK open counters
// it reads move only with its own tests.
type OAuthClientDataKeysSuite struct {
	suite.Suite
	db     *store.Store
	sealer *auth.Sealer
	kms    *appkeystest.KMS
	app    string
	// servedAtStart is whether both counters were served before any test of the suite counted.
	servedAtStart bool
}

func TestOAuthClientDataKeysSuite(t *testing.T) {
	suite.Run(t, new(OAuthClientDataKeysSuite))
}

func (s *OAuthClientDataKeysSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	db, err := store.Open(testenv.Database(dsn, "api_oauthkeys"))
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })
	s.Require().NoError(db.Migrate(s.T().Context()))
	s.Require().NoError(db.ConnectorDefinitions.Seed(s.T().Context(), providers.FS))
	s.db = db
	s.sealer, err = auth.NewSealerWithKeyring(1, map[int]string{1: "oauth client data keys test key"})
	s.Require().NoError(err)
	s.servedAtStart = appkeystest.Served(s.T(), clientSecretKind) && appkeystest.Served(s.T(), signingSecretKind)
}

func (s *OAuthClientDataKeysSuite) SetupTest() {
	s.kms = appkeystest.NewKMS()
	s.app = "app-" + rand.Text()
}

func (s *OAuthClientDataKeysSuite) TestBothKindsOfSecretAreCountedFromTheStart() {
	s.True(s.servedAtStart)
}

// The events routes open the signing secret through the server's data keys: a Slack event
// signed with it is verified, and a WhatsApp handshake finds its app.
func (s *OAuthClientDataKeysSuite) TestTheEventsRoutesOpenASigningSecretUnderTheDataKey() {
	keys := s.keys(true)
	slackApp := s.put(keys, "client-secret", "signing-secret")
	whatsappApp := "A" + rand.Text()
	_, err := s.server(keys).setConnectorOAuthClient(context.WithValue(s.T().Context(), customerContextKey{}, s.app),
		&oauthClientRequest{ID: "whatsapp", Body: ConnectorOAuthClientRequest{ProviderAppID: whatsappApp, SigningSecret: "meta-secret"}})
	s.Require().NoError(err)
	credentials, err := pgsealed.New(s.db, s.sealer, pgsealed.WithDataKeys(keys))
	s.Require().NoError(err)
	connections, err := resolver.New(resolver.Config{Store: s.db, Credentials: credentials})
	s.Require().NoError(err)
	verifier := hmacheader.New()
	server, err := NewServer(Options{Routers: map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		Store: s.db, ConnectorSecrets: s.sealer, ConnectorDataKeys: keys, ConnectorResolver: connections,
		Connectors: core.Registry{Verifiers: map[string]core.Verifier{verifier.Name(): verifier}}})
	s.Require().NoError(err)

	body := `{"type":"url_verification","challenge":"slack-challenge"}`
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte("signing-secret"))
	mac.Write([]byte("v0:" + timestamp + ":" + body))
	event := httptest.NewRequest(http.MethodPost, providerAppEventsPath+"slack_bot/"+slackApp, strings.NewReader(body))
	event.Header.Set("Content-Type", "application/json")
	event.Header.Set("X-Slack-Request-Timestamp", timestamp)
	event.Header.Set("X-Slack-Signature", "v0="+hex.EncodeToString(mac.Sum(nil)))
	answer := httptest.NewRecorder()
	server.Handler().ServeHTTP(answer, event)
	s.Equal(http.StatusOK, answer.Code, answer.Body.String())
	s.Equal("slack-challenge", answer.Body.String())

	handshake := httptest.NewRequest(http.MethodGet, providerAppEventsPath+"whatsapp/"+whatsappApp+
		"?hub.mode=subscribe&hub.verify_token="+whatsappApp+"&hub.challenge=1158201444", nil)
	answer = httptest.NewRecorder()
	server.Handler().ServeHTTP(answer, handshake)
	s.Equal(http.StatusOK, answer.Code, answer.Body.String())
	s.Equal("1158201444", answer.Body.String())
}

// An unsigned request on the public route must not make the router create the app's data key
// or rewrite the row: only a request that verified moves the signing secret.
func (s *OAuthClientDataKeysSuite) TestAnUnsignedEventMovesNothingAndAVerifiedOneMovesTheSigningSecret() {
	providerApp := s.put(nil, "client-secret", "signing-secret")
	before := s.row()
	keys := s.keys(true)
	server := s.eventsServer(keys)
	wraps := s.kms.Wraps()

	s.Equal(http.StatusUnauthorized, s.postEvent(server, providerApp, []byte("not-the-secret")).Code)
	handshake := httptest.NewRequest(http.MethodGet, providerAppEventsPath+"slack_bot/"+providerApp, nil)
	server.Handler().ServeHTTP(httptest.NewRecorder(), handshake)

	s.Equal(before, s.row())
	s.Equal(wraps, s.kms.Wraps(), "no data key was created")

	s.Equal(http.StatusOK, s.postEvent(server, providerApp, []byte("signing-secret")).Code)
	s.Equal(new(1), s.row().SigningSecretDataKeyVersion)
	s.Nil(s.row().SecretDataKeyVersion, "the client secret moves on its own use")
}

func (s *OAuthClientDataKeysSuite) TestAPutSealsBothSecretsUnderTheAppsDataKey() {
	keys := s.keys(true)
	providerApp := s.put(keys, "client-secret", "signing-secret")

	row := s.row()
	s.Equal(new(1), row.SecretDataKeyVersion)
	s.Equal(new(1), row.SigningSecretDataKeyVersion)
	s.Equal(1, row.KEKVersion, "the table's CHECK needs a key version beside a sealed secret")
	s.Equal(1, row.SigningKEKVersion)
	_, err := s.sealer.OpenWithAADVersion(row.SecretSealed, oauthClientAAD(s.app, "slack_bot"), 1)
	s.Error(err, "the router's key does not open it")
	clientOpens, signingOpens := s.kekOpens()

	s.Equal("client-secret", s.lookup(keys).Secret)
	_, signing, err := s.useProviderApp(s.sealer, keys, "slack_bot", providerApp)
	s.Require().NoError(err)
	s.Equal("signing-secret", signing)
	s.Equal(row, s.row(), "a use of a secret under the data key rewrites nothing")
	s.zeroKEKOpensSince(clientOpens, signingOpens)
}

// The AAD layouts are written out, so a change that the secrets already sealed cannot follow
// fails here.
func (s *OAuthClientDataKeysSuite) TestASecretSealedUnderTheDataKeyIsBoundToItsKindAppAndRow() {
	key := appkeystest.AddKey(s.T(), s.db, s.kms, s.app, 1)
	providerApp := s.put(s.keys(true), "client-secret", "signing-secret")
	row := s.row()

	s.Equal("client-secret", s.openUnder(key, row.SecretSealed, clientSecretKind,
		fmt.Appendf(nil, "accelerate:connector-oauth-client:v1:%d:%s:9:slack_bot", len(s.app), s.app)))
	s.Equal("signing-secret", s.openUnder(key, row.SigningSecretSealed, signingSecretKind,
		fmt.Appendf(nil, "accelerate:connector-provider-app-signing-secret:v1:%d:%s:9:slack_bot:%d:%s",
			len(s.app), s.app, len(providerApp), providerApp)))
}

func (s *OAuthClientDataKeysSuite) TestTheRoutersAppsAreSealedUnderTheAppsDataKey() {
	server := s.server(s.keys(true))
	record, err := server.managedRecord(s.T().Context(), s.app, "slack_bot", 0, slackapps.Credentials{
		AppID: "A" + rand.Text(), ClientID: "managed-client", ClientSecret: "managed-secret", SigningSecret: "managed-signing"})
	s.Require().NoError(err)

	s.Equal(new(1), record.SecretDataKeyVersion)
	s.Equal(new(1), record.SigningSecretDataKeyVersion)
	s.Equal(1, record.KEKVersion)
	s.Equal(1, record.SigningKEKVersion)
}

func (s *OAuthClientDataKeysSuite) TestARouterKeySecretMovesUnderTheDataKeyOnItsFirstUseAndCountsOnce() {
	providerApp := s.put(nil, "client-secret", "signing-secret")
	keys := s.keys(true)
	clientOpens, signingOpens := s.kekOpens()

	s.Equal("client-secret", s.lookup(keys).Secret)
	_, signing, err := s.useProviderApp(s.sealer, keys, "slack_bot", providerApp)
	s.Require().NoError(err)
	s.Equal("signing-secret", signing)

	s.Equal(clientOpens+1, appkeystest.KEKOpens(s.T(), clientSecretKind))
	s.Equal(signingOpens+1, appkeystest.KEKOpens(s.T(), signingSecretKind))
	row := s.row()
	s.Equal(new(1), row.SecretDataKeyVersion)
	s.Equal(new(1), row.SigningSecretDataKeyVersion)
	s.Equal(1, row.KEKVersion)
	s.Equal(1, row.SigningKEKVersion)
	s.Equal("client-secret", s.lookup(keys).Secret)
	_, signing, err = s.useProviderApp(s.sealer, keys, "slack_bot", providerApp)
	s.Require().NoError(err)
	s.Equal("signing-secret", signing)
	s.Equal(clientOpens+1, appkeystest.KEKOpens(s.T(), clientSecretKind), "the second use opens under the data key")
	s.Equal(signingOpens+1, appkeystest.KEKOpens(s.T(), signingSecretKind))
}

// Before sealing is on, the routers only learn to open data-key secrets (README, step 2), so
// a use moves nothing and warns about nothing.
func (s *OAuthClientDataKeysSuite) TestWithSealingOffAUseMovesNothing() {
	providerApp := s.put(nil, "client-secret", "signing-secret")
	before := s.row()
	keys := s.keys(false)
	// The suite runs before the package's parallel suites start, so no other test logs here.
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	s.T().Cleanup(func() { slog.SetDefault(previous) })

	s.Equal("client-secret", s.lookup(keys).Secret)
	_, _, err := s.useProviderApp(s.sealer, keys, "slack_bot", providerApp)
	s.Require().NoError(err)

	s.Equal(before, s.row())
	s.Empty(logged.String())
}

// The values come from a probe of the same put and lookups on the base, before data keys:
// key versions 1 and 1, the secrets open under the router's key with these AADs, and a use
// under the current key version rewrites nothing.
func (s *OAuthClientDataKeysSuite) TestWithoutDataKeysTheSecretsAreSealedAndOpenedAsBefore() {
	providerApp := s.put(nil, "client-secret", "signing-secret")

	row := s.row()
	s.Equal(1, row.KEKVersion)
	s.Equal(1, row.SigningKEKVersion)
	s.Nil(row.SecretDataKeyVersion)
	s.Nil(row.SigningSecretDataKeyVersion)
	secret, err := s.sealer.OpenWithAADVersion(row.SecretSealed,
		fmt.Appendf(nil, "accelerate:connector-oauth-client:v1:%d:%s:9:slack_bot", len(s.app), s.app), 1)
	s.Require().NoError(err)
	s.Equal("client-secret", secret)
	signing, err := s.sealer.OpenWithAADVersion(row.SigningSecretSealed,
		fmt.Appendf(nil, "accelerate:connector-provider-app-signing-secret:v1:%d:%s:9:slack_bot:%d:%s",
			len(s.app), s.app, len(providerApp), providerApp), 1)
	s.Require().NoError(err)
	s.Equal("signing-secret", signing)
	s.Equal("client-secret", s.lookup(nil).Secret)
	_, signing, err = s.useProviderApp(s.sealer, nil, "slack_bot", providerApp)
	s.Require().NoError(err)
	s.Equal("signing-secret", signing)
	s.Equal(row, s.row())
}

// Without data keys, a secret under an older key version still moves to the current one on
// use, as on the base.
func (s *OAuthClientDataKeysSuite) TestWithoutDataKeysAnOlderKeyVersionIsSealedAgainOnUse() {
	providerApp := s.put(nil, "client-secret", "signing-secret")
	rotated := s.rotated()

	found, ok, err := ConnectorClients(s.db, rotated, nil, func(string) string { return "" })(s.T().Context(),
		core.ConnectionRef{CustomerID: s.app}, core.ResolvedManifest{ConnectorID: "slack_bot"}, core.ClientCustomer)
	s.Require().NoError(err)
	s.Require().True(ok)
	s.Equal("client-secret", found.Secret)
	_, _, err = s.useProviderApp(rotated, nil, "slack_bot", providerApp)
	s.Require().NoError(err)

	row := s.row()
	s.Equal(2, row.KEKVersion)
	s.Equal(2, row.SigningKEKVersion)
	s.Nil(row.SecretDataKeyVersion)
	s.Nil(row.SigningSecretDataKeyVersion)
}

// A secret that moves from an older router key under the data key keeps the keyring's current
// version beside it, because the table's CHECK needs one with every sealed secret.
func (s *OAuthClientDataKeysSuite) TestASecretMovedUnderTheDataKeyKeepsTheCurrentKeyVersion() {
	providerApp := s.put(nil, "client-secret", "signing-secret")
	rotated, keys := s.rotated(), s.keys(true)

	found, _, err := ConnectorClients(s.db, rotated, keys, func(string) string { return "" })(s.T().Context(),
		core.ConnectionRef{CustomerID: s.app}, core.ResolvedManifest{ConnectorID: "slack_bot"}, core.ClientCustomer)
	s.Require().NoError(err)
	s.Equal("client-secret", found.Secret)
	_, _, err = s.useProviderApp(rotated, keys, "slack_bot", providerApp)
	s.Require().NoError(err)

	row := s.row()
	s.Equal(new(1), row.SecretDataKeyVersion)
	s.Equal(new(1), row.SigningSecretDataKeyVersion)
	s.Equal(2, row.KEKVersion)
	s.Equal(2, row.SigningKEKVersion)
}

// A data-key secret is not stale when the keyring moves on: sealing it again under the router's
// key would undo the data key.
func (s *OAuthClientDataKeysSuite) TestAnOlderKeyVersionBesideADataKeySecretIsLeftAlone() {
	providerApp := s.put(s.keys(true), "client-secret", "signing-secret")
	before := s.row()
	rotated, keys := s.rotated(), s.keys(false)

	found, _, err := ConnectorClients(s.db, rotated, keys, func(string) string { return "" })(s.T().Context(),
		core.ConnectionRef{CustomerID: s.app}, core.ResolvedManifest{ConnectorID: "slack_bot"}, core.ClientCustomer)
	s.Require().NoError(err)
	s.Equal("client-secret", found.Secret)
	_, _, err = s.useProviderApp(rotated, keys, "slack_bot", providerApp)
	s.Require().NoError(err)

	s.Equal(before, s.row())
}

// A rollback turns sealing off and keeps the KMS key (README): the data-key secrets still
// open, and a put seals under the router's key again.
func (s *OAuthClientDataKeysSuite) TestWithSealingTurnedOffADataKeySecretStillOpensAndAPutUsesTheRoutersKey() {
	providerApp := s.put(s.keys(true), "client-secret", "signing-secret")
	before := s.row()
	keys := s.keys(false)

	s.Equal("client-secret", s.lookup(keys).Secret)
	_, signing, err := s.useProviderApp(s.sealer, keys, "slack_bot", providerApp)
	s.Require().NoError(err)
	s.Equal("signing-secret", signing)
	s.Equal(before, s.row())

	s.put(keys, "second-secret", "second-signing")
	row := s.row()
	s.Nil(row.SecretDataKeyVersion, "a put that replaces the record replaces its data key version too")
	s.Nil(row.SigningSecretDataKeyVersion)
	s.Equal("second-secret", s.lookup(nil).Secret, "the router's key opens it without data keys")
}

func (s *OAuthClientDataKeysSuite) TestAPutWhileKMSIsDownSealsUnderTheRoutersKey() {
	keys := s.keys(true)
	s.kms.Fail(errors.New("kms: the test's outage"))
	fallbacks := appkeystest.SealFallbacks(s.T(), clientSecretKind)
	signingFallbacks := appkeystest.SealFallbacks(s.T(), signingSecretKind)

	s.put(keys, "client-secret", "signing-secret")

	row := s.row()
	s.Nil(row.SecretDataKeyVersion)
	s.Nil(row.SigningSecretDataKeyVersion)
	s.Equal(fallbacks+1, appkeystest.SealFallbacks(s.T(), clientSecretKind))
	s.Equal(signingFallbacks+1, appkeystest.SealFallbacks(s.T(), signingSecretKind))
	s.Equal("client-secret", s.lookup(nil).Secret)
}

// A use while KMS is down still succeeds under the router's key, and a secret under an older
// key version still moves to the current one, so a KMS outage does not hold back a keyring
// rotation.
func (s *OAuthClientDataKeysSuite) TestAUseWhileKMSIsDownKeepsTheRoutersKeyAndStillRewraps() {
	providerApp := s.put(nil, "client-secret", "signing-secret")
	keys, rotated := s.keys(true), s.rotated()
	s.kms.Fail(errors.New("kms: the test's outage"))

	found, _, err := ConnectorClients(s.db, rotated, keys, func(string) string { return "" })(s.T().Context(),
		core.ConnectionRef{CustomerID: s.app}, core.ResolvedManifest{ConnectorID: "slack_bot"}, core.ClientCustomer)
	s.Require().NoError(err)
	s.Equal("client-secret", found.Secret)
	_, _, err = s.useProviderApp(rotated, keys, "slack_bot", providerApp)
	s.Require().NoError(err)

	row := s.row()
	s.Nil(row.SecretDataKeyVersion)
	s.Nil(row.SigningSecretDataKeyVersion)
	s.Equal(2, row.KEKVersion)
	s.Equal(2, row.SigningKEKVersion)
}

// A data key KMS cannot unwrap is the key's fault, not the client's: the transport then
// answers with fixed text (core.KeyFault), never with the KMS error.
func (s *OAuthClientDataKeysSuite) TestADataKeySecretKMSCannotOpenIsAKeyFault() {
	providerApp := s.put(s.keys(true), "client-secret", "signing-secret")
	outage := "kms: the test's outage " + rand.Text()
	keys := s.keys(true)
	s.kms.Fail(errors.New(outage))

	_, _, err := s.lookupErr(keys)
	s.ErrorIs(err, core.ErrKeyUnavailable)
	s.NotContains(core.KeyFault(err).Error(), outage)
	_, _, err = s.useProviderApp(s.sealer, keys, "slack_bot", providerApp)
	s.ErrorIs(err, core.ErrKeyUnavailable)
}

// A router whose auth.app_keys_kms_key was removed cannot open a data-key secret, which is a
// fault of its keys too.
func (s *OAuthClientDataKeysSuite) TestADataKeySecretWithoutDataKeysIsAKeyFault() {
	providerApp := s.put(s.keys(true), "client-secret", "signing-secret")

	_, _, err := s.lookupErr(nil)
	s.ErrorIs(err, core.ErrKeyUnavailable)
	s.ErrorIs(err, appkeys.ErrNoKeys)
	_, _, err = s.useProviderApp(s.sealer, nil, "slack_bot", providerApp)
	s.ErrorIs(err, core.ErrKeyUnavailable)
}

// A row that names a data key version the app does not have fails, and never falls back to
// the router's key, even though the blob is one the router's key opens. Such a row is what a
// build from before data keys leaves when it replaces a data-key secret.
func (s *OAuthClientDataKeysSuite) TestASecretUnderAMissingDataKeyVersionFailsClosed() {
	providerApp := s.put(nil, "client-secret", "signing-secret")
	s.nameDataKeyVersion(7)
	keys := s.keys(true)

	found, _, err := s.lookupErr(keys)
	s.Error(err)
	s.Empty(found.Secret)
	_, signing, err := s.useProviderApp(s.sealer, keys, "slack_bot", providerApp)
	s.Error(err)
	s.Empty(signing)
}

// A data key that cannot be read from the store fails the open too, with no fallback.
func (s *OAuthClientDataKeysSuite) TestASecretWhoseDataKeyCannotBeReadFailsClosed() {
	providerApp := s.put(nil, "client-secret", "signing-secret")
	appkeystest.AddKey(s.T(), s.db, s.kms, s.app, 1)
	s.nameDataKeyVersion(1)
	closed, err := store.Open(testenv.Database(os.Getenv("ROUTER_POSTGRES_DSN"), "api_oauthkeys"))
	s.Require().NoError(err)
	s.Require().NoError(closed.Close())
	keys, err := appkeys.New(closed, s.kms, appkeystest.Options())
	s.Require().NoError(err)

	found, _, err := s.lookupErr(keys)
	s.Error(err)
	s.Empty(found.Secret)
	_, signing, err := s.useProviderApp(s.sealer, keys, "slack_bot", providerApp)
	s.Error(err)
	s.Empty(signing)
}

// A data-key blob copied onto another app's row opens under neither app's key.
func (s *OAuthClientDataKeysSuite) TestADataKeySecretCopiedToAnotherAppDoesNotOpen() {
	keys := s.keys(true)
	s.put(keys, "client-secret", "signing-secret")
	source := s.row()
	s.app = "app-" + rand.Text()
	s.put(keys, "other-secret", "other-signing")
	_, err := s.db.GlobalDB().ExecContext(s.T().Context(),
		"UPDATE connector_oauth_clients SET secret_sealed = ? WHERE customer_id = ? AND connector_id = 'slack_bot'",
		source.SecretSealed, s.app)
	s.Require().NoError(err)

	_, _, err = s.lookupErr(keys)

	s.ErrorIs(err, appkeys.ErrDoesNotOpen)
	s.NotErrorIs(err, core.ErrKeyUnavailable, "the row is wrong, not the key")
}

// nameDataKeyVersion has both secrets of the test's app's record name version of its data key
// and leaves the blobs as they are.
func (s *OAuthClientDataKeysSuite) nameDataKeyVersion(version int) {
	_, err := s.db.GlobalDB().ExecContext(s.T().Context(), "UPDATE connector_oauth_clients SET secret_data_key_version = ?,"+
		" signing_secret_data_key_version = ? WHERE customer_id = ? AND connector_id = 'slack_bot'", version, version, s.app)
	s.Require().NoError(err)
}

// useProviderApp is what the events route does with a provider app whose request verified: it
// opens the signing secret, then seals it again.
func (s *OAuthClientDataKeysSuite) useProviderApp(sealer *auth.Sealer, dataKeys *appkeys.Keys, connectorID, providerAppID string) (store.ConnectorOAuthClient, string, error) {
	record, secret, err := ProviderApp(s.T().Context(), s.db, sealer, dataKeys, connectorID, providerAppID)
	if err == nil {
		resealSigningSecret(s.T().Context(), s.db, sealer, dataKeys, record, secret)
	}
	return record, secret, err
}

// eventsServer is a router whose events route verifies with dataKeys.
func (s *OAuthClientDataKeysSuite) eventsServer(dataKeys *appkeys.Keys) *Server {
	credentials, err := pgsealed.New(s.db, s.sealer, pgsealed.WithDataKeys(dataKeys))
	s.Require().NoError(err)
	connections, err := resolver.New(resolver.Config{Store: s.db, Credentials: credentials})
	s.Require().NoError(err)
	verifier := hmacheader.New()
	server, err := NewServer(Options{Routers: map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		Store: s.db, ConnectorSecrets: s.sealer, ConnectorDataKeys: dataKeys, ConnectorResolver: connections,
		Connectors: core.Registry{Verifiers: map[string]core.Verifier{verifier.Name(): verifier}}})
	s.Require().NoError(err)
	return server
}

// postEvent posts a Slack url_verification to the provider app's events route, signed with key.
func (s *OAuthClientDataKeysSuite) postEvent(server *Server, providerApp string, key []byte) *httptest.ResponseRecorder {
	body := `{"type":"url_verification","challenge":"slack-challenge"}`
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("v0:" + timestamp + ":" + body))
	event := httptest.NewRequest(http.MethodPost, providerAppEventsPath+"slack_bot/"+providerApp, strings.NewReader(body))
	event.Header.Set("Content-Type", "application/json")
	event.Header.Set("X-Slack-Request-Timestamp", timestamp)
	event.Header.Set("X-Slack-Signature", "v0="+hex.EncodeToString(mac.Sum(nil)))
	answer := httptest.NewRecorder()
	server.Handler().ServeHTTP(answer, event)
	return answer
}

// keys is a Keys over the suite's database and the test's KMS, sealing when seal is set.
func (s *OAuthClientDataKeysSuite) keys(seal bool) *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = seal
	keys, err := appkeys.New(s.db, s.kms, options)
	s.Require().NoError(err)
	return keys
}

// server is a router with connectors on, over the suite's keyring and dataKeys.
func (s *OAuthClientDataKeysSuite) server(dataKeys *appkeys.Keys) *Server {
	server, err := NewServer(Options{Routers: map[routing.Modality]routing.Inspector{routing.LLM: idleInspector{}},
		Store: s.db, ConnectorSecrets: s.sealer, ConnectorDataKeys: dataKeys})
	s.Require().NoError(err)
	return server
}

// put is the test's app putting its own Slack app for slack_bot through a router with
// dataKeys, and returns the provider app id.
func (s *OAuthClientDataKeysSuite) put(dataKeys *appkeys.Keys, clientSecret, signingSecret string) string {
	providerApp := "A" + rand.Text()
	request := &oauthClientRequest{ID: "slack_bot", Body: ConnectorOAuthClientRequest{ClientID: "the-apps-client",
		ClientSecret: clientSecret, ProviderAppID: providerApp, SigningSecret: signingSecret}}
	_, err := s.server(dataKeys).setConnectorOAuthClient(context.WithValue(s.T().Context(), customerContextKey{}, s.app), request)
	s.Require().NoError(err)
	return providerApp
}

// row is the test's app's record for slack_bot as stored.
func (s *OAuthClientDataKeysSuite) row() store.ConnectorOAuthClient {
	row, err := s.db.ConnectorOAuthClients.Get(s.T().Context(), s.app, "slack_bot")
	s.Require().NoError(err)
	return row
}

// lookup is the app's own client for slack_bot as oauth2_code finds it with dataKeys.
func (s *OAuthClientDataKeysSuite) lookup(dataKeys *appkeys.Keys) oauth2code.Client {
	found, ok, err := s.lookupErr(dataKeys)
	s.Require().NoError(err)
	s.Require().True(ok)
	return found
}

func (s *OAuthClientDataKeysSuite) lookupErr(dataKeys *appkeys.Keys) (oauth2code.Client, bool, error) {
	return ConnectorClients(s.db, s.sealer, dataKeys, func(string) string { return "" })(s.T().Context(),
		core.ConnectionRef{CustomerID: s.app}, core.ResolvedManifest{ConnectorID: "slack_bot"}, core.ClientCustomer)
}

// rotated is the suite's key as version 1 and a new current version 2.
func (s *OAuthClientDataKeysSuite) rotated() *auth.Sealer {
	keyring, err := auth.NewSealerWithKeyring(2, map[int]string{1: "oauth client data keys test key", 2: "the next key"})
	s.Require().NoError(err)
	return keyring
}

// openUnder opens blob under the plain data key key, with the AAD of kind for the test's app.
func (s *OAuthClientDataKeysSuite) openUnder(key, blob []byte, kind string, rowAAD []byte) string {
	block, err := aes.NewCipher(key)
	s.Require().NoError(err)
	aead, err := cipher.NewGCM(block)
	s.Require().NoError(err)
	plain, err := aead.Open(nil, blob[:aead.NonceSize()], blob[aead.NonceSize():], appkeystest.RecordAAD(kind, s.app, rowAAD))
	s.Require().NoError(err)
	return string(plain)
}

func (s *OAuthClientDataKeysSuite) kekOpens() (float64, float64) {
	return appkeystest.KEKOpens(s.T(), clientSecretKind), appkeystest.KEKOpens(s.T(), signingSecretKind)
}

func (s *OAuthClientDataKeysSuite) zeroKEKOpensSince(client, signing float64) {
	s.Equal(client, appkeystest.KEKOpens(s.T(), clientSecretKind))
	s.Equal(signing, appkeystest.KEKOpens(s.T(), signingSecretKind))
}
