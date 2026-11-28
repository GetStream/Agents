//go:build integration

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/fakeprovider"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/schemes/bearer"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/sources/mcp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/mcpevents"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/webhook"
)

// subscriptionKind is the record label, written out so a test fails when a rename would leave
// dashboards without their series.
const subscriptionKind = "connection_event_subscriptions"

// ConnectionEventDataKeysSuite is MCP event subscriptions with auth.app_keys_seal on: the suite's
// router seals and opens their secrets under the apps' data keys, which an in-process KMS wraps,
// and its worker subscribes at the fake provider. A test that needs another router (no data keys,
// sealing off, a KMS that fails) builds an mcpevents service of its own and never starts it.
//
// The suite has a database of its own, because its worker claims every subscription due in its
// database. It does not run in parallel, so the counters it reads move only with its own tests.
type ConnectionEventDataKeysSuite struct {
	RouterSuite
	provider *fakeprovider.Server
	kms      *appkeystest.KMS
	// token is an access token the fake issued.
	token string
	// servedAtStart is whether both counters were served before any test of the suite counted.
	servedAtStart bool
	// revived is the worker a test starts after it closed the suite's own.
	revived *mcpevents.Service
}

func TestConnectionEventDataKeysSuite(t *testing.T) {
	suite.Run(t, new(ConnectionEventDataKeysSuite))
}

func (s *ConnectionEventDataKeysSuite) SetupSuite() {
	s.servedAtStart = appkeystest.Served(s.T(), subscriptionKind)
	s.database = "connection_event_keys"
	s.provider = fakeprovider.New(s.T(), fakeprovider.ClientCredentials, fakeprovider.MCPEvents)
	s.connectors = core.Registry{
		Schemes:     map[string]core.Scheme{bearer.Name: bearer.New()},
		ToolSources: map[string]core.ToolSource{mcp.Kind: mcp.New()},
	}
	s.connectorHTTP = s.provider.Client()
	s.mcpEventsOn = true
	s.kms = appkeystest.NewKMS()
	s.dataKeys, s.sealDataKeys = s.kms, true
	s.RouterSuite.SetupSuite()
	s.token = issuedToken(&s.RouterSuite, s.provider)
}

func (s *ConnectionEventDataKeysSuite) TearDownSuite() {
	if s.revived != nil {
		s.revived.Close()
	}
}

func (s *ConnectionEventDataKeysSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *ConnectionEventDataKeysSuite) TestTheSubscriptionRecordIsCountedFromTheStart() {
	s.True(s.servedAtStart)
}

// AC-6: the router seals a new subscription's secret under the app's data key. The fake checks
// the callback with a signed challenge before it subscribes, so an active row means the callback
// opened the secret under the data key, and an event signed with it is taken.
func (s *ConnectionEventDataKeysSuite) TestASubscriptionUnderTheAppsDataKeyIsVerifiedAndItsEventIsTaken() {
	connection := s.connection()
	s.declare(connection)
	opens := appkeystest.KEKOpens(s.T(), subscriptionKind)

	s.validate(connection)
	row := s.active(connection)

	s.Equal(new(1), row.SecretDataKeyVersion)
	s.Equal(1, row.KEKVersion, "the table's CHECK needs a key version beside a sealed secret")
	_, err := s.sealer.OpenWithAADVersion(row.SecretSealed, subscriptionAAD(row), 1)
	s.Error(err, "the router's key does not open the secret")
	answered := s.provider.Emit(issueCreated, map[string]any{"title": "checkout broke"})
	s.Equal(http.StatusAccepted, answered[s.remote(row).ID])
	s.Equal(opens, appkeystest.KEKOpens(s.T(), subscriptionKind))
	s.Equal(row.SecretSealed, s.row(connection).SecretSealed, "a data-key secret is not sealed again on use")
}

// A row a router without data keys made, while its connection waited on a renewal, moves under the data key at the worker's first look,
// before the fake is asked, so only that look counts an open under the router's key. The fake
// gets the same secret the row held.
func (s *ConnectionEventDataKeysSuite) TestARouterKeySubscriptionMovesUnderTheDataKeyOnItsFirstUseAndCountsOnce() {
	old, secret := s.parked(s.service(nil, s.sealer), s.sealer)
	s.Require().Nil(old.SecretDataKeyVersion)
	opens := appkeystest.KEKOpens(s.T(), subscriptionKind)

	_, err := s.store.GlobalDB().NewUpdate().Model((*store.ConnectorConnection)(nil)).
		Set("status = ?", store.ConnectionConnected).Where("id = ?", old.ConnectionID).Exec(s.T().Context())
	s.Require().NoError(err)
	_, err = s.store.GlobalDB().NewUpdate().Model((*store.ConnectionEventSubscription)(nil)).
		Set("next_attempt_at = now()").Where("id = ?", old.ID).Exec(s.T().Context())
	s.Require().NoError(err)
	row := s.active(old.ConnectionID)

	s.Equal(new(1), row.SecretDataKeyVersion)
	s.Equal(1, row.KEKVersion)
	s.Equal(secret, s.remote(row).Secret)
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), subscriptionKind))
	answered := s.provider.Emit(issueCreated, map[string]any{"title": "checkout broke"})
	s.Equal(http.StatusAccepted, answered[s.remote(row).ID])
	s.Equal(opens+1, appkeystest.KEKOpens(s.T(), subscriptionKind))
}

// Only a delivery signed with the secret moves it, so knowing the token is not enough to make
// the router create a data key or rewrite the row. The move keeps the router's current key
// version beside the data key version: here version 2, where the row was sealed under 1.
func (s *ConnectionEventDataKeysSuite) TestOnlyASignedDeliveryMovesTheSecretAndTheMoveKeepsTheCurrentKeyVersion() {
	first, err := auth.NewSealerWithKeyring(1, map[int]string{1: "first subscription test key"})
	s.Require().NoError(err)
	second, err := auth.NewSealerWithKeyring(2, map[int]string{1: "first subscription test key", 2: "second subscription test key"})
	s.Require().NoError(err)
	old, secret := s.parked(s.service(nil, first), first)
	router := s.service(s.keys(true), second)
	wraps := s.kms.Wraps()

	other, err := webhook.NewSecret()
	s.Require().NoError(err)
	s.Equal(http.StatusUnauthorized, s.verify(router, old, other).Status)
	s.Equal(old.SecretSealed, s.subscription(old.Token).SecretSealed)
	s.Equal(wraps, s.kms.Wraps(), "an unsigned delivery creates no data key")

	s.Equal(http.StatusOK, s.verify(router, old, secret).Status)
	moved := s.subscription(old.Token)
	s.Equal(new(1), moved.SecretDataKeyVersion)
	s.Equal(2, moved.KEKVersion)
	s.Equal(http.StatusOK, s.verify(router, moved, secret).Status, "the moved secret opens under the data key")
}

// A key fault at the callback is a 503, which the server retries, in words that name no KMS key
// or setting. The row is kept, so a router that can read the key answers the next try.
func (s *ConnectionEventDataKeysSuite) TestAKeyFaultAtTheCallbackIsA503InFixedWordsAndKeepsTheRow() {
	sealed, secret := s.parked(s.service(s.keys(true), s.sealer), s.sealer)
	s.Require().NotNil(sealed.SecretDataKeyVersion)
	s.kms.Fail(errors.New("kms: permission denied on the data key"))
	s.T().Cleanup(func() { s.kms.Fail(nil) })

	for name, router := range map[string]*mcpevents.Service{
		"KMS fails":    s.service(s.keys(true), s.sealer),
		"no data keys": s.service(nil, s.sealer),
	} {
		reply := s.verify(router, sealed, secret)
		s.Equal(http.StatusServiceUnavailable, reply.Status, name)
		s.Equal(map[string]string{"error": "the subscription's secret cannot be read right now, so send the delivery again later"},
			reply.Body, name)
	}
	s.kms.Fail(nil)
	s.Equal(sealOf(sealed), sealOf(s.subscription(sealed.Token)))
	s.Equal(http.StatusOK, s.verify(s.service(s.keys(true), s.sealer), sealed, secret).Status)
}

// A secret that names a data key version never falls back to the router's key, even when the
// router's key would open the blob.
func (s *ConnectionEventDataKeysSuite) TestASecretThatDoesNotOpenUnderItsDataKeyFailsClosed() {
	old, secret := s.parked(s.service(nil, s.sealer), s.sealer)
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.ConnectionEventSubscription)(nil)).
		Set("secret_data_key_version = 1").Where("id = ?", old.ID).Exec(s.T().Context())
	s.Require().NoError(err)
	// The connection's credentials are already sealed under the app's data key version 1.
	marked := s.subscription(old.Token)

	reply := s.verify(s.service(s.keys(true), s.sealer), marked, secret)

	s.Equal(http.StatusInternalServerError, reply.Status)
	s.Equal(map[string]string{"error": "something went wrong"}, reply.Body)
}

// The worker meets a secret whose data key KMS cannot unwrap. The server was asked nothing, so
// the subscription keeps its state, counts no refusal, and is looked at again after its lease.
func (s *ConnectionEventDataKeysSuite) TestAKeyFaultInTheWorkerIsRetriedAndNeverDropsTheSubscription() {
	connection := s.connection()
	s.declare(connection)
	s.validate(connection)
	row := s.active(connection)
	// Version 2 of the app's key is one the suite's router does not hold, so it must ask KMS.
	key := appkeystest.AddKey(s.T(), s.store, s.kms, s.customerID(), 2)
	blob := appkeystest.Seal(s.T(), key, []byte(s.remote(row).Secret),
		appkeystest.RecordAAD(subscriptionKind, s.customerID(), subscriptionAAD(row)))
	s.kms.Fail(errors.New("kms: unavailable"))
	s.T().Cleanup(func() { s.kms.Fail(nil) })
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.ConnectionEventSubscription)(nil)).
		Set("secret_sealed = ?", blob).Set("secret_data_key_version = 2").Set("next_attempt_at = now()").
		Where("id = ?", row.ID).Exec(s.T().Context())
	s.Require().NoError(err)

	s.Require().Eventually(func() bool { return s.row(connection).Attempts >= row.Attempts+2 }, settleFor,
		20*time.Millisecond, "the worker looks again after the lease")
	kept := s.row(connection)
	s.Equal(store.ConnectionEventActive, kept.Status)
	s.Zero(kept.Failures)
	s.Empty(kept.Error)
	s.Equal(row.RemoteID, kept.RemoteID)
	s.Equal(blob, kept.SecretSealed)
}

// The data key cannot be read because of the database or a deadline, not KMS. That is still a
// key fault: the server was asked nothing, so the subscription keeps its state, counts no
// refusal and stores no error text. The test makes the fault with a 1 ns KMS timeout.
func (s *ConnectionEventDataKeysSuite) TestAFailedReadOfTheDataKeyInTheWorkerIsRetriedAndNeverDropsTheSubscription() {
	connection := s.connection()
	s.declare(connection)
	s.validate(connection)
	row := s.active(connection)
	s.Require().NotNil(row.SecretDataKeyVersion)
	// The suite's worker would open the row with working keys, so another takes its place for
	// this test, and the suite's keys take over again when it ends.
	s.mcpEvents.Close()
	s.T().Cleanup(func() {
		s.revived = s.worker(s.appKeys)
		s.revived.Start()
	})
	failing := s.worker(s.keysTimingOut())
	failing.Start()
	s.T().Cleanup(failing.Close)
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.ConnectionEventSubscription)(nil)).
		Set("next_attempt_at = now()").Where("id = ?", row.ID).Exec(s.T().Context())
	s.Require().NoError(err)

	s.Require().Eventually(func() bool {
		rows, err := s.rows(connection)
		return err == nil && len(rows) == 1 && rows[0].Attempts >= row.Attempts+2
	}, settleFor, 20*time.Millisecond, "the worker looks again after the lease")
	kept := s.row(connection)
	s.Equal(store.ConnectionEventActive, kept.Status)
	s.Zero(kept.Failures)
	s.Empty(kept.Error)
	s.Equal(row.RemoteID, kept.RemoteID)
	s.Equal(sealOf(row), sealOf(kept))
}

// The same fault at the callback is a 503 in fixed words, which the server retries, and not a
// 500. The row is kept.
func (s *ConnectionEventDataKeysSuite) TestAFailedReadOfTheDataKeyAtTheCallbackIsA503InFixedWords() {
	sealed, secret := s.parked(s.service(s.keys(true), s.sealer), s.sealer)
	s.Require().NotNil(sealed.SecretDataKeyVersion)

	reply := s.verify(s.service(s.keysTimingOut(), s.sealer), sealed, secret)

	s.Equal(http.StatusServiceUnavailable, reply.Status)
	s.Equal(map[string]string{"error": "the subscription's secret cannot be read right now, so send the delivery again later"},
		reply.Body)
	s.Equal(sealOf(sealed), sealOf(s.subscription(sealed.Token)))
}

// A router-key secret is used while sealing is on and KMS cannot wrap a key. The move fails, and
// the delivery is still answered, with the row left as it was for the next try.
func (s *ConnectionEventDataKeysSuite) TestAFailedMoveLeavesTheRowAndStillAnswers() {
	old, secret := s.parked(s.service(nil, s.sealer), s.sealer)
	s.kms.Fail(errors.New("kms: unavailable"))
	s.T().Cleanup(func() { s.kms.Fail(nil) })

	s.Equal(http.StatusOK, s.verify(s.service(s.keys(true), s.sealer), old, secret).Status)
	s.Equal(sealOf(old), sealOf(s.subscription(old.Token)))
}

// Rollback: with sealing off, a secret under the data key still opens, a new one goes under the
// router's key, and a use moves nothing.
func (s *ConnectionEventDataKeysSuite) TestWithSealingOffADataKeySecretStillOpensAndNothingMoves() {
	sealed, secret := s.parked(s.service(s.keys(true), s.sealer), s.sealer)
	off := s.service(s.keys(false), s.sealer)

	s.Equal(http.StatusOK, s.verify(off, sealed, secret).Status)
	old, secret := s.parked(off, s.sealer)
	s.Nil(old.SecretDataKeyVersion)
	s.Equal(http.StatusOK, s.verify(off, old, secret).Status)
	s.Equal(sealOf(old), sealOf(s.subscription(old.Token)))
}

// KMS is down when a subscription is added: its secret goes under the router's key, the fallback
// is counted, and the add still succeeds.
func (s *ConnectionEventDataKeysSuite) TestASubscriptionAddedWhileKMSIsDownIsSealedUnderTheRoutersKey() {
	fallbacks := appkeystest.SealFallbacks(s.T(), subscriptionKind)
	s.kms.Fail(errors.New("kms: unavailable"))
	s.T().Cleanup(func() { s.kms.Fail(nil) })

	added, secret := s.parked(s.service(s.keys(true), s.sealer), s.sealer)

	s.Nil(added.SecretDataKeyVersion)
	s.Equal(fallbacks+1, appkeystest.SealFallbacks(s.T(), subscriptionKind))
	opened, err := s.sealer.OpenWithAADVersion(added.SecretSealed, subscriptionAAD(added), added.KEKVersion)
	s.Require().NoError(err)
	s.Equal(secret, opened)
}

// Control: a router without data keys seals and answers as the base does. The expected values
// come from the same steps run on the base, before data keys: key version 1, a secret that opens
// under the router's key, the echoed challenge, the 401 and its words, and a row left as it was.
func (s *ConnectionEventDataKeysSuite) TestWithoutDataKeysASubscriptionIsSealedAndAnsweredAsBefore() {
	router := s.service(nil, s.sealer)
	opens := appkeystest.KEKOpens(s.T(), subscriptionKind)

	sub, secret := s.parked(router, s.sealer)

	s.Equal(1, sub.KEKVersion)
	s.Nil(sub.SecretDataKeyVersion)
	s.True(strings.HasPrefix(secret, "whsec_"))
	ok := s.verify(router, sub, secret)
	s.Equal(http.StatusOK, ok.Status)
	s.Equal(map[string]string{"challenge": "challenge-" + sub.Token}, ok.Body)
	other, err := webhook.NewSecret()
	s.Require().NoError(err)
	refused := s.verify(router, sub, other)
	s.Equal(http.StatusUnauthorized, refused.Status)
	s.Equal(map[string]string{"error": "the delivery is not signed with the subscription's secret"}, refused.Body)
	s.Equal(sealOf(sub), sealOf(s.subscription(sub.Token)))
	s.Equal(opens+2, appkeystest.KEKOpens(s.T(), subscriptionKind), "each open under the router's key counts")
}

// service is another router's MCP events service over the suite's database, with dataKeys and
// sealer, that is never started.
func (s *ConnectionEventDataKeysSuite) service(dataKeys *appkeys.Keys, sealer *auth.Sealer) *mcpevents.Service {
	service, err := mcpevents.New(mcpevents.Options{Store: s.store, Sessions: s.manager, Registry: s.connectors,
		Transports: s.transports, Secrets: sealer, DataKeys: dataKeys, PublicURL: "https://router.example"})
	s.Require().NoError(err)
	return service
}

// worker is a started-by-the-test MCP events service over the suite's database that opens
// secrets with dataKeys, with the suite's lease and public URL.
func (s *ConnectionEventDataKeysSuite) worker(dataKeys *appkeys.Keys) *mcpevents.Service {
	service, err := mcpevents.New(mcpevents.Options{Store: s.store, Sessions: s.manager, Registry: s.connectors,
		Transports: s.transports, Secrets: s.sealer, DataKeys: dataKeys, PublicURL: s.server.URL, Lease: time.Second})
	s.Require().NoError(err)
	return service
}

// keysTimingOut are data keys whose KMS deadline is 1 ns, so reading the app's wrapped key from
// the store fails with a deadline before KMS is asked.
func (s *ConnectionEventDataKeysSuite) keysTimingOut() *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = true
	options.KMSTimeout = time.Nanosecond
	keys, err := appkeys.New(s.store, s.kms, options)
	s.Require().NoError(err)
	return keys
}

// keys are data keys wrapped by the suite's KMS, holding none of them yet, as another pod's.
func (s *ConnectionEventDataKeysSuite) keys(seal bool) *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = seal
	keys, err := appkeys.New(s.store, s.kms, options)
	s.Require().NoError(err)
	return keys
}

// parked is a subscription router added for a declared event, and its secret, opened under
// sealer or the app's data key. Its connection waits on a renewal, so the suite's worker never
// opens the secret or asks the fake for it.
func (s *ConnectionEventDataKeysSuite) parked(router *mcpevents.Service, sealer *auth.Sealer) (store.ConnectionEventSubscription, string) {
	connection := s.connection()
	s.declare(connection)
	connected := s.stored(connection)
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.ConnectorConnection)(nil)).
		Set("status = ?", store.ConnectionNeedsReauthorization).Where("id = ?", connection).Exec(s.T().Context())
	s.Require().NoError(err)
	s.Require().NoError(router.Reconcile(s.T().Context(), connected))
	sub := s.row(connection)
	var secret string
	if sub.SecretDataKeyVersion == nil {
		secret, err = sealer.OpenWithAADVersion(sub.SecretSealed, subscriptionAAD(sub), sub.KEKVersion)
	} else {
		var plain []byte
		plain, err = s.appKeys.Open(s.T().Context(), sub.CustomerID, *sub.SecretDataKeyVersion, sub.SecretSealed,
			appkeystest.RecordAAD(subscriptionKind, sub.CustomerID, subscriptionAAD(sub)))
		secret = string(plain)
	}
	s.Require().NoError(err)
	return s.subscription(sub.Token), secret
}

// verify is router's answer to a verification delivery to sub signed with secret.
func (s *ConnectionEventDataKeysSuite) verify(router *mcpevents.Service, sub store.ConnectionEventSubscription, secret string) mcpevents.Reply {
	body, err := json.Marshal(webhook.Event{Type: "verification", Challenge: "challenge-" + sub.Token})
	s.Require().NoError(err)
	now := time.Now()
	signature, err := webhook.Sign(secret, "msg_"+sub.Token, now, body)
	s.Require().NoError(err)
	header := http.Header{}
	header.Set("webhook-id", "msg_"+sub.Token)
	header.Set("webhook-timestamp", strconv.FormatInt(now.Unix(), 10))
	header.Set("webhook-signature", signature)
	return router.Receive(s.T().Context(), sub.Token, header, body)
}

// connection is a validated app connection to a new connector at the fake, holding the fake's
// token. No config binds it yet, so the validate subscribes to nothing.
func (s *ConnectionEventDataKeysSuite) connection() string {
	connector := "custom_crm" + strings.ReplaceAll(s.utils.uuid(), "-", "")
	manifest, err := core.ParseManifest([]byte(`
id: ` + connector + `
revision: 1
name: CRM
endpoints:
  mcp: ` + s.provider.URL + fakeprovider.PathMCP + `
schemes: [bearer]
sources:
  - kind: mcp
    endpoint: mcp
`))
	s.Require().NoError(err)
	_, err = s.store.ConnectorDefinitions.Revise(s.T().Context(), s.customerID(), manifest)
	s.Require().NoError(err)
	var created Connection
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/connections",
		withScheme(appOwned(connector), bearer.Name), &created))
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/connections/"+created.ID+"/credentials",
		map[string]any{"expected_revision": 1, "values": map[string]string{bearer.SuppliedToken: s.token}}, nil))
	s.validate(created.ID)
	return created.ID
}

// validate validates a connection through the API, which subscribes to what its configs declare.
func (s *ConnectionEventDataKeysSuite) validate(connection string) {
	var validation ConnectionValidation
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/connections/"+connection+"/validate",
		nil, &validation))
	s.Require().Equal(validationConnected, string(validation.Status), validation.Error)
}

// declare stores a config whose fixed binding of the connection declares issueCreated.
func (s *ConnectionEventDataKeysSuite) declare(connection string) {
	var read Connection
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/connections/"+connection, nil, &read))
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/configs", map[string]any{
		"name": "watcher-" + s.utils.uuid(), "mode": "text", "llm": "noted/noted-model",
		"connectors": []map[string]any{{"name": "crm", "connector_id": read.ConnectorID,
			"connection": map[string]any{"type": "fixed", "connection_id": connection}, "tools": []map[string]any{},
			"events": []map[string]any{{"event": issueCreated}}}},
	}, nil))
}

// stored is the connection as the store reads it.
func (s *ConnectionEventDataKeysSuite) stored(connection string) store.ConnectorConnection {
	stored, err := s.store.ConnectorConnections.Get(s.T().Context(), s.customerID(), connection)
	s.Require().NoError(err)
	return stored
}

// row is the connection's one subscription.
func (s *ConnectionEventDataKeysSuite) row(connection string) store.ConnectionEventSubscription {
	rows, err := s.rows(connection)
	s.Require().NoError(err)
	s.Require().Len(rows, 1)
	return rows[0]
}

// active waits for the connection's one subscription to be active, and returns it.
func (s *ConnectionEventDataKeysSuite) active(connection string) store.ConnectionEventSubscription {
	s.Require().Eventually(func() bool {
		rows, err := s.rows(connection)
		return err == nil && len(rows) == 1 && rows[0].Status == store.ConnectionEventActive
	}, settleFor, 20*time.Millisecond)
	return s.row(connection)
}

// rows reads the connection's subscriptions without failing the test, for a condition that
// Eventually checks on a goroutine of its own.
func (s *ConnectionEventDataKeysSuite) rows(connection string) ([]store.ConnectionEventSubscription, error) {
	var rows []store.ConnectionEventSubscription
	err := s.store.GlobalDB().NewSelect().Model(&rows).Where("customer_id = ?", s.customerID()).
		Where("connection_id = ?", connection).Scan(s.T().Context())
	return rows, err
}

// subscription is the row with token, as the store reads it now.
func (s *ConnectionEventDataKeysSuite) subscription(token string) store.ConnectionEventSubscription {
	sub, err := s.store.ConnectionEventSubscriptions.GetByToken(context.WithoutCancel(s.T().Context()), token)
	s.Require().NoError(err)
	return sub
}

// remote is the fake's subscription of the row.
func (s *ConnectionEventDataKeysSuite) remote(row store.ConnectionEventSubscription) fakeprovider.EventSubscription {
	for _, remote := range s.provider.EventSubscriptions() {
		if strings.HasSuffix(remote.URL, mcpevents.Path+row.Token) {
			return remote
		}
	}
	s.FailNow("the fake holds no subscription for the row")
	return fakeprovider.EventSubscription{}
}

// sealOf is what a row's secret is sealed as.
func sealOf(sub store.ConnectionEventSubscription) []any {
	return []any{sub.SecretSealed, sub.KEKVersion, sub.SecretDataKeyVersion}
}

// subscriptionAAD is mcpevents.secretAAD, written out so a change to it that stored rows cannot
// follow fails here.
func subscriptionAAD(sub store.ConnectionEventSubscription) []byte {
	return fmt.Appendf(nil, "accelerate:connection-event-subscription-secret:v1:%d:%s:%d:%s:%d:%s",
		len(sub.CustomerID), sub.CustomerID, len(sub.ConnectionID), sub.ConnectionID, len(sub.Token), sub.Token)
}
