//go:build integration

package llmrouter

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/openai/openai-go/v3"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llm/mock"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv/redisready"
)

// modelKeySealedOnBase is "sk-custom-sealed-on-the-base-9Fz4", which the API on the commit before
// data keys sealed for a model of acme-app, bound to the customer only, under "custom models
// base probe" at version 1.
const modelKeySealedOnBase = "sUIdpsJSrEJoqveGlTVx65iOry/UHOLM76/0P9DtJUZFE4MeiLbLMy/wPptOnebkDujjIcBxPDge7RbGjg=="

// CustomModelDataKeysSuite seals and opens custom model keys under the apps' data keys, wrapped
// by an in-process KMS. It reads models through the name cache a session reads through.
type CustomModelDataKeysSuite struct {
	suite.Suite
	ctx    context.Context
	store  *store.Store
	sealer *auth.Sealer
	kms    *appkeystest.KMS
	app    string
}

func TestCustomModelDataKeysSuite(t *testing.T) {
	suite.Run(t, new(CustomModelDataKeysSuite))
}

func (s *CustomModelDataKeysSuite) SetupSuite() {
	dsn, address := os.Getenv("ROUTER_POSTGRES_DSN"), os.Getenv("ROUTER_REDIS_ADDR")
	if dsn == "" || address == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN and ROUTER_REDIS_ADDR must be set")
	}
	s.ctx = context.Background()
	opened, err := redisready.Retry(address, func() (*store.Store, error) {
		return store.OpenShards(store.Options{DSN: testenv.Database(dsn, "llmrouter_custom"), Redis: store.Redis{Address: address}})
	})
	s.Require().NoError(err)
	s.Require().NoError(opened.Migrate(s.ctx))
	s.T().Cleanup(func() { s.Require().NoError(opened.Close()) })
	s.store = opened
	s.sealer, err = auth.NewSealerWithKeyring(1, map[int]string{1: "custom models base probe"})
	s.Require().NoError(err)
}

func (s *CustomModelDataKeysSuite) SetupTest() {
	s.app = "app-" + rand.Text()
	s.kms = appkeystest.NewKMS()
}

// keys are data keys wrapped by the suite's KMS, sealing new keys under them when seal is set.
func (s *CustomModelDataKeysSuite) keys(seal bool) *appkeys.Keys {
	options := appkeystest.Options()
	options.Seal = seal
	keys, err := appkeys.New(s.store, s.kms, options)
	s.Require().NoError(err)
	return keys
}

// create stores a model of the app's served at baseURL, its key sealed with keys as the API
// seals one.
func (s *CustomModelDataKeysSuite) create(keys *appkeys.Keys, baseURL, key string) store.CustomModel {
	model := store.CustomModel{ID: store.NewID(), CustomerID: s.app, Name: "tuned-" + strings.ToLower(rand.Text()[:8]),
		BaseURL: baseURL, Model: "acme/tuned"}
	s.Require().NoError(SealKey(s.ctx, s.sealer, keys, &model, key))
	s.Require().NoError(s.store.CustomModels.Insert(s.ctx, &model))
	return model
}

// read is the model as a session reads it, through the name cache.
func (s *CustomModelDataKeysSuite) read(model store.CustomModel) store.CustomModel {
	read, err := s.store.CustomModels.GetByName(s.ctx, model.CustomerID, model.Name)
	s.Require().NoError(err)
	return read
}

// keyOf is the key routing opens for model, with keys.
func (s *CustomModelDataKeysSuite) keyOf(keys *appkeys.Keys, model store.CustomModel) (string, error) {
	_, endpoint, err := NewCustomModels(s.store, s.sealer, keys, false).CustomModel(s.ctx, model.CustomerID, model.Name)
	return endpoint.APIKey, err
}

func (s *CustomModelDataKeysSuite) TestWithSealingOnAKeyIsSealedUnderTheAppsDataKeyAndOpensForItsModelAlone() {
	keys := s.keys(true)
	model := s.create(keys, "https://8.8.8.8/v1", "sk-own")

	s.Require().NotNil(model.DataKeyVersion)
	s.Zero(model.KEKVersion)
	_, err := s.sealer.OpenWithAADVersion(model.APIKeySealed, []byte(s.app), 1)
	s.Error(err, "the router's key does not open it")
	key, err := s.keyOf(keys, model)
	s.Require().NoError(err)
	s.Equal("sk-own", key)
	s.Equal(model.APIKeySealed, s.read(model).APIKeySealed, "a key under the data key is not sealed again")
	before := appkeystest.KEKOpens(s.T(), KeyRecord)
	other := s.create(nil, "https://8.8.8.8/v1", "sk-other")
	other.APIKeySealed, other.KEKVersion, other.DataKeyVersion = model.APIKeySealed, 0, model.DataKeyVersion
	s.Require().NoError(s.store.CustomModels.Update(s.ctx, &other))
	logs := captureLogs(s.T())

	_, err = s.keyOf(keys, other)
	s.ErrorIs(err, ErrCustomModelKey, "a key moved to another model of the same app does not open")
	s.Contains(keyFaultLogged(s.T(), logs, other.ID), appkeys.ErrDoesNotOpen.Error())
	s.Equal(before, appkeystest.KEKOpens(s.T(), KeyRecord), "an open under a data key is no KEK open")
}

func (s *CustomModelDataKeysSuite) TestAKeyIsSealedUnderTheRoutersKeyWhenKMSIsUnavailable() {
	s.kms.Fail(errors.New("kms: unavailable"))
	before := appkeystest.SealFallbacks(s.T(), KeyRecord)

	model := s.create(s.keys(true), "https://8.8.8.8/v1", "sk-own")

	s.Nil(model.DataKeyVersion)
	s.Equal(1, model.KEKVersion)
	opened, err := s.sealer.OpenWithAADVersion(model.APIKeySealed, []byte(s.app), 1)
	s.Require().NoError(err, "it is sealed bound to the customer, as before data keys")
	s.Equal("sk-own", opened)
	s.Equal(before+1, appkeystest.SealFallbacks(s.T(), KeyRecord))
}

// The cached model is read before the move, so a reseal that left the cache in place would
// serve the row under the router's key for another hour.
func (s *CustomModelDataKeysSuite) TestAKeyUnderTheRoutersKeyIsResealedOnItsFirstUseCountsOneKEKOpenAndDropsTheCachedModel() {
	model := s.create(nil, "https://8.8.8.8/v1", "sk-own")
	s.Nil(s.read(model).DataKeyVersion)
	keys := s.keys(true)
	before := appkeystest.KEKOpens(s.T(), KeyRecord)

	key, err := s.keyOf(keys, model)
	s.Require().NoError(err)
	s.Equal("sk-own", key)

	moved := s.read(model)
	s.Require().NotNil(moved.DataKeyVersion, "the cached model was dropped after the reseal")
	s.Zero(moved.KEKVersion)
	key, err = s.keyOf(keys, model)
	s.Require().NoError(err)
	s.Equal("sk-own", key)
	s.Equal(before+1, appkeystest.KEKOpens(s.T(), KeyRecord), "only the first use opened it under the router's key")
}

func (s *CustomModelDataKeysSuite) TestAKeyThatCannotBeResealedIsStillUsedAndLeftUnderTheRoutersKey() {
	model := s.create(nil, "https://8.8.8.8/v1", "sk-own")
	s.kms.Fail(errors.New("kms: unavailable"))

	key, err := s.keyOf(s.keys(true), model)

	s.Require().NoError(err)
	s.Equal("sk-own", key)
	stored, err := s.store.CustomModels.Get(s.ctx, s.app, model.ID)
	s.Require().NoError(err)
	s.Equal(model.APIKeySealed, stored.APIKeySealed)
	s.Nil(stored.DataKeyVersion)
	s.Equal(1, stored.KEKVersion)
}

// Each fault of a key under the data key fails the session, never a model served without its
// key: KMS fails, the router has no data keys, or the data key the row names is gone. The cause
// can name the KMS key or a setting, so only the log has it: a priority list answers the error's
// text to the caller.
func (s *CustomModelDataKeysSuite) TestAKeyUnderTheAppsDataKeyThatDoesNotOpenFailsTheSession() {
	model := s.create(s.keys(true), "https://8.8.8.8/v1", "sk-own")
	unknown := model
	unknown.ID, unknown.Name, unknown.DataKeyVersion = store.NewID(), model.Name+"-v9", new(9)
	s.Require().NoError(s.store.CustomModels.Insert(s.ctx, &unknown))
	failing := appkeystest.NewKMS()
	failing.Fail(errors.New("kms: cryptoKeys/k is unavailable"))
	options := appkeystest.Options()
	options.Seal = true
	cold, err := appkeys.New(s.store, failing, options)
	s.Require().NoError(err)

	for name, fault := range map[string]struct {
		model store.CustomModel
		keys  *appkeys.Keys
		cause string
	}{
		"kms fails":        {model, cold, "cryptoKeys/k is unavailable"},
		"no data keys":     {model, nil, "app_keys_kms_key"},
		"data key is gone": {unknown, s.keys(true), "appkeys"},
	} {
		s.Run(name, func() {
			logs := captureLogs(s.T())
			key, err := s.keyOf(fault.keys, fault.model)
			s.Require().ErrorIs(err, ErrCustomModelKey)
			s.NotErrorIs(err, routing.ErrRequest, "the caller can do nothing about it")
			s.Empty(key)
			s.Equal(ErrCustomModelKey.Error()+": "+routing.CustomProvider+"/"+fault.model.Name, err.Error(),
				"the error names the model and nothing of the key")
			s.Contains(keyFaultLogged(s.T(), logs, fault.model.ID), fault.cause)
		})
	}
}

// A priority list moves on to its next entry when a custom model's key does not open, and that
// drops the key's error. Its cause is still logged, or a key that never opens would go unseen.
func (s *CustomModelDataKeysSuite) TestAKeyThatDoesNotOpenIsLoggedWhenTheNextEntryOfAPriorityListServes() {
	model := s.create(s.keys(true), "https://8.8.8.8/v1", "sk-own")
	s.kms.Fail(errors.New("kms: cryptoKeys/k is unavailable"))
	registry := DefaultRegistry()
	RegisterMock(registry)
	config := routing.ModalityConfig{Providers: []routing.ProviderConfig{{Provider: mock.ProviderName, Model: "mock", Languages: []string{"en"}}}}
	router, err := New(Options{Registry: registry, Config: config, Store: s.store, Models: NewCustomModels(s.store, s.sealer, s.keys(true), true)})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)
	logs := captureLogs(s.T())

	session, err := router.Start(s.ctx, Request{CustomerID: s.app,
		Providers: []string{routing.CustomProvider + "/" + model.Name, mock.ProviderName + "/mock"}})

	s.Require().NoError(err, "the next entry serves")
	defer session.Close() //nolint:errcheck // the test is done with it
	s.Contains(keyFaultLogged(s.T(), logs, model.ID), "cryptoKeys/k is unavailable")
}

// An older build puts a key over a model under the data key: it writes a router-key blob and
// its kek_version, and leaves the data key version as it was.
func (s *CustomModelDataKeysSuite) TestAKeyPutByAnOlderBuildOverOneUnderTheDataKeyOpensAndMovesToTheDataKey() {
	keys := s.keys(true)
	model := s.create(keys, "https://8.8.8.8/v1", "sk-first")
	s.Require().NotNil(model.DataKeyVersion)
	blob, err := s.sealer.SealWithAAD("sk-second", []byte(s.app))
	s.Require().NoError(err)
	model.APIKeySealed, model.KEKVersion = blob, 1
	s.Require().NoError(s.store.CustomModels.Update(s.ctx, &model))
	s.Require().NotNil(s.read(model).DataKeyVersion, "the older build left the data key version")

	key, err := s.keyOf(keys, model)

	s.Require().NoError(err)
	s.Equal("sk-second", key)
	moved := s.read(model)
	s.NotEqual(blob, moved.APIKeySealed)
	s.Zero(moved.KEKVersion)
	key, err = s.keyOf(keys, model)
	s.Require().NoError(err)
	s.Equal("sk-second", key, "the key moved is the one the older build put")
}

// Sealing turned back off, as in a rollback of auth.app_keys_seal: keys under a data key keep
// opening, new keys go under the router's key, and nothing is moved.
func (s *CustomModelDataKeysSuite) TestWithSealingTurnedOffADataKeyModelStillOpensAndNothingIsResealed() {
	underDataKey := s.create(s.keys(true), "https://8.8.8.8/v1", "sk-data")
	off := s.keys(false)
	underKEK := s.create(off, "https://8.8.8.8/v1", "sk-kek")

	key, err := s.keyOf(off, underDataKey)
	s.Require().NoError(err)
	s.Equal("sk-data", key)
	key, err = s.keyOf(off, underKEK)
	s.Require().NoError(err)
	s.Equal("sk-kek", key)
	s.Nil(underKEK.DataKeyVersion)
	s.Equal(1, underKEK.KEKVersion)
	s.Equal(underKEK.APIKeySealed, s.read(underKEK).APIKeySealed, "nothing is resealed with sealing off")
}

// TestWithNothingConfiguredAKeyIsSealedAndOpensAsOnTheBase: the expected values come from the
// API's seal run on the commit before data keys (modelKeySealedOnBase).
func (s *CustomModelDataKeysSuite) TestWithNothingConfiguredAKeyIsSealedAndOpensAsOnTheBase() {
	blob, err := base64.StdEncoding.DecodeString(modelKeySealedOnBase)
	s.Require().NoError(err)
	onBase := store.CustomModel{CustomerID: "acme-app", Name: "base-" + strings.ToLower(rand.Text()[:8]),
		BaseURL: "https://8.8.8.8/v1", Model: "acme/tuned", APIKeySealed: blob, KEKVersion: 1}
	s.Require().NoError(s.store.CustomModels.Insert(s.ctx, &onBase))
	s.T().Cleanup(func() { _ = s.store.CustomModels.Delete(context.WithoutCancel(s.ctx), onBase.CustomerID, onBase.ID) })

	key, err := s.keyOf(nil, onBase)
	s.Require().NoError(err)
	s.Equal("sk-custom-sealed-on-the-base-9Fz4", key)
	s.Equal(blob, s.read(onBase).APIKeySealed, "a read writes nothing")
	model := store.CustomModel{ID: store.NewID(), CustomerID: "acme-app"}
	s.Require().NoError(SealKey(s.ctx, s.sealer, nil, &model, "sk-custom-sealed-on-the-base-9Fz4"))
	s.Equal(1, model.KEKVersion)
	s.Nil(model.DataKeyVersion)
	reopened, err := s.sealer.OpenWithAADVersion(model.APIKeySealed, []byte("acme-app"), 1)
	s.Require().NoError(err, "the base opens what is sealed now")
	s.Equal("sk-custom-sealed-on-the-base-9Fz4", reopened)
}

// openAIRefusal is what OpenAI answers a chat completion whose key it does not know.
const openAIRefusal = `{"error":{"message":"Incorrect API key provided: sk-dummy****. You can find your API key at ` +
	`https://platform.openai.com/account/api-keys.","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`

// The AC-3 shape: a model whose dummy key is sealed under the app's data key is routed on, and
// the endpoint's 401 reaches the caller as the vendor's failure, not as a key the router could
// not open.
func (s *CustomModelDataKeysSuite) TestAModelUnderTheAppsDataKeyGetsTheVendorsRefusalOfItsDummyKey() {
	var mu sync.Mutex
	var bearers []string
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		bearers = append(bearers, r.Header.Get("Authorization"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, openAIRefusal)
	}))
	s.T().Cleanup(vendor.Close)
	keys := s.keys(true)
	dummy := "sk-dummy-" + rand.Text()
	model := s.create(keys, vendor.URL+"/v1", dummy)
	s.Require().NotNil(model.DataKeyVersion, "the key is under the app's data key")
	// The mock is there only because a router needs one provider of its own.
	registry := DefaultRegistry()
	RegisterMock(registry)
	config := routing.ModalityConfig{Providers: []routing.ProviderConfig{{Provider: mock.ProviderName, Model: "mock", Languages: []string{"en"}}}}
	router, err := New(Options{Registry: registry, Config: config, Store: s.store, Models: NewCustomModels(s.store, s.sealer, keys, true)})
	s.Require().NoError(err)
	s.T().Cleanup(router.Close)

	session, err := router.Start(s.ctx, Request{CustomerID: s.app, Target: routing.CustomProvider + "/" + model.Name})
	s.Require().NoError(err)
	defer session.Close() //nolint:errcheck // the test is done with it
	stream, err := session.Create(s.ctx, llm.ResponseParams{Input: []llm.Message{{Role: llm.User, Content: "hello"}}})
	if err == nil {
		_, err = llm.Collect(stream)
	}

	s.Require().Error(err)
	s.NotErrorIs(err, ErrCustomModelKey)
	refusal, vendorFailed := errors.AsType[*openai.Error](err)
	s.Require().True(vendorFailed, "the endpoint's refusal reaches the caller: %v", err)
	s.Equal(http.StatusUnauthorized, refusal.StatusCode)
	s.Equal("invalid_api_key", refusal.Code)
	mu.Lock()
	defer mu.Unlock()
	s.Contains(bearers, "Bearer "+dummy, "the key opened under the data key was sent")
}

// lockedLog is a log the router's goroutines can write while the test reads it.
type lockedLog struct {
	mu    sync.Mutex
	lines bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lines.Write(p)
}

// captureLogs sends the default logger's lines to the log it returns, as JSON, until t ends.
func captureLogs(t *testing.T) *lockedLog {
	t.Helper()
	logs := &lockedLog{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return logs
}

// keyFaultLogged is the error logged for the key of the model with that id that did not open,
// and fails t when no such line was logged.
func keyFaultLogged(t *testing.T, logs *lockedLog, modelID string) string {
	t.Helper()
	logs.mu.Lock()
	defer logs.mu.Unlock()
	for line := range strings.SplitSeq(logs.lines.String(), "\n") {
		var logged struct {
			Msg     string `json:"msg"`
			Level   string `json:"level"`
			ModelID string `json:"model_id"`
			Error   string `json:"error"`
		}
		if json.Unmarshal([]byte(line), &logged) == nil && logged.Msg == "could not open a custom model's key" &&
			logged.ModelID == modelID {
			require.Equal(t, "ERROR", logged.Level)
			return logged.Error
		}
	}
	require.Failf(t, "no line logged the key fault", "model %s, logs:\n%s", modelID, logs.lines.String())
	return ""
}
