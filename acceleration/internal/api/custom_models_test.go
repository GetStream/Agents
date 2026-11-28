//go:build integration

package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys"
	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/llmrouter"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

type CustomModelsSuite struct {
	RouterSuite
}

func TestCustomModelsSuite(t *testing.T) {
	runSuite(t, new(CustomModelsSuite))
}

// SetupTest gives every test an app of its own, because the models listed are everything
// one customer brought.
func (s *CustomModelsSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *CustomModelsSuite) TestAModelIsStoredAndReadBackWithoutItsKey() {
	created := s.createModel(map[string]any{
		"name": "support-qwen", "base_url": "https://8.8.8.8/v1", "model": "Qwen/Qwen3.8-27B",
		"api_key": "sk-secret", "context_window": 131072, "per_million_input_tokens": 0.2,
		"trains_on_data": "no", "retention": "none",
	})

	var read CustomModel
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/models/"+created.Id, nil, &read))
	s.Equal("custom/support-qwen", read.Target)
	s.Equal("Qwen/Qwen3.8-27B", read.Model)
	s.True(read.HasApiKey)
	s.EqualValues(131072, read.ContextWindow)
	s.Equal("no", value(read.TrainsOnData))

	stored, err := s.store.CustomModels.Get(context.Background(), s.customerID(), created.Id)
	s.Require().NoError(err)
	s.NotContains(string(stored.APIKeySealed), "sk-secret", "the key is sealed at rest")
}

// Catches create or update sealing a key in a form the previous release cannot open, which
// breaks a rollback and the old pods of a rolling deploy: the key a model is created or updated
// with opens the way that release opens one, bound to the customer only.
func (s *CustomModelsSuite) TestTheKeyOpensAsThePreviousReleaseOpensIt() {
	created := s.createModel(map[string]any{
		"name": "bound", "base_url": "https://8.8.8.8/v1", "model": "a", "api_key": "sk-first",
	})
	s.Equal("sk-first", s.storedKey(created.Id))

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/models/"+created.Id,
		map[string]any{"name": "bound", "base_url": "https://8.8.8.8/v1", "model": "a", "api_key": "sk-second"}, nil))
	s.Equal("sk-second", s.storedKey(created.Id))
}

// storedKey opens the stored key of the model id the way the previous release opens it.
func (s *CustomModelsSuite) storedKey(id string) string {
	stored, err := s.store.CustomModels.Get(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	key, err := s.sealer.OpenWithAADVersion(stored.APIKeySealed, []byte(s.customerID()), stored.KEKVersion)
	s.Require().NoError(err)
	return key
}

func (s *CustomModelsSuite) TestModelsAreListedNewestFirstAPageAtATime() {
	first := s.createModel(map[string]any{"name": "first", "base_url": "https://8.8.8.8/v1", "model": "a"})
	second := s.createModel(map[string]any{"name": "second", "base_url": "https://8.8.8.8/v1", "model": "b"})

	var page CustomModelPage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/models?limit=1", nil, &page))
	s.Require().Len(page.Items, 1)
	s.Equal(second.Id, page.Items[0].Id)
	s.Require().True(page.HasMore)

	var next CustomModelPage
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/models?limit=1&cursor="+value(page.NextCursor), nil, &next))
	s.Require().Len(next.Items, 1)
	s.Equal(first.Id, next.Items[0].Id)
	s.False(next.HasMore)
}

func (s *CustomModelsSuite) TestAnUpdateWithoutAKeyKeepsTheStoredOne() {
	created := s.createModel(map[string]any{
		"name": "kept", "base_url": "https://8.8.8.8/v1", "model": "a", "api_key": "sk-secret",
	})

	var updated CustomModel
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/models/"+created.Id,
		map[string]any{"name": "renamed", "base_url": "https://8.8.8.8/v1", "model": "b"}, &updated))

	s.Equal("custom/renamed", updated.Target)
	s.True(updated.HasApiKey)
}

func (s *CustomModelsSuite) TestAnEmptyKeyRemovesTheStoredOne() {
	created := s.createModel(map[string]any{
		"name": "cleared", "base_url": "https://8.8.8.8/v1", "model": "a", "api_key": "sk-secret",
	})

	var updated CustomModel
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/models/"+created.Id,
		map[string]any{"name": "cleared", "base_url": "https://8.8.8.8/v1", "model": "a", "api_key": ""}, &updated))

	s.False(updated.HasApiKey)
}

func (s *CustomModelsSuite) TestAPrivateEndpointIsRefusedOnASharedRouter() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/models", map[string]any{
		"name": "internal", "base_url": "https://10.0.0.5/v1", "model": "a",
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "public https URL")
}

// TestEveryRefusalOfABaseURLIsOneAnswer has a name that does not resolve and a private address
// answered alike, in every field of the error: otherwise the answer tells a caller which names the router's
// resolver knows, and which of them sit on a private network.
func (s *CustomModelsSuite) TestEveryRefusalOfABaseURLIsOneAnswer() {
	refused := []string{
		"https://no-such-host.invalid/v1",
		"https://10.0.0.7/v1",
		"https://127.0.0.1/v1",
		"https://localhost/v1",
		"https://8.8.8.8:0/v1",
		"https://127.1/v1",
		"https://[fe80::1%25eth0]/v1",
		"http://8.8.8.8/v1",
		"https://user:secret@8.8.8.8/v1",
		"https://8.8.8.8/v1?key=1",
		"https://8.8.8.8/v1#part",
	}
	var first ErrorResponse
	for _, base := range refused {
		status, payload := s.serverClient.call(http.MethodPost, "/v1/agents/models", map[string]any{
			"name": "refused", "base_url": base, "model": "a",
		})
		s.Equal(http.StatusBadRequest, status, base)
		var answered ErrorResponse
		s.Require().NoError(json.Unmarshal(payload, &answered), base)
		s.Equal("invalid_request", answered.Error.Code, base)
		s.Equal("base_url must be a public https URL without userinfo, query or fragment", answered.Error.Message, base)
		s.NotContains(string(payload), "egress", base)
		if first.Error.Message == "" {
			first = answered
		}
		s.Equal(first.Error, answered.Error, base)
	}
}

// TestAnUpdateToAPrivateBaseURLGetsTheSameAnswer is the second caller of the check: the answer
// of an update does not depend on the reason either.
func (s *CustomModelsSuite) TestAnUpdateToAPrivateBaseURLGetsTheSameAnswer() {
	created := s.createModel(map[string]any{"name": "moved", "base_url": "https://8.8.8.8/v1", "model": "a"})
	answers := map[string]bool{}
	for _, base := range []string{"https://no-such-host.invalid/v1", "https://10.0.0.7/v1", "https://localhost/v1"} {
		status, failure := s.serverClient.failure(http.MethodPut, "/v1/agents/models/"+created.Id, map[string]any{
			"name": "moved", "base_url": base, "model": "a",
		})
		s.Equal(http.StatusBadRequest, status, base)
		answers[failure] = true
	}
	s.Equal(map[string]bool{"base_url must be a public https URL without userinfo, query or fragment": true}, answers)
}

func (s *CustomModelsSuite) TestHalfADataPolicyIsRefused() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/models", map[string]any{
		"name": "half", "base_url": "https://8.8.8.8/v1", "model": "a", "trains_on_data": "no",
	})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "retention")
}

func (s *CustomModelsSuite) TestASecondModelOfTheSameNameIsAConflict() {
	s.createModel(map[string]any{"name": "twice", "base_url": "https://8.8.8.8/v1", "model": "a"})

	s.Equal(http.StatusConflict, s.serverClient.do(http.MethodPost, "/v1/agents/models",
		map[string]any{"name": "twice", "base_url": "https://8.8.8.8/v1", "model": "b"}, nil))
}

func (s *CustomModelsSuite) TestADeletedModelIsGone() {
	created := s.createModel(map[string]any{"name": "gone", "base_url": "https://8.8.8.8/v1", "model": "a"})

	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/agents/models/"+created.Id, nil, nil))

	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/models/"+created.Id, nil, nil))
}

func (s *CustomModelsSuite) TestOnlyTheAppsOwnBackendMayAddAModel() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		return as.do(http.MethodPost, "/v1/agents/models", map[string]any{
			"name": "posture-" + s.utils.uuid(), "base_url": "https://8.8.8.8/v1", "model": "a",
		}, nil)
	})
}

func (s *CustomModelsSuite) TestAnotherAppsModelIsNotFound() {
	created := s.createModel(map[string]any{"name": "mine", "base_url": "https://8.8.8.8/v1", "model": "a"})

	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodGet, "/v1/agents/models/"+created.Id, nil, nil)
	})
}

func (s *CustomModelsSuite) TestARouterConfigMayNameAModelOfTheCustomersOwn() {
	s.Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/router/configs", map[string]any{
		"name": "own-" + s.utils.uuid(), "llm": map[string]any{"providers": []string{"custom/support-qwen"}},
	}, nil))
}

// This router has no data keys, so the open fails with appkeys.ErrNoKeys, whose text names a
// setting. A session that names the model is told only that something went wrong.
func (s *CustomModelsSuite) TestAKeyUnderADataKeyThisRouterCannotOpenFailsTheSessionWithoutSayingWhy() {
	version := 1
	model := store.CustomModel{CustomerID: s.customerID(), Name: "sealed-elsewhere", BaseURL: "https://8.8.8.8/v1",
		Model: "a", APIKeySealed: []byte("sealed under a data key"), DataKeyVersion: &version}
	s.Require().NoError(s.store.CustomModels.Insert(context.Background(), &model))

	answering := s.serverClient.opens("/v1/llm/stream")
	defer answering.Close() //nolint:errcheck // the test is done with it
	s.Require().NoError(answering.WriteJSON(map[string]any{"type": "start", "target": "custom/sealed-elsewhere"}))

	refused := s.await(answering, "error")
	s.Equal(somethingWentWrong, refused["error"])
}

// In a priority list whose other entry is refused, the answer and the error frame repeat the
// error of every entry. The key's error is one of them, so it must not carry the setting appkeys
// names.
func (s *CustomModelsSuite) TestAKeyUnderADataKeyThisRouterCannotOpenSaysNothingOfTheSettingInAPriorityList() {
	version := 1
	model := store.CustomModel{CustomerID: s.customerID(), Name: "sealed-elsewhere", BaseURL: "https://8.8.8.8/v1",
		Model: "a", APIKeySealed: []byte("sealed under a data key"), DataKeyVersion: &version}
	s.Require().NoError(s.store.CustomModels.Insert(context.Background(), &model))

	for _, answer := range keyFaultAnswers(&s.RouterSuite, model.Name) {
		for _, leaked := range []string{"appkeys", "app_keys_kms_key", s.customerID()} {
			s.NotContains(answer, leaked)
		}
	}
}

// keyFaultAnswers is what a caller is told for a priority list of the model of that name and a
// model the app does not have: the HTTP answer and the socket's error frame.
func keyFaultAnswers(s *RouterSuite, name string) []string {
	_, err := s.streams.LLM.Start(context.Background(), llmrouter.Request{CustomerID: s.customerID(),
		Providers: []string{"custom/" + name, "custom/not-there"}})
	s.Require().Error(err)
	answering := s.serverClient.opens("/v1/llm/stream")
	defer answering.Close() //nolint:errcheck // the test is done with it
	s.Require().NoError(answering.WriteJSON(map[string]any{"type": "start",
		"llm": map[string]any{"target": "custom/" + name, "providers": []string{"custom/" + name, "custom/not-there"}}}))
	frame, _ := s.await(answering, "error")["error"].(string)
	s.Require().NotEmpty(frame)
	return []string{refusedRequest(err).Error(), frame}
}

func (s *CustomModelsSuite) createModel(body map[string]any) CustomModel {
	var created CustomModel
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/models", body, &created))
	return created
}

// PrivateCustomModelsSuite is a self-hosted router that allows private endpoints.
type PrivateCustomModelsSuite struct {
	RouterSuite
}

func TestPrivateCustomModelsSuite(t *testing.T) {
	runSuite(t, new(PrivateCustomModelsSuite))
}

func (s *PrivateCustomModelsSuite) SetupSuite() {
	s.privateModels = true
	s.RouterSuite.SetupSuite()
}

func (s *PrivateCustomModelsSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *PrivateCustomModelsSuite) TestAPrivateEndpointIsStored() {
	for _, base := range []string{"https://10.0.0.7/v1", "http://10.0.0.7:8000/v1"} {
		var created CustomModel
		s.Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/models", map[string]any{
			"name": "vllm-" + s.utils.uuid(), "base_url": base, "model": "a",
		}, &created), base)
		s.Equal(base, created.BaseUrl, base)
	}
}

func (s *PrivateCustomModelsSuite) TestWhatIsNotAnHTTPURLIsRefused() {
	for _, base := range []string{"ftp://10.0.0.7/v1", "10.0.0.7/v1", "https://", "https://10.0.0.7:port/v1"} {
		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/models", map[string]any{
			"name": "refused", "base_url": base, "model": "a",
		})
		s.Equal(http.StatusBadRequest, status, base)
		s.Equal("base_url must be an http or https URL", failure, base)
	}
}

// CustomModelDataKeysSuite creates and updates models with auth.app_keys_seal on, the apps' data
// keys wrapped by an in-process KMS, and routes sessions on them.
type CustomModelDataKeysSuite struct {
	RouterSuite
	kms *appkeystest.KMS
}

func TestCustomModelDataKeysSuite(t *testing.T) {
	runSuite(t, new(CustomModelDataKeysSuite))
}

func (s *CustomModelDataKeysSuite) SetupSuite() {
	s.kms = appkeystest.NewKMS()
	s.dataKeys, s.sealDataKeys = s.kms, true
	s.RouterSuite.SetupSuite()
}

func (s *CustomModelDataKeysSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

// stored is the model id as the database keeps it.
func (s *CustomModelDataKeysSuite) stored(id string) store.CustomModel {
	stored, err := s.store.CustomModels.Get(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	return stored
}

// answered is what the model the session names answers: the stub says its endpoint, model and
// key.
func (s *CustomModelDataKeysSuite) answered(name string) string {
	answering := s.serverClient.opens("/v1/llm/stream")
	defer answering.Close() //nolint:errcheck // the test is done with it
	s.Require().NoError(answering.WriteJSON(map[string]any{"type": "start", "target": "custom/" + name}))
	s.await(answering, "started")
	s.Require().NoError(answering.WriteJSON(map[string]any{"type": "respond", "id": "r1",
		"messages": []map[string]any{{"role": "user", "content": "hello"}}}))
	text, _ := s.await(answering, "complete")["text"].(string)
	return text
}

// The id is part of what the key is sealed with, so a create that sealed before the id was made,
// or under another id, stores a key no session opens.
func (s *CustomModelDataKeysSuite) TestACreatedModelsKeyIsSealedUnderTheAppsDataKeyAndASessionUsesIt() {
	var created CustomModel
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/models", map[string]any{
		"name": "sealed", "base_url": "https://8.8.8.8/v1", "model": "acme/tuned", "api_key": "sk-first",
	}, &created))

	row := s.stored(created.Id)
	s.Require().NotNil(row.DataKeyVersion)
	s.Zero(row.KEKVersion)
	_, err := s.sealer.OpenWithAADVersion(row.APIKeySealed, []byte(s.customerID()), s.sealer.CurrentVersion())
	s.Error(err, "the router's key does not open it")
	s.Equal("https://8.8.8.8/v1 acme/tuned sk-first", s.answered("sealed"))
}

func (s *CustomModelDataKeysSuite) TestAnUpdatedKeyIsSealedUnderTheAppsDataKeyAndAnEmptyOneRemovesIt() {
	created := s.createModel(map[string]any{"name": "updated", "base_url": "https://8.8.8.8/v1", "model": "a"})
	s.Nil(s.stored(created.Id).DataKeyVersion)

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/models/"+created.Id,
		map[string]any{"name": "updated", "base_url": "https://8.8.8.8/v1", "model": "a", "api_key": "sk-second"}, nil))
	s.Require().NotNil(s.stored(created.Id).DataKeyVersion)
	s.Equal("https://8.8.8.8/v1 a sk-second", s.answered("updated"))

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/models/"+created.Id,
		map[string]any{"name": "updated", "base_url": "https://8.8.8.8/v1", "model": "a", "api_key": ""}, nil))
	row := s.stored(created.Id)
	s.Nil(row.APIKeySealed)
	s.Nil(row.DataKeyVersion, "a model without a key names no data key")
}

// An update that leaves api_key out keeps the stored key. Under a data key, that means the data
// key version too: a row with no router key version and no data key version opens under nothing,
// and every session on the model fails.
func (s *CustomModelDataKeysSuite) TestAnUpdateWithoutAKeyKeepsTheKeyUnderTheAppsDataKey() {
	created := s.createModel(map[string]any{"name": "kept", "base_url": "https://8.8.8.8/v1", "model": "a", "api_key": "sk-kept"})
	s.Require().NotNil(s.stored(created.Id).DataKeyVersion)

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/models/"+created.Id,
		map[string]any{"name": "kept-renamed", "base_url": "https://8.8.8.8/v1", "model": "a"}, nil))

	s.NotNil(s.stored(created.Id).DataKeyVersion, "the key stays under the data key")
	s.Equal("https://8.8.8.8/v1 a sk-kept", s.answered("kept-renamed"))
}

// The update puts its key while KMS is down, over a model whose key is under the data key: a
// data key version left on the row would make it open the new blob under the wrong key. Another
// router sealed the first key, so this one holds no data key of the app's and has to ask KMS.
func (s *CustomModelDataKeysSuite) TestAKeyPutWhileKMSIsUnavailableIsSealedUnderTheRoutersKey() {
	created := s.createModel(map[string]any{"name": "fallback", "base_url": "https://8.8.8.8/v1", "model": "a"})
	options := appkeystest.Options()
	options.Seal = true
	another, err := appkeys.New(s.store, s.kms, options)
	s.Require().NoError(err)
	row := s.stored(created.Id)
	s.Require().NoError(llmrouter.SealKey(context.Background(), s.sealer, another, &row, "sk-first"))
	s.Require().NoError(s.store.CustomModels.Update(context.Background(), &row))
	s.Require().NotNil(s.stored(created.Id).DataKeyVersion)
	s.kms.Fail(errors.New("kms: unavailable"))
	s.T().Cleanup(func() { s.kms.Fail(nil) })
	before := appkeystest.SealFallbacks(s.T(), llmrouter.KeyRecord)

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/agents/models/"+created.Id,
		map[string]any{"name": "fallback", "base_url": "https://8.8.8.8/v1", "model": "a", "api_key": "sk-second"}, nil))

	row = s.stored(created.Id)
	s.Nil(row.DataKeyVersion)
	s.Equal(s.sealer.CurrentVersion(), row.KEKVersion)
	s.Equal(before+1, appkeystest.SealFallbacks(s.T(), llmrouter.KeyRecord))
	opened, err := s.sealer.OpenWithAADVersion(row.APIKeySealed, []byte(s.customerID()), row.KEKVersion)
	s.Require().NoError(err, "it is sealed bound to the customer, as before data keys")
	s.Equal("sk-second", opened)
}

// The KMS error names the KMS key and the app. In a priority list whose other entry is refused,
// the caller is told every entry's error, so the key's error must not carry it.
func (s *CustomModelDataKeysSuite) TestAKeyUnderTheAppsDataKeySaysNothingOfKMSInAPriorityListWhenKMSFails() {
	created := s.createModel(map[string]any{"name": "under-the-data-key", "base_url": "https://8.8.8.8/v1", "model": "a"})
	options := appkeystest.Options()
	options.Seal = true
	another, err := appkeys.New(s.store, s.kms, options)
	s.Require().NoError(err)
	row := s.stored(created.Id)
	s.Require().NoError(llmrouter.SealKey(context.Background(), s.sealer, another, &row, "sk-first"))
	s.Require().NoError(s.store.CustomModels.Update(context.Background(), &row))
	s.kms.Fail(errors.New("kms: cryptoKeys/k is unavailable"))
	s.T().Cleanup(func() { s.kms.Fail(nil) })

	for _, answer := range keyFaultAnswers(&s.RouterSuite, "under-the-data-key") {
		for _, leaked := range []string{"appkeys", "cryptoKeys", "kms", s.customerID()} {
			s.NotContains(answer, leaked)
		}
	}
}

func (s *CustomModelDataKeysSuite) createModel(body map[string]any) CustomModel {
	var created CustomModel
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/agents/models", body, &created))
	return created
}
