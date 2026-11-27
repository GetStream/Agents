//go:build integration

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
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
