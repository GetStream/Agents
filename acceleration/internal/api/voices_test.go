//go:build integration

package api

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GetStream/Vision-Agents/acceleration/internal/live"
	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/tts/voices"
)

type VoicesSuite struct {
	RouterSuite
}

func TestVoicesSuite(t *testing.T) {
	suite := new(VoicesSuite)
	// The cloner's own model, reached by name only, so what it claims decides who may hold a voice.
	suite.ttsConfig = routableConfig()
	suite.ttsConfig.Providers = append(suite.ttsConfig.Providers, routing.ProviderConfig{
		Provider: "elevenlabs", Model: "eleven_flash_v2_5", Languages: []string{"la"},
		DataPolicy: options.DataHandling{TrainsOnData: options.ClaimYes, Retention: options.RetentionNone},
	})
	runSuite(t, suite)
}

// SetupTest gives every test an app of its own, because the voices listed are everything
// one customer brought with them.
func (s *VoicesSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *VoicesSuite) TestAVoiceIsRecordedPreparedAndReadBack() {
	created := s.createVoice(map[string]any{
		"name": "founder", "description": "the one from the ad",
	})
	s.Require().NotEmpty(created.Id)
	s.Equal("founder", created.Name)
	s.Empty(value(created.Samples), "a voice starts with nothing recorded")

	recorded := s.record(created.Id, map[string]any{
		"audio":      base64.StdEncoding.EncodeToString([]byte("pretend this is speech")),
		"filename":   "clip.wav",
		"transcript": "hello", "content_type": "audio/wav",
	})
	s.Require().Len(value(recorded.Samples), 1)
	s.EqualValues(22, value(value(recorded.Samples)[0].Bytes))

	prepared := s.prepare(created.Id)
	s.Require().Len(value(prepared.Bindings), 1)
	s.Equal(VoiceBindingStateReady, value(prepared.Bindings)[0].State)
	s.Equal("el-cloned", value(value(prepared.Bindings)[0].ExternalId),
		"a session names the voice, and the provider is asked for its own id")
}

func (s *VoicesSuite) TestVoiceProvidersAreTheOnesThisDeploymentCanCloneWith() {
	var listed VoiceProviders
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/voices/providers", nil, &listed))

	s.Equal([]string{"elevenlabs"}, listed.Providers)
}

func (s *VoicesSuite) TestAPreparedVoiceCanBeHeardThroughItsProvider() {
	created := s.createVoice(map[string]any{"name": "founder"})
	s.record(created.Id, map[string]any{
		"audio":    base64.StdEncoding.EncodeToString([]byte("pretend this is speech")),
		"filename": "clip.wav",
	})
	s.prepare(created.Id)

	var preview VoicePreview
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost,
		"/v1/agents/voices/"+created.Id+"/preview",
		map[string]any{"provider": "elevenlabs"}, &preview))

	s.Equal("elevenlabs", preview.Provider)
	s.Equal("audio/mpeg", preview.ContentType)
	s.Equal([]byte("spoken"), preview.Audio)
}

func (s *VoicesSuite) TestAVoiceCannotBeHeardThroughAProviderThatDoesNotHaveIt() {
	created := s.createVoice(map[string]any{"name": "founder"})

	status, failure := s.serverClient.failure(http.MethodPost,
		"/v1/agents/voices/"+created.Id+"/preview", map[string]any{"provider": "elevenlabs"})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "not ready with elevenlabs")
}

func (s *VoicesSuite) TestAskingForFishsLibrarySaysHowToSpeakInAFishVoice() {
	status, failure := s.serverClient.failure(http.MethodGet, "/v1/agents/voices/library?provider=fish", nil)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "fish voices cannot be browsed through the router yet; name a fish voice by its model id")
}

func (s *VoicesSuite) TestAProviderWithNoLibraryIsStillRefusedAsBefore() {
	status, failure := s.serverClient.failure(http.MethodGet, "/v1/agents/voices/library?provider=breeze", nil)

	// As the base answered it (1b4404556).
	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "voices: breeze publishes no voice library")
}

// The deployment's provider account lists every customer's clones as its own.
func (s *VoicesSuite) TestAnotherCustomersCloneIsNeitherListedNorHeard() {
	s.library.Register("elevenlabs", accountVoices{
		{Provider: "elevenlabs", ID: "el-cloned", Name: "founder", Own: true},
		{Provider: "elevenlabs", ID: "EXAVITQu4vr4xnSDxMaL", Name: "Sarah"},
	})
	created := s.createVoice(map[string]any{"name": "founder"})
	s.record(created.Id, map[string]any{
		"audio":    base64.StdEncoding.EncodeToString([]byte("pretend this is speech")),
		"filename": "clip.wav",
	})
	s.prepare(created.Id)
	s.Equal([]string{"el-cloned", "EXAVITQu4vr4xnSDxMaL"}, s.libraryIDs(), "its owner lists it")

	s.useApp(s.data.createApp())

	s.Equal([]string{"EXAVITQu4vr4xnSDxMaL"}, s.libraryIDs())
	status, _ := s.serverClient.failure(http.MethodGet,
		"/v1/agents/voices/library/elevenlabs/el-cloned/preview", nil)
	s.Equal(http.StatusNotFound, status)
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/voices/library/elevenlabs/EXAVITQu4vr4xnSDxMaL/preview", nil, nil))
}

func (s *VoicesSuite) libraryIDs() []string {
	var listed LibraryVoices
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/voices/library?provider=elevenlabs", nil, &listed))
	ids := make([]string, 0, len(listed.Voices))
	for _, voice := range listed.Voices {
		ids = append(ids, voice.Id)
	}
	return ids
}

// accountVoices is a provider account's library: what it lists, and a preview of any of it.
type accountVoices []voices.Library

func (a accountVoices) List(context.Context) ([]voices.Library, error) { return a, nil }

func (a accountVoices) Preview(context.Context, string) (voices.Speech, error) {
	return voices.Speech{Audio: []byte("sample"), ContentType: "audio/mpeg"}, nil
}

func (s *VoicesSuite) TestAVoiceWithNothingRecordedCannotBePrepared() {
	created := s.createVoice(map[string]any{"name": "founder"})

	status, failure := s.serverClient.failure(http.MethodPost,
		"/v1/agents/voices/"+created.Id+"/prepare", map[string]any{})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "add a recording")
}

func (s *VoicesSuite) TestAVoiceThePolicyAllowsNoModelForIsTheCallersToChange() {
	created := s.createVoice(map[string]any{"name": "founder"})
	s.record(created.Id, map[string]any{
		"audio":    base64.StdEncoding.EncodeToString([]byte("pretend this is speech")),
		"filename": "clip.wav",
	})
	s.prepare(created.Id)
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/policies/app",
		map[string]any{"allowed_models": []string{}}, nil))

	for path, body := range map[string]any{
		"/prepare": map[string]any{},
		"/preview": map[string]any{"provider": "elevenlabs"},
	} {
		status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/voices/"+created.Id+path, body)
		s.Equal(http.StatusBadRequest, status, path)
		s.Contains(failure, "your policy does not allow this model", path)
	}
}

func (s *VoicesSuite) TestAVoiceNoProviderMayHoldIsTheCallersToChange() {
	created := s.createVoice(map[string]any{"name": "founder"})
	s.record(created.Id, map[string]any{
		"audio":    base64.StdEncoding.EncodeToString([]byte("pretend this is speech")),
		"filename": "clip.wav",
	})
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/policies/app",
		map[string]any{"data_policy": map[string]any{"retention": "none"}}, nil))
	s.prepare(created.Id)
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/policies/app",
		map[string]any{"data_policy": map[string]any{"allow_training": false}}, nil))

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/voices/"+created.Id+"/prepare", map[string]any{})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "no provider meets your data policy")
}

func (s *VoicesSuite) TestAVoiceNeedsAName() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/voices",
		map[string]any{"name": "  "})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "needs a name")
}

func (s *VoicesSuite) TestADeletedVoiceStopsBeingListed() {
	created := s.createVoice(map[string]any{"name": "founder"})

	s.Require().Equal(http.StatusNoContent,
		s.serverClient.do(http.MethodDelete, "/v1/agents/voices/"+created.Id, nil, nil))

	var listed []Voice
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/agents/voices", nil, &listed))
	s.Empty(listed)
}

func (s *VoicesSuite) TestAVoiceBelongingToAnotherAppIsNotThere() {
	created := s.createVoice(map[string]any{"name": "founder"})

	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodGet, "/v1/agents/voices/"+created.Id, nil, nil)
	})
}

func (s *VoicesSuite) TestOnlyTheAppsOwnBackendMayBringAVoice() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		return as.do(http.MethodPost, "/v1/agents/voices",
			map[string]any{"name": "voice-" + s.utils.uuid()}, nil)
	})
}

func (s *VoicesSuite) TestVoiceProvidersAreRankedSeparatelyFromTranscribers() {
	// A speech-to-text failure must not make the text-to-speech provider look unhealthy.
	s.Require().NoError(s.live.RecordRequest(context.Background(), live.Usage{
		Modality: "stt", CustomerID: s.customerID(),
		Provider: "stub", Model: "stub-model",
		LatencyMs: 150, Success: false,
	}))

	var providers []Provider
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/tts/providers", nil, &providers))
	s.Require().NotEmpty(providers)

	for _, provider := range providers {
		if provider.Model == "stub-model" {
			s.Zero(provider.Health.Errors, "health is keyed by modality")
		}
	}
}

func (s *VoicesSuite) TestATwentyMegabyteRecordingIsKept() {
	created := s.createVoice(map[string]any{"name": "founder"})

	// Sent base64 as it is: about 27 MB of body.
	recorded := s.record(created.Id, map[string]any{
		"audio":    base64.StdEncoding.EncodeToString(make([]byte, 20<<20)),
		"filename": "clip.wav", "content_type": "audio/wav",
	})
	s.Require().Len(value(recorded.Samples), 1)
	s.EqualValues(20<<20, value(value(recorded.Samples)[0].Bytes))
}

func (s *VoicesSuite) TestAVoiceTheDatabaseCannotDeleteIsAnsweredAsInternalWithNothingOfTheDatabase() {
	created := s.createVoice(map[string]any{"name": "founder"})
	s.failQueries(`UPDATE "voices"`)

	status, body := s.serverClient.call(http.MethodDelete, "/v1/agents/voices/"+created.Id, nil)

	s.Equal(http.StatusInternalServerError, status)
	s.Contains(string(body), `"code":"internal_error"`)
	s.NotContains(string(body), "voices")
	s.NotContains(string(body), "deadline")
}

func (s *VoicesSuite) TestARecordingTheDatabaseCannotKeepIsAnsweredAsInternalWithNothingOfTheDatabase() {
	created := s.createVoice(map[string]any{"name": "founder"})
	s.failQueries(`INSERT INTO "voice_samples"`)

	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/voices/"+created.Id+"/samples",
		map[string]any{"audio": base64.StdEncoding.EncodeToString([]byte("pretend this is speech")), "filename": "clip.wav"})

	s.Equal(http.StatusInternalServerError, status)
	s.Contains(string(body), `"code":"internal_error"`)
	s.NotContains(string(body), "voice_samples")
	s.NotContains(string(body), "deadline")
}

func (s *VoicesSuite) TestARecordingWithNoAudioIsRefusedSayingSo() {
	created := s.createVoice(map[string]any{"name": "founder"})

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/agents/voices/"+created.Id+"/samples",
		map[string]any{"audio": "", "filename": "clip.wav"})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "no audio in it")
}

func (s *VoicesSuite) TestAVoiceWhoseRecordingsTheDatabaseCannotReadIsAnsweredAsInternalWhenPrepared() {
	created := s.createVoice(map[string]any{"name": "founder"})
	s.record(created.Id, map[string]any{
		"audio":    base64.StdEncoding.EncodeToString([]byte("pretend this is speech")),
		"filename": "clip.wav",
	})
	s.failQueries(`FROM "voice_samples"`)

	status, body := s.serverClient.call(http.MethodPost, "/v1/agents/voices/"+created.Id+"/prepare", map[string]any{})

	s.Equal(http.StatusInternalServerError, status)
	s.Contains(string(body), `"code":"internal_error"`)
	s.NotContains(string(body), "voice_samples")
	s.NotContains(string(body), "deadline")
}

func (s *VoicesSuite) TestAVoiceCannotBePreparedWithAProviderThatDoesNotClone() {
	created := s.createVoice(map[string]any{"name": "founder"})

	status, failure := s.serverClient.failure(http.MethodPost,
		"/v1/agents/voices/"+created.Id+"/prepare", map[string]any{"providers": []string{"breeze"}})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "breeze cannot be given a voice of your own")
}

func (s *VoicesSuite) TestALibraryNoProviderCouldReadIsUnavailableInNoneOfTheVendorsWords() {
	s.library.Register("murky", unreadableVoices{})

	status, body := s.serverClient.call(http.MethodGet, "/v1/agents/voices/library?provider=murky", nil)

	s.Equal(http.StatusServiceUnavailable, status)
	s.NotContains(string(body), "murky.invalid")
	s.NotContains(string(body), "overloaded")
}

func (s *VoicesSuite) TestALibraryVoiceTheProviderFailedToSpeakIsAnsweredAsInternalInNoneOfItsWords() {
	s.library.Register("murky", unreadableVoices{})

	status, body := s.serverClient.call(http.MethodGet, "/v1/agents/voices/library/murky/v1/preview", nil)

	s.Equal(http.StatusInternalServerError, status)
	s.Contains(string(body), `"code":"internal_error"`)
	s.NotContains(string(body), "murky.invalid")
	s.NotContains(string(body), "overloaded")
}

func (s *VoicesSuite) TestALibraryVoiceItsProviderDoesNotHaveIsNotFoundInNoneOfTheVendorsWords() {
	s.registerLostLibrary()

	status, body := s.serverClient.call(http.MethodGet, "/v1/agents/voices/library/lost/gone/preview", nil)

	s.Equal(http.StatusNotFound, status, string(body))
	s.Contains(string(body), `"code":"not_found"`)
	s.NotContains(string(body), "ws-81c")
	s.NotContains(string(body), "127.0.0.1")

	status, body = s.serverClient.call(http.MethodGet, "/v1/agents/voices/library/lost/busy/preview", nil)

	s.Equal(http.StatusInternalServerError, status, "a vendor that failed is not a voice that is missing")
	s.NotContains(string(body), "ws-81c")
}

// ElevenLabs has answered a missing voice with a 400 as well as a 404, and a 400 is also what a
// bad key gets: only the code in its body tells them apart.
func (s *VoicesSuite) TestALibraryVoiceElevenLabsSaysIsNotFoundWithA400IsNotFound() {
	s.registerLostLibrary()

	status, body := s.serverClient.call(http.MethodGet, "/v1/agents/voices/library/lost/moved/preview", nil)

	s.Equal(http.StatusNotFound, status, string(body))
	s.Contains(string(body), `"code":"not_found"`)
	s.NotContains(string(body), "ws-81c")

	status, body = s.serverClient.call(http.MethodGet, "/v1/agents/voices/library/lost/keyless/preview", nil)

	s.Equal(http.StatusInternalServerError, status, "a 400 for a bad key is the router's, not a missing voice")
	s.NotContains(string(body), "ws-81c")
}

func (s *VoicesSuite) TestALibraryVoiceWithNoPublishedSampleIsNotFoundSayingSo() {
	s.registerLostLibrary()

	status, failure := s.serverClient.failure(http.MethodGet, "/v1/agents/voices/library/lost/silent/preview", nil)

	s.Equal(http.StatusNotFound, status)
	s.Contains(failure, "no sample")
}

// registerLostLibrary registers, as provider lost, an ElevenLabs account on a local server
// whose voices are: gone (404), moved (400 voice_not_found), keyless (400 for a bad key),
// silent (no sample published), and any other id answered with a 500.
func (s *VoicesSuite) registerLostLibrary() {
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/voices/gone":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"detail":{"status":"voice_not_found","message":"no voice in workspace ws-81c"}}`))
		case "/v1/voices/moved":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"detail":{"status":"voice_not_found","code":"voice_not_found",` +
				`"message":"no voice in workspace ws-81c"}}`))
		case "/v1/voices/keyless":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"detail":{"status":"invalid_api_key","code":"invalid_api_key",` +
				`"message":"voice not found: invalid key for workspace ws-81c"}}`))
		case "/v1/voices/silent":
			_, _ = w.Write([]byte(`{"voice_id":"silent","preview_url":""}`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"detail":"overloaded in workspace ws-81c"}`))
		}
	}))
	s.T().Cleanup(vendor.Close)
	lister, err := voices.NewElevenLabs(voices.ElevenLabsOptions{APIKey: "key", BaseURL: vendor.URL})
	s.Require().NoError(err)
	s.library.Register("lost", lister)
}

// unreadableVoices is a provider account whose vendor answers 500 to everything.
type unreadableVoices struct{}

func (unreadableVoices) List(context.Context) ([]voices.Library, error) {
	return nil, errors.New(`murky: GET https://murky.invalid/v1/voices: 500: {"error":"overloaded"}`)
}

func (unreadableVoices) Preview(context.Context, string) (voices.Speech, error) {
	return voices.Speech{}, errors.New(`murky: GET https://murky.invalid/v1/voices/v1: 500: {"error":"overloaded"}`)
}

// createVoice brings a voice of the customer's own.
func (s *VoicesSuite) createVoice(body map[string]any) Voice {
	var created Voice
	s.Require().Equal(http.StatusCreated,
		s.serverClient.do(http.MethodPost, "/v1/agents/voices", body, &created))
	return created
}

// record adds a sample to a voice.
func (s *VoicesSuite) record(id string, sample map[string]any) Voice {
	var recorded Voice
	s.Require().Equal(http.StatusCreated,
		s.serverClient.do(http.MethodPost, "/v1/agents/voices/"+id+"/samples", sample, &recorded))
	return recorded
}

// prepare clones a voice at every provider that can hold one.
func (s *VoicesSuite) prepare(id string) Voice {
	var prepared Voice
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodPost, "/v1/agents/voices/"+id+"/prepare", map[string]any{}, &prepared))
	return prepared
}
