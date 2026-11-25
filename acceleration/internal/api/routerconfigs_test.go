//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

type RouterConfigsSuite struct {
	RouterSuite
}

func TestRouterConfigsSuite(t *testing.T) {
	runSuite(t, new(RouterConfigsSuite))
}

// SetupTest gives every test an app of its own, because a config's name has to be free.
func (s *RouterConfigsSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *RouterConfigsSuite) TestARouterConfigSurvivesBeingStoredAndReadBack() {
	created := s.createConfig(map[string]any{
		"name": "healthcare",
		"stt": map[string]any{
			"target": "en-low-latency", "providers": []string{"stub/stub-model", "en-low-latency"},
			"diarize": true, "keyterms": []string{"perioperative"},
		},
		"tts": map[string]any{"target": "en-low-latency", "voice": "aurora", "speed": 1.1},
		"llm": map[string]any{"providers": []string{"stub/stub-model", "llm-flow"}, "temperature": 0.2},
		"search": map[string]any{
			"providers": []string{"stub/stub-model", "en-low-latency"},
			"depth":     "standard", "include_domains": []string{"nice.org.uk"},
		},
	})
	s.Require().NotEmpty(created.Id)

	var read RouterConfig
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodGet, "/v1/router/configs/"+created.Id, nil, &read))

	s.Require().NotNil(read.Stt)
	s.Equal("en-low-latency", value(read.Stt.Target))
	s.Equal([]string{"stub/stub-model", "en-low-latency"}, value(read.Stt.Providers),
		"a priority list is the order it was written in, which is the point of writing one")
	s.Equal([]string{"perioperative"}, value(read.Stt.Keyterms))
	s.Require().NotNil(read.Tts)
	s.InDelta(1.1, value(read.Tts.Speed), 0.001)
	s.Require().NotNil(read.Llm)
	s.InDelta(0.2, value(read.Llm.Temperature), 0.001)
	s.Equal([]string{"stub/stub-model", "llm-flow"}, value(read.Llm.Providers))
	s.Require().NotNil(read.Search)
	s.Equal([]string{"nice.org.uk"}, value(read.Search.IncludeDomains))
	s.Equal([]string{"stub/stub-model", "en-low-latency"}, value(read.Search.Providers))
}

func (s *RouterConfigsSuite) TestAConfigFallingBackToASearchNobodyOffersIsRefused() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/router/configs",
		map[string]any{
			"name":   "clinic",
			"search": map[string]any{"providers": []string{"stub/stub-model", "altavista"}},
		})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "altavista")
}

func (s *RouterConfigsSuite) TestARouterConfigIsFoundByNameAsWellAsById() {
	// Naming a config is what a caller writes in their own code, so the id they never saw
	// cannot be the only way back to it.
	s.createConfig(map[string]any{
		"name": "clinic", "stt": map[string]any{"target": "en-low-latency"},
	})

	s.Equal(http.StatusAccepted, s.serverClient.do(http.MethodPost, "/v1/stt/recordings",
		map[string]any{
			"config_id": "clinic",
			"source":    map[string]any{"url": "https://example.test/call.mp3"},
		}, nil))
}

func (s *RouterConfigsSuite) TestUpdatingARouterConfigReplacesWhatItWas() {
	created := s.createConfig(map[string]any{
		"name": "clinic", "stt": map[string]any{"target": "en-low-latency", "diarize": true},
	})

	var updated RouterConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/router/configs/"+created.Id,
		map[string]any{"name": "clinic", "stt": map[string]any{"target": "llm-flow"}}, &updated))

	s.Equal(created.Id, updated.Id, "an update keeps the id callers already hold")
	s.Equal("llm-flow", value(updated.Stt.Target))
	s.Nil(updated.Stt.Diarize, "a field left out of a replacement is gone from it")
}

func (s *RouterConfigsSuite) TestAConfigNamingAVoiceThisDeploymentHasNeverHeardOfIsRefused() {
	// Storing it would leave a config that fails every call made under it, which is worth
	// hearing about while it is being written rather than once a socket is open.
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/router/configs",
		map[string]any{"name": "clinic", "tts": map[string]any{"providers": []string{"vocalizer"}}})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "vocalizer")
}

func (s *RouterConfigsSuite) TestAConfigWithOverwritesForAVoiceNobodyOffersIsRefused() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/router/configs",
		map[string]any{"name": "clinic", "tts": map[string]any{
			"overwrites": map[string]any{"vocalizer": map[string]any{"voice_id": "v-1"}},
		}})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "no voice for")
}

func (s *RouterConfigsSuite) TestAConfigAskingAVoiceForARetentionNothingCanBeComparedAgainstIsRefused() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/router/configs",
		map[string]any{"name": "clinic", "tts": map[string]any{
			"data_policy": map[string]any{"retention": "ages"},
		}})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "ages")
}

func (s *RouterConfigsSuite) TestAConfigHoldingASystemPromptForAConversationIsRefused() {
	// The agent that holds the conversation has instructions of its own and sends them
	// when it opens the session. A config that also carried some would overwrite them
	// from somewhere nobody thought to look.
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/router/configs",
		map[string]any{"name": "clinic", "sts": map[string]any{
			"target": "sts-fast", "instructions": "Be brief.",
		}})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "instructions")
}

func (s *RouterConfigsSuite) TestARouterConfigNobodyHasIsRefusedRatherThanIgnored() {
	// A caller that named a config meant it: transcribing at whatever the fallback happens
	// to be is not what they asked for.
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/stt/recordings",
		map[string]any{
			"config_id": "nope",
			"source":    map[string]any{"url": "https://example.test/call.mp3"},
		})

	s.Equal(http.StatusNotFound, status)
	s.Contains(failure, errUnknownRouterConfig.Message)
}

func (s *RouterConfigsSuite) TestAnotherAppsRouterConfigIsNotFound() {
	created := s.createConfig(map[string]any{
		"name": "clinic", "stt": map[string]any{"target": "en-low-latency"},
	})

	s.assertHiddenFromOtherApps(func(as *testClient) int {
		return as.do(http.MethodGet, "/v1/router/configs/"+created.Id, nil, nil)
	})
}

func (s *RouterConfigsSuite) TestOnlyTheAppsOwnBackendMayStoreARouterConfig() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		return as.do(http.MethodPost, "/v1/router/configs",
			map[string]any{"name": "config-" + s.utils.uuid()}, nil)
	})
}

// AI-3663: the hash of the file a sync wrote is on every read of the config.
func (s *RouterConfigsSuite) TestASyncedRouterConfigShowsItsHashOnEveryRead() {
	created := s.createConfig(s.syncedClinic("hash-one"))
	s.Equal("hash-one", created.SyncHash)

	s.Equal("hash-one", s.readConfig(created.Id).SyncHash)
	var listed []RouterConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/router/configs", nil, &listed))
	s.Require().Len(listed, 1)
	s.Equal("hash-one", listed[0].SyncHash)
}

func (s *RouterConfigsSuite) TestSyncingTheSameFileAgainWritesNothing() {
	created := s.createConfig(s.syncedClinic("hash-one"))
	// The create answers with the Go clock (ns on Linux); Postgres keeps µs. Compare with the stored row.
	stored := s.readConfig(created.Id)

	replaced := s.replaceConfig(created.Id, s.syncedClinic("hash-one"))

	s.Equal("hash-one", replaced.SyncHash)
	s.True(stored.UpdatedAt.Equal(replaced.UpdatedAt), "answered with the config as it was")
	s.True(stored.UpdatedAt.Equal(s.readConfig(created.Id).UpdatedAt), "and nothing was written")
}

func (s *RouterConfigsSuite) TestTheSameHashWithOtherContentIsStillWritten() {
	created := s.createConfig(s.syncedClinic("hash-one"))
	stored := s.readConfig(created.Id)
	changed := s.syncedClinic("hash-one")
	changed["stt"] = map[string]any{"target": "llm-flow"}

	s.replaceConfig(created.Id, changed)

	read := s.readConfig(created.Id)
	s.Equal("llm-flow", value(read.Stt.Target), "a body the hash does not describe is not dropped")
	s.True(read.UpdatedAt.After(stored.UpdatedAt))
	s.Empty(read.SyncHash, "nor kept under a hash that names another file")
}

// A client that reads the config, edits it and sends the whole read back sends the hash too.
func (s *RouterConfigsSuite) TestAReadSentBackEditedClearsTheHash() {
	created := s.createConfig(s.syncedClinic("hash-one"))
	var read map[string]any
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/router/configs/"+created.Id, nil, &read))
	s.Require().Equal("hash-one", read["sync_hash"])
	read["stt"] = map[string]any{"target": "llm-flow"}

	s.replaceConfig(created.Id, read)

	after := s.readConfig(created.Id)
	s.Equal("llm-flow", value(after.Stt.Target))
	s.Empty(after.SyncHash)
}

func (s *RouterConfigsSuite) TestTheSameFileUnderAnotherHashRecordsTheNewHash() {
	created := s.createConfig(s.syncedClinic("hash-one"))

	s.replaceConfig(created.Id, s.syncedClinic("hash-two"))

	s.Equal("hash-two", s.readConfig(created.Id).SyncHash)
}

// A dashboard or API replace sends no hash, so the next sync of the file sees a hash it did
// not write, whether the edit changed the content or not.
func (s *RouterConfigsSuite) TestAReplaceThatIsNoSyncClearsTheHash() {
	for name, edit := range map[string]map[string]any{
		"same content":  {"name": "clinic", "stt": map[string]any{"target": "en-low-latency"}},
		"other content": {"name": "clinic", "stt": map[string]any{"target": "llm-flow"}},
	} {
		s.Run(name, func() {
			s.useApp(s.data.createApp())
			created := s.createConfig(s.syncedClinic("hash-one"))

			s.Empty(s.replaceConfig(created.Id, edit).SyncHash)

			s.Empty(s.readConfig(created.Id).SyncHash)
		})
	}
}

// The base's behaviour, checked on 89e9e0c5c: a replace without a hash is written each time,
// even with the same body, and no answer names a sync hash.
func (s *RouterConfigsSuite) TestAReplaceWithoutAHashIsWrittenEveryTime() {
	body := map[string]any{"name": "clinic", "stt": map[string]any{"target": "en-low-latency"}}
	var created map[string]any
	s.Require().Equal(http.StatusCreated, s.serverClient.do(http.MethodPost, "/v1/router/configs", body, &created))
	s.NotContains(created, "sync_hash")
	id := created["id"].(string)

	first := s.replaceConfig(id, body)
	second := s.replaceConfig(id, body)

	s.True(second.UpdatedAt.After(first.UpdatedAt), "the second replace was written too")
	var read map[string]any
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/router/configs/"+id, nil, &read))
	s.NotContains(read, "sync_hash")
}

func (s *RouterConfigsSuite) TestASyncHashThatIsNotPrintableASCIIOrTooLongIsRefused() {
	s.createConfig(s.syncedClinic(strings.Repeat("f", 128)))
	for name, hash := range map[string]string{
		"a space":      "hash one",
		"a NUL":        "hash\x00",
		"a non-ASCII":  "hashé",
		"129 of them":  strings.Repeat("f", 129),
		"a line break": "hash\n",
	} {
		s.Run(name, func() {
			s.useApp(s.data.createApp())
			stored := s.createConfig(s.syncedClinic(""))

			created := s.syncedClinic(hash)
			created["name"] = "other"
			status, _ := s.serverClient.failure(http.MethodPost, "/v1/router/configs", created)
			s.Equal(http.StatusBadRequest, status, "a create")
			status, _ = s.serverClient.failure(http.MethodPut, "/v1/router/configs/"+stored.Id, s.syncedClinic(hash))
			s.Equal(http.StatusBadRequest, status, "a replace")
		})
	}
}

// AI-3831: a checked sync is refused over a config no sync wrote that says something else,
// whether an edit outside sync cleared the hash or the config was never synced.
func (s *RouterConfigsSuite) TestACheckedSyncOverAConfigNoSyncWroteIsRefused() {
	edited := map[string]any{"name": "clinic", "stt": map[string]any{"target": "llm-flow"}}
	for name, write := range map[string]func() string{
		"edited after a sync": func() string {
			created := s.createConfig(s.syncedClinic("hash-one"))
			s.replaceConfig(created.Id, edited)
			return created.Id
		},
		"never synced": func() string { return s.createConfig(edited).Id },
	} {
		s.Run(name, func() {
			s.useApp(s.data.createApp())
			id := write()
			stored := s.readConfig(id)

			s.assertRefusedAsUnsynced(s.serverClient, http.MethodPut, "/v1/router/configs/"+id, s.checkedClinic("hash-two"))

			read := s.readConfig(id)
			s.Equal("llm-flow", value(read.Stt.Target), "a refused sync writes nothing")
			s.True(stored.UpdatedAt.Equal(read.UpdatedAt))
			s.Empty(read.SyncHash)
		})
	}
}

// A dashboard save of the content the file already holds loses nothing, so the checked sync
// goes through and records its hash, as one without the check does.
func (s *RouterConfigsSuite) TestACheckedSyncOfWhatAConfigNoSyncWroteAlreadySaysIsNotRefused() {
	created := s.createConfig(s.syncedClinic("hash-one"))
	s.replaceConfig(created.Id, s.syncedClinic(""))

	replaced := s.replaceConfig(created.Id, s.checkedClinic("hash-two"))

	s.Equal("hash-two", replaced.SyncHash)
	changed := s.checkedClinic("hash-three")
	changed["stt"] = map[string]any{"target": "llm-flow"}
	s.Equal("llm-flow", value(s.replaceConfig(created.Id, changed).Stt.Target),
		"the recorded hash is what lets the next checked sync of an edited file through")
}

func (s *RouterConfigsSuite) TestACheckedSyncOverASyncedConfigWritesTheFile() {
	created := s.createConfig(s.checkedClinic("hash-one"))
	s.Equal("hash-one", created.SyncHash, "a create has nothing stored to write over")
	changed := s.checkedClinic("hash-two")
	changed["stt"] = map[string]any{"target": "llm-flow"}

	replaced := s.replaceConfig(created.Id, changed)

	s.Equal("llm-flow", value(replaced.Stt.Target))
	s.Equal("hash-two", s.readConfig(created.Id).SyncHash)
}

// The base's behaviour, checked on 0e5197a7a: a sync without the check writes over an edit
// made outside sync.
func (s *RouterConfigsSuite) TestASyncWithoutTheCheckWritesOverAnEditOutsideSync() {
	created := s.createConfig(s.syncedClinic("hash-one"))
	s.replaceConfig(created.Id, map[string]any{"name": "clinic", "stt": map[string]any{"target": "llm-flow"}})

	replaced := s.replaceConfig(created.Id, s.syncedClinic("hash-two"))

	s.Equal("en-low-latency", value(replaced.Stt.Target))
	s.Equal("hash-two", s.readConfig(created.Id).SyncHash)
}

// A checked sync read the config through the cache and wrote it with an update made on the id
// alone, so an edit landing between the check and the write, one the read never saw, was
// written over. The edit here goes to the table behind the cache's back, as another replica's
// does before its invalidation arrives.
func (s *RouterConfigsSuite) TestACheckedSyncIsRefusedOverAnEditItsCheckNeverSaw() {
	created := s.createConfig(s.syncedClinic("hash-one"))
	s.readConfig(created.Id)
	_, err := s.store.GlobalDB().ExecContext(s.T().Context(),
		"UPDATE router_configs SET name = ?, sync_hash = '', updated_at = now() WHERE id = ?", "clinic-dash", created.Id)
	s.Require().NoError(err)
	changed := s.checkedClinic("hash-two")
	changed["stt"] = map[string]any{"target": "llm-flow"}

	s.assertRefusedAsUnsynced(s.serverClient, http.MethodPut, "/v1/router/configs/"+created.Id, changed)

	read := s.readConfig(created.Id)
	s.Equal("clinic-dash", read.Name, "the edit is kept, and the cached copy the check read is forgotten")
	s.Empty(read.SyncHash)
}

// Another app's config of the same name, edited outside sync, refuses only that app's sync.
func (s *RouterConfigsSuite) TestAnotherAppsEditOutsideSyncRefusesOnlyItsOwnCheckedSync() {
	stranger := s.data.backendOfAnotherApp()
	var theirs RouterConfig
	s.Require().Equal(http.StatusCreated, stranger.do(http.MethodPost, "/v1/router/configs",
		map[string]any{"name": "clinic", "stt": map[string]any{"target": "llm-flow"}}, &theirs))
	ours := s.createConfig(s.syncedClinic("hash-one"))
	changed := s.checkedClinic("hash-two")
	changed["stt"] = map[string]any{"target": "llm-flow"}

	s.Equal("llm-flow", value(s.replaceConfig(ours.Id, changed).Stt.Target))
	s.assertRefusedAsUnsynced(stranger, http.MethodPut, "/v1/router/configs/"+theirs.Id, s.checkedClinic("hash-two"))
}

// assertRefusedAsUnsynced asserts the refusal agent sync answers a checked sync with, so a
// client handles every checked sync's refusal in one place.
func (s *RouterSuite) assertRefusedAsUnsynced(client *testClient, method, path string, body any) {
	status, payload := client.call(method, path, body)
	s.Equal(http.StatusConflict, status, string(payload))
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answered), string(payload))
	s.Equal(codeUnsyncedChanges, answered.Error.Code)
}

// checkedClinic is syncedClinic as a sync that asks to be told of an edit outside sync sends it.
func (s *RouterConfigsSuite) checkedClinic(hash string) map[string]any {
	body := s.syncedClinic(hash)
	body["check_changes"] = true
	return body
}

// syncedClinic is a config as a sync sends it, with hash.
func (s *RouterConfigsSuite) syncedClinic(hash string) map[string]any {
	body := map[string]any{"name": "clinic", "stt": map[string]any{"target": "en-low-latency"}}
	if hash != "" {
		body["sync_hash"] = hash
	}
	return body
}

func (s *RouterConfigsSuite) replaceConfig(id string, body map[string]any) RouterConfig {
	var replaced RouterConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPut, "/v1/router/configs/"+id, body, &replaced))
	return replaced
}

func (s *RouterConfigsSuite) readConfig(id string) RouterConfig {
	var read RouterConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/router/configs/"+id, nil, &read))
	return read
}

// createConfig stores a router config the router must accept.
func (s *RouterConfigsSuite) createConfig(body map[string]any) RouterConfig {
	var created RouterConfig
	s.Require().Equal(http.StatusCreated,
		s.serverClient.do(http.MethodPost, "/v1/router/configs", body, &created))
	return created
}

func (s *RouterConfigsSuite) TestAnUncheckedSyncOverAnEditBehindTheCacheStillWrites() {
	created := s.createConfig(s.syncedClinic("hash-one"))
	s.readConfig(created.Id)
	_, err := s.store.GlobalDB().ExecContext(s.T().Context(),
		"UPDATE router_configs SET name = ?, sync_hash = '', updated_at = now() WHERE id = ?", "clinic-dash", created.Id)
	s.Require().NoError(err)
	replaced := s.replaceConfig(created.Id, s.syncedClinic("hash-two"))
	s.Equal("clinic", replaced.Name)
	s.Equal("hash-two", s.readConfig(created.Id).SyncHash)
}
