//go:build integration

package main

import (
	"crypto/rand"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
)

// A SIP trunk added through the API with sealing on has its password sealed under the app's
// data key, which another router opens through Cloud KMS. A router that did not hand its data
// keys to the phone service seals it under the router's key, and dials through it would then
// never move to the data key.
func (s *RouterStartSuite) TestARunningRouterSealsASIPTrunkPasswordUnderTheAppsDataKey() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	fake := serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey, settings.Auth.AppKeysSeal = testKMSKey, true
	customer := "customer-" + rand.Text()
	dialPhrase := strings.Repeat("w", 12)
	s.start(settings)

	status, body := s.ask(http.MethodPost, "http://"+settings.Addr+"/v1/phone/trunks", customer,
		map[string]any{"name": "main", "host": "trunk.example.com", "username": "agent", "password": dialPhrase})
	s.Require().Equal(http.StatusCreated, status, "%s", body)
	s.NotContains(string(body), dialPhrase)
	var created struct{ ID string }
	s.Require().NoError(json.Unmarshal(body, &created))
	trunk, err := s.db.SIPTrunks.Get(s.ctx, customer, created.ID)
	s.Require().NoError(err)
	s.Equal(new(1), trunk.PasswordDataKeyVersion, "the password is under the app's data key")
	s.Zero(trunk.PasswordKEKVersion)

	other, closeOther, err := newAppKeys(s.ctx, settings, s.db)
	s.Require().NoError(err)
	s.T().Cleanup(closeOther)
	opened, err := other.Open(s.ctx, customer, 1, trunk.PasswordSealed,
		appkeystest.RecordAAD("sip_trunk", customer, []byte("sip_trunks/"+customer+"/"+trunk.ID)))
	s.Require().NoError(err)
	s.Equal(dialPhrase, string(opened))
	s.Equal(int64(1), fake.decrypts.Load())
}
