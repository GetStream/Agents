//go:build integration

package main

import (
	"crypto/rand"
	"io"
	"net/http"

	"github.com/GetStream/Vision-Agents/acceleration/internal/channels"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// TestARunningRouterMovesALinesCredentialsToTheAppsDataKeyOnItsWebhookCheck: run() hands the data
// keys to the service that answers channel deliveries. With sealing on, a line still under the
// router's key answers Meta's webhook check and moves to the app's data key on that use. A service
// without the data keys would leave the row as it was.
func (s *RouterStartSuite) TestARunningRouterMovesALinesCredentialsToTheAppsDataKeyOnItsWebhookCheck() {
	settings := s.proxySettings()
	settings.Addr = freeAddress(s.T())
	serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey, settings.Auth.AppKeysSeal = testKMSKey, true
	secrets, err := newSecretSealer(s.ctx, settings)
	s.Require().NoError(err)
	account := store.ChannelAccount{CustomerID: "customer-" + rand.Text(), Kind: string(channels.WhatsApp),
		E164: "+1555" + rand.Text()[:7], Token: "line-" + rand.Text()}
	s.Require().NoError(channels.Seal(s.ctx, secrets, nil, &account, channels.Secrets{Challenge: "verify-the-start"}))
	s.Require().NoError(s.db.ChannelAccounts.Upsert(s.ctx, &account))
	s.start(settings)

	response, err := http.Get("http://" + settings.Addr + channels.HookPath + account.Token +
		"?hub.mode=subscribe&hub.verify_token=verify-the-start&hub.challenge=42")
	s.Require().NoError(err)
	defer response.Body.Close() //nolint:errcheck // the test is done with it
	answered, err := io.ReadAll(response.Body)
	s.Require().NoError(err)

	s.Equal(http.StatusOK, response.StatusCode)
	s.Equal("42", string(answered))
	stored, err := s.db.ChannelAccounts.GetByToken(s.ctx, account.Token)
	s.Require().NoError(err)
	s.Equal(new(1), stored.SecretsDataKeyVersion, "the line moved to the app's data key on its use")
	s.Zero(stored.SecretsKEKVersion)
}
