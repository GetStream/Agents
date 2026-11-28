//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/Vision-Agents/acceleration/internal/appkeys/appkeystest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/conversation/chattest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/streamapp"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// forwardedToken is a server token the gateway would forward for an app, lasting a day.
func forwardedToken() (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"exp": time.Now().Add(24 * time.Hour).Unix()}).
		SignedString([]byte("the-apps-own-secret"))
}

// tokenRowAAD is what streamapp binds an app's token to: its purpose, customer, app and key, each
// prefixed with its length. It is written out here, so a change that tokens already sealed cannot
// follow fails here.
func tokenRowAAD(customer string, app int64, apiKey string) []byte {
	var aad []byte
	for _, part := range []string{"accelerate:stream-app-token:v1", customer, strconv.FormatInt(app, 10), apiKey} {
		aad = binary.BigEndian.AppendUint32(aad, uint32(len(part)))
		aad = append(aad, part...)
	}
	return aad
}

// TestStreamAppsActsWithATokenUnderTheAppsDataKey: the stream-apps commands build their Stream
// clients with data keys of their own. Without them a token sealed under an app's data key does
// not open, and the command cannot act in that app.
func (s *StreamAppsCLISuite) TestStreamAppsActsWithATokenUnderTheAppsDataKey() {
	fake := serveFakeCloudKMS(s.T())
	settings := s.settings
	settings.Auth.AppKeysKMSKey = testKMSKey
	app := 1_000_000_000 + rand.Int64N(8_000_000_000)
	customer := streamapp.CustomerOf(app)
	token, err := forwardedToken()
	s.Require().NoError(err)
	key := appkeystest.AddKey(s.T(), s.store, fake.kms.KMS, customer, 1)
	sealed := appkeystest.Seal(s.T(), key, []byte(token),
		appkeystest.RecordAAD("stream_app_token", customer, tokenRowAAD(customer, app, "forwarded-key")))
	_, err = s.store.StreamAppTokens.Keep(s.ctx, &store.StreamAppToken{StreamAppPK: app, CustomerID: customer,
		APIKey: "forwarded-key", Sealed: sealed, DataKeyVersion: new(1), ExpiresAt: time.Now().Add(24 * time.Hour)})
	s.Require().NoError(err)

	var acted string
	err = withStored(s.ctx, settings, slog.New(slog.DiscardHandler), func(clients *streamapp.Clients, _ *store.Store) error {
		bound, err := clients.For(s.ctx, customer)
		acted = bound.Identity.Token.Reveal()
		return err
	})

	s.Require().NoError(err)
	s.Equal(token, acted)
}

// TestARunningRouterKeepsAForwardedTokenUnderTheAppsDataKey: run() hands the data keys to the
// Stream clients. With sealing on, the token the gateway forwards with a request is kept under
// the app's data key. Clients without the data keys would keep it under the router's key.
func (s *RouterStartSuite) TestARunningRouterKeepsAForwardedTokenUnderTheAppsDataKey() {
	stream := chattest.NewServer(s.T())
	stream.SetAppFor("deploy-key", chattest.App{ID: 1})
	settings := s.settings
	settings.Addr = freeAddress(s.T())
	settings.Auth.Mode, settings.Auth.ProxyDeclaresKind = string(auth.Proxy), true
	settings.Limits.Mode = config.LimitsOff
	settings.Stream = config.Stream{APIKey: "deploy-key", APISecret: "deploy-secret", BaseURL: stream.URL,
		Tenancy: config.TenancyApp, AppID: 1, MintURL: "http://gateway-internal:8082", MintToken: "the-routers-bearer"}
	setDevKEK(s.T(), "stream tokens wiring key")
	serveFakeCloudKMS(s.T())
	settings.Auth.AppKeysKMSKey, settings.Auth.AppKeysSeal = testKMSKey, true
	s.start(settings)
	app := 1_000_000_000 + rand.Int64N(8_000_000_000)
	token, err := forwardedToken()
	s.Require().NoError(err)

	request, err := http.NewRequest(http.MethodGet, "http://"+settings.Addr+"/v1/agents/skills", nil)
	s.Require().NoError(err)
	request.Header.Set(auth.AppHeader, streamapp.CustomerOf(app))
	request.Header.Set(auth.AuthTypeHeader, auth.AuthTypeServer)
	request.Header.Set(auth.ServerTokenHeader, token)
	request.Header.Set(auth.ServerTokenKeyHeader, "forwarded-key")
	response, err := http.DefaultClient.Do(request)
	s.Require().NoError(err)
	s.Require().NoError(response.Body.Close())

	kept, err := s.db.StreamAppTokens.Get(s.ctx, app)
	s.Require().NoError(err)
	s.Equal(new(1), kept.DataKeyVersion, "the token is kept under the app's data key")
	s.Zero(kept.KEKVersion)
}

// keepTokenUnderDataKey keeps the gateway's token for a new app under the app's data key and
// returns the app and the token.
func keepTokenUnderDataKey(t testing.TB, db *store.Store, kms *appkeystest.KMS) (int64, string) {
	app := 1_000_000_000 + rand.Int64N(8_000_000_000)
	customer := streamapp.CustomerOf(app)
	token, err := forwardedToken()
	require.NoError(t, err)
	key := appkeystest.AddKey(t, db, kms, customer, 1)
	sealed := appkeystest.Seal(t, key, []byte(token),
		appkeystest.RecordAAD("stream_app_token", customer, tokenRowAAD(customer, app, "forwarded-key")))
	_, err = db.StreamAppTokens.Keep(context.Background(), &store.StreamAppToken{StreamAppPK: app, CustomerID: customer,
		APIKey: "forwarded-key", Sealed: sealed, DataKeyVersion: new(1), ExpiresAt: time.Now().Add(24 * time.Hour)})
	require.NoError(t, err)
	return app, token
}

// TestNumbersAreMovedForAnAppWithATokenUnderItsDataKey: phone move builds its Stream clients with
// data keys. Without them the token of the app the call is in does not open, and ending the call
// fails.
func (s *PhoneMoveSuite) TestNumbersAreMovedForAnAppWithATokenUnderItsDataKey() {
	fake := serveFakeCloudKMS(s.T())
	app, _ := keepTokenUnderDataKey(s.T(), s.store, fake.kms.KMS)
	settings := config.Defaults()
	settings.Postgres.DSN = testenv.Database(os.Getenv("ROUTER_POSTGRES_DSN"), "phone_move")
	settings.PublicURL = moveTestPublicURL
	settings.Auth.AppKeysKMSKey = testKMSKey
	settings.Stream = config.Stream{APIKey: "deploy-key", APISecret: "deploy-secret", BaseURL: s.chat.URL,
		Tenancy: config.TenancyApp, AppID: 1, MintURL: "http://gateway-internal:8082", MintToken: "the-routers-bearer"}
	s.T().Setenv(devKEKEnvVar, moveTestKEK)
	service, db, closeKeys, err := numberMover(s.ctx, settings, slog.New(slog.DiscardHandler))
	s.Require().NoError(err)
	defer db.Close()
	defer closeKeys()

	err = service.EndCall(s.ctx, streamapp.CustomerOf(app), app, "default", "a-call")

	s.NoError(err)
}

// TestBackfillOwnerKindsReadsTheChannelOfAnAppWithATokenUnderItsDataKey: backfill-owner-kinds
// builds its Stream clients with data keys. Without them the token does not open, and the walk
// fails the conversation instead of finding that its channel is gone.
func (s *StreamAppsCLISuite) TestBackfillOwnerKindsReadsTheChannelOfAnAppWithATokenUnderItsDataKey() {
	fake := serveFakeCloudKMS(s.T())
	settings := s.settings
	settings.Auth.AppKeysKMSKey = testKMSKey
	app, _ := keepTokenUnderDataKey(s.T(), s.store, fake.kms.KMS)
	customer := streamapp.CustomerOf(app)
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &store.AgentSession{
		ID: uuid.NewString(), CustomerID: customer, StreamAppPK: app, AgentID: "backfill-agent", ConversationID: "agent:backfill-agent",
		AgentName: "docs", UserID: "user", CallerKind: "user", State: store.SessionRunning,
	}))
	var out bytes.Buffer

	err := runBackfillOwnerKinds(s.ctx, []string{"--customer", customer}, settings, slog.New(slog.DiscardHandler), &out)

	s.Require().NoError(err, out.String())
	s.Contains(out.String(), customer+"\tagent:backfill-agent\t-\tskipped:no owner\n")
}

// TestReplicateKeepsAnImportedPinToTheAppWhoseTokenIsUnderItsDataKey: replicate builds its Stream
// clients with data keys. Without them it cannot tell which app the customer acts in, and the
// import stops.
func (s *StreamAppsCLISuite) TestReplicateKeepsAnImportedPinToTheAppWhoseTokenIsUnderItsDataKey() {
	fake := serveFakeCloudKMS(s.T())
	settings := s.settings
	settings.Auth.AppKeysKMSKey = testKMSKey
	app, _ := keepTokenUnderDataKey(s.T(), s.store, fake.kms.KMS)
	customer := streamapp.CustomerOf(app)
	session := uuid.NewString()
	s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &store.AgentSession{
		ID: session, CustomerID: customer, StreamAppPK: app, AgentID: "agent", AgentName: "docs", UserID: "user",
		CallerKind: "user", State: store.SessionRunning,
	}))
	var export bytes.Buffer
	cursor, err := s.store.ExportCustomer(s.ctx, customer, func(table string, row json.RawMessage) error {
		line, err := json.Marshal(map[string]any{"table": table, "row": row})
		export.Write(append(line, '\n'))
		return err
	})
	s.Require().NoError(err)
	fmt.Fprintf(&export, `{"cursor":%d,"customer":%q}`+"\n", cursor, customer)
	_, err = s.store.GlobalDB().ExecContext(s.ctx, "DELETE FROM agent_sessions WHERE id = ?", session)
	s.Require().NoError(err)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/data/export":
			_, _ = w.Write(export.Bytes())
		case "/v1/data/changes":
			fmt.Fprintf(w, `{"changes":[],"cursor":%d,"caught_up":true}`, cursor)
		default:
			http.NotFound(w, r)
		}
	}))
	s.T().Cleanup(source.Close)

	err = runReplicate([]string{"--from", source.URL, "--api-key", "source-key", "--api-secret", "source-secret",
		"--as", customer, "--once"}, settings, slog.New(slog.DiscardHandler))

	s.Require().NoError(err)
	var pin *int64
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(s.ctx,
		"SELECT stream_app_pk FROM agent_sessions WHERE id = ?", session).Scan(&pin))
	s.Require().NotNil(pin)
	s.Equal(app, *pin, "the row stays pinned to the app its customer acts in")
}
