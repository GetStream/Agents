//go:build integration

package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// KeysCLISuite runs `router keys` against Postgres.
type KeysCLISuite struct {
	suite.Suite
	settings config.Config
}

func TestKeysCLISuite(t *testing.T) {
	suite.Run(t, new(KeysCLISuite))
}

func (s *KeysCLISuite) SetupTest() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN not set")
	}
	for _, name := range []string{devKEKEnvVar, "ROUTER_AUTH_KEK", "ROUTER_AUTH_KEK_V1"} {
		s.T().Setenv(name, "")
	}
	s.settings = config.Defaults()
	s.settings.Postgres.DSN = testenv.Database(dsn, "router_cli")
}

func (s *KeysCLISuite) TestCreateRefusesTheKeySourcesTheRouterRefuses() {
	cases := []struct {
		name, kmsKey, want string
		env                map[string]string
	}{
		{"the dev key beside a KMS key", testKMSKey,
			"ROUTER_DEV_KEK is for a laptop or CI and is never set with auth.kms_key (ROUTER_AUTH_KMS_KEY): unset ROUTER_DEV_KEK",
			map[string]string{"ROUTER_DEV_KEK": "first-key"}},
		{"a retired plain key", "",
			"ROUTER_AUTH_KEK is no longer read: set ROUTER_DEV_KEK on a laptop or in CI, or " +
				"ROUTER_AUTH_KEK_V1_WRAPPED with auth.kms_key (ROUTER_AUTH_KMS_KEY) on a hosted router",
			map[string]string{"ROUTER_AUTH_KEK": "first-key"}},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			settings := s.settings
			settings.Auth.KMSKey = c.kmsKey
			for name, value := range c.env {
				s.T().Setenv(name, value)
			}

			err := runKeys([]string{"create", "--app-id", "refused-app"}, settings, slog.New(slog.DiscardHandler))

			s.EqualError(err, c.want)
		})
	}
}

// seedKey stores an app with one key, sealed under a key of the test's own, and returns the
// store, how the router authenticates a key against it, the app and the key.
func (s *KeysCLISuite) seedKey(settings config.Config) (*store.Store, auth.Lookup, store.App, string) {
	ctx := context.Background()
	db, err := openStore(ctx, settings)
	s.Require().NoError(err)
	s.T().Cleanup(func() { db.Close() })
	sealer, err := auth.NewSealer("keys cli test key")
	s.Require().NoError(err)

	org := store.Organization{Name: "acme"}
	s.Require().NoError(db.Organizations.Insert(ctx, &org))
	app := store.App{OrganizationID: org.ID, Name: "production"}
	s.Require().NoError(db.Apps.Insert(ctx, &app))
	id, secret, err := auth.NewCredential(auth.Live)
	s.Require().NoError(err)
	sealed, err := sealer.Seal(secret)
	s.Require().NoError(err)
	s.Require().NoError(db.APIKeys.Insert(ctx, &store.APIKey{
		ID: id, AppID: app.ID, Name: "ci", Env: "live", Sealed: sealed, KEKVersion: sealer.CurrentVersion(), Last4: auth.Last4(secret),
	}))
	return db, db.APIKeys.Lookup(sealer), app, id
}

func (s *KeysCLISuite) TestARevokedKeyStopsAuthenticatingOnARouterThatHadItCached() {
	address := os.Getenv("ROUTER_REDIS_ADDR")
	if address == "" {
		s.T().Skip("ROUTER_REDIS_ADDR not set")
	}
	settings := s.settings
	settings.Redis.Addr = address
	_, lookup, app, key := s.seedKey(settings)
	ctx := context.Background()
	_, err := lookup(ctx, key)
	s.Require().NoError(err, "the router holds the key's owner before it is revoked")

	s.Require().NoError(runKeys([]string{"revoke", "--app", app.ID, "--key", key}, settings, slog.New(slog.DiscardHandler)))

	s.Eventually(func() bool {
		_, err := lookup(ctx, key)
		return errors.Is(err, auth.ErrUnauthenticated)
	}, 2*time.Second, 10*time.Millisecond, "the router still takes a revoked key")
}

func (s *KeysCLISuite) TestRevokeRefusesAKeyOfAnotherApp() {
	db, lookup, _, key := s.seedKey(s.settings)
	ctx := context.Background()
	org := store.Organization{Name: "other"}
	s.Require().NoError(db.Organizations.Insert(ctx, &org))
	other := store.App{OrganizationID: org.ID, Name: "other"}
	s.Require().NoError(db.Apps.Insert(ctx, &other))

	err := runKeys([]string{"revoke", "--app", other.ID, "--key", key}, s.settings, slog.New(slog.DiscardHandler))

	s.EqualError(err, "app "+other.ID+" has no live key "+key)
	_, err = lookup(ctx, key)
	s.NoError(err, "the key still works for the app that holds it")
}

// clearKeySources empties every key source the shell running the test holds: a router
// refuses to start beside a retired plain key, and ROUTER_DEV_KEK beside a wrapped one.
func clearKeySources(t *testing.T) {
	t.Helper()
	for _, variable := range os.Environ() {
		if name, _, _ := strings.Cut(variable, "="); strings.HasPrefix(name, "ROUTER_AUTH_KEK") {
			t.Setenv(name, "")
		}
	}
	t.Setenv(devKEKEnvVar, "")
}

// setDevKEK sets ROUTER_DEV_KEK for the test and nothing else that holds a key.
func setDevKEK(t *testing.T, key string) {
	t.Helper()
	clearKeySources(t)
	t.Setenv(devKEKEnvVar, key)
}
