//go:build integration

package main

import (
	"context"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/providers"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// dsnEnvVar is where the store's own suite looks for a Postgres (internal/store/store_test.go).
const dsnEnvVar = "ROUTER_POSTGRES_DSN"

// OpenStoreSuite starts the router's store the way every command does, on a database of its
// own that it empties first.
type OpenStoreSuite struct {
	suite.Suite
	ctx      context.Context
	settings config.Config
}

func TestOpenStoreSuite(t *testing.T) {
	suite.Run(t, new(OpenStoreSuite))
}

func (s *OpenStoreSuite) SetupTest() {
	dsn := os.Getenv(dsnEnvVar)
	if dsn == "" {
		s.T().Skipf("%s not set", dsnEnvVar)
	}
	s.ctx = context.Background()
	s.settings = config.Defaults()
	s.settings.Postgres.DSN = testenv.Database(dsn, "openstore")

	empty, err := store.Open(s.settings.Postgres.DSN)
	s.Require().NoError(err)
	defer empty.Close()
	var database string
	s.Require().NoError(empty.GlobalDB().QueryRowContext(s.ctx, "SELECT current_database()").Scan(&database))
	s.Require().True(strings.HasSuffix(database, "_test"), "refusing to drop the schema of %s, which is not a test database", database)
	_, err = empty.GlobalDB().ExecContext(s.ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	s.Require().NoError(err)
}

func (s *OpenStoreSuite) TestARouterStartingOnAnEmptyDatabaseHasTheBuiltInsAtTheRevisionsTheyName() {
	revisions := map[string]int{}
	for _, id := range []string{"bigquery", "calcom", "calendly", "github", "gmail", "gong", "google_calendar", "google_docs", "google_drive", "hubspot", "instacart", "linear", "linear_app", "linq", "metabase", "notion", "salesforce", "sentry", "shopify", "slack", "slack_bot", "telnyx", "whatsapp"} {
		raw, err := fs.ReadFile(providers.FS, id+".yaml")
		s.Require().NoError(err)
		manifest, err := core.ParseManifest(raw)
		s.Require().NoError(err)
		revisions[id] = manifest.Revision
	}
	for range 2 {
		opened, err := openStore(s.ctx, s.settings)
		s.Require().NoError(err)
		s.T().Cleanup(func() { opened.Close() })

		definitions, err := opened.ConnectorDefinitions.Select(s.ctx, "acme", store.ConnectorDefinitionFilter{})
		s.Require().NoError(err)
		s.Require().Len(definitions, 23, "a restart adds no revision")
		for i, id := range []string{"bigquery", "calcom", "calendly", "github", "gmail", "gong", "google_calendar", "google_docs", "google_drive", "hubspot", "instacart", "linear", "linear_app", "linq", "metabase", "notion", "salesforce", "sentry", "shopify", "slack", "slack_bot", "telnyx", "whatsapp"} {
			s.Equal(id, definitions[i].ID)
			s.Equal(revisions[id], definitions[i].Revision)
		}
	}
}

// tables counts the tables of the suite's database.
func (s *OpenStoreSuite) tables() int {
	db, err := store.Open(s.settings.Postgres.DSN)
	s.Require().NoError(err)
	defer db.Close()
	var count int
	s.Require().NoError(db.GlobalDB().QueryRowContext(s.ctx, "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'").Scan(&count))
	return count
}

func (s *OpenStoreSuite) TestARouterWithAMigrateDSNNeverMigrates() {
	s.settings.Postgres.MigrateDSN = s.settings.Postgres.DSN

	_, err := openStore(s.ctx, s.settings)

	s.ErrorContains(err, "run router migrate")
	s.Zero(s.tables(), "a start changes nothing")
}

// A start connects with the serving DSN alone, so its migrate DSN may hold no password.
func (s *OpenStoreSuite) TestARouterStartsOnWhatRouterMigrateLeftWithoutConnectingAsTheOwner() {
	s.settings.Postgres.MigrateDSN = s.settings.Postgres.DSN
	s.Require().NoError(dispatchCommand("migrate", nil, s.settings, slog.New(slog.DiscardHandler)))

	s.settings.Postgres.MigrateDSN = "postgres://owner@127.0.0.1:1/router?sslmode=disable"
	opened, err := openStore(s.ctx, s.settings)

	s.Require().NoError(err)
	defer opened.Close()
	definitions, err := opened.ConnectorDefinitions.Select(s.ctx, "acme", store.ConnectorDefinitionFilter{})
	s.Require().NoError(err)
	s.Len(definitions, 23, "it seeds the built-ins as any start does")
}

// The router's own database stays a migration behind while another is brought up to date.
func (s *OpenStoreSuite) TestRouterMigrateOfAnotherDatabaseThanTheRoutersFails() {
	behind, err := openStore(s.ctx, s.settings)
	s.Require().NoError(err)
	defer behind.Close()
	_, err = behind.GlobalDB().ExecContext(s.ctx, "DELETE FROM goose_global_version WHERE version_id = 20270128000000")
	s.Require().NoError(err)
	s.settings.Postgres.MigrateDSN = testenv.Database(os.Getenv(dsnEnvVar), "openstore_other")

	err = dispatchCommand("migrate", nil, s.settings, slog.New(slog.DiscardHandler))

	s.ErrorContains(err, "has not applied global migration 20270128000000")
}

func (s *OpenStoreSuite) TestEachDatabaseIsMigratedWithItsOwnMigrateDSN() {
	shard := testenv.Database(os.Getenv(dsnEnvVar), "openstore_shard")
	emptied, err := store.Open(shard)
	s.Require().NoError(err)
	_, err = emptied.GlobalDB().ExecContext(s.ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	s.Require().NoError(err)
	s.Require().NoError(emptied.Close())
	s.settings.Postgres.ShardDSN = shard
	s.settings.Postgres.MigrateDSN = s.settings.Postgres.DSN
	s.settings.Postgres.ShardMigrateDSN = "postgres://owner@127.0.0.1:1/router?sslmode=disable"

	s.ErrorContains(dispatchCommand("migrate", nil, s.settings, slog.New(slog.DiscardHandler)), "store: shard: ")

	s.settings.Postgres.ShardMigrateDSN = shard
	s.NoError(dispatchCommand("migrate", nil, s.settings, slog.New(slog.DiscardHandler)))
}

func (s *OpenStoreSuite) TestRouterMigrateWithoutAMigrateDSNMigratesAsTheRouter() {
	s.Require().NoError(dispatchCommand("migrate", nil, s.settings, slog.New(slog.DiscardHandler)))

	s.NotZero(s.tables())
}

func (s *OpenStoreSuite) TestRouterMigrateWithoutADatabaseIsRefused() {
	s.settings.Postgres.DSN = ""

	s.ErrorContains(dispatchCommand("migrate", nil, s.settings, slog.New(slog.DiscardHandler)), "set postgres.dsn")
}
