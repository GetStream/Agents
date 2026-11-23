//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing/pricingtest"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
	usageservice "github.com/GetStream/Vision-Agents/acceleration/internal/usage"
	"github.com/GetStream/Vision-Agents/acceleration/prices"
)

// UsageCommandsSuite runs `router usage` on a database of its own that it empties first.
type UsageCommandsSuite struct {
	suite.Suite
	ctx      context.Context
	settings config.Config
	store    *store.Store
}

func TestUsageCommandsSuite(t *testing.T) {
	suite.Run(t, new(UsageCommandsSuite))
}

func (s *UsageCommandsSuite) SetupTest() {
	clearKeySources(s.T())
	s.ctx = context.Background()
	dsn := os.Getenv(dsnEnvVar)
	if dsn == "" {
		s.T().Skipf("%s not set", dsnEnvVar)
	}
	own := testenv.Database(dsn, "usagecli")
	empty, err := store.Open(own)
	s.Require().NoError(err)
	var database string
	s.Require().NoError(empty.GlobalDB().QueryRowContext(s.ctx, "SELECT current_database()").Scan(&database))
	s.Require().True(strings.HasSuffix(database, "_test"), "refusing to drop the schema of %s, which is not a test database", database)
	_, err = empty.GlobalDB().ExecContext(s.ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	s.Require().NoError(err)
	s.Require().NoError(empty.Migrate(s.ctx))
	s.store = empty
	s.T().Cleanup(func() { empty.Close() })

	s.settings = config.Defaults()
	s.settings.Postgres.DSN = own
	// Nothing is sent from here: a usage service only makes the rows wait to be shipped.
	s.settings.Usage = config.Usage{URL: "http://usage.invalid:8070", Token: "token"}
}

// quoted is what used of the item costs at the vendor's rates, and what a job charges for it at
// the prices a first load writes, both read from vendor.json so a price change there leaves the
// tests passing.
func (s *UsageCommandsSuite) quoted(modality routing.Modality, provider, model string, attributes map[string]string,
	used pricing.Quantities,
) (base, cost int64) {
	discard := slog.New(slog.DiscardHandler)
	atCost, err := pricing.NewList("vendor", pricing.AllAtCost(prices.Vendor().Entries), discard)
	s.Require().NoError(err)
	charged, err := prices.Embedded(nil, release, discard)
	s.Require().NoError(err)
	item := routing.ItemOf(modality, routing.ProviderConfig{Provider: provider, Model: model}, attributes)
	return pricingtest.Micros(s.T(), atCost, item, used), pricingtest.Micros(s.T(), charged, item, used)
}

// lostLog is a router's log in which a writer that has closed lost one row, among other lines.
func (s *UsageCommandsSuite) lostLog() (path string, lost uuid.UUID) {
	var log bytes.Buffer
	logger := newLogger(&log, slog.LevelInfo)
	logger.Info("listening", "address", ":8080")
	writer := routing.NewWriter(routing.WriterOptions{Store: s.store, Logger: logger})
	ctx, cancel := context.WithTimeout(s.ctx, time.Second)
	defer cancel()
	writer.Close(ctx)
	lost = uuid.Must(uuid.NewV7())
	routing.NewRecorderOn(routing.LLM, writer, nil, logger).Record(routing.ProviderConfig{Provider: "openai", Model: "gpt"}, routing.Stat{
		Owner:   routing.Owner{CustomerID: "app-1", Attribution: routing.Attribution{SessionID: "session-1"}, Tags: routing.Tags{"project": `a "quoted" one`}},
		UsageID: lost, StartedAt: time.Now().UTC(), Usage: routing.Usage{OutputTokens: 10}, ErrorMessage: "cut off\nmid line",
	})
	s.Require().Contains(log.String(), `msg="usage row lost" reason=shutdown`)
	logger.Info("shutting down")
	path = filepath.Join(s.T().TempDir(), "router.log")
	s.Require().NoError(os.WriteFile(path, log.Bytes(), 0o600))
	return path, lost
}

func (s *UsageCommandsSuite) requests() []store.Request {
	var rows []store.Request
	s.Require().NoError(s.store.ShardDB().NewSelect().Model(&rows).Order("started_at").Scan(s.ctx))
	return rows
}

func (s *UsageCommandsSuite) TestReplayWritesLostRowsOnce() {
	path, lost := s.lostLog()
	var first, second bytes.Buffer

	s.Require().NoError(runUsage([]string{"replay", "--from", path}, s.settings, &first))
	s.Require().NoError(runUsage([]string{"replay", "--from", path}, s.settings, &second))

	s.Contains(first.String(), "1 lost rows: 1 written, 0 already there, 0 refused")
	s.Contains(second.String(), "1 lost rows: 0 written, 1 already there, 0 refused", "replaying the file again writes nothing")
	rows := s.requests()
	s.Require().Len(rows, 1)
	s.Equal(lost, rows[0].UsageID)
	s.Equal("session-1", rows[0].SessionID)
	s.Equal(map[string]string{"project": `a "quoted" one`}, rows[0].Tags)
	s.True(rows[0].ToShip, "queued for the sweeper, with a usage service")
	s.Equal(routing.Unattributed, rows[0].APIKey, "the writer's defaults, which the row was lost before it got")
	s.Equal(usageservice.DefaultTier, rows[0].OrganizationTier)
}

func (s *UsageCommandsSuite) TestReplayReadsBackEveryRowTheRoutersLoggerLogsLost() {
	var log bytes.Buffer
	logger := newLogger(&log, slog.LevelInfo)
	writer := routing.NewWriter(routing.WriterOptions{Store: s.store, Logger: logger})
	ctx, cancel := context.WithTimeout(s.ctx, time.Second)
	defer cancel()
	writer.Close(ctx)
	recorder := routing.NewRecorderOn(routing.LLM, writer, nil, logger)
	provider := routing.ProviderConfig{Provider: "openai", Model: "gpt"}
	startedAt := time.Now().UTC().Truncate(time.Microsecond)
	keyed, unkeyed := uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7())
	recorder.Record(provider, routing.Stat{
		Owner: routing.Owner{CustomerID: "app-1", Attribution: routing.Attribution{
			OrganizationID: "org-1", APIKey: "key-a", UserID: "user-1", SessionID: "session-1",
		}, Tags: routing.Tags{"project": "alpha"}},
		UsageID: keyed, StartedAt: startedAt, Usage: routing.Usage{OutputTokens: 10},
	})
	recorder.Record(provider, routing.Stat{
		Owner:   routing.Owner{CustomerID: "app-2"},
		UsageID: unkeyed, StartedAt: startedAt, Usage: routing.Usage{InputTokens: 3},
	})
	s.Require().Equal(2, strings.Count(log.String(), lostRowMessage))

	rows, unreadable, err := lostRows(&log, false)

	s.Require().NoError(err)
	s.Zero(unreadable, "every line the router's logger wrote is read back")
	s.Require().Len(rows, 2)
	s.Equal(keyed, rows[0].UsageID)
	s.Equal("app-1", rows[0].CustomerID)
	s.Equal("org-1", rows[0].OrganizationID)
	s.Equal("key-a", rows[0].APIKey)
	s.Equal("user-1", rows[0].UserID)
	s.Equal("session-1", rows[0].SessionID)
	s.Equal(map[string]string{"project": "alpha"}, rows[0].Tags)
	s.EqualValues(10, rows[0].OutputTokens)
	s.True(startedAt.Equal(rows[0].StartedAt))
	s.Equal(unkeyed, rows[1].UsageID)
	s.Equal("app-2", rows[1].CustomerID)
	s.Equal(routing.Unattributed, rows[1].APIKey)
	s.EqualValues(3, rows[1].InputTokens)
}

func (s *UsageCommandsSuite) TestALostRowIsLoggedWithoutTheNumbersAndKeysItsTextHolds() {
	var log bytes.Buffer
	logger := newLogger(&log, slog.LevelInfo)
	writer := routing.NewWriter(routing.WriterOptions{Store: s.store, Logger: logger})
	ctx, cancel := context.WithTimeout(s.ctx, time.Second)
	defer cancel()
	writer.Close(ctx)
	key := "sk-test" + strings.Repeat("0", 16)
	lost := uuid.Must(uuid.NewV7())
	tags := routing.Tags{"caller": "call +15550003303 back", "pasted": "the key " + key, "sip-15550009909": "named", "sip-15550009910": "also named"}
	categories := []string{"sip-15550004404"}
	routing.NewRecorderOn(routing.LLM, writer, nil, logger).Record(routing.ProviderConfig{Provider: "openai", Model: "gpt"}, routing.Stat{
		Owner: routing.Owner{CustomerID: "app-1", Attribution: routing.Attribution{
			OrganizationID: "org-1", APIKey: "key-a", UserID: "phone:+15550001101", SessionID: "sip-15550002202",
			UsageCategories: categories,
		}, AgentID: "sip-15550006606", CallID: "sip-15550007707", Tags: tags},
		UsageID: lost, StartedAt: time.Now().UTC(), Usage: routing.Usage{OutputTokens: 10},
		Attributes: map[string]string{"note": "for +15550005505"},
	})
	held := []string{"5550001101", "5550002202", "5550003303", "5550004404", "5550005505", "5550006606", "5550007707", "5550009909", "5550009910", key}
	s.Equal(routing.Tags{"caller": "call +15550003303 back", "pasted": "the key " + key, "sip-15550009909": "named", "sip-15550009910": "also named"}, tags,
		"the session's own tags are not redacted in place")
	s.Equal([]string{"sip-15550004404"}, categories, "nor its categories")
	for _, text := range held {
		s.NotContains(log.String(), text)
	}

	rows, unreadable, err := lostRows(bytes.NewReader(log.Bytes()), false)

	s.Require().NoError(err)
	s.Require().Zero(unreadable)
	s.Require().Len(rows, 1)
	row, err := json.Marshal(rows[0])
	s.Require().NoError(err)
	for _, text := range held {
		s.NotContains(string(row), text, "the decoded row_hex")
	}
	s.Equal(lost, rows[0].UsageID, "what the row is billed by is kept")
	s.Equal("app-1", rows[0].CustomerID)
	s.Equal("org-1", rows[0].OrganizationID)
	s.Equal("key-a", rows[0].APIKey)
	s.EqualValues(10, rows[0].OutputTokens)
	s.Equal(routing.MaskedUser+"phone:+1•••01", rows[0].UserID, "a user id the log masks is read back masked, for replay to find in its session")
	s.Equal("named", rows[0].Tags["phone_caller"], "two tag keys masked alike both come back")
	s.Equal("also named", rows[0].Tags["phone_caller#2"])
}

func (s *UsageCommandsSuite) TestAReplayedRowWithAUserTheUsageServiceWouldRefuseIsWrittenWithoutIt() {
	lost := store.Request{
		UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: "app-1", APIKey: "key-a", UserID: "auth0|x",
		Provider: "openai", Model: "gpt", StartedAt: time.Now().UTC(), Success: true,
	}
	row, err := json.Marshal(lost)
	s.Require().NoError(err)
	// As a router that does not apply the rule would log it.
	var log bytes.Buffer
	newLogger(&log, slog.LevelInfo).Error("usage row lost", "reason", "shutdown", "usage_id", lost.UsageID, "row_hex", hex.EncodeToString(row))
	path := filepath.Join(s.T().TempDir(), "router.log")
	s.Require().NoError(os.WriteFile(path, log.Bytes(), 0o600))

	s.Require().NoError(runUsage([]string{"replay", "--from", path}, s.settings, &bytes.Buffer{}))

	rows := s.requests()
	s.Require().Len(rows, 1)
	s.Equal(lost.UsageID, rows[0].UsageID)
	s.Empty(rows[0].UserID)
	s.Equal("key-a", rows[0].APIKey)
}

func (s *UsageCommandsSuite) TestAReplayedRowLoggedWithAMaskedUserTakesItsSessionsUser() {
	const ali = "Ali.Khan@Example.com"
	for _, session := range []store.AgentSession{
		{ID: "session-ali", CustomerID: "app-1", UserID: ali},
		{ID: "session-bob", CustomerID: "app-1", UserID: "bob@other.org"},
		{ID: "session-zed", CustomerID: "app-1", UserID: "zed@Example.com"},
		{ID: "session-phone", CustomerID: "app-2", UserID: "phone:15550001101"},
	} {
		s.Require().NoError(s.store.Sessions.Upsert(s.ctx, &session))
	}
	var log bytes.Buffer
	logger := newLogger(&log, slog.LevelInfo)
	writer := routing.NewWriter(routing.WriterOptions{Store: s.store, Logger: logger})
	ctx, cancel := context.WithTimeout(s.ctx, time.Second)
	defer cancel()
	writer.Close(ctx)
	rows := []struct {
		name        string
		app         string
		attribution routing.Attribution
		user        string
	}{
		{"in its session", "app-1", routing.Attribution{SessionID: "session-ali", UserID: ali}, ali},
		{"a phone: user", "app-2", routing.Attribution{SessionID: "session-phone", UserID: "phone:15550001101"}, "phone:15550001101"},
		{"with no session", "app-1", routing.Attribution{UserID: ali}, ""},
		{"in a gone session", "app-1", routing.Attribution{SessionID: "session-gone", UserID: ali}, ""},
		{"in another user's", "app-1", routing.Attribution{SessionID: "session-bob", UserID: ali}, ""},
		// The check is that the session's user masks alike, so another user of the same
		// domain is taken for this one: the session is what names the user.
		{"in a same-domain user's", "app-1", routing.Attribution{SessionID: "session-zed", UserID: ali}, "zed@Example.com"},
		{"in another app's session", "app-2", routing.Attribution{SessionID: "session-ali", UserID: ali}, ""},
		{"an opaque user", "app-1", routing.Attribution{SessionID: "session-bob", UserID: "user-42"}, "user-42"},
		{"no user", "app-1", routing.Attribution{SessionID: "session-ali"}, ""},
	}
	ids := map[uuid.UUID]int{}
	for i, row := range rows {
		id := uuid.Must(uuid.NewV7())
		ids[id] = i
		routing.NewRecorderOn(routing.LLM, writer, nil, logger).Record(routing.ProviderConfig{Provider: "openai", Model: "gpt"}, routing.Stat{
			Owner:   routing.Owner{CustomerID: row.app, Attribution: row.attribution},
			UsageID: id, StartedAt: time.Now().UTC(), Usage: routing.Usage{OutputTokens: 10},
		})
	}
	s.Require().NotContains(log.String(), "Khan", "the log holds no email")
	path := filepath.Join(s.T().TempDir(), "router.log")
	s.Require().NoError(os.WriteFile(path, log.Bytes(), 0o600))

	s.Require().NoError(runUsage([]string{"replay", "--from", path}, s.settings, &bytes.Buffer{}))

	replayed := s.requests()
	s.Require().Len(replayed, len(rows))
	for _, row := range replayed {
		want := rows[ids[row.UsageID]]
		s.Equal(want.user, row.UserID, want.name)
	}
}

func (s *UsageCommandsSuite) TestWithNoUsageServiceAReplayedRowIsNotQueued() {
	path, _ := s.lostLog()
	s.settings.Usage = config.Usage{}

	s.Require().NoError(runUsage([]string{"replay", "--from", path}, s.settings, &bytes.Buffer{}))

	s.False(s.requests()[0].ToShip)
}

// recorded writes a row of this shard that started ago.
func (s *UsageCommandsSuite) recorded(ago time.Duration, change func(*store.Request)) uuid.UUID {
	request := store.Request{
		UsageID: uuid.Must(uuid.NewV7()), Modality: "llm", CustomerID: "app-1", Provider: "openai", Model: "gpt",
		StartedAt: time.Now().UTC().Add(-ago), Success: true,
	}
	change(&request)
	_, err := s.store.Requests.InsertMany(s.ctx, []store.Request{request})
	s.Require().NoError(err)
	return request.UsageID
}

// shipState is whether a row waits to ship, and why the usage service refused it.
type shipState struct {
	toShip    bool
	shipError string
}

func (s *UsageCommandsSuite) states() map[uuid.UUID]shipState {
	states := map[uuid.UUID]shipState{}
	for _, row := range s.requests() {
		states[row.UsageID] = shipState{toShip: row.ToShip, shipError: row.ShipError}
	}
	return states
}

// rowsOfEveryKind writes, started ten minutes ago, a row the usage service has, one it refused,
// one written before the router shipped, and one a data move carried in, which arrives as the
// import writes it (see TestAnImportedRequestKeepsItsUsageIDAndIsNeverShipped).
func (s *UsageCommandsSuite) rowsOfEveryKind() (shipped, rejected, beforeShipping, imported uuid.UUID) {
	shippedAt := time.Now().UTC().Add(-5 * time.Minute)
	shipped = s.recorded(10*time.Minute, func(request *store.Request) { request.ShippedAt = &shippedAt })
	rejected = s.recorded(10*time.Minute, func(request *store.Request) { request.ShipError = usageservice.ReasonAPIKey })
	beforeShipping = s.recorded(10*time.Minute, func(*store.Request) {})
	imported = s.recorded(10*time.Minute, func(request *store.Request) { request.CustomerID = "moved-here" })
	return shipped, rejected, beforeShipping, imported
}

func (s *UsageCommandsSuite) TestReshipQueuesTheRowsTheServiceHas() {
	shipped, rejected, beforeShipping, imported := s.rowsOfEveryKind()
	since := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	var out bytes.Buffer

	s.Require().NoError(runUsage([]string{"reship", "--since", since}, s.settings, &out))

	s.Contains(out.String(), "queued 1 rows")
	s.Equal(map[uuid.UUID]shipState{
		shipped:        {toShip: true},
		rejected:       {shipError: usageservice.ReasonAPIKey},
		beforeShipping: {},
		imported:       {},
	}, s.states(), "a row the service never had is not billed now")
}

func (s *UsageCommandsSuite) TestReshipQueuesRejectedRows() {
	shipped, rejected, beforeShipping, imported := s.rowsOfEveryKind()
	older := s.recorded(3*time.Hour, func(request *store.Request) { request.ShipError = usageservice.ReasonAPIKey })
	var out bytes.Buffer

	s.Require().NoError(runUsage([]string{"reship", "--since", "1h", "--rejected"}, s.settings, &out))

	s.Contains(out.String(), "queued 1 rows")
	s.Equal(map[uuid.UUID]shipState{
		shipped:        {},
		rejected:       {toShip: true},
		beforeShipping: {},
		imported:       {},
		older:          {shipError: usageservice.ReasonAPIKey},
	}, s.states(), "only the refused row since then is queued, and why it was refused is cleared")
}

func (s *UsageCommandsSuite) TestReshipWalksPastAFullBatch() {
	_, err := s.store.ShardDB().ExecContext(s.ctx, `
INSERT INTO requests (usage_id, modality, customer_id, provider, model, started_at, shipped_at, success)
SELECT gen_random_uuid(), 'llm', 'app-1', 'openai', 'gpt', now() - interval '10 minutes', now() - interval '5 minutes', true
FROM generate_series(1, 10050)`)
	s.Require().NoError(err)
	var out bytes.Buffer

	s.Require().NoError(runUsage([]string{"reship", "--since", "1h"}, s.settings, &out))

	s.Contains(out.String(), "queued 10050 rows")
}

func (s *UsageCommandsSuite) TestReshipNeedsAUsageService() {
	s.settings.Usage = config.Usage{}

	err := runUsage([]string{"reship", "--since", "1h"}, s.settings, &bytes.Buffer{})

	s.ErrorContains(err, "ROUTER_USAGE_URL")
}
