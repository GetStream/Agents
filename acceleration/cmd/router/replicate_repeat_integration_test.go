//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/config"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// ReplicateRepeatSuite runs `router replicate` more than once between two real stores: a source
// served the way the router serves /v1/data/export and /v1/data/changes for the app acme, and a
// target.
type ReplicateRepeatSuite struct {
	suite.Suite
	ctx    context.Context
	source *store.Store
	target *store.Store
	server *httptest.Server
	// exports counts the exports the source was asked for.
	exports int
	// truncated makes the source's export stop before its cursor line, as a broken stream does.
	truncated bool
	// changesDown makes the source answer the changes 503.
	changesDown bool
	// pageSize is how many changes the source answers in a page.
	pageSize int
	// skip is the --skip values a run passes.
	skip []string
	// logs is what the runs logged.
	logs *bytes.Buffer
}

func TestReplicateRepeatSuite(t *testing.T) {
	suite.Run(t, new(ReplicateRepeatSuite))
}

func (s *ReplicateRepeatSuite) SetupTest() {
	s.ctx = context.Background()
	s.exports, s.truncated, s.changesDown, s.pageSize = 0, false, false, 500
	s.skip, s.logs = nil, &bytes.Buffer{}
	s.source = s.emptyStore("repeat_source")
	s.target = s.emptyStore("repeat_target")
	s.server = httptest.NewServer(http.HandlerFunc(s.serveSource))
	s.T().Cleanup(s.server.Close)
}

// emptyStore is a migrated store on a database of this suite's own, emptied first.
func (s *ReplicateRepeatSuite) emptyStore(name string) *store.Store {
	dsn := os.Getenv(dsnEnvVar)
	if dsn == "" {
		s.T().Skipf("%s not set", dsnEnvVar)
	}
	own := testenv.Database(dsn, name)
	opened, err := store.Open(own)
	s.Require().NoError(err)
	s.T().Cleanup(func() { opened.Close() })
	var database string
	s.Require().NoError(opened.GlobalDB().QueryRowContext(s.ctx, "SELECT current_database()").Scan(&database))
	s.Require().True(strings.HasSuffix(database, "_test"), "refusing to drop the schema of %s, which is not a test database", database)
	_, err = opened.GlobalDB().ExecContext(s.ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	s.Require().NoError(err)
	s.Require().NoError(opened.Migrate(s.ctx))
	return opened
}

// serveSource answers the export and the changes from the source store, as internal/api/datamove.go
// does.
func (s *ReplicateRepeatSuite) serveSource(w http.ResponseWriter, r *http.Request) {
	if err := s.source.StartDataCapture(r.Context(), "acme", time.Hour, time.Second); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	switch r.URL.Path {
	case "/v1/data/export":
		s.exports++
		encoder := json.NewEncoder(w)
		cursor, err := s.source.ExportCustomer(r.Context(), "acme", func(table string, row json.RawMessage) error {
			return encoder.Encode(map[string]any{"table": table, "row": row})
		})
		if err != nil || s.truncated {
			return
		}
		_ = encoder.Encode(map[string]any{"cursor": cursor, "customer": "acme"})
	case "/v1/data/changes":
		if s.changesDown {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		changes, cursor, err := s.source.Changes(r.Context(), "acme", after, s.pageSize)
		if errors.Is(err, store.ErrChangesExpired) {
			w.WriteHeader(http.StatusGone)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(changePage{Changes: changes, Cursor: cursor, CaughtUp: len(changes) < s.pageSize})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// once runs `router replicate --once` against the source, with --as when as is not empty and
// with --full when full is, on a deployment whose retention is retention.
func (s *ReplicateRepeatSuite) onceWithin(as string, full bool, retention time.Duration) error {
	args := []string{"--from", s.server.URL, "--api-key", sourceKey, "--api-secret", sourceSecret,
		"--source-auth", auth.AuthTypeServer, "--once", "--poll", "1ms"}
	if as != "" {
		args = append(args, "--as", as)
	}
	if full {
		args = append(args, "--full")
	}
	for _, row := range s.skip {
		args = append(args, "--skip", row)
	}
	options, err := parseReplicate(args)
	s.Require().NoError(err)
	reservation, err := s.target.ReserveImportedIDs(s.ctx)
	s.Require().NoError(err)
	settings := config.Config{DataMove: config.DataMove{Retention: retention}}
	return options.copyAndFollow(s.ctx, settings, s.target, reservation, slog.New(slog.NewTextHandler(s.logs, nil)))
}

func (s *ReplicateRepeatSuite) once(as string, full bool) error {
	return s.onceWithin(as, full, time.Hour)
}

func (s *ReplicateRepeatSuite) deployment() *deployment {
	return &deployment{url: s.server.URL, key: sourceKey, secret: sourceSecret, authType: auth.AuthTypeServer, client: s.server.Client()}
}

// copiedBefore is a first copy of acme made by a run that saved no cursor, with numbers 1 and 2,
// number 2 then edited here.
func (s *ReplicateRepeatSuite) copiedBefore() {
	s.recorded(1)
	s.recorded(2)
	s.Require().NoError(s.once("", false))
	_, err := s.target.GlobalDB().ExecContext(s.ctx, "DELETE FROM data_move_cursors")
	s.Require().NoError(err)
	s.tagged(s.target, 2, "edited here")
}

// recorded records a number of acme's at the source, its digits the number given.
func (s *ReplicateRepeatSuite) recorded(digits int) {
	number := &store.PhoneNumber{E164: e164(digits), Vendor: "sip_trunk", Country: "US", CustomerID: "acme"}
	s.Require().NoError(s.source.PhoneNumbers.Insert(s.ctx, number))
}

// tagged sets the label tag of a number of acme's in a store.
func (s *ReplicateRepeatSuite) tagged(in *store.Store, digits int, label string) {
	_, err := in.GlobalDB().ExecContext(s.ctx, "UPDATE phone_numbers SET tags = jsonb_build_object('label', ?::text) WHERE e164 = ?",
		label, e164(digits))
	s.Require().NoError(err)
}

// label is a number's label tag on the target, empty when it has none and "missing" when the
// target lacks the number.
func (s *ReplicateRepeatSuite) label(digits int) string {
	var label *string
	var held bool
	s.Require().NoError(s.target.GlobalDB().QueryRowContext(s.ctx,
		"SELECT count(*) > 0, max(tags->>'label') FROM phone_numbers WHERE customer_id = 'acme' AND e164 = ?",
		e164(digits)).Scan(&held, &label))
	if !held {
		return "missing"
	}
	if label == nil {
		return ""
	}
	return *label
}

// saved is the cursor the target saved for acme's copy from the source.
func (s *ReplicateRepeatSuite) saved() *store.MoveCursor {
	saved, err := s.target.MoveCursors.Get(s.ctx, "acme", s.server.URL)
	s.Require().NoError(err)
	return saved
}

func e164(digits int) string {
	return fmt.Sprintf("+1555%07d", digits)
}

// The values a first copy lands are what one landed before runs saved a cursor (a probe on
// 60c9d2245: "source", "", one export).
func (s *ReplicateRepeatSuite) TestAFirstCopyBringsEverythingAndSavesTheCursor() {
	s.recorded(1)
	s.recorded(2)
	s.tagged(s.source, 1, "source")

	s.Require().NoError(s.once("", false))

	s.Equal("source", s.label(1))
	s.Equal("", s.label(2))
	s.Equal(1, s.exports)
	saved := s.saved()
	s.Require().NotNil(saved)
	s.NotNil(saved.Cursor, "a finished copy saves the cursor it carries on from")
	s.WithinDuration(time.Now(), saved.ReadAt, time.Minute)
}

func (s *ReplicateRepeatSuite) TestARepeatOnceBringsOnlyTheChangesSinceItsCursor() {
	s.recorded(1)
	s.recorded(2)
	s.Require().NoError(s.once("", false))
	s.tagged(s.target, 2, "edited here")
	s.tagged(s.source, 1, "source again")
	s.recorded(3)

	s.Require().NoError(s.once("acme", false))

	s.Equal("source again", s.label(1), "a change at the source since the first copy arrives")
	s.Equal("", s.label(3), "a number recorded at the source since the first copy arrives")
	s.Equal("edited here", s.label(2), "a row the source left alone keeps what was done to it here")
	s.Equal(1, s.exports, "a repeat run with --as asks for no export")
}

func (s *ReplicateRepeatSuite) TestARepeatOnceWithoutAsWritesNoRowOfTheExport() {
	s.recorded(1)
	s.recorded(2)
	s.Require().NoError(s.once("", false))
	s.tagged(s.target, 1, "edited here")
	s.tagged(s.target, 2, "edited here")
	s.tagged(s.source, 2, "source again")

	s.Require().NoError(s.once("", false))

	s.Equal("edited here", s.label(1), "the export's first row, which names the app, is not written")
	s.Equal("source again", s.label(2), "the change since the cursor is")
}

func (s *ReplicateRepeatSuite) TestAThirdRunDoesNotReplayWhatTheSecondBrought() {
	s.recorded(1)
	s.Require().NoError(s.once("acme", false))
	s.tagged(s.source, 1, "source again")
	s.Require().NoError(s.once("acme", false))
	s.tagged(s.target, 1, "edited here")

	s.Require().NoError(s.once("acme", false))

	s.Equal("edited here", s.label(1), "each page followed saves the cursor past it")
}

func (s *ReplicateRepeatSuite) TestARepeatOnceForAnAppTheSourceNowHoldsNothingOfAppliesItsDeletes() {
	s.recorded(1)
	s.recorded(2)
	s.Require().NoError(s.once("", false))
	_, err := s.source.GlobalDB().ExecContext(s.ctx, "DELETE FROM phone_numbers WHERE customer_id = 'acme'")
	s.Require().NoError(err)

	// The export is empty, so only its last line names the app.
	s.Require().NoError(s.once("", false))

	s.Equal("missing", s.label(1))
	s.Equal("missing", s.label(2))
}

func (s *ReplicateRepeatSuite) TestACopyWhoseChangesWereNotReadStillSavesTheExportsCursor() {
	s.recorded(1)
	s.changesDown = true
	s.Require().Error(s.once("acme", false))
	s.changesDown = false
	s.tagged(s.target, 1, "edited here")

	s.Require().NoError(s.once("acme", false))

	s.Equal("edited here", s.label(1), "the next run carries on from the export's cursor")
	s.Equal(1, s.exports)
}

func (s *ReplicateRepeatSuite) TestACopyThatStoppedPartwayIsCopiedAgainWithoutFull() {
	s.recorded(1)
	s.truncated = true
	s.Require().ErrorContains(s.once("", false), "the export stopped before it finished")
	s.truncated = false
	s.tagged(s.target, 1, "edited here")
	s.tagged(s.source, 1, "source")

	s.Require().NoError(s.once("", false), "a copy that never finished is not one to protect")

	s.Equal("source", s.label(1))
	s.Equal(2, s.exports)
}

func (s *ReplicateRepeatSuite) TestAnAppCopiedBeforeRunsSavedACursorIsRefused() {
	s.copiedBefore()

	err := s.once("", false)

	s.Require().ErrorContains(err, "acme already has data here (a row in phone_numbers)")
	s.ErrorContains(err, "--full")
	s.Equal("edited here", s.label(2), "nothing is written over it")
}

func (s *ReplicateRepeatSuite) TestAnAppCopiedBeforeRunsSavedACursorIsRefusedWithAsBeforeAnyExport() {
	s.copiedBefore()

	s.Require().ErrorContains(s.once("acme", false), "--full")

	s.Equal(1, s.exports, "the refusal asks the source for nothing")
	s.Equal("edited here", s.label(2))
}

func (s *ReplicateRepeatSuite) TestFullCopiesEverythingAgainAndSavesTheCursor() {
	s.copiedBefore()
	s.tagged(s.source, 1, "source again")

	s.Require().NoError(s.once("acme", true))

	s.Equal("", s.label(2), "the source's row is written over what was done here")
	s.Equal("source again", s.label(1))
	s.Equal(2, s.exports)
	s.tagged(s.target, 2, "edited here")
	s.Require().NoError(s.once("acme", false), "the run after --full carries on from its cursor")
	s.Equal("edited here", s.label(2))
	s.Equal(2, s.exports)
}

func (s *ReplicateRepeatSuite) TestACursorReadLongerAgoThanTheRetentionIsRefused() {
	s.recorded(1)
	s.Require().NoError(s.once("acme", false))
	_, err := s.target.GlobalDB().ExecContext(s.ctx, "UPDATE data_move_cursors SET read_at = now() - interval '61 minutes'")
	s.Require().NoError(err)
	s.tagged(s.target, 1, "edited here")

	err = s.once("acme", false)

	s.Require().ErrorContains(err, "ROUTER_DATA_MOVE_RETENTION")
	s.ErrorContains(err, "--full")
	s.Equal("edited here", s.label(1))
	s.Equal(1, s.exports)
}

func (s *ReplicateRepeatSuite) TestACursorTheSourceNoLongerKeepsTheChangesOfNamesFull() {
	// A change before the first copy gives its cursor a number the source can lose.
	s.recorded(1)
	s.Require().NoError(s.source.StartDataCapture(s.ctx, "acme", time.Hour, time.Second))
	s.tagged(s.source, 1, "before the copy")
	s.Require().NoError(s.once("acme", false))
	s.tagged(s.source, 1, "pruned")
	s.tagged(s.source, 1, "kept")
	// What pruning does to every change but the newest.
	_, err := s.source.GlobalDB().ExecContext(s.ctx, "DELETE FROM data_changes WHERE seq < (SELECT max(seq) FROM data_changes)")
	s.Require().NoError(err)

	s.Require().ErrorContains(s.once("acme", false), "no longer keeps the changes since that cursor: run this again with --full")

	s.Equal("before the copy", s.label(1))
}

// A copy made right after the source's recording started again carries on from that start. A
// change there after it that aged out and was pruned names --full rather than being skipped.
func (s *ReplicateRepeatSuite) TestACursorAtTheSourcesRestartWhoseNextChangeWasPrunedNamesFull() {
	s.recorded(1)
	s.recorded(2)
	s.Require().NoError(s.once("acme", false))
	_, err := s.source.GlobalDB().ExecContext(s.ctx, "UPDATE data_change_capture SET expires_at = now() - interval '1 minute'")
	s.Require().NoError(err)
	s.Require().NoError(s.once("acme", true))
	s.tagged(s.source, 2, "pruned")
	_, err = s.source.GlobalDB().ExecContext(s.ctx, "UPDATE data_changes SET at = now() - interval '30 days'")
	s.Require().NoError(err)
	s.tagged(s.source, 1, "kept")
	removed, err := s.source.PruneDataChanges(s.ctx, 7*24*time.Hour)
	s.Require().NoError(err)
	s.Require().Positive(removed)

	s.Require().ErrorContains(s.once("acme", false), "no longer keeps the changes since that cursor: run this again with --full")

	s.Equal("", s.label(1), "nothing after the hole is applied")
}

func (s *ReplicateRepeatSuite) TestARunThatBringsNoChangesStillSavesWhenItReadTheSource() {
	s.recorded(1)
	s.Require().NoError(s.once("acme", false))
	_, err := s.target.GlobalDB().ExecContext(s.ctx, "UPDATE data_move_cursors SET read_at = now() - interval '50 minutes'")
	s.Require().NoError(err)

	s.Require().NoError(s.once("acme", false))

	s.WithinDuration(time.Now(), s.saved().ReadAt, time.Minute, "the read kept the source recording, so the next run is not refused")
}

// The source leaves an organization's policy under the app's id out of the changes, and the
// cursor it answers past it is where the next run starts.
func (s *ReplicateRepeatSuite) TestARunWhosePageCarriesNoChangeStillMovesItsCursor() {
	s.recorded(1)
	s.Require().NoError(s.once("acme", false))
	_, err := s.source.GlobalDB().ExecContext(s.ctx,
		"INSERT INTO policies (scope, scope_id, updated_at) VALUES ('organization', 'acme', now())")
	s.Require().NoError(err)
	var last int64
	s.Require().NoError(s.source.GlobalDB().QueryRowContext(s.ctx,
		"SELECT max(seq) FROM data_changes WHERE customer_id = 'acme'").Scan(&last))

	s.Require().NoError(s.once("acme", false))

	s.Require().NotNil(s.saved().Cursor)
	s.Equal(last, *s.saved().Cursor)
}

func (s *ReplicateRepeatSuite) TestAnOrganizationsPolicyUnderTheAppsIDIsNotTheAppsData() {
	// The export carries only app policies, so an organization's under the same id is not a copy.
	_, err := s.target.GlobalDB().ExecContext(s.ctx,
		"INSERT INTO policies (scope, scope_id, updated_at) VALUES ('organization', 'acme', now())")
	s.Require().NoError(err)
	s.recorded(1)

	s.Require().NoError(s.once("acme", false))

	s.Equal("", s.label(1))
}

func (s *ReplicateRepeatSuite) TestTheRetentionOfTheDeploymentIsTheOneACursorIsCheckedAgainst() {
	s.recorded(1)
	s.Require().NoError(s.once("acme", false))
	_, err := s.target.GlobalDB().ExecContext(s.ctx, "UPDATE data_move_cursors SET read_at = now() - interval '5 minutes'")
	s.Require().NoError(err)
	s.tagged(s.source, 1, "source again")

	err = s.onceWithin("acme", false, time.Minute)

	s.Require().ErrorContains(err, "ROUTER_DATA_MOVE_RETENTION (1m0s)")
	s.ErrorContains(err, "takes to be the source's own")
	s.Equal("", s.label(1))
	s.Require().NoError(s.onceWithin("acme", false, time.Hour))
	s.Equal("source again", s.label(1))
}

func (s *ReplicateRepeatSuite) TestAPageThatFailedToApplyIsBroughtByTheNextRun() {
	s.recorded(1)
	s.recorded(2)
	s.Require().NoError(s.once("acme", false))
	// A page of one change, so the second change is a page of its own, which the target refuses.
	s.pageSize = 1
	_, err := s.target.GlobalDB().ExecContext(s.ctx, `
CREATE FUNCTION refuse_poison() RETURNS trigger AS $$
BEGIN
  IF NEW.tags->>'label' = 'poison' THEN RAISE EXCEPTION 'poison'; END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER refuse_poison BEFORE INSERT OR UPDATE ON phone_numbers FOR EACH ROW EXECUTE FUNCTION refuse_poison()`)
	s.Require().NoError(err)
	s.tagged(s.source, 1, "fine")
	s.tagged(s.source, 2, "poison")

	err = s.once("acme", false)

	s.Require().Error(err)
	s.NotContains(err.Error(), "--skip", "only a row another app holds is one to skip")
	s.Equal("fine", s.label(1), "the page before it landed")
	_, err = s.target.GlobalDB().ExecContext(s.ctx, "DROP TRIGGER refuse_poison ON phone_numbers")
	s.Require().NoError(err)
	s.Require().NoError(s.once("acme", false))
	s.Equal("poison", s.label(2), "the cursor is saved only after a page is applied, so the page that failed comes again")
}

func (s *ReplicateRepeatSuite) TestACursorNeverMovesBack() {
	five, three := int64(5), int64(3)
	earlier := time.Now().Add(-time.Hour)
	s.Require().NoError(s.target.MoveCursors.Upsert(s.ctx, &store.MoveCursor{CustomerID: "acme", Source: "src", Cursor: &five, ReadAt: time.Now()}))

	s.Require().NoError(s.target.MoveCursors.Upsert(s.ctx, &store.MoveCursor{CustomerID: "acme", Source: "src", Cursor: &three, ReadAt: earlier}))

	saved, err := s.target.MoveCursors.Get(s.ctx, "acme", "src")
	s.Require().NoError(err)
	s.Equal(int64(5), *saved.Cursor, "a run that read less cannot undo one that read more")
	s.WithinDuration(time.Now(), saved.ReadAt, time.Minute)

	s.Require().NoError(s.target.MoveCursors.Upsert(s.ctx, &store.MoveCursor{CustomerID: "acme", Source: "src", ReadAt: earlier}))
	saved, err = s.target.MoveCursors.Get(s.ctx, "acme", "src")
	s.Require().NoError(err)
	s.Nil(saved.Cursor, "a copy that starts again clears it")
}

func (s *ReplicateRepeatSuite) TestOnlyATableInTheSharedDatabaseMakesAnAppHeld() {
	// calls are kept in the shards, which a pod reads only its own of.
	_, err := s.target.GlobalDB().ExecContext(s.ctx,
		"INSERT INTO calls (id, customer_id, call_id, agent_id) VALUES ('c', 'acme', 'c', 'agent')")
	s.Require().NoError(err)
	table, err := s.target.HoldingTable(s.ctx, "acme")
	s.Require().NoError(err)
	s.Equal("", table, "a row in a shard table does not count")

	s.Require().NoError(s.target.PhoneNumbers.Insert(s.ctx, &store.PhoneNumber{E164: e164(1), Vendor: "telnyx", Country: "US", CustomerID: "acme"}))
	table, err = s.target.HoldingTable(s.ctx, "acme")
	s.Require().NoError(err)
	s.Equal("phone_numbers", table)
}

// config writes an agent config of a customer's in a store, under an id of its own.
func (s *ReplicateRepeatSuite) config(in *store.Store, id, customerID, name string) {
	_, err := in.GlobalDB().ExecContext(s.ctx,
		"INSERT INTO agent_configs (id, customer_id, name, instructions) VALUES (?, ?, ?, 'be brief')", id, customerID, name)
	s.Require().NoError(err)
}

// configOn is the owner and name of an agent config on the target, empty when it holds none.
func (s *ReplicateRepeatSuite) configOn(id string) (owner, name string) {
	_ = s.target.GlobalDB().QueryRowContext(s.ctx, "SELECT customer_id, name FROM agent_configs WHERE id = ?", id).Scan(&owner, &name)
	return owner, name
}

func (s *ReplicateRepeatSuite) TestACopiedRowAnotherAppHoldsStopsTheRunNamingItAndSkipLeavesItOut() {
	s.config(s.source, "config-1", "acme", "mine")
	s.recorded(1)
	s.config(s.target, "config-1", "resident", "theirs")

	err := s.once("acme", false)

	s.Require().ErrorIs(err, store.ErrAnotherCustomersRow)
	s.ErrorContains(err, `agent_configs:{"id":"config-1"}`)
	s.ErrorContains(err, "--skip")
	s.Equal("missing", s.label(1), "the copy stopped at the row")

	s.skip = []string{`agent_configs:{"id": "config-1"}`}
	s.Require().NoError(s.once("acme", false))

	s.Equal("", s.label(1), "every other row landed")
	owner, name := s.configOn("config-1")
	s.Equal("resident", owner)
	s.Equal("theirs", name)
	s.Contains(s.logs.String(), `left out a row --skip names" row="agent_configs:{\"id\":\"config-1\"}"`)
}

func (s *ReplicateRepeatSuite) TestAChangedRowAnotherAppHoldsStopsThePageAndSkipLetsTheRestOfItLand() {
	s.recorded(1)
	s.Require().NoError(s.once("acme", false))
	s.config(s.source, "config-2", "acme", "mine")
	s.tagged(s.source, 1, "after")
	s.config(s.target, "config-2", "resident", "theirs")

	err := s.once("acme", false)

	s.Require().ErrorIs(err, store.ErrAnotherCustomersRow)
	s.ErrorContains(err, `agent_configs:{"id":"config-2"}`)
	s.ErrorContains(err, "--skip")
	s.Equal("", s.label(1), "one database's part of a page is applied whole or not at all")
	cursor := *s.saved().Cursor

	s.skip = []string{`agent_configs:{"id":"config-2"}`}
	s.Require().NoError(s.once("acme", false))

	s.Equal("after", s.label(1), "the rest of the page landed")
	owner, name := s.configOn("config-2")
	s.Equal("resident", owner)
	s.Equal("theirs", name)
	s.Greater(*s.saved().Cursor, cursor)
	s.Contains(s.logs.String(), `left out a change to a row --skip names" row="agent_configs:{\"id\":\"config-2\"}"`)
}

func (s *ReplicateRepeatSuite) TestSkipLeavesOutOnlyTheRowsItNames() {
	s.config(s.source, "config-1", "acme", "mine")
	s.config(s.source, "config-2", "acme", "mine too")
	s.config(s.target, "config-1", "resident", "theirs")
	s.config(s.target, "config-2", "resident", "theirs too")
	// The same key in another table, and the other key in this one.
	s.skip = []string{`skills:{"id":"config-2"}`, `agent_configs:{"id":"config-1"}`}

	err := s.once("acme", false)

	s.Require().ErrorIs(err, store.ErrAnotherCustomersRow)
	s.ErrorContains(err, `agent_configs:{"id":"config-2"}`)
}

func (s *ReplicateRepeatSuite) TestSkipRefusesAKeyThatIsNotTheTablesOwn() {
	s.config(s.source, "config-1", "acme", "mine")
	for value, reason := range map[string]string{
		`agent_configs:{"customer_id":"acme"}`:                 "its whole key (id), not by customer_id",
		`agent_configs:{"id":"config-1","customer_id":"acme"}`: "its whole key (id), not by customer_id, id",
		`agent_configs:{}`:                         "its whole key (id)",
		`agent_branch_messages:{"session_id":"s"}`: "its whole key (original_message_id, session_id), not by session_id",
		`agents:{"id":"config-1"}`:                 `"agents" is not a table an export carries`,
		`:{"id":"config-1"}`:                       `"" is not a table an export carries`,
	} {
		s.skip = []string{value}
		s.ErrorContains(s.once("acme", false), reason, value)
	}
	s.Zero(s.exports, "refused before the source is asked for anything")
	s.skip = []string{`agent_branch_messages:{"session_id":"s","original_message_id":"m"}`}
	s.Require().NoError(s.once("acme", false), "a composite key in any order")
	owner, _ := s.configOn("config-1")
	s.Equal("acme", owner)
}

// exec runs a statement in a store's database.
func (s *ReplicateRepeatSuite) exec(in *store.Store, query string, args ...any) {
	_, err := in.GlobalDB().ExecContext(s.ctx, query, args...)
	s.Require().NoError(err)
}

func (s *ReplicateRepeatSuite) TestARowThatBelongsToOneLeftOutIsLoggedInTheCopyAndInAChange() {
	campaign := "INSERT INTO campaigns (id, customer_id, name) VALUES ('campaign-1', ?, 'Renewals'), (?, ?, 'Other')"
	contact := "INSERT INTO campaign_contacts (id, campaign_id, to_number) VALUES (?, ?, ?)"
	s.exec(s.source, campaign, "acme", "campaign-2", "acme")
	s.exec(s.target, campaign, "resident", "campaign-3", "resident")
	s.exec(s.source, contact, "copied", "campaign-1", e164(1))
	s.exec(s.source, contact, "kept", "campaign-2", e164(2))
	s.skip = []string{`campaigns:{"id":"campaign-1"}`}
	// The app's own opt-out, recorded here too: the one here is kept, and nothing is left out.
	for _, in := range []*store.Store{s.source, s.target} {
		s.Require().NoError(in.OptOuts.Insert(s.ctx, &store.OptOut{CustomerID: "acme", Recipient: e164(9), Channel: "sms", Source: "keyword"}))
	}
	const leftOut = `left out a row whose parent row is not this app's here" row="campaign_contacts:{\"id\":\"`

	s.Require().NoError(s.once("acme", false))

	s.Equal(1, strings.Count(s.logs.String(), leftOut))
	s.NotContains(s.logs.String(), "opt_outs:")
	s.Contains(s.logs.String(), leftOut+`copied\"}"`)
	s.exec(s.source, contact, "changed", "campaign-1", e164(3))

	s.Require().NoError(s.once("acme", false))

	s.Equal(2, strings.Count(s.logs.String(), leftOut))
	s.Contains(s.logs.String(), leftOut+`changed\"}"`)
	var contacts string
	s.Require().NoError(s.target.GlobalDB().QueryRowContext(s.ctx, "SELECT string_agg(id, ',') FROM campaign_contacts").Scan(&contacts))
	s.Equal("kept", contacts)

	// Copied again, a contact already here is written over, and is not one left out.
	s.logs.Reset()
	s.Require().NoError(s.once("acme", true))
	s.NotContains(s.logs.String(), leftOut+"kept")
}

func (s *ReplicateRepeatSuite) TestARowOfACompositeKeyIsRefusedByItsWholeKeyAndSkippedByIt() {
	for in, customer := range map[*store.Store]string{s.source: "acme", s.target: "resident"} {
		s.exec(in, "INSERT INTO agent_sessions (id, customer_id, state) VALUES ('session-1', ?, 'ended')", customer)
		s.exec(in, `INSERT INTO agent_branch_messages (session_id, original_message_id, customer_id, switch_id, chat_created_at, author_id)
			VALUES ('session-1', 'message-1', ?, 'switch-1', now(), 'user-1')`, customer)
	}
	s.recorded(1)
	s.skip = []string{`agent_sessions:{"id":"session-1"}`}

	err := s.once("acme", false)

	s.Require().ErrorIs(err, store.ErrAnotherCustomersRow)
	const key = `agent_branch_messages:{"original_message_id":"message-1","session_id":"session-1"}`
	s.ErrorContains(err, "import "+key+":")
	s.skip = append(s.skip, key)
	s.Require().NoError(s.once("acme", false))
	s.Equal("", s.label(1))
	var owner string
	s.Require().NoError(s.target.GlobalDB().QueryRowContext(s.ctx, "SELECT customer_id FROM agent_branch_messages").Scan(&owner))
	s.Equal("resident", owner)
}
