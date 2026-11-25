//go:build integration

package api

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// DataMoveSuite covers the three ways a customer's data goes in or out: the snapshot, the
// changes since it, and writing either of them into a deployment.
type DataMoveSuite struct {
	RouterSuite
	// logged is what the router logged.
	logged *lockedLog
}

// TestDataMoveSuite runs on its own rather than beside the other suites: the change feed waits
// for every transaction open on the Postgres server, and other suites hold one for longer than
// a change is waited for (a row locked past auditWriteTimeout).
func TestDataMoveSuite(t *testing.T) {
	suite.Run(t, new(DataMoveSuite))
}

// letGoWithin is how long a test waits for an export to end and let go of its place: well past
// the deadlines the tests set, for a machine running other suites beside this one. A wait ends
// as soon as the export has.
const letGoWithin = 30 * time.Second

func (s *DataMoveSuite) SetupSuite() {
	s.logged = &lockedLog{}
	s.logs = s.logged
	s.RouterSuite.SetupSuite()
}

func (s *DataMoveSuite) SetupTest() {
	// Last of the test's cleanups: the router's places for exports are shared by the tests
	// after it, so the exports a test left unread have ended before the next one starts.
	s.T().Cleanup(func() {
		s.Eventually(func() bool { return len(s.router.exports) == 0 }, letGoWithin, 10*time.Millisecond,
			"an export of this test is still running")
		s.Eventually(func() bool { return len(s.router.imports) == 0 }, letGoWithin, 10*time.Millisecond,
			"an import of this test is still running")
	})
	s.useApp(s.data.createApp())
}

func (s *DataMoveSuite) TestAnExportEndsWithTheCursorItsChangesCarryOnFrom() {
	// The cursor is last rather than first because it is also what says the export
	// finished: a stream that died halfway has none.
	lines := s.export()

	last := lines[len(lines)-1]
	s.Require().NotNil(last.Cursor, "the export did not finish")
	s.Equal(s.customerID(), last.Customer)
	s.NotNil(last.At)
}

func (s *DataMoveSuite) TestAnExportCarriesTheAppsOwnRows() {
	agent := s.data.createAgentConfig()

	exported := s.export()

	var configs []string
	for _, line := range exported {
		if line.Table == "agent_configs" {
			configs = append(configs, string(line.Row))
		}
	}
	s.Require().Len(configs, 1, "the app has one agent")
	s.Contains(configs[0], agent.Id)
}

func (s *DataMoveSuite) TestAnotherAppsRowsAreNotInTheExport() {
	s.skipOnARouterWithShards()
	stranger := s.data.backendOfAnotherApp()
	var theirs AgentConfig
	s.Require().Equal(http.StatusCreated, stranger.do(http.MethodPost, "/v1/agents/configs",
		AgentConfigRequest{Name: "agent-" + s.utils.uuid(), Llm: pointerTo("llm-flow")}, &theirs))

	status, exported := s.serverClient.call(http.MethodGet, "/v1/data/export", nil)

	s.Require().Equal(http.StatusOK, status)
	s.NotContains(string(exported), theirs.Id)
}

func (s *DataMoveSuite) TestWhatHappensAfterAnExportIsInTheChangesSinceIt() {
	s.export()
	agent := s.data.createAgentConfig()

	changed := s.changesSince(0)

	s.True(changed.CaughtUp, "fewer changes than were asked for is the end of them")
	var tables []string
	for _, change := range changed.Changes {
		tables = append(tables, change.Table)
	}
	s.Contains(tables, "agent_configs")
	s.Contains(s.rendered(changed.Changes), agent.Id)
}

func (s *DataMoveSuite) TestFollowingTheChangesFromWhereTheLastPageEndedRepeatsNothing() {
	s.export()
	s.data.createAgentConfig()

	first := s.changesSince(0)
	s.data.createAgentConfig()
	second := s.changesSince(first.Cursor)

	s.NotContains(s.rendered(second.Changes), s.rendered(first.Changes))
}

// The numbers are shared by every app, so the ones between the export's cursor and the app's
// first change after it can be another moving app's.
func (s *DataMoveSuite) TestTheFirstChangeAfterAnExportIsAnsweredWhenAnotherAppChangedInBetween() {
	s.skipOnARouterWithShards()
	stranger := s.data.backendOfAnotherApp()
	s.Require().Equal(http.StatusOK, stranger.do(http.MethodGet, "/v1/data/export", nil, nil))
	theirs := func() {
		s.Require().Equal(http.StatusCreated, stranger.do(http.MethodPost, "/v1/agents/configs",
			AgentConfigRequest{Name: "agent-" + s.utils.uuid(), Llm: pointerTo("llm-flow")}, nil))
	}
	theirs()
	exported := s.export()
	cursor := exported[len(exported)-1].Cursor
	s.Require().NotNil(cursor, "the export did not finish")
	theirs()
	agent := s.data.createAgentConfig()

	changed := s.changesSince(*cursor)

	s.Contains(s.rendered(changed.Changes), agent.Id)
}

func (s *DataMoveSuite) TestACursorThatIsNotOneIsRefused() {
	s.skipOnARouterWithShards()
	status, failure := s.serverClient.failure(http.MethodGet, "/v1/data/changes?after=yesterday", nil)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "cursor")
}

func (s *DataMoveSuite) TestMoreChangesThanEitherEndCanHoldAreRefused() {
	s.skipOnARouterWithShards()
	status, failure := s.serverClient.failure(http.MethodGet, "/v1/data/changes?limit=5000", nil)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "between 1 and")
}

func (s *DataMoveSuite) TestImportingAnExportWritesTheRowsItCarried() {
	// Two rows, so a second row of an app the first one just gave data here is written too.
	s.data.createAgentConfig()
	s.data.createAgentConfig()
	exported := s.export()
	// Under ids of their own: the source's ids are another app's rows on this deployment.
	var copied []string
	for i, line := range exported {
		if line.Table == "agent_configs" {
			id := store.NewID()
			exported[i].Row = s.withID(line.Row, id)
			copied = append(copied, id)
		}
	}
	s.useApp(s.data.createApp())

	var written struct {
		Rows   int64            `json:"rows"`
		Tables map[string]int64 `json:"tables"`
		Cursor *int64           `json:"cursor"`
	}
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson(exported), &written))

	s.Equal(int64(2), written.Tables["agent_configs"])
	s.Equal(int64(len(exported)-1), written.Rows, "every line but the cursor was a row")
	s.Require().NotNil(written.Cursor)
	for _, id := range copied {
		s.Equal("be brief", s.instructionsOf(id), "the copy is the app's")
	}
}

// TestAnImportedConnectorNamingTheOperatorsClientIsRefused: what the API refuses to create as
// a custom connector, an import refuses too, so the operator's client is never sent to a token
// endpoint the file names. A connector the API would create imports as it is.
func (s *DataMoveSuite) TestAnImportedConnectorNamingTheOperatorsClientIsRefused() {
	refused, taken := "custom_a"+store.NewID(), "custom_a"+store.NewID()

	// full=true, so the first line's refusal is all that differs between the two imports: the
	// second would otherwise be a 409 for an app that holds data here.
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{
		s.importedConnector(refused, `"client": {"registration": ["operator"], "env": "GOOGLE"}, "schemes": ["oauth2_code"],
			"endpoints": {"mcp": "https://mcp.acme.example/mcp", "token": "https://acme.example/token"}`)}))
	taking := s.serverClient.do(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{
		s.importedConnector(taken, `"client": {"registration": ["customer"], "auth_method": "client_secret_post"}, "schemes": ["oauth2_code"],
			"endpoints": {"mcp": "https://mcp.acme.example/mcp"}, "scopes": {"list": ["read"]}, "category": "CRM", "description": "Acme's CRM"`)}), nil)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "client.registration cannot be operator for a custom connector")
	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+refused, nil, nil))
	s.Equal(http.StatusOK, taking)
	s.Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+taken, nil, nil))
}

// The operator rule reads the whole registration list, not its first entry.
func (s *DataMoveSuite) TestAnImportedConnectorNamingTheOperatorsClientAfterAnotherIsRefused() {
	refused := "custom_a" + store.NewID()

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{
		s.importedConnector(refused, `"client": {"registration": ["customer", "operator"], "env": "GOOGLE"}, "schemes": ["oauth2_code"],
			"endpoints": {"mcp": "https://mcp.acme.example/mcp"}`)}))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "client.registration cannot be operator for a custom connector")
	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+refused, nil, nil))
}

// A definition with oauth2_client_credentials, from before custom connectors lost it, is left out
// and named, an export row and a change alike; every other line is written.
func (s *DataMoveSuite) TestAnImportNamesTheClientCredentialsDefinitionsItLeftOut() {
	skipped, changed, taken := "custom_a"+store.NewID(), "custom_a"+store.NewID(), "custom_a"+store.NewID()
	change := s.importedConnector(changed, `"schemes": ["oauth2_client_credentials"], "endpoints": {"mcp": "https://mcp.acme.example/mcp"}`)

	var written struct {
		Rows    int64            `json:"rows"`
		Tables  map[string]int64 `json:"tables"`
		Skipped []string         `json:"skipped"`
	}
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{
		s.importedConnector(skipped, `"schemes": ["oauth2_client_credentials"], "endpoints": {"mcp": "https://mcp.acme.example/mcp"}`),
		s.importedConnector(taken, `"schemes": ["bearer"], "endpoints": {"mcp": "https://mcp.acme.example/mcp"}`),
		{Change: &store.DataChange{Table: change.Table, Op: store.ChangeInsert, Payload: change.Row}},
	}), &written))

	s.Equal([]string{`connector_definitions:{"id":"` + skipped + `","revision":1}`, `connector_definitions:{"id":"` + changed + `","revision":1}`},
		written.Skipped)
	s.Equal(int64(1), written.Rows)
	s.Equal(map[string]int64{"connector_definitions": 1}, written.Tables)
	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+skipped, nil, nil))
	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+changed, nil, nil))
	s.Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+taken, nil, nil))
}

// A client-credentials definition is left out only when it is a row the API could have stored:
// one with a built-in id, or an id past its pattern, is refused as before, and an oversized id
// is not kept for an answer.
func (s *DataMoveSuite) TestAClientCredentialsDefinitionWithAnIDTheAPIRefusesIsRefused() {
	cc := `"schemes": ["oauth2_client_credentials"], "endpoints": {"mcp": "https://mcp.acme.example/mcp"}`
	for _, id := range []string{"github", "custom_", "custom_A" + store.NewID()} {
		status, _ := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{s.importedConnector(id, cc)}))
		s.Equal(http.StatusBadRequest, status, id)
	}

	big := "custom_a" + strings.Repeat("x", 1<<20)
	lines := []dataLine{}
	for range 8 {
		lines = append(lines, s.importedConnector(big, cc))
	}
	status, _ := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", ndjson(lines))
	s.Equal(http.StatusBadRequest, status, "not a 200 holding every id")
}

// A refused id comes back once in the answer, not once for the check and again for the row it
// names, so a refused 1 MiB id does not double the body.
func (s *DataMoveSuite) TestARefusedImportedIDIsInTheAnswerAtMostOnce() {
	big := "custom_a" + strings.Repeat("x", 1<<20)
	line := s.importedConnector(big, `"schemes": ["oauth2_code"], "endpoints": {"mcp": "https://mcp.acme.example/mcp"}`)
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{line}))
	s.Equal(http.StatusBadRequest, status)
	s.LessOrEqual(strings.Count(failure, big), 1)
}

// The answer names the first 100 rows an import left out and counts the rest, so it does not
// grow with the body.
func (s *DataMoveSuite) TestAnImportNamesAtMostAHundredOfTheRowsItLeftOut() {
	lines := []dataLine{}
	for range 103 {
		lines = append(lines, s.importedConnector("custom_a"+store.NewID(), `"schemes": ["oauth2_client_credentials"], "endpoints": {"mcp": "https://mcp.acme.example/mcp"}`))
	}
	var written struct {
		Rows        int64    `json:"rows"`
		Skipped     []string `json:"skipped"`
		SkippedMore int64    `json:"skipped_more"`
	}
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import?full=true", ndjson(lines), &written))

	s.Len(written.Skipped, 100)
	s.Equal(int64(3), written.SkippedMore)
	s.Zero(written.Rows)
}

// A delete removes a row whatever its payload holds, so it left nothing out.
func (s *DataMoveSuite) TestADeleteThatCarriesAClientCredentialsDefinitionLeftNothingOut() {
	id := "custom_a" + store.NewID()
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{
		s.importedConnector(id, `"schemes": ["bearer"], "endpoints": {"mcp": "https://mcp.acme.example/mcp"}`),
	}), nil))
	cc := s.importedConnector(id, `"schemes": ["oauth2_client_credentials"], "endpoints": {"mcp": "https://mcp.acme.example/mcp"}`)
	var written struct {
		Skipped []string `json:"skipped"`
	}

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{
		{Change: &store.DataChange{Table: cc.Table, Op: store.ChangeDelete,
			Key: json.RawMessage(`{"id":"` + id + `","revision":1}`), Payload: cc.Row}},
	}), &written))

	s.Empty(written.Skipped)
	s.Equal(http.StatusNotFound, s.serverClient.do(http.MethodGet, "/v1/agents/connectors/"+id, nil, nil))
}

// An import that leaves nothing out says so with an empty list.
func (s *DataMoveSuite) TestAnImportThatLeftNothingOutNamesNone() {
	var written map[string]any
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{
		s.importedConnector("custom_a"+store.NewID(), `"schemes": ["bearer"], "endpoints": {"mcp": "https://mcp.acme.example/mcp"}`),
	}), &written))

	s.Equal([]any{}, written["skipped"])
}

// A database that fails is the router's failure, and its text names the database's address.
func (s *DataMoveSuite) TestAnImportTheDatabaseCannotWriteIsAFailureWithNoDatabaseText() {
	id := "custom_a" + store.NewID()
	s.failQueries(id)

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{
		s.importedConnector(id, `"client": {"registration": ["customer"], "auth_method": "client_secret_post"}, "schemes": ["oauth2_code"],
			"endpoints": {"mcp": "https://mcp.acme.example/mcp"}, "scopes": {"list": ["read"]}, "category": "CRM", "description": "Acme's CRM"`)}))

	s.Equal(http.StatusInternalServerError, status)
	s.NotContains(failure, "store:")
}

// A row Postgres refuses for what it holds is the export's to correct, and is told so without
// Postgres's own words, which name the table's indexes and columns.
func (s *DataMoveSuite) TestAnImportedAgentNamedLikeOneHereIsRefusedWithoutTheIndexName() {
	agent := s.data.createAgentConfig()
	var exported dataLine
	for _, line := range s.export() {
		if line.Table == "agent_configs" && strings.Contains(string(line.Row), agent.Id) {
			exported = line
		}
	}
	s.Require().NotEmpty(exported.Table)

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true",
		ndjson([]dataLine{{Table: "agent_configs", Row: s.withID(exported.Row, store.NewID())}}))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "another row here already holds")
	s.NotContains(failure, "agent_configs_name_idx")
}

// A row whose key another app holds here is refused by name, and the answer says nothing of who holds it.
func (s *DataMoveSuite) TestAnImportedRowWhoseKeyAnotherAppHoldsIsRefusedNamingIt() {
	theirs := s.data.createAgentConfig()
	var exported dataLine
	for _, line := range s.export() {
		if line.Table == "agent_configs" && strings.Contains(string(line.Row), theirs.Id) {
			exported = line
		}
	}
	s.Require().NotEmpty(exported.Table)
	s.useApp(s.data.createApp())

	change := dataLine{Change: &store.DataChange{Table: "agent_configs", Op: store.ChangeUpdate,
		Key: json.RawMessage(`{"id":"` + theirs.Id + `"}`), Payload: exported.Row}}

	for _, line := range []dataLine{exported, change} {
		status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", ndjson([]dataLine{line}))

		s.Equal(http.StatusBadRequest, status)
		s.Equal(`store: import agent_configs:{"id":"`+theirs.Id+`"}: this row cannot be written here`, failure)
	}
}

// importedConnector is an export's line of a custom definition id whose manifest has fields,
// JSON members, beside its id, name and MCP source.
func (s *DataMoveSuite) importedConnector(id, fields string) dataLine {
	return dataLine{Table: "connector_definitions", Row: json.RawMessage(`{"id": "` + id + `", "revision": 1, "name": "Acme",
		"category": "", "description": "", "created_at": "2026-11-01T00:00:00Z", "manifest": {"id": "` + id + `", "revision": 1,
		"name": "Acme", "sources": [{"kind": "mcp", "endpoint": "mcp"}], ` + fields + `}}`)}
}

func (s *DataMoveSuite) TestAnExportIsNotWrittenOverAnAppThatHoldsData() {
	agent := s.data.createAgentConfig()
	exported := s.export()
	s.instruct(agent.Id, "edited here")

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", ndjson(exported))

	s.Equal(http.StatusConflict, status)
	s.Contains(failure, "full=true")
	s.Equal("edited here", s.instructionsOf(agent.Id), "the edit made here since the export is kept")
}

func (s *DataMoveSuite) TestAnExportIsNotWrittenOverAnAppThatHoldsDataWhenFullIsFalse() {
	agent := s.data.createAgentConfig()
	exported := s.export()
	s.instruct(agent.Id, "edited here")

	status, _ := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=false", ndjson(exported))

	s.Equal(http.StatusConflict, status)
	s.Equal("edited here", s.instructionsOf(agent.Id))
}

// Defect: an app whose only rows here are opt-outs had an import refused without full.
func (s *DataMoveSuite) TestAnAppHoldingOnlyAnOptOutImportsWithoutFull() {
	ctx := context.Background()
	s.Require().NoError(s.store.OptOuts.Insert(ctx, &store.OptOut{CustomerID: s.customerID(), Recipient: "+15550001112", Channel: "sms", Source: "keyword"}))
	exported := s.export()
	for i, line := range exported {
		if line.Table == "opt_outs" {
			exported[i].Row = s.withID(line.Row, store.NewID())
		}
	}
	s.useApp(s.data.createApp()) // has made no request, so no activity row
	s.Require().NoError(s.store.OptOuts.Insert(ctx, &store.OptOut{CustomerID: s.customerID(), Recipient: "+15550001113", Channel: "sms", Source: "keyword"}))

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson(exported), nil))

	for _, recipient := range []string{"+15550001112", "+15550001113"} {
		optedOut, err := s.store.OptOuts.OptedOut(ctx, s.customerID(), recipient, "sms")
		s.Require().NoError(err)
		s.True(optedOut, recipient)
	}
}

// A lookup that fails is not an answer that the app holds nothing: the export is not written.
func (s *DataMoveSuite) TestAnExportIsNotWrittenWhenWhatTheAppHoldsCannotBeRead() {
	agent := s.data.createAgentConfig()
	exported := s.export()
	s.instruct(agent.Id, "edited here")
	closed, err := store.Open(s.sharedDSN())
	s.Require().NoError(err)
	other, _, _ := s.otherNodeServingFrom(closed, nil, s.sealer, slog.New(slog.DiscardHandler))
	s.Require().NoError(closed.Close())

	status, _ := s.serverClient.on(other).failure(http.MethodPost, "/v1/data/import", ndjson(exported))

	s.Equal(http.StatusInternalServerError, status)
	s.Equal("edited here", s.instructionsOf(agent.Id), "the edit made here since the export is kept")
}

func (s *DataMoveSuite) TestFullWritesAnExportOverAnAppThatHoldsData() {
	agent := s.data.createAgentConfig()
	exported := s.export()
	s.instruct(agent.Id, "edited here")

	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodPost, "/v1/data/import?full=true", ndjson(exported), nil))

	s.Equal("be brief", s.instructionsOf(agent.Id), "the export's row is written over the edit")
}

func (s *DataMoveSuite) TestAFullThatIsNeitherTrueNorFalseIsRefused() {
	agent := s.data.createAgentConfig()
	exported := s.export()
	s.instruct(agent.Id, "edited here")

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=yes", ndjson(exported))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "full")
	s.Equal("edited here", s.instructionsOf(agent.Id))
}

func (s *DataMoveSuite) TestChangesAreAppliedToAnAppThatHoldsData() {
	// Changes are the incremental path: each carries the row as the source has it now.
	agent := s.data.createAgentConfig()
	s.export()
	s.instruct(agent.Id, "edited at the source")
	changed := s.changesSince(0)
	s.instruct(agent.Id, "edited here")

	lines := make([]dataLine, 0, len(changed.Changes))
	for i := range changed.Changes {
		lines = append(lines, dataLine{Change: &changed.Changes[i]})
	}
	s.Require().Equal(http.StatusOK,
		s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson(lines), nil))

	s.Equal("edited at the source", s.instructionsOf(agent.Id))
}

// A row that names a row the app does not hold is the export's to correct, and the answer names it.
func (s *DataMoveSuite) TestAnImportedRowThatNamesARowTheAppDoesNotHoldIsRefusedNamingIt() {
	theirs, nobodys, turn := s.utils.uuid(), s.utils.uuid(), s.utils.uuid()
	s.Require().NoError(s.store.Sessions.Upsert(context.Background(), &store.AgentSession{ID: theirs, CustomerID: s.utils.uuid(),
		AgentName: "docs", UserID: "sam", CallerKind: "user", CreatedAt: time.Now()}))

	for _, session := range []string{theirs, nobodys} {
		row := json.RawMessage(`{"id": "` + turn + `", "session_id": "` + session + `", "said": "hello", "status": "completed",
			"error": "", "created_at": "2026-01-01T00:00:00Z"}`)
		for _, line := range []dataLine{{Table: "agent_responses", Row: row},
			{Change: &store.DataChange{Seq: 1, Table: "agent_responses", Op: store.ChangeInsert, At: time.Now(), Payload: row}}} {
			status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{line}))

			s.Equal(http.StatusBadRequest, status)
			s.Equal(`store: import agent_responses:{"id":"`+turn+`"}: this row cannot be written here`, failure, "whoever holds the session")
		}
	}
}

func (s *DataMoveSuite) TestAnImportedRunOfATaskTheAppDoesNotHoldIsRefusedTheSame() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", ndjson([]dataLine{{Table: "scheduled_task_runs",
		Row: json.RawMessage(`{"id": "run", "task_id": "not-here", "state": "delivered", "trigger": "schedule", "attempts": 0, "prompt": "",
			"delivered_to": "", "session_id": "", "texted": [], "scheduled_for": "2026-01-01T00:00:00Z", "created_at": "2026-01-01T00:00:00Z"}`)}}))

	s.Equal(http.StatusBadRequest, status)
	s.Equal(`store: import scheduled_task_runs:{"id":"run"}: this row cannot be written here`, failure)
}

// A change to a policy of another scope is skipped, so the answer counts no row for it.
func (s *DataMoveSuite) TestAChangeToAnotherScopesPolicyIsNotCountedAsWritten() {
	key := `{"scope": "organization", "scope_id": ` + strconv.Quote(s.customerID()) + `}`
	lines := []dataLine{{Change: &store.DataChange{
		Seq: 1, Table: "policies", Op: store.ChangeUpdate, At: time.Now(), Key: json.RawMessage(key),
		Payload: json.RawMessage(`{"scope": "organization", "scope_id": ` + strconv.Quote(s.customerID()) +
			`, "document": {"prompt_injection": true}, "updated_at": "2026-01-01T00:00:00Z"}`),
	}}}
	var answer struct {
		Rows   int64            `json:"rows"`
		Tables map[string]int64 `json:"tables"`
	}

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson(lines), &answer))

	s.Equal(int64(0), answer.Rows)
	s.Empty(answer.Tables)
}

func (s *DataMoveSuite) TestAnImportedBoughtNumberIsNotStored() {
	ctx := context.Background()
	other, e164 := s.utils.uuid(), s.utils.number()
	s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
		E164: e164, Vendor: "plivo", Country: "US", CustomerID: other,
	}))

	// The other app's number, as written and as Plivo reads it.
	var lines []dataLine
	for _, variant := range []string{e164, strings.TrimPrefix(e164, "+")} {
		row, err := json.Marshal(map[string]any{
			"id": rand.Int64N(1 << 50), "e164": variant, "vendor": "plivo", "country": "US",
			"capabilities": []string{"voice"}, "monthly_cost_micros": 0, "customer_id": other,
			"tags": map[string]string{}, "purchased_at": time.Now().UTC(),
		})
		s.Require().NoError(err)
		lines = append(lines, dataLine{Table: "phone_numbers", Row: row})
	}
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson(lines), nil))

	status, _ := s.serverClient.call(http.MethodDelete, "/v1/phone/numbers/"+e164, nil)
	s.Equal(http.StatusNotFound, status)
	imported, err := s.store.PhoneNumbers.Select(ctx, s.customerID(), store.PhoneNumberFilter{IncludeReleased: true})
	s.Require().NoError(err)
	s.Empty(imported)
	held, err := s.store.PhoneNumbers.Select(ctx, other, store.PhoneNumberFilter{})
	s.Require().NoError(err)
	s.Len(held, 1, "the number stays with the app that holds it")
}

// The other app holds the numbers bought; the import carries on past them, as a row of an
// export and as a change, and counts only what it wrote.
func (s *DataMoveSuite) TestAnImportedNumberOnItsOwnTrunkAnotherAppHoldsIsLeftOutAndNotCounted() {
	ctx := context.Background()
	other := s.utils.uuid()
	held, free := []string{s.utils.number(), s.utils.number()}, []string{s.utils.number(), s.utils.number()}
	for _, e164 := range held {
		s.Require().NoError(s.store.PhoneNumbers.Insert(ctx, &store.PhoneNumber{
			E164: e164, Vendor: "plivo", Country: "US", CustomerID: other,
		}))
	}
	onTrunk := func(e164 string) json.RawMessage {
		row, err := json.Marshal(map[string]any{
			"id": rand.Int64N(1 << 50), "e164": e164, "vendor": "sip_trunk", "country": "US",
			"capabilities": []string{"voice"}, "monthly_cost_micros": 0,
			"tags": map[string]string{}, "purchased_at": time.Now().UTC(),
		})
		s.Require().NoError(err)
		return row
	}
	lines := []dataLine{
		{Table: "phone_numbers", Row: onTrunk(held[0])},
		{Table: "phone_numbers", Row: onTrunk(free[0])},
		{Change: &store.DataChange{Seq: 1, Table: "phone_numbers", Op: store.ChangeInsert, At: time.Now(), Payload: onTrunk(held[1])}},
		{Change: &store.DataChange{Seq: 2, Table: "phone_numbers", Op: store.ChangeInsert, At: time.Now(), Payload: onTrunk(free[1])}},
	}
	var answer struct {
		Rows   int64            `json:"rows"`
		Tables map[string]int64 `json:"tables"`
	}

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/data/import", ndjson(lines), &answer))

	s.Equal(int64(2), answer.Rows)
	s.Equal(map[string]int64{"phone_numbers": 2}, answer.Tables)
	s.Regexp(`customer=`+regexp.QuoteMeta(s.customerID())+` rows=2 numbers_left_out=2`, s.logged.String(), "the router logs how many it left out")
	imported, err := s.store.PhoneNumbers.Select(ctx, s.customerID(), store.PhoneNumberFilter{IncludeReleased: true})
	s.Require().NoError(err)
	var numbers []string
	for _, number := range imported {
		numbers = append(numbers, number.E164)
	}
	s.ElementsMatch(free, numbers)
}

func (s *DataMoveSuite) TestAnImportedNumberOnItsOwnTrunkInAnotherFormIsRefused() {
	row, err := json.Marshal(map[string]any{
		"id": rand.Int64N(1 << 50), "e164": strings.TrimPrefix(s.utils.number(), "+"), "vendor": "sip_trunk",
		"country": "US", "capabilities": []string{"voice"}, "monthly_cost_micros": 0,
		"tags": map[string]string{}, "purchased_at": time.Now().UTC(),
	})
	s.Require().NoError(err)
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import",
		ndjson([]dataLine{{Table: "phone_numbers", Row: row}}))
	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "E.164")
}

func (s *DataMoveSuite) TestAnImportLineOverTheLimitIsRefused() {
	line := `{"table":"agent_configs","row":{"instructions":"` + strings.Repeat("x", maxImportLine) + `"}}`

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", line)

	s.Require().Equal(http.StatusRequestEntityTooLarge, status, failure[:min(len(failure), 200)])
	s.Contains(failure, "64 MiB")
}

func (s *DataMoveSuite) TestAnImportLineJustUnderTheLimitIsRead() {
	s.data.createAgentConfig()
	var row json.RawMessage
	for _, line := range s.export() {
		if line.Table == "agent_configs" {
			row = line.Row
		}
	}
	s.Require().NotNil(row, "the app has an agent")
	fields := map[string]any{}
	s.Require().NoError(json.Unmarshal(row, &fields))
	id := "agent-" + s.utils.uuid()
	fields["id"], fields["name"], fields["instructions"] = id, id, ""
	empty := s.lineOf(fields)
	filler := maxImportLine - 1 - len(empty)
	fields["instructions"] = strings.Repeat("x", filler)
	line := s.lineOf(fields)
	s.Require().Len(line, maxImportLine-1)

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", line)

	s.Require().Equal(http.StatusOK, status, failure[:min(len(failure), 200)])
	var stored int
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(context.Background(),
		"SELECT length(instructions) FROM agent_configs WHERE id = ? AND customer_id = ?", id, s.customerID()).Scan(&stored))
	s.Equal(filler, stored)
}

// lineOf is one line of an import carrying an agent config row.
func (s *DataMoveSuite) lineOf(fields map[string]any) string {
	row, err := json.Marshal(fields)
	s.Require().NoError(err)
	encoded, err := json.Marshal(dataLine{Table: "agent_configs", Row: row})
	s.Require().NoError(err)
	return string(encoded)
}

func (s *DataMoveSuite) TestASecondImportOfAnAppIsRefusedWhileOneIsRunning() {
	s.heldImport()

	status, payload := s.serverClient.call(http.MethodPost, "/v1/data/import", emptyImport)

	s.Require().Equal(http.StatusConflict, status)
	var failure ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &failure))
	s.Equal("import_running", failure.Error.Code, "told apart from an app that already holds data here")
	s.Contains(failure.Error.Message, "an import of this app is already running")
}

func (s *DataMoveSuite) TestABadFullIsRefusedWhileAnImportOfTheAppRuns() {
	s.heldImport()

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=maybe", emptyImport)

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "full is true or false")
}

func (s *DataMoveSuite) TestAnImportRefusedAtTheRoutersCapLeavesItsAppFree() {
	var hangUps []context.CancelFunc
	for range maxImports {
		s.useApp(s.data.createApp())
		hangUps = append(hangUps, s.heldImport())
	}
	s.useApp(s.data.createApp())
	status, _ := s.serverClient.failure(http.MethodPost, "/v1/data/import", emptyImport)
	s.Require().Equal(http.StatusServiceUnavailable, status)

	hangUps[0]()

	s.Eventually(func() bool {
		status, _ := s.serverClient.failure(http.MethodPost, "/v1/data/import", emptyImport)
		return status == http.StatusOK
	}, letGoWithin, 10*time.Millisecond, "the app refused at the cap still counts as importing")
}

func (s *DataMoveSuite) TestAnImportWhoseBodyStallsLetsGoOfItsPlace() {
	s.withImportLineWait(2 * time.Second)
	// A declared length is longer than what is sent, and small enough that net/http reads the
	// rest of it before answering, so the answer needs the stalled read to stay bounded.
	const chunked, declared = -1, 100
	for _, stall := range []struct {
		name   string
		sent   string
		length int64
	}{
		{"after a line, chunked", "{\"cursor\":1}\n", chunked},
		{"after a line, declared length", "{\"cursor\":1}\n", declared},
		{"after a blank line, chunked", "{\"cursor\":1}\n\n", chunked},
		{"after a blank line, declared length", "{\"cursor\":1}\n\n", declared},
		{"inside a line, chunked", "{\"cursor\":1}\n{\"cur", chunked},
		{"inside a line, declared length", "{\"cursor\":1}\n{\"cur", declared},
		{"inside the first line, chunked", "{\"cur", chunked},
		{"inside the first line, declared length", "{\"cur", declared},
		{"before a line's end, declared length", "{\"table\":\"nope\",\"row\":{}}", declared},
	} {
		s.Run(stall.name, func() {
			status, payload, took, _ := s.stalledImport(stall.sent, stall.length)

			s.Less(took, 4*time.Second, "answered after one wait for the line, not two")
			s.Require().Equal(http.StatusGatewayTimeout, status, payload)
			var failure ErrorResponse
			s.Require().NoError(json.Unmarshal([]byte(payload), &failure))
			s.Equal(ErrorType("timeout"), failure.Error.Type)
			s.Equal("timeout", failure.Error.Code)
			s.Equal("the import's next line did not arrive in time: send the body without pausing", failure.Error.Message)
			s.Eventually(func() bool {
				status, _ := s.serverClient.failure(http.MethodPost, "/v1/data/import", emptyImport)
				return status == http.StatusOK
			}, letGoWithin, 10*time.Millisecond, "the import whose body stalled still holds its places")
			s.Zero(len(s.router.imports))
		})
	}
}

// A body that breaks for another reason than the wait is the caller's to correct, whether what
// broke it is its length, its framing or a line cut short.
func (s *DataMoveSuite) TestAnImportWhoseBodyBreaksIsRefusedAsNotAnExport() {
	for _, broken := range []struct{ name, head, body, says string }{
		{"cut short after a line", "Content-Length: 100", "{\"cursor\":1}\n", "unexpected EOF"},
		{"cut short inside a line", "Content-Length: 100", "{\"cursor\":1}\n{\"cur", "unexpected EOF"},
		{"a chunk of no size", "Transfer-Encoding: chunked", "zz\r\n", "invalid byte in chunk length"},
	} {
		s.Run(broken.name, func() {
			base, err := url.Parse(s.serverClient.base())
			s.Require().NoError(err)
			conn, err := net.Dial("tcp", base.Host)
			s.Require().NoError(err)
			defer conn.Close()
			s.Require().NoError(conn.SetDeadline(time.Now().Add(10 * time.Second)))
			var head strings.Builder
			s.Require().NoError(s.serverClient.header.Write(&head))
			_, err = io.WriteString(conn, "POST /v1/data/import HTTP/1.1\r\nHost: "+base.Host+"\r\n"+head.String()+broken.head+"\r\n\r\n"+broken.body)
			s.Require().NoError(err)
			s.Require().NoError(conn.(*net.TCPConn).CloseWrite())

			response, err := http.ReadResponse(bufio.NewReader(conn), nil)

			s.Require().NoError(err)
			defer response.Body.Close()
			payload, err := io.ReadAll(response.Body)
			s.Require().NoError(err)
			s.Require().Equal(http.StatusBadRequest, response.StatusCode, string(payload))
			var failure ErrorResponse
			s.Require().NoError(json.Unmarshal(payload, &failure))
			s.Equal("invalid_request", failure.Error.Code)
			s.Equal("this is not an export: "+broken.says, failure.Error.Message)
		})
	}
}

// net/http reads the rest of a body before the first of the answer is sent, which is inside the
// handler for an answer over its buffer of 2 KiB and after it for a shorter one.
func (s *DataMoveSuite) TestAnImportRefusedAtALineLetsGoOfItsConnectionWhenTheBodyStalls() {
	s.withImportLineWait(500 * time.Millisecond)
	long := strings.Repeat("1", 3000)
	for _, refused := range []struct {
		name   string
		sent   string
		length int64
	}{
		{"a short answer, declared length", "{\"cursor\":1}\nnot an export\n", 200},
		{"a short answer, chunked", "{\"cursor\":1}\nnot an export\n", -1},
		{"a long number, declared length", "{\"cursor\":" + long + "}\n", 100000},
		{"a long number, chunked", "{\"cursor\":" + long + "}\n", -1},
		{"a long table name, declared length", "{\"change\":{\"table\":\"" + long + "\",\"op\":\"insert\",\"id\":1,\"row\":{}}}\n", 100000},
		{"a long table name, chunked", "{\"change\":{\"table\":\"" + long + "\",\"op\":\"insert\",\"id\":1,\"row\":{}}}\n", -1},
	} {
		s.Run(refused.name, func() {
			status, payload, took, closed := s.stalledImport(refused.sent, refused.length)

			s.Equal(http.StatusBadRequest, status, payload)
			s.Contains(payload, "invalid_request")
			s.True(closed, "the connection is kept, with the rest of its body unread")
			s.Less(took, 5*time.Second, "answered only once the client gave up")
			s.Eventually(func() bool {
				status, _ := s.serverClient.failure(http.MethodPost, "/v1/data/import", emptyImport)
				return status == http.StatusOK
			}, settleFor, 10*time.Millisecond, "the refused import still holds its places")
			s.Zero(len(s.router.imports))
		})
	}
}

func (s *DataMoveSuite) TestAnImportWhoseLinesKeepArrivingOutlastsTheLineWait() {
	s.withImportLineWait(time.Second)
	body, writer := io.Pipe()
	go func() {
		// Blank lines count: three in a row outlast the wait the line before them began.
		for _, line := range []string{"{\"cursor\":1}\n", "{\"cursor\":1}\n", "\n", "\n", "\n", "{\"cursor\":1}\n"} {
			time.Sleep(400 * time.Millisecond)
			_, _ = writer.Write([]byte(line))
		}
		_ = writer.Close()
	}()
	request, err := http.NewRequest(http.MethodPost, s.serverClient.base()+"/v1/data/import", body)
	s.Require().NoError(err)
	request.Header = s.serverClient.header.Clone()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()

	response, err := (&http.Client{Transport: transport}).Do(request)

	s.Require().NoError(err)
	defer response.Body.Close()
	payload, _ := io.ReadAll(response.Body)
	s.Equal(http.StatusOK, response.StatusCode, "each line arrived inside the wait: %s", payload)
}

// A body read whole before its rows are written: the wait for a line is not a wait for a
// row's write, nor for the request after it on the same connection.
func (s *DataMoveSuite) TestARowWrittenPastTheLineWaitKeepsItsConnection() {
	s.skipOnARouterWithShards()
	id := s.data.createAgentConfig().Id
	exported := s.export()
	lines := []dataLine{exported[len(exported)-1]}
	for _, line := range exported {
		if line.Table == "agent_configs" {
			lines = append(lines, line, line)
		}
	}
	s.withImportLineWait(500 * time.Millisecond)
	tx, err := s.store.GlobalDB().BeginTx(context.Background(), nil)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(context.Background(), "SELECT 1 FROM agent_configs WHERE id = ? FOR UPDATE", id)
	s.Require().NoError(err)
	timer := time.AfterFunc(2*time.Second, func() { _ = tx.Rollback() })
	defer timer.Stop()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	send := func(body string, reused *bool) (int, string) {
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { *reused = info.Reused },
		})
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.serverClient.base()+"/v1/data/import?full=true", strings.NewReader(body))
		s.Require().NoError(err)
		request.Header = s.serverClient.header.Clone()
		response, err := client.Do(request)
		s.Require().NoError(err)
		defer response.Body.Close()
		payload, err := io.ReadAll(response.Body)
		s.Require().NoError(err)
		return response.StatusCode, string(payload)
	}

	var first, second bool
	status, payload := send(ndjson(lines), &first)
	s.Equal(http.StatusOK, status, "the import whose row waited on a lock: %s", payload)
	status, payload = send(ndjson(lines), &second)

	s.Equal(http.StatusOK, status, "the request after it on the same connection: %s", payload)
	s.True(second, "the second request went on a new connection")
}

func (s *DataMoveSuite) TestAFifthImportIsRefusedAndAFourthIsNot() {
	for range 4 {
		s.useApp(s.data.createApp())
		s.heldImport()
	}
	s.useApp(s.data.createApp())

	status, _ := s.serverClient.failure(http.MethodPost, "/v1/data/import", emptyImport)

	s.Equal(http.StatusServiceUnavailable, status)
}

func (s *DataMoveSuite) TestALineIsWaitedForAMinute() {
	s.Equal(time.Minute, s.router.importLineWait)
}

func (s *DataMoveSuite) TestASecondImportOfAnAppIsToldSoAtTheRoutersCap() {
	for range 4 {
		s.useApp(s.data.createApp())
		s.heldImport()
	}

	status, payload := s.serverClient.call(http.MethodPost, "/v1/data/import", emptyImport)

	s.Require().Equal(http.StatusConflict, status, string(payload))
	s.Contains(string(payload), "import_running")
}

func (s *DataMoveSuite) TestAnImportIsServedWhileItsAppsExportRuns() {
	s.skipOnARouterWithShards()
	s.seedRowsTooBigToBuffer()
	s.Require().Equal(http.StatusOK, s.stalledExport().StatusCode)

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", emptyImport)

	s.Equal(http.StatusOK, status, failure)
	status, failure = s.serverClient.failure(http.MethodGet, "/v1/data/export", nil)
	s.Equal(http.StatusConflict, status, "the export still holds its app's place")
	s.Contains(failure, "an export of this app is already running")
}

func (s *DataMoveSuite) TestAnImportOverTheRoutersCapIsRefused() {
	for range maxImports {
		s.useApp(s.data.createApp())
		s.heldImport()
	}
	s.useApp(s.data.createApp())

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", emptyImport)

	s.Require().Equal(http.StatusServiceUnavailable, status)
	s.Contains(failure, "as many imports as it takes at once")
}

func (s *DataMoveSuite) TestAnImportThatFailsLetsGoOfItsPlace() {
	for range maxImports + 1 {
		status, _ := s.serverClient.failure(http.MethodPost, "/v1/data/import", "not an export at all")
		s.Require().Equal(http.StatusBadRequest, status)
	}

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", emptyImport)

	s.Equal(http.StatusOK, status, failure)
}

func (s *DataMoveSuite) TestAnImportHungUpOnLetsGoOfItsPlace() {
	var hangUps []context.CancelFunc
	for range maxImports {
		s.useApp(s.data.createApp())
		hangUps = append(hangUps, s.heldImport())
	}
	for _, hangUp := range hangUps {
		hangUp()
	}

	// The last app hung up on its own import, so a new one of it needs both its own place and
	// one of the router's back.
	s.Eventually(func() bool {
		status, _ := s.serverClient.failure(http.MethodPost, "/v1/data/import", emptyImport)
		return status == http.StatusOK
	}, letGoWithin, 10*time.Millisecond, "the imports hung up on still hold their places")
}

func (s *DataMoveSuite) TestAnImportIsServedWhileTheRoutersExportsRun() {
	s.skipOnARouterWithShards()
	for range maxExports {
		s.useApp(s.data.createApp())
		s.seedRowsTooBigToBuffer()
		s.Require().Equal(http.StatusOK, s.stalledExport().StatusCode)
	}
	s.useApp(s.data.createApp())

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", emptyImport)

	s.Equal(http.StatusOK, status, failure)
}

// withImportLineWait bounds how long this test's imports wait for a line.
func (s *DataMoveSuite) withImportLineWait(wait time.Duration) {
	s.Equal(importLineWait, s.router.importLineWait, "the default bound")
	s.router.importLineWait = wait
	s.T().Cleanup(func() { s.router.importLineWait = importLineWait })
}

// emptyImport is an import with no rows, which is written at once.
const emptyImport = `{"cursor":1}`

// stalledImport sends the start of an import's body and no more of it, and returns the answer,
// how long it took, and whether the router closes the connection.
func (s *DataMoveSuite) stalledImport(sent string, length int64) (int, string, time.Duration, bool) {
	body, writer := io.Pipe()
	defer writer.Close()
	go func() { _, _ = writer.Write([]byte(sent)) }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The client's send waits on the body, which only closing it ends.
	context.AfterFunc(ctx, func() { _ = writer.Close() })
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.serverClient.base()+"/v1/data/import", body)
	s.Require().NoError(err)
	request.Header = s.serverClient.header.Clone()
	request.ContentLength = length
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	began := time.Now()
	response, err := (&http.Client{Transport: transport}).Do(request)
	s.Require().NoError(err, "the stalled import was never answered")
	took := time.Since(began)
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	s.Require().NoError(err)
	return response.StatusCode, string(payload), took, response.Close
}

// heldImport starts an import of the app whose body never ends, waits until it holds one of
// the router's places, and returns what hangs up on it.
func (s *DataMoveSuite) heldImport() context.CancelFunc {
	held := len(s.router.imports)
	body, writer := io.Pipe()
	ctx, hangUp := context.WithCancel(context.Background())
	s.T().Cleanup(func() {
		hangUp()
		_ = writer.Close()
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.serverClient.base()+"/v1/data/import", body)
	s.Require().NoError(err)
	request.Header = s.serverClient.header.Clone()
	transport := &http.Transport{}
	s.T().Cleanup(transport.CloseIdleConnections)
	go func() {
		response, err := (&http.Client{Transport: transport}).Do(request)
		if err == nil {
			_ = response.Body.Close()
		}
	}()
	s.Require().Eventually(func() bool { return len(s.router.imports) == held+1 }, settleFor, 10*time.Millisecond,
		"the import never started")
	return hangUp
}

// An export holds a connection of the router's pool until its client has read it all.
func (s *DataMoveSuite) TestASecondExportOfAnAppIsRefusedWhileOneIsRunning() {
	s.skipOnARouterWithShards()
	s.seedRowsTooBigToBuffer()
	s.Require().Equal(http.StatusOK, s.stalledExport().StatusCode)

	status, failure := s.serverClient.failure(http.MethodGet, "/v1/data/export", nil)

	s.Require().Equal(http.StatusConflict, status)
	s.Contains(failure, "already running")
}

func (s *DataMoveSuite) TestOtherRequestsAreServedWhileExportsRun() {
	s.skipOnARouterWithShards()
	s.seedRowsTooBigToBuffer()
	statuses := map[int]int{}
	for range suiteConnections {
		statuses[s.stalledExport().StatusCode]++
	}

	s.Equal(http.StatusCreated, s.anotherRequest())
	s.Equal(map[int]int{http.StatusOK: 1, http.StatusConflict: suiteConnections - 1}, statuses)
}

func (s *DataMoveSuite) TestOtherRequestsAreServedWhileExportsOfManyAppsRun() {
	s.skipOnARouterWithShards()
	s.Require().Less(maxExports, suiteConnections, "the suite's pool holds more than the exports a router runs")
	statuses := map[int]int{}
	for range suiteConnections {
		s.useApp(s.data.createApp())
		s.seedRowsTooBigToBuffer()
		statuses[s.stalledExport().StatusCode]++
	}

	s.Equal(http.StatusCreated, s.anotherRequest())
	s.Equal(map[int]int{http.StatusOK: maxExports, http.StatusServiceUnavailable: suiteConnections - maxExports}, statuses)
}

// The rows of a table an export ends in the middle of are not all read before it lets go.
func (s *DataMoveSuite) TestAnExportHungUpOnLetsGoAtOnce() {
	s.skipOnARouterWithShards()
	// 20,000 rows of 64 KiB: reading them all takes seconds, and a batch of them a moment.
	// Written 1,000 at a time, so no one statement runs into the driver's read timeout.
	for range 20 {
		_, err := s.store.GlobalDB().ExecContext(context.Background(), `
INSERT INTO agent_configs (id, customer_id, name, instructions)
SELECT 'agent-' || gen_random_uuid(), ?, 'agent-' || gen_random_uuid(), repeat('x', 64 << 10)
FROM generate_series(1, 1000)`, s.customerID())
		s.Require().NoError(err)
	}
	export := s.stalledExport()
	s.Require().Equal(http.StatusOK, export.StatusCode)
	lines := bufio.NewReader(export.Body)
	for range 3 {
		_, err := lines.ReadBytes('\n')
		s.Require().NoError(err)
	}

	s.Require().NoError(export.Body.Close())

	s.Eventually(func() bool { return len(s.router.exports) == 0 }, 2*time.Second, 10*time.Millisecond,
		"the export hung up on still holds its place")
}

func (s *DataMoveSuite) TestAnExportReadSlowlyEndsAtItsDeadline() {
	s.skipOnARouterWithShards()
	s.seedRowsTooBigToBuffer()
	s.withExportWait(2 * time.Second)
	slow := s.stalledExport()
	s.Require().Equal(http.StatusOK, slow.StatusCode)
	go func() {
		// 640 KB a second: the whole export would take most of a minute, and no line a minute.
		chunk := make([]byte, 64<<10)
		for {
			if _, err := io.ReadFull(slow.Body, chunk); err != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	s.Eventually(func() bool { return len(s.router.exports) == 0 }, letGoWithin, 50*time.Millisecond,
		"the export read slowly is still running")
}

// A line cut by the export's deadline breaks the response rather than ending it cleanly, so the
// client cannot take what it read for an export that stopped between lines.
func (s *DataMoveSuite) TestAnExportCutAtItsDeadlineReadsAsBroken() {
	s.skipOnARouterWithShards()
	s.seedRowsTooBigToBuffer()
	s.withExportWait(1500 * time.Millisecond)
	export := s.stalledExport()
	s.Require().Equal(http.StatusOK, export.StatusCode)

	var read bytes.Buffer
	chunk := make([]byte, 64<<10)
	var readErr error
	for readErr == nil {
		var n int
		n, readErr = export.Body.Read(chunk)
		read.Write(chunk[:n])
		time.Sleep(20 * time.Millisecond)
	}

	s.ErrorIs(readErr, io.ErrUnexpectedEOF)
	s.NotContains(read.String(), `"cursor"`, "the export was cut before its end")
}

func (s *DataMoveSuite) TestAnExportOutOfTimeBeforeItsFirstRowIsToldToTryAgain() {
	s.skipOnARouterWithShards()
	s.data.createAgentConfig()
	s.withExportWait(time.Nanosecond)

	status, failure := s.serverClient.failure(http.MethodGet, "/v1/data/export", nil)

	s.Equal(http.StatusServiceUnavailable, status)
	s.Contains(failure, "ran out of time before its first row")
}

func (s *DataMoveSuite) TestAnImportWithBlankLinesIsRead() {
	s.data.createAgentConfig()
	exported := s.export()
	rows := 0
	var lines []string
	for _, line := range exported {
		if line.Table != "" {
			rows++
		}
		encoded, err := json.Marshal(line)
		s.Require().NoError(err)
		lines = append(lines, string(encoded))
	}

	var imported DataImport
	status := s.serverClient.do(http.MethodPost, "/v1/data/import?full=true", strings.Join(lines, "\n\n  \n"), &imported)

	s.Require().Equal(http.StatusOK, status)
	s.Equal(int64(rows), imported.Rows)
}

func (s *DataMoveSuite) TestAnImportWithAnObjectOverSeveralLinesIsToldToWriteOnePerLine() {
	s.data.createAgentConfig()
	var lines []string
	for _, line := range s.export() {
		encoded, err := json.MarshalIndent(line, "", "  ")
		s.Require().NoError(err)
		lines = append(lines, string(encoded))
	}

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", strings.Join(lines, "\n"))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "one JSON object per line")
}

func (s *DataMoveSuite) TestAnImportWithTwoObjectsOnALineIsToldToWriteOnePerLine() {
	s.data.createAgentConfig()
	var objects []string
	for _, line := range s.export() {
		encoded, err := json.Marshal(line)
		s.Require().NoError(err)
		objects = append(objects, string(encoded))
	}

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", strings.Join(objects, ""))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "one JSON object per line")
}

func (s *DataMoveSuite) TestAnImportStopsAtTheLineItRefuses() {
	agent := s.data.createAgentConfig()
	exported := s.export()
	s.instruct(agent.Id, "edited here")

	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import?full=true", "{\n"+ndjson(exported))

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "one JSON object per line")
	s.Equal("edited here", s.instructionsOf(agent.Id), "no line after the refused one is written")
}

func (s *DataMoveSuite) TestAnExportWhoseClientStopsReadingIsLetGo() {
	s.skipOnARouterWithShards()
	s.seedRowsTooBigToBuffer()
	s.Equal(exportLineWait, s.router.exportLineWait, "the default bound")
	s.router.exportLineWait = 500 * time.Millisecond
	s.T().Cleanup(func() { s.router.exportLineWait = exportLineWait })
	s.Require().Equal(http.StatusOK, s.stalledExport().StatusCode)

	s.Eventually(func() bool { return len(s.router.exports) == 0 }, letGoWithin, 50*time.Millisecond,
		"the export nobody reads is still running")
}

func (s *DataMoveSuite) TestSomethingThatIsNotAnExportIsRefused() {
	status, failure := s.serverClient.failure(http.MethodPost, "/v1/data/import", "not an export at all")

	s.Equal(http.StatusBadRequest, status)
	s.Contains(failure, "not an export")
}

func (s *DataMoveSuite) TestOnlyTheAppsOwnBackendMayTakeTheDataOut() {
	s.skipOnARouterWithShards()
	s.assertPosture(serverOnly, func(as *testClient) int {
		status, _ := as.call(http.MethodGet, "/v1/data/export", nil)
		return status
	})
}

func (s *DataMoveSuite) TestOnlyTheAppsOwnBackendMayWriteDataIn() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		status, _ := as.call(http.MethodPost, "/v1/data/import", nil)
		return status
	})
}

func (s *DataMoveSuite) TestOnlyTheAppsOwnBackendMayFollowTheChanges() {
	s.skipOnARouterWithShards()
	// An export somebody may not have is no safer if the change feed hands the same rows
	// over one at a time.
	s.assertPosture(serverOnly, func(as *testClient) int {
		status, _ := as.call(http.MethodGet, "/v1/data/changes", nil)
		return status
	})
}

// A write whose trigger looked for the recording before the export started it recorded
// nothing, so an export whose snapshot is read while it is open cannot have it either.
func (s *DataMoveSuite) TestAWriteOpenWhenAnExportStartsIsInTheExportOrItsChanges() {
	s.skipOnARouterWithShards()
	open := s.openWrite(s.customerID())
	s.waitAtMost(100 * time.Millisecond)

	first, body := s.serverClient.call(http.MethodGet, "/v1/data/export", nil)
	s.Require().NoError(open.tx.Commit())
	exported := string(body)
	if first != http.StatusOK {
		exported = ndjson(s.export())
	}
	var changed changedRows
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/data/changes?after="+strconv.FormatInt(s.cursorOf(exported), 10), nil, &changed))

	s.True(strings.Contains(exported, open.id) || strings.Contains(s.rendered(changed.Changes), open.id),
		"the write is in neither the export nor the changes after it")
	s.Equal(http.StatusServiceUnavailable, first, "an export that cannot wait it out says so")
}

func (s *DataMoveSuite) TestAnExportWaitsForAWriteOpenWhenItStartedToCommit() {
	s.skipOnARouterWithShards()
	open := s.openWrite(s.customerID())
	type answer struct {
		status int
		body   []byte
		err    error
	}
	request, err := http.NewRequest(http.MethodGet, s.serverClient.base()+"/v1/data/export", nil)
	s.Require().NoError(err)
	request.Header = s.serverClient.header.Clone()
	answered := make(chan answer, 1)
	go func() {
		response, err := s.server.Client().Do(request)
		if err != nil {
			answered <- answer{err: err}
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		answered <- answer{status: response.StatusCode, body: body, err: err}
	}()
	s.Require().EventuallyWithT(func(c *assert.CollectT) {
		var capturing bool
		assert.NoError(c, s.store.GlobalDB().QueryRowContext(context.Background(),
			"SELECT EXISTS (SELECT 1 FROM data_change_capture WHERE customer_id = ?)", s.customerID()).Scan(&capturing))
		assert.True(c, capturing, "the export has not started recording")
	}, settleFor, 10*time.Millisecond)
	s.Require().NoError(open.tx.Commit())

	select {
	case got := <-answered:
		s.Require().NoError(got.err)
		s.Equal(http.StatusOK, got.status)
		s.Contains(string(got.body), open.id)
	case <-time.After(exportWritesWait):
		s.Fail("the export did not answer once the write committed")
	}
}

// Postgres cannot tell another app's open write from this one's, so the export is bounded and
// answers rather than hangs.
func (s *DataMoveSuite) TestAnExportGivesUpOnAWriteThatStaysOpen() {
	s.skipOnARouterWithShards()
	stranger := s.openWrite("another-app-" + s.utils.uuid())
	s.waitAtMost(100 * time.Millisecond)

	status, body := s.serverClient.call(http.MethodGet, "/v1/data/export", nil)
	s.Equal(http.StatusServiceUnavailable, status)
	s.Contains(string(body), "still open")

	s.Require().NoError(stranger.tx.Rollback())
	s.export()
}

// A repeatable read write that looked for the recording before the export started it holds the
// start, which gives up within the same bound as the wait after it.
func (s *DataMoveSuite) TestAnExportGivesUpOnARepeatableReadWriteThatLookedBeforeIt() {
	s.skipOnARouterWithShards()
	stranger := s.openWriteAt(sql.LevelRepeatableRead, "another-app-"+s.utils.uuid())
	s.waitAtMost(100 * time.Millisecond)

	status, body := s.serverClient.call(http.MethodGet, "/v1/data/export", nil)
	s.Equal(http.StatusServiceUnavailable, status)
	s.Contains(string(body), "still open")

	s.Require().NoError(stranger.tx.Rollback())
	s.export()
}

// Following the changes of a move whose recording expired starts it again, which gives up on such
// a write too.
func (s *DataMoveSuite) TestFollowingAnExpiredRecordingGivesUpOnARepeatableReadWriteThatLookedBeforeIt() {
	s.skipOnARouterWithShards()
	s.export()
	_, err := s.store.GlobalDB().ExecContext(context.Background(),
		"UPDATE data_change_capture SET expires_at = now() - interval '1 minute' WHERE customer_id = ?", s.customerID())
	s.Require().NoError(err)
	stranger := s.openWriteAt(sql.LevelRepeatableRead, "another-app-"+s.utils.uuid())
	s.waitAtMost(100 * time.Millisecond)

	status, body := s.serverClient.call(http.MethodGet, "/v1/data/changes?after=0", nil)
	s.Equal(http.StatusServiceUnavailable, status)
	s.Contains(string(body), "still open")

	s.Require().NoError(stranger.tx.Rollback())
}

// The start and the wait after it share one bound, not one each: a repeatable read write holds the
// start for most of the bound and a read committed write stays open past what is left of it.
func (s *DataMoveSuite) TestAnExportSpendsOneBoundOnTheStartAndTheWaitTogether() {
	s.skipOnARouterWithShards()
	stranger := s.openWrite("another-app-" + s.utils.uuid())
	looked := s.openWriteAt(sql.LevelRepeatableRead, "another-app-"+s.utils.uuid())
	s.waitAtMost(2 * time.Second)
	go func() {
		time.Sleep(1400 * time.Millisecond)
		_ = looked.tx.Rollback()
		time.Sleep(1600 * time.Millisecond)
		_ = stranger.tx.Rollback()
	}()

	status, body := s.serverClient.call(http.MethodGet, "/v1/data/export", nil)
	s.Equal(http.StatusServiceUnavailable, status)
	s.Contains(string(body), "still open")
}

// A repeatable read write that looked before the export, and ends within the bound, only delays it.
func (s *DataMoveSuite) TestAnExportStartWaitsForARepeatableReadWriteThatEndsWithinTheBound() {
	s.skipOnARouterWithShards()
	looked := s.openWriteAt(sql.LevelRepeatableRead, "another-app-"+s.utils.uuid())
	s.waitAtMost(5 * time.Second)
	go func() { time.Sleep(500 * time.Millisecond); _ = looked.tx.Rollback() }()

	s.export()
}

// Following the changes of an expired recording waits for such a write within the bound too, and
// then refuses the cursor from before the recording expired.
func (s *DataMoveSuite) TestFollowingAnExpiredRecordingWaitsForARepeatableReadWriteThatEndsWithinTheBound() {
	s.skipOnARouterWithShards()
	s.export()
	_, err := s.store.GlobalDB().ExecContext(context.Background(),
		"UPDATE data_change_capture SET expires_at = now() - interval '1 minute' WHERE customer_id = ?", s.customerID())
	s.Require().NoError(err)
	looked := s.openWriteAt(sql.LevelRepeatableRead, "another-app-"+s.utils.uuid())
	s.waitAtMost(5 * time.Second)
	go func() { time.Sleep(500 * time.Millisecond); _ = looked.tx.Rollback() }()

	status, _ := s.serverClient.call(http.MethodGet, "/v1/data/changes?after=0", nil)
	s.Equal(http.StatusGone, status)
}

// expireRecording lets the app's recording expire, as it does when nobody follows the changes for
// longer than the retention.
func (s *DataMoveSuite) expireRecording() {
	_, err := s.store.GlobalDB().ExecContext(context.Background(),
		"UPDATE data_change_capture SET expires_at = now() - interval '1 minute' WHERE customer_id = ?", s.customerID())
	s.Require().NoError(err)
}

// A write made while the recording was off is in no change, so a follower back after that is told
// to copy again rather than answered without it. The export it copies again carries on.
func (s *DataMoveSuite) TestAFollowerBackAfterTheRecordingExpiredIsToldToExportAgain() {
	exported := s.export()
	s.data.createAgentConfig()
	followed := s.changesSince(*exported[len(exported)-1].Cursor)
	s.expireRecording()
	missed := s.data.createAgentConfig()

	status, body := s.serverClient.call(http.MethodGet,
		"/v1/data/changes?after="+strconv.FormatInt(followed.Cursor, 10), nil)
	var refused ErrorResponse
	s.Require().NoError(json.Unmarshal(body, &refused))
	s.Equal(http.StatusGone, status)
	s.Equal("gone", refused.Error.Code)
	s.Contains(refused.Error.Message, "export again")

	again := s.export()
	s.Contains(ndjson(again), missed.Id, "the write made while nothing was recorded is in the new export")
	after := s.data.createAgentConfig()
	s.Contains(s.rendered(s.changesSince(*again[len(again)-1].Cursor).Changes), after.Id)
}

// A follower that reads within the retention keeps the recording on, and is answered every change.
func (s *DataMoveSuite) TestAFollowerReadingWithinTheRetentionIsAnsweredEveryChange() {
	exported := s.export()
	cursor := *exported[len(exported)-1].Cursor
	first := s.data.createAgentConfig()
	followed := s.changesSince(cursor)
	second := s.data.createAgentConfig()

	s.Contains(s.rendered(s.changesSince(followed.Cursor).Changes), second.Id)
	again := s.rendered(s.changesSince(cursor).Changes)
	s.Contains(again, first.Id)
	s.Contains(again, second.Id)
}

// A shard's tables record no changes, so a write to one after the export would never reach the
// changes: a router with a shard of its own refuses to be the source, before it records anything.
func (s *DataMoveSuite) TestARouterWithAShardOfItsOwnRefusesToBeTheSource() {
	s.refusesToBeTheSource(store.Options{DSN: s.sharedDSN(), ShardDSN: s.shardDSN()})
}

// A router writing its calls in the shared database still reads the other shards' calls, whose
// writes no change records either.
func (s *DataMoveSuite) TestARouterReadingOtherShardsRefusesToBeTheSource() {
	shared := s.sharedDSN()
	s.refusesToBeTheSource(store.Options{DSN: shared, Shards: map[string]string{"here": shared, "there": s.shardDSN()}})
}

// refusesToBeTheSource asserts a router on the databases options names refuses the export and the
// changes, and starts no recording for the caller.
func (s *DataMoveSuite) refusesToBeTheSource(options store.Options) {
	served, err := store.OpenShards(options)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(served.Close()) })
	other, _, _ := s.otherNodeServingFrom(served, nil, s.sealer, slog.New(slog.DiscardHandler))
	source := s.serverClient.on(other)

	for _, path := range []string{"/v1/data/export", "/v1/data/changes?after=0"} {
		status, body := source.call(http.MethodGet, path, nil)
		var refused ErrorResponse
		s.Require().NoError(json.Unmarshal(body, &refused), path)

		s.Equal(http.StatusBadRequest, status, path)
		s.Equal(codeNotConfigured, refused.Error.Code, path)
		s.Contains(refused.Error.Message, "with shards is not supported yet", path)
	}
	var capturing bool
	s.Require().NoError(s.store.GlobalDB().QueryRowContext(context.Background(),
		"SELECT EXISTS (SELECT 1 FROM data_change_capture WHERE customer_id = ?)", s.customerID()).Scan(&capturing))
	s.False(capturing, "a refused move started recording the app's changes")
}

// sharedDSN is the suite's database, which a router of shards shares.
func (s *DataMoveSuite) sharedDSN() string {
	return testDatabase(s.T(), os.Getenv("ROUTER_POSTGRES_DSN"))
}

// shardDSN is a migrated database of this suite's own, for one shard's tables.
func (s *DataMoveSuite) shardDSN() string {
	dsn := testenv.Database(s.sharedDSN(), "moveshard")
	shard, err := store.Open(dsn)
	s.Require().NoError(err)
	defer shard.Close()
	s.Require().NoError(migrate(context.Background(), shard, dsn))
	return dsn
}

// withExportWait bounds this test's exports as a whole.
func (s *DataMoveSuite) withExportWait(wait time.Duration) {
	s.Equal(exportWait, s.router.exportWait, "the default bound")
	s.router.exportWait = wait
	s.T().Cleanup(func() { s.router.exportWait = exportWait })
}

// seedRowsTooBigToBuffer gives the app 32 MiB of agent configs, more than the sockets between
// the router and a client hold, so an export whose client stops reading stops writing.
func (s *DataMoveSuite) seedRowsTooBigToBuffer() {
	_, err := s.store.GlobalDB().ExecContext(context.Background(), `
INSERT INTO agent_configs (id, customer_id, name, instructions)
SELECT 'agent-' || gen_random_uuid(), ?, 'agent-' || gen_random_uuid(), repeat('x', 1 << 20)
FROM generate_series(1, 32)`, s.customerID())
	s.Require().NoError(err)
}

// stalledExport starts an export on a connection of its own and reads nothing past the headers.
func (s *DataMoveSuite) stalledExport() *http.Response {
	request, err := http.NewRequest(http.MethodGet, s.serverClient.base()+"/v1/data/export", nil)
	s.Require().NoError(err)
	request.Header = s.serverClient.header.Clone()
	transport := &http.Transport{}
	response, err := (&http.Client{Transport: transport}).Do(request)
	s.Require().NoError(err)
	s.T().Cleanup(func() {
		_ = response.Body.Close()
		transport.CloseIdleConnections()
	})
	return response
}

// anotherRequest creates an agent config of the app, giving up after settleFor, and answers its
// status.
func (s *DataMoveSuite) anotherRequest() int {
	request, err := http.NewRequestWithContext(s.T().Context(), http.MethodPost, s.serverClient.base()+"/v1/agents/configs",
		strings.NewReader(`{"name":"agent-`+s.utils.uuid()+`","llm":"llm-flow"}`))
	s.Require().NoError(err)
	request.Header = s.serverClient.header.Clone()
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: settleFor}).Do(request)
	s.Require().NoError(err, "the request found no free connection")
	s.Require().NoError(response.Body.Close())
	return response.StatusCode
}

// openWrite writes an agent config of a customer in a transaction the test ends.
func (s *DataMoveSuite) openWrite(customerID string) openWrite {
	return s.openWriteAt(sql.LevelDefault, customerID)
}

// openWriteAt is openWrite in a transaction at an isolation level.
func (s *DataMoveSuite) openWriteAt(level sql.IsolationLevel, customerID string) openWrite {
	tx, err := s.store.GlobalDB().BeginTx(context.Background(), &sql.TxOptions{Isolation: level})
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = tx.Rollback() })
	id := "agent-" + s.utils.uuid()
	_, err = tx.ExecContext(context.Background(),
		"INSERT INTO agent_configs (id, customer_id, name, instructions) VALUES (?, ?, ?, ?)",
		id, customerID, id, "be brief")
	s.Require().NoError(err)
	return openWrite{tx: tx, id: id}
}

type openWrite struct {
	tx bun.Tx
	id string
}

// waitAtMost bounds how long this test's exports wait for the writes open when they start.
func (s *DataMoveSuite) waitAtMost(wait time.Duration) {
	s.Equal(exportWritesWait, s.router.exportWritesWait, "the default bound")
	s.router.exportWritesWait = wait
	s.T().Cleanup(func() { s.router.exportWritesWait = exportWritesWait })
}

// cursorOf is the cursor on the last line of an export.
func (s *DataMoveSuite) cursorOf(exported string) int64 {
	var cursor *int64
	decoder := json.NewDecoder(strings.NewReader(exported))
	for decoder.More() {
		var line dataLine
		s.Require().NoError(decoder.Decode(&line))
		cursor = line.Cursor
	}
	s.Require().NotNil(cursor, "the export did not finish")
	return *cursor
}

// skipOnARouterWithShards skips a test that takes the suite's own router as a move's source,
// which refuses to be one when ROUTER_POSTGRES_SHARD_DSN gives it shards (see the refusal tests
// below).
func (s *DataMoveSuite) skipOnARouterWithShards() {
	// Sharded, not HasShards: the refusal is what HasShards decides, and a test that skipped on
	// it would skip, rather than fail, if it said true for every router.
	if s.store.Sharded() {
		s.T().Skip("a router with shards refuses to be a move's source")
	}
}

// export reads the whole snapshot, one JSON object per line.
func (s *DataMoveSuite) export() []dataLine {
	s.skipOnARouterWithShards()
	status, body := s.serverClient.call(http.MethodGet, "/v1/data/export", nil)
	s.Require().Equal(http.StatusOK, status)

	var lines []dataLine
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	for decoder.More() {
		var line dataLine
		s.Require().NoError(decoder.Decode(&line))
		lines = append(lines, line)
	}
	s.Require().NotEmpty(lines)
	return lines
}

// changesSince waits for a change to be readable, asking from the beginning of what is
// recorded. The feed deliberately stops below the oldest transaction still open on the
// deployment, so a change written a moment ago is readable once what was in flight beside
// it has committed.
func (s *DataMoveSuite) changesSince(after int64) changedRows {
	s.skipOnARouterWithShards()
	var page changedRows
	s.Require().EventuallyWithT(func(c *assert.CollectT) {
		page = changedRows{}
		answered := s.serverClient.do(http.MethodGet,
			"/v1/data/changes?after="+strconv.FormatInt(after, 10), nil, &page)
		assert.Equal(c, http.StatusOK, answered, "the change feed's answer")
		assert.NotEmpty(c, page.Changes, "no change became readable")
	}, settleFor, 50*time.Millisecond)
	return page
}

// changedRows is what the change feed answers with.
type changedRows struct {
	Changes  []store.DataChange `json:"changes"`
	Cursor   int64              `json:"cursor"`
	CaughtUp bool               `json:"caught_up"`
}

// rendered is the changes as the JSON they carry, for asking whether a row is among them.
func (s *DataMoveSuite) rendered(changes []store.DataChange) string {
	encoded, err := json.Marshal(changes)
	s.Require().NoError(err)
	return string(encoded)
}

// instruct changes an agent config's instructions as the app.
func (s *DataMoveSuite) instruct(id, instructions string) {
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPatch, "/v1/agents/configs/"+id,
		map[string]any{"instructions": instructions}, nil))
}

// instructionsOf reads an agent config's instructions as the app.
func (s *DataMoveSuite) instructionsOf(id string) string {
	var stored AgentConfig
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/configs/"+id, nil, &stored))
	return value(stored.Instructions)
}

// withID is an exported row under another id.
func (s *DataMoveSuite) withID(row json.RawMessage, id string) json.RawMessage {
	fields := map[string]any{}
	s.Require().NoError(json.Unmarshal(row, &fields))
	fields["id"] = id
	encoded, err := json.Marshal(fields)
	s.Require().NoError(err)
	return encoded
}

// ndjson is an export as the body an import takes.
func ndjson(lines []dataLine) string {
	var written strings.Builder
	for _, line := range lines {
		encoded, err := json.Marshal(line)
		if err != nil {
			continue
		}
		written.Write(encoded)
		written.WriteByte('\n')
	}
	return written.String()
}
