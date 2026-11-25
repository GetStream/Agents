//go:build integration

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"
)

// namingRow is a row of a moved table and the row of another table it names.
type namingRow struct {
	table, id  string
	row        map[string]any
	column     string
	named, own string
}

// namedTables are the tables naming rows name a row of, by the column that names it.
var namedTables = map[string]string{"session_id": "agent_sessions", "contact_id": "campaign_contacts", "simulation_id": "simulations"}

// layouts are the destination as one database and as a shared database with a shard, each
// holding what naming rows may name: acme's own, and resident's under ids of their own.
func (s *DataMoveSuite) layouts(check func(destination *Store, rows []namingRow)) {
	s.Run("one database", func() { check(s.destination, s.namingRows(s.destination)) })
	s.Run("a shard of its own", func() {
		destination := s.shardedDestination()
		check(destination, s.namingRows(destination))
	})
}

func (s *DataMoveSuite) shardedDestination() *Store {
	dsn := os.Getenv(DSNEnvVar)
	shard := s.open(elsewhere(dsn, "moved_shard"))
	var sharded []string
	for _, table := range dataTables {
		if table.shard {
			sharded = append(sharded, table.name)
		}
	}
	_, err := shard.GlobalDB().ExecContext(s.ctx, "TRUNCATE "+strings.Join(sharded, ", ")+" CASCADE")
	s.Require().NoError(err)
	s.Require().NoError(shard.Close())
	destination, err := OpenShards(Options{DSN: elsewhere(dsn, "moved"), ShardDSN: elsewhere(dsn, "moved_shard")})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(destination.Close()) })
	s.Require().NoError(destination.Migrate(s.ctx))
	return destination
}

// holding is a session, a contact and a simulation customerID holds at a deployment, by id.
func (s *DataMoveSuite) holding(store *Store, customerID string) (session, campaign, contact, simulation string) {
	session, campaign, contact, simulation = newID(), newID(), newID(), newID()
	s.execute(store.shard, "INSERT INTO agent_sessions (id, customer_id, state) VALUES (?, ?, '')", session, customerID)
	s.execute(store.global, "INSERT INTO campaigns (id, customer_id, name) VALUES (?, ?, 'renewals')", campaign, customerID)
	s.execute(store.global, "INSERT INTO campaign_contacts (id, campaign_id, to_number) VALUES (?, ?, '+15550002222')", contact, campaign)
	s.execute(store.global, "INSERT INTO simulations (id, customer_id, name) VALUES (?, ?, 'lost card')", simulation, customerID)
	return session, campaign, contact, simulation
}

func (s *DataMoveSuite) execute(db bun.IDB, query string, args ...any) {
	_, err := db.ExecContext(s.ctx, query, args...)
	s.Require().NoError(err, query)
}

// namingRows is one row of acme's for each reference of each table that names its row by an id
// alone, naming resident's row.
func (s *DataMoveSuite) namingRows(store *Store) []namingRow {
	session, campaign, contact, simulation := s.holding(store, "acme")
	theirSession, _, theirContact, theirSimulation := s.holding(store, "resident")
	at := "2026-01-01T00:00:00Z"
	rows := []namingRow{
		{table: "agent_responses", column: "session_id", named: theirSession, own: session,
			row: map[string]any{"said": "hello", "status": "completed", "error": "", "created_at": at}},
		{table: "agent_branch_switches", column: "session_id", named: theirSession, own: session,
			row: map[string]any{"kind": "edit", "state": "done", "fork_message_id": "m", "target_response_id": "r", "resent": 0,
				"inclusive": true, "hidden_from": at, "created_at": at, "updated_at": at}},
		{table: "agent_branch_messages", column: "session_id", named: theirSession, own: session,
			row: map[string]any{"switch_id": "s", "response_id": "", "author_id": "sam", "message": map[string]any{}, "chat_created_at": at}},
		{table: "calls", column: "session_id", named: theirSession, own: session,
			row: map[string]any{"agent_id": "agent", "call_id": "c", "call_type": "default", "direction": "inbound", "instructions": "",
				"llm": "", "stt": "", "tts": "", "sts": "", "voice": "", "subagent": "", "tags": map[string]any{}, "started_at": at}},
		{table: "simulation_runs", column: "simulation_id", named: theirSimulation, own: simulation,
			row: map[string]any{"state": "passed", "mode": "text", "config_id": "", "scenario": "lost card", "assertion": "answered",
				"judge_target": "", "cases": 0, "passed": 0, "failed": 0, "stop_requested": false, "started_at": at}},
		{table: "campaign_attempts", column: "contact_id", named: theirContact, own: contact,
			row: map[string]any{"campaign_id": campaign, "step_id": "step_1", "attempt": 1, "dial": 1, "call_id": "", "vendor_call_id": "",
				"result": "", "outcome": "", "converted": false, "classify_state": "", "classify_tries": 0, "started_at": at,
				"ended_at": at, "call_outcome": "completed"}},
	}
	for i := range rows {
		rows[i].id = newID()
		rows[i].row[rows[i].key()] = rows[i].id
		rows[i].row[rows[i].column] = rows[i].named
		if rows[i].table != "campaign_attempts" {
			rows[i].row["customer_id"] = "acme"
		}
	}
	return rows
}

func (n namingRow) key() string {
	if n.table == "agent_branch_messages" {
		return "original_message_id"
	}
	return "id"
}

// naming is the row as sent, naming id.
func (n namingRow) naming(id any) json.RawMessage {
	n.row[n.column] = id
	encoded, err := json.Marshal(n.row)
	if err != nil {
		panic(err)
	}
	return encoded
}

// nameOf is what the row stored names, empty when none is stored.
func (s *DataMoveSuite) nameOf(destination *Store, n namingRow) string {
	spec, _ := dataTableByName(n.table)
	var names []string
	s.Require().NoError(destination.sideOf(spec).NewSelect().Table(n.table).ColumnExpr("COALESCE(?, '')", bun.Ident(n.column)).
		Where("? = ?", bun.Ident(n.key()), n.id).Scan(s.ctx, &names))
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// assertNamesNoRow checks a row is refused as naming one its customer does not hold, by its table
// and its key.
func (s *DataMoveSuite) assertNamesNoRow(err error, n namingRow) {
	s.Require().ErrorIs(err, ErrInvalidImport, n.table)
	s.Require().ErrorIs(err, ErrNamesNoRow, n.table)
	s.ErrorContains(err, n.table+`:{`, n.table)
	s.ErrorContains(err, `"`+n.key()+`":"`+n.id+`"`, n.table)
	s.ErrorContains(err, "a row names a row that neither the export nor this app holds", n.table)
	if n.table != "agent_branch_messages" { // whose key names its session
		s.NotContains(err.Error(), n.named, "the row it names is not named back")
	}
}

func (s *DataMoveSuite) TestAnImportedRowNamesOnlyTheImportersRows() {
	s.layouts(func(destination *Store, rows []namingRow) {
		for _, n := range rows {
			s.assertNamesNoRow(destination.ImportRow(s.ctx, "acme", n.table, n.naming(n.named)), n)
			s.Empty(s.nameOf(destination, n), n.table)
			s.Require().NoError(destination.ImportRow(s.ctx, "acme", n.table, n.naming(n.own)), n.table)
			s.Equal(n.own, s.nameOf(destination, n), n.table)
		}
	})
}

func (s *DataMoveSuite) TestAnImportDoesNotMoveARowOntoAnotherCustomersRow() {
	s.layouts(func(destination *Store, rows []namingRow) {
		for _, n := range rows {
			s.Require().NoError(destination.ImportRow(s.ctx, "acme", n.table, n.naming(n.own)), n.table)
			s.assertNamesNoRow(destination.ImportRow(s.ctx, "acme", n.table, n.naming(n.named)), n)
			s.Equal(n.own, s.nameOf(destination, n), n.table)
		}
	})
}

func (s *DataMoveSuite) TestAReplayedRowNamesOnlyTheReplayersRows() {
	s.layouts(func(destination *Store, rows []namingRow) {
		for _, n := range rows {
			written := DataChange{Table: "router_configs", Op: ChangeInsert, Payload: json.RawMessage(fmt.Sprintf(
				`{"id": %q, "name": %q, "llm": {}, "stt": {}, "tts": {}, "sts": {}, "search": {}, "created_at": "2026-01-01T00:00:00Z",
				"updated_at": "2026-01-01T00:00:00Z"}`, newID(), newID()))}
			s.assertNamesNoRow(destination.ApplyChanges(s.ctx, "acme", []DataChange{written,
				{Table: n.table, Op: ChangeInsert, Payload: n.naming(n.named)}}), n)
			s.Empty(s.nameOf(destination, n), n.table)

			s.Require().NoError(destination.ApplyChanges(s.ctx, "acme", []DataChange{
				{Table: n.table, Op: ChangeInsert, Payload: n.naming(n.own)},
				{Table: n.table, Op: ChangeUpdate, Payload: n.naming(n.own)}}), n.table)
			s.assertNamesNoRow(destination.ApplyChanges(s.ctx, "acme", []DataChange{
				{Table: n.table, Op: ChangeUpdate, Payload: n.naming(n.named)}}), n)
			s.Equal(n.own, s.nameOf(destination, n), n.table)
		}
	})
}

// A copy that brings a row before the one it names is refused until that one is here, as before,
// and the same row lands then.
func (s *DataMoveSuite) TestARowLandsOnceTheRowItNamesIsHere() {
	s.layouts(func(destination *Store, rows []namingRow) {
		for _, n := range rows {
			s.assertNamesNoRow(destination.ImportRow(s.ctx, "acme", n.table, n.naming("not-here-yet")), namingRow{
				table: n.table, id: n.id, column: n.column, named: "not-here-yet"})
			s.Require().NoError(destination.ImportRow(s.ctx, "acme", n.table, n.naming(n.own)), n.table)
		}
	})
}

// The answer is the same whether the row named is another customer's or nobody's, and comes as
// soon while a transaction holds that other customer's row.
func (s *DataMoveSuite) TestARowIsRefusedTheSameWhoeverHoldsTheRowItNames() {
	s.layouts(func(destination *Store, rows []namingRow) {
		for _, n := range rows {
			spec, _ := dataTableByName(n.table)
			missing := destination.ImportRow(s.ctx, "acme", n.table, n.naming("nobody-s"))
			s.Require().Error(missing, n.table)
			tx, err := destination.sideOf(spec).BeginTx(s.ctx, nil)
			s.Require().NoError(err)
			s.execute(tx, "SELECT 1 FROM "+namedTables[n.column]+" WHERE id = ? FOR UPDATE", n.named)

			within, cancel := context.WithTimeout(s.ctx, 2*time.Second)
			theirs := destination.ImportRow(within, "acme", n.table, n.naming(n.named))
			cancel()

			s.Require().NoError(tx.Rollback())
			s.EqualError(theirs, strings.ReplaceAll(missing.Error(), "nobody-s", n.named), n.table)
			s.Require().ErrorIs(theirs, ErrInvalidImport, n.table)
			s.Require().ErrorIs(missing, ErrInvalidImport, n.table)
		}
	})
}

func (s *DataMoveSuite) TestACallOfNoSessionIsImported() {
	s.layouts(func(destination *Store, rows []namingRow) {
		for _, n := range rows {
			if n.table == "calls" {
				s.Require().NoError(destination.ImportRow(s.ctx, "acme", n.table, n.naming(nil)))
				found, err := destination.Calls.Get(s.ctx, "acme", n.id)
				s.Require().NoError(err)
				s.Empty(found.SessionID)
			}
		}
	})
}

// A row of a campaign another customer holds here is left out, as before, whichever contact it
// names.
func (s *DataMoveSuite) TestAnAttemptOfAnotherCustomersCampaignIsLeftOut() {
	s.layouts(func(destination *Store, rows []namingRow) {
		_, theirCampaign, _, _ := s.holding(destination, "resident")
		for _, n := range rows {
			if n.table == "campaign_attempts" {
				n.row["campaign_id"] = theirCampaign
				s.Require().NoError(destination.ImportRow(s.ctx, "acme", n.table, n.naming(n.named)))
				s.Empty(s.nameOf(destination, n))
			}
		}
	})
}

func (s *DataMoveSuite) TestARowAnotherCustomerHoldsTheKeyOfIsRefusedAsThat() {
	s.layouts(func(destination *Store, rows []namingRow) {
		for _, n := range rows {
			s.Require().NoError(destination.ImportRow(s.ctx, "acme", n.table, n.naming(n.own)), n.table)
			if n.table == "campaign_attempts" {
				continue // its owner is its campaign's
			}
			s.Require().ErrorIs(destination.ImportRow(s.ctx, "other", n.table, n.naming(n.own)), ErrAnotherCustomersRow, n.table)
		}
	})
}

// A move to another deployment and back, and on from the copy, and each again, lands every row
// that names another.
func (s *DataMoveSuite) TestRowsThatNameOthersMoveThereAndBackAndOn() {
	session, campaign, contact, simulation := s.holding(s.source, "acme")
	naming := s.namingRows(s.source)
	s.execute(s.source.global, "UPDATE simulations SET config_id = 'config-1', scenario = 'lost card', assertion = 'answered', variations = 1")
	s.execute(s.source.global, "UPDATE campaigns SET config_id = 'config-1', from_number = '+15550001111', concurrency = 1")
	for _, n := range naming {
		own := map[string]string{"session_id": session, "simulation_id": simulation, "contact_id": contact}[n.column]
		if n.table == "campaign_attempts" {
			n.row["campaign_id"] = campaign
		}
		s.Require().NoError(s.source.ImportRow(s.ctx, "acme", n.table, n.naming(own)), n.table)
	}
	tables := []string{"agent_sessions", "agent_responses", "agent_branch_switches", "agent_branch_messages", "calls",
		"simulations", "simulation_runs", "campaigns", "campaign_contacts", "campaign_attempts"}
	carry := func(from *Store, as string, to *Store, under string) {
		for _, row := range s.exportFrom(from, as).rows {
			if !slices.Contains(tables, row.table) {
				continue
			}
			encoded, err := json.Marshal(row.row)
			s.Require().NoError(err)
			s.Require().NoError(to.ImportRow(s.ctx, under, row.table, encoded), row.table)
		}
	}
	counts := func(store *Store, customer string) map[string]int {
		counted := map[string]int{}
		for _, table := range tables {
			counted[table] = len(s.exportFrom(store, customer).of(table))
		}
		return counted
	}
	want := counts(s.source, "acme")
	s.Equal(1, want["campaign_attempts"])
	s.Equal(1, want["agent_responses"])

	for range 2 {
		carry(s.source, "acme", s.destination, "moved")
		s.Equal(want, counts(s.destination, "moved"))
		carry(s.destination, "moved", s.source, "acme")
		s.Equal(want, counts(s.source, "acme"))
	}
	sharded := s.shardedDestination()
	s.execute(s.destination.global, "TRUNCATE campaigns, simulations, agent_sessions CASCADE")
	s.execute(s.destination.global, "TRUNCATE calls")
	carry(s.source, "acme", sharded, "again")
	carry(s.source, "acme", sharded, "again")
	var held int
	s.Require().NoError(sharded.shard.NewRaw("SELECT count(*) FROM agent_responses WHERE customer_id = 'again'").Scan(s.ctx, &held))
	s.Equal(1, held)
}

// racing imports n naming its own row while takeover, which deletes that row and gives its id to
// resident, is committing, and answers what the import did.
func (s *DataMoveSuite) racing(destination *Store, n namingRow, takeover ...string) error {
	spec, _ := dataTableByName(n.table)
	db := destination.sideOf(spec)
	tx, err := db.BeginTx(s.ctx, nil)
	s.Require().NoError(err)
	defer func() { _ = tx.Rollback() }()
	for _, statement := range takeover {
		s.execute(tx, statement)
	}
	imported := make(chan error, 1)
	go func() { imported <- destination.ImportRow(s.ctx, "acme", n.table, n.naming(n.own)) }()
	s.waitingOnALock(db, "INSERT INTO "+n.table)
	s.Require().NoError(tx.Commit())
	select {
	case err := <-imported:
		return err
	case <-time.After(10 * time.Second):
		return errors.New("the import is still waiting")
	}
}

func (s *DataMoveSuite) TestARowWhoseNamedRowIsTakenOverWhileItIsImportedIsRefused() {
	s.layouts(func(destination *Store, rows []namingRow) {
		for _, n := range rows {
			spec, _ := dataTableByName(n.table)
			target := namedTables[n.column]
			columns, values := "id, customer_id, name", "'resident', 'theirs'"
			switch target {
			case "agent_sessions":
				columns, values = "id, customer_id, state", "'resident', ''"
			case "campaign_contacts":
				var campaign string
				s.Require().NoError(destination.global.NewRaw("SELECT id FROM campaigns WHERE customer_id = 'resident' LIMIT 1").Scan(s.ctx, &campaign))
				columns, values = "id, campaign_id, to_number", "'"+campaign+"', '+15550003333'"
			}
			err := s.racing(destination, n, "DELETE FROM "+target+" WHERE id = '"+n.own+"'",
				fmt.Sprintf("INSERT INTO %s (%s) VALUES ('%s', %s)", target, columns, n.own, values))
			s.assertNamesNoRow(err, namingRow{table: n.table, id: n.id, column: n.column, named: n.own})
			s.Empty(s.nameOf(destination, n), n.table)
			s.execute(destination.sideOf(spec), "DELETE FROM "+target+" WHERE id = '"+n.own+"'")
			if target == "agent_sessions" {
				s.execute(destination.shard, "INSERT INTO agent_sessions (id, customer_id, state) VALUES (?, 'acme', '')", n.own)
			}
		}
	})
}

func (s *DataMoveSuite) TestAChildWhoseParentIsTakenOverWhileItIsImportedIsLeftOut() {
	_, campaign, _, _ := s.holding(s.destination, "acme")
	contact := namingRow{table: "campaign_contacts", id: newID(), column: "campaign_id", own: campaign,
		row: map[string]any{"to_number": "+15550004444", "stage": Enrolled, "attempt": 0, "converted": false, "outcome": "",
			"instructions": "", "step_id": "", "time_zone": "", "leased_by": "", "variables": map[string]any{}, "seq": 7,
			"created_at": "2026-01-01T00:00:00Z"}}
	contact.row["id"] = contact.id

	s.Require().NoError(s.racing(s.destination, contact, "DELETE FROM campaigns WHERE id = '"+campaign+"'",
		"INSERT INTO campaigns (id, customer_id, name) VALUES ('"+campaign+"', 'resident', 'theirs')"))

	s.Empty(s.nameOf(s.destination, contact))
}

// A row naming an id nobody holds is refused at once, and stays refused when another customer's
// row takes that id in the meantime.
func (s *DataMoveSuite) TestARowNamingAnIdBeingTakenByAnotherCustomerIsRefused() {
	s.layouts(func(destination *Store, rows []namingRow) {
		n := rows[0]
		tx, err := destination.shard.BeginTx(s.ctx, nil)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback() }()
		taken := newID()
		s.execute(tx, "INSERT INTO agent_sessions (id, customer_id, state) VALUES (?, 'resident', '')", taken)

		err = destination.ImportRow(s.ctx, "acme", n.table, n.naming(taken))

		s.assertNamesNoRow(err, namingRow{table: n.table, id: n.id, column: n.column, named: taken})
		s.Require().NoError(tx.Commit())
		s.ErrorIs(destination.ImportRow(s.ctx, "acme", n.table, n.naming(taken)), ErrInvalidImport)
	})
}

// A row being written elsewhere that names the same row, or an edit of that row, holds no import up.
func (s *DataMoveSuite) TestAnImportDoesNotWaitForAnEditOfTheRowItNames() {
	s.layouts(func(destination *Store, rows []namingRow) {
		n := rows[0]
		for _, lock := range []string{"KEY SHARE", "NO KEY UPDATE"} {
			tx, err := destination.shard.BeginTx(s.ctx, nil)
			s.Require().NoError(err)
			s.execute(tx, "SELECT 1 FROM agent_sessions WHERE id = ? FOR "+lock, n.own)

			within, cancel := context.WithTimeout(s.ctx, 2*time.Second)
			err = destination.ImportRow(within, "acme", n.table, n.naming(n.own))
			cancel()

			s.Require().NoError(tx.Rollback())
			s.Require().NoError(err, lock)
		}
	})
}

// A delete of a session waits for an import of its turns and takes them with it, so neither
// deadlocks on the other.
func (s *DataMoveSuite) TestASessionsDeleteAndAnImportOfItsTurnsDoNotDeadlock() {
	s.layouts(func(destination *Store, rows []namingRow) {
		n := rows[0]
		s.Require().NoError(destination.ImportRow(s.ctx, "acme", n.table, n.naming(n.own)))
		tx, err := destination.shard.BeginTx(s.ctx, nil)
		s.Require().NoError(err)
		defer func() { _ = tx.Rollback() }()
		var stale []string
		s.Require().NoError(destination.importRow(s.ctx, tx, "acme", n.table, n.naming(n.own), &stale))
		deleted := make(chan error, 1)
		go func() { deleted <- destination.Sessions.Delete(s.ctx, "acme", n.own) }()
		s.waitingOnALock(destination.shard, "DELETE FROM")

		s.Require().NoError(destination.importRow(s.ctx, tx, "acme", n.table, n.naming(n.own), &stale))
		s.Require().NoError(tx.Commit())

		s.Require().NoError(<-deleted)
		s.Empty(s.nameOf(destination, n))
	})
}

// bareColumns are the columns of moved tables that may name a row of another and are held by no
// foreign key, so a row arrives whatever they name. Each is read with the customer: a lookup of it
// finds only a row of the importing customer's. A column that is added to a moved table and could
// name a row is added here with its reader's customer check, or given a foreign key that holds
// the customer, or a reference in dataTables.
var bareColumns = map[string][]string{
	"agent_configs": {"agents", "calls", "channels_chat", "channels_lines", "connectors", "floor_control", "keyterms",
		"left_out_connectors", "llm_fallbacks", "resume_history", "sandbox_options", "scheduling", "skills", "tags", "tool_groups",
		"turn_taking", "video", "visible_tools", "voice", "workflow"},
	"skills":                 {"config_id", "files"},
	"connector_definitions":  {"manifest"},
	"connector_connections":  {"account_id", "cached_tools", "connector_id", "granted_scopes", "inputs", "metadata", "owner_id"},
	"router_configs":         {"llm", "search", "sts", "stt", "tts"},
	"sip_trunks":             {"codecs"},
	"phone_numbers":          {"capabilities", "config_id", "stream_app_pk", "tags", "vendor_id"},
	"campaigns":              {"calling_hours", "config_id", "sequence", "tags", "usage_categories", "user_id"},
	"campaign_contacts":      {"call_id", "step_id", "variables", "vendor_call_id"},
	"campaign_attempts":      {"call_id", "step_id", "vendor_call_id"},
	"simulations":            {"config_id", "tags"},
	"simulation_runs":        {"config_id"},
	"simulation_cases":       {"call_id", "transcript"},
	"users":                  {"custom"},
	"scheduled_tasks":        {"channels", "config_id", "conversation_id", "custom", "origin_session_id", "quiet_hours", "session_id", "usage_categories", "user_id"},
	"scheduled_task_runs":    {"session_id", "texted"},
	"recordings":             {"result", "stt", "tags", "tts"},
	"agent_sessions":         {"agent_id", "caller_tools", "config_id", "connector_selections", "conversation_id", "custom", "floor_control", "model_overwrites", "resume_history", "stream_app_pk", "usage_categories", "user_id"},
	"agent_session_tools":    {"tools"},
	"calls":                  {"agent_id", "agent_user_id", "call_id", "campaign_id", "contact_id", "conversation_id", "skills", "stream_app_pk", "tags"},
	"agent_responses":        {"parent_response_id", "question_response_id"},
	"agent_response_items":   {"payload", "session_id"},
	"conversation_documents": {"conversation_id", "owner_user_id", "response_id", "session_id"},
	"agent_branch_switches":  {"fork_message_id", "target_response_id"},
	"agent_branch_messages":  {"author_id", "message", "original_message_id", "response_id", "switch_id"},
	"agent_logs":             {"agent_id", "config_id", "details", "session_id", "source_id", "user_id"},
	"requests": {"agent_id", "attributes", "call_id", "meters", "operation_id", "session_id",
		"source_usage_id", "tags", "turn_id", "usage_categories", "usage_id", "user_id"},
	"turn_stats_hourly":   {"agent_id"},
	"turn_latency_hourly": {"agent_id"},
	"customer_activity":   {"organization_id"},
	"policies":            {"document"},
}

// bareTexts are the text columns of moved tables that hold an id under another name, by how they are read:
// forked_from is only answered back, claimed_by is looked up among the customer's users, a
// channel_thread of another customer's is refused by the delivery, and spend by api_key is read with the app.
var bareTexts = map[string][]string{
	"agent_sessions":  {"api_key", "forked_from"},
	"campaigns":       {"api_key"},
	"requests":        {"api_key"},
	"scheduled_tasks": {"api_key", "channel_thread"},
	"users":           {"claimed_by"},
}

// words are the text columns of each moved table that hold no row's id.
var words = map[string]string{
	"agent_configs":          "greeting greeting_mode guardrail harness image instructions knowledge_namespace llm mode name region review_instructions role routing_policy sandbox_provider search sts stt subagent sync_hash tts ui_instructions video_source",
	"skills":                 "description instructions name",
	"connector_definitions":  "category description name sync_hash",
	"connector_connections":  "auth_scheme client_registration label last_error owner_type status tls_scheme tools_digest validation_code validation_error validation_status",
	"router_configs":         "name sync_hash",
	"voices":                 "description name",
	"sip_trunks":             "host name transport username",
	"phone_numbers":          "country e164 stream_call_type vendor",
	"knowledge_urls":         "declared_title description error namespace state title url",
	"knowledge_documents":    "content_type error namespace object_key source state text",
	"campaigns":              "default_country from_number name paused_reason state type",
	"campaign_contacts":      "instructions leased_by outcome reason stage time_zone to_number",
	"campaign_attempts":      "call_outcome classify_state outcome result",
	"simulations":            "assertion caller_stt caller_target caller_tts caller_voice judge_target mode name scenario",
	"simulation_runs":        "assertion error judge_target mode scenario state",
	"simulation_cases":       "ended error scenario state verdict",
	"users":                  "email kind name timezone",
	"scheduled_tasks":        "caller_kind created_by instructions leased_by rrule state timezone title",
	"scheduled_task_runs":    "delivered_to error prompt state trigger",
	"opt_outs":               "channel recipient source",
	"recordings":             "callback error modality model provider source status text",
	"agent_sessions":         "agent_name caller_kind description end_reason modality project project_tag return_url state team title",
	"calls":                  "call_type direction from_number instructions llm review_notes sts stt subagent summary to_number tts voice",
	"agent_responses":        "error said status",
	"agent_response_items":   "kind text tool_name",
	"conversation_documents": "markdown name sha256",
	"agent_branch_switches":  "kind state",
	"agent_logs":             "event_type message severity source",
	"requests":               "error_code error_message modality model organization_tier price_version provider purpose region ship_error sku",
	"stats_hourly":           "modality model provider",
	"stats_daily":            "modality model provider",
	"stats_tags_hourly":      "modality tag_key tag_value",
	"stats_tags_daily":       "modality tag_key tag_value",
	"turn_latency_hourly":    "leg",
	"customer_activity":      "area client last_refusal_code",
	"policies":               "scope",
}

// forcedColumns are written by the import itself or left to this deployment, whatever was sent
// (identity, vouched).
var forcedColumns = map[string][]string{
	"connector_connections": {"client_id", "provider_unit_id"},
	"phone_numbers":         {"dlc_use_case_id", "stream_call_id", "stream_route_id", "stream_trunk_id"},
	"requests":              {"limit_grants", "organization_id"},
}

func (s *DataMoveSuite) TestEveryReferenceOfAMovedTableIsCheckedOrReadWithTheCustomer() {
	for _, table := range dataTables {
		var checked []string
		if table.parent != "" {
			checked = append(checked, table.parentColumn)
		}
		for _, reference := range table.references {
			checked = append(checked, reference.column)
		}
		var keys []struct {
			Name    string
			Columns []string `bun:",array"`
			Target  string
		}
		s.Require().NoError(s.destination.sideOf(table).NewRaw(`
SELECT c.conname AS name, c.confrelid::regclass::text AS target,
	ARRAY(SELECT a.attname::text FROM unnest(c.conkey) AS k JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k) AS columns
FROM pg_constraint c WHERE c.conrelid = ?::regclass AND c.contype = 'f'`, table.name).Scan(s.ctx, &keys))
		var held []string
		for _, key := range keys {
			held = append(held, key.Columns...)
			if table.customer != "" && slices.Contains(key.Columns, table.customer) {
				continue
			}
			s.Require().Len(key.Columns, 1, key.Name)
			s.Contains(checked, key.Columns[0], "%s of %s holds no customer: it is a reference", key.Name, table.name)
			named := dataReference{key.Columns[0], key.Target}
			s.True(slices.Contains(table.references, named) || table.parent != "" && named == table.belonging(), key.Name)
		}
		var columns []string
		s.Require().NoError(s.destination.sideOf(table).NewRaw(`
SELECT attname::text FROM pg_attribute WHERE attrelid = ?::regclass AND attnum > 0 AND NOT attisdropped AND attgenerated = ''
	AND (attname ~ '(_ids?|_pk)$' OR atttypid::regtype::text ~ '^(text|uuid|jsonb?|.*\[\])$')`, table.name).Scan(s.ctx, &columns))
		for _, column := range columns {
			known := column == "id" || slices.Contains(bareTexts[table.name], column) || slices.Contains(strings.Fields(words[table.name]), column) || column == table.customer || column == "customer_id" || slices.Contains(held, column) ||
				slices.Contains(bareColumns[table.name], column) || slices.Contains(forcedColumns[table.name], column) ||
				slices.Contains(table.vouched, column)
			s.True(known, "%s.%s may name a row of another table: see bareColumns", table.name, column)
		}
	}
}

// A moved row arrives whatever its bare ids name, and what each names is looked up with the
// customer, so one of another customer's is not found by them, and theirs does not find the row.
func (s *DataMoveSuite) TestABareIdOfAnotherCustomersRowIsNotFound() {
	session, campaign, contact, simulation := s.holding(s.destination, "resident")
	config := s.seedAgentConfig(s.destination, "resident", "theirs")
	voice := s.seedVoice(s.destination, "resident", "theirs")
	connection := s.seedConnectorConnection(s.destination, "resident")
	turn := newID()
	s.execute(s.destination.shard, "INSERT INTO agent_responses (id, session_id, customer_id, created_at) VALUES (?, ?, 'resident', '2026-01-01')", turn, session)
	db := s.source.GlobalDB()
	s.execute(db, "INSERT INTO agent_configs (id, customer_id, name, voice, connectors) VALUES ('own', 'acme', 'own', ?, jsonb_build_array("+
		"jsonb_build_object('name', 'a', 'connector_id', 'custom_acme', 'connection', jsonb_build_object('type', 'fixed', 'connection_id', ?::text))))", voice, connection)
	s.execute(db, "INSERT INTO skills (id, customer_id, config_id, name, description, instructions) VALUES ('sk', 'acme', ?, 'look', '', '')", config)
	s.execute(db, "INSERT INTO agent_sessions (id, customer_id, state, config_id, forked_from) VALUES ('se', 'acme', '', ?, ?)", config, session)
	s.execute(db, "INSERT INTO agent_responses (id, session_id, customer_id, parent_response_id, question_response_id) VALUES ('r', 'se', 'acme', ?, ?)", turn, turn)
	s.execute(db, "INSERT INTO conversation_documents (id, customer_id, owner_user_id, session_id, response_id, name, markdown, sha256)"+
		" VALUES ('d', 'acme', 'sam', ?, ?, 'a', 'b', 'c')", session, turn)
	s.execute(db, "INSERT INTO calls (id, customer_id, agent_id, call_id, campaign_id, contact_id) VALUES ('c', 'acme', 'a', 'c', ?, ?)", campaign, contact)
	s.execute(db, "INSERT INTO agent_logs (customer_id, source, severity, event_type, message, config_id, session_id, occurred_at)"+
		" VALUES ('acme', 'agent', 'info', 'turn', 'm', ?, ?, now())", config, session)
	s.execute(db, "INSERT INTO scheduled_tasks (id, customer_id, config_id, user_id, title, instructions, start_local, timezone, state, session_id, origin_session_id)"+
		" VALUES ('t', 'acme', ?, 'sam', 'a', 'b', '2026-12-01', 'UTC', 'paused', ?, ?)", config, session, session)

	s.move("acme", "acme")

	here := s.destination
	moved, err := here.AgentConfigs.Get(s.ctx, "acme", "own")
	s.Require().NoError(err)
	s.Equal(connection, moved.Connectors[0].Connection.ConnectionID)
	_, err = here.Voices.Get(s.ctx, "acme", moved.Models.Voice)
	s.ErrorIs(err, ErrNoVoice)
	_, err = here.ConnectorConnections.Get(s.ctx, "acme", connection)
	s.ErrorIs(err, ErrNoConnectorConnection)
	uses, err := here.ConnectorConnections.Uses(s.ctx, "resident", []string{connection})
	s.Require().NoError(err)
	s.Empty(uses)
	row, err := here.Sessions.Get(s.ctx, "acme", "se")
	s.Require().NoError(err)
	s.Equal(session, row.ForkedFrom)
	_, err = here.Sessions.Get(s.ctx, "acme", row.ForkedFrom)
	s.Error(err)
	_, err = here.AgentConfigs.Get(s.ctx, "acme", row.ConfigID)
	s.ErrorIs(err, ErrNoAgentConfig)
	answer, err := here.Responses.Get(s.ctx, "acme", "se", "r")
	s.Require().NoError(err)
	s.Equal(turn, answer.ParentResponseID)
	_, err = here.Responses.Get(s.ctx, "acme", "se", answer.ParentResponseID)
	s.ErrorIs(err, ErrUnknownResponse)
	siblings, err := here.Responses.Siblings(s.ctx, "acme", "se", "r", SiblingsOfReply)
	s.Require().NoError(err)
	s.Len(siblings, 1)
	_, err = here.Campaigns.Get(s.ctx, "acme", campaign)
	s.Error(err)
	_, err = here.Simulations.Get(s.ctx, "acme", simulation)
	s.Error(err)

	skills, err := here.Skills.Select(s.ctx, "resident", SkillFilter{ConfigID: config})
	s.Require().NoError(err)
	s.Empty(skills)
	calls, err := here.Calls.Select(s.ctx, "resident", CallFilter{CampaignID: campaign})
	s.Require().NoError(err)
	s.Empty(calls)
	logs, err := here.AgentLogs.Select(s.ctx, "resident", AgentLogFilter{SessionID: session, ConfigID: config})
	s.Require().NoError(err)
	s.Empty(logs)
	documents, err := here.ConversationDocuments.Select(s.ctx, "resident", DocumentScope{SessionID: session, Owner: "sam"})
	s.Require().NoError(err)
	s.Empty(documents)
	tasks, err := here.ScheduledTasks.Select(s.ctx, "resident", ScheduledTaskFilter{SessionID: session})
	s.Require().NoError(err)
	s.Empty(tasks)

	s.Require().NoError(here.ScheduledTasks.DeleteForSession(s.ctx, "resident", session))
	s.Require().NoError(here.Sessions.Delete(s.ctx, "resident", session))
	left := s.exportFrom(here, "acme")
	s.Len(left.of("scheduled_tasks"), 1)
	s.Len(left.of("conversation_documents"), 1)
	s.Len(left.of("agent_logs"), 1)
}

// A foreign key that holds the customer refuses a row the same way, named by its key.
func (s *DataMoveSuite) TestARunOfATaskNotHereIsRefusedByItsKey() {
	err := s.destination.ImportRow(s.ctx, "acme", "scheduled_task_runs", json.RawMessage(`{"id": "run", "task_id": "not-here", "state": "delivered",
		"trigger": "schedule", "attempts": 0, "prompt": "", "delivered_to": "", "session_id": "", "texted": [],
		"scheduled_for": "2026-01-01T00:00:00Z", "created_at": "2026-01-01T00:00:00Z"}`))

	s.ErrorIs(err, ErrInvalidImport)
	s.ErrorIs(err, ErrNamesNoRow)
	s.EqualError(err, `store: import scheduled_task_runs:{"id":"run"}: a row names a row that neither the export nor this app holds`)
}

func (s *DataMoveSuite) TestAUniqueValueIsNotRefusedAsNamingNoRow() {
	s.execute(s.destination.global, "INSERT INTO campaigns (id, customer_id, name) VALUES ('cp', 'acme', 'c'); INSERT INTO campaign_contacts (id, campaign_id, to_number) VALUES ('held', 'cp', '+15550002222')")
	var row []byte
	s.Require().NoError(s.destination.global.NewRaw("SELECT to_jsonb(d) || '{\"id\": \"other\"}' FROM campaign_contacts d").Scan(s.ctx, &row))

	err := s.destination.ImportRow(s.ctx, "acme", "campaign_contacts", row)

	s.Require().ErrorIs(err, ErrInvalidImport)
	s.NotErrorIs(err, ErrNamesNoRow)
	s.EqualError(err, "store: import campaign_contacts: a row holds a value, such as a name, that another row here already holds")
}

func (s *DataMoveSuite) TestADeleteOfATrunkWithANumberIsRefused() {
	trunk := s.trunkOf(s.destination, "acme").ID
	s.Require().NoError(s.destination.ImportRow(s.ctx, "acme", "phone_numbers", s.importedNumber("+15550000124", sipTrunkVendor, "sip_trunk_id", trunk)))

	err := s.destination.ApplyChanges(s.ctx, "acme", []DataChange{{Table: "sip_trunks", Op: ChangeDelete, Key: []byte(`{"id": "` + trunk + `"}`)}})

	s.Require().ErrorIs(err, ErrInvalidImport)
	s.NotErrorIs(err, ErrNamesNoRow)
	s.EqualError(err, "store: delete sip_trunks: another row here still names this row")
	s.True(isViolation(err, foreignKeyViolation))
}

func (s *DataMoveSuite) TestADeleteWhoseKeyDoesNotReadIsNotRefusedAsNamed() {
	err := s.destination.ApplyChanges(s.ctx, "acme", []DataChange{{Table: "phone_numbers", Op: ChangeDelete, Key: []byte(`{"id": "not a number"}`)}})

	s.Require().ErrorIs(err, ErrInvalidImport)
	s.EqualError(err, "store: import phone_numbers: a row holds a value its column cannot take")
}

func (s *DataMoveSuite) TestAnImportOfARowThatNamesAnotherIsTwoQueries() {
	rows := s.namingRows(s.destination)
	sent := &queryCount{}
	counted, err := Open(elsewhere(os.Getenv(DSNEnvVar), "moved"))
	s.Require().NoError(err)
	defer counted.Close()
	for _, n := range rows {
		s.Require().NoError(counted.ImportRow(s.ctx, "acme", n.table, n.naming(n.own)), n.table)
	}
	counted.global.AddQueryHook(sent)
	for _, n := range rows {
		before := sent.sent.Load()
		s.Require().NoError(counted.ImportRow(context.WithoutCancel(s.ctx), "acme", n.table, n.naming(n.own)), n.table)
		s.EqualValues(2, sent.sent.Load()-before, "%s is validated and written", n.table)
	}
}
