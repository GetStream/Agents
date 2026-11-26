package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"github.com/GetStream/Vision-Agents/acceleration/internal/connectors/core"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// ErrChangesExpired says the cursor a caller is resuming from is older than anything
// still recorded, so there is no way to tell what happened in between. The answer is to
// export again rather than to carry on from a number nothing can honour.
var ErrChangesExpired = errors.New("store: the changes since that cursor are no longer kept")

// ErrNotACustomConnector is an imported connector definition whose manifest the API would
// have refused for a custom connector (importableDefinition).
var ErrNotACustomConnector = errors.New("not a definition the API would store for a custom connector")

// ErrInvalidImport is an export that cannot be written as it is: a table no export carries, a
// row that does not read, a row the database refuses for what it holds, a row whose key
// another customer holds here (ErrAnotherCustomersRow), or one that names a row this customer does
// not hold here (ErrNamesNoRow). Its message names the table, and for those last two the row's own
// key, but nothing of the database. A database that failed is never one.
var ErrInvalidImport = errors.New("store: the export cannot be imported")

// invalidImport marks err as ErrInvalidImport without changing its message.
func invalidImport(err error) error { return marked{err, ErrInvalidImport} }

// NumbersLeftOutError is the numbers on the customer's own trunk an import did not write, because
// another customer holds them here (importableNumber). It is not the import's failure: the rest
// of the row's import, or of the batch of changes, is written, and the caller counts the numbers
// and goes on.
type NumbersLeftOutError struct {
	Numbers []string
}

func (e *NumbersLeftOutError) Error() string {
	return fmt.Sprintf("store: %d numbers on the app's own trunk were left out: another app holds them here", len(e.Numbers))
}

func (e *NumbersLeftOutError) Unwrap() error { return ErrNumberHeldElsewhere }

// marked is err that is also kind, with err's own message.
type marked struct{ err, kind error }

func (e marked) Error() string   { return e.err.Error() }
func (e marked) Unwrap() []error { return []error{e.err, e.kind} }

// refusedRow is a row of table that Postgres refused for what it holds, as an ErrInvalidImport
// that says why in words of its own: Postgres's name a constraint and its columns. It is nil
// for any other error, such as a database that did not answer.
func refusedRow(table string, err error) error {
	var refused pgdriver.Error
	if !errors.As(err, &refused) {
		return nil
	}
	var reason string
	switch code := refused.Field('C'); {
	case code == uniqueViolation:
		reason = "a row holds a value, such as a name, that another row here already holds"
	case code == foreignKeyViolation:
		reason = ErrNamesNoRow.Error()
	case strings.HasPrefix(code, "23"):
		reason = "a row breaks a rule of its table"
	case strings.HasPrefix(code, "22"):
		reason = "a row holds a value its column cannot take"
	default:
		return nil
	}
	// The Postgres error stays in the chain for errors.As, but not in the text: it names the
	// constraint and the values that broke it.
	return stack.Wrap(invalidImport(marked{fmt.Errorf("store: import %s: %s", table, reason), err}))
}

// dataTable is one table a customer's rows live in.
//
// A table either holds the customer id itself or belongs to one through a parent, and
// the two need different queries in every direction: what to select, what to force on the
// way in, and how the trigger works out whose row it is. The order matters, because it is
// the order rows are exported and inserted in, and a child cannot be written before the
// row it points at.
type dataTable struct {
	name string
	// customer is the column holding the customer id, empty when parent owns the row. A table
	// with a parent may hold one as well, empty on its rows from before it was kept: the row is
	// then the customer's whose parent it is.
	customer string
	// parent and parentColumn name the row this one belongs to.
	parent       string
	parentColumn string
	// references are the other rows this one names through a foreign key on an id alone, which
	// any customer's row satisfies. A foreign key that holds the customer too needs none.
	references []dataReference
	// filter is an extra predicate on an export, for a table that holds more than one
	// kind of row.
	filter string
	// order is the order an export reads the table's rows in, for a table whose rows must
	// arrive in one.
	order string
	// shard is a table kept in the shards rather than the shared database.
	shard bool
	// moving is a shard table that was kept in the shared database, where its older rows are
	// until they are copied over: an export reads them there too.
	moving bool
	// across is the column a row is known by in every deployment, for a table whose primary
	// key each deployment draws from a sequence of its own, so the same id is two rows in
	// two deployments. Such a row is imported without its id, and is written once: importing
	// it again, or a row the destination already holds under it, changes nothing.
	across string
	// copied is the column a row imported here keeps the source's id in, for a table with no
	// column a row is known by in every deployment and whose ids, drawn from a sequence of each
	// deployment's, name other rows here. Such a row is imported without its id and is written
	// once: importing it again, or replaying its change, finds it by the source's id and changes
	// nothing.
	copied string
	// vouched are the columns this deployment sets on its own word, such as users.minted: the
	// router handed that guest out. An import leaves them to the local default, since a row
	// the customer sends is the customer's word, not this deployment's. What it says of
	// users.minted is kept as that word, in users.minted_elsewhere.
	vouched []string
}

// dataReference is a column of a moved table that names a row of another one by its id.
type dataReference struct{ column, table string }

// ofSession is the reference of a row that names its session.
var ofSession = []dataReference{{"session_id", "agent_sessions"}}

// dataTables is everything that belongs to a customer, parents first.
//
// Deliberately absent: organizations, apps and api_keys, which are the credentials rather
// than the data, and which a deployment mints for itself; and the goose version tables,
// which are this schema's own bookkeeping. Organization-scope policies are absent too, since one
// organization's decisions cover apps the caller may not have. So are
// connector_authorization_attempts: one lives minutes, is sealed under this deployment's
// key and finishes at this deployment's callback, so a copy could only expire. And
// connector_oauth_clients: a client's secret is sealed under this deployment's key, and a
// client without it cannot authenticate, so the app puts it again where it moved to. So is
// custom_models, whose endpoint keys are sealed the same way, and provider_keys, the keys the
// app keeps for its own accounts at the vendors, which it puts again where it moved to. And
// connector_config_tokens, sealed under this deployment's key for the provider app whose
// events URL is this deployment's. And connector_event_destinations, whose signing secrets are
// sealed under this deployment's key, with connector_event_deliveries, forwards in flight. And
// connection_event_subscriptions: their secrets are sealed under this deployment's key and
// their callbacks are this deployment's URL. And deliveries_seen, the deliveries this
// deployment acted on in the last day, which is where their retries arrive. And
// channel_threads, channel_turns, contact_map and episodes: the thread channels,
// omni-channels and cards they point at live in the Stream app, which a move does not carry
// either; and user_phones, whose links join only those contact_map rows; and
// channel_thread_waiting and channel_thread_inbound, messages in flight for minutes. And
// connector_invocations and connector_audit: this deployment's record of what it did with
// credentials sealed under its own key. And connector_tool_pins: the deployment a connection
// moves to pins its tools again on first use. And chat_inbound, Stream
// Chat messages in flight for minutes, and image_jobs, pictures in flight for minutes. And
// call_bridges, an answer parked for seconds at this deployment's URL: its key is the hash of
// the token that authenticates the fetch, and its trunk password is sealed under this
// deployment's key. And voice_samples and voice_bindings: a sample's audio is in this
// deployment's bucket and a binding's clone in this deployment's account at the provider, so
// like a knowledge file's, neither means anything where the customer moves. A voice arrives
// with its name and description, to be given a recording and prepared again. And
// phone_number_orders: a purchase or release in flight at this deployment's vendor accounts,
// which only this deployment's sweep can settle. And the shard_ copies of the rollups, with
// shard_stats_shipped: each shard ships them from its own rows (Store.ShipStats), which move.
var dataTables = []dataTable{
	{name: "agent_configs", customer: "customer_id"},
	{name: "skills", customer: "customer_id"},
	// Only the customer's own definitions: the built-ins are under no customer, and every
	// deployment seeds its own. They come before the connections that pin them.
	{name: "connector_definitions", customer: "customer_id"},
	// The next consent where it moved to records its client again, and the next validate its
	// result.
	{name: "connector_connections", customer: "customer_id", vouched: []string{
		"client_registration", "client_id", "client_updated_at",
		"validation_revision", "validation_status", "validation_code", "validation_error", "validation_checked_at",
	}},
	{name: "router_configs", customer: "customer_id"},
	{name: "voices", customer: "customer_id"},
	{name: "sip_trunks", customer: "customer_id"},
	{name: "phone_numbers", customer: "customer_id"},
	{name: "knowledge_urls", customer: "customer_id"},
	// A file still waiting to be read is in this deployment's bucket, which a move does not
	// carry, so an imported document is taken as read: its text is what it had.
	{name: "knowledge_documents", customer: "customer_id", vouched: []string{"state", "error", "object_key"}},
	{name: "campaigns", customer: "customer_id"},
	{name: "campaign_contacts", parent: "campaigns", parentColumn: "campaign_id"},
	{name: "campaign_attempts", parent: "campaigns", parentColumn: "campaign_id",
		references: []dataReference{{"contact_id", "campaign_contacts"}}},
	{name: "simulations", customer: "customer_id"},
	{name: "simulation_runs", customer: "customer_id", references: []dataReference{{"simulation_id", "simulations"}}},
	{name: "simulation_cases", parent: "simulation_runs", parentColumn: "run_id"},
	{name: "users", customer: "customer_id", vouched: []string{"minted", "shard"}},
	{name: "scheduled_tasks", customer: "customer_id"},
	{name: "scheduled_task_runs", customer: "customer_id"},
	// Who asked not to be reached: STOP and START at the source carry on here through the changes.
	// Revoked rows go first, so one revoked at the source lifts its copy here before the live
	// row that replaced it arrives, rather than after the import skipped that row beside it.
	{name: "opt_outs", customer: "customer_id", order: "revoked_at IS NULL"},
	{name: "recordings", customer: "customer_id", shard: true},
	{name: "agent_sessions", customer: "customer_id", shard: true, vouched: []string{"changed_at"}},
	{name: "agent_session_tools", customer: "customer_id", parent: "agent_sessions", parentColumn: "session_id", shard: true},
	// After the sessions their session_id points at.
	{name: "calls", customer: "customer_id", shard: true, vouched: []string{"changed_at"}, references: ofSession},
	{name: "agent_responses", customer: "customer_id", shard: true, references: ofSession},
	{name: "agent_response_items", parent: "agent_responses", parentColumn: "response_id", shard: true},
	{name: "conversation_documents", customer: "customer_id", shard: true},
	// The channel's hidden ranges are in the Stream app and stay there, so the copies of what
	// they hid and the switches that hid it go with their session.
	{name: "agent_branch_switches", customer: "customer_id", shard: true, references: ofSession},
	{name: "agent_branch_messages", customer: "customer_id", shard: true, references: ofSession},
	// Nothing points at a log's id, so a copied log is given one of this deployment's
	// (20261206120000_agent_logs_source_id.sql). Every database draws ids of its own range
	// (logRangeBits), so the source's id names one row however many databases it read.
	{name: "agent_logs", customer: "customer_id", copied: "source_id", shard: true, moving: true},
	// No foreign key points at a request's id. An agent log row names it as text, and
	// AgentLogs.Get only takes a request's error by that number from a row that carries none of
	// its own and whose time and agent are the request's, so a moved row cannot show the error
	// of whichever request holds its number here. Its usage id is the unit of work wherever it is.
	{name: "requests", customer: "customer_id", shard: true, across: "usage_id"},
	{name: "stats_hourly", customer: "customer_id", shard: true},
	{name: "stats_daily", customer: "customer_id", shard: true},
	{name: "stats_tags_hourly", customer: "customer_id", shard: true},
	{name: "stats_tags_daily", customer: "customer_id", shard: true},
	{name: "turn_stats_hourly", customer: "customer_id", shard: true},
	{name: "turn_latency_hourly", customer: "customer_id", shard: true},
	// In the shared database: one row per app, area, client and day it made requests in. The
	// organization is the app's in every deployment, since both are Stream's ids.
	{name: "customer_activity", customer: "app_id"},
	// Only what this app decided. The organization's policy covers apps whose rows are
	// not this caller's to read.
	{name: "policies", customer: "scope_id", filter: "scope = 'app'"},
}

// A secret column never leaves the deployment that holds it.
//
// A sealed key secret is worth nothing anywhere else, since the key that unseals it is
// this deployment's. An OAuth token is worse than worthless: it belongs to whoever
// granted it, to this deployment, and handing it out because somebody asked for a copy of
// their data would be a way to walk off with a customer's users' accounts. Both are left
// out on the way out and left alone on the way in, so a connection arrives needing to be
// authorized again rather than arriving broken.
// A guest's refresh secret hash is the same kind of thing: a moved guest is handed a new
// one by its own token or by the app's backend, or comes back as a new guest.
//
// isSecret is the one list: the export, the import and the record_data_change trigger
// (watchDataChanges) all read it. A trigger attached before the list was passed applies the
// same rule in SQL (record_data_change's own copy): change both together.
func isSecret(column string) bool {
	return strings.HasSuffix(column, "_sealed") || strings.HasSuffix(column, "_secret_hash") ||
		slices.Contains(oauthSecrets, column)
}

// oauthSecrets are an OAuth grant's tokens and the state of one under way.
var oauthSecrets = []string{"access_token", "refresh_token", "oauth_state", "code_verifier"}

// retiredTables are tables an export from an older release may carry that this schema dropped.
// An import skips their rows, inserts and deletes alike.
var retiredTables = []string{"agent_plugin_connections", "agent_plugin_clients", "agent_plugin_event_subscriptions", "agent_plugin_event_deliveries"}

// DataChange is one thing that happened to one row.
type DataChange struct {
	Seq   int64           `json:"seq"`
	Table string          `json:"table"`
	Op    string          `json:"op"`
	Key   json.RawMessage `json:"key"`
	// Payload is the row as it now reads, and is absent for a delete.
	Payload json.RawMessage `json:"payload,omitempty"`
	At      time.Time       `json:"at"`
}

// Operations a change records.
const (
	ChangeInsert = "insert"
	ChangeUpdate = "update"
	ChangeDelete = "delete"
)

// shape is what a table's columns are, read from the catalog rather than kept in a list
// here that a migration could leave behind.
type shape struct {
	columns []string
	keys    []string
	// sequences are the sequences behind the columns whose default draws from one, by column.
	sequences map[string]string
}

// secrets are the table's columns that never leave the deployment (isSecret).
func (s shape) secrets() []string {
	var secrets []string
	for _, column := range s.columns {
		if isSecret(column) {
			secrets = append(secrets, column)
		}
	}
	return secrets
}

// tableShapes caches those, since they only change when a migration does.
type tableShapes struct {
	mu     sync.Mutex
	byName map[string]shape
}

// StartDataCapture records that a customer is moving, so their changes are written down
// for the deployment they are moving to.
//
// Nothing is recorded for a customer without one of these rows: on a busy deployment
// every model request is a row, and recording all of them for everybody in case somebody
// one day leaves is a cost nobody agreed to. It expires so that a move abandoned halfway
// stops recording on its own, and reading changes pushes it out again.
//
// A capture that was off and is turned on is a start, and updates data_change_starts in the
// same transaction: a repeatable read or serializable write that looked for a capture before
// it holds that row (record_data_change), so a start waits for it, and gives up with
// ErrWritesOpen past within. Pushing out a capture still on is no start, so following the
// changes waits for nobody.
//
// A start of a capture that expired also takes a change number of its own, above every one handed
// out before, into recorded_since: what was written while it was off was never recorded and has
// no number, so Changes refuses a cursor below it. The row outlives its capture for that
// (PruneDataChanges). A capture within captureLapseMargin of expiring counts as expired: a
// write can run its trigger past expires_at before this renewal commits and is not recorded, so
// the renewal fails closed.
func (s *Store) StartDataCapture(ctx context.Context, customerID string, retention, within time.Duration) error {
	if customerID == "" {
		return errors.New("store: a customer id is required")
	}
	err := s.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// A lock_timeout of 0 waits for ever. The wait stays under the statement limit, which
		// would otherwise end it first and answer a statement timeout instead of ErrWritesOpen.
		if _, err := tx.ExecContext(ctx, "SELECT set_config('lock_timeout', ?, true)",
			fmt.Sprintf("%dms", max(min(within, captureLockWait).Milliseconds(), 1))); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE data_change_starts SET started = started + 1
WHERE NOT EXISTS (SELECT 1 FROM data_change_capture WHERE customer_id = ? AND expires_at > now())`,
			customerID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
INSERT INTO data_change_capture (customer_id, expires_at)
VALUES (?, now() + make_interval(secs => ?))
ON CONFLICT (customer_id) DO UPDATE SET expires_at = EXCLUDED.expires_at,
  recorded_since = CASE WHEN data_change_capture.expires_at > now() + make_interval(secs => ?) THEN data_change_capture.recorded_since
    ELSE nextval('data_changes_seq_seq') END`,
			customerID, retention.Seconds(), captureLapseMargin.Seconds())
		return err
	})
	if isViolation(err, lockNotAvailable) {
		return ErrWritesOpen
	}
	if err != nil {
		return fmt.Errorf("store: start data capture: %w", err)
	}
	return nil
}

// captureLockWait is the longest StartDataCapture waits on a write open before it, a second
// under the statement limit.
const captureLockWait = statementTimeout - time.Second

// captureLapseMargin is how close to expiring a capture may be and still count as lapsed when
// it is renewed. It is well above a renewal's usual length (milliseconds) and far below any
// retention.
const captureLapseMargin = 5 * time.Second

// ErrWritesOpen says a transaction that wrote before a capture started was still open when
// StartDataCapture or WaitForOpenWrites gave up on it.
var ErrWritesOpen = errors.New("store: a write from before the capture started is still open")

// WaitForOpenWrites returns once every transaction that had written to the shared database
// when it was called has ended, or ErrWritesOpen past within. Called right after
// StartDataCapture, it is what lets an export read nothing short.
//
// The trigger asks whether its customer is moving in a query of its own, after its row is
// written: record_data_change is VOLATILE, so under READ COMMITTED, the level every write
// here runs at, each of its queries takes a new snapshot
// (https://www.postgresql.org/docs/current/xfunc-volatility.html). A write whose query ran
// before the capture committed records nothing, and if it is still open when the export
// reads, it is in neither. Such a transaction took its transaction id, and the lock on it,
// before its row and so before that query, so it holds that lock now. A transaction whose
// query comes later sees the capture and records its change. The lock goes once the commit
// is visible, as record_data_change's notes on CommitTransaction say, so a snapshot taken
// after this returns holds every row of the ones waited for.
// A repeatable read or serializable transaction that has not written yet is not waited for:
// its write is refused instead (record_data_change).
//
// Only this database's transactions: ids are counted across the whole Postgres server, whose
// other databases write nothing of this one's. A shard's tables record no changes
// (watchDataChanges), so a shard has nothing to wait for.
func (s *Store) WaitForOpenWrites(ctx context.Context, within time.Duration) error {
	deadline := time.Now().Add(within)
	var open []string
	if err := s.global.QueryRowContext(ctx, `
SELECT COALESCE(array_agg(l.transactionid::text), '{}') FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.locktype = 'transactionid'
  AND a.datid = (SELECT oid FROM pg_database WHERE datname = current_database())`,
	).Scan(pgdialect.Array(&open)); err != nil {
		return fmt.Errorf("store: open writes: %w", err)
	}
	for pause := migrateLockFirst; len(open) > 0; pause = min(2*pause, migrateLockMost) {
		if time.Now().After(deadline) {
			return ErrWritesOpen
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(pause, time.Until(deadline))):
		}
		if err := s.global.QueryRowContext(ctx, `
SELECT COALESCE(array_agg(transactionid::text), '{}') FROM pg_locks
WHERE locktype = 'transactionid' AND transactionid::text = ANY(?)`,
			pgdialect.Array(open)).Scan(pgdialect.Array(&open)); err != nil {
			return fmt.Errorf("store: open writes: %w", err)
		}
	}
	return nil
}

// exportLimit is how long an export's statements may run and its transactions sit idle.
const exportLimit = time.Hour

// ExportCustomer writes every row belonging to a customer and returns the cursor their
// changes carry on from.
//
// The rows are read in one repeatable-read transaction, so what comes out is the customer
// as they were at one moment rather than a stitch of several. The cursor is the newest
// change kept at or below the watermark taken right before that transaction, 0 with none,
// and is deliberately conservative: a change at or below it is already in these rows, and
// one above it may or may not be, which is safe because applying a change twice lands in
// the same place as applying it once. It is not the watermark itself: with no change kept
// at or below it, Changes would read a number rolled back right after it as a change
// pruned, and refuse the cursor. It can be the last change of theirs pruned with none kept
// at or below it: Changes reads a cursor there as a start and checks no gap after it.
//
// Call it after StartDataCapture and WaitForOpenWrites, in that order: on its own it still misses
// a write that was open when the capture started and commits after the snapshot.
//
// What is not here: the audio behind a voice sample and a call recording, which live in
// an object bucket rather than in Postgres. The rows naming them come across, and copying
// the bucket is a copy between two object stores that neither deployment should be in the
// middle of.
//
// Its transactions run past the serving limits, to exportLimit: a table of a large customer
// streams for longer than a statement may take, and each transaction sits idle while the
// others, or a slow reader of the rows, are served.
func (s *Store) ExportCustomer(ctx context.Context, customerID string, write func(table string, row json.RawMessage) error) (int64, error) {
	if customerID == "" {
		return 0, errors.New("store: a customer id is required")
	}

	// Before the transaction, whose snapshot is taken at its first statement: every change at
	// or below the mark has then ended before the snapshot and is in it.
	mark, err := s.changeWatermark(ctx)
	if err != nil {
		return 0, err
	}
	tx, err := s.global.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return 0, fmt.Errorf("store: export: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // read-only, and the error on the way out says nothing
	if err := boundTransaction(ctx, tx, exportLimit, lockTimeout, exportLimit); err != nil {
		return 0, fmt.Errorf("store: export: %w", err)
	}

	// Never below where the recording last started again: Changes refuses a cursor from before
	// it. A transaction holding a number at or below it had written its row before that start,
	// so the WaitForOpenWrites before this export saw it end and these rows hold its write.
	// Nor below the last change of theirs pruned: its transaction had ended before the prune read
	// its mark, so these rows hold it too.
	since, pruned, err := recordedSince(ctx, tx, customerID)
	if err != nil {
		return 0, err
	}
	var cursor int64
	if err := tx.QueryRowContext(ctx,
		"SELECT GREATEST(COALESCE(MAX(seq), 0), ?, ?) FROM data_changes WHERE seq <= ?", since, pruned, mark).Scan(&cursor); err != nil {
		return 0, fmt.Errorf("store: export cursor: %w", err)
	}

	if !s.HasShards() {
		for _, table := range dataTables {
			if err := s.exportTable(ctx, tx, table, customerID, write); err != nil {
				return 0, err
			}
		}
		return cursor, nil
	}

	// The shard is read at one moment of its own. No table of the shared database points at a
	// shard's, so writing the shared tables first still writes every parent first.
	shardTx, err := s.shard.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return 0, fmt.Errorf("store: export: shard: %w", err)
	}
	defer shardTx.Rollback() //nolint:errcheck // read-only, and the error on the way out says nothing
	if err := boundTransaction(ctx, shardTx, exportLimit, lockTimeout, exportLimit); err != nil {
		return 0, fmt.Errorf("store: export: shard: %w", err)
	}
	for _, table := range dataTables {
		if !table.shard {
			if err := s.exportTable(ctx, tx, table, customerID, write); err != nil {
				return 0, err
			}
		}
	}
	for _, table := range dataTables {
		if !table.shard {
			continue
		}
		if err := s.exportTable(ctx, shardTx, table, customerID, write); err != nil {
			return 0, err
		}
		if table.moving && s.Sharded() {
			if err := s.exportTable(ctx, tx, table, customerID, write); err != nil {
				return 0, err
			}
		}
	}
	return cursor, nil
}

// changeWatermark is a sequence number every change at or below has ended: committed and
// visible to any statement started after it returns, or rolled back.
//
// Taking the highest number that can be seen would be wrong: the numbers are handed out
// when a row is inserted and become visible when its transaction commits, so a reader
// could step over a lower number belonging to a transaction still in flight and never
// come back for it. Nor does any committed row bound what a transaction still running
// holds, whatever the order of their transaction ids. So each transaction recording a
// change holds a lock naming the highest number handed out before its first one
// (record_data_change), and this stops at the lowest such lock.
//
// The two reads are in this order, each its own statement. A number at or below the
// first was handed out before it, so its transaction's lock was taken before the second:
// either the lock is there and the mark is below the number, or its transaction has ended.
func (s *Store) changeWatermark(ctx context.Context) (int64, error) {
	var handedOut int64
	if err := s.global.QueryRowContext(ctx,
		"SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM data_changes_seq_seq",
	).Scan(&handedOut); err != nil {
		return 0, fmt.Errorf("store: change watermark: %w", err)
	}
	var held sql.NullInt64
	if err := s.global.QueryRowContext(ctx, `
SELECT MIN((classid::bigint << 32) | objid::bigint) FROM pg_locks
WHERE locktype = 'advisory' AND objsubid = 2
  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&held); err != nil {
		return 0, fmt.Errorf("store: change watermark: %w", err)
	}
	if held.Valid && held.Int64 < handedOut {
		return held.Int64, nil
	}
	return handedOut, nil
}

// recordedSince is the number a customer's changes have been recorded from without a gap: 0
// unless their capture expired and was started again (StartDataCapture). pruned is the highest
// number of theirs PruneDataChanges dropped, 0 if none.
func recordedSince(ctx context.Context, tx bun.Tx, customerID string) (since, pruned int64, err error) {
	err = tx.QueryRowContext(ctx,
		"SELECT recorded_since, pruned_through FROM data_change_capture WHERE customer_id = ?", customerID).Scan(&since, &pruned)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, 0, fmt.Errorf("store: recorded since: %w", err)
	}
	return since, pruned, nil
}

func (s *Store) exportTable(ctx context.Context, tx bun.Tx, table dataTable, customerID string, write func(string, json.RawMessage) error) error {
	shape, err := s.shapeOf(ctx, table.name)
	if err != nil {
		return err
	}
	query, args := table.selectRows(customerID, shape.secrets())
	// Read through a cursor in the transaction's snapshot rather than as one query: closing a
	// query's rows early reads every row left, so an export that ends partway would hold its
	// transaction until the whole table had been read. A batch is all it reads instead.
	if _, err := tx.ExecContext(ctx, "DECLARE export_rows NO SCROLL CURSOR FOR "+query, args...); err != nil {
		return fmt.Errorf("store: export %s: %w", table.name, err)
	}
	for {
		fetched, err := s.exportBatch(ctx, tx, table.name, write)
		if err != nil {
			return err
		}
		if fetched < exportBatchRows {
			break
		}
	}
	if _, err := tx.ExecContext(ctx, "CLOSE export_rows"); err != nil {
		return fmt.Errorf("store: export %s: %w", table.name, err)
	}
	return nil
}

// exportBatchRows is how many rows an export reads from its cursor at a time, and so the most it
// reads past the row it stops at. A row can be a whole document's text of several MiB, which
// a hundred keep to a moment's reading, and most rows are well under a KiB.
const exportBatchRows = 100

// exportBatch writes the next exportBatchRows rows of the export_rows cursor and returns how many
// it read.
func (s *Store) exportBatch(ctx context.Context, tx bun.Tx, table string, write func(string, json.RawMessage) error) (int, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf("FETCH %d FROM export_rows", exportBatchRows))
	if err != nil {
		return 0, fmt.Errorf("store: export %s: %w", table, err)
	}
	defer rows.Close()

	fetched := 0
	for rows.Next() {
		fetched++
		var row json.RawMessage
		if err := rows.Scan(&row); err != nil {
			return 0, fmt.Errorf("store: export %s: %w", table, err)
		}
		if row, err = s.exportPin(table, row); err != nil {
			return 0, err
		}
		if err := write(table, row); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: export %s: %w", table, err)
	}
	return fetched, nil
}

// selectRows reads one table's rows for one customer, without the columns that never
// leave.
func (t dataTable) selectRows(customerID string, secrets []string) (string, []any) {
	var subject strings.Builder
	subject.WriteString("to_jsonb(t)")
	for _, column := range secrets {
		subject.WriteString(" - '" + column + "'")
	}

	if t.parent != "" {
		// Two customers may each have a parent of one id: a row naming its customer is the one of
		// the parent of that customer's.
		named := ""
		if t.customer != "" {
			named = fmt.Sprintf(" AND (t.%[1]s IS NULL OR t.%[1]s = p.customer_id)", t.customer)
		}
		return fmt.Sprintf(
			"SELECT %s FROM %s AS t JOIN %s AS p ON p.id = t.%s WHERE p.customer_id = ?%s",
			subject.String(), t.name, t.parent, t.parentColumn, named), []any{customerID}
	}
	where := t.customer + " = ?"
	if t.filter != "" {
		where += " AND " + t.filter
	}
	if t.order != "" {
		where += " ORDER BY " + t.order
	}
	return fmt.Sprintf("SELECT %s FROM %s AS t WHERE %s", subject.String(), t.name, where), []any{customerID}
}

// ImportRow writes one exported row into this deployment, under the customer doing the
// importing.
//
// Whose row it is comes from the caller rather than from the row: an export names a
// customer inside every row it carries, and believing that would let anybody holding
// credentials for one app write into another. For a row that belongs to a customer
// through a parent there is no column to overwrite, so the parent is checked instead and
// a row whose parent is somebody else's is silently not written. A row whose key a row of
// another customer holds here is refused (ErrAnotherCustomersRow), and so is one whose
// references name a row this customer does not hold here.
//
// It upserts, so importing the same export twice is the same as importing it once, which
// is what lets a move be resumed after it was interrupted.
//
// A number on the customer's own trunk that another customer holds here is not written:
// *NumbersLeftOutError.
func (s *Store) ImportRow(ctx context.Context, customerID, table string, row json.RawMessage) error {
	// A cached row's write outlives ctx, so an import that has ended stops here.
	if err := ctx.Err(); err != nil {
		return err
	}
	spec, _ := dataTableByName(table)
	var stale []string
	if table == "phone_numbers" || table == "connector_definitions" {
		// A number is checked against who else holds it under a lock its write has to keep
		// (holders), and only a transaction keeps one. A definition is weighed with the configs
		// binding it as the transaction writes it.
		return s.sideOf(spec).RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			if err := s.importRow(ctx, tx, customerID, table, row, &stale); err != nil || table != "connector_definitions" {
				return err
			}
			return s.refuseImportedDefinitions(ctx, tx, customerID, []json.RawMessage{row})
		})
	}
	// Most rows of an export have no keys to drop and pay for no transaction.
	if !readCached(table) {
		return s.importRow(ctx, s.sideOf(spec), customerID, table, row, &stale)
	}
	err := s.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		if err := s.importRow(ctx, tx, customerID, table, row, &stale); err != nil || table != "agent_configs" {
			return err
		}
		return s.refuseImportedConfigs(ctx, tx, customerID, []json.RawMessage{row})
	}, func() []string { return stale })
	s.Users.forgetKeys(stale)
	return err
}

// sideOf is the database a table's rows are written to here: this shard's or the shared one.
func (s *Store) sideOf(table dataTable) *bun.DB {
	if table.shard {
		return s.shard
	}
	return s.global
}

// lineColumns are what attaching a number, or giving it a line, writes on its row (PhoneNumbers.Attach,
// TrunkGroupTx.SetLine): where its calls go and which agent answers them. The sealed bridge
// password is one too, and an import never writes it (isSecret).
var lineColumns = []string{"stream_trunk_id", "stream_route_id", "stream_app_pk", "stream_call_id", "stream_call_type",
	"attached_at", "config_id"}

// importRow writes one row of an export or of a change (ImportRow). It adds to stale the cached
// values the row was and is read through, for the caller to forget once the write has committed.
func (s *Store) importRow(ctx context.Context, db bun.IDB, customerID, table string, row json.RawMessage, stale *[]string) error {
	if slices.Contains(retiredTables, table) {
		return nil
	}
	spec, found := dataTableByName(table)
	if !found {
		return invalidImport(fmt.Errorf("store: %q is not a table an export carries", table))
	}
	row, err := s.importPin(ctx, customerID, table, row)
	if err != nil {
		return err
	}
	switch table {
	case "scheduled_tasks":
		row, err = pauseImported(row, []string{ScheduledTaskActive}, ScheduledTaskPaused, true)
	case "campaigns":
		row, err = pauseImported(row, []string{Running, Scheduled}, Paused, false)
	case "campaign_contacts":
		row, err = requeueImportedContact(row)
	case "campaign_attempts":
		row, err = endImportedAttempt(row, time.Now())
	case "simulation_runs":
		row, err = renameImported(row, map[string]string{"held_until": "lease_until"})
	case "opt_outs":
		row, err = normalizeImportedOptOut(row)
	case "agent_configs":
		row, err = renameImported(row, map[string]string{"sandbox": "sandbox_provider", "episode_cards": "episode_cards_on",
			"channels": "channels_lines", "chat_channels": "channels_chat"})
	}
	if err != nil {
		return err
	}
	if table == "phone_numbers" {
		if importable, err := importableNumber(ctx, db, customerID, row); err != nil || !importable {
			return err
		}
	}
	if table == "connector_definitions" {
		// The built-in customer owns the built-in definitions every app shares, and an import
		// never writes them: `router replicate` without --as hands over "" until a row names
		// a customer.
		if customerID == BuiltinCustomer {
			return fmt.Errorf("store: an import writes a connector definition under a customer, and none is known yet: %w (name the customer with --as)", ErrNotACustomConnector)
		}
		if SkippedRow(table, row) != "" {
			return nil
		}
		if row, err = importableDefinition(row); err != nil {
			return err
		}
	}
	shape, err := s.shapeOf(ctx, table)
	if err != nil {
		return err
	}

	columns := spec.written(shape)
	quoted := strings.Join(columns, ", ")
	picked := slices.Clone(columns)
	if i := slices.Index(columns, "sync_hash"); i >= 0 {
		// A row from a release before sync hashes carries none, and no sync wrote it here.
		picked[i] = "COALESCE(r.sync_hash, '')"
	}
	if i := slices.Index(columns, "minted_elsewhere"); i >= 0 {
		// Minted at the source, or where the source had the guest from.
		picked[i] = "r.minted IS TRUE OR r.minted_elsewhere IS TRUE"
	}
	if i := slices.Index(columns, "usage_id"); spec.name == "requests" && i >= 0 {
		// The usage id is this deployment's, made from the one the row had where it was first
		// recorded, or from the source's id for a row from before usage ids. A usage id names a
		// unit of work this deployment bills, such as a month of a number, so the one sent is
		// not kept. Importing the row again, from wherever it moved since, names the same row.
		// A row taken back where it was first recorded is that row again: one this customer
		// already holds here names no charge still to come.
		origin := "COALESCE(r.source_usage_id, r.usage_id)"
		picked[i] = fmt.Sprintf("CASE WHEN EXISTS (SELECT 1 FROM requests AS h WHERE h.usage_id = %[1]s AND h.customer_id = r.customer_id) THEN %[1]s ELSE %[2]s END",
			origin, importedUsageID("customer_id", "COALESCE("+origin+"::text, id::text)"))
		if j := slices.Index(columns, "source_usage_id"); j >= 0 {
			// Where the row was first recorded, however many moves ago.
			picked[j] = origin
		}
	}
	if i := slices.Index(columns, "config_id"); spec.name == "phone_numbers" && i >= 0 {
		// The source's agent only while it is this customer's and live here, read under the lock a
		// config delete waits on, as the other writers of a number's agent read it (liveConfig).
		picked[i] = "CASE WHEN EXISTS (SELECT 1 FROM agent_configs AS c WHERE c.id = r.config_id" +
			" AND c.customer_id = r.customer_id AND c.deleted_at IS NULL FOR SHARE) THEN r.config_id END"
	}
	if i := slices.Index(columns, spec.copied); spec.copied != "" && i >= 0 {
		// The source's id, whatever the source kept as the id of where the row came from.
		picked[i] = shape.keys[0]
	}
	selected := strings.Join(picked, ", ")

	assignments := make([]string, 0, len(columns))
	for _, column := range columns {
		switch {
		case spec.isKey(shape, column):
		case spec.name == "phone_numbers" && slices.Contains(lineColumns, column):
			// A number on a trunk here was attached or given a line here (PhoneNumbers.Attach, TrunkGroupTx.SetLine),
			// since an imported row arrives off any trunk (identity). Where its calls go is then
			// this deployment's, so the source's copy of the row, in a copy taken again or a
			// change applied later, does not detach it. The agent is too only once attached:
			// a line alone never wrote it (SetLine), so the source's agent still arrives. A number
			// the source gave back keeps nothing, as a release here clears the line first, and a
			// row that is now another number's has no line for that one.
			kept := "phone_numbers.stream_trunk_id IS NOT NULL"
			if column == "config_id" {
				kept = "phone_numbers.attached_at IS NOT NULL"
			}
			assignments = append(assignments, fmt.Sprintf(
				"%[1]s = CASE WHEN %[2]s AND EXCLUDED.released_at IS NULL AND EXCLUDED.e164 = phone_numbers.e164 THEN phone_numbers.%[1]s ELSE EXCLUDED.%[1]s END", column, kept))
		case spec.name == "agent_sessions" && column == "project_tag":
			// A session carried on in another shard is a row in each, and arrives as one: the row
			// that does not say the tag leaves the one that does, whichever arrives last.
			assignments = append(assignments, "project_tag = COALESCE(EXCLUDED.project_tag, agent_sessions.project_tag)")
		default:
			assignments = append(assignments, column+" = EXCLUDED."+column)
		}
	}
	if spec.across != "" {
		assignments = nil
	}

	// The row first, then the customer, in the order the placeholders appear: the
	// customer is forced onto the row of a table that names one, and checked against the
	// parent of a table that does not.
	subject, where := "?::jsonb", ""
	if spec.customer != "" {
		subject = fmt.Sprintf("?::jsonb || %s", spec.identity())
	}
	if spec.parent != "" {
		where = " WHERE " + spec.belonging().held("r")
	}
	if spec.copied != "" {
		where = " WHERE NOT " + spec.copiedBefore(shape, columns)
	}
	if table == "opt_outs" {
		// An opt-out live here under another id is this deployment's, and stays: the row arriving
		// would break the one-live-row index, and dropping the live one would let the person be
		// reached again. A revoked row is history beside it, and revokes only the row of its own id,
		// so a START replayed from the source lifts only what the source sent. Two writes of one
		// live opt-out here at the same moment can still fail the statement on that index.
		where = " WHERE NOT (r.revoked_at IS NULL AND EXISTS (SELECT 1 FROM opt_outs AS h WHERE h.customer_id = r.customer_id" +
			" AND h.recipient = r.recipient AND h.channel = r.channel AND h.revoked_at IS NULL AND h.id <> r.id))"
	}
	args := []any{string(row), customerID}
	read := args
	if spec.customer == "" {
		read = args[:1]
	}
	if spec.customer != "" && spec.parent != "" {
		// The customer forced onto the row, then checked against its parent.
		args = append(args, customerID)
	}
	for _, reference := range spec.references {
		joined := " AND "
		if where == "" {
			joined = " WHERE "
		}
		where += fmt.Sprintf("%s(r.%s IS NULL OR %s)", joined, reference.column, reference.held("r"))
		args = append(args, customerID)
	}
	if table == "requests" {
		// The organization a request counts against is the app's here, whatever the row says.
		// It is in the shared database and requests in this shard's, so it is read first. An app
		// this deployment neither saw nor registered has none here, and its requests keep none.
		organization, err := s.AppOrganizations.OrganizationOf(ctx, customerID)
		if err != nil {
			return err
		}
		args = append(args, organization)
		read = args
	}
	if err := validateImported(ctx, db, spec, shape, columns, picked, subject, read); err != nil {
		if refused := refusedRow(table, err); refused != nil {
			return refused
		}
		return fmt.Errorf("store: import %s: %w", table, err)
	}

	// A row nobody may update is one whose primary key is all it has, and writing it
	// again would be the same row. Doing nothing is both the right answer and a legal
	// statement, where an empty SET list is neither.
	//
	// The update is guarded by who already holds the row rather than by who is writing
	// it. Primary keys are carried across as they are, so without this an import could
	// name a row it happened to know the id of and overwrite another customer's.
	conflict := "DO NOTHING"
	if len(assignments) > 0 {
		conflict = "DO UPDATE SET " + strings.Join(assignments, ", ") + " WHERE " + spec.owned(spec.name)
		args = append(args, customerID)
	}
	// A connection connected here holds credentials sealed here, at this row's revision and key
	// version, which an import would reset to a connection needing them. The owner authorized it
	// here, and the source can send nothing it could use instead: not even a newer row, since a
	// refresh at a source still live during the move rewrites the row there.
	//
	// A refresh here checkpoints the row as needing reauthorization before the provider answers,
	// so the status alone would let an import in during the call and fail the save that follows.
	// An import writes key version 0 and only a seal here raises it: a row past 0 is one sealed here.
	if table == "connector_connections" {
		conflict += fmt.Sprintf(" AND %s.status <> '%s' AND %s.credentials_kek_version = 0", spec.name, ConnectionConnected, spec.name)
	}
	// A number bought here and still held is this deployment's, whatever row names its id.
	if table == "phone_numbers" {
		conflict += " AND NOT " + boughtHere("phone_numbers")
	}
	// An opt-out here stays on the recipient and channel it was recorded for: a row arriving
	// under its id for another one neither moves nor lifts it.
	if table == "opt_outs" {
		conflict += " AND opt_outs.recipient = EXCLUDED.recipient AND opt_outs.channel = EXCLUDED.channel"
	}

	// Every writer of a log draws its id under its customer's lock (AgentLogs.Insert), so ids
	// commit in order and a reader resuming after one misses none. The row is read through the
	// lock, so it is taken before the id is drawn and held until the statement's transaction ends.
	locked, through, returning := "", "", ""
	if table == "agent_logs" {
		locked, through = "WITH locked AS (SELECT pg_advisory_xact_lock(hashtextextended(?, ?))) ", "locked, "
		args = append([]any{customerID, agentLogLockSeed}, args...)
	}
	// A row renamed, or a skill moved to another config, is cached under what it was too. What it
	// was is read in the statement that writes it, locked, so the one forgotten is the one a
	// concurrent write did not already replace. It is read in the FROM, so before the row is written.
	// The lock is the one the upsert takes, which a child row being inserted does not wait on.
	if cachedByID(table) {
		locked, through = fmt.Sprintf("WITH was AS (SELECT (SELECT to_jsonb(h) FROM %s AS h"+
			" WHERE h.id::text = ?::jsonb ->> 'id' AND h.customer_id = ? FOR NO KEY UPDATE) AS prior) ", spec.name), "was, "
		args = append([]any{string(row), customerID}, args...)
		returning = " RETURNING (SELECT prior FROM was), " + cachedRow(table, spec.name)
	}

	query := fmt.Sprintf(
		"%sINSERT INTO %s (%s) SELECT %s FROM %sjsonb_populate_record(NULL::%s, %s) AS r%s ON CONFLICT %s %s%s",
		locked, spec.name, quoted, selected, through, spec.name, subject, where, spec.conflictTarget(shape), conflict, returning)

	var written int64
	if returning == "" {
		result, err := db.ExecContext(ctx, query, args...)
		if err != nil {
			if isViolation(err, foreignKeyViolation) {
				// A foreign key that holds the customer: the row it names is missing, or another's.
				named, keyErr := rowKey(table, shape.keys, row)
				if keyErr != nil {
					return keyErr
				}
				return stack.Wrap(invalidImport(marked{fmt.Errorf("store: import %s: %w", named, ErrNamesNoRow), err}))
			}
			if refused := refusedRow(table, err); refused != nil {
				return refused
			}
			return fmt.Errorf("store: import %s: %w", table, err)
		}
		*stale = append(*stale, cachedKeys(customerID, table, row)...)
		if written, err = result.RowsAffected(); err != nil {
			return stack.Wrap(fmt.Errorf("store: import %s: %w", table, err))
		}
	} else if written, err = importCached(ctx, db, customerID, table, query, args, stale); err != nil {
		return err
	}
	if written > 0 || spec.copied != "" {
		return nil
	}
	// Nothing written is the importer's own row kept as it is here, or a row whose key another
	// customer holds, which is refused rather than dropped. A copied row's key names its customer.
	keys := shape.keys
	if spec.across != "" {
		keys = []string{spec.across}
	}
	keyed, held, imported := make([]string, 0, len(keys)), make([]string, 0, len(keys)), make([]string, 0, len(keys))
	for _, key := range keys {
		expression := "r." + key
		if i := slices.Index(columns, key); i >= 0 {
			expression = picked[i]
		}
		keyed = append(keyed, expression+" AS "+key)
		held, imported = append(held, "h."+key), append(imported, "i."+key)
	}
	var taken bool
	if err := db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT EXISTS (SELECT 1 FROM (SELECT %s FROM jsonb_populate_record(NULL::%s, %s) AS r) AS i JOIN %s AS h ON (%s) = (%s) WHERE NOT %s)",
		strings.Join(keyed, ", "), spec.name, subject, spec.name, strings.Join(held, ", "), strings.Join(imported, ", "), spec.owned("h")),
		append(slices.Clone(read), customerID)...).Scan(&taken); err != nil {
		return fmt.Errorf("store: import %s: %w", table, err)
	}
	if taken {
		key, err := rowKey(table, shape.keys, row)
		if err != nil {
			return err
		}
		return invalidImport(fmt.Errorf("store: import %s: %w", key, ErrAnotherCustomersRow))
	}
	if len(spec.references) == 0 {
		return nil
	}
	// A row of a parent that is not this customer's is left out, whatever else it names. Every
	// other row of these tables is written, so this one names a row the customer does not hold.
	if spec.parent != "" {
		var held bool
		if err := db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT EXISTS (SELECT 1 FROM jsonb_populate_record(NULL::%s, ?::jsonb) AS r WHERE %s)",
			spec.name, spec.owned("r")), string(row), customerID).Scan(&held); err != nil {
			return fmt.Errorf("store: import %s: %w", table, err)
		}
		if !held {
			return nil
		}
	}
	key, err := rowKey(table, shape.keys, row)
	if err != nil {
		return err
	}
	return stack.Wrap(invalidImport(fmt.Errorf("store: import %s: %w", key, ErrNamesNoRow)))
}

// ErrNamesNoRow refuses an imported row that names one its customer does not hold here, whether
// or not another does, as an ErrInvalidImport that names the row as rowKey does.
var ErrNamesNoRow = errors.New("a row names a row that neither the export nor this app holds")

// importCached runs an import's query of a cachedByID table, which returns what each row it wrote
// was and is, and adds to stale the values both are cached under. It returns how many it wrote.
func importCached(ctx context.Context, db bun.IDB, customerID, table, query string, args []any, stale *[]string) (int64, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		if refused := refusedRow(table, err); refused != nil {
			return 0, refused
		}
		return 0, fmt.Errorf("store: import %s: %w", table, err)
	}
	defer rows.Close()
	written := int64(0)
	for rows.Next() {
		var was, is []byte
		if err := rows.Scan(&was, &is); err != nil {
			return 0, fmt.Errorf("store: import %s: %w", table, err)
		}
		written++
		*stale = append(*stale, cachedKeys(customerID, table, was)...)
		*stale = append(*stale, cachedKeys(customerID, table, is)...)
	}
	if err := rows.Err(); err != nil {
		if refused := refusedRow(table, err); refused != nil {
			return 0, refused
		}
		return 0, fmt.Errorf("store: import %s: %w", table, err)
	}
	return written, nil
}

// ErrAnotherCustomersRow refuses an imported row whose key a row of another customer holds here,
// as an ErrInvalidImport that names the row as rowKey does.
var ErrAnotherCustomersRow = errors.New("a row with this key belongs to another customer here")

// rowKey names an imported row by its table and the values of its primary key, as
// agent_configs:{"id":"config-1"}.
func rowKey(table string, keys []string, row json.RawMessage) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row, &fields); err != nil {
		return "", invalidImport(fmt.Errorf("store: import %s: %w", table, err))
	}
	key := make(map[string]json.RawMessage, len(keys))
	for _, column := range keys {
		key[column] = fields[column]
	}
	encoded, err := json.Marshal(key)
	if err != nil {
		return "", stack.Wrap(err)
	}
	return table + ":" + string(encoded), nil
}

// cachedByID is whether a moved table's rows are cached by their id and something else they
// hold, which a write can change.
func cachedByID(table string) bool {
	return slices.Contains([]string{"agent_configs", "router_configs", "skills", "voices"}, table)
}

// readCached is whether a moved table's rows are read through a cache (cachedKeys). Every such
// table is in the shared database.
func readCached(table string) bool {
	return cachedByID(table) || table == "policies" || table == "users"
}

// cachedRow is a row of a cachedByID table, as relation names it in a write, in the form cachedKeys
// reads. A voice's carries the providers of its bindings, whose ids are cached under the voice
// (VoiceBindings.GetReady) and read only while it is not deleted.
func cachedRow(table, relation string) string {
	if table != "voices" {
		return "to_jsonb(" + relation + ")"
	}
	return fmt.Sprintf("to_jsonb(%[1]s) || jsonb_build_object('binding_providers',"+
		" ARRAY(SELECT vb.provider FROM voice_bindings AS vb WHERE vb.voice_id = %[1]s.id))", relation)
}

// cachedKeys are the cached values a row of a moved table, as row reads under customerID, is read
// through: what the table's own writes forget (AgentConfigs.Update and its siblings). An app's
// policy is cached by its app alone, so any row of it names the same values. A user is held by
// every replica that saw them (Users.Forget).
func cachedKeys(customerID, table string, row []byte) []string {
	if !readCached(table) {
		return nil
	}
	if table == "policies" {
		return policyKeys(ScopeApp, customerID)
	}
	if len(row) == 0 {
		return nil
	}
	// The columns are text, which jsonb_populate_record also fills from a number, so a value is
	// read as the text it is written as. A row that is not a JSON object was refused before it
	// was written.
	var fields map[string]any
	decoder := json.NewDecoder(bytes.NewReader(row))
	decoder.UseNumber()
	if decoder.Decode(&fields) != nil {
		return nil
	}
	text := func(column string) string {
		if value, found := fields[column]; found && value != nil {
			return fmt.Sprint(value)
		}
		return ""
	}
	switch table {
	case "agent_configs":
		return []string{agentConfigKey(customerID, text("id")), agentConfigNameKey(customerID, text("name"))}
	case "router_configs":
		return []string{routerConfigKey(customerID, text("id")), routerConfigNameKey(customerID, text("name"))}
	case "voices":
		keys := []string{voiceKey(customerID, text("id")), voiceNameKey(customerID, text("name"))}
		providers, _ := fields["binding_providers"].([]any)
		for _, provider := range providers {
			keys = append(keys, voiceBindingKey(customerID, text("id"), fmt.Sprint(provider)))
		}
		return keys
	case "users":
		return []string{userKey(customerID, text("id")), mintedKey(customerID, text("id"))}
	}
	return []string{skillsKey(customerID, text("config_id"))}
}

// validated is a model that refuses a row the API would not store.
type validated interface{ Validate() error }

// importedModels are the models of the tables an import writes, for those that have a Validate,
// so an imported row is held to what the API holds the same row to. A phone number is checked
// by importableNumber instead, which takes one given back in the form it was held in then.
var importedModels = map[string]func() validated{
	"agent_configs":          func() validated { return &AgentConfig{} },
	"skills":                 func() validated { return &Skill{} },
	"connector_definitions":  func() validated { return &ConnectorDefinition{} },
	"connector_connections":  func() validated { return &ConnectorConnection{} },
	"router_configs":         func() validated { return &RouterConfig{} },
	"voices":                 func() validated { return &Voice{} },
	"sip_trunks":             func() validated { return &SIPTrunk{} },
	"knowledge_urls":         func() validated { return &KnowledgeURL{} },
	"knowledge_documents":    func() validated { return &KnowledgeDocument{} },
	"campaigns":              func() validated { return &Campaign{} },
	"campaign_contacts":      func() validated { return &Contact{} },
	"campaign_attempts":      func() validated { return &CampaignAttempt{} },
	"simulations":            func() validated { return &Simulation{} },
	"simulation_runs":        func() validated { return &SimulationRun{} },
	"simulation_cases":       func() validated { return &SimulationCase{} },
	"scheduled_tasks":        func() validated { return &ScheduledTask{} },
	"scheduled_task_runs":    func() validated { return &ScheduledTaskRun{} },
	"opt_outs":               func() validated { return &OptOut{} },
	"recordings":             func() validated { return &Recording{} },
	"agent_sessions":         func() validated { return &AgentSession{} },
	"agent_session_tools":    func() validated { return &agentSessionTools{} },
	"calls":                  func() validated { return &Call{} },
	"agent_responses":        func() validated { return &AgentResponse{} },
	"agent_response_items":   func() validated { return &AgentResponseItem{} },
	"conversation_documents": func() validated { return &ConversationDocument{} },
	"agent_branch_switches":  func() validated { return &AgentBranchSwitch{} },
	"agent_branch_messages":  func() validated { return &AgentBranchMessage{} },
	"agent_logs":             func() validated { return &AgentLog{} },
	"requests":               func() validated { return &Request{} },
	"policies":               func() validated { return &Policy{} },
}

// validateImported reads the row as the import would write it, with what it forces and fills
// in, and refuses it when its model's Validate does. A column the import leaves out is read as
// the row sent it.
func validateImported(ctx context.Context, db bun.IDB, spec dataTable, shape shape, columns, picked []string, subject string, args []any) error {
	model, found := importedModels[spec.name]
	if !found {
		return nil
	}
	read := make([]string, 0, len(shape.columns))
	for _, column := range shape.columns {
		expression := "r." + column
		if i := slices.Index(columns, column); i >= 0 {
			expression = picked[i]
		}
		read = append(read, expression+" AS "+column)
	}
	row := model()
	if err := db.NewRaw(fmt.Sprintf("SELECT %s FROM jsonb_populate_record(NULL::%s, %s) AS r",
		strings.Join(read, ", "), spec.name, subject), args...).Scan(ctx, row); err != nil {
		return stack.Wrap(fmt.Errorf("read the row: %w", err))
	}
	return row.Validate()
}

// ConfigRule is what an import holds the agent configs it writes to, as a config write holds
// them: Config weighs one config, and Graph a parent and its child agents taken as one
// (api.ImportRule, the identity and sensitive-connector rules, whose packages import this one).
// Each answers false, with what is wrong, for a refusal.
type ConfigRule struct {
	Config func(ctx context.Context, definitions Definitions, customerID string, config AgentConfig) (string, bool, error)
	Graph  func(ctx context.Context, definitions Definitions, customerID string, configs []AgentConfig) (string, bool, error)
}

// SetConfigRule holds every agent config an import writes from now on to rule. Without it an
// import holds them to their Validate alone.
func (s *Store) SetConfigRule(rule ConfigRule) { s.configRule.Store(&rule) }

// refuseImportedConfigs refuses an import when an agent config one of rows wrote breaks the
// config rule alone or in a set of agents it is part of (importedGraphComplaint). Each is read
// on db, the import's transaction, so it is weighed as written, beside the configs and
// definitions the transaction wrote before it.
func (s *Store) refuseImportedConfigs(ctx context.Context, db bun.IDB, customerID string, rows []json.RawMessage) error {
	rule := s.configRule.Load()
	if rule == nil {
		return nil
	}
	definitions := definitionsOn{db}
	for _, row := range rows {
		id, err := importedID("agent_configs", row)
		if err != nil {
			return err
		}
		// Only what the rule reads: a row near the import's line limit is all instructions.
		var written AgentConfig
		err = db.NewSelect().Model(&written).
			Column("id", "name", "connectors", "agents", "sandbox_provider", "sandbox_options",
				"identity_agent_id", "identity_user_id", "identity_user_name").
			Where("customer_id = ?", customerID).
			Where("id = ?", id).
			Where("deleted_at IS NULL").
			Scan(ctx)
		// A config deleted at the source runs nothing.
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: import agent_configs: %w", err))
		}
		message, ok, err := importedComplaint(ctx, db, definitions, rule, customerID, written)
		if err != nil {
			return err
		}
		if !ok {
			key, err := rowKey("agent_configs", []string{"id"}, row)
			if err != nil {
				return err
			}
			return invalidImport(fmt.Errorf("store: import %s: %s", key, message))
		}
	}
	return nil
}

// refuseImportedDefinitions refuses an import when a connector definition one of rows wrote
// makes a live agent config binding it break the config rule, alone or in a set of agents it
// is part of. An export carries agent configs before connector definitions, so a config is
// written while a custom connector it binds is not defined yet, and the rule weighs that
// binding only once the definition is. Each is read on db, the import's transaction.
func (s *Store) refuseImportedDefinitions(ctx context.Context, db bun.IDB, customerID string, rows []json.RawMessage) error {
	rule := s.configRule.Load()
	if rule == nil {
		return nil
	}
	definitions := definitionsOn{db}
	for _, row := range rows {
		id, err := importedID("connector_definitions", row)
		if err != nil {
			return err
		}
		bound, err := json.Marshal([]map[string]string{{"connector_id": id}})
		if err != nil {
			return stack.Wrap(err)
		}
		var binding []AgentConfig
		if err := db.NewSelect().Model(&binding).
			Where("customer_id = ?", customerID).
			Where("connectors @> ?::jsonb", string(bound)).
			Where("deleted_at IS NULL").
			Order("created_at", "id").
			Scan(ctx); err != nil {
			return stack.Wrap(fmt.Errorf("store: import connector_definitions: configs binding %s: %w", id, err))
		}
		for _, config := range binding {
			message, ok, err := importedComplaint(ctx, db, definitions, rule, customerID, config)
			if err != nil {
				return err
			}
			if !ok {
				key, err := rowKey("connector_definitions", []string{"id", "revision"}, row)
				if err != nil {
					return err
				}
				return invalidImport(fmt.Errorf("store: import %s: the agent %q binds it: %s", key, config.Name, message))
			}
		}
	}
	return nil
}

// importedComplaint is rule.Config over config, then importedGraphComplaint.
func importedComplaint(ctx context.Context, db bun.IDB, definitions Definitions, rule *ConfigRule, customerID string,
	config AgentConfig) (string, bool, error) {
	message, ok, err := rule.Config(ctx, definitions, customerID, config)
	if err != nil || !ok {
		return message, ok, err
	}
	return importedGraphComplaint(ctx, db, definitions, rule, customerID, config)
}

// importedGraphComplaint is rule.Graph over each set of agents config is part of, as a config
// write weighs them (api.sensitiveGraphComplaint): itself with its children, and each live
// config calling it with all of that one's children, every one read on db.
func importedGraphComplaint(ctx context.Context, db bun.IDB, definitions Definitions, rule *ConfigRule, customerID string,
	config AgentConfig) (string, bool, error) {
	parents, err := configsCalling(ctx, db, customerID, config.Name, config.ID)
	if err != nil {
		return "", false, err
	}
	if len(config.Agents) > 0 {
		parents = append([]AgentConfig{config}, parents...)
	}
	for _, parent := range parents {
		names := make([]string, 0, len(parent.Agents))
		for _, child := range parent.Agents {
			names = append(names, child.Config)
		}
		// A child deleted since, or never held here, runs nothing (childRunner.Run).
		var children []AgentConfig
		if err := db.NewSelect().Model(&children).
			Where("customer_id = ?", customerID).
			Where("name IN (?)", bun.In(names)).
			Where("deleted_at IS NULL").
			Scan(ctx); err != nil {
			return "", false, stack.Wrap(fmt.Errorf("store: import agent_configs: children: %w", err))
		}
		message, ok, err := rule.Graph(ctx, definitions, customerID, append([]AgentConfig{parent}, children...))
		if err != nil || !ok {
			return message, ok, err
		}
	}
	return "", true, nil
}

// importedID is the id an imported row of table gives under the exact key "id", which is the
// one the insert reads (jsonb_populate_record): a JSON decode into a struct also matches a key
// in another case, and the last one it meets wins.
func importedID(table string, row json.RawMessage) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row, &fields); err != nil {
		return "", invalidImport(fmt.Errorf("store: import %s: %w", table, err))
	}
	var id string
	if raw, ok := fields["id"]; ok {
		if err := json.Unmarshal(raw, &id); err != nil {
			return "", invalidImport(fmt.Errorf("store: import %s: id: %w", table, err))
		}
	}
	return id, nil
}

// definitionsOn reads the connector definitions a customer can see on db.
type definitionsOn struct{ db bun.IDB }

func (d definitionsOn) Latest(ctx context.Context, customerID, id string) (ConnectorDefinition, error) {
	return latestVisibleDefinition(ctx, d.db, customerID, id)
}

// The gap an import reserves ahead of the ids it writes, at most. Once a row's id comes
// within half a gap of what was reserved, the reservation moves a gap past it, so a live
// insert here draws a number a moved row would hold only when the source handed out half a
// gap of numbers to other customers between two of the moved customer's rows. 2^40 makes
// that a trillion numbers at once, and a bigint sequence still holds 2^23 such reservations.
const maxReservedGap = int64(1) << 40

// reservedGapShare is the part of a sequence's range one reservation takes when the range is
// too small for maxReservedGap, as for an int4 sequence: 1/256 of its range keeps 256
// reservations' room and, for int4, still asks the source to draw four million numbers
// between two of the moved customer's rows before they could meet a live one.
const reservedGapShare = 256

// IDReservation keeps the sequences of this deployment ahead of the ids one run of an import
// writes, so no row inserted here, whoever's, draws a number a moved row holds.
//
// An import keeps the source's ids, and the sequence behind such a column here knows nothing
// of them. Setting the sequence just to each row's id would leave its next number at the
// source's next one, and a live insert here would take the number of the source's next moved
// row first. So a run moves the sequence a gap ahead at once and again only when a row comes
// within half a gap of that, which is about one setval per sequence per run.
//
// Only for a source the operator vouches for: the numbers are the row's word, and a row naming
// a number near a sequence's largest would use up its room for every insert into its table.
type IDReservation struct {
	// sequences are those behind the columns an import writes, by table and column.
	sequences map[string]map[string]*reservedSequence
}

// reservedSequence is one sequence and how far a run has moved it.
type reservedSequence struct {
	name string
	side *bun.DB
	// mark is the number the run last set the sequence to, or read it at: this deployment
	// draws none at or below it any more.
	mark int64
	gap  int64
	max  int64
}

// ReserveImportedIDs reads the sequences behind the columns an import writes, before a run
// writes any row, and fails if one has no room left for the run's reservations.
func (s *Store) ReserveImportedIDs(ctx context.Context) (*IDReservation, error) {
	reservation := &IDReservation{sequences: map[string]map[string]*reservedSequence{}}
	for _, spec := range dataTables {
		shape, err := s.shapeOf(ctx, spec.name)
		if err != nil {
			return nil, err
		}
		for _, column := range spec.written(shape) {
			name, drawn := shape.sequences[column]
			if !drawn {
				continue
			}
			sequence := &reservedSequence{name: name, side: s.sideOf(spec)}
			// The last number a sequence handed out is its last value once it has handed that
			// out, or the one before.
			query := fmt.Sprintf(`
SELECT CASE WHEN s.is_called THEN s.last_value ELSE s.last_value - 1 END, p.seqmax
FROM %s AS s, pg_sequence AS p
WHERE p.seqrelid = ?::regclass`, name)
			if err := sequence.side.QueryRowContext(ctx, query, name).Scan(&sequence.mark, &sequence.max); err != nil {
				return nil, fmt.Errorf("store: read %s: %w", name, err)
			}
			sequence.gap = min(maxReservedGap, sequence.max/reservedGapShare)
			if sequence.max-sequence.mark < 2*sequence.gap {
				return nil, fmt.Errorf("store: %s has %d numbers left, fewer than the %d a copy reserves and leaves to this deployment",
					name, sequence.max-sequence.mark, 2*sequence.gap)
			}
			if reservation.sequences[spec.name] == nil {
				reservation.sequences[spec.name] = map[string]*reservedSequence{}
			}
			reservation.sequences[spec.name][column] = sequence
		}
	}
	return reservation, nil
}

// Cover moves the sequences past the numbers a row is about to be imported with, before it
// lands, so no live insert draws one of them once it is here.
//
// The sequence is read and set in one statement nothing locks: a live insert drawing in
// between is not handed its number again, since the sequence is set a gap past what it had
// handed out. It is never set below that.
func (r *IDReservation) Cover(ctx context.Context, table string, row json.RawMessage) error {
	sequences := r.sequences[table]
	if len(sequences) == 0 {
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row, &fields); err != nil {
		return fmt.Errorf("store: read a row of %s: %w", table, err)
	}
	for column, sequence := range sequences {
		id, err := strconv.ParseInt(string(fields[column]), 10, 64)
		if err != nil {
			return fmt.Errorf("store: %s.%s of a row is %s, not a number: %w", table, column, fields[column], err)
		}
		if id <= sequence.mark-sequence.gap/2 {
			continue
		}
		query := fmt.Sprintf(`
SELECT setval(?::regclass, GREATEST(?, CASE WHEN is_called THEN last_value ELSE last_value - 1 END) + ?)
FROM %s`, sequence.name)
		if err := sequence.side.QueryRowContext(ctx, query, sequence.name, id, sequence.gap).Scan(&sequence.mark); err != nil {
			return fmt.Errorf("store: reserve %s.%s: %w", table, column, err)
		}
	}
	return nil
}

// pauseImported makes an imported scheduled task or campaign one this deployment cannot
// deliver yet. The deployment it came from goes on delivering until the move is cut over, so
// an active task would be delivered twice. It arrives paused and marked move_paused, which is
// what `router replicate --resume` resumes at cut-over (ResumeMovedRows); a task arrives with
// the claim of a worker there cleared. Every change the move carries is imported the same
// way, so a task still active there stays paused here until then, and one paused, completed
// or failed there keeps its state and loses the mark: the user paused it, so the cut-over
// leaves it paused.
//
// A campaign running or scheduled there does not ring anybody here either way: every router
// dials a running campaign, so it arrives paused, and marked so that the cut-over can name the
// campaigns the source was still running. One the user paused there before the last catch-up
// loses the mark: the copy holds every call it made there.
func pauseImported(row json.RawMessage, active []string, paused string, claimed bool) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row, &fields); err != nil {
		return nil, invalidImport(fmt.Errorf("store: read an imported row: %w", err))
	}
	moved := slices.ContainsFunc(active, func(state string) bool { return string(fields["state"]) == `"`+state+`"` })
	if moved {
		fields["state"] = json.RawMessage(`"` + paused + `"`)
	}
	fields["move_paused"] = json.RawMessage(strconv.FormatBool(moved))
	if claimed {
		fields["leased_by"] = json.RawMessage(`""`)
		fields["lease_until"] = json.RawMessage("null")
	}
	return json.Marshal(fields)
}

// renameImported carries an imported row's columns that a migration since renamed under their
// new names (renames, old to new), as an export from an older release names them.
func renameImported(row json.RawMessage, renames map[string]string) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row, &fields); err != nil {
		return nil, invalidImport(fmt.Errorf("store: read an imported row: %w", err))
	}
	for old, renamed := range renames {
		value, found := fields[old]
		if !found {
			continue
		}
		if _, carried := fields[renamed]; !carried {
			fields[renamed] = value
		}
		delete(fields, old)
	}
	return json.Marshal(fields)
}

// normalizeImportedOptOut writes an imported opt-out's recipient as this deployment keeps it
// (optOutRecipient), so the one-live-row check and the update guard compare it with what is
// kept here, and a source that never normalized its rows hands over ones that match.
func normalizeImportedOptOut(row json.RawMessage) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row, &fields); err != nil {
		return nil, invalidImport(fmt.Errorf("store: read an imported row: %w", err))
	}
	var recipient string
	if err := json.Unmarshal(fields["recipient"], &recipient); err != nil {
		return row, nil // validateImported refuses a row without one
	}
	recipient, _ = optOutRecipient(recipient, "")
	written, err := json.Marshal(recipient)
	if err != nil {
		return nil, err
	}
	fields["recipient"] = written
	return json.Marshal(fields)
}

// requeueImportedContact gives up the source's lease on a contact it was ringing, since the
// runner that held it is in the deployment it came from. One in a call keeps its stage with a
// lease already run out, so once the move is cut over a runner here settles it from its dial,
// as it would one whose runner died: its next attempt waits its delay. That lease ends at the
// Unix epoch. Postgres's clock is always after that time, also when the clock of the importing
// router is ahead. One awaiting its answered call's outcome keeps waiting, and has the
// transcript read here.
func requeueImportedContact(row json.RawMessage) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row, &fields); err != nil {
		return nil, invalidImport(fmt.Errorf("store: read an imported row: %w", err))
	}
	until := json.RawMessage("null")
	switch string(fields["stage"]) {
	case `"` + InProgress + `"`:
		until = json.RawMessage(`"1970-01-01T00:00:00Z"`)
	case `"` + AwaitingOutcome + `"`:
	default:
		return row, nil
	}
	fields["leased_by"] = json.RawMessage(`""`)
	fields["lease_until"] = until
	return json.Marshal(fields)
}

// endImportedAttempt closes a call that was still happening there, which nobody here will end,
// as one whose ending is unknown, and makes an answered one whose outcome is still to be read due now rather than
// when the source's lease on it would have run out.
func endImportedAttempt(row json.RawMessage, now time.Time) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row, &fields); err != nil {
		return nil, invalidImport(fmt.Errorf("store: read an imported row: %w", err))
	}
	stamp, err := json.Marshal(now)
	if err != nil {
		return nil, fmt.Errorf("store: write an imported row: %w", err)
	}
	if string(fields["classify_state"]) == `"`+classifyPending+`"` {
		fields["classify_after"] = stamp
	}
	if ended, ok := fields["ended_at"]; !ok || string(ended) == "null" {
		fields["call_outcome"] = json.RawMessage(`"unknown"`)
		fields["ended_at"] = stamp
	}
	return json.Marshal(fields)
}

// SkipOccurrencesBeforeCutOver moves each of the customer's tasks a move paused whose next
// run came at or before the cut-over on to its next occurrence after it, as a resume counts a
// paused task's next run from now. The source went on delivering after the last catch-up
// until it was stopped, and what it delivered then never reached this copy, so such an
// occurrence was delivered there or slept through, unless the source held its run past the
// cut-over. next is the task's next occurrence after
// the cut-over, or nil with the missed run to record, if any, when it has none left: then the
// task is completed and loses the mark.
func (s *Store) SkipOccurrencesBeforeCutOver(ctx context.Context, customerID string, cutOver time.Time,
	next func(ScheduledTask) (*time.Time, *ScheduledTaskRun, error)) error {
	err := s.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var tasks []ScheduledTask
		// Locked, as a pause or an edit here may land meanwhile and writes the whole row.
		if err := tx.NewSelect().Model(&tasks).
			Where("customer_id = ?", customerID).
			Where("move_paused").
			Where("state = ?", ScheduledTaskPaused).
			Where("next_run_at <= ?", cutOver).
			// An occurrence the source held for quiet hours past the cut-over was never
			// delivered there: the worker here takes its run over when the hold ends.
			Where(`NOT EXISTS (SELECT 1 FROM scheduled_task_runs AS str
				WHERE str.customer_id = st.customer_id AND str.task_id = st.id
				AND str.scheduled_for = st.next_run_at AND str.state = ? AND str.postponed_until > ?)`,
				ScheduledTaskRunRunning, cutOver).
			For("UPDATE").
			Scan(ctx); err != nil {
			return fmt.Errorf("store: moved scheduled tasks due before the cut-over: %w", err)
		}
		for _, task := range tasks {
			at, missed, err := next(task)
			if err != nil {
				return err
			}
			update := tx.NewUpdate().Model((*ScheduledTask)(nil)).
				Set("updated_at = now()").
				Where("customer_id = ?", task.CustomerID).
				Where("id = ?", task.ID)
			if at != nil {
				update = update.Set("next_run_at = ?", *at)
			} else {
				update = update.Set("state = ?", ScheduledTaskCompleted).Set("next_run_at = NULL").Set("move_paused = false")
			}
			if _, err := update.Exec(ctx); err != nil {
				return fmt.Errorf("store: skip a moved task's occurrences before the cut-over: %w", err)
			}
			if at == nil && missed != nil {
				missed.ID, missed.CreatedAt = newID(), time.Now().UTC()
				// The occurrence's run, if the last catch-up brought one, is the one on record.
				if _, err := tx.NewInsert().Model(missed).On("CONFLICT DO NOTHING").Exec(ctx); err != nil {
					return fmt.Errorf("store: record a moved task's missed occurrence: %w", err)
				}
			}
		}
		return settleSkippedRuns(ctx, tx, customerID, tasks)
	})
	return stack.Wrap(err)
}

// settleSkippedRuns settles a run of an occurrence the cut-over skipped that the source had
// started when the last catch-up read it, as the run of an occurrence the task slept through
// here: nothing here finishes it, and a run left running reads as one a worker is on, keeps a
// completed task from the prune and is never pruned itself. What became of it there never
// reached this copy, so it is written neither delivered nor failed. tasks are as they were
// read, before the cut-over moved their next run.
func settleSkippedRuns(ctx context.Context, tx bun.Tx, customerID string, tasks []ScheduledTask) error {
	if len(tasks) == 0 {
		return nil
	}
	ids, due := make([]string, 0, len(tasks)), make([]time.Time, 0, len(tasks))
	for _, task := range tasks {
		ids, due = append(ids, task.ID), append(due, *task.NextRunAt)
	}
	if _, err := tx.NewUpdate().Model((*ScheduledTaskRun)(nil)).
		Set("state = ?", ScheduledTaskRunMissed).
		Set("error = ?", scheduledTaskRunPaused).
		Set("finished_at = now()").
		Where("customer_id = ?", customerID).
		Where("(task_id, scheduled_for) IN (SELECT * FROM unnest(?::text[], ?::timestamptz[]))",
			pgdialect.Array(ids), pgdialect.Array(due)).
		Where("trigger = ?", ScheduledTaskTriggerSchedule).
		Where("state = ?", ScheduledTaskRunRunning).
		Exec(ctx); err != nil {
		return fmt.Errorf("store: settle the source's runs of moved tasks' skipped occurrences: %w", err)
	}
	return nil
}

// ResumeMovedRows is the cut-over of a move into this deployment: it resumes the customer's
// scheduled tasks a move imported paused because they were active where they came from, and
// clears their mark, so a task the user paused stays paused. A task keeps its next run, which
// SkipOccurrencesBeforeCutOver, run first, has moved past the cut-over, so nothing the source
// delivered is delivered again.
//
// A marked task whose agent config is deleted or missing here has nothing to run on, as the
// API's resume refuses it (errUnknownConfig): it stays paused and loses the mark. The check is
// made here rather than when the config is deleted, since a later import marks the task again
// whenever the source changes it. The config row is locked FOR SHARE, as refuseDeletedConfig
// does, so a delete landing meanwhile either is seen or finds the task active and pauses it.
//
// A user holds at most the config's limit of active tasks on it (AgentScheduling.MaxActive),
// as the API's resume holds them to (errTooManyScheduledTasks). A user who made tasks here
// before the cut-over has less room than the move brought tasks, so each user's marked tasks
// are resumed oldest first while the room lasts. The rest stay paused and keep the mark: it
// says the cut-over still owes them a resume, which running it again pays once the user has
// cancelled or paused tasks here. The room is counted under the lock a create or resume counts
// under (lockTaskLimits), so neither can take a place the cut-over counted as free.
//
// The users are resumed movedHoldersPerTransaction at a time, one transaction each, since each
// takes one lock per user and config and the server holds only so many at once. One that fails
// leaves the ones before it done, which running the cut-over again does not redo, and what
// they did is returned with its error. Each waits
// for a task row or a limit at most the serving connection's lock_timeout (3s): a create or
// resume holds them only for its own transaction, and a cut-over that gives up is run again.
//
// It returns how many tasks it resumed, how many it left paused for want of a config, how many
// for want of room, and the campaigns still running or scheduled there at the last catch-up.
// Those it leaves paused and marked: a contact the source rang after that is still enrolled
// here, and resuming the campaign here (Campaigns.Resume) rings it again, so that is left to a
// person. Any change of a campaign's state here clears the mark (transitionIn). Running it
// again resumes nothing twice.
func (s *Store) ResumeMovedRows(ctx context.Context, customerID string) (resumed, skipped, overLimit int64, campaigns []string, err error) {
	var holders []scheduledTaskHolder
	var rows []struct {
		UserID   string `bun:"user_id"`
		ConfigID string `bun:"config_id"`
	}
	if err := s.global.NewSelect().Model((*ScheduledTask)(nil)).
		Distinct().
		Column("user_id", "config_id").
		Where("customer_id = ?", customerID).
		Where("move_paused").
		Where("state = ?", ScheduledTaskPaused).
		Order("user_id", "config_id").
		Scan(ctx, &rows); err != nil {
		return 0, 0, 0, nil, stack.Wrap(fmt.Errorf("store: moved scheduled tasks: %w", err))
	}
	for _, row := range rows {
		holders = append(holders, scheduledTaskHolder{row.UserID, row.ConfigID})
	}
	for chunk := range slices.Chunk(holders, movedHoldersPerTransaction) {
		var r, k, o int64
		err := s.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			var err error
			r, k, o, err = resumeMovedTasks(ctx, tx, customerID, chunk)
			return err
		})
		if err != nil {
			return resumed, skipped, overLimit, nil, stack.Wrap(err)
		}
		resumed, skipped, overLimit = resumed+r, skipped+k, overLimit+o
	}
	if err := s.global.NewSelect().Model((*Campaign)(nil)).
		Column("id").
		Where("customer_id = ?", customerID).
		Where("move_paused").
		Order("id").
		Scan(ctx, &campaigns); err != nil {
		return resumed, skipped, overLimit, nil, stack.Wrap(fmt.Errorf("store: moved campaigns: %w", err))
	}
	return resumed, skipped, overLimit, campaigns, nil
}

// movedHoldersPerTransaction is how many users on a config one transaction of the cut-over
// resumes. Each takes a limit lock, and the server's lock table holds max_locks_per_transaction
// times max_connections at once (64 times 100 by default) for every transaction together.
var movedHoldersPerTransaction = 500

// scheduledTaskHolder is a user on an agent config, who holds at most its limit of active tasks.
type scheduledTaskHolder struct{ user, config string }

// resumeMovedTasks resumes the marked tasks of some of a customer's users on their configs. It
// reads the limit of each live config they are on and the active tasks each holds on each, in
// one query each. It takes the configs, the task rows, then the limits: the order
// ScheduledTasks.Update takes them in, so a resume of one of these tasks waits for the cut-over,
// or the cut-over for it, rather than each holding what the other waits for. The configs stay
// locked until the transaction ends, so a config edit waits for it. A config deleted or
// missing has no limit. The task rows are read again once locked, so a task resumed or paused
// by hand meanwhile is counted as it now is and not resumed.
func resumeMovedTasks(ctx context.Context, tx bun.Tx, customerID string, holders []scheduledTaskHolder) (resumed, skipped, overLimit int64, err error) {
	// The holders as two arrays read side by side: a list of pairs would be planned as one
	// condition per pair.
	users, configIDs := make([]string, 0, len(holders)), make([]string, 0, len(holders))
	for _, holder := range holders {
		users, configIDs = append(users, holder.user), append(configIDs, holder.config)
	}

	var configs []AgentConfig
	if err := tx.NewSelect().Model(&configs).
		Column("id", "scheduling").
		Where("customer_id = ?", customerID).
		Where("id IN (?)", bun.In(configIDs)).
		Where("deleted_at IS NULL").
		For("SHARE").
		Scan(ctx); err != nil {
		return 0, 0, 0, fmt.Errorf("store: lock the configs of moved scheduled tasks: %w", err)
	}
	limits := map[string]int{}
	for _, config := range configs {
		limits[config.ID] = config.Scheduling.MaxActive()
	}

	// Oldest first, which is the order the room goes in.
	var marked []ScheduledTask
	if err := tx.NewSelect().Model(&marked).
		Column("id", "user_id", "config_id").
		Where("customer_id = ?", customerID).
		Where("(user_id, config_id) IN (SELECT * FROM unnest(?::text[], ?::text[]))", pgdialect.Array(users), pgdialect.Array(configIDs)).
		Where("move_paused").
		Where("state = ?", ScheduledTaskPaused).
		Order("created_at", "id").
		For("NO KEY UPDATE").
		Scan(ctx); err != nil {
		return 0, 0, 0, fmt.Errorf("store: lock moved scheduled tasks: %w", err)
	}
	if len(marked) == 0 {
		return 0, 0, 0, nil
	}
	still := make([]scheduledTaskHolder, 0, len(marked))
	for _, task := range marked {
		still = append(still, scheduledTaskHolder{task.UserID, task.ConfigID})
	}
	if err := lockTaskLimits(ctx, tx, customerID, still); err != nil {
		return 0, 0, 0, err
	}
	users, configIDs = users[:0], configIDs[:0]
	for _, holder := range still {
		users, configIDs = append(users, holder.user), append(configIDs, holder.config)
	}

	var counts []struct {
		UserID   string `bun:"user_id"`
		ConfigID string `bun:"config_id"`
		Count    int    `bun:"count"`
	}
	if err := tx.NewSelect().Model((*ScheduledTask)(nil)).
		Column("user_id", "config_id").
		ColumnExpr("count(*) AS count").
		Where("customer_id = ?", customerID).
		Where("(user_id, config_id) IN (SELECT * FROM unnest(?::text[], ?::text[]))", pgdialect.Array(users), pgdialect.Array(configIDs)).
		Where("state = ?", ScheduledTaskActive).
		Group("user_id", "config_id").
		Scan(ctx, &counts); err != nil {
		return 0, 0, 0, fmt.Errorf("store: count scheduled tasks: %w", err)
	}
	held := map[scheduledTaskHolder]int{}
	for _, count := range counts {
		held[scheduledTaskHolder{count.UserID, count.ConfigID}] = count.Count
	}
	var resume, unmark []string
	for _, task := range marked {
		limit, live := limits[task.ConfigID]
		if !live {
			unmark = append(unmark, task.ID)
			continue
		}
		key := scheduledTaskHolder{task.UserID, task.ConfigID}
		if held[key] >= limit {
			overLimit++
			continue
		}
		held[key]++
		resume = append(resume, task.ID)
	}
	if len(resume) > 0 {
		result, err := tx.NewUpdate().Model((*ScheduledTask)(nil)).
			Set("state = ?", ScheduledTaskActive).
			Set("move_paused = false").
			Set("updated_at = now()").
			Where("customer_id = ?", customerID).
			Where("id IN (?)", bun.In(resume)).
			Where("move_paused").
			Where("state = ?", ScheduledTaskPaused).
			Exec(ctx)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("store: resume moved scheduled tasks: %w", err)
		}
		if resumed, err = result.RowsAffected(); err != nil {
			return 0, 0, 0, fmt.Errorf("store: resume moved scheduled tasks: %w", err)
		}
	}
	if len(unmark) > 0 {
		result, err := tx.NewUpdate().Model((*ScheduledTask)(nil)).
			Set("move_paused = false").
			Set("updated_at = now()").
			Where("customer_id = ?", customerID).
			Where("id IN (?)", bun.In(unmark)).
			Where("move_paused").
			Where("state = ?", ScheduledTaskPaused).
			Exec(ctx)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("store: unmark moved scheduled tasks without a config: %w", err)
		}
		if skipped, err = result.RowsAffected(); err != nil {
			return 0, 0, 0, fmt.Errorf("store: unmark moved scheduled tasks without a config: %w", err)
		}
	}
	return resumed, skipped, overLimit, nil
}

// importableNumber says whether an imported number is written here, and refuses one the API
// would not store. Only a number on the customer's own trunk moves. A bought one is held in
// the vendor account of the deployment that bought it, with that one's line, trunk and use
// case, and its bills move in its requests: written here it would be billed again (Rentals)
// and given back at this deployment's vendor account. So it is not written, and the customer
// keeps it by buying or porting it here.
//
// A number on the customer's own trunk that another customer holds here is left out
// (*NumbersLeftOutError), unless the customer holds it here too: a row the customer wrote before
// that rule is still its own to write again. One given back is only its bill's history. Whether
// the two would share a Stream app is not known here, where the router acts in none, so this is
// stricter than adding it to a trunk (PhoneNumbers.InsertOnSIPTrunk), which the customer can do
// once the move is over. Refused rather than left out, the number would stop the whole move, and
// every run after it, on a row another app's number decides.
func importableNumber(ctx context.Context, db bun.IDB, customerID string, row json.RawMessage) (bool, error) {
	// Read the way the insert reads it, by exact column name: a JSON decoder also matches a
	// key in another case, which the insert then leaves out.
	var e164, vendor sql.NullString
	var cost sql.NullInt64
	var released bool
	if err := db.QueryRowContext(ctx,
		"SELECT e164, vendor, monthly_cost_micros, released_at IS NOT NULL FROM jsonb_populate_record(NULL::phone_numbers, ?::jsonb)",
		string(row)).Scan(&e164, &vendor, &cost, &released); err != nil {
		if refused := refusedRow("phone_numbers", err); refused != nil {
			return false, refused
		}
		return false, fmt.Errorf("store: read an imported number: %w", err)
	}
	if vendor.String != sipTrunkVendor {
		return false, nil
	}
	number := PhoneNumber{CustomerID: customerID, E164: e164.String, Vendor: vendor.String, MonthlyCostMicros: cost.Int64}
	if err := number.Validate(); err != nil {
		return false, stack.Wrap(err)
	}
	if released {
		return true, nil
	}
	held, err := holders(ctx, db, number.E164)
	if err != nil {
		return false, err
	}
	if len(held) > 0 && !slices.Contains(held, customerID) {
		return false, stack.Wrap(&NumbersLeftOutError{Numbers: []string{number.E164}})
	}
	return true, nil
}

// boughtHere is the condition that a number, as the given relation names it, was bought here
// and is still held: no import writes one (importableNumber), so none may update or delete it.
func boughtHere(relation string) string {
	return fmt.Sprintf("(%[1]s.released_at IS NULL AND %[1]s.vendor <> '%[2]s')", relation, sipTrunkVendor)
}

// importableDefinition refuses an imported definition the API would not have stored, and gives the row the manifest as
// the API would have stored it, so no field this release does not read arrives for one that does. Every imported
// definition is a custom one, since the customer is forced onto the row (identity) and importRow refuses an import with
// no customer, and only the API's rules keep a custom manifest from naming the operator's client or anything else the
// router would trust a built-in with (core.Manifest.AsCustom).
func importableDefinition(row json.RawMessage) (json.RawMessage, error) {
	// Read by exact column name, as the insert reads the row: a JSON decoder also matches a
	// key in another case, which the insert then leaves out.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(row, &fields); err != nil {
		return nil, invalidImport(fmt.Errorf("store: read an imported connector definition: %w", err))
	}
	var id string
	if raw, ok := fields["id"]; ok {
		if err := json.Unmarshal(raw, &id); err != nil {
			return nil, invalidImport(fmt.Errorf("store: read an imported connector definition's id: %w", err))
		}
	}
	var manifest core.Manifest
	if raw, ok := fields["manifest"]; ok {
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return nil, invalidImport(fmt.Errorf("store: read the manifest of imported connector definition %q: %w", id, err))
		}
	}
	custom, err := manifest.AsCustom(id)
	if err != nil {
		return nil, fmt.Errorf("store: imported connector definition %q: %w: %w", id, ErrNotACustomConnector, err)
	}
	// A row from a release before sync hashes carries none, as importRow's COALESCE reads it.
	var hash string
	if raw, ok := fields["sync_hash"]; ok && json.Unmarshal(raw, &hash) != nil || len(hash) > SyncHashMaxLength || !syncHash.MatchString(hash) {
		return nil, invalidImport(fmt.Errorf("store: imported connector definition %q: sync_hash is not %s of at most %d characters",
			id, SyncHashDescription, SyncHashMaxLength))
	}
	// The columns a caller is shown are the manifest's, as the API writes them (saveRevision), so
	// they are held to its limits too.
	for column, value := range map[string]any{"manifest": custom, "name": custom.Name, "category": custom.Category,
		"description": custom.Description} {
		if fields[column], err = json.Marshal(value); err != nil {
			return nil, stack.Wrap(err)
		}
	}
	return json.Marshal(fields)
}

// SkippedRow names a row an import leaves out rather than writes, as rowKey does, and is "" for
// any other row: a custom connector definition with oauth2_client_credentials, which custom
// connectors could have before they lost the scheme. No connection could use one, since the
// scheme needs a token endpoint a custom connector cannot set (core.CustomManifest), so it
// should not stop a move as importableDefinition's refusal would.
func SkippedRow(table string, row json.RawMessage) string {
	if table != "connector_definitions" {
		return ""
	}
	// Read by exact column name, as importableDefinition reads it, which refuses a row this
	// cannot read.
	var fields map[string]json.RawMessage
	var manifest core.Manifest
	if json.Unmarshal(row, &fields) != nil || json.Unmarshal(fields["manifest"], &manifest) != nil ||
		!slices.Contains(manifest.Schemes, "oauth2_client_credentials") {
		return ""
	}
	// A row is named only by a key the API could have written: anything else is refused by
	// importableDefinition as before, and never held in a list or echoed back.
	var id string
	var revision int64
	if json.Unmarshal(fields["id"], &id) != nil || !skippedID.MatchString(id) ||
		json.Unmarshal(fields["revision"], &revision) != nil || revision < 1 {
		return ""
	}
	key, err := rowKey(table, []string{"id", "revision"}, row)
	if err != nil {
		return ""
	}
	return key
}

var skippedID = regexp.MustCompile(core.CustomIDPattern)

// identity is what the importing deployment overwrites on every row of this table, which
// is whose it is. A table holding more than one kind of row has the kind forced too, so
// an app-scope policy cannot be imported as its organization's.
func (t dataTable) identity() string {
	if t.name == "policies" {
		return "jsonb_build_object('scope_id', ?::text, 'scope', 'app')"
	}
	// A connector connection arrives without its credentials, so it arrives saying it needs
	// one, rather than connected with nothing to connect with. Key version 0 and no
	// expiry are what a connection without credentials holds,
	// so the source credentials' key version and expiry do not come along either, on an import or
	// on every change applied after it. Nor does its provider unit: a row in an import is
	// whatever the importer wrote, and the column still holds one unit per connector
	// (connector_connections_provider_unit_idx), so it is emptied here.
	if t.name == "connector_connections" {
		return "jsonb_build_object('customer_id', ?::text, 'status', '" + ConnectionNeedsReauthorization + "'," +
			" 'credentials_kek_version', 0, 'expires_at', NULL, 'provider_unit_id', NULL)"
	}
	// A trunk arrives without its password, since the key that sealed it is the other
	// deployment's, so it arrives at key version 0, which is what a trunk without one
	// holds.
	if t.name == "sip_trunks" {
		return "jsonb_build_object('customer_id', ?::text, 'password_kek_version', 0)"
	}
	// Nor does an attached number arrive with where its calls go: the trunk, route and call it was attached to are named by whatever
	// the importer wrote, and a hook that finds one call under two customers' numbers answers
	// it for neither (ErrAmbiguousHook). It arrives needing to be attached again. Nor with its
	// use case, which names one of the source's.
	if t.name == "phone_numbers" {
		return "jsonb_build_object('customer_id', ?::text, 'stream_trunk_id', NULL, 'stream_route_id', NULL," +
			" 'stream_call_id', NULL, 'stream_call_type', NULL, 'attached_at', NULL, 'dlc_use_case_id', NULL)"
	}
	// A request was billed by the deployment that recorded it, so this one never ships it, and
	// what that one's usage service answered for it is not this one's: a reship of the rows
	// this one shipped or had refused leaves it alone. Nor was it drawn from a grant here,
	// whose ids are this deployment's own.
	// Nor is it billed here, and it counts against the importing app's organization alone.
	if t.name == "requests" {
		return "jsonb_build_object('customer_id', ?::text, 'to_ship', false, 'shipped_at', NULL, 'ship_error', '', 'limit_grants', '{}'::bigint[])" +
			" || jsonb_build_object('billable', false)" +
			" || jsonb_build_object('organization_id', ?::text)"
	}
	return fmt.Sprintf("jsonb_build_object('%s', ?::text)", t.customer)
}

// owned is the condition that a row of this table, as the given relation names it,
// belongs to the customer the query's next placeholder carries.
func (t dataTable) owned(relation string) string {
	if t.parent != "" {
		return t.belonging().owned(relation, "")
	}
	return fmt.Sprintf("%s.%s = ?", relation, t.customer)
}

// belonging is the reference of a row of a table with a parent to the row it belongs to.
func (t dataTable) belonging() dataReference { return dataReference{t.parentColumn, t.parent} }

// owned is the condition that the row this reference names, in the given relation, belongs to
// the customer the query's next placeholder carries. locking is a locking clause for that row.
func (r dataReference) owned(relation, locking string) string {
	named, _ := dataTableByName(r.table)
	owner := fmt.Sprintf("%s.%s = ?", r.table, named.customer)
	if named.customer == "" {
		owner = named.belonging().owned(r.table, "")
	}
	return fmt.Sprintf("EXISTS (SELECT 1 FROM %[1]s WHERE %[1]s.id = %[2]s.%[3]s AND %[4]s%[5]s)",
		r.table, relation, r.column, owner, locking)
}

// held is owned with the named row locked until the statement's transaction ends, for a row
// about to be written: the foreign key then looks for the id again, whoever's row it is by
// then. KEY SHARE is the lock the foreign key takes itself.
func (r dataReference) held(relation string) string {
	return r.owned(relation, " FOR KEY SHARE OF "+r.table)
}

// conflictTarget is the index a row of this table is known by across deployments: what an
// import resolves a conflict on. The one a copied row is known by holds only copied rows.
func (t dataTable) conflictTarget(s shape) string {
	switch {
	case t.across != "":
		return "(" + t.across + ")"
	case t.copied != "":
		return fmt.Sprintf("(%s, %s) WHERE %s IS NOT NULL", t.customer, t.copied, t.copied)
	}
	return "(" + strings.Join(s.keys, ", ") + ")"
}

// copiedBefore is the condition that the row r, about to be imported with the given columns,
// is here already, with every column it writes the same, the customer included: copied here by
// an import from before the copied column, under the source's id, and so with the same id and
// no source id; or the very row r was copied from, which r names by its source id, when a copy
// is taken back to where it came from. A row that differs in any of them, when it was ingested
// included, is another log here that holds the number, and r is copied beside it.
func (t dataTable) copiedBefore(s shape, columns []string) string {
	same := make([]string, 0, len(s.keys))
	for _, key := range s.keys {
		same = append(same, fmt.Sprintf("h.%s = r.%s", key, key))
	}
	before := fmt.Sprintf("h.%s IS NULL AND %s", t.copied, strings.Join(same, " AND "))
	back := fmt.Sprintf("h.%s = r.%s", s.keys[0], t.copied)
	var here, imported []string
	for _, column := range columns {
		if column != t.copied {
			here, imported = append(here, "h."+column), append(imported, "r."+column)
		}
	}
	return fmt.Sprintf("EXISTS (SELECT 1 FROM %s AS h WHERE ((%s) OR %s) AND (%s) IS NOT DISTINCT FROM (%s))",
		t.name, before, back, strings.Join(here, ", "), strings.Join(imported, ", "))
}

// captured are the columns a change records of a row. A row known by another column than its
// primary key records both: the primary key is what a row without the other is found by.
func (t dataTable) captured(s shape) []string {
	if t.across != "" {
		return append([]string{t.across}, s.keys...)
	}
	return s.keys
}

// importedUsageID is the usage id a request is imported under, as SQL, made from the customer
// importing it and the request's usage id where it was first recorded (its id in the source, for
// one from before usage ids), so importing it again and replaying its delete name the same row.
func importedUsageID(customer, sourceID string) string {
	return fmt.Sprintf("md5('import:' || %s || ':' || %s)::uuid", customer, sourceID)
}

func (t dataTable) isKey(s shape, column string) bool {
	for _, key := range s.keys {
		if key == column {
			return true
		}
	}
	return false
}

// Changes returns what has happened to a customer's rows since a cursor, oldest first,
// and the cursor to ask from next time.
//
// A cursor older than what is still kept, than where the recording last started again after
// it expired, or than the last change of theirs pruned, is ErrChangesExpired rather than a
// shorter answer: the caller would otherwise carry on from a gap it could not see, and a
// missing change is a row that is wrong on the other side forever.
func (s *Store) Changes(ctx context.Context, customerID string, after int64, limit int) ([]DataChange, int64, error) {
	if customerID == "" {
		return nil, 0, errors.New("store: a customer id is required")
	}
	if limit <= 0 {
		limit = 500
	}

	// Read first, so the statements after it see every change at or below it.
	mark, err := s.changeWatermark(ctx)
	if err != nil {
		return nil, 0, err
	}

	// Sequence numbers are shared by every customer and pruning drops the oldest of all of
	// them, so a gap before this customer's first change is the other customers' changes,
	// not a pruned one: only a cursor older than the oldest change kept has lost any. A cursor
	// at recorded_since is a start, as 0 is, and the gap after it is not checked: a number rolled
	// back after it leaves the same gap. With none of theirs kept, a quiet customer and one whose
	// every change aged out look the same to this check. The highest number of theirs pruning
	// dropped (pruned_through) tells them apart: a cursor below it lost a change, at
	// recorded_since or after it, whatever is kept. A cursor at pruned_through is a start too: no
	// change of theirs above it was pruned, or pruned_through would be higher, so the gap after it
	// is other customers' pruned changes or numbers that were never a change. Pruning drops only
	// numbers at or below the mark it read, so the gap right after a cursor at or above this mark
	// is a change still in flight rather than a pruned one. The mark can go down: the trigger
	// reads the sequence and takes its lock in two statements, and a mark read between them
	// is above that lock. A prune in that window can drop an aged-out change before this
	// check sees it, so the follower is refused (ErrChangesExpired) on a later read, not
	// handed a gap.
	//
	// This check and the rows are one snapshot: a prune between two statements would drop
	// rows after the cursor, and the rows read would then start above them with the check
	// already passed. The mark is read before the transaction, as in an export, so every
	// change at or below it has ended before the snapshot is taken.
	tx, err := s.global.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, 0, fmt.Errorf("store: changes: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // read-only, and the error on the way out says nothing

	since, pruned, err := recordedSince(ctx, tx, customerID)
	if err != nil {
		return nil, 0, err
	}
	if after < since || after < pruned {
		return nil, 0, ErrChangesExpired
	}
	var kept sql.NullInt64
	var theirs bool
	if err := tx.QueryRowContext(ctx,
		"SELECT (SELECT MIN(seq) FROM data_changes), EXISTS (SELECT 1 FROM data_changes WHERE customer_id = ?)",
		customerID).Scan(&kept, &theirs); err != nil {
		return nil, 0, fmt.Errorf("store: changes: %w", err)
	}
	if after > max(since, pruned) && after < mark && theirs && kept.Int64 > after+1 {
		return nil, 0, ErrChangesExpired
	}

	// A policy is recorded under its scope id whatever its scope, so an organization's sharing the
	// customer's id is left out, as an export leaves it out. It is left out before the limit, so a
	// page is as full as the customer's own changes make it.
	rows, err := tx.QueryContext(ctx, `
SELECT seq, table_name, op, key, payload, at
FROM data_changes
WHERE customer_id = ? AND seq > ? AND seq <= ? AND (table_name <> 'policies' OR key ->> 'scope' = 'app')
ORDER BY seq
LIMIT ?`, customerID, after, mark, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("store: changes: %w", err)
	}
	defer rows.Close()

	changes := []DataChange{}
	cursor := after
	for rows.Next() {
		var change DataChange
		var payload []byte
		if err := rows.Scan(&change.Seq, &change.Table, &change.Op, &change.Key, &payload, &change.At); err != nil {
			return nil, 0, fmt.Errorf("store: changes: %w", err)
		}
		change.Payload = payload
		if change.Op != ChangeDelete && len(payload) > 0 {
			if change.Payload, err = s.exportPin(change.Table, payload); err != nil {
				return nil, 0, err
			}
		}
		changes = append(changes, change)
		cursor = change.Seq
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("store: changes: %w", err)
	}
	// A page that is not full holds every change of theirs up to the mark, so the cursor moves past
	// the ones left out too: one left ahead of it would be pruned in time and expire the cursor.
	if len(changes) < limit {
		if err := tx.QueryRowContext(ctx,
			"SELECT COALESCE(MAX(seq), ?) FROM data_changes WHERE customer_id = ? AND seq > ? AND seq <= ?",
			cursor, customerID, cursor, mark).Scan(&cursor); err != nil {
			return nil, 0, fmt.Errorf("store: changes: %w", err)
		}
	}
	return changes, cursor, nil
}

// ApplyChanges replays changes onto this deployment, in one transaction so a batch either
// arrives or does not. With shards there is one for the shared tables and one for this
// shard's, each replayed in order, as no row of one points at a row of the other. A number
// left out (*NumbersLeftOutError) is answered once the rest has been written.
func (s *Store) ApplyChanges(ctx context.Context, customerID string, changes []DataChange) error {
	if customerID == "" {
		return errors.New("store: a customer id is required")
	}
	if !s.Sharded() {
		return s.applyChanges(ctx, s.global, customerID, changes)
	}
	var shared, sharded []DataChange
	for _, change := range changes {
		if spec, _ := dataTableByName(change.Table); spec.shard {
			sharded = append(sharded, change)
		} else {
			shared = append(shared, change)
		}
	}
	left := s.applyChanges(ctx, s.global, customerID, shared)
	if _, ok := errors.AsType[*NumbersLeftOutError](left); left != nil && !ok {
		return left
	}
	if err := s.applyChanges(ctx, s.shard, customerID, sharded); err != nil {
		return err
	}
	return left
}

func (s *Store) applyChanges(ctx context.Context, db *bun.DB, customerID string, changes []DataChange) error {
	if len(changes) == 0 {
		return nil
	}
	var stale, left []string
	_, err := s.cachedWrite(ctx, db, func(ctx context.Context, tx bun.Tx) error {
		if err := lockNumbers(ctx, tx, changes); err != nil {
			return err
		}
		var configs, defined []json.RawMessage
		for _, change := range changes {
			if change.Skipped() {
				continue
			}
			switch change.Op {
			case ChangeDelete:
				if err := s.deleteRow(ctx, tx, customerID, change, &stale); err != nil {
					return err
				}
			default:
				err := s.importRow(ctx, tx, customerID, change.Table, change.Payload, &stale)
				if leftOut, ok := errors.AsType[*NumbersLeftOutError](err); ok {
					left = append(left, leftOut.Numbers...)
					continue
				}
				if err != nil {
					return err
				}
				switch change.Table {
				case "agent_configs":
					configs = append(configs, change.Payload)
				case "connector_definitions":
					defined = append(defined, change.Payload)
				}
			}
		}
		// Weighed once the page is written, so a parent and its child are weighed as the page
		// leaves them, whichever of them came first.
		if err := s.refuseImportedConfigs(ctx, tx, customerID, configs); err != nil {
			return err
		}
		return s.refuseImportedDefinitions(ctx, tx, customerID, defined)
	}, func() []string { return stale }, func() bool { return len(stale) > 0 })
	s.Users.forgetKeys(stale)
	if err != nil {
		return err
	}
	if len(left) > 0 {
		return stack.Wrap(&NumbersLeftOutError{Numbers: left})
	}
	return nil
}

// lockNumbers takes, up front and in one order, the lock under which each number the changes
// write is checked against who else holds it (holders), which importRow then takes again. Taken
// one change at a time, in the changes' order, two batches naming two numbers in opposite orders
// would each wait for the other until Postgres aborted one. A number is read by its exact key,
// as the insert reads it.
func lockNumbers(ctx context.Context, tx bun.Tx, changes []DataChange) error {
	var numbers []string
	for _, change := range changes {
		if change.Table != "phone_numbers" || change.Op == ChangeDelete {
			continue
		}
		var fields map[string]json.RawMessage
		var e164 string
		if json.Unmarshal(change.Payload, &fields) != nil || json.Unmarshal(fields["e164"], &e164) != nil || e164 == "" {
			// A row that does not read is refused by importRow, which reads it again.
			continue
		}
		numbers = append(numbers, e164)
	}
	if len(numbers) == 0 {
		return nil
	}
	slices.Sort(numbers)
	numbers = slices.Compact(numbers)
	// unnest reads the array in order, so the locks are taken in it.
	if _, err := tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock(hashtextextended(number, ?)) FROM unnest(?::text[]) AS number",
		numberHoldersLockSeed, pgdialect.Array(numbers)); err != nil {
		return stack.Wrap(fmt.Errorf("store: lock the holders of numbers: %w", err))
	}
	return nil
}

// Skipped is whether ApplyChanges leaves the change out: one to a policy of another scope than an
// app's. The trigger records a policy under its scope id whatever its scope, so an organization's
// that shares an id with the app is recorded under the app, and replaying it would write it over
// the app's (identity).
func (change DataChange) Skipped() bool {
	if change.Table != "policies" {
		return false
	}
	var key struct {
		Scope string `json:"scope"`
	}
	return json.Unmarshal(change.Key, &key) != nil || key.Scope != string(ScopeApp)
}

// deleteRow removes what a delete change names, scoped to the customer replaying it so a
// change carrying somebody else's key cannot reach their row. It adds to stale the cached values
// the row was read through.
func (s *Store) deleteRow(ctx context.Context, db bun.IDB, customerID string, change DataChange, stale *[]string) error {
	if slices.Contains(retiredTables, change.Table) {
		return nil
	}
	spec, found := dataTableByName(change.Table)
	if !found {
		return invalidImport(fmt.Errorf("store: %q is not a table an export carries", change.Table))
	}
	shape, err := s.shapeOf(ctx, change.Table)
	if err != nil {
		return err
	}

	var conditions []string
	var args []any
	if spec.across != "" {
		// Never by the id, which names another row here: by the usage id the row was imported
		// under, made from the one the change carries, or from its source id for a row from
		// before usage ids.
		conditions = append(conditions, fmt.Sprintf("t.%s = %s", spec.across, importedUsageID("?::text",
			fmt.Sprintf("COALESCE((?::jsonb ->> '%s')::uuid::text, ?::jsonb ->> 'id')", spec.across))))
		args = append(args, customerID, string(change.Key), string(change.Key))
	} else if spec.copied != "" {
		// Never by the id either: by the source's id, which a copied row keeps. A row copied
		// before the copied column holds none and is left, as the id it holds may as well be
		// a log of this deployment's.
		conditions = append(conditions, fmt.Sprintf("t.%s = (?::jsonb ->> '%s')::bigint", spec.copied, shape.keys[0]))
		args = append(args, string(change.Key))
	} else {
		for _, column := range shape.keys {
			// The customer in the key is the source's, which a move under another id does not
			// keep: the customer's own condition below is what scopes the delete.
			if column == spec.customer {
				continue
			}
			// Read as the column's own type, so a time matches however its JSON writes it.
			conditions = append(conditions, fmt.Sprintf("t.%[1]s = (jsonb_populate_record(NULL::%[2]s, ?::jsonb)).%[1]s", column, spec.name))
			args = append(args, string(change.Key))
		}
	}
	if spec.parent != "" {
		// Two customers may each have a parent of one id: a row naming its customer is only the
		// one of the customer replaying it.
		if spec.customer != "" {
			conditions = append(conditions, fmt.Sprintf("(t.%[1]s IS NULL OR t.%[1]s = ?)", spec.customer))
			args = append(args, customerID)
		}
		conditions = append(conditions, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM %s AS p WHERE p.id = t.%s AND p.customer_id = ?)",
			spec.parent, spec.parentColumn))
	} else {
		conditions = append(conditions, fmt.Sprintf("t.%s = ?", spec.customer))
		if spec.filter != "" {
			conditions = append(conditions, spec.filter)
		}
	}
	if spec.name == "phone_numbers" {
		conditions = append(conditions, "NOT "+boughtHere("t"))
	}
	args = append(args, customerID)

	query := fmt.Sprintf("DELETE FROM %s AS t WHERE %s", spec.name, strings.Join(conditions, " AND "))
	if !cachedByID(change.Table) {
		result, err := db.ExecContext(ctx, query, args...)
		if err != nil {
			if isViolation(err, foreignKeyViolation) {
				return stack.Wrap(invalidImport(marked{fmt.Errorf("store: delete %s: another row here still names this row", change.Table), err}))
			}
			if refused := refusedRow(change.Table, err); refused != nil {
				return refused
			}
			return fmt.Errorf("store: delete %s: %w", change.Table, err)
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: delete %s: %w", change.Table, err)
		}
		if deleted > 0 {
			*stale = append(*stale, cachedKeys(customerID, change.Table, change.Key)...)
		}
		return nil
	}
	rows, err := db.QueryContext(ctx, query+" RETURNING "+cachedRow(change.Table, "t"), args...)
	if err != nil {
		if refused := refusedRow(change.Table, err); refused != nil {
			return refused
		}
		return fmt.Errorf("store: delete %s: %w", change.Table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var row []byte
		if err := rows.Scan(&row); err != nil {
			return fmt.Errorf("store: delete %s: %w", change.Table, err)
		}
		*stale = append(*stale, cachedKeys(customerID, change.Table, row)...)
	}
	if err := rows.Err(); err != nil {
		if refused := refusedRow(change.Table, err); refused != nil {
			return refused
		}
		return fmt.Errorf("store: delete %s: %w", change.Table, err)
	}
	return nil
}

// PruneDataChanges drops the changes nobody can still use. It returns how many it removed.
// A capture's row is kept after it expires: it is what tells a start after it from a first one
// (StartDataCapture), one small row per customer that ever moved. It also notes there the highest
// number of the customer's it dropped (pruned_through), in the same statement, so a cursor below
// it is refused (Changes) whatever is kept after it.
//
// It drops only the oldest numbers, the ones below the first change still young and at
// or below the watermark, so what is kept is everything from one number on and a cursor
// before it is told it expired (Changes). By age alone it could keep a lower number and
// drop a higher one: a change's time is when its transaction started, not when it was
// numbered, and one still in flight is not seen at all.
func (s *Store) PruneDataChanges(ctx context.Context, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, nil
	}
	mark, err := s.changeWatermark(ctx)
	if err != nil {
		return 0, err
	}
	// LEAST ignores a NULL: with no change still young, the mark is the only bound. Every router
	// prunes, and of two prunes at once the one that dropped the lower numbers can commit last, so
	// pruned_through only goes up. A WITH statement that changes rows runs once and to the end
	// whether or not the query reads it (https://www.postgresql.org/docs/current/queries-with.html).
	//
	// The lowest numbers go first, writeBatch of them per statement, so a backlog is pruned in
	// statements that each finish inside the statement limit, and pruned_through still says
	// how far the pruning got.
	var removed int64
	for {
		var batch int64
		err = s.global.QueryRowContext(ctx, `
WITH pruned AS (
  DELETE FROM data_changes WHERE seq IN (
    SELECT seq FROM data_changes
    WHERE at < now() - make_interval(secs => ?)
      AND seq <= LEAST(?, (SELECT MIN(seq) - 1 FROM data_changes WHERE at >= now() - make_interval(secs => ?)))
    ORDER BY seq LIMIT ?)
  RETURNING customer_id, seq
), noted AS (
  UPDATE data_change_capture AS c SET pruned_through = p.seq
  FROM (SELECT customer_id, MAX(seq) AS seq FROM pruned GROUP BY customer_id) AS p
  WHERE c.customer_id = p.customer_id AND c.pruned_through < p.seq
)
SELECT count(*) FROM pruned`,
			retention.Seconds(), mark, retention.Seconds(), writeBatch).Scan(&batch)
		if err != nil {
			return removed, fmt.Errorf("store: prune data changes: %w", err)
		}
		removed += batch
		if batch < int64(writeBatch) {
			return removed, nil
		}
	}
}

// watchDataChanges attaches the recording trigger to every table of the set an export carries,
// connecting with dsn.
//
// Changes are only recorded for a customer StartDataCapture marked, which it does in the shared
// database, so a shard's own tables record none, and a deployment with shards (HasShards)
// refuses to be a move's source rather than lose what its shards write after the export.
//
// It runs on every Migrate rather than once in a migration, so a table added later is
// covered by the deployment reaching it rather than by somebody remembering to write the
// trigger by hand. Replacing a trigger that is already right costs a catalogue write at
// startup and nothing afterwards.
func (s *Store) watchDataChanges(ctx context.Context, dsn string, set migrationSet) error {
	// Not the migration's pool, which has no lock limit: a trigger waiting behind a long write
	// would hold every write queued behind it.
	db, err := connect(dsn, 1)
	if err != nil {
		return err
	}
	defer db.Close() //nolint:errcheck // a pool the triggers are done with
	for _, table := range dataTables {
		if table.shard != (set.name == shardSet.name) {
			continue
		}
		shape, err := s.shapeOf(ctx, table.name)
		if err != nil {
			return err
		}
		arguments := []string{"'" + table.customer + "'", "''", "''"}
		if table.parent != "" {
			arguments = []string{"'parent'", "'" + table.parentColumn + "'", "'" + table.parent + "'"}
		}
		arguments = append(arguments, "'{"+strings.Join(shape.secrets(), ",")+"}'")
		for _, key := range table.captured(shape) {
			arguments = append(arguments, "'"+key+"'")
		}

		query := fmt.Sprintf(`
CREATE OR REPLACE TRIGGER data_changes_recorded
AFTER INSERT OR UPDATE OR DELETE ON %s
FOR EACH ROW EXECUTE FUNCTION record_data_change(%s)`, table.name, strings.Join(arguments, ", "))
		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("store: watch %s: %w", table.name, err)
		}
	}
	return nil
}

// checkDataChangesWatched refuses a database where a table of the set an export carries has no
// recording trigger, or has one that records a column isSecret leaves out, unless a later
// release has migrated: it may have added the column and be about to attach its trigger, and
// no router of this release writes it. The trigger's other arguments are not compared: the
// later release's may be there already.
func (s *Store) checkDataChangesWatched(ctx context.Context, set migrationSet, later bool) error {
	for _, table := range dataTables {
		if table.shard != (set.name == shardSet.name) {
			continue
		}
		shape, err := s.shapeOf(ctx, table.name)
		if err != nil {
			return err
		}
		// The arguments are kept each ending in a zero byte, which escape writes as \000.
		var attached sql.NullString
		if err := s.sideOf(table).QueryRowContext(ctx, `
SELECT (SELECT encode(tgargs, 'escape') FROM pg_trigger
  WHERE tgrelid = to_regclass(?) AND tgname = 'data_changes_recorded' AND tgenabled IN ('O', 'A'))`,
			table.name).Scan(&attached); err != nil {
			return fmt.Errorf("store: check the trigger of %s: %w", table.name, err)
		}
		if !attached.Valid {
			return fmt.Errorf("store: changes of %s are not recorded: run router migrate with this release", table.name)
		}
		arguments := strings.Split(attached.String, `\000`)
		var left []string
		if len(arguments) > 3 {
			left = strings.Split(strings.Trim(arguments[3], "{}"), ",")
		}
		for _, secret := range shape.secrets() {
			if !later && !slices.Contains(left, secret) {
				return fmt.Errorf("store: recorded changes of %s would hold %s: run router migrate with this release", table.name, secret)
			}
		}
	}
	return nil
}

// shapeOf reads a table's columns and primary key out of the catalogue, once.
func (s *Store) shapeOf(ctx context.Context, table string) (shape, error) {
	s.shapes.mu.Lock()
	defer s.shapes.mu.Unlock()

	if known, found := s.shapes.byName[table]; found {
		return known, nil
	}
	spec, known := dataTableByName(table)
	if !known {
		return shape{}, fmt.Errorf("store: %q is not a table an export carries", table)
	}

	read := shape{sequences: map[string]string{}}
	// pg_get_serial_sequence finds the sequence of a serial and of an identity column alike.
	rows, err := s.sideOf(spec).QueryContext(ctx, `
SELECT a.attname,
       EXISTS (SELECT 1 FROM pg_index i
               WHERE i.indrelid = a.attrelid AND i.indisprimary AND a.attnum = ANY (i.indkey)) AS is_key,
       pg_get_serial_sequence(?, a.attname)
FROM pg_attribute a
WHERE a.attrelid = ?::regclass AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
ORDER BY a.attnum`, table, table)
	if err != nil {
		return shape{}, fmt.Errorf("store: shape of %s: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var column string
		var isKey bool
		var sequence sql.NullString
		if err := rows.Scan(&column, &isKey, &sequence); err != nil {
			return shape{}, fmt.Errorf("store: shape of %s: %w", table, err)
		}
		read.columns = append(read.columns, column)
		if isKey {
			read.keys = append(read.keys, column)
		}
		if sequence.Valid {
			read.sequences[column] = sequence.String
		}
	}
	if err := rows.Err(); err != nil {
		return shape{}, fmt.Errorf("store: shape of %s: %w", table, err)
	}
	if len(read.keys) == 0 {
		return shape{}, fmt.Errorf("store: %s has no primary key to import against", table)
	}

	if s.shapes.byName == nil {
		s.shapes.byName = map[string]shape{}
	}
	s.shapes.byName[table] = read
	return read, nil
}

// insertable is every column an import writes: the whole row but the secrets, which are
// left to the local default so a connection arrives needing to be authorized again
// rather than arriving with somebody else's token in it, and but the table's vouched
// columns, which only this deployment sets.
func (s shape) insertable(table dataTable) []string {
	columns := make([]string, 0, len(s.columns))
	for _, column := range s.columns {
		if !isSecret(column) && !slices.Contains(table.vouched, column) {
			columns = append(columns, column)
		}
	}
	return columns
}

// written are the columns an import writes of a row of this table.
func (t dataTable) written(s shape) []string {
	columns := s.insertable(t)
	if t.across != "" || t.copied != "" {
		// The id is the destination's to draw: the source's may be one of its own rows'.
		columns = slices.DeleteFunc(columns, func(column string) bool { return t.isKey(s, column) })
	}
	return columns
}

func dataTableByName(name string) (dataTable, bool) {
	for _, table := range dataTables {
		if table.name == name {
			return table, true
		}
	}
	return dataTable{}, false
}

// HoldingTable names the first table an export carries that holds a row of the customer's in the
// shared database, empty when none does. A table kept in the shards is left out: it holds the
// customer's own traffic here, and a shard's pod reads only its own shard, so counting it would
// make the answer depend on which pod runs. A table that belongs to a customer through a parent
// is left out too: its rows are written only under a parent of the customer's. So are opt-outs:
// an import keeps one recorded here live (importRow), so an app holding only those holds nothing
// an export would write over.
func (s *Store) HoldingTable(ctx context.Context, customerID string) (string, error) {
	for _, table := range dataTables {
		if table.customer == "" || (table.shard && !table.moving) || table.name == "opt_outs" {
			continue
		}
		where := table.customer + " = ?"
		if table.filter != "" {
			where += " AND " + table.filter
		}
		var holds bool
		query := "SELECT EXISTS (SELECT 1 FROM " + table.name + " WHERE " + where + ")"
		if err := s.global.QueryRowContext(ctx, query, customerID).Scan(&holds); err != nil {
			return "", fmt.Errorf("store: holds %s: %w", table.name, err)
		}
		if holds {
			return table.name, nil
		}
	}
	return "", nil
}
