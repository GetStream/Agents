package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

type AgentLog struct {
	Cursor string `bun:"-" json:"cursor"`
	// Mark is where a reader who read up to this row is, which Cursor encodes.
	Mark          AgentLogMark `bun:"-" json:"-"`
	bun.BaseModel `bun:"table:agent_logs,alias:l" json:"-"`
	ID            int64          `bun:"id,pk,autoincrement" json:"id,string"`
	CustomerID    string         `json:"-"`
	ConfigID      string         `json:"config_id"`
	AgentID       string         `json:"agent_id"`
	SessionID     string         `json:"session_id"`
	UserID        string         `json:"user_id,omitempty"`
	Source        string         `json:"source"`
	Severity      string         `json:"severity"`
	EventType     string         `json:"event_type"`
	Message       string         `json:"message"`
	OccurredAt    time.Time      `json:"occurred_at"`
	IngestedAt    time.Time      `json:"ingested_at"`
	Details       map[string]any `bun:"details,type:jsonb" json:"details,omitempty"`
}

// Validate refuses a log row no reader could place: every row names its customer, where it
// came from, a level a filter knows, and what kind of event it is.
func (l *AgentLog) Validate() error {
	return validate("agent log").
		required("customer_id", l.CustomerID).
		required("source", l.Source).
		oneOf("severity", l.Severity, severityOrder...).
		required("event_type", l.EventType).
		done()
}

// ErrNoAgentLog says a customer holds no log row by that id.
var ErrNoAgentLog = errors.New("store: no such log row")

// severityOrder is the levels a log is written at, least serious first. A
// filter names the least serious level it wants and gets that one and everything
// above it, so asking for warnings is asking to be shown the problems without
// the running commentary.
var severityOrder = []string{"info", "warn", "error"}

// severitiesFrom is the levels a filter admits. An empty filter admits all of
// them, and so does one naming a level nothing is written at, which is the
// reading that shows too much rather than too little.
func severitiesFrom(least string) []string {
	if least == "" {
		return nil
	}
	for i, level := range severityOrder {
		if level == least {
			return severityOrder[i:]
		}
	}
	return nil
}

// AgentLogFilter picks a customer's log rows.
type AgentLogFilter struct {
	// Severity is the least serious level to show, not the only one: see
	// severitiesFrom.
	ConfigID, SessionID, UserID, Severity, Search string
	Sources                                       []string
	From, To                                      time.Time
	// Before and After bound the ids of each range they name; a range they do not name is
	// not bounded.
	Before, After AgentLogMark
	Limit         int
	Forward       bool
}

// agentLogLockSeed seeds the advisory lock every writer of a customer's logs draws its id under.
const agentLogLockSeed = 731

// logRangeBits is how many low bits of a log id count the logs of the database that drew it.
// The bits above are that database's range: 0 for the shared database's, which numbered every
// log before they moved to the shards, and a shard's number (shard_numbers) for a shard's. An
// id names one row in every database, and says which one holds it.
const logRangeBits = 52

// logRange is the range of the database that drew a log id.
func logRange(id int64) int64 { return id >> logRangeBits }

// AgentLogMark is how far a reader has read a customer's logs: the highest id read of each
// range, by range. Each database numbers its logs in the order they commit, so a reader resuming
// after its mark misses none.
type AgentLogMark map[int64]int64

// Cursor is the mark for a client to hand back: the ids, one per range, shared database first.
// A mark of the shared database alone is the bare id every cursor was before the shards.
func (m AgentLogMark) Cursor() string {
	parts := make([]string, 0, len(m))
	for _, part := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, strconv.FormatInt(m[part], 10))
	}
	return base64.RawURLEncoding.EncodeToString([]byte(strings.Join(parts, ",")))
}

// Above is the bound that leaves out nothing at or below m, for Before.
func (m AgentLogMark) Above() AgentLogMark {
	above := make(AgentLogMark, len(m))
	for part, id := range m {
		above[part] = id + 1
	}
	return above
}

// ParseAgentLogCursor reads a mark a client handed back; the empty cursor is no mark.
func ParseAgentLogCursor(cursor string) (AgentLogMark, error) {
	if cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: log cursor: %w", err))
	}
	mark := AgentLogMark{}
	for part := range strings.SplitSeq(string(raw), ",") {
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return nil, stack.Wrap(fmt.Errorf("store: log cursor: %w", err))
		}
		if _, twice := mark[logRange(id)]; twice || id < 0 {
			return nil, stack.Wrap(fmt.Errorf("store: log cursor %q names a range twice or a negative id", raw))
		}
		mark[logRange(id)] = id
	}
	return mark, nil
}

// logSequence is the sequence a database draws its log ids from.
const logSequence = "pg_get_serial_sequence('agent_logs', 'id')::regclass"

// markBound is SQL for the bound mark sets on the ids of the database a statement runs in: the
// bound of the range its sequence draws, or otherwise when mark names none. A scalar subquery,
// which Postgres reads once and scans the index up to.
func markBound(mark AgentLogMark, otherwise int64) (string, []any) {
	parts := make([]int64, 0, len(mark))
	bounds := make([]int64, 0, len(mark))
	for part, id := range mark {
		parts, bounds = append(parts, part), append(bounds, id)
	}
	return `COALESCE((SELECT v.bound FROM pg_sequence AS s, unnest(?::bigint[], ?::bigint[]) AS v (part, bound)
		WHERE s.seqrelid = ` + logSequence + ` AND v.part = (s.seqmin - 1) >> ?), ?)`,
		[]any{pgdialect.Array(parts), pgdialect.Array(bounds), logRangeBits, otherwise}
}

// shardNumber is a shard's number, which sets its range of log ids apart.
type shardNumber struct {
	bun.BaseModel `bun:"table:shard_numbers"`
	Name          string `bun:"name,pk"`
	Number        int64  `bun:"number"`
}

// rangeLogIDs moves the ids this shard's logs draw into its own range, by its number in
// shard_numbers, so no other database draws them. It connects with the migrate DSNs, whose
// role owns the sequence.
func (s *Store) rangeLogIDs(ctx context.Context) error {
	global, err := connect(s.globalDSN, 1)
	if err != nil {
		return stack.Wrap(err)
	}
	defer global.Close() //nolint:errcheck // a pool the migration is done with
	shard, err := connect(s.shardDSN, 1)
	if err != nil {
		return stack.Wrap(err)
	}
	defer shard.Close() //nolint:errcheck // a pool the migration is done with

	numbered := &shardNumber{Name: s.shardNames[0]}
	err = global.NewInsert().Model(numbered).Column("name").
		On("CONFLICT (name) DO UPDATE").Set("name = EXCLUDED.name").
		Returning("number").Scan(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: number shard %s: %w", numbered.Name, err))
	}
	low := numbered.Number<<logRangeBits + 1
	ranged := func(ctx context.Context, db bun.IDB) (bool, error) {
		var least int64
		err := db.QueryRowContext(ctx, "SELECT seqmin FROM pg_sequence WHERE seqrelid = "+logSequence).Scan(&least)
		return least == low, err
	}
	if done, err := ranged(ctx, shard); err != nil || done {
		return stack.Wrap(err)
	}
	// Under the table's lock, so a second router starting beside the first, which may already
	// be drawing ids of the range, does not move the sequence back to its start.
	err = shard.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if _, err := tx.ExecContext(ctx, "LOCK TABLE agent_logs IN EXCLUSIVE MODE"); err != nil {
			return err
		}
		if done, err := ranged(ctx, tx); err != nil || done {
			return err
		}
		_, err := tx.ExecContext(ctx, fmt.Sprintf("ALTER SEQUENCE agent_logs_id_seq MINVALUE %d MAXVALUE %d START %d RESTART %d",
			low, low+1<<logRangeBits-2, low, low))
		return err
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: range log ids of shard %s: %w", numbered.Name, err))
	}
	return nil
}

// AgentLogs are what customers' agents did, in the shard where it happened, numbered per
// customer in the order they commit so a reader resuming after one misses none. Logs from
// before they moved to the shards are still read from the shared database.
type AgentLogs struct {
	store *Store
}

// Insert stores one log row, redacted, under its customer's lock.
func (r *AgentLogs) Insert(ctx context.Context, entry *AgentLog) error {
	entry.Message = SafeLogText(entry.Message)
	entry.Details = safeLogDetails(entry.Details)
	if err := entry.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if entry.OccurredAt.IsZero() {
		entry.OccurredAt = time.Now().UTC()
	}
	entry.IngestedAt = time.Now().UTC()
	err := r.store.shard.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		if err := lockUntilCommit(ctx, tx, entry.CustomerID, agentLogLockSeed); err != nil {
			return err
		}
		_, err := tx.NewInsert().Model(entry).Exec(ctx)
		return err
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: record agent log: %w", err))
	}
	return nil
}

// logRequests writes the provider_request log row of each written request, in calls' order:
// the call each happened in read from calls in one query, and the rows written to logs in one
// statement under every customer's lock, taken in customer order as every batch takes them, in
// the shard's transaction that wrote the requests.
func (r *AgentLogs) logRequests(ctx context.Context, tx bun.Tx, requests []Request) error {
	if len(requests) == 0 {
		return nil
	}
	found, err := callsOf(ctx, tx, requests)
	if err != nil {
		return err
	}
	entries := make([]AgentLog, len(requests))
	customers := make([]string, 0, len(requests))
	for i, request := range requests {
		entries[i] = requestLogEntry(request, found[i])
		customers = append(customers, request.CustomerID)
	}
	slices.Sort(customers)
	customers = slices.Compact(customers)
	// unnest reads the array in order, so the locks are taken in it.
	if _, err := tx.ExecContext(ctx,
		"SELECT pg_advisory_xact_lock(hashtextextended(customer, ?)) FROM unnest(?::text[]) AS customer",
		agentLogLockSeed, pgdialect.Array(customers)); err != nil {
		return err
	}
	_, err = tx.NewInsert().Model(&entries).Exec(ctx)
	return err
}

// HighWater is the highest id of a customer's log rows in each range, the range's start in a
// database holding none.
func (r *AgentLogs) HighWater(ctx context.Context, customerID string) (AgentLogMark, error) {
	highs, err := everyHolder(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]int64, error) {
		var id int64
		err := db.NewSelect().Model((*AgentLog)(nil)).
			ColumnExpr("COALESCE(MAX(id), (SELECT seqmin - 1 FROM pg_sequence WHERE seqrelid = "+logSequence+"))").
			Where("customer_id = ?", customerID).Scan(ctx, &id)
		return []int64{id}, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: log high water: %w", err))
	}
	mark := AgentLogMark{}
	for _, id := range slices.Concat(highs...) {
		mark[logRange(id)] = id
	}
	return mark, nil
}

// Select returns a page of a customer's log rows without their details, newest first unless
// the filter reads forward. Each row's Mark is the filter's, moved past it.
func (r *AgentLogs) Select(ctx context.Context, customerID string, f AgentLogFilter) ([]AgentLog, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}
	limit := min(max(f.Limit, 1), 251)
	pages, err := everyHolder(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]AgentLog, error) {
		return selectLogs(ctx, db, customerID, f, limit)
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: agent logs: %w", err))
	}
	from := f.Before
	if f.Forward {
		from = f.After
	}
	return interleave(pages, from, f.Forward, limit), nil
}

// selectLogs is Select's page of one database, in the order of its ids.
func selectLogs(ctx context.Context, db bun.IDB, customerID string, f AgentLogFilter, limit int) ([]AgentLog, error) {
	rows := []AgentLog{}
	q := db.NewSelect().Model(&rows).ExcludeColumn("details").Where("customer_id = ?", customerID)
	if f.ConfigID != "" {
		q = q.Where("config_id = ?", f.ConfigID)
	}
	if f.SessionID != "" {
		q = q.Where("session_id = ?", f.SessionID)
	}
	if f.UserID != "" {
		q = q.Where("user_id = ?", f.UserID)
	}
	if levels := severitiesFrom(f.Severity); len(levels) > 0 {
		q = q.Where("severity IN (?)", bun.In(levels))
	}
	if len(f.Sources) > 0 {
		q = q.Where("source IN (?)", bun.In(f.Sources))
	}
	if f.Search != "" {
		q = q.Where("message ILIKE ?", "%"+likeEscaper.Replace(f.Search)+"%")
	}
	if !f.From.IsZero() {
		q = q.Where("occurred_at >= ?", f.From)
	}
	if !f.To.IsZero() {
		q = q.Where("occurred_at < ?", f.To)
	}
	if f.Before != nil {
		bound, args := markBound(f.Before, math.MaxInt64)
		q = q.Where("id < "+bound, args...)
	}
	if f.After != nil {
		bound, args := markBound(f.After, 0)
		q = q.Where("id > "+bound, args...)
	}
	order := "id DESC"
	if f.Forward {
		order = "id ASC"
	}
	err := q.Order(order).Limit(limit).Scan(ctx)
	return rows, err
}

// interleave merges the pages of each database, each in the order of its ids, into one of at
// most limit rows in the order they were ingested, newest first unless forward. It takes every
// page in its own order, so a mark moved past any row leaves out no row of that page before
// it. A row read twice, the shared database being a peer's too, is kept once.
func interleave(pages [][]AgentLog, from AgentLogMark, forward bool, limit int) []AgentLog {
	mark := maps.Clone(from)
	if mark == nil {
		mark = AgentLogMark{}
	}
	next := make([]int, len(pages))
	seen := map[int64]bool{}
	rows := []AgentLog{}
	for len(rows) < limit {
		pick := -1
		for i, page := range pages {
			if next[i] == len(page) {
				continue
			}
			if pick < 0 || logFirst(page[next[i]], pages[pick][next[pick]], forward) {
				pick = i
			}
		}
		if pick < 0 {
			break
		}
		row := pages[pick][next[pick]]
		next[pick]++
		if seen[row.ID] {
			continue
		}
		seen[row.ID] = true
		mark[logRange(row.ID)] = row.ID
		row.Mark = maps.Clone(mark)
		row.Cursor = row.Mark.Cursor()
		rows = append(rows, row)
	}
	return rows
}

// logFirst reports whether a comes before b in a page newest first, or oldest first forward.
func logFirst(a, b AgentLog, forward bool) bool {
	order := a.IngestedAt.Compare(b.IngestedAt)
	if order == 0 {
		order = cmp.Compare(a.ID, b.ID)
	}
	if forward {
		return order < 0
	}
	return order > 0
}

// Get returns one of a customer's log rows with its details, or ErrNoAgentLog.
func (r *AgentLogs) Get(ctx context.Context, customerID string, id int64) (AgentLog, error) {
	if customerID == "" {
		return AgentLog{}, stack.Wrap(fmt.Errorf("%w: %d", ErrNoAgentLog, id))
	}
	row, err := whicheverHolder(ctx, r.store, func(ctx context.Context, db bun.IDB) (AgentLog, error) {
		var row AgentLog
		err := db.NewSelect().Model(&row).Where("customer_id = ? AND id = ?", customerID, id).Scan(ctx)
		return row, err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return AgentLog{}, stack.Wrap(fmt.Errorf("%w: %d", ErrNoAgentLog, id))
	}
	if err != nil {
		return AgentLog{}, stack.Wrap(fmt.Errorf("store: agent log: %w", err))
	}
	// The log row's own details carry the error, and are the answer; a row written before they
	// did carries none. A request's id is only unique in the database that numbered it, with
	// shards or after a data move, which gives a moved request an id of its new deployment's, so
	// an older row's request is found by that number only when it also started when the row
	// says it happened, for the same agent: both writers log a request at its started_at.
	_, carried := row.Details["error_code"]
	if row.EventType == "provider_request" && !r.store.Sharded() && !carried {
		rawID, _ := row.Details["request_id"].(string)
		requestID, parseErr := strconv.ParseInt(rawID, 10, 64)
		if parseErr == nil && requestID > 0 {
			var request Request
			lookup := r.store.shard.NewSelect().Model(&request).Column("error_code", "error_message").
				Where("customer_id = ? AND id = ?", customerID, requestID).
				Where("started_at = ? AND agent_id = ?", row.OccurredAt, row.AgentID).
				Scan(ctx)
			if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
				return AgentLog{}, stack.Wrap(fmt.Errorf("store: agent log request: %w", lookup))
			}
			if lookup == nil {
				if request.ErrorCode != "" {
					row.Details["error_code"] = SafeLogText(request.ErrorCode)
				}
				if request.ErrorMessage != "" {
					row.Details["error_message"] = SafeLogText(request.ErrorMessage)
				}
			}
		}
	}
	return row, nil
}

// DeleteUser deletes an app's logs that name one of its end users across every shard and the
// shared database, whose message and details hold what they said and did, writeBatch of them
// per statement, and then makes the changes recorded of the logs naming them, deleted ones'
// included, deletes with no payload (forgottenChanges). It answers how many logs it deleted.
// A log written after the last batch stays, for a run again to delete.
func (r *AgentLogs) DeleteUser(ctx context.Context, appID, userID string) (int64, error) {
	if appID == "" || userID == "" {
		return 0, stack.Wrap(errors.New("store: an app and a user id are required"))
	}
	counts, err := everyHolder(ctx, r.store, func(ctx context.Context, db bun.IDB) (int64, error) {
		var deleted int64
		for {
			batch := db.NewSelect().Model((*AgentLog)(nil)).Column("id").
				Where("customer_id = ? AND user_id = ?", appID, userID).Limit(writeBatch)
			result, err := db.NewDelete().Model((*AgentLog)(nil)).Where("customer_id = ? AND id IN (?)", appID, batch).Exec(ctx)
			if err != nil {
				return deleted, err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return deleted, err
			}
			deleted += n
			if n < int64(writeBatch) {
				break
			}
		}
		if _, err := forgottenChanges(db, appID, "agent_logs").Where("payload->>'user_id' = ?", userID).Exec(ctx); err != nil {
			return deleted, fmt.Errorf("changes: %w", err)
		}
		return deleted, nil
	})
	var deleted int64
	for _, count := range counts {
		deleted += count
	}
	if err != nil {
		return deleted, stack.Wrap(fmt.Errorf("store: delete user logs: %w", err))
	}
	return deleted, nil
}

// CountUser counts what DeleteUser would delete across every shard and the shared database.
func (r *AgentLogs) CountUser(ctx context.Context, appID, userID string) (int64, error) {
	counts, err := everyHolder(ctx, r.store, func(ctx context.Context, db bun.IDB) (int64, error) {
		count, err := db.NewSelect().Model((*AgentLog)(nil)).Where("customer_id = ? AND user_id = ?", appID, userID).Count(ctx)
		return int64(count), err
	})
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: count user logs: %w", err))
	}
	var total int64
	for _, count := range counts {
		total += count
	}
	return total, nil
}
