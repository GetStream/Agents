package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

type AgentLog struct {
	Cursor        string `bun:"-" json:"cursor"`
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
	Before, After                                 int64
	Limit                                         int
	Forward                                       bool
}

// agentLogLockSeed seeds the advisory lock every writer of a customer's logs draws its id under.
const agentLogLockSeed = 731

// AgentLogs are what customers' agents did, in the shared database, numbered per customer in the
// order they commit so a reader resuming after one misses none.
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
	err := r.store.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
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
// statement under every customer's lock, taken in customer order as every batch takes them.
// With one database both are the transaction that wrote the requests.
func (r *AgentLogs) logRequests(ctx context.Context, calls, logs bun.IDB, requests []Request) error {
	if len(requests) == 0 {
		return nil
	}
	found, err := callsOf(ctx, calls, requests)
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
	return logs.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		// unnest reads the array in order, so the locks are taken in it.
		if _, err := tx.ExecContext(ctx,
			"SELECT pg_advisory_xact_lock(hashtextextended(customer, ?)) FROM unnest(?::text[]) AS customer",
			agentLogLockSeed, pgdialect.Array(customers)); err != nil {
			return err
		}
		_, err := tx.NewInsert().Model(&entries).Exec(ctx)
		return err
	})
}

// HighWater is the highest id of a customer's log rows, 0 when there is none.
func (r *AgentLogs) HighWater(ctx context.Context, customerID string) (int64, error) {
	var id int64
	err := r.store.global.NewSelect().Model((*AgentLog)(nil)).ColumnExpr("COALESCE(MAX(id),0)").Where("customer_id = ?", customerID).Scan(ctx, &id)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: log high water: %w", err))
	}
	return id, nil
}

// Select returns a page of a customer's log rows without their details, newest first unless
// the filter reads forward.
func (r *AgentLogs) Select(ctx context.Context, customerID string, f AgentLogFilter) ([]AgentLog, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}
	rows := []AgentLog{}
	q := r.store.global.NewSelect().Model(&rows).ExcludeColumn("details").Where("customer_id = ?", customerID)
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
	if f.Before > 0 {
		q = q.Where("id < ?", f.Before)
	}
	if f.After > 0 {
		q = q.Where("id > ?", f.After)
	}
	order := "id DESC"
	if f.Forward {
		order = "id ASC"
	}
	if err := q.Order(order).Limit(min(max(f.Limit, 1), 251)).Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: agent logs: %w", err))
	}
	for i := range rows {
		rows[i].Cursor = base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(rows[i].ID, 10)))
	}
	return rows, nil
}

// Get returns one of a customer's log rows with its details, or ErrNoAgentLog.
func (r *AgentLogs) Get(ctx context.Context, customerID string, id int64) (AgentLog, error) {
	if customerID == "" {
		return AgentLog{}, stack.Wrap(fmt.Errorf("%w: %d", ErrNoAgentLog, id))
	}
	var row AgentLog
	err := r.store.global.NewSelect().Model(&row).Where("customer_id = ? AND id = ?", customerID, id).Scan(ctx)
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

// DeleteUser deletes an app's logs that name one of its end users, whose message and details
// hold what they said and did, writeBatch of them per statement, and then makes the changes
// recorded of the logs naming them, deleted ones' included, deletes with no payload
// (forgottenChanges). It answers how many logs it deleted. A log written after the last batch
// stays, for a run again to delete.
func (r *AgentLogs) DeleteUser(ctx context.Context, appID, userID string) (int64, error) {
	if appID == "" || userID == "" {
		return 0, stack.Wrap(errors.New("store: an app and a user id are required"))
	}
	db := r.store.global
	var deleted int64
	for {
		batch := db.NewSelect().Model((*AgentLog)(nil)).Column("id").
			Where("customer_id = ? AND user_id = ?", appID, userID).Limit(writeBatch)
		result, err := db.NewDelete().Model((*AgentLog)(nil)).Where("customer_id = ? AND id IN (?)", appID, batch).Exec(ctx)
		if err != nil {
			return deleted, stack.Wrap(fmt.Errorf("store: delete user logs: %w", err))
		}
		n, err := result.RowsAffected()
		if err != nil {
			return deleted, stack.Wrap(fmt.Errorf("store: delete user logs: %w", err))
		}
		deleted += n
		if n < int64(writeBatch) {
			break
		}
	}
	if _, err := forgottenChanges(db, appID, "agent_logs").Where("payload->>'user_id' = ?", userID).Exec(ctx); err != nil {
		return deleted, stack.Wrap(fmt.Errorf("store: delete user logs: changes: %w", err))
	}
	return deleted, nil
}

// CountUser counts what DeleteUser would delete.
func (r *AgentLogs) CountUser(ctx context.Context, appID, userID string) (int64, error) {
	count, err := r.store.global.NewSelect().Model((*AgentLog)(nil)).Where("customer_id = ? AND user_id = ?", appID, userID).Count(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: count user logs: %w", err))
	}
	return int64(count), nil
}
