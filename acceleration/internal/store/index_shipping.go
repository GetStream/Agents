package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

const (
	// indexShipEvery is how often this shard's changed sessions and calls are shipped, and so
	// how far behind the shared database's homes and lists can be.
	indexShipEvery = 5 * time.Second
	// indexOverlap is how far before its mark a ship reads again. A row's changed_at is set
	// before the write commits, so a slow commit can land behind a mark already past it;
	// shipping a row twice changes nothing.
	indexOverlap = time.Minute
)

// indexShipped is how far a shard has shipped one kind of row (Store.ShipIndex).
type indexShipped struct {
	bun.BaseModel `bun:"table:shard_index_shipped"`

	Shard          string    `bun:"shard,pk"`
	Kind           string    `bun:"kind,pk"`
	ShippedThrough time.Time `bun:"shipped_through"`
}

// indexed is a shard table the shared database keeps a copy of, row for row.
type indexed struct {
	// kind names it in shard_index_shipped.
	kind  string
	table string
	index string
	// alias is the table's in the model the lists read it into.
	alias string
	// columns are the copy's, but for the shard and what the copy generates itself.
	columns []string
	// jsonNotNull are the jsonb columns that are NOT NULL, which can still hold a JSON null:
	// jsonb_populate_recordset reads that as SQL NULL.
	jsonNotNull []string
}

var (
	indexedSessions = indexed{kind: "sessions", table: "agent_sessions", index: "session_index", alias: "asn", columns: []string{
		"id", "customer_id", "stream_app_pk", "config_id", "agent_name", "agent_id", "conversation_id", "user_id",
		"caller_kind", "api_key", "usage_categories", "team", "title", "description", "project", "project_tag", "custom",
		"model_overwrites", "forked_from", "fork_point", "connector_selections", "return_url", "no_ui", "remembers",
		"resume_history", "floor_control", "tool_timeout_ms", "caller_tools", "state", "modality", "created_at",
		"updated_at", "opened_at", "closed_at", "end_reason", "last_response_at", "pinned_at", "review_score",
		"changed_at",
	}, jsonNotNull: []string{"custom", "model_overwrites", "connector_selections", "resume_history"}}
	indexedCalls = indexed{kind: "calls", table: "calls", index: "call_index", alias: "c", columns: []string{
		"id", "session_id", "customer_id", "stream_app_pk", "call_type", "call_id", "agent_id", "conversation_id",
		"agent_user_id", "campaign_id", "contact_id", "from_number", "to_number", "direction", "started_at",
		"ended_at", "stt", "tts", "llm", "subagent", "sts", "voice", "instructions", "skills", "summary",
		"review_score", "review_notes", "tags", "changed_at",
	}, jsonNotNull: []string{"tags"}}
)

// shippedRow is what a ship reads of each row besides the row itself.
type shippedRow struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	CreatedAt  time.Time `json:"created_at"`
	ChangedAt  time.Time `json:"changed_at"`
}

// Indexed reports whether the session and call lists read the shared database's copies of
// every shard's rows (ShipIndex) rather than this shard's own.
func (s *Store) Indexed() bool { return s.homed }

// ShipIndex writes into the shared database what it keeps of this shard's sessions and calls
// changed since the last ship: where each session lives (SessionHomes), and the rows the lists
// read. A shard that never shipped ships every row it has first, statsShipBatch at a time, and
// carries on in the next ship when this one runs out of time.
func (s *Store) ShipIndex(ctx context.Context, _ time.Time) error {
	if err := s.shipIndexed(ctx, indexedSessions); err != nil {
		return err
	}
	return s.shipIndexed(ctx, indexedCalls)
}

func (s *Store) shipIndexed(ctx context.Context, table indexed) error {
	from, err := s.indexedSince(ctx, table)
	if err != nil {
		return err
	}
	columns := strings.Join(table.columns, ", ")
	set := make([]string, 0, len(table.columns))
	selected := make([]string, 0, len(table.columns))
	for _, column := range table.columns {
		set = append(set, column+" = EXCLUDED."+column)
		if slices.Contains(table.jsonNotNull, column) {
			column = "COALESCE(" + column + ", 'null'::jsonb)"
		}
		selected = append(selected, column)
	}
	// A row is replaced only by one that changed later: two ships overlapping write the newest.
	upsert := fmt.Sprintf(`INSERT INTO %[1]s (shard, %[2]s)
SELECT ?, %[3]s FROM jsonb_populate_recordset(NULL::%[1]s, ?::jsonb)
ON CONFLICT (customer_id, id) DO UPDATE SET shard = EXCLUDED.shard, %[4]s
WHERE %[1]s.changed_at <= EXCLUDED.changed_at`, table.index, columns, strings.Join(selected, ", "), strings.Join(set, ", "))

	var after *shippedRow
	for {
		page := s.shard.NewSelect().TableExpr(table.table).ColumnExpr("*").
			Where("changed_at >= ?", from).
			OrderExpr("changed_at, customer_id, id").
			Limit(statsShipBatch)
		if after != nil {
			page = page.Where("(changed_at, customer_id, id) > (?, ?, ?)", after.ChangedAt, after.CustomerID, after.ID)
		}
		var encoded []byte
		err := s.shard.NewSelect().
			ColumnExpr("COALESCE(json_agg(to_jsonb(shipped) ORDER BY changed_at, customer_id, id), '[]')").
			TableExpr("(?) AS shipped", page).
			Scan(ctx, &encoded)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: ship %s: read: %w", table.index, err))
		}
		var rows []shippedRow
		if err := json.Unmarshal(encoded, &rows); err != nil {
			return stack.Wrap(fmt.Errorf("store: ship %s: decode: %w", table.index, err))
		}
		if len(rows) == 0 {
			return nil
		}
		if table.kind == indexedSessions.kind {
			homes := make([]SessionHome, len(rows))
			for i, row := range rows {
				homes[i] = SessionHome{CustomerID: row.CustomerID, SessionID: row.ID, OpenedAt: row.CreatedAt}
			}
			if err := s.SessionHomes.record(ctx, homes); err != nil {
				return err
			}
		}
		if _, err := s.global.ExecContext(ctx, upsert, s.ShardName(), string(encoded)); err != nil {
			return stack.Wrap(fmt.Errorf("store: ship %s: %w", table.index, err))
		}
		last := rows[len(rows)-1]
		_, err = s.global.NewInsert().Model(&indexShipped{Shard: s.ShardName(), Kind: table.kind, ShippedThrough: last.ChangedAt}).
			On("CONFLICT (shard, kind) DO UPDATE").
			Set("shipped_through = GREATEST(?TableAlias.shipped_through, EXCLUDED.shipped_through)").
			Set("shipped_at = now()").
			Exec(ctx)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: ship %s: mark: %w", table.index, err))
		}
		if len(rows) < statsShipBatch {
			return nil
		}
		after = &last
	}
}

// indexedSince is the time from which this shard's rows of table may be missing from the
// shared copy or older there: its mark less indexOverlap, and zero before its first ship.
func (s *Store) indexedSince(ctx context.Context, table indexed) (time.Time, error) {
	var mark indexShipped
	err := s.global.NewSelect().Model(&mark).Where("shard = ? AND kind = ?", s.ShardName(), table.kind).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, stack.Wrap(fmt.Errorf("store: %s shipped: %w", table.index, err))
	}
	return mark.ShippedThrough.Add(-indexOverlap), nil
}

// indexedLists reads a list of a customer's rows of table, as build picks the columns of, narrows
// and orders the query, from this shard's own rows and from the shared copy of every other
// shard's: one query each, so a list pays no more than one trip to the shared database. A store
// whose lists are not indexed reads this shard alone.
func indexedLists[T any](ctx context.Context, s *Store, table indexed, customerID string, build func(*bun.SelectQuery) *bun.SelectQuery, region func(*T, string)) ([][]T, error) {
	var own []T
	if err := build(s.shard.NewSelect().Model((*T)(nil))).Scan(ctx, &own); err != nil {
		return nil, err
	}
	if !s.Indexed() {
		return [][]T{own}, nil
	}
	for i := range own {
		region(&own[i], s.ShardName())
	}
	var copied []T
	err := build(s.global.NewSelect().Model((*T)(nil)).ModelTableExpr(table.index+" AS "+table.alias)).
		ColumnExpr("?TableAlias.shard").
		Where("?TableAlias.shard <> ?", s.ShardName()).
		Scan(ctx, &copied)
	if err != nil {
		return nil, err
	}
	return [][]T{own, copied}, nil
}

// unindex drops the shared copy of a row deleted from this shard.
func (s *Store) unindex(ctx context.Context, table indexed, customerID, id string) error {
	if !s.Indexed() {
		return nil
	}
	_, err := s.global.NewDelete().TableExpr(table.index).
		Where("customer_id = ?", customerID).
		Where("id = ?", id).
		Where("shard = ?", s.ShardName()).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: unindex %s: %w", table.index, err))
	}
	return nil
}
