package store

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// replacedTable is a rollup the shared database keeps of every shard, whose rows a ship
// replaces hour by hour rather than one by one: a group can go away, as a request's
// organization is written after it, and only replacing the whole hour drops it.
type replacedTable struct {
	name string
	// at is the column holding the hour (or minute) a row is of.
	at      string
	columns []string
	// query reads the rows of [?0, ?1) from the shard, named as columns.
	query string
}

// spendDims are the request columns a spend read filters or groups by, which a row of
// shard_spend_hourly sums the requests of one value of.
var spendDims = []string{"customer_id", "organization_id", "api_key", "user_id", "usage_categories", "tags",
	"modality", "provider", "model", "billable", "success", "measured"}

var spendSums = []string{"rows", "cost_micros", "input_tokens", "cached_input_tokens", "output_tokens", "audio_ms",
	"characters", "images", "duration_ms"}

// spendKey is the dims of a shard_spend_hourly row, from the expressions of its spendDims.
func spendKey(dims []string) string {
	return `md5(concat_ws(E'\x1f', ` + strings.Join(dims, "::text, ") + `::text))`
}

var shippedSpend = replacedTable{
	name:    "shard_spend_hourly",
	at:      "started_at",
	columns: slices.Concat([]string{"started_at", "dims"}, spendDims, spendSums),
	query: `
SELECT started_at, ` + spendKey(spendDims) + ` AS dims, ` +
		strings.Join(spendDims, ", ") + ", " + strings.Join(spendSums, ", ") + `
FROM (
    SELECT date_trunc('hour', started_at, 'UTC') AS started_at, customer_id, organization_id, api_key, user_id,
        usage_categories, tags, modality, provider, model, billable, success, ` + measured("") + ` AS measured,
        COUNT(*) AS rows,
        COALESCE(SUM(cost_micros), 0) AS cost_micros,
        COALESCE(SUM(input_tokens), 0) AS input_tokens,
        COALESCE(SUM(cached_input_tokens), 0) AS cached_input_tokens,
        COALESCE(SUM(output_tokens), 0) AS output_tokens,
        COALESCE(SUM(audio_ms), 0) AS audio_ms,
        COALESCE(SUM(characters), 0) AS characters,
        COALESCE(SUM(images), 0) AS images,
        COALESCE(SUM(duration_ms), 0)::double precision AS duration_ms
    FROM requests
    WHERE started_at >= ?0 AND started_at < ?1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
) AS grouped`,
}

var shippedActivity = replacedTable{
	name:    "shard_activity_hourly",
	at:      "bucket",
	columns: []string{"customer_id", "bucket", "sessions", "messages", "calls", "voice_minutes", "phone_minutes"},
	query: `
WITH session_counts AS (
    SELECT customer_id, date_trunc('hour', created_at, 'UTC') AS bucket, COUNT(*) AS n
    FROM agent_sessions
    WHERE created_at >= ?0 AND created_at < ?1
    GROUP BY 1, 2
),
message_counts AS (
    SELECT customer_id, date_trunc('hour', created_at, 'UTC') AS bucket, COUNT(*) AS n
    FROM agent_responses
    WHERE created_at >= ?0 AND created_at < ?1
    GROUP BY 1, 2
),
call_counts AS (
    SELECT
        customer_id,
        date_trunc('hour', started_at, 'UTC') AS bucket,
        COUNT(*) AS n,
        SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at, now()) - started_at)))::double precision / 60 AS voice_minutes,
        COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at, now()) - started_at)))
            FILTER (WHERE from_number IS NOT NULL OR to_number IS NOT NULL), 0)::double precision / 60 AS phone_minutes
    FROM calls
    WHERE started_at >= ?0 AND started_at < ?1 AND call_id <> ''
    GROUP BY 1, 2
),
keys AS (
    SELECT customer_id, bucket FROM session_counts
    UNION SELECT customer_id, bucket FROM message_counts
    UNION SELECT customer_id, bucket FROM call_counts
)
SELECT k.customer_id, k.bucket,
    COALESCE(s.n, 0) AS sessions, COALESCE(m.n, 0) AS messages, COALESCE(c.n, 0) AS calls,
    COALESCE(c.voice_minutes, 0) AS voice_minutes, COALESCE(c.phone_minutes, 0) AS phone_minutes
FROM keys AS k
LEFT JOIN session_counts AS s USING (customer_id, bucket)
LEFT JOIN message_counts AS m USING (customer_id, bucket)
LEFT JOIN call_counts AS c USING (customer_id, bucket)`,
}

var shippedPeople = replacedTable{
	name:    "shard_people_hourly",
	at:      "bucket",
	columns: []string{"customer_id", "bucket", "user_id"},
	query: `
SELECT DISTINCT customer_id, bucket, user_id FROM (
    SELECT sess.customer_id, date_trunc('hour', sess.created_at, 'UTC') AS bucket, sess.user_id, sess.caller_kind
    FROM agent_sessions AS sess
    WHERE sess.created_at >= ?0 AND sess.created_at < ?1
    UNION ALL
    SELECT a.customer_id, date_trunc('hour', a.created_at, 'UTC') AS bucket, s.user_id, s.caller_kind
    FROM agent_responses AS a
    JOIN agent_sessions AS s ON s.customer_id = a.customer_id AND s.id = a.session_id
    WHERE a.created_at >= ?0 AND a.created_at < ?1
) AS seen
WHERE user_id <> '' AND caller_kind <> 'anonymous'`,
}

var shippedSessionTurns = replacedTable{
	name:    "shard_session_turns_hourly",
	at:      "bucket",
	columns: []string{"customer_id", "bucket", "config_id", "session_id", "turns"},
	query: `
SELECT a.customer_id, date_trunc('hour', a.created_at, 'UTC') AS bucket, COALESCE(s.config_id, '') AS config_id,
    a.session_id, COUNT(*) AS turns
FROM agent_responses AS a
JOIN agent_sessions AS s ON s.customer_id = a.customer_id AND s.id = a.session_id
WHERE a.created_at >= ?0 AND a.created_at < ?1 AND a.rewound_at IS NULL
GROUP BY 1, 2, 3, 4`,
}

// toolRan is every tool result of [?0, ?1), with the turn it was in, as CustomerToolUsage
// reads them for one customer.
var toolRan = fmt.Sprintf(`
WITH results AS (
    SELECT r.response_id, r.ordinal, r.tool_name, r.at, r.payload, r.text
    FROM agent_response_items AS r
    WHERE r.kind = '%[1]s' AND r.at >= ?0 AND r.at < ?1
),
turns AS (
    SELECT
        a.id,
        a.created_at,
        ROW_NUMBER() OVER (PARTITION BY a.customer_id, a.session_id ORDER BY a.created_at, a.id COLLATE "C") - 1 AS before
    FROM agent_responses AS a
    WHERE a.session_id IN (SELECT DISTINCT b.session_id FROM results JOIN agent_responses AS b ON b.id = results.response_id)
        AND a.created_at < ?1 AND a.rewound_at IS NULL
),
ran AS (
    SELECT
        a.customer_id,
        COALESCE(s.config_id, '') AS config_id,
        date_trunc('hour', r.at, 'UTC') AS bucket,
        r.tool_name AS tool,
        a.session_id,
        t.before,
        t.created_at AS turn_at,
        t.id AS turn_id,
        r.at,
        r.payload->>'error' IS NOT NULL AS failed,
        COALESCE((r.payload->>'tokens')::double precision,
            (octet_length(r.tool_name) + octet_length(COALESCE(r.payload->>'arguments', ''))
                + octet_length(r.text) + %[3]d - 1) / %[3]d) AS tokens,
        EXTRACT(EPOCH FROM (r.at - started.at)) * 1000 AS ms,
        started.payload->>'loading' AS loading,
        (started.payload->>'definition_tokens')::bigint AS definition_tokens,
        (started.payload->>'summary_tokens')::bigint AS summary_tokens
    FROM results AS r
    JOIN agent_responses AS a ON a.id = r.response_id
    JOIN agent_sessions AS s ON s.customer_id = a.customer_id AND s.id = a.session_id
    LEFT JOIN turns AS t ON t.id = a.id
    LEFT JOIN LATERAL (
        SELECT c.at, c.payload FROM agent_response_items AS c
        WHERE c.response_id = r.response_id AND c.kind = '%[2]s' AND c.tool_name = r.tool_name
            AND c.ordinal < r.ordinal
            AND COALESCE(c.payload->>'call_id', '') = COALESCE(r.payload->>'call_id', '')
        ORDER BY c.ordinal DESC LIMIT 1
    ) AS started ON true
    WHERE a.created_at < ?1
)`, ItemToolResult, ItemToolCall, charsPerToken)

// shippedTools has a row with an empty tool for every tool of the hour, whose p99_ms the
// tools' own percentiles cannot give.
var shippedTools = replacedTable{
	name: "shard_tool_hourly",
	at:   "bucket",
	columns: []string{"customer_id", "bucket", "config_id", "tool", "calls", "failed", "tokens_total", "ms_total",
		"ms_count", "p99_ms", "offered_at", "loading", "definition_tokens", "summary_tokens"},
	query: toolRan + `,
offered AS (
    SELECT DISTINCT ON (customer_id, bucket, config_id, tool) customer_id, bucket, config_id, tool,
        at, loading, definition_tokens, summary_tokens
    FROM ran WHERE definition_tokens IS NOT NULL
    ORDER BY customer_id, bucket, config_id, tool, at DESC
)
SELECT ran.customer_id, ran.bucket, ran.config_id, COALESCE(ran.tool, '') AS tool,
    COUNT(*) AS calls,
    COUNT(*) FILTER (WHERE failed) AS failed,
    COALESCE(SUM(tokens), 0)::double precision AS tokens_total,
    COALESCE(SUM(ms), 0)::double precision AS ms_total,
    COUNT(ms) AS ms_count,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99_ms,
    o.at AS offered_at,
    COALESCE(o.loading, '') AS loading,
    o.definition_tokens,
    o.summary_tokens
FROM ran
LEFT JOIN offered AS o USING (customer_id, bucket, config_id, tool)
GROUP BY GROUPING SETS (
    (ran.customer_id, ran.bucket, ran.config_id, ran.tool, o.at, o.loading, o.definition_tokens, o.summary_tokens),
    (ran.customer_id, ran.bucket, ran.config_id))`,
}

var shippedToolSessions = replacedTable{
	name: "shard_tool_sessions_hourly",
	at:   "bucket",
	columns: []string{"customer_id", "bucket", "config_id", "tool", "session_id", "first_at", "first_turn",
		"first_before"},
	query: toolRan + `
SELECT DISTINCT ON (customer_id, bucket, config_id, tool, session_id)
    customer_id, bucket, config_id, tool, session_id, turn_at AS first_at, turn_id AS first_turn, before AS first_before
FROM ran
ORDER BY customer_id, bucket, config_id, tool, session_id, before`,
}

var shippedConcurrency = replacedTable{
	name:    "shard_concurrency_minute",
	at:      "minute",
	columns: []string{"customer_id", "minute", "sessions"},
	query: `
WITH open AS (
    SELECT customer_id, created_at, closed_at FROM agent_sessions WHERE closed_at > ?0 AND created_at < ?1
    UNION ALL
    SELECT customer_id, created_at, closed_at FROM agent_sessions WHERE closed_at IS NULL AND created_at < ?1
)
SELECT s.customer_id, m.minute, COUNT(*) AS sessions
FROM open AS s
JOIN generate_series(?0::timestamptz, ?1::timestamptz - interval '1 minute', interval '1 minute') AS m(minute)
    ON s.created_at < m.minute + interval '1 minute' AND (s.closed_at IS NULL OR s.closed_at > m.minute)
GROUP BY 1, 2`,
}

// replacedRollups are the rollups a ship replaces hour by hour.
var replacedRollups = []replacedTable{
	shippedSpend, shippedActivity, shippedPeople, shippedSessionTurns, shippedTools, shippedToolSessions, shippedConcurrency,
}

// replaceRows writes the rows table's query reads from this shard over [from, to) into the
// shared database as the shard's, in place of every row the shard shipped of that window. The
// rows cross as JSON, as shipRows does.
func (s *Store) replaceRows(ctx context.Context, shard string, table replacedTable, from, to time.Time) error {
	var encoded []byte
	err := s.shard.NewSelect().
		ColumnExpr("COALESCE(json_agg(shipped), '[]')").
		TableExpr("("+table.query+") AS shipped", from, to).
		Scan(ctx, &encoded)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: ship %s: read: %w", table.name, err))
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(encoded, &rows); err != nil {
		return stack.Wrap(fmt.Errorf("store: ship %s: decode: %w", table.name, err))
	}

	columns := strings.Join(table.columns, ", ")
	insert := fmt.Sprintf(`INSERT INTO %[1]s (shard, %[2]s)
SELECT ?, %[2]s FROM jsonb_populate_recordset(NULL::%[1]s, ?::jsonb)`, table.name, columns)
	err = s.global.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewDelete().TableExpr(table.name).
			Where("shard = ?", shard).
			Where("? >= ?", bun.Ident(table.at), from).
			Where("? < ?", bun.Ident(table.at), to).
			Exec(ctx)
		if err != nil {
			return err
		}
		for batch := range slices.Chunk(rows, statsShipBatch) {
			encoded, err := json.Marshal(batch)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, insert, shard, string(encoded)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: ship %s: %w", table.name, err))
	}
	return nil
}
