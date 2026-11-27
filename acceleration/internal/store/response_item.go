package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// The kinds of item a response is made of.
const (
	// ItemSaid is what the person asked, which opens every response.
	ItemSaid = "said"
	// ItemThought is the model reasoning out loud, when a model reports any.
	ItemThought = "thought"
	// ItemToolCall is the agent deciding to use something, and ItemToolResult what came
	// back. They are separate items because the gap between them is where the time goes.
	ItemToolCall   = "tool_call"
	ItemToolResult = "tool_result"
	// ItemAnswer is what the agent said.
	ItemAnswer = "answer"
	// ItemBlocked is a turn a guardrail refused, and ItemError one that broke.
	ItemBlocked = "blocked"
	ItemError   = "error"
)

// AgentResponseItem is one thing that happened while the agent took a turn.
//
// Deltas are not items. A hundred fragments of one sentence are the sentence, and keeping
// them would make this table mostly punctuation; a caller watching a turn happen reads the
// deltas off the socket, and a caller reading one back wants the shape of it.
type AgentResponseItem struct {
	bun.BaseModel `bun:"table:agent_response_items,alias:ari"`

	ResponseID string `bun:"response_id,pk"`
	// Ordinal is the position within the response, assigned by the writer so a batch keeps
	// the order it was produced in rather than the order it was inserted in.
	Ordinal   int    `bun:"ordinal,pk"`
	SessionID string `bun:"session_id,notnull"`
	Kind      string `bun:"kind,notnull"`
	Text      string `bun:"text,notnull"`
	ToolName  string `bun:"tool_name,notnull"`
	// Payload is whatever the kind carries that text cannot: a tool's arguments, a
	// guardrail's reason, the attachments behind an answer.
	Payload map[string]any `bun:"payload,type:jsonb,nullzero"`
	At      time.Time      `bun:"at,notnull"`
	// RedactAt is when a tool call's or a tool result's arguments and output are taken out
	// (ResponseItems.Redact), stamped from the session's tool output retention when it is
	// written. Nil is never, which every item recorded without a retention is.
	RedactAt *time.Time `bun:"redact_at"`
}

// Validate refuses an item that names no response, session or kind.
func (i *AgentResponseItem) Validate() error {
	return validate("response item").
		required("response_id", i.ResponseID).
		required("session_id", i.SessionID).
		required("kind", i.Kind).
		check(i.Ordinal >= 0, "ordinal", "is negative").
		done()
}

// redactedToolKeys are what a redacted tool item keeps of its payload: what ties a call to
// its result, says which product and SDK ran it, and the counts tool usage reads: how the
// tool was offered, what its definition and the call cost in tokens. Anything else the
// payload carries, the arguments above all, goes. An error is kept as having happened, not
// as its text.
var redactedToolKeys = []string{"call_id", "product", "sdk", "loading", "definition_tokens", "summary_tokens", "tokens"}

// Redact takes out what a tool item carried of the call itself: its text (the result, or
// a search's query), its arguments, and the wording of its error. What is left still says
// which tool ran, when, whether it failed and which call a result answers, so a list of
// items reads the same and tool usage still counts it. redactToolOutputSQL is the same
// thing for rows already written, and the two are tested against each other.
func (i *AgentResponseItem) Redact() {
	payload := map[string]any{"redacted": true}
	for _, key := range redactedToolKeys {
		if value, ok := i.Payload[key]; ok && value != nil {
			payload[key] = value
		}
	}
	if _, failed := i.Payload["error"]; failed {
		payload["error"] = "redacted"
	}
	i.Text = ""
	i.Payload = payload
	i.RedactAt = nil
}

// ItemPosition is the last item of a page.
type ItemPosition struct {
	At         time.Time `json:"t"`
	ResponseID string    `json:"r"`
	Ordinal    int       `json:"o"`
}

// ItemFilter picks a page of a session's items in the order they happened.
type ItemFilter struct {
	// ResponseID narrows the page to one turn's items. Empty is every turn's.
	ResponseID string
	Limit      int
	// After starts the page after this item. Nil is the first page.
	After *ItemPosition
}

// How much of a conversation is handed back at once. A long session is thousands of items,
// and the only caller who wants every one is a caller walking the whole conversation, which
// is what paging is for.
const (
	defaultItemLimit = 200
	maxItemLimit     = 1000
)

// ItemLimit is the page size an item list uses for the limit asked for. ResponseItems.Select
// returns one row more than this, the same as the session queries.
func ItemLimit(asked int) int { return ClampLimit(asked, defaultItemLimit, maxItemLimit) }

// ResponseItems are what happened in each turn, in the shard that ran it.
type ResponseItems struct {
	store *Store
}

// InsertMany writes a batch of items. They are written together because they arrive
// together: a turn that uses three tools produces its items in bursts, and one round trip
// each would have the writer behind the conversation it is recording.
//
// Writing the same item twice is not an error. The writer batches on a timer as well as on
// a full buffer, so a flush overlapping the end of a turn can offer an item the previous
// flush already took, and the ordinal is what settles it.
func (r *ResponseItems) InsertMany(ctx context.Context, items []AgentResponseItem) error {
	if len(items) == 0 {
		return nil
	}
	for i := range items {
		if err := items[i].Validate(); err != nil {
			return stack.Wrap(err)
		}
		// What was said is the conversation, read back to the model when it is carried on, so
		// it is kept whole, up to maxTurnText. A tool's output and an error are cut as a log line is.
		switch items[i].Kind {
		case ItemSaid, ItemThought, ItemAnswer:
			items[i].Text = turnText(items[i].Text)
		default:
			items[i].Text = SafeLogText(items[i].Text)
		}
		if items[i].At.IsZero() {
			items[i].At = time.Now().UTC()
		}
	}

	_, err := r.store.shard.NewInsert().Model(&items).
		On("CONFLICT (response_id, ordinal) DO NOTHING").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: append response items: %w", err))
	}
	return nil
}

// redactToolOutputSQL is AgentResponseItem.Redact for up to ?1 rows of this shard whose
// redact_at is no later than ?0, oldest first. SKIP LOCKED because every router of the
// shard runs it: two of them on one row would wait on each other for nothing.
const redactToolOutputSQL = `
WITH due AS (
    SELECT response_id, ordinal FROM agent_response_items
    WHERE redact_at IS NOT NULL AND redact_at <= ?0
    ORDER BY redact_at
    LIMIT ?1
    FOR UPDATE SKIP LOCKED
)
UPDATE agent_response_items AS t SET
    text = '',
    payload = jsonb_strip_nulls(jsonb_build_object(
        'call_id', t.payload->'call_id',
        'product', t.payload->'product',
        'sdk', t.payload->'sdk',
        'loading', t.payload->'loading',
        'definition_tokens', t.payload->'definition_tokens',
        'summary_tokens', t.payload->'summary_tokens',
        'tokens', t.payload->'tokens',
        'error', CASE WHEN t.payload->'error' IS NOT NULL THEN to_jsonb('redacted'::text) END,
        'redacted', true)),
    redact_at = NULL
FROM due
WHERE t.response_id = due.response_id AND t.ordinal = due.ordinal`

// Redact takes the arguments and output out of up to limit tool items in this router's
// shard whose time to be redacted has come by now, and returns how many it redacted. Each
// row's time was set from its own customer's retention when it was written, so a run reads
// no customer's settings and touches no row that is not due. The other shards' routers
// redact theirs.
func (r *ResponseItems) Redact(ctx context.Context, now time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	result, err := r.store.shard.NewRaw(redactToolOutputSQL, now, limit).Exec(ctx)
	if err != nil {
		return 0, stack.Wrap(fmt.Errorf("store: redact tool output: %w", err))
	}
	return result.RowsAffected()
}

// Select returns a session's items in the order they happened, across every turn.
//
// One flat stream rather than a list per turn, because that is how a conversation reads and
// how it is rendered: the question, what the agent did about it, what it said, then the next
// question.
func (r *ResponseItems) Select(ctx context.Context, customerID, sessionID string, filter ItemFilter) ([]AgentResponseItem, error) {
	if customerID == "" || sessionID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}

	// Joined to responses rather than trusting the session id on the item, because the
	// customer is on the response and an item is only this caller's if the turn that made
	// it was.
	query := r.store.shard.NewSelect().Model((*AgentResponseItem)(nil)).
		Join("JOIN agent_responses AS r ON r.id = ari.response_id").
		Where("r.customer_id = ?", customerID).
		Where("ari.session_id = ?", sessionID).
		Where("r.rewound_at IS NULL").
		Order("ari.at ASC", "ari.response_id ASC", "ari.ordinal ASC").
		Limit(ItemLimit(filter.Limit) + 1)
	if filter.ResponseID != "" {
		query = query.Where("ari.response_id = ?", filter.ResponseID)
	}
	if filter.After != nil {
		query = query.Where("(ari.at, ari.response_id, ari.ordinal) > (?, ?, ?)",
			filter.After.At, filter.After.ResponseID, filter.After.Ordinal)
	}
	var items []AgentResponseItem
	if err := query.Scan(ctx, &items); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: session items: %w", err))
	}
	return items, nil
}
