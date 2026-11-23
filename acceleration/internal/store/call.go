package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// Call is one stretch of a session held on a Stream call, kept so it can be found after the
// process that held it is gone. A session somebody goes back and forth between typing and
// talking in has a call for each time they talked.
//
// What was said is not here: the transcript is in Stream Chat, keyed by AgentID, and the
// timings are in Turn. Who the call was with, which config it ran and which conversation it
// was part of are its session's. This row carries the models the call ran on and the
// judgements made after it ended.
type Call struct {
	bun.BaseModel `bun:"table:calls,alias:c"`

	ID string `bun:"id,pk"`
	// SessionID is the session the call was part of. Empty for a call whose session was
	// deleted, and for one whose session was never kept.
	SessionID  string `bun:"session_id,nullzero"`
	CustomerID string `bun:"customer_id,notnull"`
	// StreamAppPK is the Stream app this was made in, and the one it is finished in. Zero,
	// stored as NULL, is the deployment's own app.
	StreamAppPK int64 `bun:"stream_app_pk,nullzero"`
	// CallType and CallID are the Stream call, and AgentID is the transcript channel.
	CallType string `bun:"call_type,notnull"`
	CallID   string `bun:"call_id,notnull"`
	AgentID  string `bun:"agent_id,notnull"`
	// ConversationID is the conversation the call was bound to when it started, as its
	// session's row names it. It outlives that row, so a deleted session's call can still
	// find its transcript. Empty for a call that kept no conversation.
	ConversationID string `bun:"conversation_id,nullzero"`
	// AgentUserID is the Stream user the agent spoke the call's lines as. A line marked as the
	// agent's is the agent's only when this user or AgentID wrote it (chatlog.Read.Writer).
	// Empty for a call from before the column, whose lines are read as they were then.
	AgentUserID string `bun:"agent_user_id,nullzero"`
	CampaignID  string `bun:"campaign_id,nullzero"`
	ContactID   string `bun:"contact_id,nullzero"`
	FromNumber  string `bun:"from_number,nullzero"`
	ToNumber    string `bun:"to_number,nullzero"`
	// Direction is inbound or outbound: who rang whom.
	Direction string    `bun:"direction,notnull"`
	StartedAt time.Time `bun:"started_at,notnull"`
	// EndedAt is nil while the call is still running.
	EndedAt *time.Time `bun:"ended_at"`
	// STT, TTS, LLM and Subagent are the targets the call ran with, after a session's
	// overrides were folded into whatever config it named. They are what was asked for
	// rather than what each turn resolved to: a shortcut is several models and routing
	// fails over between them, so per-turn providers live in requests.
	STT      string `bun:"stt,nullzero"`
	TTS      string `bun:"tts,nullzero"`
	LLM      string `bun:"llm,nullzero"`
	Subagent string `bun:"subagent,nullzero"`
	// STS is the speech-to-speech target a native call ran with, in place of the three
	// above.
	STS string `bun:"sts,nullzero"`
	// Voice is the voice the call asked for, empty for the provider's default.
	Voice string `bun:"voice,nullzero"`
	// Instructions is what the agent was told to be on this call.
	Instructions string `bun:"instructions,nullzero"`
	// Skills names what the fast model could hand to the subagent. The instructions
	// behind each name live in the skill registry.
	Skills []string `bun:"skills,type:jsonb,nullzero"`
	// Summary, ReviewScore and ReviewNotes are written after the call by a short model
	// pass over the transcript, so they are empty until it has run.
	Summary     string            `bun:"summary,nullzero"`
	ReviewScore *int              `bun:"review_score"`
	ReviewNotes string            `bun:"review_notes,nullzero"`
	Tags        map[string]string `bun:"tags,type:jsonb,nullzero"`
}

// Validate refuses a call no list could read back. An empty direction is the inbound one
// Start fills in.
func (c *Call) Validate() error {
	return validate("call").
		required("id", c.ID).
		required("customer_id", c.CustomerID).
		oneOf("direction", c.Direction, "", Inbound, Outbound).
		check(c.ReviewScore == nil || *c.ReviewScore >= 1 && *c.ReviewScore <= 5, "review_score", "is not from 1 to 5").
		done()
}

// CallFilter narrows which calls are listed. Every field is optional, and an empty filter
// is the customer's most recent calls.
type CallFilter struct {
	AgentID    string
	SessionID  string
	CampaignID string
	// Running limits the list to calls that have not ended.
	Running bool
	// From and To bound when the call started. A zero time is unbounded.
	From time.Time
	To   time.Time
	// MaxReviewScore limits the list to reviewed calls scored at or below it. Zero leaves
	// the review out of it.
	MaxReviewScore int
	// Limit caps how many come back. Zero leaves the store's own default.
	Limit int
}

// defaultCallLimit caps a call list that did not ask for one.
const defaultCallLimit = 50

// maxCallLimit caps one a caller asked too much of.
const maxCallLimit = 500

// ErrNoCall is a call id, or an agent id in an app, no call row is found by. A sentinel, as
// ErrNoAgentConfig is, so a caller can tell a call nobody made from the database failing. The
// text is the one these errors always had.
var ErrNoCall = errors.New("store: there is no call")

func unknownCall(id string) error {
	return stack.Wrap(fmt.Errorf("%w %s", ErrNoCall, id))
}

// Calls are the calls sessions held, in the shard that ran them.
type Calls struct {
	store *Store
}

// Start records that a call began.
//
// It is written when the agent joins rather than when the call ends, so a call that is
// still running is findable and one the process died during is not simply lost. Starting
// the same call twice updates the row instead of failing: a retried write must not turn
// into a second call.
func (r *Calls) Start(ctx context.Context, call *Call) error {
	if err := call.Validate(); err != nil {
		return stack.Wrap(err)
	}
	if call.StartedAt.IsZero() {
		call.StartedAt = time.Now().UTC()
	}
	if call.Direction == "" {
		call.Direction = Inbound
	}
	// The client opening the session of a call the router placed may name neither number,
	// so what the router dialled is what the row says.
	if call.CallID != "" {
		placed, err := r.store.CallLegs.SelectPlaced(ctx, call.CustomerID, []string{call.CallID})
		if err != nil {
			return err
		}
		if len(placed) > 0 {
			call.Direction = Outbound
			call.FromNumber, call.ToNumber = placed[0].LocalNumber, placed[0].RemoteNumber
		}
	}
	if call.Tags == nil {
		call.Tags = map[string]string{}
	}

	_, err := r.store.shard.NewInsert().Model(call).
		On("CONFLICT (id) DO UPDATE").
		Set("ended_at = NULL").
		Set("agent_user_id = COALESCE(c.agent_user_id, EXCLUDED.agent_user_id)").
		Set("conversation_id = COALESCE(c.conversation_id, EXCLUDED.conversation_id)").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: start call: %w", err))
	}
	return nil
}

// Finish records that a call ended. A call that already ended keeps the first time: the
// agent leaves once, and a second close is the same leaving reported again.
func (r *Calls) Finish(ctx context.Context, customerID, id string, at time.Time) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a call id are required"))
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	_, err := r.store.shard.NewUpdate().Model((*Call)(nil)).
		Set("ended_at = ?", at).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("ended_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: finish call: %w", err))
	}
	return nil
}

// ChangeModels rewrites the targets and voice a running call is on, after it was moved onto
// other models mid-call.
func (r *Calls) ChangeModels(ctx context.Context, call *Call) error {
	if call.CustomerID == "" || call.ID == "" {
		return stack.Wrap(errors.New("store: a customer and a call id are required"))
	}

	// Set rather than Column: the fields are nullzero, and a target a call no longer runs
	// is the empty string these columns hold, never NULL.
	_, err := r.store.shard.NewUpdate().Model((*Call)(nil)).
		Set("stt = ?", call.STT).
		Set("tts = ?", call.TTS).
		Set("llm = ?", call.LLM).
		Set("subagent = ?", call.Subagent).
		Set("sts = ?", call.STS).
		Set("voice = ?", call.Voice).
		Where("id = ?", call.ID).
		Where("customer_id = ?", call.CustomerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: change call models: %w", err))
	}
	return nil
}

// Review writes what a model made of the call once it was over, and gives its session the
// score as well.
func (r *Calls) Review(ctx context.Context, customerID, id, summary string, score *int, notes string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a call id are required"))
	}

	type reviewed struct {
		SessionID sql.NullString `bun:"session_id"`
	}
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]reviewed, error) {
		var calls []reviewed
		_, err := db.NewUpdate().Model((*Call)(nil)).
			Set("summary = ?", summary).
			Set("review_score = ?", score).
			Set("review_notes = ?", notes).
			Where("id = ?", id).
			Where("customer_id = ?", customerID).
			Returning("session_id").
			Exec(ctx, &calls)
		return calls, err
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: review call: %w", err))
	}
	var sessions []string
	found := false
	for _, calls := range lists {
		for _, call := range calls {
			found = true
			if call.SessionID.Valid {
				sessions = append(sessions, call.SessionID.String)
			}
		}
	}
	if !found {
		return unknownCall(id)
	}
	// The session carries the score too, so a session list filters and sorts on it without a
	// join. A call whose session was deleted or never kept has nothing to write to, which is
	// not a failure.
	if len(sessions) == 0 {
		return nil
	}
	_, err = onEveryShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (sql.Result, error) {
		return db.NewUpdate().Model((*AgentSession)(nil)).
			Set("review_score = ?", score).
			Where("id IN (?)", bun.In(sessions)).
			Where("customer_id = ?", customerID).
			Exec(ctx)
	})
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: review call: session score: %w", err))
	}
	return nil
}

// Get returns one call a customer ran.
func (r *Calls) Get(ctx context.Context, customerID, id string) (Call, error) {
	if customerID == "" || id == "" {
		return Call{}, stack.Wrap(errors.New("store: a customer and a call id are required"))
	}

	call, err := whicheverShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (Call, error) {
		var call Call
		err := db.NewSelect().Model(&call).
			Where("id = ?", id).
			Where("customer_id = ?", customerID).
			Limit(1).
			Scan(ctx)
		return call, err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Call{}, unknownCall(id)
	}
	if err != nil {
		return Call{}, stack.Wrap(fmt.Errorf("store: call: %w", err))
	}
	return call, nil
}

// Latest is the newest call of each of a customer's sessions that held one, by session id.
// A session that never held a call is not in it.
func (r *Calls) Latest(ctx context.Context, customerID string, sessionIDs []string) (map[string]Call, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}
	latest := map[string]Call{}
	if len(sessionIDs) == 0 {
		return latest, nil
	}
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]Call, error) {
		var calls []Call
		err := db.NewSelect().Model(&calls).
			DistinctOn("c.session_id").
			Where("c.customer_id = ?", customerID).
			Where("c.session_id IN (?)", bun.In(sessionIDs)).
			Order("c.session_id", "c.started_at DESC").
			Scan(ctx)
		return calls, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: latest calls: %w", err))
	}
	for _, calls := range lists {
		for _, call := range calls {
			if held, ok := latest[call.SessionID]; !ok || call.StartedAt.After(held.StartedAt) {
				latest[call.SessionID] = call
			}
		}
	}
	return latest, nil
}

// LastVoiced is the newest call of a session that was in a Stream call, which voice goes back
// to once started again. ErrNoCall when voice was never started on it.
func (r *Calls) LastVoiced(ctx context.Context, customerID, sessionID string) (Call, error) {
	if customerID == "" || sessionID == "" {
		return Call{}, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}

	call, err := whicheverShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (Call, error) {
		var call Call
		err := db.NewSelect().Model(&call).
			Where("c.customer_id = ?", customerID).
			Where("c.session_id = ?", sessionID).
			Where("c.call_id <> ''").
			Order("c.started_at DESC").
			Limit(1).
			Scan(ctx)
		return call, err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Call{}, unknownCall(sessionID)
	}
	if err != nil {
		return Call{}, stack.Wrap(fmt.Errorf("store: last voiced call: %w", err))
	}
	return call, nil
}

// Select returns a customer's calls, newest first.
func (r *Calls) Select(ctx context.Context, customerID string, filter CallFilter) ([]Call, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = defaultCallLimit
	}
	if limit > maxCallLimit {
		limit = maxCallLimit
	}

	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]Call, error) {
		query := db.NewSelect().Model((*Call)(nil)).
			Where("customer_id = ?", customerID).
			Order("started_at DESC").
			Limit(limit)
		if filter.AgentID != "" {
			query = query.Where("agent_id = ?", filter.AgentID)
		}
		if filter.SessionID != "" {
			query = query.Where("session_id = ?", filter.SessionID)
		}
		if filter.CampaignID != "" {
			query = query.Where("campaign_id = ?", filter.CampaignID)
		}
		if filter.Running {
			query = query.Where("ended_at IS NULL")
		}
		if !filter.From.IsZero() {
			query = query.Where("started_at >= ?", filter.From)
		}
		if !filter.To.IsZero() {
			query = query.Where("started_at < ?", filter.To)
		}
		if filter.MaxReviewScore > 0 {
			query = query.Where("review_score <= ?", filter.MaxReviewScore)
		}
		var calls []Call
		err := query.Scan(ctx, &calls)
		return calls, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer calls: %w", err))
	}
	return merged(lists, func(a, b Call) int { return b.StartedAt.Compare(a.StartedAt) }, limit), nil
}

// ModelCalls returns every model response in one call, including flow and delegated
// requests, in the order they were sent. TurnID relates each request to its exchange.
func (r *Calls) ModelCalls(ctx context.Context, customerID, agentID, callID string, from time.Time, to *time.Time) ([]Request, error) {
	if customerID == "" || agentID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and an agent id are required"))
	}
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]Request, error) {
		query := db.NewSelect().Model((*Request)(nil)).
			Where("customer_id = ?", customerID).
			Where("agent_id = ?", agentID).
			Where("modality = ?", "llm").
			Where("started_at >= ?", from).
			Order("started_at ASC", "id ASC")
		if callID != "" {
			query = query.Where("call_id = ?", callID)
		}
		if to != nil {
			query = query.Where("started_at <= ?", *to)
		}
		var requests []Request
		err := query.Scan(ctx, &requests)
		return requests, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: call model calls: %w", err))
	}
	return merged(lists, func(a, b Request) int {
		return cmp.Or(a.StartedAt.Compare(b.StartedAt), cmp.Compare(a.ID, b.ID))
	}, 0), nil
}

// UsedModel is a provider/model that successfully served a call.
type UsedModel struct {
	Modality string `bun:"modality"`
	Provider string `bun:"provider"`
	Model    string `bun:"model"`
}

// UsedModels returns the distinct provider/models that did work on a call, most recently
// used first. They are keyed by agent and window the way turns are, because a request row
// carries the Stream call rather than the session that recorded it.
func (r *Calls) UsedModels(ctx context.Context, customerID, agentID string, from time.Time, to *time.Time) ([]UsedModel, error) {
	if customerID == "" || agentID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and an agent id are required"))
	}

	type usedAt struct {
		UsedModel
		LastUsed time.Time `bun:"last_used"`
	}
	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]usedAt, error) {
		query := db.NewSelect().
			TableExpr("requests").
			ColumnExpr("modality, provider, model, MAX(started_at) AS last_used").
			Where("customer_id = ?", customerID).
			Where("agent_id = ?", agentID).
			Where("success").
			Where("started_at >= ?", from).
			Group("modality", "provider", "model").
			OrderExpr("MAX(started_at) DESC")
		if to != nil {
			query = query.Where("started_at <= ?", *to)
		}
		var used []usedAt
		err := query.Scan(ctx, &used)
		return used, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: call used models: %w", err))
	}

	all := distinct(merged(lists, func(a, b usedAt) int { return b.LastUsed.Compare(a.LastUsed) }, 0),
		func(u usedAt) string { return u.Modality + "\x00" + u.Provider + "\x00" + u.Model })
	var used []UsedModel
	for _, model := range all {
		used = append(used, model.UsedModel)
	}
	return used, nil
}

// CallUsage is what a call spent, summed over every request it made.
type CallUsage struct {
	InputTokens       int64 `bun:"input_tokens"`
	CachedInputTokens int64 `bun:"cached_input_tokens"`
	OutputTokens      int64 `bun:"output_tokens"`
	CostMicros        int64 `bun:"cost_micros"`
	Requests          int64 `bun:"requests"`
}

// RuntimeModality is the modality of the rows that bill the time a session was open, a slice
// at a time. They cost what the session's time cost, and are not requests.
const RuntimeModality = "runtime"

// SandboxModality is the modality of the rows that bill the time a session's sandbox held its
// provider's resources, a slice at a time. Like runtime they are a session's time, not
// requests.
const SandboxModality = "sandbox"

// requestCount counts a query's rows that are requests, leaving the sliced time of runtime and
// sandboxes out.
const requestCount = "COUNT(*) FILTER (WHERE modality NOT IN ('" + RuntimeModality + "', '" + SandboxModality + "'))"

// ownSlicesOnly keeps out of a call's totals the sliced time of other sessions of its agent:
// a call's runtime and sandbox are its own session's (session_id = ?).
const ownSlicesOnly = "(modality NOT IN ('" + RuntimeModality + "', '" + SandboxModality + "') OR session_id = ?)"

// ownWorkOnly keeps out of a call's totals everything other sessions of its agent did, not
// only their sliced time: a row that names a session is counted only when it names this one.
// A row naming none (written before requests carried the session, or outside any session)
// can only be matched by agent and window, so those still count.
const ownWorkOnly = "(session_id = ? OR (session_id = '' AND modality NOT IN ('" + RuntimeModality + "', '" + SandboxModality + "')))"

// sessionFilter is the clause that keeps a call's totals to its own session. Without a
// session id only the sliced time can be told apart.
func sessionFilter(sessionID string) string {
	if sessionID == "" {
		return ownSlicesOnly
	}
	return ownWorkOnly
}

// Usage totals the work one call paid for: its agent's rows in its window that are its own
// session's (sessionID), so another session of the same agent at the same time is not
// counted. A row that names no session is matched by agent and window alone.
//
// Failed requests are counted: a model that read the prompt and then fell over is still
// billed for having read it. The call's runtime and sandbox time are in its cost and not
// among its requests.
func (r *Calls) Usage(ctx context.Context, customerID, agentID, sessionID string, from time.Time, to *time.Time) (CallUsage, error) {
	if customerID == "" || agentID == "" {
		return CallUsage{}, stack.Wrap(errors.New("store: a customer and an agent id are required"))
	}

	answers, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) (CallUsage, error) {
		query := db.NewSelect().
			TableExpr("requests").
			ColumnExpr("COALESCE(SUM(input_tokens), 0) AS input_tokens").
			ColumnExpr("COALESCE(SUM(cached_input_tokens), 0) AS cached_input_tokens").
			ColumnExpr("COALESCE(SUM(output_tokens), 0) AS output_tokens").
			ColumnExpr("COALESCE(SUM(cost_micros), 0) AS cost_micros").
			ColumnExpr(requestCount+" AS requests").
			Where("customer_id = ?", customerID).
			Where("agent_id = ?", agentID).
			Where(sessionFilter(sessionID), sessionID).
			Where("started_at >= ?", from)
		if to != nil {
			query = query.Where("started_at <= ?", *to)
		}
		var spent CallUsage
		err := query.Scan(ctx, &spent)
		return spent, err
	})
	if err != nil {
		return CallUsage{}, stack.Wrap(fmt.Errorf("store: call usage: %w", err))
	}
	var spent CallUsage
	for _, answer := range answers {
		spent.InputTokens += answer.InputTokens
		spent.CachedInputTokens += answer.CachedInputTokens
		spent.OutputTokens += answer.OutputTokens
		spent.CostMicros += answer.CostMicros
		spent.Requests += answer.Requests
	}
	return spent, nil
}

// ModelTokens is what one model read and wrote over a call.
type ModelTokens struct {
	Modality          string     `bun:"modality"`
	Provider          string     `bun:"provider"`
	Model             string     `bun:"model"`
	InputTokens       int64      `bun:"input_tokens"`
	CachedInputTokens int64      `bun:"cached_input_tokens"`
	OutputTokens      int64      `bun:"output_tokens"`
	CostMicros        int64      `bun:"cost_micros"`
	OutputCostMicros  int64      `bun:"output_cost_micros"`
	Requests          int64      `bun:"requests"`
	InputParts        InputParts `bun:"embed:input_"`
}

// Tokens is what each model a call used read, wrote and cost, the busiest first. Models that
// bill by audio or characters rather than tokens read zero tokens. Matched like Usage, and
// read while the call is still going as well as after: to is nil for one that has not ended.
// Runtime is no model's requests.
func (r *Calls) Tokens(ctx context.Context, customerID, agentID, sessionID string, from time.Time, to *time.Time) ([]ModelTokens, error) {
	if customerID == "" || agentID == "" {
		return nil, stack.Wrap(errors.New("store: a customer and an agent id are required"))
	}

	lists, err := everyShard(ctx, r.store, func(ctx context.Context, db bun.IDB) ([]ModelTokens, error) {
		query := db.NewSelect().
			TableExpr("requests").
			ColumnExpr("modality, provider, model").
			ColumnExpr("COALESCE(SUM(input_tokens), 0) AS input_tokens").
			ColumnExpr("COALESCE(SUM(cached_input_tokens), 0) AS cached_input_tokens").
			ColumnExpr("COALESCE(SUM(output_tokens), 0) AS output_tokens").
			ColumnExpr("COALESCE(SUM(cost_micros), 0) AS cost_micros").
			ColumnExpr("COALESCE(SUM(output_cost_micros), 0) AS output_cost_micros").
			ColumnExpr(requestCount+" AS requests").
			ColumnExpr("COALESCE(SUM(input_instruction_tokens), 0) AS input_instruction_tokens").
			ColumnExpr("COALESCE(SUM(input_message_tokens), 0) AS input_message_tokens").
			ColumnExpr("COALESCE(SUM(input_tool_definition_tokens), 0) AS input_tool_definition_tokens").
			ColumnExpr("COALESCE(SUM(input_tool_use_tokens), 0) AS input_tool_use_tokens").
			ColumnExpr("COALESCE(SUM(input_image_tokens), 0) AS input_image_tokens").
			ColumnExpr("COALESCE(SUM(input_video_tokens), 0) AS input_video_tokens").
			Where("customer_id = ?", customerID).
			Where("agent_id = ?", agentID).
			Where(sessionFilter(sessionID), sessionID).
			Where("started_at >= ?", from).
			Group("modality", "provider", "model").
			OrderExpr("SUM(input_tokens + output_tokens) DESC, provider, model")
		if to != nil {
			query = query.Where("started_at <= ?", *to)
		}
		var used []ModelTokens
		err := query.Scan(ctx, &used)
		return used, err
	})
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: call tokens: %w", err))
	}
	if len(lists) == 1 {
		return lists[0], nil
	}

	var order []string
	sums := map[string]*ModelTokens{}
	for _, list := range lists {
		for _, m := range list {
			key := m.Modality + "\x00" + m.Provider + "\x00" + m.Model
			sum := sums[key]
			if sum == nil {
				sum = &ModelTokens{Modality: m.Modality, Provider: m.Provider, Model: m.Model}
				sums[key] = sum
				order = append(order, key)
			}
			sum.InputTokens += m.InputTokens
			sum.CachedInputTokens += m.CachedInputTokens
			sum.OutputTokens += m.OutputTokens
			sum.CostMicros += m.CostMicros
			sum.OutputCostMicros += m.OutputCostMicros
			sum.Requests += m.Requests
			sum.InputParts.InstructionTokens += m.InputParts.InstructionTokens
			sum.InputParts.MessageTokens += m.InputParts.MessageTokens
			sum.InputParts.ToolDefinitionTokens += m.InputParts.ToolDefinitionTokens
			sum.InputParts.ToolUseTokens += m.InputParts.ToolUseTokens
			sum.InputParts.ImageTokens += m.InputParts.ImageTokens
			sum.InputParts.VideoTokens += m.InputParts.VideoTokens
		}
	}
	var used []ModelTokens
	for _, key := range order {
		used = append(used, *sums[key])
	}
	slices.SortStableFunc(used, func(a, b ModelTokens) int {
		return cmp.Or(cmp.Compare(b.InputTokens+b.OutputTokens, a.InputTokens+a.OutputTokens),
			strings.Compare(a.Provider, b.Provider), strings.Compare(a.Model, b.Model))
	})
	return used, nil
}
