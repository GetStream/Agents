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
	"unicode"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/GetStream/Vision-Agents/acceleration/internal/auth"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// What a session is doing, as a stored row reports it.
const (
	// SessionRunning is a session the router still holds.
	SessionRunning = "running"
	// SessionClosed is one that ended, however it ended.
	SessionClosed = "closed"
)

// How the user took part in a session. It only moves up: a session that has seen video
// stays video.
const (
	ModalityText  = "text"
	ModalityVoice = "voice"
	ModalityVideo = "video"
)

// ModelOverwrites is what a caller asked to change about the models for one session.
//
// It is one object rather than a dozen top-level fields because it is one idea: everything
// here overrides what the agent config already decided, and a caller reading the session
// back wants to see what they changed in one place rather than diffed against a config they
// would have to fetch. The route fields name targets the deployment's own catalogue
// resolves; Thinking, Temperature and MaxOutputTokens are folded into the LLM options the
// same way the router's own overrides are.
type ModelOverwrites struct {
	LLM    string `json:"llm,omitempty"`
	STT    string `json:"stt,omitempty"`
	TTS    string `json:"tts,omitempty"`
	STS    string `json:"sts,omitempty"`
	Search string `json:"search,omitempty"`
	// Thinking is how hard to reason: off, low, medium or high. It becomes the reasoning
	// effort on the LLM options, which is what the providers that support one are sent.
	Thinking        string   `json:"thinking,omitempty"`
	Temperature     *float64 `json:"temperature,omitempty"`
	MaxOutputTokens *int     `json:"max_output_tokens,omitempty"`
	Verbosity       string   `json:"verbosity,omitempty"`
}

// Empty reports whether the caller asked to change nothing, which is the usual case and
// worth not writing a row for.
func (m ModelOverwrites) Empty() bool {
	return m == ModelOverwrites{}
}

// ForkPoint is where a fork's history out of its parent stops.
type ForkPoint struct {
	// Messages is whether the fork carried its parent's history at all.
	Messages bool `json:"messages"`
	// ResponseID is the parent's response the carried history stops at, empty for all of it.
	ResponseID string `json:"response_id,omitempty"`
}

// AgentSession is one session a caller opened, kept so it can be found after the process
// that held it is gone.
//
// Deliberately not a Call. A session holds a conversation, typed and spoken, and has a call
// for each stretch of it held on one (Calls.Latest). A call row carries numbers and a
// direction and the review a finished call gets, which a text conversation somebody opened
// from a page has none of.
//
// What was said is not here either. The transcript is in Stream Chat under ConversationID
// and the turn timings are in Turn; this row is the handle that ties them together and
// carries what the caller labelled the conversation with.
type AgentSession struct {
	bun.BaseModel `bun:"table:agent_sessions,alias:asn"`

	// ID is the session id the caller already holds the session by.
	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	// StreamAppPK is the Stream app this was made in, and the one it is finished in. Zero,
	// stored as NULL, is the deployment's own app.
	StreamAppPK int64 `bun:"stream_app_pk,nullzero"`
	// ConfigID names the stored config, and AgentName the name it was found by. The name
	// is kept as well because it is what a caller filters on, and because renaming a
	// config must not rewrite what older sessions were opened against.
	ConfigID  string `bun:"config_id,nullzero"`
	AgentName string `bun:"agent_name,notnull"`
	// AgentID is the transcript channel and ConversationID the same thing as a CID.
	AgentID        string `bun:"agent_id,notnull"`
	ConversationID string `bun:"conversation_id,notnull"`
	// UserID is whose session it is; CallerKind says what that name is worth.
	UserID     string `bun:"user_id,notnull"`
	CallerKind string `bun:"caller_kind,notnull"`
	// APIKey and UsageCategories are the key, or origin, and the categories the session was
	// opened with, which its usage is recorded under for its whole life. A session carried on
	// from its row keeps them; one from before they were kept has no key.
	APIKey          string   `bun:"api_key,notnull"`
	UsageCategories []string `bun:"usage_categories,array,nullzero"`
	// Team is the team in UsageCategories, a column of its own so a list narrowed to one team
	// reads an index. Empty for a session counted under none. It never changes.
	Team        string `bun:"team,notnull"`
	Title       string `bun:"title,notnull"`
	Description string `bun:"description,notnull"`
	// Project is what the conversation belongs to.
	Project string `bun:"project,notnull"`
	// ProjectTag is the project the session's usage is tagged with unless its config's tags, or
	// explicit ones, name another: the last a server-side caller named, which a device's fork may
	// have named another Project over. Empty for none. Written as the session opens, or is carried
	// on from a row made before it was kept, where it is nil (ReadProjectTag), and never changed.
	ProjectTag *string `bun:"project_tag"`
	// Custom is the caller's own, handed back untouched and never read by the router.
	Custom          map[string]any  `bun:"custom,type:jsonb,nullzero"`
	ModelOverwrites ModelOverwrites `bun:"model_overwrites,type:jsonb,nullzero"`
	// ForkedFrom is the session this one continued from, empty for one opened fresh.
	ForkedFrom string `bun:"forked_from,nullzero"`
	// ForkPoint is how much of its parent's history a fork carried, which a fork carried on from
	// its row reads again. Nil for a session that is not a fork, and for a fork made before it
	// was kept, which is carried on without its parent's turns.
	ForkPoint *ForkPoint `bun:"fork_point,type:jsonb"`
	// ConnectorSelections are the connections the caller picked for the config's session
	// bindings, which a fork re-resolves. References only, never a credential.
	ConnectorSelections []SessionConnectorSelection `bun:"connector_selections,type:jsonb,notnull"`
	// ReturnURL is where the consents the session asks for in the chat go back to. Empty,
	// stored as NULL, is the dashboard. Never shown in the session's own answer.
	ReturnURL string `bun:"return_url,nullzero"`
	// NoUI is a session opened with ui false, whose model is not told its config's
	// UIInstructions, which reopening it keeps. False, stored as NULL, is the default.
	NoUI bool `bun:"no_ui,nullzero"`
	// Remembers is whether the session was opened with a memory user id, so it could have
	// taught the memory store anything. Nil, as on a row made before it was kept, is not known.
	Remembers *bool `bun:"remembers"`
	// ResumeHistory is what the session was opened with, which reopening it keeps. Zero
	// takes the config's.
	ResumeHistory ResumeHistory `bun:"resume_history,type:jsonb,nullzero"`
	// FloorControl is what the session was opened with over its config's, which reopening
	// it keeps. Zero takes the config's, and the router's default after it.
	FloorControl FloorControl `bun:"floor_control,type:jsonb,nullzero"`
	// ToolTimeoutMs is the tool_timeout_ms the session runs with, which reopening it keeps.
	// Zero, stored as NULL, is the default.
	ToolTimeoutMs int `bun:"tool_timeout_ms,nullzero"`
	// CallerTools are the tools the session's caller declared, which reopening it offers the
	// model again. Definitions only: what a call returned is in the session's items.
	CallerTools []CallerTool `bun:"caller_tools,type:jsonb,nullzero"`
	State       string       `bun:"state,notnull"`
	// Modality is ModalityText, ModalityVoice or ModalityVideo.
	Modality  string    `bun:"modality,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
	// OpenedAt is when the router running the session opened it, first or carried on since.
	// Zero, stored as NULL, on a row from before it was kept.
	OpenedAt time.Time `bun:"opened_at,nullzero"`
	// ClosedAt is nil while the session is still running.
	ClosedAt *time.Time `bun:"closed_at"`
	// EndReason is why it ended (session.EndReason), empty while it runs and for one that
	// ended before reasons were kept.
	EndReason      string     `bun:"end_reason,notnull"`
	LastResponseAt *time.Time `bun:"last_response_at"`
	// PinnedAt is when the session was pinned, nil for one that is not. Pinning is not a
	// change to the conversation, so it moves neither UpdatedAt nor LastResponseAt.
	PinnedAt *time.Time `bun:"pinned_at"`
	// ReviewScore is the score the review of the session's call gave it, from 1 to 5, kept
	// here as well so a list filters and sorts on it without joining calls. Nil until the
	// review is written, and for a session nobody spoke in.
	ReviewScore *int `bun:"review_score"`
	// Rank is how well a search matched, zero outside a search.
	Rank float32 `bun:"rank,scanonly"`
	// Region is the shard the session lives in, as a list read it; empty on a deployment of
	// one unnamed shard, and on a row read any other way.
	Region string `bun:"shard,scanonly"`
}

// Validate refuses a session no list or reopen could read back. An empty state or modality is
// the default Upsert fills in.
func (s *AgentSession) Validate() error {
	return validate("session").
		required("id", s.ID).
		required("customer_id", s.CustomerID).
		oneOf("state", s.State, "", SessionRunning, SessionClosed).
		oneOf("modality", s.Modality, "", ModalityText, ModalityVoice, ModalityVideo).
		check(s.ToolTimeoutMs >= 0, "tool_timeout_ms", "is negative").
		check(s.ReviewScore == nil || *s.ReviewScore >= 1 && *s.ReviewScore <= 5, "review_score", "is not from 1 to 5").
		done()
}

// TaggedProject is what ProjectTag says, empty for none. For a row
// from before it was kept it is the session's project if a server-side caller opened it, except
// for a fork: its project may be its parent's, which anybody may have named.
func (s *AgentSession) TaggedProject() string {
	switch {
	case s.ProjectTag != nil:
		return *s.ProjectTag
	case s.ForkedFrom == "" && s.CallerKind == string(auth.KindServer):
		return s.Project
	}
	return ""
}

// ErrProjectTagUnread is a session whose project tag a shard that did not answer may say.
var ErrProjectTagUnread = errors.New("store: a shard did not answer for a session's project tag")

// ErrNoSession is a session that is not there: never saved, or deleted since.
var ErrNoSession = errors.New("store: there is no session")

func unknownStoredSession(id string) error {
	return stack.Wrap(fmt.Errorf("%w %s", ErrNoSession, id))
}

// CallerTool is one tool a session's caller declared, as it was declared, so a reopened
// session offers and runs it as the first one did.
type CallerTool struct {
	Name          string              `json:"name"`
	Description   string              `json:"description"`
	Parameters    map[string]any      `json:"parameters,omitempty"`
	Client        bool                `json:"client,omitempty"`
	DisplayTitle  string              `json:"display_title,omitempty"`
	Approval      *CallerToolApproval `json:"approval,omitempty"`
	Interruptible bool                `json:"interruptible,omitempty"`
	TimeoutMs     int64               `json:"timeout_ms,omitempty"`
}

// CallerToolApproval is the question a person answers before a caller tool's call runs.
type CallerToolApproval struct {
	Title          string `json:"title"`
	Message        string `json:"message,omitempty"`
	ReasonArgument string `json:"reason_argument,omitempty"`
	AllowTitle     string `json:"allow_title,omitempty"`
	DeclineTitle   string `json:"decline_title,omitempty"`
}

// SessionConnectorSelection is the connection a session's caller picked for one session
// binding of its config, by the binding's alias.
type SessionConnectorSelection struct {
	Name         string `json:"name"`
	ConnectionID string `json:"connection_id"`
}

// SessionPosition is the last session of a page, by every key any order sorts on, so one
// position serves whichever order the list has.
type SessionPosition struct {
	// LastMessageAt is when the agent last answered, or when the session began.
	LastMessageAt time.Time `json:"t"`
	ID            string    `json:"id"`
	Rank          float32   `json:"r,omitempty"`
	Pinned        bool      `json:"p,omitempty"`
	// ReviewScore is nil for a session nobody reviewed, which sorts after every score.
	ReviewScore *int `json:"v,omitempty"`
}

// SessionFilter narrows a session list to the ones worth reading.
//
// UserID is not a field a caller sets freely: the handler fills it from the credential for
// anybody who is not the app's own backend, because a filter a user could widen is not a
// filter at all.
type SessionFilter struct {
	UserID  string
	AgentID string
	// CallerKinds, when set, admits only the sessions opened by one of these kinds of
	// caller. A user id is only worth what proved it, so an end user is narrowed by both
	// halves of who they are, the same as a session they reach by id.
	CallerKinds []string
	// Where is the filter the request spelled out, with its $and and $or. Nil admits every
	// session. It is ANDed with every field above, so nothing in it reaches past them: an
	// end user's UserID and CallerKinds hold whatever the expression says.
	Where *SessionExpr
	// Order is what the list is sorted by. The zero value is the most recent message first.
	// A search is always sorted by relevance, whatever is here.
	Order SessionOrder
	Limit int
	// Cursor starts the page after this session, in Order. Nil is the first page.
	Cursor *SessionPosition
}

// How many sessions are handed back at once. A person scrolling a sidebar reads a page at a
// time, and a backend exporting everything pages rather than asking for it all.
const (
	defaultSessionLimit = 25
	maxSessionLimit     = 200
)

// SessionLimit is the page size a session or turn list uses for the limit asked for.
//
// The queries return one row more than this, so a caller can tell the page is not the last
// without counting.
func SessionLimit(asked int) int { return ClampLimit(asked, defaultSessionLimit, maxSessionLimit) }

// ClampLimit is the page size for the limit asked for: fallback when none was, at most most.
func ClampLimit(asked, fallback, most int) int {
	if asked <= 0 {
		return fallback
	}
	return min(asked, most)
}

// Sessions are the sessions callers opened, in the shard that ran them.
type Sessions struct {
	store *Store
}

// Upsert records that a session was opened.
//
// Written when the session starts rather than when it ends, so a session that is still
// running is findable and one the process died during is not simply lost. Saving the same
// session twice updates the row: a retried write must not become a second conversation. It
// also opens a text session that ended again, which is how one is carried on under its id.
// Saving one under an id another customer's row has is ErrIDTaken, and leaves that row alone.
//
// Incognito sessions never get here. The manager does not call this for one, which is what
// the flag means -- not a row that is hidden, but no row at all.
func (r *Sessions) Upsert(ctx context.Context, session *AgentSession) error {
	if err := session.Validate(); err != nil {
		return stack.Wrap(err)
	}
	now := time.Now().UTC()
	if session.CreatedAt.IsZero() {
		session.CreatedAt = now
	}
	session.UpdatedAt = now
	if session.State == "" {
		session.State = SessionRunning
	}
	if session.Modality == "" {
		session.Modality = ModalityText
	}
	session.Custom = orEmptyMap(session.Custom)
	// The column's own default, which a nil slice would write as JSON null.
	session.ConnectorSelections = orEmpty(session.ConnectorSelections)

	// No conflict target is named: every unique index arbitrates the insert, so it neither fails
	// on one it did not name nor depends on which of them the table has. A row there already is
	// then updated only if it is the customer's own. Twice at most: a row deleted between the
	// insert and the update is inserted again.
	for range 2 {
		result, err := r.store.shard.NewInsert().Model(session).On("CONFLICT DO NOTHING").Exec(ctx)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: save session: %w", err))
		}
		written, err := result.RowsAffected()
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: save session: %w", err))
		}
		if written == 1 {
			return r.store.SessionHomes.record(ctx, []SessionHome{{CustomerID: session.CustomerID, SessionID: session.ID, OpenedAt: session.CreatedAt}})
		}
		result, err = r.store.shard.NewUpdate().With("saved", r.store.shard.NewValues(session)).
			Model((*AgentSession)(nil)).TableExpr("saved").
			Set("title = saved.title").
			Set("description = saved.description").
			Set("custom = saved.custom").
			Set("conversation_id = saved.conversation_id").
			Set("state = saved.state").
			Set("closed_at = NULL").
			Set("end_reason = ''").
			Set("updated_at = saved.updated_at").
			Set("opened_at = saved.opened_at").
			// A reopened session's tool settings are kept for the kind of caller that opened it (the
			// row's caller_kind, which a reopen does not change): an end user's device carrying on
			// its backend's conversation neither clears the backend's tools nor sets its timeout.
			Set("tool_timeout_ms = CASE WHEN ?TableAlias.caller_kind = saved.caller_kind " +
				"THEN saved.tool_timeout_ms ELSE ?TableAlias.tool_timeout_ms END").
			Set("caller_tools = CASE WHEN ?TableAlias.caller_kind = saved.caller_kind " +
				"THEN saved.caller_tools ELSE ?TableAlias.caller_tools END").
			// A session reopened from its row is opened without the memory it had, and what it
			// remembered before is still in the store, so once true it stays true. Not known
			// stays not known: NULL OR false is NULL.
			Set("remembers = ?TableAlias.remembers OR saved.remembers").
			Set("project_tag = COALESCE(?TableAlias.project_tag, saved.project_tag)").
			Where("?TableAlias.customer_id = saved.customer_id").
			Where("?TableAlias.id = saved.id").
			// A write of a run opened before the one the row holds is late, from a router that held
			// the session earlier, and changes nothing: the close that router queued next would
			// otherwise close the run opened since. A write with no time is a router's from before
			// opened_at was kept.
			Where("saved.opened_at IS NULL OR ?TableAlias.opened_at IS NULL OR ?TableAlias.opened_at <= saved.opened_at").
			Exec(ctx)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: save session: %w", err))
		}
		written, err = result.RowsAffected()
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: save session: %w", err))
		}
		if written == 1 {
			// Again on an update, so a session whose first write failed to record its home
			// records it on the retry.
			return r.store.SessionHomes.record(ctx, []SessionHome{{CustomerID: session.CustomerID, SessionID: session.ID, OpenedAt: session.CreatedAt}})
		}
		// Nothing written: the row is another customer's, or this customer's own of a run
		// opened since, which a late write leaves as it is.
		own, err := r.store.shard.NewSelect().Model((*AgentSession)(nil)).
			Where("id = ?", session.ID).
			Where("customer_id = ?", session.CustomerID).
			Exists(ctx)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: save session: %w", err))
		}
		if own {
			return nil
		}
	}
	return stack.Wrap(fmt.Errorf("%w: session %s", ErrIDTaken, session.ID))
}

// SetToolTimeout records the tool_timeout_ms a running session was given since it opened, by
// a caller of kind. Only the kind that opened it changes the row, as on Upsert. Zero is the
// default.
func (r *Sessions) SetToolTimeout(ctx context.Context, customerID, id, kind string, milliseconds int) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a session id are required"))
	}
	var timeout *int
	if milliseconds > 0 {
		timeout = &milliseconds
	}
	_, err := r.store.shard.NewUpdate().Model((*AgentSession)(nil)).
		Set("tool_timeout_ms = ?", timeout).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("caller_kind = ?", kind).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: set session tool timeout: %w", err))
	}
	return nil
}

// Close records that a session ended, and why: empty when nothing said. One that already
// ended keeps the first time and reason: a session is left once, and a second close is the
// same leaving reported again. opened is when the router closing it opened it: a row another
// router opened again since, carrying the session on, is that router's to close.
func (r *Sessions) Close(ctx context.Context, customerID, id string, opened, at time.Time, reason string) error {
	if customerID == "" || id == "" || opened.IsZero() {
		return stack.Wrap(errors.New("store: a customer, a session id and when it opened are required"))
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	_, err := r.store.shard.NewUpdate().Model((*AgentSession)(nil)).
		Set("closed_at = ?", at).
		Set("updated_at = ?", at).
		Set("state = ?", SessionClosed).
		Set("end_reason = ?", reason).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("closed_at IS NULL").
		Where("opened_at IS NULL OR opened_at <= ?", opened).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: close session: %w", err))
	}
	return nil
}

// SawVideo records that the agent saw the user's video on a session. The session's clock is
// left alone: the turn that looked at the video moves it.
func (r *Sessions) SawVideo(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a session id are required"))
	}

	_, err := r.store.shard.NewUpdate().Model((*AgentSession)(nil)).
		Set("modality = ?", ModalityVideo).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("modality <> ?", ModalityVideo).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: saw video: %w", err))
	}
	return nil
}

// Voiced records that voice was started on a session: a session that was text becomes a
// voice one; one that saw video stays a video one. The call it joined is a row of its own
// (Calls.Start).
func (r *Sessions) Voiced(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a session id are required"))
	}

	_, err := r.store.shard.NewUpdate().Model((*AgentSession)(nil)).
		Set("modality = ?", ModalityVoice).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("modality = ?", ModalityText).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: session voiced: %w", err))
	}
	return nil
}

// Describe changes what a session is called. It is separate from Upsert because renaming a
// conversation is something a person does long after it ended, when there is no spec left to
// save.
func (r *Sessions) Describe(ctx context.Context, customerID, id, title, description string, custom map[string]any) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a session id are required"))
	}

	query := r.store.shard.NewUpdate().Model((*AgentSession)(nil)).
		Set("updated_at = ?", time.Now().UTC()).
		Set("title = ?", title).
		Set("description = ?", description).
		Where("id = ?", id).
		Where("customer_id = ?", customerID)
	if custom != nil {
		query = query.Set("custom = ?", jsonbOf(custom))
	}
	result, err := query.Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: describe session: %w", err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: describe session: %w", err))
	}
	if affected == 0 {
		return unknownStoredSession(id)
	}
	return nil
}

// ChooseConnection records connectionID as the session's selection for the connector binding
// called name, replacing one it had, in one statement so two writes for different names never
// drop each other's. A fork or a reopened chat re-resolves it as it does the selections the
// session was opened with. A session with no row, such as an incognito one, is left as it is.
func (r *Sessions) ChooseConnection(ctx context.Context, customerID, id, name, connectionID string) error {
	if customerID == "" || id == "" || name == "" || connectionID == "" {
		return stack.Wrap(errors.New("store: a customer, a session, a binding name and a connection id are required"))
	}
	_, err := r.store.shard.NewUpdate().Model((*AgentSession)(nil)).
		Set("updated_at = ?", time.Now().UTC()).
		Set(`connector_selections = (
			SELECT coalesce(jsonb_agg(chosen), '[]'::jsonb) FROM jsonb_array_elements(connector_selections) AS chosen
			WHERE chosen->>'name' <> ?) || jsonb_build_array(jsonb_build_object('name', ?::text, 'connection_id', ?::text))`,
			name, name, connectionID).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: choose session connection: %w", err))
	}
	return nil
}

// Exists reports whether any customer has a session with this id, in this shard or, by its
// home, in another.
func (r *Sessions) Exists(ctx context.Context, id string) (bool, error) {
	exists, err := r.store.shard.NewSelect().Model((*AgentSession)(nil)).Where("id = ?", id).Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: session exists: %w", err))
	}
	if exists {
		return true, nil
	}
	return r.store.SessionHomes.taken(ctx, id)
}

// Get returns one session a customer ran in this shard. A session that lives in another shard
// is read there (SessionHomes).
func (r *Sessions) Get(ctx context.Context, customerID, id string) (AgentSession, error) {
	if customerID == "" || id == "" {
		return AgentSession{}, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}

	var session AgentSession
	err := r.store.shard.NewSelect().Model(&session).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentSession{}, unknownStoredSession(id)
	}
	if err != nil {
		return AgentSession{}, stack.Wrap(fmt.Errorf("store: stored session: %w", err))
	}
	return session, nil
}

// ReadProjectTag gives a row that does not say its project tag the one it has. Since a session
// stays in its home shard, the row in hand already says who opened it.
func (r *Sessions) ReadProjectTag(_ context.Context, session *AgentSession) error {
	if session.ProjectTag != nil {
		return nil
	}
	session.ProjectTag = new(session.TaggedProject())
	return nil
}

// ConversationPin is the Stream app a customer's conversation was kept in, read off the
// newest session that held it. Found is false for a conversation no session of theirs did.
func (r *Sessions) ConversationPin(ctx context.Context, customerID, conversationID string) (int64, bool, error) {
	type pinned struct {
		Pin       sql.NullInt64 `bun:"stream_app_pk"`
		CreatedAt time.Time     `bun:"created_at"`
	}
	var newest []pinned
	err := r.store.shard.NewSelect().Model((*AgentSession)(nil)).Column("stream_app_pk", "created_at").
		Where("customer_id = ?", customerID).
		Where("conversation_id = ?", conversationID).
		OrderExpr("created_at DESC").
		Limit(1).
		Scan(ctx, &newest)
	if err != nil {
		return 0, false, stack.Wrap(fmt.Errorf("store: conversation pin: %w", err))
	}
	if len(newest) == 0 {
		return 0, false, nil
	}
	return newest[0].Pin.Int64, true, nil
}

// HeldConversation is a customer's conversation a session held.
type HeldConversation struct {
	CustomerID     string `bun:"customer_id"`
	ConversationID string `bun:"conversation_id"`
}

// HeldConversations pages through the conversations this shard's sessions held, by customer and
// then conversation, from after the one given: the zero value starts at the first. Only
// customerID's when it is not empty. For a job that walks them all, which each shard does for
// its own.
func (r *Sessions) HeldConversations(ctx context.Context, customerID string, after HeldConversation, limit int) ([]HeldConversation, error) {
	query := r.store.shard.NewSelect().Model((*AgentSession)(nil)).
		Distinct().Column("customer_id", "conversation_id").
		Where("conversation_id <> ''").
		Where("(customer_id, conversation_id) > (?, ?)", after.CustomerID, after.ConversationID).
		OrderExpr("customer_id, conversation_id").
		Limit(limit)
	if customerID != "" {
		query = query.Where("customer_id = ?", customerID)
	}
	var held []HeldConversation
	if err := query.Scan(ctx, &held); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: held conversations: %w", err))
	}
	return held, nil
}

// CallerKinds are the kinds of caller whose sessions held a customer's conversation under a
// user id, across this shard and the shared index of the others, each once.
func (r *Sessions) CallerKinds(ctx context.Context, customerID, conversationID, userID string) ([]string, error) {
	var kinds []string
	err := r.store.shard.NewSelect().Model((*AgentSession)(nil)).
		Distinct().Column("caller_kind").
		Where("customer_id = ?", customerID).
		Where("conversation_id = ?", conversationID).
		Where("user_id = ?", userID).
		Scan(ctx, &kinds)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: caller kinds: %w", err))
	}
	if !r.store.Indexed() {
		return kinds, nil
	}
	var others []string
	err = r.store.global.NewSelect().TableExpr("session_index").
		Distinct().Column("caller_kind").
		Where("customer_id = ?", customerID).
		Where("conversation_id = ?", conversationID).
		Where("user_id = ?", userID).
		Where("shard <> ?", r.store.ShardName()).
		Scan(ctx, &others)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: caller kinds: %w", err))
	}
	for _, kind := range others {
		if !slices.Contains(kinds, kind) {
			kinds = append(kinds, kind)
		}
	}
	return kinds, nil
}

// HeldAsGuest reports whether a guest whose token said the role held a session under a customer's
// user id, on this shard or in the shared index of the others. Such a user's next token says the
// role though other sessions they held may say none: before a refreshed guest token kept the
// role, a guest came back from a refresh as a signed-in user.
func (r *Sessions) HeldAsGuest(ctx context.Context, customerID, userID string) (bool, error) {
	held, err := r.store.shard.NewSelect().Model((*AgentSession)(nil)).
		Where("customer_id = ?", customerID).
		Where("user_id = ?", userID).
		Where("caller_kind = ?", UserKindGuest).
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: held as guest: %w", err))
	}
	if held || !r.store.Indexed() {
		return held, nil
	}
	held, err = r.store.global.NewSelect().TableExpr("session_index").
		Where("customer_id = ?", customerID).
		Where("user_id = ?", userID).
		Where("caller_kind = ?", UserKindGuest).
		Where("shard <> ?", r.store.ShardName()).
		Exists(ctx)
	if err != nil {
		return false, stack.Wrap(fmt.Errorf("store: held as guest: %w", err))
	}
	return held, nil
}

// Delete deletes one of a customer's sessions, and its turns and their items with it. A
// session that is not there is not an error: a retried delete has nothing left to do.
func (r *Sessions) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a session id are required"))
	}

	// The documents first: they hold the person's files, and have no foreign key to go with
	// the session by.
	_, err := r.store.shard.NewDelete().Model((*ConversationDocument)(nil)).
		Where("session_id = ?", id).
		Where("customer_id = ?", customerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete session documents: %w", err))
	}
	// The copies of hidden messages go with the session, and the changes recorded of them,
	// which hold the same words, are emptied (forgottenChanges). A data-modifying WITH runs
	// whether or not it is read.
	changes := forgottenChanges(r.store.shard, customerID, "agent_branch_messages").Where("payload->>'session_id' = ?", id)
	_, err = r.store.shard.NewDelete().With("changes", changes).Model((*AgentSession)(nil)).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete session: %w", err))
	}
	if err := r.store.unindex(ctx, indexedSessions, customerID, id); err != nil {
		return err
	}
	return r.store.SessionHomes.forget(ctx, customerID, id)
}

// Select returns a customer's sessions in the filter's order, the most recent message first
// unless it says otherwise, narrowed by the filter.
//
// This is the structured half of finding an old conversation: everything here is an exact
// match, so the answer is the same every time and a page can hold a cursor into it. Search is
// the other half, for when what the caller has is words rather than a filter.
func (r *Sessions) Select(ctx context.Context, customerID string, filter SessionFilter) ([]AgentSession, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}
	// Relevance is a search's: without words there is no rank to sort on.
	if filter.Order.By == SessionSortRelevance {
		filter.Order.By = SessionSortLastMessage
	}

	lists, err := indexedLists(ctx, r.store, indexedSessions, customerID, func(query *bun.SelectQuery) *bun.SelectQuery {
		query = query.ColumnExpr("?TableColumns").Where("?TableAlias.customer_id = ?", customerID)
		query = orderSessions(query, filter.Order, "", nil, filter.Cursor)
		return narrowSessions(query, filter)
	}, inRegion)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: query sessions: %w", err))
	}
	return pageOfSessions(lists, filter), nil
}

// Search returns the sessions whose title, description, project or agent match the words
// given, best match first.
//
// Only what the caller wrote is searched, not the transcript. Searching what was said means
// either reading every conversation out of Stream Chat on every query or keeping a second
// copy of every message here, and the first is too slow to offer while the second is a
// transcript that can drift from the real one. Titles and descriptions are what a person
// names a conversation with, and naming them is the thing to encourage.
//
// An empty query is the same as no query: it falls through to Select rather than matching
// nothing, because a search box a person has not typed in yet should show them their
// conversations.
func (r *Sessions) Search(ctx context.Context, customerID, text string, filter SessionFilter) ([]AgentSession, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}
	if text == "" {
		return r.Select(ctx, customerID, filter)
	}

	words, args := searchedFor(text)
	rank := "ts_rank(searchable, " + words + ")"
	filter.Order = SessionOrder{By: SessionSortRelevance}
	lists, err := indexedLists(ctx, r.store, indexedSessions, customerID, func(query *bun.SelectQuery) *bun.SelectQuery {
		query = query.ColumnExpr("?TableColumns").
			ColumnExpr(rank+" AS rank", args...).
			Where("?TableAlias.customer_id = ?", customerID).
			Where("searchable @@ "+words, args...)
		query = orderSessions(query, filter.Order, rank, args, filter.Cursor)
		return narrowSessions(query, filter)
	}, inRegion)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: search sessions: %w", err))
	}
	return pageOfSessions(lists, filter), nil
}

// searchedFor is what a search's text asks for, as a tsquery expression and its arguments.
//
// websearch_to_tsquery rather than to_tsquery, because the text comes from a search box: it
// takes quoted phrases and bare words and never fails on punctuation, where to_tsquery would
// answer a syntax error to somebody who typed an apostrophe.
//
// A search box also searches as the person types, so a word at the very end is the start of
// one and matches as a prefix: "Lis" finds "Lisbon". It is only letters and digits, which is
// what lets it go to to_tsquery. A word followed by a space, inside a quote or excluded with a
// minus was finished, and websearch_to_tsquery reads it as written.
func searchedFor(text string) (string, []any) {
	runes := []rune(text)
	start := len(runes)
	for start > 0 && (unicode.IsLetter(runes[start-1]) || unicode.IsDigit(runes[start-1])) {
		start--
	}
	head := string(runes[:start])
	if start == len(runes) || strings.Count(head, "\"")%2 == 1 || strings.HasSuffix(head, "-") {
		return "websearch_to_tsquery('english', ?)", []any{text}
	}
	return "(websearch_to_tsquery('english', ?) && to_tsquery('english', ? || ':*'))",
		[]any{head, string(runes[start:])}
}

// inRegion names the shard a session read from this one lives in.
func inRegion(session *AgentSession, shard string) { session.Region = shard }

// pageOfSessions merges the pages indexedLists read into one in the filter's order. Each is one
// longer than asked, so the merged one still tells the caller whether there is more.
func pageOfSessions(lists [][]AgentSession, filter SessionFilter) []AgentSession {
	if len(lists) == 1 {
		return lists[0]
	}
	sessions := merged(lists, func(a, b AgentSession) int {
		return filter.Order.Compare(a.Position(), b.Position())
	}, SessionLimit(filter.Limit)+1)
	return sessions
}

// narrowSessions applies a filter to either kind of session query, so the two cannot drift
// apart in what they admit. That matters more than the duplication it saves: a filter
// honoured by query and forgotten by search is one a caller uses to read somebody else's
// conversations.
func narrowSessions(query *bun.SelectQuery, filter SessionFilter) *bun.SelectQuery {
	query = query.Limit(SessionLimit(filter.Limit) + 1)

	if filter.UserID != "" {
		query = query.Where("user_id = ?", filter.UserID)
	}
	if len(filter.CallerKinds) > 0 {
		query = query.Where("caller_kind IN (?)", bun.In(filter.CallerKinds))
	}
	if filter.AgentID != "" {
		query = query.Where("agent_id = ?", filter.AgentID)
	}
	// What the request asked for goes in last and as one parenthesised condition, ANDed with
	// everything above: an $or in it chooses among the sessions the caller may see, and
	// never adds one.
	if filter.Where != nil {
		condition, args := filter.Where.where()
		query = query.Where(condition, args...)
	}
	return query
}

// Pin pins one of a customer's sessions, or unpins it, and returns when it was pinned: nil
// once it is not. Pinning a session that is already pinned keeps the time it was first
// pinned at. Nothing else about the row moves, its updated_at included: a pin is where a
// person keeps a conversation, not something that happened in it.
func (r *Sessions) Pin(ctx context.Context, customerID, id string, pinned bool, at time.Time) (*time.Time, error) {
	if customerID == "" || id == "" {
		return nil, stack.Wrap(errors.New("store: a customer and a session id are required"))
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}

	type pin struct {
		PinnedAt *time.Time `bun:"pinned_at"`
	}
	query := r.store.shard.NewUpdate().Model((*AgentSession)(nil)).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Returning("pinned_at")
	if pinned {
		query = query.Set("pinned_at = COALESCE(pinned_at, ?)", at)
	} else {
		query = query.Set("pinned_at = NULL")
	}
	var pins []pin
	if _, err := query.Exec(ctx, &pins); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: pin session: %w", err))
	}
	if len(pins) == 0 {
		return nil, unknownStoredSession(id)
	}
	return pins[0].PinnedAt, nil
}

// SessionEnding is how many of some apps' sessions are still running, or ended for one
// reason.
type SessionEnding struct {
	Running   bool   `bun:"running"`
	EndReason string `bun:"end_reason"`
	Sessions  int64  `bun:"sessions"`
}

// Endings counts the sessions apps opened from since on, over every shard, by whether they
// still run and why they ended: running ones first, then by reason.
func (r *Sessions) Endings(ctx context.Context, apps []string, since time.Time) ([]SessionEnding, error) {
	db, table := r.store.sessionList()
	var endings []SessionEnding
	err := db.NewSelect().
		TableExpr(table).
		ColumnExpr("closed_at IS NULL AS running, end_reason, COUNT(*) AS sessions").
		Where("customer_id IN (?)", bun.In(apps)).
		Where("created_at >= ?", since).
		GroupExpr("1, 2").
		OrderExpr("1 DESC, 2").
		Scan(ctx, &endings)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: session endings: %w", err))
	}
	return endings, nil
}

// sessionList is the database and table a list of every shard's sessions reads: the shared
// copy when lists are indexed (Store.ShipIndex), this shard's own otherwise.
func (s *Store) sessionList() (bun.IDB, string) {
	if s.Indexed() {
		return s.global, indexedSessions.index
	}
	return s.shard, indexedSessions.table
}

// Latest is the limit sessions apps opened most recently from since on, over every shard,
// newest first. Only the id, the app, the agent, the modality, when it opened and how it
// ended are read.
func (r *Sessions) Latest(ctx context.Context, apps []string, since time.Time, limit int) ([]AgentSession, error) {
	// Each app's newest from its customer index, so at most limit rows of each app are read
	// however many it opened in the window, and then ordered and cut.
	db, table := r.store.sessionList()
	query := `
SELECT asn.*
FROM unnest(?::text[]) AS app(id)
CROSS JOIN LATERAL (
    SELECT id, customer_id, agent_name, modality, created_at, closed_at, end_reason
    FROM ` + table + `
    WHERE customer_id = app.id AND created_at >= ?
    ORDER BY created_at DESC, id DESC
    LIMIT ?
) AS asn
ORDER BY created_at DESC, id DESC
LIMIT ?`
	var sessions []AgentSession
	if err := db.NewRaw(query, pgdialect.Array(apps), since, limit, limit).Scan(ctx, &sessions); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: latest sessions: %w", err))
	}
	return sessions, nil
}

// SessionEnd is what this shard knows of whether a session is over.
type SessionEnd struct {
	// Known says the session has a row; ClosedAt is when it closed, nil while open.
	Known    bool
	ClosedAt *time.Time
	// Running says it recorded runtime since the time asked about: what tells a session with
	// no row, such as an incognito one, from one that is gone.
	Running bool
}

// Ended reports whether the session is over: closed, or unknown here and silent.
func (e SessionEnd) Ended() bool {
	return e.ClosedAt != nil || !e.Known && !e.Running
}

// SessionKey is a session by its customer and id. The id is one the customer chose, so it
// names a session only within that customer.
type SessionKey struct {
	CustomerID string
	ID         string
}

// Ends is what this shard knows of whether each of keys is over, a session with no row
// counting as running while it has recorded runtime since since.
func (r *Sessions) Ends(ctx context.Context, keys []SessionKey, since time.Time) (map[SessionKey]SessionEnd, error) {
	ends := map[SessionKey]SessionEnd{}
	if len(keys) == 0 {
		return ends, nil
	}
	var sessions []struct {
		CustomerID string     `bun:"customer_id"`
		ID         string     `bun:"id"`
		ClosedAt   *time.Time `bun:"closed_at"`
	}
	customers, ids := sessionKeyColumns(keys)
	if err := r.store.shard.NewSelect().TableExpr("agent_sessions").Column("customer_id", "id", "closed_at").
		Where("(customer_id, id) IN (SELECT * FROM unnest(?::text[], ?::text[]))", customers, ids).
		Scan(ctx, &sessions); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: session ends: %w", err))
	}
	for _, session := range sessions {
		ends[SessionKey{session.CustomerID, session.ID}] = SessionEnd{Known: true, ClosedAt: session.ClosedAt}
	}
	var unknown []SessionKey
	for _, key := range keys {
		if _, known := ends[key]; !known {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return ends, nil
	}
	var running []struct {
		CustomerID string `bun:"customer_id"`
		SessionID  string `bun:"session_id"`
	}
	customers, ids = sessionKeyColumns(unknown)
	err := r.store.shard.NewSelect().TableExpr("requests").ColumnExpr("DISTINCT customer_id, session_id").
		Where("modality = ?", RuntimeModality).
		Where("(customer_id, session_id) IN (SELECT * FROM unnest(?::text[], ?::text[]))", customers, ids).
		Where("started_at >= ?", since).
		Scan(ctx, &running)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: session ends: %w", err))
	}
	for _, key := range unknown {
		ends[key] = SessionEnd{}
	}
	for _, key := range running {
		ends[SessionKey{key.CustomerID, key.SessionID}] = SessionEnd{Running: true}
	}
	return ends, nil
}

// sessionKeyColumns is keys as the two arrays an unnest pairs back up.
func sessionKeyColumns(keys []SessionKey) (customers, ids any) {
	customerIDs, sessionIDs := make([]string, len(keys)), make([]string, len(keys))
	for i, key := range keys {
		customerIDs[i], sessionIDs[i] = key.CustomerID, key.ID
	}
	return pgdialect.Array(customerIDs), pgdialect.Array(sessionIDs)
}

// jsonbOf renders a map for a jsonb comparison. A map that will not encode is sent as an
// empty object rather than failing the query: the values come from a query string, and the
// worst an unencodable one can mean is that nothing matches it.
func jsonbOf[V any](values map[string]V) string {
	encoded, err := json.Marshal(values)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}
