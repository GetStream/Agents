package store

import (
	"cmp"
	"path"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
)

// ActivityBucket is how much one bucket was used, and by how many people.
type ActivityBucket struct {
	Bucket       time.Time `bun:"bucket"`
	ActiveUsers  int64     `bun:"active_users"`
	Sessions     int64     `bun:"sessions"`
	Messages     int64     `bun:"messages"`
	Calls        int64     `bun:"calls"`
	VoiceMinutes float64   `bun:"voice_minutes"`
	PhoneMinutes float64   `bun:"phone_minutes"`
}

// ConcurrencyPeak is the most sessions a customer had open at once, and when that was
// first reached. At is nil when no session was open in the window.
type ConcurrencyPeak struct {
	Sessions int64      `bun:"sessions"`
	At       *time.Time `bun:"at"`
}

// ToolUsage is how often a tool ran and what it cost. Tool is empty on the row that sums
// every tool.
type ToolUsage struct {
	Tool   string `bun:"tool"`
	Calls  int64  `bun:"calls"`
	Failed int64  `bun:"failed"`
	// Sessions is how many sessions made at least one of the calls.
	Sessions int64 `bun:"sessions"`
	// AvgTurnsBeforeUse is how many turns a session took before its first call, nil on the
	// row over every tool.
	AvgTurnsBeforeUse *float64 `bun:"avg_turns_before_use"`
	// AvgTokens is what a call put in front of the model, its arguments and result,
	// estimated the way a prompt's composition counts tool use.
	AvgTokens float64 `bun:"avg_tokens"`
	// AvgMs and P99Ms are from the call starting to its result, nil when no call in the
	// group has both.
	AvgMs *float64 `bun:"avg_ms"`
	P99Ms *float64 `bun:"p99_ms"`
	// Loading, DefinitionTokens and SummaryTokens are how the latest call that recorded them
	// had the tool offered, and what its definition costs a turn whole and as a summary.
	// Empty and nil when no call did; SummaryTokens is set only for a summarised tool.
	Loading          string `bun:"loading"`
	DefinitionTokens *int64 `bun:"definition_tokens"`
	SummaryTokens    *int64 `bun:"summary_tokens"`
}

// SessionTurns is how many sessions took a turn and how many turns they took.
type SessionTurns struct {
	Sessions int64 `bun:"sessions"`
	Turns    int64 `bun:"turns"`
}

// ChildAgent is another of the app's text agents a text agent may hand work to, as it is
// stored in agent_configs.agents.
type ChildAgent struct {
	// Config is the name of the agent config the child runs from, in the same app.
	Config string `json:"config"`
	// Tool is the name the parent's model calls the child by.
	Tool string `json:"tool"`
	// Description is what the parent's model is told the child does.
	Description string `json:"description,omitempty"`
	// TimeoutMs is how long one call to the child may take. Zero is the default.
	TimeoutMs int `json:"timeout_ms,omitempty"`
	// MaxToolRounds is the child's agent.Options.MaxToolRounds: zero is the default, negative
	// is no limit.
	MaxToolRounds int `json:"max_tool_rounds,omitempty"`
}

// ConnectorBinding is one connector an agent config may call tools of, as it is stored in
// agent_configs.connectors.
//
// The JSON is the prototype's (ConnectorBinding in internal/store/models.go on
// codex/connector-support at cf62af0d), and boundByConfig finds a fixed
// binding by containment on {"connection": {"type": "fixed", "connection_id": ...}}, so a
// change to these tags is a change to that query. core.Binding is the same thing for the
// adapters, flattened and with a time.Duration; it has no JSON of its own, so it is not
// what is stored.
type ConnectorBinding struct {
	// Name is the alias the connector's tools are offered under.
	Name        string            `json:"name"`
	ConnectorID string            `json:"connector_id"`
	Connection  ConnectionBinding `json:"connection"`
	Tools       []ToolGrant       `json:"tools"`
	// ToolPatterns, when set, are the grant instead of Tools: tool names or path.Match
	// patterns. Nil is a binding that grants Tools. The API refuses an empty list. The field
	// is JSON inside agent_configs.connectors, so an older
	// router drops it and reads Tools, which a pattern binding leaves empty: it grants none.
	ToolPatterns *[]string `json:"tool_patterns,omitempty"`
	Required     bool      `json:"required"`
	// TimeoutMs is how long one tool call may take. Zero leaves the session's default.
	TimeoutMs int `json:"timeout_ms,omitempty"`
	// Events are the MCP events the binding's fixed connection is subscribed to, each opening
	// a conversation of its own when it arrives (internal/mcpevents). Empty subscribes to none.
	Events []BindingEvent `json:"events,omitempty"`
	// Policy is how the binding's calls behave around speech and interruptions. Nil is a
	// binding written before it existed, or without one, and behaves as one always has.
	Policy *BindingPolicy `json:"policy,omitempty"`
	// Sandbox is whether code in the session's sandbox reaches the connector's API with the
	// binding's connection, through the env var the connector's manifest names. The field is
	// JSON inside agent_configs.connectors, so an older router drops it and reads the binding
	// as one without sandbox access.
	Sandbox bool `json:"sandbox,omitempty"`
	// RequireApproval are tool names or path.Match patterns whose calls wait for a person to
	// allow each one before it is sent. The field is JSON inside agent_configs.connectors, so
	// an older router drops it and sends those calls with nobody asked.
	RequireApproval []string `json:"require_approval,omitempty"`
	// Loading is how the binding's tools are offered: ToolLoadingFull, ToolLoadingSummary or
	// ToolLoadingDeferred. Empty is full.
	Loading string `json:"loading,omitempty"`
	// OutputRetention is how long the turns a session records keep what the binding's tools
	// were given and gave back: options.RetentionNone, a duration, or empty for as long as the
	// session. The app's and the organization's policies can shorten it.
	OutputRetention options.Retention `json:"output_retention,omitempty"`
}

// The ways a tool is offered. Full sends its whole description and input schema from the
// start; summary sends the first line of its description and a schema without descriptions,
// and the first call returns the rest; deferred offers nothing until the model finds the tool
// with search_tool and loads it with load_tool.
const (
	ToolLoadingFull     = "full"
	ToolLoadingSummary  = "summary"
	ToolLoadingDeferred = "deferred"
)

// ToolGroup is a named set of functions an SDK provides, as agent_configs.tool_groups stores it.
type ToolGroup struct {
	Name string `json:"name"`
	// Loading is ToolLoadingFull, ToolLoadingSummary or ToolLoadingDeferred. Empty is full.
	Loading string `json:"loading,omitempty"`
	// Tools are the functions' names.
	Tools []string `json:"tools"`
	// OutputRetention is ConnectorBinding.OutputRetention for the group's functions.
	OutputRetention options.Retention `json:"output_retention,omitempty"`
}

// RequiresApproval reports whether the binding's require_approval names tool, by its name at
// the provider.
func (b ConnectorBinding) RequiresApproval(tool string) bool {
	return slices.ContainsFunc(b.RequireApproval, func(pattern string) bool {
		matched, _ := path.Match(pattern, tool)
		return matched
	})
}

// The values of BindingPolicy.OnInterrupt. Empty lets the call outlive an interruption, and
// leaves it to be stopped when the caller withdraws what it was asked.
const (
	// InterruptCancel stops waiting for the call when the turn is interrupted, as Pipecat
	// does by default (cancel_on_interruption=True).
	InterruptCancel = "cancel"
	// InterruptWait lets the call finish after an interruption; its result goes into the
	// history.
	InterruptWait = "wait"
)

// BindingPolicy is a binding's policy envelope, as it was written: a field left out is
// stored empty and means its default.
type BindingPolicy struct {
	// PreSpeech is what the agent says while one of the binding's tools runs, in place of
	// the phrase it would pick itself. Empty leaves the agent's own.
	PreSpeech string `json:"pre_speech,omitempty"`
	// OnInterrupt is InterruptCancel, InterruptWait, or empty for neither.
	OnInterrupt string `json:"on_interrupt,omitempty"`
	// Cancellable is whether the provider is told to stop a call the session stopped
	// waiting for. Nil is true.
	Cancellable *bool `json:"cancellable,omitempty"`
}

// BindingEvent is one MCP event a binding subscribes to on its connection's server.
type BindingEvent struct {
	// Event is the event's name, as the server's events/list gives it.
	Event string `json:"event"`
	// Arguments are the event's filters, as its inputSchema describes them.
	Arguments map[string]any `json:"arguments,omitempty"`
	// Instructions say what the agent does with the event when it arrives.
	Instructions string `json:"instructions,omitempty"`
}

// LeftOutConnector is one connector a sync left out, as it is stored in
// agent_configs.left_out_connectors.
type LeftOutConnector struct {
	// Name is the alias agent.yaml gave it.
	Name        string `json:"name"`
	ConnectorID string `json:"connector_id"`
	// Connection is the id or label agent.yaml named, which no app connection has.
	Connection string `json:"connection"`
}

// ConnectionBinding selects the connection a binding's tools are called through: "fixed",
// an app-owned connection named by ConnectionID, or "session", the verified end user's own,
// picked when a session is created and so with no ConnectionID here.
type ConnectionBinding struct {
	Type         string `json:"type"`
	ConnectionID string `json:"connection_id,omitempty"`
}

// ToolGrant is one exact tool a binding allows, pinned to the digest of the schema it was
// granted against.
type ToolGrant struct {
	Name         string `json:"name"`
	SchemaDigest string `json:"schema_digest"`
}

// How a sender on a channel becomes an end user.
const (
	// ChannelIdentityPhone makes the sender an end user of their own, phone:+1555..., so
	// anybody who writes is answered. What they say is theirs and nobody else's.
	ChannelIdentityPhone = "phone"
	// ChannelIdentityLink answers only a number somebody has tied to an end user with a
	// code, which is what an agent reading a person's own calendar or orders needs.
	ChannelIdentityLink = "link"
)

// AgentChannels are the lines an agent answers on outside a Stream Chat channel.
type AgentChannels struct {
	WhatsApp *ChannelLine `json:"whatsapp,omitempty"`
	SMS      *ChannelLine `json:"sms,omitempty"`
	IMessage *ChannelLine `json:"imessage,omitempty"`
	// Identity is how a sender becomes an end user: phone or link. Empty is phone.
	Identity string `json:"identity,omitempty"`
}

// ChatChannel is one Stream Chat channel type an agent config answers in, stored in
// agent_configs.chat_channels. An object rather than the type's name alone, so how the agent
// answers in channels of the type can be said beside it.
type ChatChannel struct {
	// Type is the channel type's name, as Stream Chat names it in a cid (type:id).
	Type string `json:"type"`
	// Answer is which messages in a channel of the type are answered: ChatChannelAnswerAlways,
	// as empty is, or ChatChannelAnswerMentioned.
	Answer string `json:"answer,omitempty"`
}

// The messages a ChatChannel answers: every one, or only one that mentions the agent.
const (
	ChatChannelAnswerAlways    = "always"
	ChatChannelAnswerMentioned = "mentioned"
)

// AnswersOnlyMentionsIn is whether the config answers a message in a channel of the type given
// only when the message mentions the agent.
func (c AgentConfig) AnswersOnlyMentionsIn(channelType string) bool {
	for _, channel := range c.Channels.Chat {
		if channel.Type == channelType {
			return channel.Answer == ChatChannelAnswerMentioned
		}
	}
	return false
}

// AgentScheduling is an agent config's scheduled-task settings, stored in
// agent_configs.scheduling.
type AgentScheduling struct {
	// Enabled offers the scheduling tools to the agent's sessions. Off unless a
	// config turns it on: it adds four tools to every turn.
	Enabled bool `json:"enabled,omitempty"`
	// MaxTasksPerUser caps one user's active tasks. Zero is the default of 25.
	MaxTasksPerUser int `json:"max_tasks_per_user,omitempty"`
	// DefaultTimezone is the zone a task takes when neither the user nor their
	// session has ever named one.
	DefaultTimezone string `json:"default_timezone,omitempty"`
}

// DefaultMaxActiveTasks is the cap on a user's active tasks when the config sets none.
const DefaultMaxActiveTasks = 25

// MaxActive is how many active tasks one user may hold on this config.
func (a AgentScheduling) MaxActive() int {
	if a.MaxTasksPerUser > 0 {
		return a.MaxTasksPerUser
	}
	return DefaultMaxActiveTasks
}

// AgentCalls is an agent config's call tools, stored in agent_configs.calls. A nil tool
// was never set, which is its default.
type AgentCalls struct {
	EndCall  *EndCallSettings  `json:"end_call,omitempty"`
	Press    *PressSettings    `json:"press,omitempty"`
	Transfer *TransferSettings `json:"transfer,omitempty"`
}

// EndCallSettings is the end_call tool: off unless a config turns it on.
type EndCallSettings struct {
	Enabled bool `json:"enabled"`
	// When tells the agent when to end the call.
	When string `json:"when,omitempty"`
}

// PressSettings is the tool that presses keys on the call: on unless a config turns it off.
type PressSettings struct {
	Enabled bool `json:"enabled"`
}

// TransferSettings is the transfer tool: on unless a config turns it off.
type TransferSettings struct {
	Enabled      bool                  `json:"enabled"`
	Destinations []TransferDestination `json:"destinations,omitempty"`
}

// TransferDestination is a number the agent may transfer a call to.
type TransferDestination struct {
	Name   string `json:"name"`
	Number string `json:"number"`
	// When tells the agent when to transfer here.
	When string `json:"when,omitempty"`
	// Warm announces the caller to the person answering before connecting them.
	Warm bool `json:"warm,omitempty"`
}

// EndCallOn is whether end_call is offered: off when never set.
func (c AgentCalls) EndCallOn() bool { return c.EndCall != nil && c.EndCall.Enabled }

// PressOn is whether press is offered: on when never set.
func (c AgentCalls) PressOn() bool { return c.Press == nil || c.Press.Enabled }

// TransferOn is whether transfer is offered: on when never set.
func (c AgentCalls) TransferOn() bool { return c.Transfer == nil || c.Transfer.Enabled }

// Lines are the channels named, by kind.
func (c AgentChannels) Lines() map[string]string {
	named := map[string]string{}
	for kind, line := range map[string]*ChannelLine{
		"whatsapp": c.WhatsApp, "sms": c.SMS, "imessage": c.IMessage,
	} {
		if line != nil && line.Number != "" {
			named[kind] = line.Number
		}
	}
	return named
}

// ChannelLine is one channel an agent is reachable on.
type ChannelLine struct {
	// Number is the line, which has to be one the app connected.
	Number string `json:"number"`
}

// Which way a call, a leg of one or a text went.
const (
	// Inbound is somebody ringing the agent.
	Inbound = "inbound"
	// Outbound is the agent ringing somebody, which is what a campaign does.
	Outbound = "outbound"
)

// ActivityGranularity selects how wide an activity bucket is. It is separate from
// Granularity, and coarser, because distinct users cannot be summed: a month of them is
// who came back rather than the sum of its days, so a month has to be counted as a month.
type ActivityGranularity string

const (
	ActivityDaily   ActivityGranularity = "daily"
	ActivityMonthly ActivityGranularity = "monthly"
)

// truncateUnit returns the Postgres date_trunc unit for the granularity.
func (g ActivityGranularity) truncateUnit() string {
	if g == ActivityMonthly {
		return "month"
	}
	return "day"
}

// Valid reports whether the granularity is one this store knows.
func (g ActivityGranularity) Valid() bool {
	return g == ActivityDaily || g == ActivityMonthly
}

// FloorControl is the model that controls an agent's floor, hearing the audio: when a
// caller's turn has ended, and what is done about speech over the agent. The zero value is no
// choice: the agent gets the router's default, the flow controller unless the router defaults
// every agent to chorus-asr (config.Agent.FloorControlDefault). An agent that chose decides for
// itself whatever the router's default is: FloorControlChorusASR, or FloorControlFlowController
// to keep the flow controller. MaxWaitMs is the most a caller who has finished may wait for the
// answer: the point of the server's measured trade-off with the fewest cut-ins whose p90 wait
// is within it (audiofloor.Policy.Resolve). Zero is the server's default point. It is
// chorus-asr's alone.
type FloorControl struct {
	Model     string `json:"model,omitempty"`
	MaxWaitMs int    `json:"max_wait_ms,omitempty"`
}

// FloorControlChorusASR has chorus-asr control the floor, the one model there is, named as
// the stream/chorus-asr transcriber is.
const FloorControlChorusASR = "stream/chorus-asr"

// FloorControlFlowController keeps the flow controller, which rules the floor where no model
// does, whatever the router's default: the explicit opt-out, as distinct from no choice. It is
// named for what rules the floor, as FloorControlChorusASR is, and not "default", which would
// now mean two things.
const FloorControlFlowController = "flow-controller"

// IsZero reports whether no model was chosen, which leaves the choice to the router's default.
// It is also what the nullzero columns read: an opt-out is stored, and is not zero.
func (f FloorControl) IsZero() bool {
	return f == FloorControl{}
}

// ResumeHistory is how much of a conversation a model is handed back when a session opens
// on it: reopened, resumed by id, joined by voice or forked. A field left unset is no
// opinion, and the zero value is the router's default: replies as text, the newest 100 turns
// within 60000 characters. It is set at four levels, the most specific winning field by
// field (Over): a session, its agent config, its app's policy, its organization's.
type ResumeHistory struct {
	// IncludeTools hands each reply back with the tool calls it made and what they
	// returned, as they were recorded. Over MaxCharacters the oldest results are trimmed
	// first, and the conversation's first message is kept. Nil is no opinion, so a level
	// can turn off what a broader one turned on.
	IncludeTools *bool `json:"include_tools,omitempty"`
	// MaxMessages is how many turns are handed back, zero for 100. Tool calls and their
	// results are not turns.
	MaxMessages int `json:"max_messages,omitempty"`
	// MaxCharacters is how much text is handed back, tool calls and results included, zero
	// for 60000.
	MaxCharacters int `json:"max_characters,omitempty"`
}

// Over is r with base's settings wherever r has none.
func (r ResumeHistory) Over(base ResumeHistory) ResumeHistory {
	if r.IncludeTools == nil {
		r.IncludeTools = base.IncludeTools
	}
	r.MaxMessages = cmp.Or(r.MaxMessages, base.MaxMessages)
	r.MaxCharacters = cmp.Or(r.MaxCharacters, base.MaxCharacters)
	return r
}

// Tools reports whether replies are handed back with their tool calls.
func (r ResumeHistory) Tools() bool {
	return r.IncludeTools != nil && *r.IncludeTools
}

// The kinds of end user a row records. They are the verified kinds of auth.Kind: an
// anonymous caller goes by a name nobody checked, so recording it would be recording the
// claim rather than the person.
const (
	UserKindGuest         = "guest"
	UserKindAuthenticated = "authenticated"
)

// User is an end user an app has been seen acting for.
//
// The row is not what makes them work -- a user is a real Stream user, and chat and video
// need nothing here -- it is what lets an app ask who its users are, and what makes
// claiming a guest possible. Claiming moves what a guest said onto a real account, so it
// has to be an operation only a backend may ask for, and that needs a record of which ids
// were ever guests and which have already been claimed.
type User struct {
	bun.BaseModel `bun:"table:users,alias:u"`

	CustomerID string `bun:"customer_id,pk"`
	// ID is the Stream user id, which the caller holds and sends back to claim.
	ID string `bun:"id,pk"`
	// Kind is what the credential proved them to be, UserKindGuest or
	// UserKindAuthenticated. Only a guest may be claimed.
	Kind string `bun:"kind,notnull"`
	// Minted says the router handed this guest out (Users.InsertGuest). A guest Users.Seen
	// wrote from a guest token is not: an app signs those itself, for any id it likes.
	Minted bool `bun:"minted,notnull"`
	// MintedElsewhere says an import brought this guest as one another deployment's router
	// minted. It is the importing app's word, so it decides which role the guest's tokens say
	// (api.seenAsGuest), that they are a guest to what asks Minted, that their own token leaves
	// them one (RecordAccount) and which kind a backfill records as their conversation's owner's,
	// and never that the id is this customer's.
	MintedElsewhere bool   `bun:"minted_elsewhere,nullzero"`
	Name            string `bun:"name,notnull"`
	// Timezone is the IANA zone the user was last explicitly known to be in,
	// which a scheduled task resolves to when nothing nearer names one. Empty
	// when nobody has ever said.
	Timezone  string         `bun:"timezone"`
	Custom    map[string]any `bun:"custom,type:jsonb,nullzero"`
	CreatedAt time.Time      `bun:"created_at,notnull"`
	// Email is the address the app's backend last said this user signs in with
	// (auth.EmailHeader), empty while it has said none. Only a server-side caller can say it.
	Email string `bun:"email,nullzero"`
	// Shard is the shard that last served a request the user's own client sent (Users.Served),
	// empty while none has.
	Shard string `bun:"shard,nullzero"`
	// ClaimedBy is the real user this guest turned out to be, empty while they are still a
	// guest. A guest is claimed once: a second claim naming somebody else would move one
	// person's conversations onto another's account.
	ClaimedBy string `bun:"claimed_by,nullzero"`
	// ClaimFinishedAt is when every step of ClaimedBy's claim ran (Users.FinishGuestClaim), nil
	// while a claim that set ClaimedBy has not got that far.
	ClaimFinishedAt *time.Time `bun:"claim_finished_at"`
	// RefreshSecretHash is the SHA-256 of the refresh secret this guest was last handed
	// (GuestSecretHash), nil for one handed none. Never sent anywhere: it is the credential
	// that lets a guest whose token expired come back as themselves.
	RefreshSecretHash []byte `bun:"refresh_secret_hash,nullzero" json:"-"`
	// RefreshSecretExpiresAt is when RefreshSecretHash stops working (GuestSecretTTL after it was
	// issued or last rotated). Meaningless on a row holding no secret.
	RefreshSecretExpiresAt *time.Time `bun:"refresh_secret_expires_at,nullzero"`
	// PreviousRefreshSecretHash is the secret the last rotation spent, nil when the secret held
	// was not rotated from another. Presented again, it says the secret leaked (RotateGuestSecret).
	// Kept, with RefreshSecretHash nil, as the mark of a secret a replay cleared.
	PreviousRefreshSecretHash []byte `bun:"previous_refresh_secret_hash,nullzero" json:"-"`
	// RefreshFamilySecretHash is the SHA-256 of the family of RefreshSecretHash's secret
	// (GuestSecretFamilyHash), which every rotation keeps. Nil for a secret from before families.
	RefreshFamilySecretHash []byte `bun:"refresh_family_secret_hash,nullzero" json:"-"`
}

// CreatedPosition is the last row of a page ordered by when rows were made.
type CreatedPosition struct {
	At time.Time `json:"t"`
	ID string    `json:"id"`
}
