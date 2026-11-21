package store

import (
	"cmp"
	"path"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
)

// Request is one recorded unit of work, stored so billing, cost and health can all be
// derived from the same rows.
type Request struct {
	bun.BaseModel `bun:"table:requests,alias:r"`

	ID int64 `bun:"id,pk,autoincrement"`
	// UsageID is the unit of work, chosen before the row is first written, so writing it
	// again is a no-op rather than a second charge. Rows from before it was kept have none.
	// RecordRequests refuses a row without one; nullzero leaves a missing one to the column's
	// random default, which is what a router from before it gets.
	UsageID  uuid.UUID `bun:"usage_id,type:uuid,nullzero"`
	Modality string    `bun:"modality,notnull"`
	// CustomerID owns the request. Every statistic is keyed by it.
	CustomerID string `bun:"customer_id,notnull"`
	// OrganizationID is who is billed for it. The rest say who asked: OrganizationTier as
	// Stream sent it, APIKey the key or the origin of work no key started, UserID empty when
	// there is none, and UsageCategories what the customer counts the work under (nil
	// writes none). APIKey names a key, never holds its secret: a row the writer loses is
	// logged in full.
	OrganizationID   string   `bun:"organization_id,notnull"`
	OrganizationTier string   `bun:"organization_tier,notnull"`
	APIKey           string   `bun:"api_key,notnull"`
	UserID           string   `bun:"user_id,notnull"`
	UsageCategories  []string `bun:"usage_categories,array,nullzero"`
	// LimitGrants are the grants the work was admitted on past a spent limit, which its
	// billable cost is drawn from (20261112121000_requests_limit_grants.sql).
	LimitGrants []int64 `bun:"limit_grants,array,nullzero"`
	// AgentID is the agent the work was done for. Empty outside a conversation.
	AgentID string `bun:"agent_id,nullzero"`
	// SessionID is the agent session the work ran in, empty outside one.
	SessionID string `bun:"session_id,notnull"`
	// CallID is the call the work happened in. Empty outside a conversation.
	CallID string `bun:"call_id,nullzero"`
	// Tags are the customer's own cost labels, aggregated into the tag rollups so spend
	// can be broken down by whatever the customer labelled it with.
	Tags      map[string]string `bun:"tags,type:jsonb,nullzero"`
	Provider  string            `bun:"provider,notnull"`
	Model     string            `bun:"model,notnull"`
	StartedAt time.Time         `bun:"started_at,notnull"`
	// OperationID correlates a provider request with the turn or synthesis that caused it.
	OperationID string `bun:"operation_id,nullzero"`
	Purpose     string `bun:"purpose,nullzero"`
	TurnID      string `bun:"turn_id,nullzero"`
	// DurationMs is the full request; LatencyMs is the first useful output.
	DurationMs *float64 `bun:"duration_ms"`
	// AudioMs is billable audio, transcribed or produced.
	AudioMs int64 `bun:"audio_ms,notnull"`
	// Characters is billable text.
	Characters int64 `bun:"characters,notnull"`
	// InputTokens is the whole prompt an LLM read, cached part included.
	InputTokens int64 `bun:"input_tokens,notnull"`
	// CachedInputTokens is the part of the prompt served from the provider's cache.
	CachedInputTokens int64 `bun:"cached_input_tokens,notnull"`
	// OutputTokens is everything an LLM generated, reasoning included.
	OutputTokens int64 `bun:"output_tokens,notnull"`
	// InputParts is what the prompt was made of, estimated and scaled to InputTokens.
	InputParts InputParts `bun:"embed:input_"`
	// Images is how many pictures were drawn.
	Images int64 `bun:"images,notnull"`
	// Meters are the priced quantities with no column of their own: cache writes, speech to
	// speech by modality, requests, pixels.
	Meters pricing.Quantities `bun:"meters,type:jsonb,nullzero"`
	// Attributes chose the price (a service tier, a country) or explain the row.
	Attributes map[string]string `bun:"attributes,type:jsonb,nullzero"`
	LatencyMs  *float64          `bun:"latency_ms"`
	// CostMicros is millionths of a dollar: what the work is charged at its entry's prices.
	CostMicros int64 `bun:"cost_micros,notnull"`
	// OutputCostMicros is the part of CostMicros the generated tokens were priced at.
	OutputCostMicros int64 `bun:"output_cost_micros,notnull"`
	// PriceVersion names what priced the row: the price change that wrote its entry, the
	// embedded or file list a router prices with before it reads the database, "quoted" for
	// an amount the vendor quoted outright, and empty for local or unpriced work.
	PriceVersion string `bun:"price_version,notnull"`
	// SKU is the priced item's, as its entry names it: empty for work no entry matched.
	SKU string `bun:"sku,notnull"`
	// Region and BYOK are the priced item's, whether or not an entry matched it: where the
	// work ran, and whether on the customer's own key. Rows from before them read "" and false.
	Region string `bun:"region,notnull"`
	BYOK   bool   `bun:"byok,notnull"`
	// BaseCostMicros is the cost at the vendor's rates, for reference, so the margin on the
	// row is CostMicros - BaseCostMicros.
	BaseCostMicros int64 `bun:"base_cost_micros,notnull"`
	// Billable is false for free and local work, which counts toward no spend.
	Billable     bool   `bun:"billable,notnull"`
	Success      bool   `bun:"success,notnull"`
	ErrorCode    string `bun:"error_code,nullzero"`
	ErrorMessage string `bun:"error_message,nullzero"`
	// ToShip is true until the usage service has the row; ShipError is why it refused it.
	ToShip    bool       `bun:"to_ship,notnull"`
	ShippedAt *time.Time `bun:"shipped_at"`
	ShipError string     `bun:"ship_error,notnull"`
}

// Quantities is what the row is priced by: the typed columns as pricing meters, the prompt
// split into tokens read fresh, read from cache and written to it, then Meters. Zero
// quantities are left out.
func (r Request) Quantities() pricing.Quantities {
	used := pricing.Quantities{}
	add := func(meter pricing.Meter, quantity int64) {
		if quantity != 0 {
			used[meter] += quantity
		}
	}
	// Cached and cache-written tokens are part of the prompt, so they are billed once at
	// their own rate rather than twice.
	fresh := max(r.InputTokens-r.CachedInputTokens-r.Meters[pricing.CacheWriteInputTokens], 0)
	add(pricing.AudioMs, r.AudioMs)
	add(pricing.Characters, r.Characters)
	add(pricing.InputTokens, fresh)
	add(pricing.CachedInputTokens, r.CachedInputTokens)
	add(pricing.OutputTokens, r.OutputTokens)
	add(pricing.Images, r.Images)
	for meter, quantity := range r.Meters {
		add(meter, quantity)
	}
	return used
}

// InputParts splits an LLM prompt by what it carried. No provider reports this, so it is
// estimated from the request and scaled to the provider's count: the parts sum to the
// prompt's tokens, and only the split between them is a guess.
type InputParts struct {
	InstructionTokens    int64 `bun:"instruction_tokens,notnull"`
	MessageTokens        int64 `bun:"message_tokens,notnull"`
	ToolDefinitionTokens int64 `bun:"tool_definition_tokens,notnull"`
	ToolUseTokens        int64 `bun:"tool_use_tokens,notnull"`
	ImageTokens          int64 `bun:"image_tokens,notnull"`
	VideoTokens          int64 `bun:"video_tokens,notnull"`
}

// Bucket is one aggregated row from stats_hourly or stats_daily.
type Bucket struct {
	Modality               string    `bun:"modality"`
	CustomerID             string    `bun:"customer_id"`
	Provider               string    `bun:"provider"`
	Model                  string    `bun:"model"`
	Bucket                 time.Time `bun:"bucket"`
	AudioMsTotal           int64     `bun:"audio_ms_total"`
	CharactersTotal        int64     `bun:"characters_total"`
	InputTokensTotal       int64     `bun:"input_tokens_total"`
	CachedInputTokensTotal int64     `bun:"cached_input_tokens_total"`
	OutputTokensTotal      int64     `bun:"output_tokens_total"`
	ImagesTotal            int64     `bun:"images_total"`
	CostMicrosTotal        int64     `bun:"cost_micros_total"`
	RequestCount           int64     `bun:"request_count"`
	ErrorCount             int64     `bun:"error_count"`
	LatencyP50Ms           *float64  `bun:"latency_p50_ms"`
	LatencyP95Ms           *float64  `bun:"latency_p95_ms"`
	Uptime                 *float64  `bun:"uptime"`
}

// SpendBucket is what one group cost in one bucket, across every modality. Value is a
// modality or a cost label value; "other" is everything outside the biggest few and the
// empty string is spend that carried no such label, so the rows still add up to the total.
type SpendBucket struct {
	Bucket          time.Time `bun:"bucket"`
	Value           string    `bun:"value"`
	CostMicrosTotal int64     `bun:"cost_micros_total"`
	RequestCount    int64     `bun:"request_count"`
}

// TagKeySummary is one cost label key, with what it covers and its largest values. It is
// what lets a reader be shown the breakdown that means something: the keys are the
// customer's own, so which of them identifies a product and which an end customer is
// only visible in how many values each was used with and how much of the bill it covers.
type TagKeySummary struct {
	Key             string
	ValueCount      int64
	CostMicrosTotal int64
	RequestCount    int64
	// Coverage is the share of the window's requests carrying this key, from 0 to 1.
	Coverage  float64
	TopValues []TagValueSummary
}

// TagValueSummary is one value of a cost label key, and its share of what that key covers.
type TagValueSummary struct {
	Value           string
	CostMicrosTotal int64
	RequestCount    int64
	Share           float64
}

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

// TurnSample is what a finished turn adds to its hour's stats. The legs are pointers
// because a pipeline need not have all of them, and a leg that never happened is left out
// of the percentiles rather than counted as instant.
type TurnSample struct {
	CustomerID  string
	AgentID     string
	StartedAt   time.Time
	Interrupted bool
	AudioOutMs  float64
	// STTLatencyMs is the provider's decode time for the transcript that settled the turn.
	STTLatencyMs *float64
	// LLMTTFTMs is the wait between asking the model and its first token.
	LLMTTFTMs *float64
	// TTSTTFBMs is the wait between sending the first sentence and the first audio.
	TTSTTFBMs *float64
	// RoundtripMs is the whole delay: settled transcript to first audio published.
	RoundtripMs *float64
}

// TurnBucket is one agent's turns in an hour or a day. The percentiles are read off a
// histogram, so they are within 5% of the true ones.
type TurnBucket struct {
	CustomerID       string    `bun:"customer_id"`
	AgentID          string    `bun:"agent_id"`
	Bucket           time.Time `bun:"bucket"`
	TurnCount        int64     `bun:"turn_count"`
	InterruptedCount int64     `bun:"interrupted_count"`
	AudioOutMsTotal  float64   `bun:"audio_out_ms_total"`
	STTLatencyP50Ms  *float64  `bun:"-"`
	STTLatencyP95Ms  *float64  `bun:"-"`
	LLMTTFTP50Ms     *float64  `bun:"-"`
	LLMTTFTP95Ms     *float64  `bun:"-"`
	TTSTTFBP50Ms     *float64  `bun:"-"`
	TTSTTFBP95Ms     *float64  `bun:"-"`
	RoundtripP50Ms   *float64  `bun:"-"`
	RoundtripP95Ms   *float64  `bun:"-"`
	RoundtripP99Ms   *float64  `bun:"-"`
}

// TagBucket is one aggregated row from stats_tags_hourly or stats_tags_daily: what the
// requests carrying one label cost in one time bucket.
type TagBucket struct {
	Modality               string    `bun:"modality"`
	CustomerID             string    `bun:"customer_id"`
	TagKey                 string    `bun:"tag_key"`
	TagValue               string    `bun:"tag_value"`
	Bucket                 time.Time `bun:"bucket"`
	AudioMsTotal           int64     `bun:"audio_ms_total"`
	CharactersTotal        int64     `bun:"characters_total"`
	InputTokensTotal       int64     `bun:"input_tokens_total"`
	CachedInputTokensTotal int64     `bun:"cached_input_tokens_total"`
	OutputTokensTotal      int64     `bun:"output_tokens_total"`
	ImagesTotal            int64     `bun:"images_total"`
	CostMicrosTotal        int64     `bun:"cost_micros_total"`
	RequestCount           int64     `bun:"request_count"`
	ErrorCount             int64     `bun:"error_count"`
	LatencyP50Ms           *float64  `bun:"latency_p50_ms"`
	LatencyP95Ms           *float64  `bun:"latency_p95_ms"`
	Uptime                 *float64  `bun:"uptime"`
}

// AgentConfig is a named set of the decisions a session is created with.
//
// It holds only what a caller would otherwise repeat on every call. Everything that is
// about one conversation rather than the agent behind it, the call id above all, stays in
// the create request: a config is who the agent is, not which call it is on.
type AgentConfig struct {
	bun.BaseModel `bun:"table:agent_configs,alias:ac"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	Name       string `bun:"name,notnull"`
	// Role is what the config is for in the app around it, beyond answering people:
	// RoleAgentModerator, or empty for an ordinary agent. One live config per customer holds a
	// role, test copies aside (20261103120000_agent_config_role.sql).
	Role string `bun:"role,notnull"`
	// Mode is whether the agent is spoken to or written to: AgentModeVoice or
	// AgentModeText. A text agent uses neither speech target and joins no call.
	Mode string `bun:"mode,notnull"`
	// Region is where the agent's work is done, such as pricing.RegionEU: every model its
	// sessions run, its knowledge base and its sandbox are there, and nothing of it is done
	// elsewhere (20261202135000_agent_region.sql). Empty is stored as pricing.DefaultRegion.
	Region string `bun:"region,notnull"`
	// STT, TTS, LLM, Subagent and Search are routing targets. Empty leaves the session
	// default. STS names one native audio model that hears and speaks for itself; naming
	// one makes the agent native, and the three cascade targets are then not used.
	STT            string `bun:"stt,notnull"`
	TTS            string `bun:"tts,notnull"`
	STS            string `bun:"sts,notnull"`
	Voice          string `bun:"voice,notnull"`
	LLM            string `bun:"llm,notnull"`
	Subagent       string `bun:"subagent,notnull"`
	VideoSource    string `bun:"video_source,notnull"`
	VideoMaxFrames int    `bun:"video_max_frames,notnull"`
	Search         string `bun:"search,notnull"`
	// Image is what the agent draws pictures with. Empty offers no drawing tool.
	Image        string `bun:"image,notnull"`
	Instructions string `bun:"instructions,notnull"`
	Greeting     string `bun:"greeting,notnull"`

	// LLMFallbacks are the targets a turn falls back to, in order, when LLM fails it. LLM
	// is still the first choice; nil falls back only among LLM's own candidates.
	LLMFallbacks []string `bun:"llm_fallbacks,type:jsonb"`

	// Agents are the app's other text agents this one may hand work to, each offered to its
	// model as a tool. Only a text agent has them, and none of them has any of its own.
	Agents []ChildAgent `bun:"agents,type:jsonb"`

	// GreetingMode is GreetingExact, said word for word, or GreetingVariation, which the
	// model rewords on every call.
	GreetingMode string `bun:"greeting_mode,notnull"`
	// Guardrail is a guardrail.md: frontmatter saying how to screen a turn, then the
	// policy in prose. Empty, which most configs are, means every turn is answered.
	Guardrail string `bun:"guardrail,notnull"`
	// ReviewInstructions is what the model reviewing a finished call judges it by, in place
	// of the default scoring guide. Empty, which most configs are, is the default.
	ReviewInstructions string `bun:"review_instructions,notnull"`
	// UIInstructions teach the model to answer with UI, such as agent.yaml's ui: compiled by
	// the SDK. Apart from Instructions so a session can leave them out (AgentSession.NoUI).
	UIInstructions string `bun:"ui_instructions,notnull"`
	// Skills names entries in the skill registry rather than carrying their instructions,
	// so editing a skill changes every config that uses it.
	Skills []string `bun:"skills,type:jsonb"`
	// Connectors are the connectors this agent may call tools of, each under an alias.
	Connectors []ConnectorBinding `bun:"connectors,type:jsonb"`
	// LeftOutConnectors are the connectors agent.yaml named that the last sync left out of
	// Connectors, because the app has no connection by the id or label they name. Only a
	// sync writes them, as the SDK sent them; nothing reads them but the API.
	LeftOutConnectors []LeftOutConnector `bun:"left_out_connectors,type:jsonb"`
	// The row's plugins, plugin_events and mcp_servers columns, from before connectors, are
	// not modelled: no code reads them, a save leaves them as they are and an insert gives them
	// their default [].

	// Channels are the lines this agent answers on outside a Stream Chat channel: a
	// WhatsApp number, a number to text, an iMessage line.
	Channels AgentChannels `bun:"channels,type:jsonb,notnull"`
	// ChatChannels are the Stream Chat channel types of the customer's own app this agent
	// answers in besides agent channels, such as a team's group channels
	// (20261222120000_agent_config_chat_channels.sql). Empty, as every config starts, answers
	// in agent channels alone.
	ChatChannels []ChatChannel `bun:"chat_channels,type:jsonb"`
	// Scheduling is whether the agent may schedule tasks for its users, and the
	// defaults those tasks take. The zero value is off.
	Scheduling AgentScheduling `bun:"scheduling,type:jsonb,notnull"`
	// Calls are the call tools the agent's phone sessions are offered: end the call, press
	// a key, transfer. A tool never set takes its default.
	Calls AgentCalls `bun:"calls,type:jsonb,notnull"`
	// Keyterms are the business-specific words a transcriber would otherwise get wrong.
	Keyterms []string `bun:"keyterms,type:jsonb"`
	// VisibleTools names the tools whose steps end users see on a persistent conversation's
	// replies, as names or path.Match patterns. Empty shows search and web_search.
	VisibleTools []string `bun:"visible_tools,type:jsonb"`
	// KnowledgeNamespace is what the agent may look things up in.
	KnowledgeNamespace string `bun:"knowledge_namespace,notnull"`
	// Sandbox is where the subagent may run code it writes, "daytona" being the one
	// provider there is. Empty means it runs none.
	Sandbox string `bun:"sandbox,notnull"`
	// SandboxOptions is how the sandbox is built and how long code may run in it.
	SandboxOptions sandbox.Config `bun:"sandbox_options,type:jsonb,notnull"`
	// Harness is which harness the agent's sessions run, "default" being the one there is.
	Harness string            `bun:"harness,notnull"`
	Tags    map[string]string `bun:"tags,type:jsonb"`
	// DispatchIncomingCall and DispatchText leave that work to the customer's own dispatch
	// worker. With DispatchText an end user's message is handed to a worker instead of the
	// model, and the model only answers when the worker asks it to.
	DispatchIncomingCall bool `bun:"dispatch_incoming_call,notnull"`
	DispatchText         bool `bun:"dispatch_text,notnull"`
	// EpisodeCards has each session under the config start with the person's other episode
	// cards (20261007042200_agent_config_episode_cards.sql), and the config write them as
	// record does: the on mode. Off unless a config turns it on.
	EpisodeCards bool `bun:"episode_cards,notnull"`
	// EpisodeCardsRecord has each call, app chat and channel thread under the config write
	// its episode card and summary into the person's omni-channel while no session reads them
	// (20261217090000_agent_config_episode_cards_record.sql): the record mode. Read only beside
	// EpisodeCards off; WritesCards is the two together.
	EpisodeCardsRecord bool `bun:"episode_cards_record,notnull"`
	// ToolGroups are the functions an SDK provides that the config's sessions may offer, and
	// how each group is loaded (20270103090000_agent_config_tool_groups.sql). A function no
	// group names is not offered. The row's progressive_tools and tool_output_retention
	// columns, which groups and connector bindings replaced, are not modelled: a router still
	// running the release before reads them, so they are left in place.
	ToolGroups []ToolGroup `bun:"tool_groups,type:jsonb"`
	// ResumeHistory is how the config's sessions are handed back a conversation they
	// carry on. The zero value is the router's default.
	ResumeHistory ResumeHistory `bun:"resume_history,type:jsonb,notnull"`
	// FloorControl is the model that controls the floor in the config's sessions, and how
	// long a caller may wait once they stop, as the config chose
	// (20261221090000_floor_control_max_wait.sql). The zero value, NULL, is no choice: the
	// router's default rules (session.FloorControlOf).
	FloorControl FloorControl `bun:"floor_control,type:jsonb,nullzero"`
	// SyncHash is a fingerprint of the last directory written onto this config. Empty
	// if it was never synced from a directory.
	SyncHash  string     `bun:"sync_hash,notnull"`
	CreatedAt time.Time  `bun:"created_at,notnull"`
	UpdatedAt time.Time  `bun:"updated_at,notnull"`
	DeletedAt *time.Time `bun:"deleted_at"`
}

// WritesCards is whether the config's calls, app chats and channel threads write episode
// cards and their summaries: on (EpisodeCards) or record (EpisodeCardsRecord).
func (c AgentConfig) WritesCards() bool {
	return c.EpisodeCards || c.EpisodeCardsRecord
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
// codex/connector-support at cf62af0d), and ConnectorConnectionReferenced finds a fixed
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

// RouterConfig is a named set of per-modality routing options, for a caller that routes
// one modality at a time rather than holding a conversation.
//
// It is a separate table from agent_configs because it configures a different thing.
// An agent config describes a conversation: a voice, instructions, a greeting, the skills
// the subagent may be handed. This describes transcribing, speaking, answering and
// searching on their own, each with the options that modality has and no others.
type RouterConfig struct {
	bun.BaseModel `bun:"table:router_configs,alias:rc"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	Name       string `bun:"name,notnull"`
	// The option blocks, one per routed modality. They are the routing package's own
	// types rather than copies of them, so what is stored and what reaches a provider
	// cannot drift apart.
	STT       options.STT    `bun:"stt,type:jsonb"`
	TTS       options.TTS    `bun:"tts,type:jsonb"`
	LLM       options.LLM    `bun:"llm,type:jsonb"`
	STS       options.STS    `bun:"sts,type:jsonb"`
	Search    options.Search `bun:"search,type:jsonb"`
	CreatedAt time.Time      `bun:"created_at,notnull"`
	UpdatedAt time.Time      `bun:"updated_at,notnull"`
	DeletedAt *time.Time     `bun:"deleted_at"`
}

// How an agent is talked to.
const (
	// AgentModeVoice joins a call, transcribes what it hears and speaks its replies.
	AgentModeVoice = "voice"
	// AgentModeText holds the same conversation in writing, using neither speech target.
	AgentModeText = "text"
)

// What an agent config is for, beyond answering people (AgentConfig.Role).
const (
	// RoleAgentModerator is the app's Nora: the agent the chat worker hands flagged messages
	// to for review.
	RoleAgentModerator = "agent_moderator"
)

// How a greeting is said.
const (
	// GreetingExact says the greeting word for word.
	GreetingExact = "exact"
	// GreetingVariation has the model say its own variation of the greeting.
	GreetingVariation = "variation"
)

// Skill is one kind of work worth handing to the slower model, stored so the config that
// owns it can name it.
//
// A skill belongs to one config rather than to the customer: two agents that both need
// the same kind of work have one each, so editing either leaves the other alone.
type Skill struct {
	CaptureVideo  bool `bun:"capture_video,notnull"`
	bun.BaseModel `bun:"table:skills,alias:sk"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	// ConfigID is the agent config this skill belongs to.
	ConfigID string `bun:"config_id,notnull"`
	Name     string `bun:"name,notnull"`
	// Description is the one line the fast model sees.
	Description string `bun:"description,notnull"`
	// Instructions is the full prompt, which only the subagent sees.
	Instructions string `bun:"instructions,notnull"`
	// Files are the rest of a skill folder, by path relative to it: references read on
	// demand and scripts run in the sandbox.
	Files map[string]string `bun:"files,type:jsonb,notnull"`
	// DeadlineMs is how long the work may run before it is abandoned. Zero is the
	// harness's own default.
	DeadlineMs int64      `bun:"deadline_ms,notnull"`
	CreatedAt  time.Time  `bun:"created_at,notnull"`
	UpdatedAt  time.Time  `bun:"updated_at,notnull"`
	DeletedAt  *time.Time `bun:"deleted_at"`
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
	for _, channel := range c.ChatChannels {
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

// Where a knowledge url has got to.
const (
	// KnowledgeURLPending means it has been added but not yet read.
	KnowledgeURLPending = "pending"
	// KnowledgeURLIndexed means the page is in the knowledge base and can be looked up.
	KnowledgeURLIndexed = "indexed"
	// KnowledgeURLFailed means it could not be read, and Error says why.
	KnowledgeURLFailed = "failed"
)

// KnowledgeURL is a page an agent's knowledge base is kept filled from.
//
// The passages are in turbopuffer rather than here; this is the subscription to them. It
// carries how many were written because that is what makes removing them exact: the ids
// are the url and a position, so knowing the count is knowing which ids to delete.
type KnowledgeURL struct {
	bun.BaseModel `bun:"table:knowledge_urls,alias:ku"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	// Namespace is the knowledge base the page is read into.
	Namespace string `bun:"namespace,notnull"`
	URL       string `bun:"url,notnull"`
	// Title is what the page called itself, as of the last time it was read.
	Title string `bun:"title,notnull"`
	// DeclaredTitle and Description are what the subscription says the page is. They are
	// the caller's words and are left alone by a read, which is why the title a page gives
	// itself is kept apart from the one it was subscribed with.
	DeclaredTitle string `bun:"declared_title,notnull"`
	Description   string `bun:"description,notnull"`
	// State is pending, indexed or failed.
	State string `bun:"state,notnull"`
	// Error is why the last read failed, and is empty otherwise.
	Error string `bun:"error,notnull"`
	// Passages is how many the page was last cut into.
	Passages int `bun:"passages,notnull"`
	// LastIndexedAt is when it was last read successfully. Nil means never.
	LastIndexedAt *time.Time `bun:"last_indexed_at"`
	// RefreshHours is how often the page is read again on its own. Zero is never.
	RefreshHours int        `bun:"refresh_hours,notnull"`
	CreatedAt    time.Time  `bun:"created_at,notnull"`
	UpdatedAt    time.Time  `bun:"updated_at,notnull"`
	DeletedAt    *time.Time `bun:"deleted_at"`
}

// KnowledgeDocument is a document a knowledge base was filled with, posted or synced
// rather than read from a page. Like a KnowledgeURL it carries how many passages it was
// cut into, so removing it, or what a shorter version no longer covers, is exact.
type KnowledgeDocument struct {
	bun.BaseModel `bun:"table:knowledge_documents,alias:kd"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	Namespace  string `bun:"namespace,notnull"`
	// Source is what the passages are keyed by, and what a reader would call the document.
	Source string `bun:"source,notnull"`
	// Passages covers every passage that may be stored for the document: while its passages
	// are written, and after that write failed, both the old version's and the new one's.
	Passages int `bun:"passages,notnull"`
	// Text is the document as it was posted, or as its file was read. Listings leave it out.
	Text string `bun:"text,notnull"`
	// State is pending while an uploaded file waits to be read or posted text is written,
	// failed when that could not be done, and indexed otherwise. Error says why it failed.
	State string `bun:"state,notnull"`
	Error string `bun:"error,notnull"`
	// Pages is how many pages the uploaded file had, 0 for text and for a file without pages.
	Pages int `bun:"pages,notnull"`
	// ContentType and Bytes are what was sent: the file's type as the upload named it, and
	// its size.
	ContentType string `bun:"content_type,notnull"`
	Bytes       int64  `bun:"bytes,notnull"`
	// ObjectKey is the uploaded file in the knowledge bucket while it waits to be read, and
	// empty once it has been. A read that finds another key here was overtaken by a newer
	// upload of the same source, and leaves the row to it.
	ObjectKey string    `bun:"object_key,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
}

// Where a knowledge document has got to.
const (
	// KnowledgeDocumentPending is an uploaded file not read yet, or posted text whose
	// passages are being written.
	KnowledgeDocumentPending = "pending"
	// KnowledgeDocumentIndexed is a document whose passages can be looked up.
	KnowledgeDocumentIndexed = "indexed"
	// KnowledgeDocumentFailed is an uploaded file that could not be read, or posted text
	// whose write stopped part way, and Error says why. The passages written before the
	// failure can still be looked up.
	KnowledgeDocumentFailed = "failed"
)

// What a provider has made of a voice's samples.
const (
	// VoicePending means preparation has been asked for but has not finished.
	VoicePending = "pending"
	// VoiceReady means the provider has the voice and will speak in it.
	VoiceReady = "ready"
	// VoiceFailed means the provider would not take it, and Error says why.
	VoiceFailed = "failed"
)

// Voice is a voice a customer brought with them, as opposed to one from a provider's own
// library.
//
// What a session asks for is this row rather than a provider's id for it, because the
// router fails over between providers mid-call and an id one provider knows means nothing
// to the next. Which id it means is worked out once the provider is chosen.
type Voice struct {
	bun.BaseModel `bun:"table:voices,alias:v"`

	ID          string     `bun:"id,pk"`
	CustomerID  string     `bun:"customer_id,notnull"`
	Name        string     `bun:"name,notnull"`
	Description string     `bun:"description,notnull"`
	CreatedAt   time.Time  `bun:"created_at,notnull"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull"`
	DeletedAt   *time.Time `bun:"deleted_at"`
}

// VoiceSample is one recording of the voice. The audio is in object storage, so the row
// says where it went rather than holding it.
type VoiceSample struct {
	bun.BaseModel `bun:"table:voice_samples,alias:vs"`

	ID      string `bun:"id,pk"`
	VoiceID string `bun:"voice_id,notnull"`
	// ObjectKey locates the audio in the bucket the deployment was given.
	ObjectKey   string `bun:"object_key,notnull"`
	ContentType string `bun:"content_type,notnull"`
	Bytes       int64  `bun:"bytes,notnull"`
	// Transcript is what is said in the recording, which the providers that ask for one
	// clone more faithfully with.
	Transcript string    `bun:"transcript,notnull"`
	CreatedAt  time.Time `bun:"created_at,notnull"`
}

// VoiceBinding is what one provider made of a voice's samples.
type VoiceBinding struct {
	bun.BaseModel `bun:"table:voice_bindings,alias:vb"`

	ID       string `bun:"id,pk"`
	VoiceID  string `bun:"voice_id,notnull"`
	Provider string `bun:"provider,notnull"`
	// ExternalID is what the provider calls this voice, and what a session asks it for.
	ExternalID string `bun:"external_id,notnull"`
	// State is one of VoicePending, VoiceReady or VoiceFailed.
	State     string    `bun:"state,notnull"`
	Error     string    `bun:"error,notnull"`
	CreatedAt time.Time `bun:"created_at,notnull"`
	UpdatedAt time.Time `bun:"updated_at,notnull"`
	// SyncedAt is when this provider last came back with a voice that can be spoken in,
	// and nil until one does. UpdatedAt cannot answer that: it moves again when the
	// binding goes back to pending.
	SyncedAt *time.Time `bun:"synced_at,nullzero"`
}

// Which way a call, a leg of one or a text went.
const (
	// Inbound is somebody ringing the agent.
	Inbound = "inbound"
	// Outbound is the agent ringing somebody, which is what a campaign does.
	Outbound = "outbound"
)

// Which pipeline a simulation puts the agent through.
const (
	// SimulationText hands the agent the words, which tests everything between hearing
	// and answering without needing a voice.
	SimulationText = "text"
	// SimulationAudio generates speech and runs the whole pipeline, so what the caller
	// hears is what the agent actually said.
	SimulationAudio = "audio"
)

// How a run or one of its conversations ended.
const (
	// SimulationPending is a case not yet started.
	SimulationPending = "pending"
	// SimulationRunning means the conversations are still being had.
	SimulationRunning = "running"
	// SimulationPassed means the judge was satisfied. A run passed only if every one of
	// its cases did.
	SimulationPassed = "passed"
	// SimulationFailed means the judge was not.
	SimulationFailed = "failed"
	// SimulationErrored means it never got as far as a ruling.
	SimulationErrored = "errored"
	// SimulationCancelled means somebody stopped it, or the process did.
	SimulationCancelled = "cancelled"
)

// Why one conversation stopped.
const (
	// EndedComplete is the caller deciding it had asked everything it came to ask.
	EndedComplete = "complete"
	// EndedTurns is the conversation running out of turns.
	EndedTurns = "turns"
	// EndedTimeout is it running out of time.
	EndedTimeout = "timeout"
	// EndedFailed is the agent stopping answering.
	EndedFailed = "failed"
)

// Simulation is a conversation to have with an agent and something that has to be true at
// the end of it.
type Simulation struct {
	bun.BaseModel `bun:"table:simulations,alias:sm"`

	ID         string `bun:"id,pk"`
	CustomerID string `bun:"customer_id,notnull"`
	Name       string `bun:"name,notnull"`
	// Mode is SimulationText or SimulationAudio.
	Mode string `bun:"mode,notnull"`
	// ConfigID is the agent being tested.
	ConfigID string `bun:"config_id,notnull"`
	// Scenario is what to ask, in the customer's own words. It is a brief for the caller
	// rather than a script.
	Scenario string `bun:"scenario,notnull"`
	// Assertion is what has to be true at the end for the run to have passed.
	Assertion string `bun:"assertion,notnull"`
	// Variations is how many ways of asking the same thing one run tries.
	Variations int `bun:"variations,notnull"`
	// JudgeTarget and CallerTarget are routing targets like any other. Empty takes the
	// default.
	JudgeTarget  string `bun:"judge_target,notnull"`
	CallerTarget string `bun:"caller_target,notnull"`
	// CallerTTS, CallerSTT and CallerVoice are how the caller speaks and listens in an
	// audio simulation, and mean nothing in a text one.
	CallerTTS   string `bun:"caller_tts,notnull"`
	CallerSTT   string `bun:"caller_stt,notnull"`
	CallerVoice string `bun:"caller_voice,notnull"`
	// MaxTurns bounds one conversation.
	MaxTurns  int               `bun:"max_turns,notnull"`
	Tags      map[string]string `bun:"tags,type:jsonb,nullzero"`
	CreatedAt time.Time         `bun:"created_at,notnull"`
	UpdatedAt time.Time         `bun:"updated_at,notnull"`
	DeletedAt *time.Time        `bun:"deleted_at"`
}

// SimulationRun is one press of Run, and the parent of however many conversations the
// variations asked for.
//
// What was run is copied onto the row rather than referenced, because editing a simulation
// must not rewrite what an old run tested.
type SimulationRun struct {
	bun.BaseModel `bun:"table:simulation_runs,alias:smr"`

	ID           string `bun:"id,pk"`
	CustomerID   string `bun:"customer_id,notnull"`
	SimulationID string `bun:"simulation_id,notnull"`
	State        string `bun:"state,notnull"`
	// Cases, Passed and Failed are the tally, so the log can list a run without reading
	// its conversations.
	Cases       int        `bun:"cases,notnull"`
	Passed      int        `bun:"passed,notnull"`
	Failed      int        `bun:"failed,notnull"`
	Mode        string     `bun:"mode,notnull"`
	ConfigID    string     `bun:"config_id,notnull"`
	Scenario    string     `bun:"scenario,notnull"`
	Assertion   string     `bun:"assertion,notnull"`
	JudgeTarget string     `bun:"judge_target,notnull"`
	Error       string     `bun:"error,nullzero"`
	StartedAt   time.Time  `bun:"started_at,notnull"`
	FinishedAt  *time.Time `bun:"finished_at"`
	// HeldUntil is when the router having the conversations stops being their owner unless
	// it renews, and StopRequested a cancel asked of a router that may not be that one.
	HeldUntil     *time.Time `bun:"held_until"`
	StopRequested bool       `bun:"stop_requested,notnull"`
}

// SimulationCase is one conversation. With variations off a run has one of these; with
// them expanded, ten, each asking the same thing a different way.
type SimulationCase struct {
	bun.BaseModel `bun:"table:simulation_cases,alias:smc"`

	ID    string `bun:"id,pk"`
	RunID string `bun:"run_id,notnull"`
	// Variation is which way of asking this was, and the order they are listed in.
	Variation int `bun:"variation,notnull"`
	// Scenario is the wording this case used.
	Scenario string `bun:"scenario,notnull"`
	State    string `bun:"state,notnull"`
	// CallID is the session that held the conversation, written as soon as it exists so a
	// run in progress can be watched.
	CallID string `bun:"call_id,nullzero"`
	// Transcript is what was said, oldest first.
	Transcript []SimulationLine `bun:"transcript,type:jsonb"`
	Turns      int              `bun:"turns,notnull"`
	// Passed, Verdict and Score are the judge's ruling. A nil Passed means it never got to
	// rule, which is not the same as having ruled against.
	Passed  *bool  `bun:"passed"`
	Verdict string `bun:"verdict,nullzero"`
	Score   *int   `bun:"score"`
	// Ended is why the conversation stopped.
	Ended      string     `bun:"ended,notnull"`
	Error      string     `bun:"error,nullzero"`
	StartedAt  time.Time  `bun:"started_at,notnull"`
	FinishedAt *time.Time `bun:"finished_at"`
}

// SimulationLine is one thing said, in the order it was said.
type SimulationLine struct {
	// Caller is true when the simulated caller said it rather than the agent.
	Caller bool   `json:"caller"`
	Text   string `json:"text"`
	// Intended is what the agent meant to say, where that differs from what the caller
	// heard. Only an audio simulation has both, and the difference is the whole point of
	// running one.
	Intended string `json:"intended,omitempty"`
	// Tools are what the agent did on this turn rather than said, which is what a question
	// about whether something was actually done is settled by.
	Tools []SimulationTool `json:"tools,omitempty"`
	At    time.Time        `json:"at"`
}

// SimulationTool is one tool the agent ran during a simulated conversation.
type SimulationTool struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
	Result    string `json:"result"`
	Error     string `json:"error,omitempty"`
}

// SimulationRunFilter narrows which runs are listed. Every field is optional, and an empty
// filter is the customer's most recent runs.
type SimulationRunFilter struct {
	CustomerID   string
	SimulationID string
	State        string
	// Limit caps how many come back. Zero leaves the store's own default.
	Limit int
}

// Granularity selects which rollup table to read or write.
type Granularity string

const (
	Hourly Granularity = "hourly"
	Daily  Granularity = "daily"
)

// table returns the rollup table for the granularity.
func (g Granularity) table() string {
	if g == Daily {
		return "stats_daily"
	}
	return "stats_hourly"
}

// tagTable returns the tag rollup table for the granularity.
func (g Granularity) tagTable() string {
	if g == Daily {
		return "stats_tags_daily"
	}
	return "stats_tags_hourly"
}

// truncateUnit returns the Postgres date_trunc unit for the granularity.
func (g Granularity) truncateUnit() string {
	if g == Daily {
		return "day"
	}
	return "hour"
}

// Valid reports whether the granularity is one this store knows.
func (g Granularity) Valid() bool {
	return g == Hourly || g == Daily
}

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
	// Minted says the router handed this guest out (RecordGuest). A guest the users recorder
	// wrote from a guest token is not: an app signs those itself, for any id it likes.
	Minted bool   `bun:"minted,notnull"`
	Name   string `bun:"name,notnull"`
	// Timezone is the IANA zone the user was last explicitly known to be in,
	// which a scheduled task resolves to when nothing nearer names one. Empty
	// when nobody has ever said.
	Timezone  string         `bun:"timezone"`
	Custom    map[string]any `bun:"custom,type:jsonb,nullzero"`
	CreatedAt time.Time      `bun:"created_at,notnull"`
	// Email is the address the app's backend last said this user signs in with
	// (auth.EmailHeader), empty while it has said none. Only a server-side caller can say it.
	Email string `bun:"email,nullzero"`
	// ClaimedBy is the real user this guest turned out to be, empty while they are still a
	// guest. A guest is claimed once: a second claim naming somebody else would move one
	// person's conversations onto another's account.
	ClaimedBy string     `bun:"claimed_by,nullzero"`
	ClaimedAt *time.Time `bun:"claimed_at"`
	// RefreshSecretHash is the SHA-256 of the refresh secret this guest was last handed
	// (GuestSecretHash), nil for one handed none. Never sent anywhere: it is the credential
	// that lets a guest whose token expired come back as themselves.
	RefreshSecretHash []byte `bun:"refresh_secret_hash,nullzero" json:"-"`
}

// CreatedPosition is the last row of a page ordered by when rows were made.
type CreatedPosition struct {
	At time.Time `json:"t"`
	ID string    `json:"id"`
}
