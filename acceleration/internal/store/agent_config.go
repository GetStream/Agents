package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/redis/rueidis/rueidisaside"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/sandbox"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stt"
)

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
	// role, test copies aside (agent_configs_role_idx).
	Role string `bun:"role,notnull"`
	// Mode is whether the agent is spoken to or written to: AgentModeVoice or
	// AgentModeText. A text agent uses neither speech target and joins no call.
	Mode string `bun:"mode,notnull"`
	// Region is where the agent's work is done, such as pricing.RegionEU: every model its
	// sessions run, its knowledge base and its sandbox are there, and nothing of it is done
	// elsewhere. Empty is stored as pricing.DefaultRegion.
	Region string `bun:"region,notnull"`
	// Models are what the agent's sessions run on, one column each.
	Models AgentModels `bun:"embed:"`
	// Video is what the agent sees, in the video_ columns.
	Video        AgentVideo `bun:"embed:video_"`
	Instructions string     `bun:"instructions,notnull"`
	Greeting     string     `bun:"greeting,notnull"`

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
	// Channels are where the agent answers besides agent channels, in the channels_ columns.
	Channels AgentAnswersIn `bun:"embed:channels_"`
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
	// Sandbox is where the subagent may run code it writes, in the sandbox_ columns.
	Sandbox AgentSandbox `bun:"embed:sandbox_"`
	// Harness is which harness the agent's sessions run, "default" being the one there is.
	Harness string            `bun:"harness,notnull"`
	Tags    map[string]string `bun:"tags,type:jsonb"`
	// Dispatch is the work left to the customer's own dispatch worker, in the dispatch_
	// columns.
	Dispatch AgentDispatch `bun:"embed:dispatch_"`
	// EpisodeCards is how the config's conversations use episode cards, in the
	// episode_cards_ columns.
	EpisodeCards AgentEpisodeCards `bun:"embed:episode_cards_"`
	// ToolGroups are the functions an SDK provides that the config's sessions may offer, and
	// how each group is loaded. A function no group names is not offered.
	ToolGroups []ToolGroup `bun:"tool_groups,type:jsonb"`
	// ResumeHistory is how the config's sessions are handed back a conversation they
	// carry on. The zero value is the router's default.
	ResumeHistory ResumeHistory `bun:"resume_history,type:jsonb,notnull"`
	// FloorControl is the model that controls the floor in the config's sessions, and how
	// long a caller may wait once they stop, as the config chose. The zero value, NULL, is no
	// choice: the router's default rules (session.FloorControlOf).
	FloorControl FloorControl `bun:"floor_control,type:jsonb,nullzero"`
	// SyncHash is a fingerprint of the last directory written onto this config. Empty
	// if it was never synced from a directory.
	SyncHash  string     `bun:"sync_hash,notnull"`
	CreatedAt time.Time  `bun:"created_at,notnull"`
	UpdatedAt time.Time  `bun:"updated_at,notnull"`
	DeletedAt *time.Time `bun:"deleted_at"`
}

// AgentModels are the models an agent's sessions run on.
type AgentModels struct {
	// STT, TTS, LLM, Subagent and Search are routing targets. Empty leaves the session
	// default. STS names one native audio model that hears and speaks for itself; naming
	// one makes the agent native, and the three cascade targets are then not used.
	STT      string `bun:"stt,notnull"`
	TTS      string `bun:"tts,notnull"`
	STS      string `bun:"sts,notnull"`
	Voice    string `bun:"voice,notnull"`
	LLM      string `bun:"llm,notnull"`
	Subagent string `bun:"subagent,notnull"`
	Search   string `bun:"search,notnull"`
	// Image is what the agent draws pictures with. Empty offers no drawing tool.
	Image string `bun:"image,notnull"`
	// LLMFallbacks are the targets a turn falls back to, in order, when LLM fails it. LLM
	// is still the first choice; nil falls back only among LLM's own candidates.
	LLMFallbacks []string `bun:"llm_fallbacks,type:jsonb"`
}

// AgentVideo is what an agent sees.
type AgentVideo struct {
	Source    string `bun:"source,notnull"`
	MaxFrames int    `bun:"max_frames,notnull"`
}

// AgentSandbox is where an agent's subagent may run code it writes.
type AgentSandbox struct {
	// Provider is "daytona", the one provider there is, or empty, which runs no code.
	Provider string `bun:"provider,notnull"`
	// Options is how the sandbox is built and how long code may run in it.
	Options sandbox.Config `bun:"options,type:jsonb,notnull"`
}

// AgentDispatch leaves work to the customer's own dispatch worker. With Text an end user's
// message is handed to a worker instead of the model, and the model only answers when the
// worker asks it to.
type AgentDispatch struct {
	IncomingCall bool `bun:"incoming_call,notnull"`
	Text         bool `bun:"text,notnull"`
}

// AgentEpisodeCards is how an agent's conversations use the person's episode cards.
type AgentEpisodeCards struct {
	// On has each session under the config start with the person's other episode cards, and
	// the config write them as Record does. Off unless a config turns it on.
	On bool `bun:"on,notnull"`
	// Record has each call, app chat and channel thread under the config write its episode
	// card and summary into the person's omni-channel while no session reads them. Read only
	// beside On off; WritesCards is the two together.
	Record bool `bun:"record,notnull"`
}

// AgentAnswersIn is where an agent answers besides agent channels.
type AgentAnswersIn struct {
	// Lines are the lines it answers on outside a Stream Chat channel: a WhatsApp number, a
	// number to text, an iMessage line.
	Lines AgentChannels `bun:"lines,type:jsonb,notnull"`
	// Chat are the Stream Chat channel types of the customer's own app it answers in, such as
	// a team's group channels. Empty, as every config starts, answers in agent channels alone.
	Chat []ChatChannel `bun:"chat,type:jsonb"`
}

// WritesCards is whether the config's calls, app chats and channel threads write episode
// cards and their summaries: on or record.
func (c AgentConfig) WritesCards() bool {
	return c.EpisodeCards.On || c.EpisodeCards.Record
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

// regionPattern is what agent_configs_region_check and knowledge_namespaces_region_check allow.
var regionPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

// Validate refuses a config the table would refuse, or one no session could be created from.
// It reads the config as Insert and Update store it, with its defaults filled in, so a caller
// may ask before it writes anything else.
func (c *AgentConfig) Validate() error {
	stored := *c
	normalizeConfig(&stored)
	c = &stored
	return validate("agent config").
		required("customer_id", c.CustomerID).
		required("name", c.Name).
		oneOf("mode", c.Mode, AgentModeVoice, AgentModeText).
		oneOf("role", c.Role, "", RoleAgentModerator).
		check(regionPattern.MatchString(c.Region), "region", "is not a region such as us or eu").
		oneOf("greeting_mode", c.GreetingMode, GreetingExact, GreetingVariation).
		check(c.Video.MaxFrames >= 1, "video_max_frames", "is below 1").
		check(len(c.Keyterms) <= stt.MaxKeyterms, "keyterms", fmt.Sprintf("names more than %d terms", stt.MaxKeyterms)).
		oneOf("sandbox_provider", c.Sandbox.Provider, "", SandboxDaytona).
		sandboxOptions(c.Sandbox.Options).
		done()
}

// SandboxDaytona is the one sandbox provider there is (AgentSandbox.Provider).
const SandboxDaytona = "daytona"

// sandboxOptions refuses a sandbox no provider builds, in its schema's bounds.
func (v *validation) sandboxOptions(options sandbox.Config) *validation {
	v.check(options.TimeoutMs >= 0 && options.TimeoutMs <= int(sandbox.MaxTimeout.Milliseconds()),
		"sandbox_options.timeout_ms", fmt.Sprintf("is not between 0 and %d", sandbox.MaxTimeout.Milliseconds())).
		check(options.CPU >= 0 && options.CPU <= 16, "sandbox_options.cpu", "is not between 0 and 16").
		check(options.MemoryGB >= 0 && options.MemoryGB <= 64, "sandbox_options.memory_gb", "is not between 0 and 64").
		check(options.DiskGB >= 0 && options.DiskGB <= 100, "sandbox_options.disk_gb", "is not between 0 and 100").
		maxLength("sandbox_options.image", options.Image, 256).
		check(len(options.Setup) <= 32, "sandbox_options.setup", "is more than 32 commands").
		check(len(options.AllowedDomains) <= 64, "sandbox_options.allowed_domains", "is more than 64 domains").
		check(len(options.NetworkAllowList) <= 64, "sandbox_options.network_allow_list", "is more than 64 CIDR ranges").
		check(len(options.AllowedDomains) == 0 || len(options.NetworkAllowList) == 0, "sandbox_options",
			"takes allowed_domains or network_allow_list, not both")
	for _, command := range options.Setup {
		v.check(strings.TrimSpace(command) != "" && len(command) <= 2048, "sandbox_options.setup", "has a command that is not between 1 and 2048 characters")
	}
	for _, domain := range options.AllowedDomains {
		v.check(domain != "" && len(domain) <= 253 && !strings.ContainsAny(domain, ", \t\n/") && !isAddress(domain),
			"sandbox_options.allowed_domains", fmt.Sprintf("has %q, which is not a domain", domain))
	}
	for _, cidr := range options.NetworkAllowList {
		_, err := netip.ParsePrefix(cidr)
		v.check(err == nil, "sandbox_options.network_allow_list", fmt.Sprintf("has %q, which is not a CIDR range such as 10.0.0.0/8", cidr))
	}
	return v
}

// isAddress says whether an allowed domain is an IP address, with or without brackets, a port
// or a trailing dot. The sandbox's egress proxy refuses every address host (sandbox/egress
// Proxy.ServeHTTP), since a grant names hosts and an address cannot be told apart from another
// host's.
func isAddress(domain string) bool {
	host := domain
	if split, _, err := net.SplitHostPort(domain); err == nil {
		host = split
	}
	_, err := netip.ParseAddr(strings.Trim(strings.TrimSuffix(host, "."), "[]"))
	return err == nil
}

// ErrNoAgentConfig is a config id the customer holds no live config by. A sentinel, so a
// caller can tell a deleted config from the database failing. The text is the one these
// errors always had.
var ErrNoAgentConfig = errors.New("store: there is no agent config")

// ErrNameTaken says a create or a rename asked for a name that another live record of the
// same kind already has: an agent config, a router config or a voice of the same customer,
// or a skill of the same agent config. The name is the writer's to change, so unlike the
// database failing it is the caller's to fix.
var ErrNameTaken = errors.New("store: the name is taken")

// ErrRoleTaken says a write would give a second live agent config of the same customer a
// role another one holds. A test copy holds none for this, so it is never refused.
var ErrRoleTaken = errors.New("store: the role is taken")

// AgentConfigs are the agent configs customers hold, in the shared database. A config is read
// on every session that opens, by id or by name, so both are cached.
type AgentConfigs struct {
	store *Store
}

// configColumns are the columns an update writes: everything about a config except who
// owns it and when it was created, which an update cannot change.
//
// It is named rather than written into the call so a test can hold it against the model.
// A field added to AgentConfig and forgotten here is stored on create and silently
// dropped on every update after, which reads as a setting that will not save.
var configColumns = []string{
	"name", "role", "mode", "region", "stt", "tts", "sts", "voice", "llm", "llm_fallbacks", "agents", "subagent",
	"video_source", "video_max_frames", "search", "image", "instructions", "greeting", "greeting_mode", "guardrail", "review_instructions", "ui_instructions",
	"skills", "connectors", "left_out_connectors", "channels_lines", "channels_chat", "scheduling", "calls", "keyterms", "visible_tools", "knowledge_namespace", "sandbox_provider", "sandbox_options", "harness", "tags",
	"dispatch_incoming_call", "dispatch_text", "episode_cards_on", "episode_cards_record", "tool_groups", "resume_history", "floor_control", "sync_hash", "updated_at",
}

// Insert stores a new config and fills in its id and timestamps. Each connection it binds as
// fixed has to be live, and stays locked until the config is stored (lockBoundConnections). A
// channel connection another live config binds is refused with a *ChannelConnectionTakenError
// (refuseSecondChannelAgent).
func (r *AgentConfigs) Insert(ctx context.Context, config *AgentConfig) error {
	normalizeConfig(config)
	if err := config.Validate(); err != nil {
		return stack.Wrap(err)
	}

	config.ID = newID()
	now := time.Now().UTC()
	config.CreatedAt = now
	config.UpdatedAt = now
	config.DeletedAt = nil

	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		missing, err := lockBoundConnections(ctx, tx, config)
		if err != nil {
			return err
		}
		if err := refuseUnbindable(missing, nil); err != nil {
			return err
		}
		undefined, err := lockCustomDefinitions(ctx, tx, config.CustomerID, boundConnectors(config.Connectors))
		if err != nil {
			return err
		}
		if err := refuseUndefined(undefined, nil); err != nil {
			return err
		}
		if err := refuseSecondChannelAgent(ctx, tx, config, nil, false); err != nil {
			return err
		}
		if err := refuseSecondLineAgent(ctx, tx, config); err != nil {
			return err
		}
		if _, err := tx.NewInsert().Model(config).Exec(ctx); err != nil {
			switch constraint(err) {
			case "agent_configs_name_idx":
				return ErrNameTaken
			case "agent_configs_role_idx":
				return ErrRoleTaken
			}
			return fmt.Errorf("store: create agent config: %w", err)
		}
		return nil
	}, func() []string {
		// Only the name, because the id was minted by the insert and nothing can have asked for
		// it yet. The name may well have been asked for and not found.
		return []string{agentConfigNameKey(config.CustomerID, config.Name)}
	})
	if err != nil {
		return stack.Wrap(err)
	}
	return nil
}

// ErrAgentConfigChanged says a save was refused because the config is not the one saved at
// the updated_at its caller made the change from (Save's unmodifiedSince).
var ErrAgentConfigChanged = errors.New("store: save agent config: saved since the caller's updated_at")

// ErrStaleAgentConfig says a save was refused because it was made from a copy of the config,
// read through the cache or not, that another save has replaced since. Nothing was written,
// and the cached copies of the config are forgotten, so a read now returns it as it is.
var ErrStaleAgentConfig = errors.New("store: save agent config: saved since the copy was read")

// Update replaces a config a customer holds. Every field is written, so an update is what the
// config now is rather than what changed about it. A connection it binds as fixed is locked as
// Insert locks it, and has to be live unless the stored config binds it already: a forced
// delete leaves that binding behind on purpose, and a save that keeps it is not a new bind. A
// new bind of a channel connection another live config binds is refused as Insert refuses it.
//
// The config is forgotten under its id and under both the name it had and the one it has, since
// a rename leaves whoever asked for the old name holding a config that no longer answers to it.
func (r *AgentConfigs) Update(ctx context.Context, config *AgentConfig) error {
	return r.save(ctx, config, time.Time{}, time.Time{})
}

// Save is Update for a change made to a copy of the config, the one stored at read: it is
// written only over that config. When another save has replaced it since, nothing is written
// and ErrStaleAgentConfig says so, so a change checked against one config is never written
// over another one nobody checked it against. The caller reads the config again and makes its
// change on that. With unmodifiedSince set, the config must also be the one saved then, or the
// save is refused with ErrAgentConfigChanged.
//
// Both are compared with the row's updated_at for equality, at the column's microseconds. A
// later-than comparison would let through a save whose replica's clock is behind.
func (r *AgentConfigs) Save(ctx context.Context, config *AgentConfig, read, unmodifiedSince time.Time) error {
	return r.save(ctx, config, read, unmodifiedSince)
}

// save is Update when read is zero and Save otherwise. The row is locked first and what it binds
// after, the order every write of a stored config takes them in, so no two writes wait on each
// other in a cycle: neither a connection delete nor Insert locks a stored config's row.
func (r *AgentConfigs) save(ctx context.Context, config *AgentConfig, read, unmodifiedSince time.Time) error {
	if config.ID == "" {
		return stack.Wrap(&ValidationError{Model: "agent config", Field: "id", Reason: "is required"})
	}
	normalizeConfig(config)
	if err := config.Validate(); err != nil {
		return stack.Wrap(err)
	}

	config.UpdatedAt = time.Now().UTC()

	var stored AgentConfig
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		err := tx.NewSelect().Model(&stored).Column("name", "connectors", "tags", "updated_at").
			Where("id = ?", config.ID).
			Where("customer_id = ?", config.CustomerID).
			Where("deleted_at IS NULL").
			For("UPDATE").
			Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return unknownAgentConfig(config.ID)
		}
		if err != nil {
			return fmt.Errorf("store: update agent config: %w", err)
		}
		if !unmodifiedSince.IsZero() && !stored.UpdatedAt.Equal(unmodifiedSince.Truncate(time.Microsecond)) {
			return ErrAgentConfigChanged
		}
		if !read.IsZero() && !stored.UpdatedAt.Equal(read.Truncate(time.Microsecond)) {
			return ErrStaleAgentConfig
		}

		missing, err := lockBoundConnections(ctx, tx, config)
		if err != nil {
			return err
		}
		undefined, err := lockCustomDefinitions(ctx, tx, config.CustomerID, boundConnectors(config.Connectors))
		if err != nil {
			return err
		}
		if err := refuseUnbindable(missing, stored.Connectors); err != nil {
			return err
		}
		if err := refuseUndefined(undefined, stored.Connectors); err != nil {
			return err
		}
		if err := refuseSecondChannelAgent(ctx, tx, config, stored.Connectors, stored.TestCopy()); err != nil {
			return err
		}
		if err := refuseSecondLineAgent(ctx, tx, config); err != nil {
			return err
		}
		_, err = tx.NewUpdate().Model(config).
			Column(configColumns...).
			Where("ac.id = ?", config.ID).
			Where("ac.customer_id = ?", config.CustomerID).
			Exec(ctx)
		switch constraint(err) {
		case "agent_configs_name_idx":
			return ErrNameTaken
		case "agent_configs_role_idx":
			return ErrRoleTaken
		}
		if err != nil {
			return fmt.Errorf("store: update agent config: %w", err)
		}
		return nil
	}, func() []string {
		// Named once the locked read found the row: a commit that fails can still have landed,
		// and a save refused as stale leaves the copy it was made from to be read again.
		if stored.Name == "" {
			return nil
		}
		return []string{
			agentConfigKey(config.CustomerID, config.ID),
			agentConfigNameKey(config.CustomerID, config.Name),
			agentConfigNameKey(config.CustomerID, stored.Name),
		}
	})
	if err != nil {
		return stack.Wrap(err)
	}
	return nil
}

// Delete marks a config as gone. The row stays, because the calls that ran under it still name
// it.
func (r *AgentConfigs) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a config id are required"))
	}

	var name string
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		err := tx.NewUpdate().Model((*AgentConfig)(nil)).
			Set("deleted_at = ?", time.Now().UTC()).
			Where("id = ?", id).
			Where("customer_id = ?", customerID).
			Where("deleted_at IS NULL").
			Returning("name").
			Scan(ctx, &name)
		if err != nil {
			return err
		}
		// Read under the row's lock, which a number given this config waits on (liveConfig):
		// no number can be pointed at it between this read and the commit.
		answering, err := answeredBy(ctx, tx, customerID, id)
		if err != nil {
			return err
		}
		if len(answering) > 0 {
			// Refused and rolled back: the cached config is still the live one.
			name = ""
			return &ConfigAnswersNumbersError{Numbers: answering}
		}
		return nil
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if name == "" {
			return nil
		}
		return []string{agentConfigKey(customerID, id), agentConfigNameKey(customerID, name)}
	})
	if errors.Is(err, sql.ErrNoRows) {
		return unknownAgentConfig(id)
	}
	if _, answers := errors.AsType[*ConfigAnswersNumbersError](err); answers {
		return stack.Wrap(err)
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete agent config: %w", err))
	}
	return nil
}

// Get returns one config a customer holds.
func (r *AgentConfigs) Get(ctx context.Context, customerID, id string) (AgentConfig, error) {
	if customerID == "" || id == "" {
		return AgentConfig{}, stack.Wrap(errors.New("store: a customer and a config id are required"))
	}
	return cached(ctx, r.store, agentConfigKey(customerID, id), func(ctx context.Context) (AgentConfig, error) {
		return r.where(ctx, customerID, "id", id)
	})
}

// GetByName returns the config a customer holds under this name, and ErrNoAgentConfig when it
// holds none. That none is cached too, for missTTL: a session opened by name asks the same
// question on every conversation.
func (r *AgentConfigs) GetByName(ctx context.Context, customerID, name string) (AgentConfig, error) {
	if customerID == "" || name == "" {
		return AgentConfig{}, stack.Wrap(errors.New("store: a customer and a config name are required"))
	}
	answer, err := cached(ctx, r.store, agentConfigNameKey(customerID, name), func(ctx context.Context) (found[AgentConfig], error) {
		config, err := r.where(ctx, customerID, "name", name)
		if errors.Is(err, ErrNoAgentConfig) {
			rueidisaside.OverrideCacheTTL(ctx, missTTL)
			return found[AgentConfig]{}, nil
		}
		return found[AgentConfig]{Value: config, Found: err == nil}, err
	})
	if err != nil {
		return AgentConfig{}, err
	}
	if !answer.Found {
		return AgentConfig{}, unknownAgentConfig(name)
	}
	return answer.Value, nil
}

// GetUnscoped returns a config by id alone, without being told whose it is.
//
// Every other read here is scoped to a customer, because every other caller already knows
// which customer it is acting for. This one is for the case where the id is all there is:
// an agent channel that names the config answering in it, whose owner has to be worked out
// rather than taken from a header. Finding out who owns a config is the whole point of it,
// so a caller that knows the customer should use Get instead.
func (r *AgentConfigs) GetUnscoped(ctx context.Context, id string) (AgentConfig, error) {
	if id == "" {
		return AgentConfig{}, stack.Wrap(errors.New("store: a config id is required"))
	}

	var config AgentConfig
	err := r.store.global.NewSelect().Model(&config).
		Where("id = ?", id).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentConfig{}, unknownAgentConfig(id)
	}
	if err != nil {
		return AgentConfig{}, stack.Wrap(fmt.Errorf("store: agent config owner: %w", err))
	}
	return config, nil
}

// GetByRole returns the live config a customer holds in this role, and ErrNoAgentConfig when
// none holds it. Test copies are left out, since one may carry the role of the config it
// copies.
func (r *AgentConfigs) GetByRole(ctx context.Context, customerID, role string) (AgentConfig, error) {
	if customerID == "" || role == "" {
		return AgentConfig{}, stack.Wrap(errors.New("store: a customer and a role are required"))
	}

	var config AgentConfig
	err := r.store.global.NewSelect().Model(&config).
		Where("customer_id = ?", customerID).
		Where("role = ?", role).
		Where("deleted_at IS NULL").
		Where(notTestCopy).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentConfig{}, unknownAgentConfig(role)
	}
	if err != nil {
		return AgentConfig{}, stack.Wrap(fmt.Errorf("store: agent config by role: %w", err))
	}
	return config, nil
}

// GetByChatChannel returns the customer's live agent config that answers in Stream Chat
// channels of the type channelType (AgentConfig.Channels.Chat): the oldest, when more than one
// names it, as the oldest config binding a line answers on it. A test copy (DraftOfTag)
// answers in none. It is ErrNoAgentConfig when no config names the type.
func (r *AgentConfigs) GetByChatChannel(ctx context.Context, customerID, channelType string) (AgentConfig, error) {
	if customerID == "" || channelType == "" {
		return AgentConfig{}, stack.Wrap(errors.New("store: a customer and a channel type are required"))
	}

	var config AgentConfig
	err := r.store.global.NewSelect().Model(&config).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Where(notTestCopy).
		Where("channels_chat @> jsonb_build_array(jsonb_build_object('type', ?::text))", channelType).
		Order("created_at", "id").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentConfig{}, unknownAgentConfig(channelType)
	}
	if err != nil {
		return AgentConfig{}, stack.Wrap(fmt.Errorf("store: agent config answering chat channel: %w", err))
	}
	return config, nil
}

// NameCalling is the name of a live config the customer holds, other than except, whose agents
// name the config called child, and empty when none does.
func (r *AgentConfigs) NameCalling(ctx context.Context, customerID, child, except string) (string, error) {
	named, err := json.Marshal([]map[string]string{{"config": child}})
	if err != nil {
		return "", stack.Wrap(err)
	}
	var parent string
	err = r.store.global.NewSelect().Model((*AgentConfig)(nil)).Column("name").
		Where("customer_id = ?", customerID).
		Where("id <> ?", except).
		Where("agents @> ?::jsonb", string(named)).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx, &parent)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", stack.Wrap(fmt.Errorf("store: agent config calling: %w", err))
	}
	return parent, nil
}

// Select returns every live config a customer holds, newest first. It is not paged: a
// customer holds few, and the API lists them all.
func (r *AgentConfigs) Select(ctx context.Context, customerID string) ([]AgentConfig, error) {
	if customerID == "" {
		return nil, stack.Wrap(errCustomerRequired)
	}

	var configs []AgentConfig
	err := r.store.global.NewSelect().Model(&configs).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Order("created_at DESC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer agent configs: %w", err))
	}
	return configs, nil
}

func (r *AgentConfigs) where(ctx context.Context, customerID, column, value string) (AgentConfig, error) {
	var config AgentConfig
	err := r.store.global.NewSelect().Model(&config).
		Where("customer_id = ?", customerID).
		Where("? = ?", bun.Ident(column), value).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return AgentConfig{}, unknownAgentConfig(value)
	}
	if err != nil {
		return AgentConfig{}, stack.Wrap(fmt.Errorf("store: agent config: %w", err))
	}
	return config, nil
}

// lockBoundConnections locks each live connection of the config's customer that the config
// binds as fixed until the transaction ends, and returns the ids it binds that are not live.
// It makes a bind and an unforced delete (ConnectorConnections.DeleteUnbound, which locks the
// connection FOR UPDATE first) wait for each other, so one of them always sees the other:
//
//   - A delete that locked the connection first is waited for. Under READ COMMITTED the
//     locking read then re-checks its WHERE against the row as the delete left it, so a
//     deleted connection is not returned and the bind is refused
//     (https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED).
//   - A delete that comes second waits for this transaction, and then reads the binding it
//     wrote.
//
// FOR KEY SHARE, not FOR SHARE: it conflicts with the delete's FOR UPDATE and not with the
// FOR NO KEY UPDATE a plain UPDATE of a non-key column takes (table 13.3,
// https://www.postgresql.org/docs/current/explicit-locking.html#LOCKING-ROWS), so a config
// save does not hold up a credential save. A config write locks its own row before the
// connection, and the delete locks no config row, so the two never wait on each other in a
// cycle.
func lockBoundConnections(ctx context.Context, tx bun.Tx, config *AgentConfig) ([]string, error) {
	var bound []string
	for _, binding := range config.Connectors {
		if binding.Connection.Type == "fixed" && !slices.Contains(bound, binding.Connection.ConnectionID) {
			bound = append(bound, binding.Connection.ConnectionID)
		}
	}
	if len(bound) == 0 {
		return nil, nil
	}
	var live []string
	err := tx.NewSelect().Model((*ConnectorConnection)(nil)).Column("id").
		Where("customer_id = ?", config.CustomerID).
		Where("id IN (?)", bun.In(bound)).
		Where("deleted_at IS NULL").
		For("KEY SHARE").
		Scan(ctx, &live)
	if err != nil {
		return nil, fmt.Errorf("store: lock bound connector connections: %w", err)
	}
	return slices.DeleteFunc(bound, func(id string) bool { return slices.Contains(live, id) }), nil
}

// refuseUnbindable refuses a fixed binding to a connection that is not live, unless kept, the
// bindings stored before this write, has it already.
func refuseUnbindable(missing []string, kept []ConnectorBinding) error {
	for _, id := range missing {
		if !slices.ContainsFunc(kept, func(binding ConnectorBinding) bool {
			return binding.Connection.Type == "fixed" && binding.Connection.ConnectionID == id
		}) {
			return stack.Wrap(fmt.Errorf("%w: %s", ErrNoConnectorConnection, id))
		}
	}
	return nil
}

// boundConnectors are the connector ids bindings name.
func boundConnectors(bindings []ConnectorBinding) []string {
	ids := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		ids = append(ids, binding.ConnectorID)
	}
	return ids
}

// refuseUndefined refuses a binding to a custom connector the customer has no definition of,
// unless kept, the bindings stored before this write, names it already: a forced connector
// delete leaves its bindings behind on purpose (ConnectorDefinitions.Delete), and a save that
// keeps one is not a new bind, as refuseUnbindable keeps a forced connection delete's.
func refuseUndefined(undefined []string, kept []ConnectorBinding) error {
	for _, id := range undefined {
		if !slices.ContainsFunc(kept, func(binding ConnectorBinding) bool { return binding.ConnectorID == id }) {
			return stack.Wrap(fmt.Errorf("%w: %s", ErrNoConnectorDefinition, id))
		}
	}
	return nil
}

// normalizeConfig fills in the JSONB columns a nil slice or map would write as null,
// which the columns are not, and the mode a caller that predates it leaves empty.
func normalizeConfig(config *AgentConfig) {
	if config.Video.MaxFrames == 0 {
		config.Video.MaxFrames = 1
	}
	if config.Mode == "" {
		config.Mode = AgentModeVoice
	}
	if config.Region == "" {
		config.Region = pricing.DefaultRegion
	}
	if config.GreetingMode == "" {
		config.GreetingMode = GreetingExact
	}
	config.Skills = orEmpty(config.Skills)
	config.Connectors = orEmpty(config.Connectors)
	config.LeftOutConnectors = orEmpty(config.LeftOutConnectors)
	config.Keyterms = orEmpty(config.Keyterms)
	config.VisibleTools = orEmpty(config.VisibleTools)
	config.Channels.Chat = orEmpty(config.Channels.Chat)
	config.Tags = orEmptyMap(config.Tags)
}

func unknownAgentConfig(id string) error {
	return stack.Wrap(fmt.Errorf("%w %s", ErrNoAgentConfig, id))
}

func agentConfigKey(customerID, id string) string {
	return cacheKey("agent-config", customerID, id)
}

func agentConfigNameKey(customerID, name string) string {
	return cacheKey("agent-config-name", customerID, name)
}
