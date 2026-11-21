package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

// PolicyScope is what a policy document belongs to.
type PolicyScope string

const (
	// ScopeOrganization is every app in an organization.
	ScopeOrganization PolicyScope = "organization"
	// ScopeApp is one app.
	ScopeApp PolicyScope = "app"
	// ScopeUser is one end user of one app, by UserScopeID. Its document holds a quota only.
	ScopeUser PolicyScope = "user"
)

// UserScopeID is the scope id of one app's end user. A user id is the app's own name for
// somebody, so the same id in two apps is two users.
func UserScopeID(appID, userID string) string { return appID + "/" + userID }

// SplitUserScopeID is the app and the user a UserScopeID names. A user id holds no '/', so
// the last one splits them.
func SplitUserScopeID(id string) (appID, userID string, ok bool) {
	slash := strings.LastIndexByte(id, '/')
	if slash <= 0 || slash == len(id)-1 {
		return "", "", false
	}
	return id[:slash], id[slash+1:], true
}

// PolicyDocument is what an organization, an app or one end user has decided. Spend limits
// are not here: they are rows of their own (Limit), at every level.
//
// Every field is optional, and absent means "no opinion" rather than "off": an app that
// says nothing about training inherits whatever its organization said.
type PolicyDocument struct {
	// Quota is what one end user may spend in a UTC day: on an organization's document the
	// default for every user of its apps, on an app's the default for its users, and on a
	// user's their own.
	Quota *Quota `json:"quota,omitempty"`
	// DataPolicy is the floor every routed request is held to, on top of whatever the
	// request itself asked for.
	DataPolicy PolicyDataPolicy `json:"data_policy,omitzero"`
	// PromptInjection screens what an LLM is asked for injection attempts.
	PromptInjection *bool `json:"prompt_injection,omitempty"`
	// AllowedModels are the "provider/model" names requests may be routed to. Nil allows
	// every model, and an empty list allows none.
	AllowedModels *[]string `json:"allowed_models,omitempty"`
	// Tags are laid over the request's own on every row of usage.
	Tags map[string]string `json:"tags,omitempty"`
	// RequireOwnStreamApp keeps an app out of the deployment's own Stream app: it is never
	// written there for want of one of its own, and what it wrote there before can only be
	// read. An organization's is set only by the operator.
	RequireOwnStreamApp *bool `json:"require_own_stream_app,omitempty"`
	// ResumeHistory is how the scope's sessions are handed back a conversation they carry
	// on, under whatever an agent config or a session says (ResumeHistory.Over). An app's
	// wins over its organization's field by field.
	ResumeHistory *ResumeHistory `json:"resume_history,omitempty"`
	// BalanceWarningMicros is the balance, in micros, at or below which the organization is
	// warned, over its tier's; on an organization's document only.
	BalanceWarningMicros *int64 `json:"balance_warning_micros,omitempty"`
	// ToolOutputRetention is the longest any of the scope's sessions keeps what its tools
	// were given and gave back. An agent config can shorten it and never lengthen it.
	ToolOutputRetention options.Retention `json:"tool_output_retention,omitempty"`
}

// PolicyDataPolicy is what a policy requires of what a provider does with what it is sent. It
// says nothing of where work is done: that is an agent's own (AgentConfig.Region), or a
// direct call's, and a document written with a region has it ignored.
type PolicyDataPolicy struct {
	// AllowTraining false requires a provider that has said it does not train on what it
	// is sent. Nil asks nothing, and true permits it.
	AllowTraining *bool `json:"allow_training,omitempty"`
	// Retention is the longest a provider may keep what it is sent. Empty asks nothing.
	Retention options.Retention `json:"retention,omitempty"`
}

// Floor is the data policy every routed request of the scope is held to.
func (p PolicyDataPolicy) Floor() options.DataPolicy {
	return options.DataPolicy{AllowTraining: p.AllowTraining, Retention: p.Retention}
}

// Quota is what one end user may spend in a UTC day. A nil field is no opinion, so the
// next level's number shows through, and the deployment's below them all; zero is no cap.
type Quota struct {
	MessagesPerUserPerDay *int64 `json:"messages_per_user_per_day,omitempty"`
	TokensPerUserPerDay   *int64 `json:"tokens_per_user_per_day,omitempty"`
}

// Policy is one scope's stored document.
type Policy struct {
	bun.BaseModel `bun:"table:policies,alias:pol"`

	Scope     PolicyScope    `bun:"scope,pk"`
	ScopeID   string         `bun:"scope_id,pk"`
	Document  PolicyDocument `bun:"document,type:jsonb,notnull"`
	UpdatedAt time.Time      `bun:"updated_at,notnull"`
}

// Validate refuses a policy with no scope id or of a scope there is none of.
func (p *Policy) Validate() error {
	return validate("policy").
		oneOf("scope", string(p.Scope), string(ScopeOrganization), string(ScopeApp), string(ScopeUser)).
		required("scope_id", p.ScopeID).
		done()
}

// ErrNoPolicy says a scope has no document of its own.
var ErrNoPolicy = errors.New("store: no such policy")

// UserPolicyLimit is the default and the most users SelectUsers answers in one page.
const UserPolicyLimit = 100

// Policies are the documents organizations, apps and end users have decided, in the shared
// database. Every request reads its scopes', so a document is cached by scope, and whether an
// app has any user documents by app.
type Policies struct {
	store *Store
}

// Get returns a scope's document, through the cache. A scope nobody has written a policy for
// gets the empty document rather than an error, since that is what almost every scope is.
func (r *Policies) Get(ctx context.Context, scope PolicyScope, id string) (PolicyDocument, error) {
	if id == "" {
		return PolicyDocument{}, stack.Wrap(errors.New("store: a policy needs a scope id"))
	}
	return cached(ctx, r.store, cacheKey("policy", string(scope), id), func(ctx context.Context) (PolicyDocument, error) {
		var policy Policy
		err := r.store.global.NewSelect().Model(&policy).
			Where("scope = ?", scope).
			Where("scope_id = ?", id).
			Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return PolicyDocument{}, nil
		}
		if err != nil {
			return PolicyDocument{}, stack.Wrap(fmt.Errorf("store: policy: %w", err))
		}
		return policy.Document, nil
	})
}

// Upsert replaces a scope's document and drops what every replica had cached of it. An
// organization's document that leaves RequireOwnStreamApp out keeps the one stored.
func (r *Policies) Upsert(ctx context.Context, policy *Policy) error {
	if err := policy.Validate(); err != nil {
		return stack.Wrap(err)
	}
	policy.UpdatedAt = time.Now().UTC()
	replace := "document = EXCLUDED.document"
	if policy.Scope == ScopeOrganization {
		// The operator sets an organization's require_own_stream_app, and anybody else saves
		// its document without it. The stored value is kept in the same statement that writes
		// the rest, so a save that read the document before the operator wrote cannot undo it.
		replace = "document = jsonb_strip_nulls(jsonb_build_object('require_own_stream_app', " +
			"pol.document->'require_own_stream_app')) || EXCLUDED.document"
	}
	_, err := r.store.global.NewInsert().Model(policy).
		On("CONFLICT (scope, scope_id) DO UPDATE").
		Set(replace).
		Set("updated_at = EXCLUDED.updated_at").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save policy: %w", err))
	}
	r.store.forget(ctx, policyKeys(policy.Scope, policy.ScopeID)...)
	return nil
}

// Delete removes a scope's document and drops what every replica had cached of it, or
// returns ErrNoPolicy.
func (r *Policies) Delete(ctx context.Context, scope PolicyScope, id string) error {
	if id == "" {
		return stack.Wrap(errors.New("store: a policy needs a scope id"))
	}
	result, err := r.store.global.NewDelete().Model((*Policy)(nil)).
		Where("scope = ?", scope).
		Where("scope_id = ?", id).
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete policy: %w", err))
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete policy: %w", err))
	}
	r.store.forget(ctx, policyKeys(scope, id)...)
	if deleted == 0 {
		return stack.Wrap(ErrNoPolicy)
	}
	return nil
}

// SelectUsers is a page of an app's users with a policy of their own, by user id in byte
// order, after the user id after when it is not empty. It reads limit+1 rows, so a caller can
// tell whether there is more.
func (r *Policies) SelectUsers(ctx context.Context, appID, after string, limit int) ([]Policy, error) {
	if appID == "" {
		return nil, stack.Wrap(errors.New("store: an app's users need an app"))
	}
	if limit <= 0 || limit > UserPolicyLimit {
		limit = UserPolicyLimit
	}
	var policies []Policy
	query := r.store.global.NewSelect().Model(&policies).
		Where("scope = ?", ScopeUser).
		Where(`(scope_id COLLATE "C") LIKE ? ESCAPE '\'`, likePrefix(UserScopeID(appID, ""))).
		OrderExpr(`scope_id COLLATE "C"`).
		Limit(limit + 1)
	if after != "" {
		query = query.Where(`(scope_id COLLATE "C") > ?`, UserScopeID(appID, after))
	}
	if err := query.Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: user policies: %w", err))
	}
	return policies, nil
}

// HasUsers reports, through the cache, whether any user of an app has a policy of their own,
// so a user of an app with none is answered without a lookup of their own.
func (r *Policies) HasUsers(ctx context.Context, appID string) (bool, error) {
	return cached(ctx, r.store, cacheKey("user-policies", appID), func(ctx context.Context) (bool, error) {
		exists, err := r.store.global.NewSelect().Model((*Policy)(nil)).
			Where("scope = ?", ScopeUser).
			Where(`(scope_id COLLATE "C") LIKE ? ESCAPE '\'`, likePrefix(UserScopeID(appID, ""))).
			Exists(ctx)
		if err != nil {
			return false, stack.Wrap(fmt.Errorf("store: has user policies: %w", err))
		}
		return exists, nil
	})
}

// policyKeys are the cached values a write of one scope's document changes.
func policyKeys(scope PolicyScope, id string) []string {
	keys := []string{cacheKey("policy", string(scope), id)}
	if scope == ScopeUser {
		if appID, _, ok := SplitUserScopeID(id); ok {
			keys = append(keys, cacheKey("user-policies", appID))
		}
	}
	return keys
}

// likePrefix is a LIKE pattern matching what starts with prefix, its own % and _ taken as
// themselves.
func likePrefix(prefix string) string {
	return likeEscaper.Replace(prefix) + "%"
}
