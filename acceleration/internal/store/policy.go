package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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
	// PromptInjectionFailClosed refuses a response the screen could not check, the same way
	// as a flagged one. Without it, such a response goes through unchecked.
	PromptInjectionFailClosed *bool `json:"prompt_injection_fail_closed,omitempty"`
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

// AppPolicies is what decides an app's requests: its own document, the organization it was
// last seen under (AppOrganizations), empty for none, and that organization's document.
type AppPolicies struct {
	App            PolicyDocument `bun:"app" json:"app"`
	OrganizationID string         `bun:"organization_id" json:"organization_id"`
	Organization   PolicyDocument `bun:"organization" json:"organization"`
}

// OfApp returns an app's AppPolicies, through the cache, read in one statement. A write of the
// app's document, of its organization's or of which organization it is in drops it.
func (r *Policies) OfApp(ctx context.Context, appID string) (AppPolicies, error) {
	if appID == "" {
		return AppPolicies{}, stack.Wrap(errors.New("store: a policy needs a scope id"))
	}
	return cached(ctx, r.store, appPoliciesKey(appID), func(ctx context.Context) (AppPolicies, error) {
		var read AppPolicies
		err := r.store.global.NewSelect().
			TableExpr("(SELECT ?::text AS app_id) AS a", appID).
			Join("LEFT JOIN policies AS app ON app.scope = ? AND app.scope_id = a.app_id", ScopeApp).
			Join("LEFT JOIN app_organizations AS ao ON ao.app_id = a.app_id").
			Join("LEFT JOIN policies AS org ON org.scope = ? AND org.scope_id = ao.organization_id", ScopeOrganization).
			ColumnExpr("COALESCE(app.document, '{}') AS app").
			ColumnExpr("COALESCE(ao.organization_id, '') AS organization_id").
			ColumnExpr("COALESCE(org.document, '{}') AS organization").
			Scan(ctx, &read)
		if err != nil {
			return AppPolicies{}, stack.Wrap(fmt.Errorf("store: app policies: %w", err))
		}
		return read, nil
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
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().Model(policy).
			On("CONFLICT (scope, scope_id) DO UPDATE").
			Set(replace).
			Set("updated_at = EXCLUDED.updated_at").
			Exec(ctx)
		return err
	}, func() []string { return r.keys(ctx, policy.Scope, policy.ScopeID) })
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: save policy: %w", err))
	}
	return nil
}

// Delete removes a scope's document and drops what every replica had cached of it, or
// returns ErrNoPolicy.
func (r *Policies) Delete(ctx context.Context, scope PolicyScope, id string) error {
	if id == "" {
		return stack.Wrap(errors.New("store: a policy needs a scope id"))
	}
	var deleted int64
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		result, err := tx.NewDelete().Model((*Policy)(nil)).
			Where("scope = ?", scope).
			Where("scope_id = ?", id).
			Exec(ctx)
		if err != nil {
			return err
		}
		deleted, err = result.RowsAffected()
		return err
	}, func() []string { return r.keys(ctx, scope, id) })
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete policy: %w", err))
	}
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

// keys are what every replica cached of a scope's document. An organization's is in the
// AppPolicies of every app seen under it, listed after the write committed: an app seen under
// it since drops its own (AppOrganizations.Upsert). A listing that fails is logged, as a
// deletion that fails is (Store.forget), and the TTL covers it.
func (r *Policies) keys(ctx context.Context, scope PolicyScope, id string) []string {
	keys := policyKeys(scope, id)
	if scope == ScopeOrganization && r.store.cache != nil {
		// Detached, as the write it follows outlives its request, but bounded: a lock held on
		// app_organizations would otherwise keep the write's caller waiting on this list.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.store.writeTimeout)
		defer cancel()
		var apps []string
		err := r.store.global.NewSelect().Model((*AppOrganization)(nil)).Column("app_id").
			Where("organization_id = ?", id).Scan(ctx, &apps)
		if err != nil {
			slog.Default().Error("could not list the apps whose cached policies an organization's changed", "organization", id, "error", err)
		}
		for _, app := range apps {
			keys = append(keys, appPoliciesKey(app))
		}
	}
	return keys
}

// policyKeys are the cached values a write of one scope's document changes, but for the
// AppPolicies of an organization's apps (keys).
func policyKeys(scope PolicyScope, id string) []string {
	keys := []string{cacheKey("policy", string(scope), id)}
	if scope == ScopeApp {
		keys = append(keys, appPoliciesKey(id))
	}
	if scope == ScopeUser {
		if appID, _, ok := SplitUserScopeID(id); ok {
			keys = append(keys, cacheKey("user-policies", appID))
		}
	}
	return keys
}

func appPoliciesKey(appID string) string { return cacheKey("app-policies", appID) }

// likePrefix is a LIKE pattern matching what starts with prefix, its own % and _ taken as
// themselves.
func likePrefix(prefix string) string {
	return likeEscaper.Replace(prefix) + "%"
}
