package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/redis/rueidis/rueidisaside"
	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/options"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

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
	// SyncHash is a fingerprint of the file the CLI last synced onto this config
	// (20270201001200_connector_router_sync_hash.sql), as AgentConfig.SyncHash is for a
	// directory. Empty if it was never synced, or was written since by anything that is not a
	// sync.
	SyncHash string `bun:"sync_hash,notnull"`
}

// Validate refuses a config the table would refuse.
func (c *RouterConfig) Validate() error {
	return validate("router config").
		required("customer_id", c.CustomerID).
		required("name", c.Name).
		done()
}

// ErrNoRouterConfig is a router config id the customer holds no live config by, as
// ErrNoAgentConfig is for an agent config.
var ErrNoRouterConfig = errors.New("store: there is no router config")

// RouterConfigs are the router presets customers hold, in the shared database. A preset is
// read on every request that names one, by id or by name, so both are cached.
type RouterConfigs struct {
	store *Store
}

// Insert stores a new config and fills in its id and timestamps.
func (r *RouterConfigs) Insert(ctx context.Context, config *RouterConfig) error {
	if err := config.Validate(); err != nil {
		return stack.Wrap(err)
	}

	config.ID = newID()
	now := time.Now().UTC()
	config.CreatedAt = now
	config.UpdatedAt = now
	config.DeletedAt = nil

	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().Model(config).Exec(ctx)
		return err
	}, func() []string { return []string{routerConfigNameKey(config.CustomerID, config.Name)} })
	if err != nil {
		if constraint(err) == "router_configs_name_idx" {
			return stack.Wrap(ErrNameTaken)
		}
		return stack.Wrap(fmt.Errorf("store: create router config: %w", err))
	}
	return nil
}

// Update replaces a config a customer holds. Every field is written, so an update is what the
// config now is rather than what changed about it. The name it had is forgotten as well as the
// one it has.
func (r *RouterConfigs) Update(ctx context.Context, config *RouterConfig) error {
	if config.ID == "" {
		return stack.Wrap(&ValidationError{Model: "router config", Field: "id", Reason: "is required"})
	}
	if err := config.Validate(); err != nil {
		return stack.Wrap(err)
	}

	config.UpdatedAt = time.Now().UTC()
	// The name the row had is read in the same statement, locked, so the one forgotten is the
	// one a concurrent rename did not already replace.
	was := r.store.global.NewSelect().Model((*RouterConfig)(nil)).Column("name").
		Where("id = ?", config.ID).
		Where("customer_id = ?", config.CustomerID).
		Where("deleted_at IS NULL").
		For("UPDATE")
	var wasName string
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		return tx.NewUpdate().Model(config).
			With("was", was).
			TableExpr("was").
			Column("name", "stt", "tts", "llm", "sts", "search", "sync_hash", "updated_at").
			Where("rc.id = ?", config.ID).
			Where("rc.customer_id = ?", config.CustomerID).
			Returning("was.name").
			Scan(ctx, &wasName)
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if wasName == "" {
			return nil
		}
		return []string{
			routerConfigKey(config.CustomerID, config.ID),
			routerConfigNameKey(config.CustomerID, config.Name),
			routerConfigNameKey(config.CustomerID, wasName),
		}
	})
	switch {
	case constraint(err) == "router_configs_name_idx":
		return stack.Wrap(ErrNameTaken)
	case errors.Is(err, sql.ErrNoRows):
		return unknownRouterConfig(config.ID)
	case err != nil:
		return stack.Wrap(fmt.Errorf("store: update router config: %w", err))
	}
	return nil
}

// Delete marks a config as gone. The row stays, because the requests that ran under it still
// name it.
func (r *RouterConfigs) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a config id are required"))
	}

	var name string
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		return tx.NewUpdate().Model((*RouterConfig)(nil)).
			Set("deleted_at = ?", time.Now().UTC()).
			Where("id = ?", id).
			Where("customer_id = ?", customerID).
			Where("deleted_at IS NULL").
			Returning("name").
			Scan(ctx, &name)
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if name == "" {
			return nil
		}
		return []string{routerConfigKey(customerID, id), routerConfigNameKey(customerID, name)}
	})
	if errors.Is(err, sql.ErrNoRows) {
		return unknownRouterConfig(id)
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete router config: %w", err))
	}
	return nil
}

// Get returns one config a customer holds.
func (r *RouterConfigs) Get(ctx context.Context, customerID, id string) (RouterConfig, error) {
	if customerID == "" || id == "" {
		return RouterConfig{}, stack.Wrap(errors.New("store: a customer and a config id are required"))
	}
	return cached(ctx, r.store, routerConfigKey(customerID, id), func(ctx context.Context) (RouterConfig, error) {
		return r.where(ctx, customerID, "id", id)
	})
}

// GetByName returns the config a customer holds under this name, which is how a caller that
// names a config rather than an id reaches one, and ErrNoRouterConfig when it holds none.
// That none is cached too, for missTTL.
func (r *RouterConfigs) GetByName(ctx context.Context, customerID, name string) (RouterConfig, error) {
	if customerID == "" || name == "" {
		return RouterConfig{}, stack.Wrap(errors.New("store: a customer and a config name are required"))
	}
	answer, err := cached(ctx, r.store, routerConfigNameKey(customerID, name), func(ctx context.Context) (found[RouterConfig], error) {
		config, err := r.where(ctx, customerID, "name", name)
		if errors.Is(err, ErrNoRouterConfig) {
			rueidisaside.OverrideCacheTTL(ctx, missTTL)
			return found[RouterConfig]{}, nil
		}
		return found[RouterConfig]{Value: config, Found: err == nil}, err
	})
	if err != nil {
		return RouterConfig{}, err
	}
	if !answer.Found {
		return RouterConfig{}, unknownRouterConfig(name)
	}
	return answer.Value, nil
}

// Select returns every live config a customer holds, newest first. It is not paged: a
// customer holds few, and the API lists them all.
func (r *RouterConfigs) Select(ctx context.Context, customerID string) ([]RouterConfig, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}

	var configs []RouterConfig
	err := r.store.global.NewSelect().Model(&configs).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Order("created_at DESC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer router configs: %w", err))
	}
	return configs, nil
}

func (r *RouterConfigs) where(ctx context.Context, customerID, column, value string) (RouterConfig, error) {
	var config RouterConfig
	err := r.store.global.NewSelect().Model(&config).
		Where("customer_id = ?", customerID).
		Where("? = ?", bun.Ident(column), value).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return RouterConfig{}, unknownRouterConfig(value)
	}
	if err != nil {
		return RouterConfig{}, stack.Wrap(fmt.Errorf("store: router config: %w", err))
	}
	return config, nil
}

func unknownRouterConfig(id string) error {
	return stack.Wrap(fmt.Errorf("%w %s", ErrNoRouterConfig, id))
}

func routerConfigKey(customerID, id string) string {
	return cacheKey("router-config", customerID, id)
}

func routerConfigNameKey(customerID, name string) string {
	return cacheKey("router-config-name", customerID, name)
}
