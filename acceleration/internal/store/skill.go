package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
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

// Validate refuses a skill no config could name.
func (k *Skill) Validate() error {
	return validate("skill").
		required("customer_id", k.CustomerID).
		required("config_id", k.ConfigID).
		required("name", k.Name).
		check(k.DeadlineMs >= 0, "deadline_ms", "is negative").
		done()
}

// ErrNoSkill is a skill id the customer holds no live skill by, as ErrNoAgentConfig is for
// a config.
var ErrNoSkill = errors.New("store: there is no skill")

// SkillFilter picks which of a customer's skills Select returns.
type SkillFilter struct {
	// ConfigID narrows them to one agent config's own. Empty is every one of them.
	ConfigID string
}

// Skills are the skills agent configs hold, in the shared database. A session reads its
// config's skills as it opens, so they are cached a config at a time.
type Skills struct {
	store *Store
}

// Insert stores a new skill and fills in its id and timestamps.
func (r *Skills) Insert(ctx context.Context, skill *Skill) error {
	if err := skill.Validate(); err != nil {
		return stack.Wrap(err)
	}

	skill.ID = newID()
	now := time.Now().UTC()
	skill.CreatedAt = now
	skill.UpdatedAt = now
	skill.DeletedAt = nil
	if skill.Files == nil {
		skill.Files = map[string]string{}
	}

	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().Model(skill).Exec(ctx)
		return err
	}, func() []string { return []string{skillsKey(skill.CustomerID, skill.ConfigID)} })
	if err != nil {
		if constraint(err) == "skills_name_idx" {
			return stack.Wrap(ErrNameTaken)
		}
		return stack.Wrap(fmt.Errorf("store: create skill: %w", err))
	}
	return nil
}

// Update replaces a skill a customer holds. It may move the skill to another config, so the
// config it was in is forgotten as well as the one it is in.
func (r *Skills) Update(ctx context.Context, skill *Skill) error {
	if skill.ID == "" {
		return stack.Wrap(&ValidationError{Model: "skill", Field: "id", Reason: "is required"})
	}
	if err := skill.Validate(); err != nil {
		return stack.Wrap(err)
	}

	skill.UpdatedAt = time.Now().UTC()
	if skill.Files == nil {
		skill.Files = map[string]string{}
	}
	// The config the row was in is read in the same statement, locked, so the one forgotten is
	// the one a concurrent move did not already replace.
	was := r.store.global.NewSelect().Model((*Skill)(nil)).Column("config_id").
		Where("id = ?", skill.ID).
		Where("customer_id = ?", skill.CustomerID).
		Where("deleted_at IS NULL").
		For("UPDATE")
	var wasConfig string
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		return tx.NewUpdate().Model(skill).
			With("was", was).
			TableExpr("was").
			Column("config_id", "name", "description", "instructions", "files", "capture_video", "deadline_ms", "updated_at").
			Where("sk.id = ?", skill.ID).
			Where("sk.customer_id = ?", skill.CustomerID).
			Returning("was.config_id").
			Scan(ctx, &wasConfig)
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if wasConfig == "" {
			return nil
		}
		return []string{
			skillsKey(skill.CustomerID, skill.ConfigID),
			skillsKey(skill.CustomerID, wasConfig),
		}
	})
	switch {
	case constraint(err) == "skills_name_idx":
		return stack.Wrap(ErrNameTaken)
	case errors.Is(err, sql.ErrNoRows):
		return unknownSkill(skill.ID)
	case err != nil:
		return stack.Wrap(fmt.Errorf("store: update skill: %w", err))
	}
	return nil
}

// Delete marks a skill as gone.
func (r *Skills) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a skill id are required"))
	}

	var configID string
	err := r.store.detachedWrite(ctx, func(ctx context.Context, tx bun.Tx) error {
		return tx.NewUpdate().Model((*Skill)(nil)).
			Set("deleted_at = ?", time.Now().UTC()).
			Where("id = ?", id).
			Where("customer_id = ?", customerID).
			Where("deleted_at IS NULL").
			Returning("config_id").
			Scan(ctx, &configID)
	}, func() []string {
		// Named once the write answered: a commit that fails can still have landed.
		if configID == "" {
			return nil
		}
		return []string{skillsKey(customerID, configID)}
	})
	if errors.Is(err, sql.ErrNoRows) {
		return unknownSkill(id)
	}
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete skill: %w", err))
	}
	return nil
}

// Get returns one skill a customer holds.
func (r *Skills) Get(ctx context.Context, customerID, id string) (Skill, error) {
	if customerID == "" || id == "" {
		return Skill{}, stack.Wrap(errors.New("store: a customer and a skill id are required"))
	}

	var skill Skill
	err := r.store.global.NewSelect().Model(&skill).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Skill{}, unknownSkill(id)
	}
	if err != nil {
		return Skill{}, stack.Wrap(fmt.Errorf("store: skill: %w", err))
	}
	return skill, nil
}

// Select returns the skills a customer holds, newest first.
func (r *Skills) Select(ctx context.Context, customerID string, filter SkillFilter) ([]Skill, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}

	var skills []Skill
	query := r.store.global.NewSelect().Model(&skills).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL")
	if filter.ConfigID != "" {
		query = query.Where("config_id = ?", filter.ConfigID)
	}
	if err := query.Order("created_at DESC").Scan(ctx); err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer skills: %w", err))
	}
	return skills, nil
}

// GetByNames returns one config's skills with any of these names. A name nobody defined is
// simply absent, so the caller can report which ones it could not find.
//
// The config's skills are cached as one set rather than a name at a time: a write knows the
// config it changed, and one key is one round trip however many names a session asks for. A
// name the set lacks is then as cheap to ask about as one it has, which is what a session
// naming a built-in asks on every conversation.
func (r *Skills) GetByNames(ctx context.Context, customerID, configID string, names []string) ([]Skill, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}
	// Skills belong to a config, so a session that was not created from one reaches
	// nothing here and takes the built-in set.
	if configID == "" || len(names) == 0 {
		return nil, nil
	}

	all, err := cached(ctx, r.store, skillsKey(customerID, configID), func(ctx context.Context) ([]Skill, error) {
		return r.Select(ctx, customerID, SkillFilter{ConfigID: configID})
	})
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(all, func(skill Skill) bool { return !slices.Contains(names, skill.Name) }), nil
}

func unknownSkill(id string) error {
	return stack.Wrap(fmt.Errorf("%w %s", ErrNoSkill, id))
}

func skillsKey(customerID, configID string) string {
	return cacheKey("skills", customerID, configID)
}
