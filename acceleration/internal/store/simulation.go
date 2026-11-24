package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
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

// defaultMaxTurns is how long a conversation a simulation that did not say runs for. A
// scenario with three things to ask needs more turns than three, because the agent asks
// questions back.
const defaultMaxTurns = 12

// maxTurns and maxVariations bound what a customer may ask for, since every turn is a
// model call and every variation is a whole conversation.
const (
	maxTurns      = 200
	maxVariations = 10
)

// Validate refuses a simulation that could not be run, so the complaint arrives when it is
// written rather than once per conversation at whatever hour it was started. It reads the
// simulation as Insert and Update store it, with its defaults filled in.
func (m *Simulation) Validate() error {
	return validate("simulation").
		required("customer_id", m.CustomerID).
		required("name", m.Name).
		required("config_id", m.ConfigID).
		required("scenario", m.Scenario).
		required("assertion", m.Assertion).
		oneOf("mode", m.Mode, SimulationText, SimulationAudio).
		check(m.Variations >= 1 && m.Variations <= maxVariations, "variations", fmt.Sprintf("is not between 1 and %d", maxVariations)).
		check(m.MaxTurns >= 1 && m.MaxTurns <= maxTurns, "max_turns", fmt.Sprintf("is not between 1 and %d", maxTurns)).
		done()
}

// ErrNoSimulation is a simulation id the customer holds no live simulation by.
var ErrNoSimulation = errors.New("store: there is no simulation")

// Simulations are the simulations customers wrote, in the shared database.
type Simulations struct {
	store *Store
}

// Insert stores a simulation and fills in its id and timestamps.
func (r *Simulations) Insert(ctx context.Context, simulation *Simulation) error {
	normalizeSimulation(simulation)
	if err := simulation.Validate(); err != nil {
		return stack.Wrap(err)
	}

	simulation.ID = newID()
	now := time.Now().UTC()
	simulation.CreatedAt = now
	simulation.UpdatedAt = now
	simulation.DeletedAt = nil

	if _, err := r.store.global.NewInsert().Model(simulation).Exec(ctx); err != nil {
		return stack.Wrap(fmt.Errorf("store: create simulation: %w", err))
	}
	return nil
}

// Update replaces what a simulation asks. The runs it already has are left alone: they carry
// their own copy of what they tested.
func (r *Simulations) Update(ctx context.Context, simulation *Simulation) error {
	if simulation.ID == "" {
		return stack.Wrap(&ValidationError{Model: "simulation", Field: "id", Reason: "is required"})
	}
	normalizeSimulation(simulation)
	if err := simulation.Validate(); err != nil {
		return stack.Wrap(err)
	}

	simulation.UpdatedAt = time.Now().UTC()

	result, err := r.store.global.NewUpdate().Model(simulation).
		Column("name", "mode", "config_id", "scenario", "assertion", "variations",
			"judge_target", "caller_target", "caller_tts", "caller_stt", "caller_voice",
			"max_turns", "tags", "updated_at").
		Where("id = ?", simulation.ID).
		Where("customer_id = ?", simulation.CustomerID).
		Where("deleted_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: update simulation: %w", err))
	}
	updated, err := updatedOne(result, "update simulation")
	if err != nil {
		return err
	}
	if !updated {
		return unknownSimulation(simulation.ID)
	}
	return nil
}

// Delete marks a simulation as gone. The row stays, because the runs that named it are still
// worth reading.
func (r *Simulations) Delete(ctx context.Context, customerID, id string) error {
	if customerID == "" || id == "" {
		return stack.Wrap(errors.New("store: a customer and a simulation id are required"))
	}

	result, err := r.store.global.NewUpdate().Model((*Simulation)(nil)).
		Set("deleted_at = ?", time.Now().UTC()).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Exec(ctx)
	if err != nil {
		return stack.Wrap(fmt.Errorf("store: delete simulation: %w", err))
	}
	deleted, err := updatedOne(result, "delete simulation")
	if err != nil {
		return err
	}
	if !deleted {
		return unknownSimulation(id)
	}
	return nil
}

// Get returns one simulation a customer holds.
func (r *Simulations) Get(ctx context.Context, customerID, id string) (Simulation, error) {
	if customerID == "" || id == "" {
		return Simulation{}, stack.Wrap(errors.New("store: a customer and a simulation id are required"))
	}

	var simulation Simulation
	err := r.store.global.NewSelect().Model(&simulation).
		Where("id = ?", id).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Simulation{}, unknownSimulation(id)
	}
	if err != nil {
		return Simulation{}, stack.Wrap(fmt.Errorf("store: simulation: %w", err))
	}
	return simulation, nil
}

// Select returns every live simulation a customer holds, newest first. It is not paged: a
// customer holds few, and the API lists them all.
func (r *Simulations) Select(ctx context.Context, customerID string) ([]Simulation, error) {
	if customerID == "" {
		return nil, stack.Wrap(errors.New("store: customer id is required"))
	}

	var simulations []Simulation
	err := r.store.global.NewSelect().Model(&simulations).
		Where("customer_id = ?", customerID).
		Where("deleted_at IS NULL").
		Order("created_at DESC").
		Scan(ctx)
	if err != nil {
		return nil, stack.Wrap(fmt.Errorf("store: customer simulations: %w", err))
	}
	return simulations, nil
}

// normalizeSimulation fills in what was left out, so nothing downstream has to decide what
// an empty column meant.
func normalizeSimulation(simulation *Simulation) {
	if simulation.Variations <= 0 {
		simulation.Variations = 1
	}
	if simulation.MaxTurns <= 0 {
		simulation.MaxTurns = defaultMaxTurns
	}
	simulation.Tags = orEmptyMap(simulation.Tags)
}

func unknownSimulation(id string) error {
	return stack.Wrap(fmt.Errorf("%w %s", ErrNoSimulation, id))
}
