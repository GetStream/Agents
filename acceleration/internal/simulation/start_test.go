//go:build integration

package simulation

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
	"github.com/GetStream/Vision-Agents/acceleration/internal/testenv"
)

// StartSuite covers what Start refuses before a run is written.
type StartSuite struct {
	suite.Suite
	ctx context.Context
	db  *store.Store
}

func TestStartSuite(t *testing.T) {
	suite.Run(t, new(StartSuite))
}

func (s *StartSuite) SetupSuite() {
	dsn := os.Getenv("ROUTER_POSTGRES_DSN")
	if dsn == "" {
		s.T().Skip("ROUTER_POSTGRES_DSN must be set")
	}
	s.ctx = context.Background()
	db, err := store.Open(testenv.Database(dsn, "simulation"))
	s.Require().NoError(err)
	s.Require().NoError(db.Migrate(s.ctx))
	s.db = db
	s.T().Cleanup(func() { s.Require().NoError(db.Close()) })
}

// Defect: an audio run on a deployment without speech models refused with nothing a caller can
// tell from a database failure, which the API then answers as the router's own.
func (s *StartSuite) TestAnAudioRunWithoutSpeechModelsIsRefusedAsNoAudio() {
	simulation := store.Simulation{
		CustomerID: "app-" + rand.Text(), Name: "orders", Mode: store.SimulationAudio, ConfigID: "config-" + rand.Text(),
		Scenario: "order a pizza", Assertion: "was one ordered?", Variations: 1,
	}
	s.Require().NoError(s.db.Simulations.Insert(s.ctx, &simulation))
	runner := &Runner{store: s.db, logger: slog.New(slog.DiscardHandler), running: map[string]context.CancelFunc{}}

	_, err := runner.Start(s.ctx, simulation.CustomerID, simulation.ID)

	s.True(errors.Is(err, ErrNoAudio), "%v", err)
}
