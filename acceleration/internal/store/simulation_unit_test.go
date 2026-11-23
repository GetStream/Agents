package store

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestASimulationIsBoundedInVariationsAndTurns(t *testing.T) {
	simulation := func(variations, turns int) *Simulation {
		return &Simulation{
			CustomerID: "acme",
			Name:       "refund",
			ConfigID:   "support",
			Scenario:   "ask for a refund",
			Assertion:  "the agent offers one",
			Mode:       SimulationText,
			Variations: variations,
			MaxTurns:   turns,
		}
	}

	require.NoError(t, simulation(10, 200).Validate())
	var invalid *ValidationError
	require.ErrorAs(t, simulation(11, 12).Validate(), &invalid)
	require.Equal(t, "variations", invalid.Field)
	require.ErrorAs(t, simulation(1, 201).Validate(), &invalid)
	require.Equal(t, "max_turns", invalid.Field)

	unrun := simulation(1, 12)
	unrun.Mode = "video"
	require.ErrorAs(t, unrun.Validate(), &invalid)
	require.Equal(t, "mode", invalid.Field)
}
