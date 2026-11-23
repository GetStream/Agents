package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimulationRunValidate(t *testing.T) {
	valid := func() SimulationRun {
		return SimulationRun{CustomerID: "app", SimulationID: "simulation", State: SimulationRunning, Mode: SimulationText}
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*SimulationRun){
		"customer_id":   func(m *SimulationRun) { m.CustomerID = "" },
		"simulation_id": func(m *SimulationRun) { m.SimulationID = "" },
		"state":         func(m *SimulationRun) { m.State = SimulationPending },
		"mode":          func(m *SimulationRun) { m.Mode = "video" },
		"cases":         func(m *SimulationRun) { m.Failed = -1 },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
