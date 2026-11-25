package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimulationCaseValidate(t *testing.T) {
	valid := func() SimulationCase {
		return SimulationCase{RunID: "run", State: SimulationPending}
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*SimulationCase){
		"run_id": func(m *SimulationCase) { m.RunID = "" },
		"state":  func(m *SimulationCase) { m.State = "" },
		"ended":  func(m *SimulationCase) { m.Ended = "bored" },
		"turns":  func(m *SimulationCase) { m.Turns = -1 },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
