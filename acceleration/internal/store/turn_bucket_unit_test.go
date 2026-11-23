package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTurnSampleValidate(t *testing.T) {
	valid := func() TurnSample { return TurnSample{CustomerID: "acme", AgentID: "agent-1"} }
	sample := valid()
	require.NoError(t, sample.Validate())

	for field, broken := range map[string]func(*TurnSample){
		"customer_id": func(s *TurnSample) { s.CustomerID = "" },
		"agent_id":    func(s *TurnSample) { s.AgentID = "" },
	} {
		sample := valid()
		broken(&sample)
		var invalid *ValidationError
		require.True(t, errors.As(sample.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
