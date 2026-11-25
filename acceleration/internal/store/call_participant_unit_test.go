package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallParticipantValidate(t *testing.T) {
	valid := func() CallParticipant {
		return CallParticipant{CallType: "default", CallID: "c"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*CallParticipant){
		"call_type": func(m *CallParticipant) { m.CallType = "" },
		"call_id":   func(m *CallParticipant) { m.CallID = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
