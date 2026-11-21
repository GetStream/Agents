package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallLegValidate(t *testing.T) {
	valid := func() CallLeg {
		return CallLeg{CustomerID: "app", CallType: "default", CallID: "c", Direction: Outbound}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*CallLeg){
		"customer_id": func(m *CallLeg) { m.CustomerID = "" },
		"call_type":   func(m *CallLeg) { m.CallType = "" },
		"call_id":     func(m *CallLeg) { m.CallID = "" },
		"direction":   func(m *CallLeg) { m.Direction = "sideways" },
		"end_reason":  func(m *CallLeg) { m.EndReason = "bored" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
