package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCallLegValidate(t *testing.T) {
	valid := func() CallLeg {
		return CallLeg{CustomerID: "app", CallType: "default", CallID: "c", Direction: Outbound, DialedAt: time.Now()}
	}
	model := valid()
	require.NoError(t, model.Validate())
	placed := time.Now()
	model.DialedAt, model.PlacedAt, model.Token, model.Outcome = time.Time{}, &placed, "token", "busy"
	require.NoError(t, model.Validate(), "a placed leg not dialled yet")

	for field, broken := range map[string]func(*CallLeg){
		"customer_id": func(m *CallLeg) { m.CustomerID = "" },
		"call_type":   func(m *CallLeg) { m.CallType = "" },
		"call_id":     func(m *CallLeg) { m.CallID = "" },
		"direction":   func(m *CallLeg) { m.Direction = "sideways" },
		"end_reason":  func(m *CallLeg) { m.EndReason = "bored" },
		"outcome":     func(m *CallLeg) { m.Outcome = "maybe" },
		"placed_at":   func(m *CallLeg) { m.Token = "token" },
		"dialed_at":   func(m *CallLeg) { m.DialedAt = time.Time{} },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
