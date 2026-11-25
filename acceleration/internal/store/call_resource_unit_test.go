package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallResourceValidate(t *testing.T) {
	valid := func() CallResource {
		return CallResource{TrunkID: "t", RouteID: "r", CallID: "c"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*CallResource){
		"trunk_id": func(m *CallResource) { m.TrunkID = "" },
		"route_id": func(m *CallResource) { m.RouteID = "" },
		"call_id":  func(m *CallResource) { m.CallID = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
