package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamTrunkValidate(t *testing.T) {
	valid := func() StreamTrunk {
		return StreamTrunk{ID: "t1", CustomerID: "app", Vendor: "telnyx"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*StreamTrunk){
		"id":          func(m *StreamTrunk) { m.ID = "" },
		"customer_id": func(m *StreamTrunk) { m.CustomerID = "" },
		"vendor":      func(m *StreamTrunk) { m.Vendor = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
