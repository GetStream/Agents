package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVoiceValidate(t *testing.T) {
	valid := func() Voice {
		return Voice{CustomerID: "app", Name: "founder"}
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*Voice){
		"customer_id": func(m *Voice) { m.CustomerID = "" },
		"name":        func(m *Voice) { m.Name = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
