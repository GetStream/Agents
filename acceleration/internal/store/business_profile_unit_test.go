package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBusinessProfileValidate(t *testing.T) {
	valid := func() BusinessProfile {
		return BusinessProfile{CustomerID: "app"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*BusinessProfile){
		"customer_id": func(m *BusinessProfile) { m.CustomerID = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
