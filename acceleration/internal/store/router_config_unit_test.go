package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouterConfigValidate(t *testing.T) {
	valid := func() RouterConfig {
		return RouterConfig{CustomerID: "app", Name: "fast"}
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*RouterConfig){
		"customer_id": func(m *RouterConfig) { m.CustomerID = "" },
		"name":        func(m *RouterConfig) { m.Name = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
