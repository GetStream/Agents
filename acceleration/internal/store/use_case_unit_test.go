package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUseCaseValidate(t *testing.T) {
	valid := func() UseCase {
		return UseCase{CustomerID: "app", Name: "alerts", Status: "draft"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*UseCase){
		"customer_id": func(m *UseCase) { m.CustomerID = "" },
		"name":        func(m *UseCase) { m.Name = "" },
		"status":      func(m *UseCase) { m.Status = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
