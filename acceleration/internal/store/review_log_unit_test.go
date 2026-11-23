package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReviewLogValidate(t *testing.T) {
	valid := func() ReviewLog {
		return ReviewLog{UseCaseID: "uc", CustomerID: "app", Actor: "app"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*ReviewLog){
		"use_case_id": func(m *ReviewLog) { m.UseCaseID = "" },
		"customer_id": func(m *ReviewLog) { m.CustomerID = "" },
		"actor":       func(m *ReviewLog) { m.Actor = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
