package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCustomModelValidate(t *testing.T) {
	valid := func() CustomModel {
		return CustomModel{CustomerID: "app", Name: "tuned", BaseURL: "https://models.example.com/v1", Model: "llama"}
	}
	require.NoError(t, (&CustomModel{CustomerID: "app", Name: "tuned", BaseURL: "http://10.0.0.7:8000/v1", Model: "llama", TrainsOnData: "no"}).Validate())

	for field, broken := range map[string]func(*CustomModel){
		"customer_id":               func(m *CustomModel) { m.CustomerID = "" },
		"name":                      func(m *CustomModel) { m.Name = "  " },
		"base_url":                  func(m *CustomModel) { m.BaseURL = "models.example.com/v1" },
		"model":                     func(m *CustomModel) { m.Model = "" },
		"context_window":            func(m *CustomModel) { m.ContextWindow = -1 },
		"per_million_input_tokens":  func(m *CustomModel) { m.PerMillionInputTokens = -0.5 },
		"per_million_output_tokens": func(m *CustomModel) { m.PerMillionOutputTokens = -2 },
		"trains_on_data":            func(m *CustomModel) { m.TrainsOnData = "sometimes" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
