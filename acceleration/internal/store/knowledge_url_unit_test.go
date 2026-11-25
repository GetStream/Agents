package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKnowledgeURLValidate(t *testing.T) {
	valid := func() KnowledgeURL {
		return KnowledgeURL{CustomerID: "app", Namespace: "docs", URL: "https://acme.example", State: KnowledgeURLPending}
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*KnowledgeURL){
		"customer_id":   func(m *KnowledgeURL) { m.CustomerID = "" },
		"namespace":     func(m *KnowledgeURL) { m.Namespace = "" },
		"url":           func(m *KnowledgeURL) { m.URL = "" },
		"state":         func(m *KnowledgeURL) { m.State = "" },
		"passages":      func(m *KnowledgeURL) { m.Passages = -1 },
		"refresh_hours": func(m *KnowledgeURL) { m.RefreshHours = -1 },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
