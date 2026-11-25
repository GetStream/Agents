package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKnowledgeNamespaceValidate(t *testing.T) {
	valid := func() KnowledgeNamespace {
		return KnowledgeNamespace{CustomerID: "app", Namespace: "docs", Region: "eu-west-1", Account: KnowledgeAccountStream}
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*KnowledgeNamespace){
		"customer_id": func(m *KnowledgeNamespace) { m.CustomerID = "" },
		"namespace":   func(m *KnowledgeNamespace) { m.Namespace = "" },
		"region":      func(m *KnowledgeNamespace) { m.Region = "EU" },
		"account":     func(m *KnowledgeNamespace) { m.Account = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
