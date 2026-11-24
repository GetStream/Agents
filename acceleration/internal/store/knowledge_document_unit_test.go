package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKnowledgeDocumentValidate(t *testing.T) {
	valid := func() KnowledgeDocument {
		return KnowledgeDocument{CustomerID: "app", Namespace: "docs", Source: "faq.md", State: KnowledgeDocumentIndexed}
	}
	ok := valid()
	require.NoError(t, ok.Validate())

	for field, broken := range map[string]func(*KnowledgeDocument){
		"customer_id": func(m *KnowledgeDocument) { m.CustomerID = "" },
		"namespace":   func(m *KnowledgeDocument) { m.Namespace = "" },
		"source":      func(m *KnowledgeDocument) { m.Source = "" },
		"state":       func(m *KnowledgeDocument) { m.State = "done" },
		"passages":    func(m *KnowledgeDocument) { m.Passages = -1 },
		"pages":       func(m *KnowledgeDocument) { m.Pages = -1 },
		"bytes":       func(m *KnowledgeDocument) { m.Bytes = -1 },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
