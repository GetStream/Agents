package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSandboxRecipientValidate(t *testing.T) {
	valid := func() SandboxRecipient {
		return SandboxRecipient{CustomerID: "app", Recipient: "+15550000001"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*SandboxRecipient){
		"customer_id": func(m *SandboxRecipient) { m.CustomerID = "" },
		"recipient":   func(m *SandboxRecipient) { m.Recipient = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
