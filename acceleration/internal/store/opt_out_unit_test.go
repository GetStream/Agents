package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptOutValidate(t *testing.T) {
	valid := func() OptOut {
		return OptOut{CustomerID: "app", Recipient: "+15550000001", Channel: "sms"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*OptOut){
		"customer_id": func(m *OptOut) { m.CustomerID = "" },
		"recipient":   func(m *OptOut) { m.Recipient = "" },
		"channel":     func(m *OptOut) { m.Channel = "" },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
