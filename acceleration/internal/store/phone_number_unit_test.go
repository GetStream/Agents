package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPhoneNumberValidate(t *testing.T) {
	valid := func() PhoneNumber {
		return PhoneNumber{CustomerID: "app", E164: "+15551234567", Vendor: "telnyx", Country: "US"}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*PhoneNumber){
		"customer_id":         func(m *PhoneNumber) { m.CustomerID = "" },
		"e164":                func(m *PhoneNumber) { m.E164 = "15551234567" },
		"vendor":              func(m *PhoneNumber) { m.Vendor = " " },
		"monthly_cost_micros": func(m *PhoneNumber) { m.MonthlyCostMicros = -1 },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
