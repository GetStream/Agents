package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPhoneNumberOrderValidate(t *testing.T) {
	valid := func() PhoneNumberOrder {
		return PhoneNumberOrder{CustomerID: "app", Vendor: "telnyx", E164: "+15551234567", Action: OrderBuy, CreatedAt: time.Now(),
			NextAttemptAt: time.Now()}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*PhoneNumberOrder){
		"customer_id":     func(m *PhoneNumberOrder) { m.CustomerID = "" },
		"vendor":          func(m *PhoneNumberOrder) { m.Vendor = "" },
		"e164":            func(m *PhoneNumberOrder) { m.E164 = "15551234567" },
		"action":          func(m *PhoneNumberOrder) { m.Action = "port" },
		"created_at":      func(m *PhoneNumberOrder) { m.CreatedAt = time.Time{} },
		"next_attempt_at": func(m *PhoneNumberOrder) { m.NextAttemptAt = time.Time{} },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
