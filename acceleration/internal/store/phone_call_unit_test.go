package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPhoneCallValidate(t *testing.T) {
	valid := func() PhoneCall {
		return PhoneCall{CustomerID: "app", CallType: "default", CallID: "c", Vendor: "telnyx", PlacedAt: time.Now()}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*PhoneCall){
		"customer_id": func(m *PhoneCall) { m.CustomerID = "" },
		"call_type":   func(m *PhoneCall) { m.CallType = "" },
		"call_id":     func(m *PhoneCall) { m.CallID = "" },
		"vendor":      func(m *PhoneCall) { m.Vendor = "" },
		"outcome":     func(m *PhoneCall) { m.Outcome = "maybe" },
		"placed_at":   func(m *PhoneCall) { m.PlacedAt = time.Time{} },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
