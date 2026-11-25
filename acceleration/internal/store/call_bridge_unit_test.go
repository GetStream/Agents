package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCallBridgeValidate(t *testing.T) {
	valid := func() CallBridge {
		return CallBridge{Token: "tok", CustomerID: "app", Vendor: "plivo", TrunkURI: "sip:x@example.com", ExpiresAt: time.Now()}
	}
	model := valid()
	require.NoError(t, model.Validate())

	for field, broken := range map[string]func(*CallBridge){
		"token":       func(m *CallBridge) { m.Token = "" },
		"customer_id": func(m *CallBridge) { m.CustomerID = "" },
		"vendor":      func(m *CallBridge) { m.Vendor = "" },
		"trunk_uri":   func(m *CallBridge) { m.TrunkURI = "" },
		"expires_at":  func(m *CallBridge) { m.ExpiresAt = time.Time{} },
	} {
		model := valid()
		broken(&model)
		var invalid *ValidationError
		require.True(t, errors.As(model.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
