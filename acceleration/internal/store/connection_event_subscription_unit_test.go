package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionEventSubscriptionValidate(t *testing.T) {
	valid := func() ConnectionEventSubscription {
		return ConnectionEventSubscription{CustomerID: "acme-app", ConnectionID: "connection", ConfigID: "config",
			Binding: "github", Event: "push", Key: "repo", Token: "token", SecretSealed: []byte("sealed"), KEKVersion: 1,
			Status: ConnectionEventPending}
	}
	for _, status := range []string{ConnectionEventPending, ConnectionEventActive, ConnectionEventFailed} {
		sub := valid()
		sub.Status = status
		require.NoError(t, sub.Validate(), status)
	}

	for field, broken := range map[string]func(*ConnectionEventSubscription){
		"customer_id":   func(s *ConnectionEventSubscription) { s.CustomerID = "" },
		"connection_id": func(s *ConnectionEventSubscription) { s.ConnectionID = "" },
		"config_id":     func(s *ConnectionEventSubscription) { s.ConfigID = "" },
		"binding":       func(s *ConnectionEventSubscription) { s.Binding = "" },
		"event":         func(s *ConnectionEventSubscription) { s.Event = "" },
		"key":           func(s *ConnectionEventSubscription) { s.Key = "" },
		"token":         func(s *ConnectionEventSubscription) { s.Token = "" },
		"secret_sealed": func(s *ConnectionEventSubscription) { s.SecretSealed = nil },
		"kek_version":   func(s *ConnectionEventSubscription) { s.KEKVersion = 0 },
		"status":        func(s *ConnectionEventSubscription) { s.Status = "accepted" },
		"failures":      func(s *ConnectionEventSubscription) { s.Failures = -1 },
		"attempts":      func(s *ConnectionEventSubscription) { s.Attempts = -1 },
	} {
		sub := valid()
		broken(&sub)
		var invalid *ValidationError
		require.True(t, errors.As(sub.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
