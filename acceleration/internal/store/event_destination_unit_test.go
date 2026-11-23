package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventDestinationValidate(t *testing.T) {
	valid := func() EventDestination {
		return EventDestination{ID: "destination", CustomerID: "acme-app", ConnectorID: "github", URL: "https://example.com/hooks",
			Forward: ForwardAll, SecretSealed: []byte("sealed"), KEKVersion: 1}
	}
	destination := valid()
	require.NoError(t, destination.Validate())

	for field, broken := range map[string]func(*EventDestination){
		"id":            func(d *EventDestination) { d.ID = "" },
		"customer_id":   func(d *EventDestination) { d.CustomerID = "" },
		"connector_id":  func(d *EventDestination) { d.ConnectorID = "" },
		"url":           func(d *EventDestination) { d.URL = "" },
		"forward":       func(d *EventDestination) { d.Forward = "handled" },
		"secret_sealed": func(d *EventDestination) { d.SecretSealed = nil },
		"kek_version":   func(d *EventDestination) { d.KEKVersion = 0 },
	} {
		destination := valid()
		broken(&destination)
		var invalid *ValidationError
		require.True(t, errors.As(destination.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}

func TestEventDeliveryValidate(t *testing.T) {
	valid := func() EventDelivery {
		return EventDelivery{ID: "msg_1", Body: []byte(`{}`), NextAttemptAt: time.Now()}
	}
	delivery := valid()
	require.NoError(t, delivery.Validate())

	for field, broken := range map[string]func(*EventDelivery){
		"id":              func(d *EventDelivery) { d.ID = "" },
		"body":            func(d *EventDelivery) { d.Body = nil },
		"attempts":        func(d *EventDelivery) { d.Attempts = -1 },
		"next_attempt_at": func(d *EventDelivery) { d.NextAttemptAt = time.Time{} },
	} {
		delivery := valid()
		broken(&delivery)
		var invalid *ValidationError
		require.True(t, errors.As(delivery.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
