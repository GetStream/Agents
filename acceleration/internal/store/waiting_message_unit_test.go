package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWaitingMessageValidate(t *testing.T) {
	valid := func() waitingMessage {
		return waitingMessage{CustomerID: "acme-app", ConnectorID: "acme", ThreadKey: "C0000CHAN:1", ProviderMessageID: "1759740000.000200", Raw: []byte(`{}`)}
	}
	message := valid()
	require.NoError(t, message.Validate())

	for field, broken := range map[string]func(*waitingMessage){
		"customer_id":         func(m *waitingMessage) { m.CustomerID = "" },
		"connector_id":        func(m *waitingMessage) { m.ConnectorID = "" },
		"thread_key":          func(m *waitingMessage) { m.ThreadKey = "" },
		"provider_message_id": func(m *waitingMessage) { m.ProviderMessageID = "" },
		"raw":                 func(m *waitingMessage) { m.Raw = nil },
	} {
		m := valid()
		broken(&m)
		var invalid *ValidationError
		require.True(t, errors.As(m.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
