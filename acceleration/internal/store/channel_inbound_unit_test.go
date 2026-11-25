package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelInboundValidate(t *testing.T) {
	valid := func() ChannelInbound {
		return ChannelInbound{ChannelID: "thread-one", ProviderMessageID: "1759740000.000100", CustomerID: "acme-app", ConnectorID: "acme", ConfigID: "config-one"}
	}
	message := valid()
	require.NoError(t, message.Validate())

	for field, broken := range map[string]func(*ChannelInbound){
		"channel_id":          func(m *ChannelInbound) { m.ChannelID = "" },
		"provider_message_id": func(m *ChannelInbound) { m.ProviderMessageID = "" },
		"customer_id":         func(m *ChannelInbound) { m.CustomerID = "" },
		"connector_id":        func(m *ChannelInbound) { m.ConnectorID = "" },
		"config_id":           func(m *ChannelInbound) { m.ConfigID = "" },
		"state":               func(m *ChannelInbound) { m.State = "written" },
	} {
		m := valid()
		broken(&m)
		var invalid *ValidationError
		require.True(t, errors.As(m.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
