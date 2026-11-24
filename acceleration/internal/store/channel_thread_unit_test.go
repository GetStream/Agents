package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelThreadValidate(t *testing.T) {
	valid := func() ChannelThread {
		return ChannelThread{ChannelID: "thread-one", CustomerID: "acme-app", ConnectorID: "acme", ThreadKey: "C0000CHAN:1", ConnectionID: "connection-one"}
	}
	thread := valid()
	require.NoError(t, thread.Validate())

	for field, broken := range map[string]func(*ChannelThread){
		"channel_id":    func(th *ChannelThread) { th.ChannelID = "" },
		"customer_id":   func(th *ChannelThread) { th.CustomerID = "" },
		"connector_id":  func(th *ChannelThread) { th.ConnectorID = "" },
		"thread_key":    func(th *ChannelThread) { th.ThreadKey = "" },
		"connection_id": func(th *ChannelThread) { th.ConnectionID = "" },
	} {
		th := valid()
		broken(&th)
		var invalid *ValidationError
		require.True(t, errors.As(th.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
