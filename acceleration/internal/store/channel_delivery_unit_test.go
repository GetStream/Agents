package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelDeliveryValidate(t *testing.T) {
	valid := func() channelDelivery {
		return channelDelivery{AccountID: "account-one", MessageID: "wamid.one"}
	}
	delivery := valid()
	require.NoError(t, delivery.Validate())

	for field, broken := range map[string]func(*channelDelivery){
		"account_id": func(d *channelDelivery) { d.AccountID = "" },
		"message_id": func(d *channelDelivery) { d.MessageID = "" },
	} {
		d := valid()
		broken(&d)
		var invalid *ValidationError
		require.True(t, errors.As(d.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
