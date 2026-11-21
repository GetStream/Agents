package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelThreadMessageValidate(t *testing.T) {
	valid := func() channelThreadMessage {
		return channelThreadMessage{ChannelID: "thread-one", Kind: ClaimReply, MessageID: "msg-one"}
	}
	message := valid()
	require.NoError(t, message.Validate())

	for field, broken := range map[string]func(*channelThreadMessage){
		"channel_id": func(m *channelThreadMessage) { m.ChannelID = "" },
		"kind":       func(m *channelThreadMessage) { m.Kind = "answer" },
		"message_id": func(m *channelThreadMessage) { m.MessageID = "" },
	} {
		m := valid()
		broken(&m)
		var invalid *ValidationError
		require.True(t, errors.As(m.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
