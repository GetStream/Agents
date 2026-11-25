package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChatInboundValidate(t *testing.T) {
	valid := func() ChatInbound {
		return ChatInbound{StreamAppPK: 1257545, MessageID: "msg-one", Body: []byte(`{}`), State: ChatInboundQueued}
	}
	delivery := valid()
	require.NoError(t, delivery.Validate())

	for field, broken := range map[string]func(*ChatInbound){
		"stream_app_pk": func(d *ChatInbound) { d.StreamAppPK = 0 },
		"message_id":    func(d *ChatInbound) { d.MessageID = "" },
		"body":          func(d *ChatInbound) { d.Body = nil },
		"state":         func(d *ChatInbound) { d.State = "answered" },
	} {
		d := valid()
		broken(&d)
		var invalid *ValidationError
		require.True(t, errors.As(d.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
