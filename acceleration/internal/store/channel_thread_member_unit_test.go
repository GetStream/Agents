package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelThreadMemberValidate(t *testing.T) {
	valid := func() channelThreadMember {
		return channelThreadMember{ChannelID: "thread-one", Member: "+12025550199"}
	}
	member := valid()
	require.NoError(t, member.Validate())

	for field, broken := range map[string]func(*channelThreadMember){
		"channel_id": func(m *channelThreadMember) { m.ChannelID = "" },
		"member":     func(m *channelThreadMember) { m.Member = "" },
	} {
		m := valid()
		broken(&m)
		var invalid *ValidationError
		require.True(t, errors.As(m.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
