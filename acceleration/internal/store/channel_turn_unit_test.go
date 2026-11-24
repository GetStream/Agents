package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelTurnValidate(t *testing.T) {
	valid := func() ChannelTurn {
		return ChannelTurn{StreamAppPK: 7, CID: "callouts:one", LeasedBy: "router-a", LeaseUntil: time.Now()}
	}
	deployment := valid()
	deployment.StreamAppPK = 0
	require.NoError(t, deployment.Validate())

	for field, broken := range map[string]func(*ChannelTurn){
		"cid":         func(turn *ChannelTurn) { turn.CID = "" },
		"leased_by":   func(turn *ChannelTurn) { turn.LeasedBy = "" },
		"lease_until": func(turn *ChannelTurn) { turn.LeaseUntil = time.Time{} },
	} {
		turn := valid()
		broken(&turn)
		var invalid *ValidationError
		require.True(t, errors.As(turn.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
