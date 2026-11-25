package store

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelLinkValidate(t *testing.T) {
	valid := func() ChannelLink {
		return ChannelLink{Code: "code-one", CustomerID: "acme-app", ConfigID: "config-one", UserID: "user-one", ExpiresAt: time.Now()}
	}
	link := valid()
	require.NoError(t, link.Validate())

	for field, broken := range map[string]func(*ChannelLink){
		"code":        func(l *ChannelLink) { l.Code = "" },
		"customer_id": func(l *ChannelLink) { l.CustomerID = "" },
		"config_id":   func(l *ChannelLink) { l.ConfigID = "" },
		"user_id":     func(l *ChannelLink) { l.UserID = "" },
		"expires_at":  func(l *ChannelLink) { l.ExpiresAt = time.Time{} },
	} {
		l := valid()
		broken(&l)
		var invalid *ValidationError
		require.True(t, errors.As(l.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
